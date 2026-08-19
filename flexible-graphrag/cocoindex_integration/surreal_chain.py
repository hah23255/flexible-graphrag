"""CocoIndex native SurrealDB QA chain.

This module owns the SurrealQL generation and execution logic for the flat
schema that CocoIndex writes to SurrealDB:

- ``graph_entity`` table — id, name, entity_type, dynamic props
- ``graph_chunk``  table — id, text, doc_id, filename
- ``relation_{pred}`` edge tables (e.g. ``relation_works_for``)
- ``mentions`` edge table  (graph_chunk → graph_entity)

Unlike the LangChain ``SurrealDBAdapter`` schema (per-type tables such as
``graph_Company`` / ``graph_Person`` with uppercase relation tables), this flat
layout requires its own prompt and query logic.  Keeping it here rather than in
the general ``langchain/`` tree ensures the CocoIndex-specific knowledge stays
inside ``cocoindex_integration``.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

_logger = logging.getLogger(__name__)

_UUID_PREFIX_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:",
    re.IGNORECASE,
)
_QUESTION_STOPWORDS = frozenset({
    "who", "what", "where", "when", "how", "is", "are", "was", "were",
    "the", "a", "an", "for", "works", "work", "at", "in", "of", "does", "do",
    "have", "has", "had", "organized", "tell", "me", "about", "with", "from",
})


class _CocoSurrealDBChain:
    """Self-contained QA chain for the CocoIndex native SurrealDB schema.

    CocoIndex writes a flat schema:
    - ``graph_entity`` table: id, name, entity_type, [dynamic entity props]
    - ``graph_chunk``  table: id, text, doc_id, filename
    - ``relation_{rel_type}`` edge tables (e.g. ``relation_works_for``)
    - ``mentions`` edge table (graph_chunk → graph_entity)

    This chain introspects the schema once at construction time, then
    uses the LLM to generate SurrealQL tailored for this layout.
    """

    output_key = "result"

    def __init__(self, conn_cfg: dict, llm: Any, include_intermediate: bool = False) -> None:
        self._cfg = conn_cfg
        self._llm = llm
        self._include_intermediate = include_intermediate
        self._rel_tables: list[str] = []
        self._schema_str = self._build_schema_str()
        self._last_source_filename: str = ""

    # ------------------------------------------------------------------
    # Low-level DB helpers
    # ------------------------------------------------------------------

    def _connect(self) -> Any:
        from surrealdb.connections.blocking_ws import BlockingWsSurrealConnection  # noqa: PLC0415
        conn = BlockingWsSurrealConnection(self._cfg["url"])
        conn.signin({"username": self._cfg["username"], "password": self._cfg["password"]})
        conn.use(self._cfg["namespace"], self._cfg["database"])
        return conn

    def _normalize_query_rows(self, raw: Any) -> list[Any]:
        """Flatten SurrealDB query responses into a list of row dicts or scalars."""
        if raw is None:
            return []
        if isinstance(raw, dict):
            if "result" in raw:
                return self._normalize_query_rows(raw["result"])
            return [raw]
        if not isinstance(raw, list):
            return [raw]
        rows: list[Any] = []
        for item in raw:
            if isinstance(item, dict) and "result" in item:
                rows.extend(self._normalize_query_rows(item["result"]))
            elif isinstance(item, list):
                rows.extend(self._normalize_query_rows(item))
            elif isinstance(item, dict):
                rows.append(item)
            else:
                rows.append(item)
        return rows

    def _safe_query(self, surql: str, params: dict | None = None) -> list:
        try:
            conn = self._connect()
            try:
                result = conn.query(surql, params or {})
                return self._normalize_query_rows(result)
            finally:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
        except Exception as exc:  # noqa: BLE001
            _logger.debug("CocoSurrealDB query error: %s", exc)
            return []

    def _ensure_rel_tables(self) -> None:
        if self._rel_tables:
            return
        try:
            info = self._safe_query("INFO FOR DB")
            if info and isinstance(info[0], dict):
                tables = info[0].get("tables", {})
                self._rel_tables = [
                    str(t) for t in tables if str(t).startswith("relation_")
                ]
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def _build_schema_str(self) -> str:
        entity_types: list[str] = []
        try:
            rows = self._safe_query("SELECT DISTINCT entity_type FROM graph_entity LIMIT 50")
            for r in rows:
                if isinstance(r, dict):
                    v = r.get("entity_type")
                    if v:
                        entity_types.append(str(v))
        except Exception:  # noqa: BLE001
            pass

        rel_tables: list[str] = []
        try:
            info = self._safe_query("INFO FOR DB")
            if info and isinstance(info[0], dict):
                tables = info[0].get("tables", {})
                for t in tables:
                    if str(t).startswith("relation_"):
                        rel_tables.append(str(t))
        except Exception:  # noqa: BLE001
            pass

        parts = [
            "CocoIndex native SurrealDB schema:",
            "TABLE graph_entity: id, name (string), entity_type (string), [dynamic entity properties]",
            "TABLE graph_chunk: id, text (string), doc_id (string), filename (string)",
        ]
        if entity_types:
            parts.append(f"Known entity_type values: {', '.join(sorted(entity_types))}")
        if rel_tables:
            parts.append("Relation tables (each has in->out linking graph_entity records):")
            for rt in sorted(rel_tables):
                parts.append(f"  {rt}: in (graph_entity), out (graph_entity), doc_id, predicate")
        else:
            parts.append(
                "Relation tables: relation_<predicate> — e.g. relation_works_for, "
                "relation_has_department, relation_has_location"
            )
        parts.append("TABLE mentions: in (graph_chunk), out (graph_entity) — links chunks to entities")
        self._rel_tables = rel_tables
        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Enrichment — relation paths + source chunk text
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_display_name(name: Any) -> str:
        text = str(name or "").strip()
        return _UUID_PREFIX_RE.sub("", text).strip()

    @classmethod
    def _extract_keyword(cls, question: str) -> str:
        words = re.findall(r"[a-z0-9]+", (question or "").lower())
        candidates = [w for w in words if w not in _QUESTION_STOPWORDS and len(w) >= 3]
        if candidates:
            return candidates[-1]
        return words[-1] if words else ""

    @staticmethod
    def _rel_table_to_predicate(table: str) -> str:
        return table.removeprefix("relation_").upper()

    @staticmethod
    def _normalize_chunk_id(raw_id: Any) -> str:
        text = str(raw_id or "").strip()
        text = text.replace("⟨", "").replace("⟩", "")
        if text.startswith("graph_chunk:"):
            return text.split(":", 1)[-1]
        return text

    def _fetch_chunk_records(self, keyword: str) -> list[dict[str, str]]:
        """Return one record per source chunk for documents matching *keyword*."""
        if not keyword:
            return []
        kw = keyword.replace('"', "")
        doc_id_rows = self._safe_query(
            "SELECT VALUE doc_id FROM graph_entity "
            f'WHERE string::lowercase(name) CONTAINS "{kw}" '
            "LIMIT 10"
        )
        doc_ids: list[str] = []
        for row in doc_id_rows:
            doc_id = str(row).strip()
            if doc_id and doc_id not in doc_ids:
                doc_ids.append(doc_id)
        if not doc_ids:
            return []

        in_clause = ", ".join(f'"{doc_id}"' for doc_id in doc_ids)
        rows = self._safe_query(
            "SELECT text, file_name, id, doc_id, chunk_index FROM graph_chunk "
            f"WHERE doc_id IN [{in_clause}] "
            "ORDER BY chunk_index, id"
        )
        records: list[dict[str, str]] = []
        seen_ids: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            text_str = str(row.get("text") or "").strip()
            if not text_str:
                continue
            chunk_id = self._normalize_chunk_id(row.get("id") or text_str[:80])
            if chunk_id in seen_ids:
                continue
            seen_ids.add(chunk_id)
            filename = str(row.get("filename") or row.get("file_name") or "").strip()
            records.append({
                "id": chunk_id,
                "text": text_str,
                "filename": filename,
                "doc_id": str(row.get("doc_id") or "").strip(),
            })
        return records

    @staticmethod
    def _chunk_id_variants(chunk_id: str) -> list[str]:
        cid = str(chunk_id or "").strip()
        if not cid:
            return []
        bare = cid.split(":", 1)[-1] if cid.startswith("graph_chunk:") else cid
        variants = [bare, cid]
        if not cid.startswith("graph_chunk:"):
            variants.append(f"graph_chunk:{bare}")
        return list(dict.fromkeys(v for v in variants if v))

    def _fetch_paths_for_chunk(self, chunk_id: str, doc_id: str = "") -> list[str]:
        """Return relation path lines extracted for a single source chunk."""
        if not chunk_id:
            return []
        self._ensure_rel_tables()
        paths: list[str] = []
        seen: set[str] = set()
        id_variants = self._chunk_id_variants(chunk_id)
        for table in sorted(self._rel_tables):
            predicate = self._rel_table_to_predicate(table)
            for cid in id_variants:
                rows = self._safe_query(
                    f"SELECT in.name AS subject, out.name AS object FROM {table} "
                    "WHERE chunk_id = $chunk_id "
                    "LIMIT 40",
                    {"chunk_id": cid},
                )
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    subject = self._clean_display_name(row.get("subject"))
                    obj = self._clean_display_name(row.get("object"))
                    if not subject or not obj:
                        continue
                    line = f"{subject} -> {predicate} -> {obj}"
                    if line in seen:
                        continue
                    seen.add(line)
                    paths.append(line)
        if paths or not doc_id:
            return paths
        # Last resort: same doc_id when chunk_id was stored in a different format.
        for table in sorted(self._rel_tables):
            predicate = self._rel_table_to_predicate(table)
            rows = self._safe_query(
                f"SELECT in.name AS subject, out.name AS object, chunk_id FROM {table} "
                "WHERE doc_id = $doc_id "
                "LIMIT 80",
                {"doc_id": doc_id},
            )
            for row in rows:
                if not isinstance(row, dict):
                    continue
                stored_cid = self._normalize_chunk_id(row.get("chunk_id"))
                if stored_cid not in id_variants:
                    continue
                subject = self._clean_display_name(row.get("subject"))
                obj = self._clean_display_name(row.get("object"))
                if not subject or not obj:
                    continue
                line = f"{subject} -> {predicate} -> {obj}"
                if line in seen:
                    continue
                seen.add(line)
                paths.append(line)
        return paths

    @staticmethod
    def _combine_chunk_result(paths: list[str], chunk_text: str) -> str:
        """Merge relation paths and chunk paragraph into one search result body."""
        path_block = "\n".join(paths).strip()
        chunk_text = (chunk_text or "").strip()
        if path_block and chunk_text:
            return f"{path_block}\n\n{chunk_text}"
        return path_block or chunk_text

    def _fetch_relation_paths(self, keyword: str) -> list[str]:
        """Return human-readable triplet lines for relations touching *keyword*."""
        if not keyword:
            return []
        self._ensure_rel_tables()
        paths: list[str] = []
        seen: set[str] = set()
        kw = keyword.replace('"', "")
        for table in sorted(self._rel_tables):
            predicate = self._rel_table_to_predicate(table)
            surql = (
                f"SELECT in.name AS subject, out.name AS object FROM {table} "
                f'WHERE string::lowercase(in.name) CONTAINS "{kw}" '
                f'OR string::lowercase(out.name) CONTAINS "{kw}" '
                "LIMIT 40;"
            )
            for row in self._safe_query(surql):
                if not isinstance(row, dict):
                    continue
                subject = self._clean_display_name(row.get("subject"))
                obj = self._clean_display_name(row.get("object"))
                if not subject or not obj:
                    continue
                line = f"{subject} -> {predicate} -> {obj}"
                if line in seen:
                    continue
                seen.add(line)
                paths.append(line)
        return paths

    @staticmethod
    def _is_no_result_answer(text: str) -> bool:
        t = (text or "").strip().lower()
        if not t:
            return True
        no_data = (
            "i don't know", "i do not know", "no information",
            "not enough information", "cannot find", "could not find",
        )
        return any(t.startswith(p) for p in no_data)

    def build_result_nodes(self, question: str, top_k: int = 10) -> list[dict[str, Any]]:
        """Build one result per source chunk with paths + paragraph text combined."""
        keyword = self._extract_keyword(question)
        chunks = self._fetch_chunk_records(keyword)
        default_filename = chunks[0]["filename"] if chunks else ""
        self._last_source_filename = default_filename

        hits: list[dict[str, Any]] = []
        path_counts: list[int] = []
        for i, chunk in enumerate(chunks[:top_k]):
            paths = self._fetch_paths_for_chunk(
                chunk.get("id", ""),
                doc_id=chunk.get("doc_id", ""),
            )
            path_counts.append(len(paths))
            text = self._combine_chunk_result(paths, chunk.get("text", ""))
            if not text:
                continue
            hits.append({
                "text": text,
                "file_name": chunk.get("filename") or default_filename,
                "score": max(0.5, 1.0 - i * 0.05),
                "kind": "chunk",
            })

        if hits:
            _logger.info(
                "CocoSurrealDB chunk results: keyword=%r chunks=%d/%d paths_per_chunk=%s",
                keyword,
                len(hits),
                len(chunks),
                path_counts,
            )
            return hits

        # Fallback when no chunks matched — use keyword-wide paths or LLM answer.
        paths = self._fetch_relation_paths(keyword)
        if paths:
            hits.append({
                "text": "\n".join(paths[:top_k]),
                "file_name": default_filename,
                "score": 1.0,
                "kind": "path",
            })
            return hits

        surql = self._generate_surql(question)
        _logger.info("CocoSurrealDB generated SurrealQL (fallback): %s", surql)
        results = self._safe_query(surql)
        context = self._format_context(results)
        answer = self._generate_answer(question, context)
        if answer and not self._is_no_result_answer(answer):
            hits.append({
                "text": answer.strip(),
                "file_name": default_filename,
                "score": 1.0,
                "kind": "answer",
            })
        return hits[:top_k]

    # ------------------------------------------------------------------
    # Query generation + answering
    # ------------------------------------------------------------------

    _SURQL_GEN_TEMPLATE = """\
You are a SurrealDB expert querying a CocoIndex knowledge-graph.

Schema:
{schema}

CRITICAL RULES — follow every rule or the query will return no results:
1. The entity table is 'graph_entity' with an 'entity_type' column.
   There are NO per-type tables like graph_Person or graph_Company.
2. ALWAYS use partial, case-insensitive matching on names:
       WHERE string::lowercase(name) CONTAINS "keyword"
   Use only lowercase in the keyword string. NEVER use exact = equality on names.
3. Filter by entity_type using case-insensitive comparison:
       WHERE string::lowercase(entity_type) = "company"
   entity_type values may be uppercase (e.g. COMPANY, PERSON) — always use string::lowercase().
4. Use array::distinct() on every traversal to remove duplicates.
5. ALWAYS append .name to traversals to get entity names instead of RecordIDs:
       <-relation_works_for<-graph_entity.name    (names of people pointing TO entity)
       ->relation_has_department->graph_entity.name (names of departments entity points TO)
6. Return ONLY the SELECT query — no explanation, no markdown fences.
7. DO NOT generate DELETE or UPDATE statements.

Examples:

Q: Who works for Acme?
A: SELECT array::distinct(<-relation_works_for<-graph_entity.name) AS workers
   FROM graph_entity WHERE string::lowercase(entity_type) = "company"
   AND string::lowercase(name) CONTAINS "acme";

Q: What departments does Acme have?
A: SELECT array::distinct(->relation_has_department->graph_entity.name) AS departments
   FROM graph_entity WHERE string::lowercase(entity_type) = "company"
   AND string::lowercase(name) CONTAINS "acme";

Q: What locations or offices does Acme have?
A: SELECT array::distinct(->relation_has_location->graph_entity.name) AS locations
   FROM graph_entity WHERE string::lowercase(entity_type) = "company"
   AND string::lowercase(name) CONTAINS "acme";

Q: How is Acme organized?
A: SELECT
     array::distinct(->relation_has_department->graph_entity.name) AS departments,
     array::distinct(<-relation_works_for<-graph_entity.name) AS employees
   FROM graph_entity WHERE string::lowercase(entity_type) = "company"
   AND string::lowercase(name) CONTAINS "acme";

Q: Who supported CMIS?
A: SELECT array::distinct(<-relation_supports<-graph_entity.name) AS supporters
   FROM graph_entity WHERE string::lowercase(name) CONTAINS "cmis";

User question: {question}

SurrealQL query:"""

    _QA_TEMPLATE = """\
The following are structured results from a knowledge-graph query. \
Each line is a field name followed by the values that were found. \
Use these results to directly answer the question.

Graph query results:
{context}

Question: {question}

Instructions:
- The results above contain the answer — do NOT say you don't know.
- List the names or values found in the results.
- Give a concise, direct answer.

Answer:"""

    def _generate_surql(self, question: str) -> str:
        from langchain_core.prompts import PromptTemplate  # noqa: PLC0415
        prompt = PromptTemplate(
            input_variables=["schema", "question"],
            template=self._SURQL_GEN_TEMPLATE,
        )
        chain = prompt | self._llm
        try:
            resp = chain.invoke({"schema": self._schema_str, "question": question})
            raw = resp.content if hasattr(resp, "content") else str(resp)
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[-1]
                raw = raw.rsplit("```", 1)[0]
            return raw.strip()
        except Exception as exc:  # noqa: BLE001
            _logger.debug("CocoSurrealDB SurrealQL generation error: %s", exc)
            kw = question[:20].lower().replace('"', "")
            return f"SELECT * FROM graph_entity WHERE string::lowercase(name) CONTAINS \"{kw}\""

    @staticmethod
    def _item_to_name(item: Any) -> str:
        """Extract a display name from a SurrealDB result item.

        Handles plain strings, dicts with a 'name' key, RecordID objects
        (returned when .name suffix is omitted), and anything else.
        """
        if isinstance(item, str):
            return item
        if isinstance(item, dict):
            return item.get("name") or item.get("id") or str(item)
        # RecordID / Thing objects expose .id or convert to "table:id" via str()
        rid = getattr(item, "id", None)
        if rid is not None:
            # Try to extract the human-readable record ID part after the colon
            rid_str = str(rid)
            return rid_str.split(":")[-1] if ":" in rid_str else rid_str
        return str(item)

    def _format_context(self, results: list) -> str:
        parts: list[str] = []
        for row in results[:20]:
            if not isinstance(row, dict):
                continue
            for k, v in row.items():
                if v is None:
                    continue
                if isinstance(v, list):
                    names = [self._item_to_name(item) for item in v[:20] if item is not None]
                    if names:
                        parts.append(f"{k}: {', '.join(names)}")
                elif isinstance(v, dict):
                    parts.append(f"{k}: {v.get('name') or v.get('id') or str(v)}")
                else:
                    parts.append(f"{k}: {v}")
        return "\n".join(parts)

    def _generate_answer(self, question: str, context: str) -> str:
        if not context or not context.strip():
            return ""
        from langchain_core.prompts import PromptTemplate  # noqa: PLC0415
        prompt = PromptTemplate(
            input_variables=["context", "question"],
            template=self._QA_TEMPLATE,
        )
        chain = prompt | self._llm
        try:
            resp = chain.invoke({"context": context, "question": question})
            return resp.content if hasattr(resp, "content") else str(resp)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("CocoSurrealDB answer generation error: %s", exc)
            return context

    # ------------------------------------------------------------------
    # Public chain interface (invoke / ainvoke)
    # ------------------------------------------------------------------

    def invoke(self, inputs: Any) -> dict:
        if isinstance(inputs, dict):
            question = (
                inputs.get("query") or inputs.get("question") or
                inputs.get("input") or str(inputs)
            )
        else:
            question = str(inputs)
        hits = self.build_result_nodes(question, top_k=1)
        text = hits[0]["text"] if hits else ""
        surql = ""  # logged inside build_result_nodes
        out: dict = {self.output_key: text, "generated_surql": surql}
        if self._include_intermediate:
            out["intermediate_steps"] = [{"hits": hits}]
        return out

    async def ainvoke(self, inputs: Any) -> dict:
        return await asyncio.to_thread(self.invoke, inputs)
