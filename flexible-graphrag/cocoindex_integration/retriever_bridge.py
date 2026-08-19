"""Read-only retriever construction for CocoIndex-owned stores.

When ``VECTOR_BACKEND=cocoindex`` / ``GRAPH_BACKEND=cocoindex`` the CocoIndex Rust
engine owns ingestion and the LlamaIndex ``VectorStoreIndex`` /
``PropertyGraphIndex`` is intentionally skipped at init.  Read-only store adapters
still exist, so hybrid search must build retrievers that *read* the
CocoIndex-written rows instead of dropping the vector/graph modality entirely.

This module keeps all CocoIndex-schema knowledge (LanceDB / Postgres custom
vector readers, LI-compatible property-graph read via
``PropertyGraphIndex.from_existing``) inside ``cocoindex_integration`` so the base
``retriever_setup.py`` only needs thin one-line hooks.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Matches CocoIndex entity PKs of the form "{uuid}:{entity_name}" where the UUID
# is the 36-character hex-and-dash standard format.  Used to strip the prefix from
# triplet display strings so users see "Acme Corporation" not "4350e2e3-...:Acme Corporation".
_UUID_PREFIX_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:",
    re.IGNORECASE,
)

# LlamaIndex Neo4jPropertyGraphStore.VECTOR_INDEX_NAME (hardcoded upstream).
_LI_NEO4J_VECTOR_INDEX = "entity"
# CocoIndex native Neo4j connector (coco_vec_{label}_{field} after _runtime name cleanup).
_COCO_NEO4J_VECTOR_INDEX = "coco_vec_Entity_embedding"
_DEFAULT_CHUNK_PREAMBLE = "Here are some facts extracted from the provided text:\n\n"


def _make_coco_falkordb_read_graph_store(cfg: dict[str, Any]) -> Any:
    from llama_index.core.graph_stores.types import ChunkNode  # noqa: PLC0415
    from llama_index.core.schema import TextNode  # noqa: PLC0415
    from llama_index.core.vector_stores.utils import metadata_dict_to_node  # noqa: PLC0415
    from llama_index.graph_stores.falkordb import FalkorDBPropertyGraphStore  # noqa: PLC0415

    class _CocoFalkorDBReadGraphStore(FalkorDBPropertyGraphStore):
        """Read CocoIndex chunk nodes that lack LlamaIndex ``_node_content`` metadata.

        Also overrides ``vector_query`` to cast stored embeddings through ``vecf32()``
        because CocoIndex writes embeddings as plain lists, not FalkorDB Vectorf32 type.
        """

        def vector_query(self, query: Any, **kwargs: Any) -> Any:  # type: ignore[override]
            """Override to wrap ``e.embedding`` with ``vecf32()`` so the stored
            list-type embedding can be used with ``vec.euclideanDistance``."""
            conditions = None
            if query.filters:
                conditions = [
                    f"e.{f.key} {f.operator.value} {f.value}"
                    for f in query.filters.filters
                ]
            filters = (
                f" {query.filters.condition.value} ".join(conditions).replace("==", "=")
                if conditions is not None
                else "1 = 1"
            )
            from llama_index.core.graph_stores.types import EntityNode  # noqa: PLC0415

            data = self.structured_query(
                f"""MATCH (e:`__Entity__`)
                WHERE e.embedding IS NOT NULL AND ({filters})
                WITH e, vec.euclideanDistance(vecf32(e.embedding), vecf32($embedding)) AS score
                ORDER BY score LIMIT $limit
                RETURN e.id AS name,
                   [l in labels(e) WHERE l <> '__Entity__' | l][0] AS type,
                   e{{.* , embedding: Null, name: Null, id: Null}} AS properties,
                   score""",
                param_map={
                    "embedding": query.query_embedding,
                    "dimension": len(query.query_embedding),
                    "limit": query.similarity_top_k,
                },
            )
            data = data or []
            nodes, scores = [], []
            for rec in data:
                nodes.append(
                    EntityNode(
                        name=rec["name"],
                        label=rec["type"],
                        properties={k: v for k, v in (rec.get("properties") or {}).items() if v is not None},
                    )
                )
                scores.append(rec["score"])
            return nodes, scores

        def get_llama_nodes(self, node_ids: list[str]) -> list[Any]:
            nodes = self.get(ids=node_ids)
            converted: list[Any] = []
            for node in nodes:
                text = getattr(node, "text", None) or ""
                if isinstance(node, ChunkNode) or text:
                    props = dict(getattr(node, "properties", None) or {})
                    props.setdefault("ref_doc_id", props.get("doc_id", ""))
                    converted.append(TextNode(id_=node.id, text=text, metadata=props))
                    continue
                try:
                    converted.append(metadata_dict_to_node(node.properties))
                    if text:
                        converted[-1].set_content(text)
                except Exception:  # noqa: BLE001
                    continue
            return converted

        async def aget_llama_nodes(self, node_ids: list[str]) -> list[Any]:
            return self.get_llama_nodes(node_ids)

    url = cfg.get("url", cfg.get("uri", "falkor://localhost:6379"))
    database = str(cfg.get("database", cfg.get("graph", "falkor")))
    return _CocoFalkorDBReadGraphStore(
        url=url,
        database=database,
        refresh_schema=False,
    )


def _make_coco_neo4j_read_graph_store(cfg: dict[str, Any]) -> Any:
    from llama_index.core.graph_stores.types import ChunkNode  # noqa: PLC0415
    from llama_index.core.schema import TextNode  # noqa: PLC0415
    from llama_index.core.vector_stores.utils import metadata_dict_to_node  # noqa: PLC0415
    from llama_index.graph_stores.neo4j import Neo4jPropertyGraphStore  # noqa: PLC0415

    class _CocoNeo4jReadGraphStore(Neo4jPropertyGraphStore):
        """Read CocoIndex chunk nodes that lack LlamaIndex ``_node_content`` metadata."""

        def get_llama_nodes(self, node_ids: list[str]) -> list[Any]:
            nodes = self.get(ids=node_ids)
            converted: list[Any] = []
            for node in nodes:
                text = getattr(node, "text", None) or ""
                if isinstance(node, ChunkNode) or text:
                    props = dict(getattr(node, "properties", None) or {})
                    props.setdefault("ref_doc_id", props.get("doc_id", ""))
                    converted.append(
                        TextNode(
                            id_=node.id,
                            text=text,
                            metadata=props,
                        )
                    )
                    continue
                try:
                    converted.append(metadata_dict_to_node(node.properties))
                    if text:
                        converted[-1].set_content(text)
                except Exception:  # noqa: BLE001
                    continue
            return converted

        async def aget_llama_nodes(self, node_ids: list[str]) -> list[Any]:
            return self.get_llama_nodes(node_ids)

    return _CocoNeo4jReadGraphStore(
        username=cfg.get("username", "neo4j"),
        password=cfg["password"],
        url=cfg.get("url", "bolt://localhost:7687"),
        database=cfg.get("database", "neo4j"),
        refresh_schema=False,
        create_indexes=False,
    )


def _looks_like_triplet_only(text: str) -> bool:
    stripped = (text or "").strip()
    return " -> " in stripped and not stripped.startswith(_DEFAULT_CHUNK_PREAMBLE)


def _fetch_chunk_text_via_mentions(graph_store: Any, entity_id: str) -> str:
    """Fallback for graphs ingested before ``triplet_source_id`` was written."""
    try:
        rows = graph_store.structured_query(
            "MATCH (c:__Node__)-[:MENTIONS]->(e:__Entity__ {id: $eid}) "
            "WHERE c.text IS NOT NULL AND c.text <> '' "
            "RETURN c.text AS text LIMIT 1",
            param_map={"eid": entity_id},
        )
        if rows:
            return str(rows[0].get("text") or "")
    except Exception as exc:  # noqa: BLE001
        logger.debug("MENTIONS chunk lookup failed for %s: %s", entity_id, exc)
    return ""


def _enrich_triplet_results_with_chunk_text(
    graph_store: Any, nodes: list[Any]
) -> list[Any]:
    """Prepend source chunk paragraphs when ``add_source_text`` could not."""
    from llama_index.core.schema import NodeWithScore, TextNode  # noqa: PLC0415

    enriched: list[Any] = []
    chunk_cache: dict[str, str] = {}
    for item in nodes:
        if not isinstance(item, NodeWithScore):
            enriched.append(item)
            continue
        text = item.node.get_content() or ""
        if not _looks_like_triplet_only(text):
            enriched.append(item)
            continue

        entity_id = (text.split(" -> ", 1)[0] or "").strip()
        chunk_id = item.node.ref_doc_id
        chunk_text = ""
        if chunk_id:
            if chunk_id not in chunk_cache:
                og_nodes = graph_store.get_llama_nodes([chunk_id])
                chunk_cache[chunk_id] = (
                    og_nodes[0].get_content() if og_nodes else ""
                )
            chunk_text = chunk_cache.get(chunk_id, "")
        elif entity_id:
            cache_key = f"mentions:{entity_id}"
            if cache_key not in chunk_cache:
                chunk_cache[cache_key] = _fetch_chunk_text_via_mentions(
                    graph_store, entity_id
                )
            chunk_text = chunk_cache.get(cache_key, "")

        if not chunk_text:
            enriched.append(item)
            continue

        new_node = TextNode(**item.node.dict())
        new_node.text = (
            _DEFAULT_CHUNK_PREAMBLE + text + "\n\n" + chunk_text
        )
        enriched.append(NodeWithScore(node=new_node, score=item.score))
    return enriched


def _clean_entity_name_prefixes(nodes: list[Any]) -> list[Any]:
    """Strip CocoIndex UUID-prefixed entity IDs from result node texts.

    The CocoIndex native Neo4j connector stores entity primary keys as
    ``"{doc_uuid}:{entity_name}"`` (e.g. ``"4350e2e3-...:Acme Corporation"``).
    LlamaIndex's ``get_rel_map()`` uses ``source.id`` in its RETURN clause, so
    triplet strings end up with this UUID prefix.  This function strips those
    prefixes from the display text after scores have already been computed, so
    score assignment is completely unaffected.
    """
    from llama_index.core.schema import NodeWithScore, TextNode  # noqa: PLC0415

    result: list[Any] = []
    for item in nodes:
        if not isinstance(item, NodeWithScore):
            result.append(item)
            continue
        text = item.node.get_content() or ""
        if not _UUID_PREFIX_RE.search(text):
            result.append(item)
            continue
        clean_text = _UUID_PREFIX_RE.sub("", text)
        new_node = TextNode(**item.node.dict())
        new_node.text = clean_text
        result.append(NodeWithScore(node=new_node, score=item.score))
    return result


class _CocoGraphChunkTextRetriever:
    """Thin wrapper ensuring CocoIndex graph hits include chunk paragraphs."""

    def __init__(self, inner: Any, graph_store: Any) -> None:
        from llama_index.core.base.base_retriever import BaseRetriever  # noqa: PLC0415

        if not isinstance(inner, BaseRetriever):
            raise TypeError("inner retriever must be a LlamaIndex BaseRetriever")
        self._inner = inner
        self._graph_store = graph_store

    def retrieve(self, query_bundle: Any) -> list[Any]:
        nodes = self._inner.retrieve(query_bundle)
        nodes = _enrich_triplet_results_with_chunk_text(self._graph_store, nodes)
        return _clean_entity_name_prefixes(nodes)

    async def aretrieve(self, query_bundle: Any) -> list[Any]:
        nodes = await self._inner.aretrieve(query_bundle)
        nodes = _enrich_triplet_results_with_chunk_text(self._graph_store, nodes)
        return _clean_entity_name_prefixes(nodes)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _discover_neo4j_entity_vector_index(graph_store: Any) -> str:
    """Return the Neo4j VECTOR index on :__Entity__(embedding), if any."""
    try:
        driver = graph_store._driver
        database = getattr(graph_store, "_database", "neo4j")
        with driver.session(database=database) as session:
            rows = list(
                session.run(
                    "SHOW INDEXES YIELD name, type, labelsOrTypes, properties "
                    "WHERE type = 'VECTOR' "
                    "RETURN name, labelsOrTypes, properties"
                )
            )
        entity_indexes = [
            row["name"]
            for row in rows
            if "__Entity__" in (row.get("labelsOrTypes") or [])
            and "embedding" in (row.get("properties") or [])
        ]
        # Prefer CocoIndex-owned index name; fall back to LlamaIndex "entity" if mixed DB.
        for preferred in (_COCO_NEO4J_VECTOR_INDEX, _LI_NEO4J_VECTOR_INDEX):
            if preferred in entity_indexes:
                return preferred
        if entity_indexes:
            return entity_indexes[0]
    except Exception as exc:  # noqa: BLE001
        logger.debug("Could not discover Neo4j entity vector index: %s", exc)
    return _COCO_NEO4J_VECTOR_INDEX


def _patch_li_neo4j_vector_index(graph_store: Any, config: Any) -> None:
    """Point LlamaIndex at the CocoIndex-created Neo4j entity vector index."""
    if str(getattr(config, "pg_graph_db", "")).lower() != "neo4j":
        return
    _graph_backend = (getattr(config, "graph_backend", "llamaindex") or "llamaindex").lower()
    if _graph_backend != "cocoindex":
        return
    try:
        import llama_index.graph_stores.neo4j.neo4j_property_graph as _npg  # noqa: PLC0415
    except ImportError:
        return

    configured = getattr(config, "langchain_pg_vector_index", _LI_NEO4J_VECTOR_INDEX)
    if configured and configured != _LI_NEO4J_VECTOR_INDEX:
        index_name = configured
    else:
        index_name = _discover_neo4j_entity_vector_index(graph_store)

    if index_name != _npg.VECTOR_INDEX_NAME:
        logger.info(
            "CocoIndex read-graph: patching LlamaIndex VECTOR_INDEX_NAME %r -> %r",
            _npg.VECTOR_INDEX_NAME,
            index_name,
        )
        _npg.VECTOR_INDEX_NAME = index_name


def has_cocoindex_read_vector(system: Any, config: Any) -> bool:
    """True when CocoIndex owns the vector store and a read-only adapter exists.

    Requires ``VECTOR_BACKEND=cocoindex``, a non-LangChain vector store adapter
    with a ``get_store()`` method (the LlamaIndex read adapter kept for queries).
    """
    _vec_backend = (getattr(config, "vector_backend", "llamaindex") or "llamaindex").lower()
    _vs = getattr(system, "vector_store", None)
    return (
        _vec_backend == "cocoindex"
        and _vs is not None
        and hasattr(_vs, "get_store")
        and not (hasattr(_vs, "is_langchain") and _vs.is_langchain())
    )


def build_cocoindex_read_vector_retriever(
    system: Any, top_k: int = 10
) -> Optional[Any]:
    """Build a read-only LlamaIndex retriever over a CocoIndex-written vector store.

    Returns the retriever, or ``None`` if construction failed (caller continues
    without the vector modality).  No nodes are added — CocoIndex owns ingestion.

    LanceDB and Postgres are special cases: CocoIndex writes a different schema
    than LlamaIndex (``point_id``/``embedding`` vs ``id``/``vector``/metadata
    dict), so ``LanceDBVectorStore`` / ``PGVectorStore`` cannot read the
    CocoIndex-written rows.  Dedicated retrievers read the CocoIndex schema
    directly.  All other targets (Qdrant, …) write a LlamaIndex-compatible
    schema and use a plain read-only ``VectorStoreIndex``.
    """
    _vs_adapter = system.vector_store
    _adapter_type = type(_vs_adapter).__name__

    if _adapter_type == "LlamaIndexLanceDBAdapter":
        try:
            from cocoindex_integration.connectors.cocoindex.vector.lancedb_retriever import (  # noqa: PLC0415
                CocoIndexLanceDBVectorRetriever,
            )
            retriever = CocoIndexLanceDBVectorRetriever(
                uri=_vs_adapter._lancedb_uri,
                table_name=_vs_adapter._lancedb_table_name,
                embed_model=system.embed_model,
                similarity_top_k=top_k,
            )
            logger.info(
                "CocoIndex LanceDB vector retriever created (uri=%s, table=%s)",
                _vs_adapter._lancedb_uri,
                _vs_adapter._lancedb_table_name,
            )
            return retriever
        except Exception as _ldb_err:  # noqa: BLE001
            logger.warning("Could not create CocoIndex LanceDB retriever: %s", _ldb_err)
            return None

    if _adapter_type == "LlamaIndexPostgresVectorAdapter":
        try:
            from cocoindex_integration.connectors.cocoindex.vector.postgres_retriever import (  # noqa: PLC0415
                CocoIndexPostgresVectorRetriever,
            )
            retriever = CocoIndexPostgresVectorRetriever(
                db_config=_vs_adapter._db_config,
                table_name=_vs_adapter._table_name,
                pg_schema=_vs_adapter._pg_schema,
                embed_model=system.embed_model,
                similarity_top_k=top_k,
            )
            logger.info(
                "CocoIndex Postgres vector retriever created (host=%s, table=%s)",
                _vs_adapter._db_config.get("host", "localhost"),
                _vs_adapter._table_name,
            )
            return retriever
        except Exception as _pg_err:  # noqa: BLE001
            logger.warning("Could not create CocoIndex Postgres retriever: %s", _pg_err)
            return None

    # All other CocoIndex vector targets (Qdrant, …) write a schema compatible
    # with LlamaIndex's VectorStoreIndex.
    try:
        from llama_index.core import VectorStoreIndex  # noqa: PLC0415

        if hasattr(_vs_adapter, "get_fresh_store"):
            _raw_vs = _vs_adapter.get_fresh_store()
        else:
            _raw_vs = _vs_adapter.get_store()
        _ro_index = VectorStoreIndex.from_vector_store(
            _raw_vs, embed_model=system.embed_model
        )
        retriever = _ro_index.as_retriever(
            similarity_top_k=top_k,
            embed_model=system.embed_model,
        )
        logger.info(
            "Vector retriever created from CocoIndex-owned vector store "
            "(read-only, VECTOR_BACKEND=cocoindex, db=%s)",
            _adapter_type,
        )
        return retriever
    except Exception as _err:  # noqa: BLE001
        logger.warning("Could not create CocoIndex read-only vector retriever: %s", _err)
        return None


# CocoIndex property-graph targets whose native connector reproduces the
# LlamaIndex footprint (``:__Node__``/``:__Entity__``/``:Chunk`` labels, ``id``
# primary key, one entity vector index) closely enough that an LlamaIndex
# ``PropertyGraphIndex.from_existing`` can read them.  SurrealDB is intentionally
# excluded — its CocoIndex schema diverges (a single ``graph_entity`` table +
# lowercase ``relation_*`` edges vs the LC per-type ``graph_<Type>`` +
# uppercase ``relation_<PRED>`` schema the QA chain expects).
_COCO_LI_READABLE_PG = frozenset({"neo4j", "falkordb"})


def has_cocoindex_read_graph(system: Any, config: Any) -> bool:
    """True when CocoIndex owns the property graph AND it is LI-readable.

    Requires ``GRAPH_BACKEND=cocoindex`` with KG enabled, a store whose CocoIndex
    connector reproduces the LlamaIndex footprint (neo4j / falkordb), and the LI
    ``PropertyGraphIndex`` skipped at init (``system.graph_index is None`` — the
    normal state for ``GRAPH_BACKEND=cocoindex``).
    """
    _graph_backend = (getattr(config, "graph_backend", "llamaindex") or "llamaindex").lower()
    if _graph_backend != "cocoindex":
        return False
    if not getattr(config, "enable_knowledge_graph", False):
        return False
    _db = str(getattr(config, "pg_graph_db", "none") or "none").lower()
    if _db not in _COCO_LI_READABLE_PG:
        return False
    return getattr(system, "graph_index", None) is None


def _build_coco_read_graph_store(config: Any) -> Any:
    """Build a read-only LlamaIndex property graph store (no index DDL)."""
    _db = str(getattr(config, "pg_graph_db", "none") or "none").lower()
    cfg = config.graph_db_config or {}

    if _db == "neo4j":
        return _make_coco_neo4j_read_graph_store(cfg)

    if _db == "falkordb":
        try:
            return _make_coco_falkordb_read_graph_store(cfg)
        except ImportError:
            logger.warning(
                "CocoIndex read-graph: FalkorDB LlamaIndex store not available — "
                "falling through to DatabaseFactory"
            )

    from factories import DatabaseFactory  # noqa: PLC0415

    return DatabaseFactory.create_graph_store(
        config.pg_graph_db,
        cfg,
        config.get_active_schema(),
        has_separate_vector_store=False,
        llm_provider=config.llm_provider,
        llm_config=config.llm_config,
        app_config=config,
    )


def has_cocoindex_surreal_graph(system: Any, config: Any) -> bool:
    """True when CocoIndex owns a SurrealDB property graph and KG extraction ran.

    Activates when:
    - ``GRAPH_BACKEND=cocoindex``
    - ``PG_GRAPH_DB=surrealdb``
    - ``ENABLE_KNOWLEDGE_GRAPH=true``
    - The LI ``PropertyGraphIndex`` was skipped (``system.graph_index is None``)
    """
    _graph_backend = (getattr(config, "graph_backend", "llamaindex") or "llamaindex").lower()
    if _graph_backend != "cocoindex":
        return False
    if not getattr(config, "enable_knowledge_graph", False):
        return False
    if str(getattr(config, "pg_graph_db", "none") or "none").lower() != "surrealdb":
        return False
    return getattr(system, "graph_index", None) is None


def _make_coco_surreal_graph_retriever(chain: Any, top_k: int) -> Any:
    from llama_index.core.base.base_retriever import BaseRetriever  # noqa: PLC0415
    from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode  # noqa: PLC0415

    def _hits_to_nodes(hits: list[dict[str, Any]]) -> list[Any]:
        nodes: list[Any] = []
        for hit in hits:
            text = str(hit.get("text") or "").strip()
            if not text:
                continue
            file_name = str(hit.get("file_name") or "").strip()
            meta: dict[str, Any] = {
                "source": "langchain_graph_qa",
                "result_kind": hit.get("kind", ""),
            }
            if file_name:
                meta["file_name"] = file_name
                meta["source_files"] = [file_name]
            nodes.append(
                NodeWithScore(
                    node=TextNode(text=text, metadata=meta),
                    score=float(hit.get("score") or 0.5),
                )
            )
        return nodes

    class _Retriever(BaseRetriever):
        def __init__(self) -> None:
            super().__init__()
            self._chain = chain
            self._top_k = top_k

        def _retrieve(self, query_bundle: QueryBundle) -> list[Any]:
            hits = self._chain.build_result_nodes(query_bundle.query_str, self._top_k)
            return _hits_to_nodes(hits)

        async def _aretrieve(self, query_bundle: QueryBundle) -> list[Any]:
            import asyncio  # noqa: PLC0415

            hits = await asyncio.to_thread(
                self._chain.build_result_nodes,
                query_bundle.query_str,
                self._top_k,
            )
            return _hits_to_nodes(hits)

    return _Retriever()


def build_cocoindex_surreal_qa_retriever(
    system: Any, config: Any, top_k: int = 5
) -> Optional[Any]:
    """Build a LI retriever for the CocoIndex native SurrealDB schema.

    Returns one result per source chunk with relation paths and chunk text
    combined in each item (matching Neo4j/FalkorDB CocoIndex graph hits).
    """
    try:
        from langchain.graph.retrievers.li_logging_retriever import wrap_with_logging  # noqa: PLC0415
    except ImportError as exc:
        logger.warning("CocoIndex SurrealDB QA: missing LangChain retrievers — %s", exc)
        return None

    _lc_llm = getattr(system, "lc_llm", None)
    if _lc_llm is None:
        try:
            from langchain.llm.llm_factory import get_langchain_llm as _get_lc_llm  # noqa: PLC0415
            _lc_llm = _get_lc_llm(config)
        except Exception as _exc:  # noqa: BLE001
            logger.warning("CocoIndex SurrealDB QA: could not build LangChain LLM — %s", _exc)
    if _lc_llm is None:
        logger.warning("CocoIndex SurrealDB QA: no LangChain LLM available — skipping")
        return None

    try:
        from cocoindex_integration.connectors.cocoindex.property_graph._surreal import (  # noqa: PLC0415
            _build_surreal_config,
        )
        _raw_cfg = config.graph_db_config or {}
        conn_cfg = _build_surreal_config(_raw_cfg)
    except Exception as exc:  # noqa: BLE001
        logger.warning("CocoIndex SurrealDB QA: could not build connection config — %s", exc)
        return None

    try:
        from cocoindex_integration.surreal_chain import _CocoSurrealDBChain  # noqa: PLC0415
        chain = _CocoSurrealDBChain(
            conn_cfg=conn_cfg,
            llm=_lc_llm,
            include_intermediate=False,
        )
        li_retriever = _make_coco_surreal_graph_retriever(chain, top_k=top_k)
        tagged = wrap_with_logging(li_retriever, label="cocoindex_surreal_qa")
        logger.info(
            "CocoIndex native SurrealDB QA retriever created "
            "(db=%s, ns=%s)",
            conn_cfg.get("database", "?"),
            conn_cfg.get("namespace", "?"),
        )
        return tagged
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not create CocoIndex SurrealDB QA retriever: %s", exc)
        return None


def build_cocoindex_read_graph_retriever(
    system: Any, config: Any, top_k: int = 5
) -> Optional[Any]:
    """Build a read-only LlamaIndex retriever over a CocoIndex-written property graph.

    Reuses the same ``DatabaseFactory.create_graph_store`` call the LlamaIndex
    backend uses, then wraps it in a read-only ``PropertyGraphIndex.from_existing``
    and returns ``.as_retriever(...)``.  Returns ``None`` if no LI store can be
    built (caller continues without the graph modality).  No nodes are written —
    CocoIndex owns ingestion.
    """
    try:
        from llama_index.core import PropertyGraphIndex  # noqa: PLC0415

        graph_store = _build_coco_read_graph_store(config)
        if graph_store is None:
            logger.warning(
                "CocoIndex read-graph: no LlamaIndex property graph store for db=%s",
                config.pg_graph_db,
            )
            return None
        _patch_li_neo4j_vector_index(graph_store, config)
        _ro_index = PropertyGraphIndex.from_existing(
            property_graph_store=graph_store,
            embed_model=system.embed_model,
        )
        retriever = _ro_index.as_retriever(
            include_text=True,
            similarity_top_k=top_k,
            include_metadata=True,
        )
        retriever = _CocoGraphChunkTextRetriever(retriever, graph_store)
        logger.info(
            "Graph retriever created from CocoIndex-owned property graph "
            "(read-only, GRAPH_BACKEND=cocoindex, db=%s)",
            config.pg_graph_db,
        )
        return retriever
    except Exception as _err:  # noqa: BLE001
        logger.warning("Could not create CocoIndex read-only graph retriever: %s", _err)
        return None
