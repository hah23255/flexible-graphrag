"""Custom LlamaIndex retriever that queries a CocoIndex-written pgvector table.

CocoIndex writes vectors to Postgres with its own schema:
  point_id (text PK), doc_id, chunk_index, text, file_name, file_path,
  file_type, source_type, modified_at, ref_doc_id, start_char_idx,
  end_char_idx, total_chunks, metadata_json, embedding vector(N)

LlamaIndex's PGVectorStore writes to a different table (``data_<name>``) with
its own schema.  This retriever bypasses PGVectorStore entirely and queries the
CocoIndex-written table directly via asyncpg.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from llama_index.core.base.base_retriever import BaseRetriever
from llama_index.core.callbacks import CallbackManager
from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

logger = logging.getLogger(__name__)


def _encode_vector_str(embedding: List[float]) -> str:
    """Convert a Python float list to pgvector text format ``'[1.0,2.0,...]'``."""
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"


def _get(row: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return default


class CocoIndexPostgresVectorRetriever(BaseRetriever):
    """Retriever that reads CocoIndex-written pgvector rows directly via asyncpg.

    Parameters
    ----------
    db_config:
        Dict with connection info: host, port, database, username, password.
    table_name:
        The table CocoIndex writes to (e.g. ``hybrid_search_vectors``).
    pg_schema:
        Postgres schema (default ``public``).
    embed_model:
        LlamaIndex embedding model used to embed the query string.
    similarity_top_k:
        Number of nearest neighbours to retrieve.
    distance_metric:
        ``cosine`` (default), ``l2``, or ``ip``.
    """

    def __init__(
        self,
        db_config: Dict[str, Any],
        table_name: str,
        pg_schema: str = "public",
        embed_model: Any = None,
        similarity_top_k: int = 10,
        distance_metric: str = "cosine",
        callback_manager: Optional[CallbackManager] = None,
    ) -> None:
        self._db_config = db_config
        self._table_name = table_name
        self._pg_schema = pg_schema or "public"
        self._embed_model = embed_model
        self._similarity_top_k = similarity_top_k
        self._distance_metric = distance_metric
        super().__init__(callback_manager=callback_manager)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _distance_op(self) -> str:
        ops = {"cosine": "<=>", "l2": "<->", "ip": "<#>"}
        return ops.get(self._distance_metric, "<=>")

    def _similarity_expr(self) -> str:
        op = self._distance_op()
        if op == "<#>":
            # inner product: higher = more similar; negate distance
            return f"(embedding {op} $1::vector) * -1"
        return f"1 - (embedding {op} $1::vector)"

    async def _embed_query(self, query_str: str) -> List[float]:
        if self._embed_model is None:
            raise RuntimeError("CocoIndexPostgresVectorRetriever: embed_model is None")
        result = await asyncio.to_thread(
            self._embed_model.get_text_embedding, query_str
        )
        return result

    async def _connect(self) -> Any:
        """Open a fresh asyncpg connection."""
        import asyncpg  # noqa: PLC0415

        cfg = self._db_config
        conn = await asyncpg.connect(
            host=cfg.get("host", "localhost"),
            port=int(cfg.get("port", 5432)),
            database=cfg.get("database", "postgres"),
            user=cfg.get("username", cfg.get("user", "postgres")),
            password=cfg.get("password"),
        )
        return conn

    async def _search(self, query_str: str) -> List[NodeWithScore]:
        """Embed query and run pgvector similarity search."""
        embedding = await self._embed_query(query_str)
        emb_str = _encode_vector_str(embedding)

        fqt = f'"{self._pg_schema}"."{self._table_name}"'
        sim_expr = self._similarity_expr()
        sql = f"""
            SELECT
                point_id, doc_id, chunk_index, text,
                file_name, file_path, file_type, source_type,
                modified_at, ref_doc_id,
                start_char_idx, end_char_idx, total_chunks,
                {sim_expr} AS similarity
            FROM {fqt}
            ORDER BY embedding {self._distance_op()} $1::vector
            LIMIT $2
        """

        conn = None
        try:
            conn = await self._connect()
            rows = await conn.fetch(sql, emb_str, self._similarity_top_k)
        except Exception as exc:  # noqa: BLE001
            err_lower = str(exc).lower()
            if any(k in err_lower for k in ("does not exist", "not found", "no such table")):
                logger.debug(
                    "CocoIndexPostgresVectorRetriever: table '%s' not found yet — "
                    "returning empty results (%s)",
                    self._table_name, exc,
                )
            else:
                logger.warning(
                    "CocoIndexPostgresVectorRetriever: query error: %s", exc
                )
            return []
        finally:
            if conn is not None:
                await conn.close()

        nodes: List[NodeWithScore] = []
        for row in rows:
            r = dict(row)
            text = str(r.get("text") or "")
            meta: Dict[str, Any] = {}
            for col in (
                "file_name", "file_path", "file_type", "source_type",
                "modified_at", "chunk_index", "total_chunks",
                "start_char_idx", "end_char_idx",
            ):
                val = r.get(col)
                if val is not None:
                    meta[col] = val

            point_id = str(r.get("point_id") or "")
            doc_id = str(r.get("doc_id") or r.get("ref_doc_id") or "")
            chunk_idx = int(r.get("chunk_index") or 0)
            node_id = point_id or f"{doc_id}:{chunk_idx}"

            similarity = float(r.get("similarity") or 0.0)

            node = TextNode(text=text, id_=node_id, metadata=meta)
            nodes.append(NodeWithScore(node=node, score=similarity))

        logger.debug(
            "CocoIndexPostgresVectorRetriever: query='%s' → %d result(s)",
            query_str[:60], len(nodes),
        )
        return nodes

    # ------------------------------------------------------------------
    # BaseRetriever interface
    # ------------------------------------------------------------------

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        return asyncio.get_event_loop().run_until_complete(
            self._search(query_bundle.query_str)
        )

    async def _aretrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        return await self._search(query_bundle.query_str)


__all__ = ["CocoIndexPostgresVectorRetriever"]
