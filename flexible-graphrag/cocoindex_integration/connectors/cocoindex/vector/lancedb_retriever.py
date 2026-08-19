"""Custom LlamaIndex vector retriever for the CocoIndex-native LanceDB schema.

When ``VECTOR_BACKEND=cocoindex`` and ``VECTOR_DB=lancedb``, CocoIndex writes
embeddings to a LanceDB table using its own schema (``point_id``, ``embedding``,
flat metadata columns).  LlamaIndex's ``LanceDBVectorStore.query()`` expects
``id``, ``vector`` (or configured name), and a ``metadata`` dict column — none
of which match the CocoIndex layout.

This retriever bypasses ``LanceDBVectorStore`` entirely and queries the table
directly using the LanceDB Python API, then maps the results to
``NodeWithScore`` objects that LlamaIndex's fusion retriever can consume.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, List, Optional

from llama_index.core.base.base_retriever import BaseRetriever
from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode
from llama_index.core.callbacks import CallbackManager

logger = logging.getLogger(__name__)


class CocoIndexLanceDBVectorRetriever(BaseRetriever):
    """LlamaIndex retriever that reads from a CocoIndex-native LanceDB table.

    Parameters
    ----------
    uri : str
        LanceDB data directory URI (e.g. ``"./lancedb"``).
    table_name : str
        LanceDB table name (e.g. ``"hybrid_search"``).
    embed_model : Any
        LlamaIndex embedding model used to embed the query string.
    similarity_top_k : int
        Number of results to return.
    """

    def __init__(
        self,
        uri: str,
        table_name: str,
        embed_model: Any,
        similarity_top_k: int = 10,
        callback_manager: Optional[CallbackManager] = None,
    ) -> None:
        self._uri = uri
        self._table_name = table_name
        self._embed_model = embed_model
        self._top_k = similarity_top_k
        super().__init__(callback_manager=callback_manager)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _open_table(self):
        """Open and return the LanceDB table (sync)."""
        import lancedb  # noqa: PLC0415
        db = lancedb.connect(self._uri)
        if self._table_name not in db.table_names():
            raise RuntimeError(
                f"LanceDB table '{self._table_name}' does not exist yet. "
                "Ingest at least one document before searching."
            )
        return db.open_table(self._table_name)

    def _detect_vector_col(self, tbl) -> str:
        """Return the name of the vector/embedding column in *tbl*."""
        field_names = {f.name for f in tbl.schema}
        for candidate in ("embedding", "vector"):
            if candidate in field_names:
                return candidate
        raise RuntimeError(
            f"Could not find a vector column in LanceDB table '{self._table_name}'. "
            f"Available columns: {sorted(field_names)}"
        )

    def _embed_query(self, query_str: str) -> List[float]:
        """Return the dense embedding for *query_str* (sync)."""
        return self._embed_model.get_query_embedding(query_str)

    def _search(self, query_str: str) -> List[NodeWithScore]:
        """Run vector search against the CocoIndex-native LanceDB table."""
        import numpy as np  # noqa: PLC0415

        try:
            tbl = self._open_table()
        except RuntimeError as _e:
            # Table not yet created — no documents ingested yet via CocoIndex.
            logger.debug("CocoIndex LanceDB retriever: %s — returning empty results", _e)
            return []

        vec_col = self._detect_vector_col(tbl)
        query_vec = self._embed_query(query_str)

        results = (
            tbl.search(query=query_vec, vector_column_name=vec_col)
            .limit(self._top_k)
            .to_pandas()
        )

        if results.empty:
            return []

        # Convert L2/cosine distance to similarity (lower distance → higher score).
        if "_distance" in results.columns:
            similarities = np.exp(-results["_distance"].values).tolist()
        elif "score" in results.columns:
            raw = results["score"].values
            mx = float(raw.max()) if len(raw) > 0 else 1.0
            similarities = np.exp(raw - mx).tolist()
        else:
            similarities = np.linspace(1.0, 0.1, len(results)).tolist()

        col_set = set(results.columns)

        def _get(row, col, default=None):
            """Safely get a column value from a pandas Series row."""
            return row[col] if col in col_set else default

        nodes: List[NodeWithScore] = []
        for (_, row), sim in zip(results.iterrows(), similarities):
            # Build metadata dict from known CocoIndex columns
            meta: dict = {}
            for col in (
                "file_name", "file_path", "file_type", "source_type",
                "modified_at", "chunk_index", "total_chunks",
                "start_char_idx", "end_char_idx",
            ):
                val = _get(row, col)
                if val is not None:
                    meta[col] = val

            # Use point_id if available, fall back to doc_id+chunk_index
            point_id = str(_get(row, "point_id") or "")
            doc_id = str(_get(row, "doc_id") or _get(row, "ref_doc_id") or "")
            chunk_idx = int(_get(row, "chunk_index", 0) or 0)
            node_id = point_id or f"{doc_id}:{chunk_idx}"

            text = str(_get(row, "text") or "")

            node = TextNode(
                text=text,
                id_=node_id,
                metadata=meta,
            )
            nodes.append(NodeWithScore(node=node, score=float(sim)))

        return nodes

    # ------------------------------------------------------------------
    # LlamaIndex retriever interface
    # ------------------------------------------------------------------

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        return self._search(query_bundle.query_str)

    async def _aretrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        return await asyncio.to_thread(self._search, query_bundle.query_str)


__all__ = ["CocoIndexLanceDBVectorRetriever"]
