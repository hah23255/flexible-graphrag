"""langchain.vector.vector_store_adapter — LangChain vector store base adapter.

ABC lives in :mod:`adapters.vector.vector_store_adapter`.
Per-backend adapters live in :mod:`langchain.vector.adapters`.
LlamaIndex implementation lives in :mod:`llamaindex.vector.vector_store_factory`.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, List

from adapters.vector.vector_store_adapter import VectorStoreAdapter

logger = logging.getLogger(__name__)


class LangChainVectorAdapter(VectorStoreAdapter):
    """Generic wrapper for a LangChain VectorStore (Qdrant, Chroma, ES, etc.).

    Subclasses specialise construction for specific backends
    (see :mod:`langchain.vector.adapters`).  This base class can also be
    used directly when a pre-built LangChain store object is available.
    """

    def __init__(self, store: Any, delete_key: str = "ref_doc_id"):
        self._store = store
        self._delete_key = delete_key

    def get_store(self) -> Any:
        return self._store

    def delete(self, ref_doc_id: str) -> None:
        if self._store is None:
            return
        try:
            if hasattr(self._store, "delete"):
                self._store.delete(filter={self._delete_key: ref_doc_id})
            logger.info("LangChain vector: deleted docs for ref_doc_id=%s", ref_doc_id)
        except Exception as exc:
            logger.warning("LangChain vector delete failed for %s: %s", ref_doc_id, exc)

    def is_langchain(self) -> bool:
        return True

    async def insert_nodes(self, nodes: List[Any]) -> None:
        """Insert pre-embedded LlamaIndex TextNodes (LI pipeline path only).

        Called by the *non*-CocoIndex ingestion pipeline (``ingest/update_vector.py``)
        when ``VECTOR_BACKEND=langchain``.  The CocoIndex pipeline instead uses the
        connector-layer writer
        (``cocoindex_integration.connectors.flexible._vector_writer``) so no LI
        TextNode is created on the LC path.
        """
        from langchain.utils import llamaindex_nodes_to_langchain_docs  # local import — langchain optional dep
        lc_docs = llamaindex_nodes_to_langchain_docs(nodes)
        store = self._store
        store_name = type(store).__name__
        aadd = getattr(store, "aadd_documents", None)
        add = getattr(store, "add_documents", None)
        if aadd is not None:
            await aadd(lc_docs)
            logger.info("insert_nodes: added %d docs to %s via aadd_documents", len(lc_docs), store_name)
        elif add is not None:
            await asyncio.to_thread(add, lc_docs)
            logger.info("insert_nodes: added %d docs to %s via add_documents", len(lc_docs), store_name)
        else:
            logger.warning("insert_nodes: %s has no add_documents — skipped", store_name)

    # ------------------------------------------------------------------
    # Helpers for subclasses that need to auto-create their backing store
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_vector_size(vector_size: int, embedding) -> int:
        """Return a concrete vector dimension.

        Resolution order:
        1. *vector_size* argument (pre-computed by factory from app_config)
        2. ``embedding.embed_query("hello")`` (one API call — last resort)
        3. 1536 (safe default for OpenAI small)
        """
        if vector_size and vector_size > 0:
            return vector_size
        if embedding is not None:
            try:
                return len(embedding.embed_query("hello"))
            except Exception:
                pass
        return 1536


__all__ = ["LangChainVectorAdapter"]
