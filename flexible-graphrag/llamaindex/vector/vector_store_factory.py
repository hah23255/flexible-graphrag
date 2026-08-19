"""llamaindex.vector.vector_store_factory — LlamaIndex vector store factory.

Provides :func:`create_vector_store` (pure LlamaIndex, all VectorDBType values).
``factories.py`` delegates to this module so all existing call-sites continue to work.

Per-backend creation logic lives in :mod:`llamaindex.vector.adapters`.
The ABC lives in :mod:`adapters.vector.vector_store_adapter`.
The LangChain implementation lives in :mod:`langchain.vector.vector_store_adapter`.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional
import logging

from config import VectorDBType, LLMProvider
from llamaindex.llm.embedding_factory import get_embedding_dimension
from adapters.vector.vector_store_adapter import VectorStoreAdapter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# LlamaIndex base adapter (subclassed by each per-backend adapter)
# ---------------------------------------------------------------------------

class LlamaIndexVectorAdapter(VectorStoreAdapter):
    """Wraps a LlamaIndex VectorStore.

    Per-backend subclasses live in :mod:`llamaindex.vector.adapters`.
    Each subclass builds its own store in ``__init__`` and calls
    ``super().__init__(store)``.

    ``__getattr__`` delegation makes this a transparent proxy — LlamaIndex APIs
    that access store-specific attributes (e.g. ``add``, ``delete``,
    ``query``) work without explicitly unwrapping the adapter.
    """

    def __init__(self, store):
        self._store = store

    def get_store(self):
        return self._store

    def delete(self, ref_doc_id: str) -> None:
        if self._store is None:
            return
        try:
            self._store.delete(ref_doc_id)
            logger.info(f"Deleted vector docs for ref_doc_id={ref_doc_id}")
        except Exception as exc:
            logger.warning(f"Vector delete failed for {ref_doc_id}: {exc}")

    def is_langchain(self) -> bool:
        return False

    async def insert_nodes(self, nodes: List[Any]) -> None:
        """Insert pre-embedded LlamaIndex TextNodes into the vector store.

        Mirrors the dispatch logic in ``ingest/update_vector.py::update_vector()``:
        - ElasticsearchStore / WeaviateVectorStore: use ``async_add`` directly
          (they use aiohttp internally; calling from a thread would bind the session
          to the wrong event loop).
        - Everything else: ``store.add(nodes)`` in a thread-pool executor.

        The nodes must already have ``node.embedding`` set and their SOURCE
        relationship populated so ``node.ref_doc_id`` resolves to the stable
        document ID used for deletion.
        """
        store = self._store
        store_name = type(store).__name__
        if store_name in ("ElasticsearchStore", "WeaviateVectorStore") and hasattr(store, "async_add"):
            if store_name == "WeaviateVectorStore":
                # Only auto-connect a real async client.  Sync-backed stores
                # (LlamaIndexWeaviateAdapter) patch async_add → to_thread(add)
                # and must not have connect() awaited on the sync client.
                aclient = getattr(store, "_aclient", None)
                if aclient is not None:
                    try:
                        connected = bool(getattr(aclient, "is_connected", lambda: False)())
                    except Exception:
                        connected = False
                    if not connected:
                        await aclient.connect()
            node_ids = await store.async_add(nodes)
            logger.info("insert_nodes: added %d nodes to %s via async_add", len(node_ids), store_name)
        else:
            await asyncio.to_thread(store.add, nodes)
            logger.info("insert_nodes: added %d nodes to %s", len(nodes), store_name)

    def __getattr__(self, name: str):
        """Delegate unknown attribute access to the wrapped LlamaIndex store."""
        store = self.__dict__.get("_store")
        if store is None:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        return getattr(store, name)


# ---------------------------------------------------------------------------
# Factory — resolves embed_dim then delegates to adapters/factory.py
# ---------------------------------------------------------------------------

def create_vector_store(
    db_type: VectorDBType,
    config: Dict[str, Any],
    llm_provider: LLMProvider = None,
    llm_config: Dict[str, Any] = None,
    app_config=None,
) -> LlamaIndexVectorAdapter:
    """Create and return a :class:`LlamaIndexVectorAdapter` subclass for *db_type*."""
    from llamaindex.vector.adapters.factory import create_vector_store as _create

    embedding_kind = getattr(app_config, "embedding_kind", None) if app_config else None
    embedding_model = getattr(app_config, "embedding_model", None) if app_config else None
    embedding_dimension = getattr(app_config, "embedding_dimension", None) if app_config else None
    embed_dim = get_embedding_dimension(
        embedding_kind=embedding_kind,
        embedding_model=embedding_model,
        embedding_dimension=embedding_dimension,
    )
    logger.info(f"Detected embedding dimension: {embed_dim} (kind: {embedding_kind}, model: {embedding_model})")
    return _create(db_type, config, embed_dim)


def build_vector_adapter(
    db_type: VectorDBType,
    config: Dict[str, Any],
    llm_provider: LLMProvider = None,
    llm_config: Dict[str, Any] = None,
    app_config=None,
) -> LlamaIndexVectorAdapter:
    """Convenience alias for :func:`create_vector_store`."""
    return create_vector_store(db_type, config, llm_provider, llm_config, app_config)

