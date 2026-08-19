"""langchain.vector.adapters — per-backend LangChain vector store adapters.

Each adapter module is loaded lazily by :func:`build_lc_vector_store`
based on the configured ``VECTOR_DB`` value.  Nothing is imported here
at package load time so that optional backend libraries (qdrant-client,
langchain-milvus, etc.) are only imported when the selected adapter is
actually instantiated.
"""
from .factory import build_lc_vector_store

__all__ = ["build_lc_vector_store"]
