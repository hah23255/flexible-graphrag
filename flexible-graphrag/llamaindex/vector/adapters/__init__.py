"""llamaindex.vector.adapters — per-backend LlamaIndex vector store adapters.

Each adapter module is loaded lazily by :func:`create_vector_store`
based on the configured ``VECTOR_DB`` value.  Nothing is imported here
at package load time so that optional backend libraries are only imported
when the selected adapter is actually instantiated.
"""
from .factory import create_vector_store

__all__ = ["create_vector_store"]
