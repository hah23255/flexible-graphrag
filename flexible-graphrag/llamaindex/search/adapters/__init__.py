"""llamaindex.search.adapters — per-backend LlamaIndex search adapters.

Each adapter module is loaded lazily by :func:`create_search_store`
based on the configured ``SEARCH_DB`` value.  Nothing is imported here
at package load time so that optional backend libraries are only imported
when the selected adapter is actually instantiated.
"""
from .factory import create_search_store

__all__ = ["create_search_store"]
