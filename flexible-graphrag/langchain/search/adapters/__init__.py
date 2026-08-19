"""langchain.search.adapters — per-backend LangChain search adapters.

Each adapter module is loaded lazily by :func:`build_langchain_search_store`
based on the configured ``SEARCH_DB`` value.  Nothing is imported here
at package load time so that optional backend libraries (elasticsearch,
opensearch-py, rank_bm25, etc.) are only imported when the selected adapter
is actually instantiated.
"""
from .factory import build_langchain_search_store

__all__ = ["build_langchain_search_store"]
