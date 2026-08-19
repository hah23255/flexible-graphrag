"""langchain.search — LangChain search store implementations (ES, OpenSearch, BM25).

Structure
---------
lc_search_retriever     LCSearchRetriever (Layer 0, pure LC)
li_search_retriever     LangChainRetrieverWrapper (Layer 1, LI wrapper)
search_store_adapter    LangChainSearchAdapter (base)
retriever               LangChainSearchRetriever (LlamaIndex BaseRetriever)
adapters/
  factory               build_langchain_search_store (lazy-selects per-backend adapter)
  bm25_adapter          BM25SearchAdapter           ─┐ loaded lazily by factory
  elasticsearch_adapter ElasticsearchSearchAdapter   │
  opensearch_adapter    OpenSearchSearchAdapter      ─┘

Per-backend adapter classes are NOT re-exported here.  Import them directly
from their modules or let :func:`build_langchain_search_store` select the right one.
"""
from .lc_search_retriever import LCSearchRetriever
from .li_search_retriever import LangChainRetrieverWrapper
from .search_store_adapter import LangChainSearchAdapter
from .retriever import LangChainSearchRetriever
from .adapters.factory import build_langchain_search_store
from adapters.search.search_store_adapter import SearchStoreAdapter

__all__ = [
    "SearchStoreAdapter",
    "LangChainSearchAdapter",
    "LCSearchRetriever",
    "LangChainRetrieverWrapper",
    "LangChainSearchRetriever",
    "build_langchain_search_store",
]
