"""langchain.vector — LangChain vector store implementations.

Structure
---------
lc_vector_retriever     LCVectorRetriever (Layer 0, pure LC)
li_vector_retriever     LangChainVectorStoreRetriever (Layer 1, LI wrapper)
vector_store_adapter    LangChainVectorAdapter (base)
retriever               LangChainVectorRetriever (LlamaIndex BaseRetriever)
adapters/
  factory               build_lc_vector_store (lazy-selects per-backend adapter)
  qdrant_adapter        QdrantVectorAdapter     ─┐
  neo4j_adapter         Neo4jVectorAdapter       │  loaded lazily by factory
  elasticsearch_adapter ElasticsearchVectorAdapter│
  opensearch_adapter    OpenSearchVectorAdapter   │
  chroma_adapter        ChromaVectorAdapter       │
  milvus_adapter        MilvusVectorAdapter       │
  weaviate_adapter      WeaviateVectorAdapter     │
  pinecone_adapter      PineconeVectorAdapter     │
  postgres_adapter      PostgresVectorAdapter     │
  lancedb_adapter       LanceDBVectorAdapter     ─┘

Per-backend adapter classes are NOT re-exported here.  Import them directly
from their modules or let :func:`build_lc_vector_store` select the right one.
"""
from .lc_vector_retriever import LCVectorRetriever
from .li_vector_retriever import LangChainVectorStoreRetriever
from .vector_store_adapter import LangChainVectorAdapter
from .retriever import LangChainVectorRetriever
from .adapters.factory import build_lc_vector_store
from adapters.vector.vector_store_adapter import VectorStoreAdapter

__all__ = [
    "VectorStoreAdapter",
    "LangChainVectorAdapter",
    "LCVectorRetriever",
    "LangChainVectorStoreRetriever",
    "LangChainVectorRetriever",
    "build_lc_vector_store",
]
