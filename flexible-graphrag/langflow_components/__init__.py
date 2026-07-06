"""
Langflow Component Wrappers for Flexible GraphRAG

Thin Langflow nodes that delegate to the real flexible-graphrag machinery (the same the
FastAPI backend uses): the ingest/* pipeline, retriever_setup, and the store/adapter layer
(full LangChain + LlamaIndex support), driven by the backend .env.

Ingestion (granular, one shared system threaded along the edges):
    Data Source -> Document Processor -> Text Splitter -> {Vector Store, Knowledge Graph,
    Search Index, RDF Graph}
Query (independent nodes):
    Hybrid Search, AI Query

Available Components:
    - FlexibleDataSourceComponent: Resolve a source; build the shared system (.env + overrides)
    - FlexibleDocProcessorComponent: Parse files into documents (Docling/LlamaParse)
    - FlexibleSplitterComponent: Chunk documents
    - FlexibleVectorStoreComponent: Vector indexing
    - FlexibleGraphStoreComponent: KG extraction + property-graph indexing
    - FlexibleSearchStoreComponent: Fulltext/BM25 indexing
    - FlexibleRDFStoreComponent: RDF graph
    - FlexibleHybridSearchComponent: Hybrid retrieval (independent)
    - FlexibleAIQueryComponent: LLM Q&A (independent)
"""

from .flexible_graphrag import (
    FlexibleDataSourceComponent,
    FlexibleDocProcessorComponent,
    FlexibleSplitterComponent,
    FlexibleVectorStoreComponent,
    FlexibleKGExtractionComponent,
    FlexibleGraphStoreComponent,
    FlexibleSearchStoreComponent,
    FlexibleRDFStoreComponent,
    FlexibleIngestSummaryComponent,
    FlexibleHybridSearchComponent,
    FlexibleAIQueryComponent,
    FlexibleQuerySummaryComponent,
)

__all__ = [
    "FlexibleDataSourceComponent",
    "FlexibleDocProcessorComponent",
    "FlexibleSplitterComponent",
    "FlexibleVectorStoreComponent",
    "FlexibleKGExtractionComponent",
    "FlexibleGraphStoreComponent",
    "FlexibleSearchStoreComponent",
    "FlexibleRDFStoreComponent",
    "FlexibleIngestSummaryComponent",
    "FlexibleHybridSearchComponent",
    "FlexibleAIQueryComponent",
    "FlexibleQuerySummaryComponent",
]
