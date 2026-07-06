"""
Flexible GraphRAG Components for Langflow

Thin nodes over the real flexible-graphrag machinery (ingest/* pipeline + retriever_setup
+ adapter/store layer, both LangChain and LlamaIndex). Ingestion threads a shared system
along the edges; query nodes are independent.
"""

from .data_source import FlexibleDataSourceComponent
from .document_processor import FlexibleDocProcessorComponent
from .text_splitter import FlexibleSplitterComponent
from .vector_store import FlexibleVectorStoreComponent
from .kg_extraction import FlexibleKGExtractionComponent
from .graph_store import FlexibleGraphStoreComponent
from .search_store import FlexibleSearchStoreComponent
from .rdf_store import FlexibleRDFStoreComponent
from .ingest_summary import FlexibleIngestSummaryComponent
from .hybrid_search import FlexibleHybridSearchComponent
from .ai_query import FlexibleAIQueryComponent
from .query_summary import FlexibleQuerySummaryComponent

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
