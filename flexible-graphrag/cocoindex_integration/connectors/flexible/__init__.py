"""Flexible connector family — CocoIndex targets/source that wrap flexible-graphrag.

These connectors reuse flexible-graphrag's own LlamaIndex / LangChain store
adapters, so every ``VECTOR_DB`` / ``PG_GRAPH_DB`` / ``RDF_GRAPH_DB`` /
``SEARCH_DB`` value already configured in ``.env`` works with no extra wiring.

Targets (all built on ``FlexibleConnector`` + the generic ``FlexibleReconcileHandler``):
    ``FlexibleVector``          — 10 vector stores
    ``FlexiblePropertyGraph``   — 15 property graph stores
    ``FlexibleSearch``          — Elasticsearch / OpenSearch / BM25
    ``FlexibleRDFGraph``        — Fuseki / GraphDB / Oxigraph / Neptune RDF

Source (standalone — reads, not a ``FlexibleConnector`` target):
    ``FlexibleDataSource``      — all 14 flexible-graphrag data sources
"""
