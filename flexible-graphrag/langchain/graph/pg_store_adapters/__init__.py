"""langchain.graph.pg_store_adapters — per-backend LC property-graph adapters.

Each adapter module defines a single adapter class with:

    ``get_graph()``           -> raw LangChain graph object
    ``create_qa_chain(llm)``  -> QA chain for text-to-query retrieval

Adapters available
------------------
neo4j_adapter          Neo4jAdapter
arangodb_adapter       ArangoDBAdapter
neptune_pg_adapter     NeptunePropertyGraphAdapter, NeptuneAnalyticsAdapter
apache_age_adapter     ApacheAGEAdapter  (guarded — antlr4 broken on Py 3.14)
cosmos_gremlin_adapter CosmosDBGremlinAdapter
spanner_adapter        SpannerGraphAdapter
surrealdb_adapter      SurrealDBAdapter
memgraph_adapter       MemgraphAdapter
falkordb_adapter       FalkorDBAdapter
arcadedb_lc_adapter    ArcadeDBLangChainAdapter
nebula_adapter         NebulaGraphAdapter
hugegraph_adapter      HugeGraphAdapter
tigergraph_adapter     TigerGraphAdapter
ladybug_adapter        LangChainLadybugAdapter

Each adapter module is loaded lazily by :func:`create_property_graph_adapter`
based on the configured ``PG_GRAPH_DB`` value.  Nothing is imported here at
package load time so that optional backend libraries are only imported when the
selected adapter is actually instantiated.
"""
from .factory import _ADAPTER_REGISTRY, create_property_graph_adapter, _build_vector_index_config

__all__ = [
    "_ADAPTER_REGISTRY",
    "create_property_graph_adapter",
    "_build_vector_index_config",
]
