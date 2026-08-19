"""llamaindex.graph.adapters — per-backend LlamaIndex property graph adapters.

Adapters available
------------------
neo4j_adapter              LlamaIndexNeo4jGraphAdapter
ladybug_adapter            LlamaIndexLadybugAdapter
falkordb_adapter           LlamaIndexFalkorDBAdapter
arcadedb_adapter           LlamaIndexArcadeDBAdapter
memgraph_adapter           LlamaIndexMemgraphAdapter
nebula_adapter             LlamaIndexNebulaAdapter
neptune_adapter            LlamaIndexNeptuneAdapter
neptune_analytics_adapter  LlamaIndexNeptuneAnalyticsAdapter

Each adapter module is loaded lazily by :func:`create_graph_store`
based on the configured ``PG_GRAPH_DB`` value.  Nothing is imported here
at package load time so that optional backend libraries are only imported
when the selected adapter is actually instantiated.
"""
from .factory import create_graph_store

__all__ = ["create_graph_store"]
