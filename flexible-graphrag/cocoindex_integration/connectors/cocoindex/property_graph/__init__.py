"""CocoIndex-native property-graph connectors (one module per store).

Selection is table-driven via ``COCO_PG_REGISTRY`` (name → builder).

Implemented (CocoIndex native + flexible-graphrag LI/LC adapters):
    neo4j (``neo4j.py``), falkordb (``falkordb.py``), surrealdb (``surrealdb.py``)

The remaining 12 property-graph stores (arcadedb, arangodb, apache_age, hugegraph,
memgraph, nebula, tigergraph, cosmos_gremlin, spanner, neptune, neptune_analytics,
ladybug) are flexible-graphrag LI/LC only.  ``app.py`` falls back to
``FlexiblePropertyGraph`` for any store not in ``COCO_PG_REGISTRY``; set
``GRAPH_BACKEND=llamaindex`` or ``langchain`` to choose the adapter.

The direct-Cypher relation / MENTIONS writers (``write_relations_sync`` /
``ensure_node_stubs_sync``) live in ``_cypher.py`` and are re-exported here for
``app.py``; they are Cypher-generic so a future FalkorDB connector can reuse them.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from cocoindex_integration.connectors.cocoindex.base import CocoPropertyGraph
from cocoindex_integration.connectors.cocoindex.property_graph.neo4j import (
    CocoNeo4j,
    build_neo4j,
)
from cocoindex_integration.connectors.cocoindex.property_graph.falkordb import (
    CocoFalkorDB,
    build_falkordb,
)
from cocoindex_integration.connectors.cocoindex.property_graph.surrealdb import (
    CocoSurrealDB,
    build_surrealdb,
)
from cocoindex_integration.connectors.cocoindex.property_graph._cypher import (
    write_relations_sync,
    ensure_node_stubs_sync,
    set_entity_type_labels_sync,
    set_entity_properties_sync,
)

logger = logging.getLogger(__name__)

#: name → builder(db_cfg) -> Optional[CocoPropertyGraph].  ``None`` = recognised
#: name without a native v1.x implementation.
COCO_PG_REGISTRY: Dict[str, Optional[Callable[[Dict[str, Any]], Optional[CocoPropertyGraph]]]] = {
    "neo4j": build_neo4j,
    "falkordb": build_falkordb,
    "surrealdb": build_surrealdb,
}

#: Property-graph stores that have a native CocoIndex target connector (names).
COCO_PG_TARGETS = frozenset(COCO_PG_REGISTRY)


def coco_pg_target(pg_graph_db: str, db_cfg: Dict[str, Any]) -> Optional[Any]:
    """Return a CocoPropertyGraph connector (or None) for *pg_graph_db*.

    3 stores have a native CocoIndex connector (neo4j, falkordb, surrealdb).
    All other stores (arcadedb, arangodb, apache_age, hugegraph, memgraph,
    nebula, tigergraph, spanner, neptune, neptune_analytics, ladybug, …) are
    flexible-graphrag LI/LC only — the caller falls back to
    ``FlexiblePropertyGraph`` and ``GRAPH_BACKEND`` controls which adapter is
    used.  *db_cfg* is the parsed JSON config dict.
    """
    builder = COCO_PG_REGISTRY.get(pg_graph_db.lower())
    if builder is None:
        logger.warning(
            "[coco] %s: flexible-graphrag LI/LC only — "
            "set GRAPH_BACKEND=llamaindex or langchain for this store", pg_graph_db,
        )
        return None
    return builder(db_cfg)


__all__ = [
    "CocoNeo4j",
    "CocoFalkorDB",
    "CocoSurrealDB",
    "build_neo4j",
    "build_falkordb",
    "build_surrealdb",
    "write_relations_sync",
    "ensure_node_stubs_sync",
    "set_entity_type_labels_sync",
    "set_entity_properties_sync",
    "COCO_PG_REGISTRY",
    "COCO_PG_TARGETS",
    "coco_pg_target",
]
