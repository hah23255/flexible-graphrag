"""CocoIndex-native connector family.

Connectors that write straight through CocoIndex's own connectors (Qdrant,
Neo4j, …) using the v1.x two-level TargetState architecture.  Unified with the
``flexible`` family *by convention only* (shared method names + shared row types)
— not via a cross-family base class.

Layout
------
* ``base``            — ``CocoConnector`` root + ``CocoVector`` / ``CocoPropertyGraph`` / ``CocoSource``
                        / ``CocoNativePassthrough`` (generic source, no dedicated module needed)
* ``rows``            — CocoIndex-native row schemas (``CocoVectorRow`` etc.)
* ``_runtime``        — ContextKeys, resource lifespan, and import-time connector patches
* ``vector``          — Qdrant/LanceDB/Postgres(pgvector) implemented;
                        turbopuffer/valkey/zvec/doris/sqlite placeholders
* ``property_graph``  — Neo4j/FalkorDB/SurrealDB implemented
* ``sources``         — localfs/S3/Azure/GDrive wired; OCI/Postgres/Kafka/Iggy as generic passthrough
* ``relational``      — Postgres/Doris/SQLite/Snowflake/BigQuery placeholder entries (row source/target)
* ``streams``         — Kafka/Iggy placeholder entries (message-stream source + target)

Importing this package eagerly imports ``_runtime`` so the CocoIndex Qdrant /
Neo4j monkey-patches are installed and the ``@coco.lifespan`` resource provider
is registered.
"""

from __future__ import annotations

# Import _runtime first for its import-time side effects (patches + lifespan).
from cocoindex_integration.connectors.cocoindex import _runtime  # noqa: F401
from cocoindex_integration.connectors.cocoindex.base import (
    CocoConnector,
    CocoNativePassthrough,
    CocoPropertyGraph,
    CocoSource,
    CocoVector,
)
from cocoindex_integration.connectors.cocoindex.rows import (
    CocoKGTripleRow,
    CocoVectorRow,
)
from cocoindex_integration.connectors.cocoindex.vector import (
    COCO_VECTOR_REGISTRY,
    COCO_VECTOR_TARGETS,
    CocoQdrant,
    coco_vector_target,
)
from cocoindex_integration.connectors.cocoindex.property_graph import (
    COCO_PG_REGISTRY,
    COCO_PG_TARGETS,
    CocoNeo4j,
    coco_pg_target,
    ensure_node_stubs_sync,
    write_relations_sync,
)
from cocoindex_integration.connectors.cocoindex.sources import (
    COCO_SOURCE_REGISTRY,
    COCO_SOURCES,
    CocoAmazonS3,
    CocoLocalFileSystem,
    build_native_passthrough,
    coco_source,
)
from cocoindex_integration.connectors.cocoindex.relational import (
    COCO_RELATIONAL_REGISTRY,
    COCO_RELATIONAL_TARGETS,
    CocoRelational,
    coco_relational_target,
)
from cocoindex_integration.connectors.cocoindex.streams import (
    COCO_STREAM_REGISTRY,
    COCO_STREAMS,
    CocoStream,
    coco_stream,
)

__all__ = [
    # Base classes
    "CocoConnector",
    "CocoVector",
    "CocoPropertyGraph",
    "CocoSource",
    "CocoNativePassthrough",
    "CocoRelational",
    "CocoStream",
    # Row schemas
    "CocoVectorRow",
    "CocoKGTripleRow",
    # Vector connectors
    "CocoQdrant",
    "coco_vector_target",
    "COCO_VECTOR_REGISTRY",
    "COCO_VECTOR_TARGETS",
    # Property-graph connectors
    "CocoNeo4j",
    "coco_pg_target",
    "COCO_PG_REGISTRY",
    "COCO_PG_TARGETS",
    "write_relations_sync",
    "ensure_node_stubs_sync",
    # Source connectors
    "CocoLocalFileSystem",
    "CocoAmazonS3",
    "build_native_passthrough",
    "coco_source",
    "COCO_SOURCE_REGISTRY",
    "COCO_SOURCES",
    # Relational/analytical targets
    "coco_relational_target",
    "COCO_RELATIONAL_REGISTRY",
    "COCO_RELATIONAL_TARGETS",
    # Message-stream connectors
    "coco_stream",
    "COCO_STREAM_REGISTRY",
    "COCO_STREAMS",
]
