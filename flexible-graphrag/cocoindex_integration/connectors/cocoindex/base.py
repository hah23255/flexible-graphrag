"""
Base classes for the CocoIndex-native connector family.

The hierarchy is intentionally thin — it only captures what is genuinely shared
across native connectors, not a deep framework:

    CocoConnector              root — capability flags (can_read / can_write) + kind
    ├─ CocoVector              vector-store targets (Qdrant, LanceDB, pgvector, …)
    ├─ CocoPropertyGraph       property-graph targets (Neo4j, FalkorDB, …)
    ├─ CocoSource              source connectors (LocalFileSystem, AmazonS3, …)
    │   └─ CocoNativePassthrough  generic source for any CocoIndex connector that
    │                              does not need a dedicated flexible-graphrag module

``CocoConnector`` is unified with the *flexible* family (``FlexibleConnector``)
**by convention only** — both expose the same lifecycle method *names* and write
the same ``connectors.rows`` / ``connectors.cocoindex.rows`` data types — NOT via
a shared cross-family base class or Protocol.  The two families write through
fundamentally different mechanisms (CocoIndex ``declare_record`` reconciliation
vs. direct LlamaIndex / LangChain adapter calls), so a common ABC would leak.

Capability flags
----------------
``can_read`` / ``can_write`` model dual-role stores (e.g. Postgres, local
filesystem can be both a source and a target).  Target-only stores set
``can_read=False``; source-only connectors set ``can_write=False``.  Store-to-
store export can later be built on top of these flags.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict

logger = logging.getLogger(__name__)


class CocoConnector:
    """Root base for every CocoIndex-native connector.

    Subclasses are lightweight *descriptors*: they hold the ``ContextKey`` /
    config needed for CocoIndex ``mount_*_target`` / ``declare_*`` calls, which
    are issued from ``app.py``'s pipeline.  The base only standardises the
    capability flags and a human-readable identity.
    """

    #: Connector kind — "vector" | "property_graph" | "search" | "source" | …
    kind: str = "connector"
    #: Short store name — "qdrant" | "neo4j" | …
    name: str = ""
    #: True when this connector can be read from (source role).
    can_read: bool = False
    #: True when this connector can be written to (target role).
    can_write: bool = True

    def describe(self) -> str:
        roles = []
        if self.can_read:
            roles.append("read")
        if self.can_write:
            roles.append("write")
        return f"Coco {self.kind} '{self.name}' ({'/'.join(roles) or 'none'})"


class CocoVector(CocoConnector):
    """Kind-base for CocoIndex-native vector-store targets."""
    kind = "vector"
    can_read = False
    can_write = True


class CocoPropertyGraph(CocoConnector):
    """Kind-base for CocoIndex-native property-graph targets."""
    kind = "property_graph"
    can_read = False
    can_write = True


class CocoSource(CocoConnector):
    """Kind-base for CocoIndex-native source connectors (readers).

    Sources read documents; most are read-only (``can_write=False``).  Dual-role
    stores that are also valid write targets (e.g. the local filesystem, Postgres)
    override ``can_write=True`` so store-to-store export can be built on the flags.

    ``native_available`` records whether a *native CocoIndex execution path* is
    actually wired for this source in ``app.py``:

    * ``True``  — the pipeline reads via CocoIndex's own source connector
      (e.g. ``localfs.walk_dir`` + ``mount_each``), giving CocoIndex-native
      change detection.
    * ``False`` — only a descriptor exists (scaffold); the pipeline falls back to
      ``FlexibleDataSource`` for the actual read, which already supports the store.
    """
    kind = "source"
    can_read = True
    can_write = False
    #: True when a native CocoIndex read path is wired in app.py (else flexible fallback).
    native_available: bool = False


@dataclass
class CocoNativePassthrough(CocoSource):
    """Generic source descriptor for any CocoIndex-native source without a dedicated module.

    Eliminates the need for a per-source ``.py`` file for CocoIndex connectors
    whose config can be mapped from environment variables without custom logic.
    The descriptor records the CocoIndex module and class name so ``flexible_app.py``
    can import and instantiate the connector dynamically at pipeline run time.

    Examples
    --------
    OCI Object Storage::

        CocoNativePassthrough(
            name="oci_object_storage",
            coco_module="cocoindex.connectors.oci_object_storage",
            coco_class="OciObjectStorageSource",
            config_env_prefix="OCI_",
        )

    Kafka (source role)::

        CocoNativePassthrough(
            name="kafka",
            coco_module="cocoindex.connectors.kafka",
            coco_class="KafkaSource",
            config_env_prefix="KAFKA_",
            can_write=True,   # Kafka is dual-role
        )

    When ``native_available=False`` (CocoIndex module not importable) the pipeline
    falls back to ``FlexibleDataSource`` automatically.
    """
    #: Importable Python module path for the CocoIndex connector.
    #: e.g. "cocoindex.connectors.oci_object_storage"
    coco_module: str = ""
    #: Class name within *coco_module* to instantiate.
    #: e.g. "OciObjectStorageSource"
    coco_class: str = ""
    #: Environment-variable prefix used to populate the source config.
    #: e.g. "OCI_" gathers OCI_NAMESPACE, OCI_BUCKET, etc.
    config_env_prefix: str = ""
    #: Extra static config to merge on top of env-var harvest.
    extra_config: Dict[str, Any] = field(default_factory=dict)
