"""CocoIndex-native source connectors (one module per wired source).

Selection is table-driven via ``COCO_SOURCE_REGISTRY`` (name → builder).  A
builder returns a :class:`CocoSource` descriptor whose ``native_available`` flag
records whether a native CocoIndex read path is available.  The actual dispatch
lives in ``flexible_app.flexible_app_main``, which looks the source up in
``native_apps.NATIVE_READERS`` and otherwise falls back to ``FlexibleMapView`` /
``FlexibleDataSource`` (which together cover all 14 flexible-graphrag sources).

Wired sources (``native_available=True`` when deps + config are present):
    localfs / filesystem  → ``CocoLocalFileSystem``  (dedicated module)
    amazon_s3 / s3        → ``CocoAmazonS3``         (dedicated module)
    azure_blob            → ``CocoAzureBlob``         (dedicated module)
    google_drive          → ``CocoGoogleDrive``       (dedicated module)

Generic passthrough sources (``native_available=True`` when CocoIndex module is
importable — **no dedicated module needed**, uses ``CocoNativePassthrough``):
    oci_object_storage    → OCI Object Storage via ``cocoindex.connectors.oci_object_storage``
    postgres_source       → PostgreSQL table source via ``cocoindex.connectors.postgres``
    kafka                 → Kafka topic source via ``cocoindex.connectors.kafka``
    iggy                  → Apache Iggy topic source via ``cocoindex.connectors.iggy``

Any ``DATA_SOURCE`` value not in this registry, or any wired/passthrough source
whose required deps / config are missing at runtime, falls back to
``FlexibleDataSource`` automatically.

Adding new CocoIndex-native sources
-------------------------------------
For sources that map config from env vars without custom logic, use
``build_native_passthrough()`` instead of writing a new module::

    "my_source": build_native_passthrough(
        name="my_source",
        coco_module="cocoindex.connectors.my_source",
        coco_class="MySourceClass",
        config_env_prefix="MY_SOURCE_",
    ),
"""

from __future__ import annotations

import importlib
import logging
from typing import Any, Callable, Dict, Optional

from cocoindex_integration.connectors.cocoindex.base import CocoNativePassthrough, CocoSource
from cocoindex_integration.connectors.cocoindex.sources.localfs import (
    CocoLocalFileSystem,
    build_localfs,
)
from cocoindex_integration.connectors.cocoindex.sources.amazon_s3 import (
    CocoAmazonS3,
    build_amazon_s3,
)
from cocoindex_integration.connectors.cocoindex.sources.azure_blob import (
    CocoAzureBlob,
    build_azure_blob,
)
from cocoindex_integration.connectors.cocoindex.sources.google_drive import (
    CocoGoogleDrive,
    build_google_drive,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Generic passthrough factory
# ─────────────────────────────────────────────────────────────────────────────

def build_native_passthrough(
    name: str,
    coco_module: str,
    coco_class: str,
    config_env_prefix: str = "",
    can_write: bool = False,
) -> Callable[[Dict[str, Any]], Optional[CocoNativePassthrough]]:
    """Return a builder that creates a ``CocoNativePassthrough`` without a dedicated module.

    This factory avoids the need for a per-source ``.py`` file for CocoIndex
    connectors whose configuration can be harvested from environment variables
    without custom normalisation logic.

    Parameters
    ----------
    name:
        Short store/source name used in log messages (e.g. ``"oci_object_storage"``).
    coco_module:
        Importable Python path for the CocoIndex connector module
        (e.g. ``"cocoindex.connectors.oci_object_storage"``).
    coco_class:
        Class name within *coco_module* to instantiate at pipeline run time
        (e.g. ``"OciObjectStorageSource"``).
    config_env_prefix:
        Environment-variable prefix for config harvest.  E.g. ``"OCI_"`` gathers
        ``OCI_NAMESPACE``, ``OCI_BUCKET``, etc. as ``{"namespace": ..., "bucket": ...}``.
    can_write:
        Set ``True`` for dual-role connectors (e.g. Kafka, Iggy) that can also
        act as write targets.
    """
    def _builder(cfg: Dict[str, Any]) -> Optional[CocoNativePassthrough]:
        src = CocoNativePassthrough(
            name=name,
            can_write=can_write,
            coco_module=coco_module,
            coco_class=coco_class,
            config_env_prefix=config_env_prefix,
        )
        try:
            importlib.import_module(coco_module)
            src.native_available = True
            logger.debug(
                "[coco] %s: native connector available (%s.%s)",
                name, coco_module, coco_class,
            )
        except ImportError:
            src.native_available = False
            logger.warning(
                "[coco] %s: %s not importable — "
                "falling back to FlexibleDataSource('%s')",
                name, coco_module, name,
            )
        return src

    _builder.__name__ = f"build_{name}"
    return _builder


# ─────────────────────────────────────────────────────────────────────────────
# Source registry
# ─────────────────────────────────────────────────────────────────────────────

#: source name (data_source value) → builder(cfg) -> Optional[CocoSource].
#: Aliases map different names to the same builder.
COCO_SOURCE_REGISTRY: Dict[str, Callable[[Dict[str, Any]], Optional[CocoSource]]] = {
    # ── Wired: dedicated adapter modules ─────────────────────────────────────
    "localfs": build_localfs,
    "filesystem": build_localfs,
    "amazon_s3": build_amazon_s3,
    "s3": build_amazon_s3,
    "azure_blob": build_azure_blob,
    "google_drive": build_google_drive,
    # ── Generic passthrough: no dedicated module needed ───────────────────────
    # ``native_available=True`` when the CocoIndex package exposes the module.
    # Falls back to FlexibleDataSource when unavailable.
    "oci_object_storage": build_native_passthrough(
        name="oci_object_storage",
        coco_module="cocoindex.connectors.oci_object_storage",
        coco_class="OciObjectStorageSource",
        config_env_prefix="OCI_",
    ),
    "postgres_source": build_native_passthrough(
        name="postgres_source",
        coco_module="cocoindex.connectors.postgres",
        coco_class="PgTableSource",
        config_env_prefix="POSTGRES_SOURCE_",
        can_write=True,  # Postgres is dual-role (source + target)
    ),
    "kafka": build_native_passthrough(
        name="kafka",
        coco_module="cocoindex.connectors.kafka",
        coco_class="KafkaSource",
        config_env_prefix="KAFKA_",
        can_write=True,  # Kafka is dual-role (source + target)
    ),
    "iggy": build_native_passthrough(
        name="iggy",
        coco_module="cocoindex.connectors.iggy",
        coco_class="IggySource",
        config_env_prefix="IGGY_",
        can_write=True,  # Iggy is dual-role (source + target)
    ),
}

#: Source types that have a native CocoIndex source descriptor (names).
COCO_SOURCES = frozenset(COCO_SOURCE_REGISTRY)


def coco_source(source_type: str, cfg: Dict[str, Any]) -> Optional[CocoSource]:
    """Return a CocoSource descriptor for *source_type* (or None if unrecognised).

    A returned descriptor may still have ``native_available == False`` (scaffold
    or missing CocoIndex module), in which case callers should fall back to
    ``FlexibleDataSource``.  *cfg* is the pipeline config dict.
    """
    builder = COCO_SOURCE_REGISTRY.get(source_type.lower())
    if builder is None:
        return None
    return builder(cfg)


__all__ = [
    # Wired source classes
    "CocoLocalFileSystem",
    "CocoAmazonS3",
    "CocoAzureBlob",
    "CocoGoogleDrive",
    # Wired builders
    "build_localfs",
    "build_amazon_s3",
    "build_azure_blob",
    "build_google_drive",
    # Generic passthrough
    "build_native_passthrough",
    "CocoNativePassthrough",
    # Registry + selector
    "COCO_SOURCE_REGISTRY",
    "COCO_SOURCES",
    "coco_source",
]
