"""CocoIndex-native Qdrant vector connector."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from cocoindex_integration.connectors.cocoindex.base import CocoVector
from cocoindex_integration.connectors.cocoindex._runtime import _get_qdrant_key

logger = logging.getLogger(__name__)


@dataclass
class CocoQdrant(CocoVector):
    """Descriptor for the native CocoIndex Qdrant target.

    Holds the ContextKey (for the QdrantClient) and the collection name.
    The actual CollectionSchema is created lazily inside ``_run_pipeline``
    (async) from the embedding dimension.
    """
    name = "qdrant"

    collection_name: str = "flexible_graphrag"
    db_key: Any = None  # ContextKey[QdrantClient]
    distance: str = "cosine"
    # populated on first use so we don't hit the Qdrant API at import time
    _schema_cache: Dict[int, Any] = field(default_factory=dict, repr=False)

    async def get_schema(self, embedding_dim: int) -> Any:
        """Return (or lazily create) a CollectionSchema for *embedding_dim*.

        Uses a named vector ``"text-dense"`` to match the collection schema
        created by LlamaIndexQdrantAdapter (which calls
        ``recreate_collection(vectors_config={"text-dense": VectorParams(…)})``).
        Unnamed vectors (``""``) would be rejected with HTTP 400 by a collection
        that only has named vectors.
        """
        if embedding_dim not in self._schema_cache:
            import numpy as np
            from cocoindex.connectors.qdrant import CollectionSchema, QdrantVectorDef
            from cocoindex.resources.schema import VectorSchema
            schema = await CollectionSchema.create(
                {
                    "text-dense": QdrantVectorDef(
                        schema=VectorSchema(dtype=np.dtype(np.float32), size=embedding_dim),
                        distance=self.distance,  # type: ignore[arg-type]
                    )
                }
            )
            self._schema_cache[embedding_dim] = schema
        return self._schema_cache[embedding_dim]

    async def mount_root_collection(self) -> Optional[Any]:
        """Mount this Qdrant collection at the app_main (root) component scope.

        Native CocoIndex Qdrant connector only.  Returns the collection mount
        handle (or None when the embedding dimension cannot be resolved).  The
        pipeline (``app.py``) stores this handle so per-file ``declare_point()``
        calls attach to a single persistent collection handler, which lets
        CocoIndex reconcile individual point deletions when a source file is
        removed.

        The embedding dimension is resolved via ``_resolve_cocoindex_dim()``
        (``COCOINDEX_EMBEDDING_DIMENSION`` → ``{KIND}_EMBEDDING_DIMENSION`` →
        ``EMBEDDING_DIMENSION``) — NOT CocoIndex's internal default dim — so the
        collection schema matches the vectors the pipeline actually writes.
        """
        import cocoindex as coco  # noqa: PLC0415
        from cocoindex.connectors.qdrant import declare_collection_target as _qdrant_decl  # noqa: PLC0415
        try:
            from cocoindex.connectorkits.target import ManagedBy as _ManagedBy  # noqa: PLC0415
        except ImportError:
            class _ManagedBy:  # type: ignore[no-redef]
                SYSTEM = "system"

        emb_dim = 0
        try:
            from cocoindex_integration.connectors.flexible.base import _resolve_cocoindex_dim  # noqa: PLC0415
            emb_dim = _resolve_cocoindex_dim()
        except Exception:
            emb_dim = 0
        if emb_dim <= 0:
            logger.warning(
                "[native/qdrant] could not resolve embedding dimension at app_main — "
                "root collection mount skipped; set COCOINDEX_EMBEDDING_DIMENSION or "
                "EMBEDDING_DIMENSION"
            )
            return None
        try:
            schema = await self.get_schema(emb_dim)
            coll = await coco.use_mount(  # type: ignore[call-overload]
                coco.component_subpath("qdrant_coll"),
                _qdrant_decl,
                self.db_key,
                self.collection_name,
                schema,
                managed_by=_ManagedBy.SYSTEM,  # type: ignore[arg-type]
            )
            logger.info(
                "[native/qdrant] root collection mount ready (collection='%s', emb_dim=%d)",
                self.collection_name, emb_dim,
            )
            return coll
        except Exception as exc:
            logger.error("[native/qdrant] root collection mount failed: %s", exc)
            return None


def build_qdrant(db_cfg: Dict[str, Any]) -> Optional[CocoQdrant]:
    """Build a :class:`CocoQdrant` from a parsed JSON config dict (or None)."""
    key = _get_qdrant_key()
    if key is None:
        logger.warning("[coco] cocoindex not installed — cannot create Qdrant target")
        return None
    collection = db_cfg.get("collection_name", "flexible_graphrag")
    distance = db_cfg.get("distance", "cosine")
    logger.info("[coco] CocoQdrant: collection=%s distance=%s", collection, distance)
    return CocoQdrant(collection_name=collection, db_key=key, distance=distance)
