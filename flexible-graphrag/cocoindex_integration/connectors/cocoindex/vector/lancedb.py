"""CocoIndex-native LanceDB vector connector."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import pyarrow as pa

from cocoindex_integration.connectors.cocoindex.base import CocoVector
from cocoindex_integration.connectors.cocoindex._runtime import _get_lancedb_key

logger = logging.getLogger(__name__)


@dataclass
class CocoLanceDB(CocoVector):
    """Descriptor for the native CocoIndex LanceDB target."""

    name = "lancedb"

    table_name: str = "flexible_graphrag"
    db_key: Any = None  # ContextKey[LanceAsyncConnection]
    distance: str = "cosine"
    _schema_cache: Dict[int, Any] = field(default_factory=dict, repr=False)

    async def get_schema(self, embedding_dim: int) -> Any:
        """Return (or lazily create) a ``TableSchema`` for *embedding_dim*."""
        if embedding_dim not in self._schema_cache:
            from cocoindex.connectors.lancedb import TableSchema, ColumnDef  # noqa: PLC0415

            self._schema_cache[embedding_dim] = TableSchema(
                columns={
                    "point_id":       ColumnDef(type=pa.string(), nullable=False),
                    "doc_id":         ColumnDef(type=pa.string()),
                    "chunk_index":    ColumnDef(type=pa.int64()),
                    "text":           ColumnDef(type=pa.string()),
                    "file_name":      ColumnDef(type=pa.string()),
                    "file_path":      ColumnDef(type=pa.string()),
                    "file_type":      ColumnDef(type=pa.string()),
                    "source_type":    ColumnDef(type=pa.string()),
                    "modified_at":    ColumnDef(type=pa.string()),
                    "ref_doc_id":     ColumnDef(type=pa.string()),
                    "start_char_idx": ColumnDef(type=pa.int64(), nullable=True),
                    "end_char_idx":   ColumnDef(type=pa.int64(), nullable=True),
                    "total_chunks":   ColumnDef(type=pa.int64(), nullable=True),
                    "metadata_json":  ColumnDef(type=pa.string(), nullable=True),
                    "embedding":      ColumnDef(
                        type=pa.list_(pa.float32(), list_size=embedding_dim),
                        nullable=True,
                    ),
                },
                primary_key=["point_id"],
            )
        return self._schema_cache[embedding_dim]

    async def mount_root_table(self) -> Optional[Any]:
        """Mount the LanceDB table at app_main (root) scope."""
        import cocoindex as coco  # noqa: PLC0415
        from cocoindex.connectors.lancedb import mount_table_target  # noqa: PLC0415

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
                "[native/lancedb] could not resolve embedding dimension at app_main — "
                "root table mount skipped; set COCOINDEX_EMBEDDING_DIMENSION or "
                "EMBEDDING_DIMENSION"
            )
            return None
        try:
            schema = await self.get_schema(emb_dim)
            table = await coco.use_mount(  # type: ignore[call-overload]
                coco.component_subpath("lancedb_table"),
                mount_table_target,
                self.db_key,
                self.table_name,
                schema,
                managed_by=_ManagedBy.SYSTEM,  # type: ignore[arg-type]
            )
            # Note: declare_vector_index is intentionally NOT called here.
            # LanceDB refuses to create an ANN index on an empty table
            # ("Creating empty vector indices with train=False is not yet implemented").
            # Without the declaration CocoIndex won't attempt ANN index creation;
            # LanceDB falls back to flat (brute-force) kNN which works correctly
            # at any table size.  A user-managed ANN index can be added later
            # directly via the LanceDB API once the table has sufficient rows.
            logger.info(
                "[native/lancedb] root table mount ready (table='%s', emb_dim=%d)",
                self.table_name, emb_dim,
            )
            return table
        except Exception as exc:
            logger.error("[native/lancedb] root table mount failed: %s", exc)
            return None


def build_lancedb(db_cfg: Dict[str, Any]) -> Optional[CocoLanceDB]:
    """Build a :class:`CocoLanceDB` from a parsed JSON config dict (or None)."""
    key = _get_lancedb_key()
    if key is None:
        logger.warning("[coco] cocoindex not installed — cannot create LanceDB target")
        return None
    try:
        from cocoindex.connectors import lancedb as _lancedb_mod  # noqa: F401, PLC0415
    except ImportError:
        logger.warning(
            "[coco] cocoindex[lancedb] not installed — "
            "run: uv pip install 'cocoindex[lancedb]'"
        )
        return None
    uri = db_cfg.get("uri", db_cfg.get("path", "./lancedb_data"))
    table = db_cfg.get("table_name", db_cfg.get("collection_name", "flexible_graphrag"))
    distance = db_cfg.get("distance", "cosine")
    logger.info("[coco] CocoLanceDB: uri=%s table=%s distance=%s", uri, table, distance)
    return CocoLanceDB(table_name=table, db_key=key, distance=distance)


__all__ = ["CocoLanceDB", "build_lancedb"]
