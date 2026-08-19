"""CocoIndex-native Postgres (pgvector) vector connector."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


def _encode_vector(value: Any) -> str:
    """Serialize a list/array to pgvector text format, e.g. '[1.0,2.0,3.0]'.

    asyncpg does not have a native codec for the pgvector ``vector`` type, so
    CocoIndex's postgres target requires the value to arrive as a string.
    Without this encoder the raw Python list reaches asyncpg and triggers:
        DataError: invalid input for query argument $N (expected str, got list)
    """
    return "[" + ",".join(str(float(x)) for x in value) + "]"


from cocoindex_integration.connectors.cocoindex.base import CocoVector
from cocoindex_integration.connectors.cocoindex._runtime import _get_postgres_key

logger = logging.getLogger(__name__)


@dataclass
class CocoPostgres(CocoVector):
    """Descriptor for the native CocoIndex Postgres/pgvector target."""

    name = "postgres"

    table_name: str = "hybrid_search_vectors"
    pg_schema: str = "public"
    db_key: Any = None  # ContextKey[asyncpg.Pool]
    distance: str = "cosine"
    index_method: str = "hnsw"
    _schema_cache: Dict[int, Any] = field(default_factory=dict, repr=False)

    async def get_schema(self, embedding_dim: int) -> Any:
        """Return (or lazily create) a ``TableSchema`` for *embedding_dim*."""
        if embedding_dim not in self._schema_cache:
            from cocoindex.connectors.postgres import ColumnDef, TableSchema  # noqa: PLC0415

            self._schema_cache[embedding_dim] = TableSchema(
                {
                    "point_id":       ColumnDef(type="text", nullable=False),
                    "doc_id":         ColumnDef(type="text"),
                    "chunk_index":    ColumnDef(type="bigint"),
                    "text":           ColumnDef(type="text"),
                    "file_name":      ColumnDef(type="text"),
                    "file_path":      ColumnDef(type="text"),
                    "file_type":      ColumnDef(type="text"),
                    "source_type":    ColumnDef(type="text"),
                    "modified_at":    ColumnDef(type="text"),
                    "ref_doc_id":     ColumnDef(type="text"),
                    "start_char_idx": ColumnDef(type="bigint", nullable=True),
                    "end_char_idx":   ColumnDef(type="bigint", nullable=True),
                    "total_chunks":   ColumnDef(type="bigint", nullable=True),
                    "metadata_json":  ColumnDef(type="text", nullable=True),
                    "embedding":      ColumnDef(type=f"vector({embedding_dim})", encoder=_encode_vector),
                },
                primary_key=["point_id"],
            )
        return self._schema_cache[embedding_dim]

    async def mount_root_table(self) -> Optional[Any]:
        """Mount the Postgres table at app_main (root) scope."""
        import cocoindex as coco  # noqa: PLC0415
        from cocoindex.connectors.postgres import mount_table_target  # noqa: PLC0415

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
                "[native/postgres] could not resolve embedding dimension at app_main — "
                "root table mount skipped; set COCOINDEX_EMBEDDING_DIMENSION or "
                "EMBEDDING_DIMENSION"
            )
            return None
        try:
            schema = await self.get_schema(emb_dim)
            table = await coco.use_mount(  # type: ignore[call-overload]
                coco.component_subpath("postgres_table"),
                mount_table_target,
                self.db_key,
                self.table_name,
                schema,
                pg_schema_name=self.pg_schema or None,
                managed_by=_ManagedBy.SYSTEM,  # type: ignore[arg-type]
            )
            try:
                table.declare_vector_index(
                    column="embedding",
                    metric=self.distance,  # type: ignore[arg-type]
                    method=self.index_method,  # type: ignore[arg-type]
                )
            except Exception as idx_exc:  # noqa: BLE001
                logger.debug("[native/postgres] vector index declaration: %s", idx_exc)
            logger.info(
                "[native/postgres] root table mount ready "
                "(schema=%s table='%s', emb_dim=%d)",
                self.pg_schema, self.table_name, emb_dim,
            )
            return table
        except Exception as exc:
            logger.error("[native/postgres] root table mount failed: %s", exc)
            return None


def build_postgres(db_cfg: Dict[str, Any]) -> Optional[CocoPostgres]:
    """Build a :class:`CocoPostgres` from a parsed JSON config dict (or None)."""
    key = _get_postgres_key()
    if key is None:
        logger.warning("[coco] cocoindex not installed — cannot create Postgres target")
        return None
    try:
        from cocoindex.connectors import postgres as _postgres_mod  # noqa: F401, PLC0415
    except ImportError:
        logger.warning(
            "[coco] cocoindex[postgres] not installed — "
            "run: uv pip install 'cocoindex[postgres]'"
        )
        return None
    table = db_cfg.get(
        "table_name",
        db_cfg.get("collection_name", "hybrid_search_vectors"),
    )
    pg_schema = str(db_cfg.get("schema", db_cfg.get("pg_schema", "public")))
    distance = db_cfg.get("distance", "cosine")
    index_method = db_cfg.get("index_method", db_cfg.get("method", "hnsw"))
    logger.info(
        "[coco] CocoPostgres: schema=%s table=%s distance=%s method=%s",
        pg_schema, table, distance, index_method,
    )
    return CocoPostgres(
        table_name=str(table),
        pg_schema=pg_schema,
        db_key=key,
        distance=str(distance),
        index_method=str(index_method),
    )


__all__ = ["CocoPostgres", "build_postgres"]
