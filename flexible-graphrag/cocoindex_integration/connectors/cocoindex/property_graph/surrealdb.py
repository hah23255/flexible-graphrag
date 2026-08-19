"""CocoIndex-native SurrealDB property-graph connector.

Uses CocoIndex ``graph_chunk`` + ``graph_entity`` tables for nodes and direct
SurrealQL (``_surreal.py``) for dynamic relation / MENTIONS edges.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from cocoindex_integration.connectors.cocoindex.base import CocoPropertyGraph
from cocoindex_integration.connectors.cocoindex._runtime import _get_surrealdb_key
from cocoindex_integration.connectors.cocoindex.property_graph._surreal import (
    _build_surreal_client,
)

logger = logging.getLogger(__name__)


@dataclass
class CocoSurrealDB(CocoPropertyGraph):
    """Descriptor for the native CocoIndex SurrealDB target."""

    name = "surrealdb"

    db_key: Any = None
    chunk_table_name: str = "graph_chunk"
    entity_table_name: str = "graph_entity"
    chunk_pk: str = "id"
    entity_pk: str = "id"
    mention_rel_type: str = "mentions"
    vector_metric: str = "cosine"

    _chunk_schema: Any = field(default=None, repr=False)
    _entity_schema: Any = field(default=None, repr=False)
    _vec_indexed: set = field(default_factory=set, repr=False)

    def _build_schemas(self, embedding_dim: int = 0) -> None:
        if self._chunk_schema is not None:
            return
        from cocoindex.connectors.surrealdb import ColumnDef, TableSchema  # noqa: PLC0415

        _emb_type = (
            f"array<float, {embedding_dim}>"
            if embedding_dim > 0
            else "array<float>"
        )
        self._chunk_schema = TableSchema(
            {
                self.chunk_pk:    ColumnDef(type="string", nullable=False),
                "_node_type":     ColumnDef(type="string"),
                "doc_id":         ColumnDef(type="string"),
                "chunk_index":    ColumnDef(type="int"),
                "text":           ColumnDef(type="string"),
                "file_name":      ColumnDef(type="string"),
                "file_path":      ColumnDef(type="string"),
                "file_type":      ColumnDef(type="string"),
                "modified_at":    ColumnDef(type="string"),
                "embedding":      ColumnDef(type=_emb_type, nullable=True),
            },
        )
        self._entity_schema = TableSchema(
            {
                self.entity_pk:   ColumnDef(type="string", nullable=False),
                "name":           ColumnDef(type="string"),
                "entity_type":    ColumnDef(type="string"),
                "entity_labels":  ColumnDef(type="array", nullable=True),
                "doc_id":         ColumnDef(type="string"),
                "ref_doc_id":     ColumnDef(type="string"),
                "file_name":      ColumnDef(type="string"),
                "source_type":    ColumnDef(type="string"),
                "embedding":      ColumnDef(type=_emb_type, nullable=True),
            },
        )

    async def declare_root_tables(self) -> Tuple[Any, Any]:
        """Mount chunk + entity tables at root scope."""

        entity_emb_dim = 0
        try:
            from cocoindex_integration.connectors.flexible.base import _resolve_main_dim  # noqa: PLC0415
            entity_emb_dim = _resolve_main_dim()
        except Exception:
            entity_emb_dim = 0
        self._build_schemas(entity_emb_dim)

        chunk_table = await self._mount_root_table(
            self.chunk_table_name,
            self._chunk_schema,
        )
        entity_table = await self._mount_root_table(
            self.entity_table_name,
            self._entity_schema,
        )
        logger.info(
            "[coco/surrealdb] root tables ready: %s (chunks) and %s (entities)",
            self.chunk_table_name, self.entity_table_name,
        )
        return chunk_table, entity_table

    async def _mount_root_table(self, table_name: str, schema: Any) -> Any:
        """Mount a root table; drop stale empty DDL when CocoIndex sees 'already exists'."""
        import asyncio as _asyncio  # noqa: PLC0415
        import json as _json  # noqa: PLC0415
        import os as _os  # noqa: PLC0415

        from cocoindex.connectors.surrealdb import mount_table_target  # noqa: PLC0415

        from cocoindex_integration.connectors.cocoindex.property_graph._surreal import (  # noqa: PLC0415
            remove_table_sync,
        )

        try:
            return await mount_table_target(self.db_key, table_name, schema)
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise
            logger.info(
                "[native/surrealdb] table %s already exists (empty DDL from prior cleanup) "
                "— dropping and remounting",
                table_name,
            )
            _raw = _os.getenv(
                "SURREALDB_GRAPH_DB_CONFIG", _os.getenv("GRAPH_DB_CONFIG", "{}")
            ) or "{}"
            _cfg: Dict[str, Any] = _json.loads(_raw)
            await _asyncio.to_thread(remove_table_sync, _cfg, table_name)
            return await mount_table_target(self.db_key, table_name, schema)

    async def mount_root_tables(self) -> Tuple[Any, Any, Optional[Any]]:
        """Mount root tables, declare entity vector index, build direct client."""
        import json as _json  # noqa: PLC0415
        import os as _os  # noqa: PLC0415

        try:
            self.begin_cycle()
            chunk_table, entity_table = await self.declare_root_tables()
        except Exception as exc:
            logger.error(
                "[native/surrealdb] root table mount failed: %s — "
                "is SurrealDB running? "
                "(docker compose -f docker/includes/surrealdb.yaml up -d)",
                exc,
            )
            return None, None, None

        entity_emb_dim = 0
        try:
            from cocoindex_integration.connectors.flexible.base import _resolve_main_dim  # noqa: PLC0415
            entity_emb_dim = _resolve_main_dim()
        except Exception:
            entity_emb_dim = 0
        if entity_emb_dim > 0 and entity_table is not None:
            self._declare_vector_index(entity_table, entity_emb_dim)
        else:
            logger.warning(
                "[native/surrealdb] could not resolve entity embedding dimension at startup "
                "— vector index will be created on first ingest."
            )

        client_tuple: Optional[Any] = None
        try:
            _raw = _os.getenv(
                "SURREALDB_GRAPH_DB_CONFIG", _os.getenv("GRAPH_DB_CONFIG", "{}")
            ) or "{}"
            _cfg: Dict[str, Any] = _json.loads(_raw)
            _cfg.setdefault("chunk_table", self.chunk_table_name)
            _cfg.setdefault("entity_table", self.entity_table_name)
            client_tuple = _build_surreal_client(_cfg)
        except Exception as exc:
            logger.warning(
                "[native/surrealdb] direct SurrealDB client failed: %s — relations skipped",
                exc,
            )

        logger.info(
            "[native/surrealdb] root tables mounted (entity vector index dim=%d)",
            entity_emb_dim,
        )
        return chunk_table, entity_table, client_tuple

    def begin_cycle(self) -> None:
        self._vec_indexed.clear()

    def _declare_vector_index(self, table: Any, dim: int) -> None:
        if dim in self._vec_indexed:
            return
        try:
            table.declare_vector_index(
                field="embedding",
                metric=self.vector_metric,  # type: ignore[arg-type]
                method="hnsw",
                dimension=dim,
            )
            self._vec_indexed.add(dim)
            logger.info(
                "[coco/surrealdb] vector index declared on %s(embedding) dim=%d metric=%s",
                self.entity_table_name, dim, self.vector_metric,
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if "vector_index" in msg or "attachment type" in msg:
                logger.debug(
                    "[coco/surrealdb] vector index not supported by server "
                    "(SurrealDB v3 / CocoIndex connector): %s",
                    exc,
                )
            else:
                logger.warning(
                    "[coco/surrealdb] could not declare vector index on %s: %s",
                    self.entity_table_name, exc,
                )


def build_surrealdb(db_cfg: Dict[str, Any]) -> Optional[CocoSurrealDB]:
    """Build a :class:`CocoSurrealDB` from a parsed JSON config dict (or None)."""
    key = _get_surrealdb_key()
    if key is None:
        logger.warning("[coco] cocoindex not installed — cannot create SurrealDB target")
        return None
    try:
        from cocoindex.connectors import surrealdb as _surreal_mod  # noqa: F401, PLC0415
    except ImportError:
        logger.warning(
            "[coco] cocoindex[surrealdb] not installed — "
            "run: uv pip install 'cocoindex[surrealdb]'"
        )
        return None
    chunk_table = str(db_cfg.get("chunk_table", "graph_chunk"))
    entity_table = str(db_cfg.get("entity_table", "graph_entity"))
    logger.info(
        "[coco] CocoSurrealDB: db_key=%s chunk=%s entity=%s",
        key.key, chunk_table, entity_table,
    )
    return CocoSurrealDB(
        db_key=key,
        chunk_table_name=chunk_table,
        entity_table_name=entity_table,
    )


__all__ = ["CocoSurrealDB", "build_surrealdb"]
