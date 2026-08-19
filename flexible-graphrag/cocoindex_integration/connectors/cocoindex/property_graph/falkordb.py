"""CocoIndex-native FalkorDB property-graph connector.

Mirrors ``neo4j.CocoNeo4j``: ``__Node__`` chunk table + ``__Entity__`` entity table,
single vector index on entity embeddings, and direct-Cypher relation / MENTIONS
writes (reusing ``_cypher.py`` via a thin FalkorDB graph adapter).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from cocoindex_integration.connectors.cocoindex.base import CocoPropertyGraph
from cocoindex_integration.connectors.cocoindex._runtime import (
    SHARED_ENTITY_LABEL,
    SHARED_NODE_LABEL,
    _get_falkordb_key,
    _safe_rel_type,
)

logger = logging.getLogger(__name__)


class _FalkorSession:
    """Neo4j-driver-shaped session wrapper over a FalkorDB ``Graph``."""

    def __init__(self, graph: Any) -> None:
        self._graph = graph

    def run(self, cypher: str, **params: Any) -> None:
        self._graph.query(cypher, params)

    def __enter__(self) -> "_FalkorSession":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None


class _FalkorDriverAdapter:
    """Lets ``_cypher.write_relations_sync`` reuse FalkorDB via ``session().run()``."""

    def __init__(self, graph: Any) -> None:
        self._graph = graph

    def session(self, database: Any = None) -> _FalkorSession:  # noqa: ARG002
        return _FalkorSession(self._graph)


def _parse_falkordb_url(url: str) -> Tuple[str, int]:
    """Parse ``falkor://host:port`` or ``redis://`` style URLs."""
    if "://" in url:
        parsed = urlparse(url)
        host = parsed.hostname or "localhost"
        port = parsed.port or 6379
        return host, int(port)
    return "localhost", 6379


def _build_falkordb_graph(db_cfg: Dict[str, Any]) -> Optional[Any]:
    """Return a FalkorDB ``Graph`` handle for direct Cypher (relations / MENTIONS)."""
    try:
        from falkordb import FalkorDB  # noqa: PLC0415
    except ImportError:
        logger.warning("[native/falkordb] falkordb package not installed")
        return None
    url = db_cfg.get("url", db_cfg.get("uri", "falkor://localhost:6379"))
    host, port = _parse_falkordb_url(str(url))
    if db_cfg.get("host"):
        host = str(db_cfg["host"])
    if db_cfg.get("port"):
        port = int(db_cfg["port"])
    database = db_cfg.get("database", db_cfg.get("graph", "falkor"))
    try:
        client = FalkorDB(host=host, port=port)
        graph = client.select_graph(database)
        logger.info(
            "[native/falkordb] direct Cypher graph ready (host=%s port=%s graph=%s)",
            host, port, database,
        )
        return graph
    except Exception as exc:
        logger.warning("[native/falkordb] direct Cypher graph failed: %s", exc)
        return None


@dataclass
class CocoFalkorDB(CocoPropertyGraph):
    """Descriptor for the native CocoIndex FalkorDB target."""

    name = "falkordb"

    db_key: Any = None
    chunk_table_name: str = SHARED_NODE_LABEL
    base_entity_label: str = "Entity"
    entity_label_prefix: str = ""
    relation_type_prefix: str = ""
    chunk_pk: str = "id"
    entity_pk: str = "id"
    relation_pk: str = "relation_id"
    mention_rel_type: str = "MENTIONS"
    mention_pk: str = "mention_id"
    vector_metric: str = "cosine"

    _chunk_schema: Any = field(default=None, repr=False)
    _entity_schema: Any = field(default=None, repr=False)
    _vec_indexed: set = field(default_factory=set, repr=False)

    def _build_schemas(self, embedding_dim: int = 0) -> None:
        if self._chunk_schema is not None:
            return
        from cocoindex.connectors.falkordb import TableSchema, ColumnDef  # noqa: PLC0415

        _emb_type = (
            f"vector<float32, {embedding_dim}>"
            if embedding_dim > 0
            else "array"
        )
        self._chunk_schema = TableSchema(
            columns={
                self.chunk_pk:    ColumnDef(type="string", nullable=False),
                "_node_type":     ColumnDef(type="string"),
                "doc_id":         ColumnDef(type="string"),
                "chunk_index":    ColumnDef(type="integer"),
                "text":           ColumnDef(type="string"),
                "file_name":      ColumnDef(type="string"),
                "file_path":      ColumnDef(type="string"),
                "file_type":      ColumnDef(type="string"),
                "modified_at":    ColumnDef(type="string"),
                "embedding":      ColumnDef(type=_emb_type, nullable=True),
            },
            primary_key=self.chunk_pk,
        )
        self._entity_schema = TableSchema(
            columns={
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
            primary_key=self.entity_pk,
        )

    async def declare_root_tables(self) -> Tuple[Any, Any]:
        """Mount ``__Node__`` + ``__Entity__`` at root scope."""
        from cocoindex.connectors.falkordb import mount_table_target  # noqa: PLC0415

        entity_emb_dim = 0
        try:
            from cocoindex_integration.connectors.flexible.base import _resolve_main_dim  # noqa: PLC0415
            entity_emb_dim = _resolve_main_dim()
        except Exception:
            entity_emb_dim = 0
        self._build_schemas(entity_emb_dim)

        chunk_table = await mount_table_target(
            self.db_key,
            self.chunk_table_name,
            self._chunk_schema,
            primary_key=self.chunk_pk,
        )
        entity_table = await mount_table_target(
            self.db_key,
            SHARED_ENTITY_LABEL,
            self._entity_schema,
            primary_key=self.entity_pk,
        )
        logger.info(
            "[coco/falkordb] root tables ready: :%s (chunks) and :%s (entities)",
            self.chunk_table_name, SHARED_ENTITY_LABEL,
        )
        return chunk_table, entity_table

    async def mount_root_tables(self) -> Tuple[Any, Any, Optional[Any]]:
        """Mount root tables, declare entity vector index, build direct graph driver."""
        import json as _json  # noqa: PLC0415
        import os as _os  # noqa: PLC0415

        try:
            self.begin_cycle()
            chunk_table, entity_table = await self.declare_root_tables()
        except Exception as exc:
            logger.error("[native/falkordb] root table mount failed: %s", exc)
            return None, None, None

        entity_emb_dim = 0
        try:
            from cocoindex_integration.connectors.flexible.base import _resolve_main_dim  # noqa: PLC0415
            entity_emb_dim = _resolve_main_dim()
        except Exception:
            entity_emb_dim = 0
        if entity_emb_dim > 0 and entity_table is not None:
            self._declare_vector_index(entity_table, SHARED_ENTITY_LABEL, entity_emb_dim)
        else:
            logger.warning(
                "[native/falkordb] could not resolve entity embedding dimension at startup "
                "— vector index will be created on first ingest."
            )

        driver_tuple: Optional[Any] = None
        try:
            _raw = _os.getenv(
                "FALKORDB_GRAPH_DB_CONFIG", _os.getenv("GRAPH_DB_CONFIG", "{}")
            ) or "{}"
            _cfg: Dict[str, Any] = _json.loads(_raw)
            _graph = _build_falkordb_graph(_cfg)
            if _graph is not None:
                driver_tuple = (_FalkorDriverAdapter(_graph), None)
        except Exception as exc:
            logger.warning(
                "[native/falkordb] direct Cypher driver failed: %s — relations skipped", exc
            )

        logger.info(
            "[native/falkordb] root tables mounted (entity vector index dim=%d)",
            entity_emb_dim,
        )
        return chunk_table, entity_table, driver_tuple

    def begin_cycle(self) -> None:
        self._vec_indexed.clear()

    def _declare_vector_index(self, table: Any, label: str, dim: int) -> None:
        if label in self._vec_indexed:
            return
        try:
            table.declare_vector_index(
                field="embedding",
                metric=self.vector_metric,  # type: ignore[arg-type]
                dimension=dim,
            )
            self._vec_indexed.add(label)
            logger.info(
                "[coco/falkordb] vector index declared on :%s(embedding) dim=%d metric=%s",
                label, dim, self.vector_metric,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[coco/falkordb] could not declare vector index on :%s: %s", label, exc
            )


def build_falkordb(db_cfg: Dict[str, Any]) -> Optional[CocoFalkorDB]:  # noqa: ARG001
    """Build a :class:`CocoFalkorDB` from a parsed JSON config dict (or None)."""
    key = _get_falkordb_key()
    if key is None:
        logger.warning("[coco] cocoindex not installed — cannot create FalkorDB target")
        return None
    try:
        from cocoindex.connectors import falkordb as _falkordb_mod  # noqa: F401, PLC0415
    except ImportError:
        logger.warning(
            "[coco] cocoindex[falkordb] not installed — "
            "run: uv pip install 'cocoindex[falkordb]'"
        )
        return None
    logger.info("[coco] CocoFalkorDB: db_key=%s", key.key)
    return CocoFalkorDB(db_key=key)


__all__ = [
    "CocoFalkorDB",
    "build_falkordb",
    "_FalkorDriverAdapter",
]
