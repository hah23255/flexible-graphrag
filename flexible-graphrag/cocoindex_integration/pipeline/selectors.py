"""Target / source pickers and native root-mount helpers.

All singleton state is owned by :mod:`state`.  Functions here read and write
``state.*`` attributes via the module reference — no ``global`` keyword needed.

Import pattern in other pipeline modules::

    from cocoindex_integration.pipeline import selectors as _sel

    vector_target = _sel._pick_vector_target(cfg)
    await _sel._mount_native_target_roots(cfg)
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

from cocoindex_integration.pipeline import state as _state  # noqa: E402
from cocoindex_integration.pipeline import providers as _providers  # noqa: E402
from cocoindex_integration.pipeline.env_config import _load_app_settings  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Target pickers
# ─────────────────────────────────────────────────────────────────────────────

def _pick_vector_target(cfg: Dict[str, Any]) -> Optional[Any]:
    """Return the vector write target for the configured backend.

    ``VECTOR_BACKEND=cocoindex`` → CocoIndex native connector when available.
    Otherwise → FlexibleVectorTarget backed by flexible-graphrag's adapters.

    Returns the same singleton instance on every call so LlamaIndex / PyTorch
    is only loaded once per ``cocoindex update`` run.
    """
    if _state._vector_target_singleton is not None:
        return _state._vector_target_singleton

    vector_db = cfg.get("vector_db", "none")
    if vector_db in ("none", "", None):
        return None
    vector_backend = cfg.get("vector_backend", "llamaindex")
    db_cfg: Dict[str, Any] = {}
    try:
        db_cfg = json.loads(cfg.get("vector_db_config_json", "{}") or "{}")
    except json.JSONDecodeError:
        pass

    if vector_backend == "cocoindex":
        from cocoindex_integration.connectors.cocoindex.vector import (  # noqa: PLC0415
            COCO_VECTOR_TARGETS, coco_vector_target,
        )
        if vector_db in COCO_VECTOR_TARGETS:
            target = coco_vector_target(vector_db, db_cfg)
            if target is not None:
                logger.info("Vector target: CocoIndex native %s connector", vector_db)
                _state._vector_target_singleton = target
                return _state._vector_target_singleton
            logger.warning(
                "VECTOR_BACKEND=cocoindex but native %s connector unavailable — "
                "falling back to FlexibleVector", vector_db,
            )

    try:
        from cocoindex_integration.connectors.flexible.vector import FlexibleVector  # noqa: PLC0415
        app_settings = _load_app_settings()
        logger.info("Vector target: FlexibleVector (%s, backend=%s)", vector_db, vector_backend)
        _state._vector_target_singleton = FlexibleVector(app_settings)  # type: ignore[arg-type]
    except ImportError:
        pass
    return _state._vector_target_singleton


def _pick_pg_target(cfg: Dict[str, Any]) -> Optional[Any]:
    """Return the property-graph write target for the configured backend.

    ``GRAPH_BACKEND=llamaindex|langchain`` → FlexiblePGTarget (all 15 PG stores).
    ``GRAPH_BACKEND=cocoindex``            → CocoIndex native connector when available.
    """
    if _state._pg_target_singleton is not None:
        return _state._pg_target_singleton

    pg_graph_db = cfg.get("pg_graph_db", "none")
    if pg_graph_db in ("none", "", None):
        return None
    graph_backend = cfg.get("graph_backend", "llamaindex")
    db_cfg: Dict[str, Any] = {}
    try:
        db_cfg = json.loads(cfg.get("pg_graph_db_config_json", "{}") or "{}")
    except json.JSONDecodeError:
        pass

    if graph_backend == "cocoindex":
        from cocoindex_integration.connectors.cocoindex.property_graph import (  # noqa: PLC0415
            COCO_PG_TARGETS, coco_pg_target,
        )
        if pg_graph_db in COCO_PG_TARGETS:
            target = coco_pg_target(pg_graph_db, db_cfg)
            if target is not None:
                logger.info("PG target: CocoIndex native %s connector", pg_graph_db)
                _state._pg_target_singleton = target
                return _state._pg_target_singleton
            logger.warning(
                "GRAPH_BACKEND=cocoindex but native %s connector unavailable — "
                "falling back to FlexiblePropertyGraph", pg_graph_db,
            )

    try:
        from cocoindex_integration.connectors.flexible.property_graph import FlexiblePropertyGraph  # noqa: PLC0415
        app_settings = _load_app_settings()
        logger.info("PG target: FlexiblePropertyGraph (%s, backend=%s)", pg_graph_db, graph_backend)
        # Pass the RESOLVED backend: cfg["graph_backend"] already reflects
        # _resolve_pipeline_config()'s downgrade to langchain for LangChain-only
        # stores.  Letting the target re-read GRAPH_BACKEND from the env instead
        # silently discarded that.
        _state._pg_target_singleton = FlexiblePropertyGraph(  # type: ignore[arg-type]
            app_settings, backend=graph_backend,
        )
    except ImportError:
        pass
    return _state._pg_target_singleton


def _pick_rdf_target(cfg: Dict[str, Any]) -> Optional[Any]:
    """Return the RDF triple-store target, or None when ``RDF_GRAPH_DB=none``."""
    if _state._rdf_target_singleton is not None:
        return _state._rdf_target_singleton

    rdf_graph_db = cfg.get("rdf_graph_db", "none")
    if rdf_graph_db in ("none", "", None):
        return None

    try:
        from cocoindex_integration.connectors.flexible.rdf import FlexibleRDFGraph  # noqa: PLC0415
        app_settings = _load_app_settings()
        logger.info("RDF target: FlexibleRDFGraph (%s)", rdf_graph_db)
        _state._rdf_target_singleton = FlexibleRDFGraph(app_settings)  # type: ignore[arg-type]
    except ImportError:
        logger.warning("FlexibleRDFGraph not importable — RDF writes disabled")
    return _state._rdf_target_singleton


def _pick_search_target(cfg: Dict[str, Any]) -> Optional[Any]:
    """Return the full-text search target, or None when ``SEARCH_DB=none``.

    Returns ``None`` in OpenSearch hybrid mode (``VECTOR_DB=opensearch`` *and*
    ``SEARCH_DB=opensearch``): the vector index handles all retrieval, so a
    separate search index (``hybrid_search_fulltext``) would only receive
    redundant writes that are never queried.
    """
    if _state._search_target_singleton is not None:
        return _state._search_target_singleton

    search_db = cfg.get("search_db", "none")
    if search_db in ("none", "", None):
        return None

    vector_db = cfg.get("vector_db", "none")
    if search_db == "opensearch" and vector_db == "opensearch":
        logger.info(
            "OpenSearch hybrid mode: skipping FlexibleSearch target "
            "(VECTOR_DB=opensearch + SEARCH_DB=opensearch — vector index handles all retrieval)"
        )
        return None

    try:
        from cocoindex_integration.connectors.flexible.search import FlexibleSearch  # noqa: PLC0415
        app_settings = _load_app_settings()
        logger.info("Search target: FlexibleSearch (%s)", search_db)
        _state._search_target_singleton = FlexibleSearch(app_settings)  # type: ignore[arg-type]
    except ImportError:
        logger.warning("FlexibleSearch not importable — search writes disabled")
    return _state._search_target_singleton


# NOTE: there is deliberately no ``_pick_source`` here to mirror the four target
# pickers above.  Source selection happens in ``flexible_app.flexible_app_main``,
# which looks the source up in ``native_apps.NATIVE_READERS`` (a lister + a
# per-file ``@coco.fn`` worker) and falls back to ``FlexibleMapView`` /
# ``FlexibleDataSource``.  A descriptor-returning picker cannot express that pair,
# so it would only ever duplicate the availability check.


# ─────────────────────────────────────────────────────────────────────────────
# Native target root-mount helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _mount_root_qdrant_collection(vector_target: Any, cfg: Dict[str, Any]) -> Optional[Any]:
    """Mount the native Qdrant collection at the app_main (root) component scope."""
    if vector_target is None:
        _state._root_qdrant_coll = None
        return None
    _state._root_qdrant_coll = await vector_target.mount_root_collection()
    return _state._root_qdrant_coll


async def _mount_root_neo4j_tables(pg_target: Any, cfg: Dict[str, Any]) -> None:
    """Mount ``__Node__`` (chunks) and ``__Entity__`` (entities) at root scope."""
    if pg_target is None:
        _state._root_neo4j_chunk_tbl = None
        _state._root_neo4j_entity_tbl = None
        _state._root_neo4j_driver = None
        return
    (
        _state._root_neo4j_chunk_tbl,
        _state._root_neo4j_entity_tbl,
        _state._root_neo4j_driver,
    ) = await pg_target.mount_root_tables()


async def _mount_root_falkordb_tables(pg_target: Any, cfg: Dict[str, Any]) -> None:
    """Mount FalkorDB ``__Node__`` + ``__Entity__`` at root scope (native only)."""
    if pg_target is None:
        _state._root_falkordb_chunk_tbl = None
        _state._root_falkordb_entity_tbl = None
        _state._root_falkordb_driver = None
        return
    (
        _state._root_falkordb_chunk_tbl,
        _state._root_falkordb_entity_tbl,
        _state._root_falkordb_driver,
    ) = await pg_target.mount_root_tables()


async def _mount_root_lance_table(vector_target: Any, cfg: Dict[str, Any]) -> Optional[Any]:
    """Mount the native LanceDB table at app_main (root) scope."""
    if vector_target is None:
        _state._root_lance_table = None
        return None
    _state._root_lance_table = await vector_target.mount_root_table()
    return _state._root_lance_table


async def _mount_root_postgres_table(vector_target: Any, cfg: Dict[str, Any]) -> Optional[Any]:
    """Mount the native Postgres/pgvector table at app_main (root) scope."""
    if vector_target is None:
        _state._root_postgres_table = None
        return None
    _state._root_postgres_table = await vector_target.mount_root_table()
    return _state._root_postgres_table


async def _mount_root_surrealdb_tables(pg_target: Any, cfg: Dict[str, Any]) -> None:
    """Mount SurrealDB chunk + entity tables at root scope (native only)."""
    if pg_target is None:
        _state._root_surrealdb_chunk_tbl = None
        _state._root_surrealdb_entity_tbl = None
        _state._root_surrealdb_client = None
        return
    (
        _state._root_surrealdb_chunk_tbl,
        _state._root_surrealdb_entity_tbl,
        _state._root_surrealdb_client,
    ) = await pg_target.mount_root_tables()


async def _ensure_native_pg_roots_if_missing(cfg: Dict[str, Any]) -> None:
    """Retry native PG root mount when tables are still None.

    In live mode, if the DB was down during the first mount, module globals
    stay ``None`` for the whole session.  Call this before graph writes to
    recover automatically when the DB comes back up.
    """
    from cocoindex_integration.connectors.cocoindex.property_graph import (  # noqa: PLC0415
        CocoFalkorDB,
        CocoNeo4j,
        CocoSurrealDB,
    )

    _pt = _pick_pg_target(cfg)
    if isinstance(_pt, CocoNeo4j) and (
        _state._root_neo4j_chunk_tbl is None or _state._root_neo4j_entity_tbl is None
    ):
        logger.info("[native/neo4j] root tables missing — retrying mount …")
        await _mount_root_neo4j_tables(_pt, cfg)
    elif isinstance(_pt, CocoFalkorDB) and (
        _state._root_falkordb_chunk_tbl is None or _state._root_falkordb_entity_tbl is None
    ):
        logger.info("[native/falkordb] root tables missing — retrying mount …")
        await _mount_root_falkordb_tables(_pt, cfg)
    elif isinstance(_pt, CocoSurrealDB) and (
        _state._root_surrealdb_chunk_tbl is None or _state._root_surrealdb_entity_tbl is None
    ):
        logger.info("[native/surrealdb] root tables missing — retrying mount …")
        await _mount_root_surrealdb_tables(_pt, cfg)


async def _mount_native_target_roots(cfg: Dict[str, Any]) -> None:
    """Register flexible providers and mount native vector/PG roots once per run.

    Call once at the start of each ``app_main`` / ``flexible_app_main`` so that:
    * CocoIndex-native targets have their root collection / table mounted.
    * Flexible targets have their ``TargetStateProvider`` registered.
    """
    from cocoindex_integration.connectors.cocoindex.vector import (  # noqa: PLC0415
        CocoLanceDB,
        CocoPostgres,
        CocoQdrant,
    )
    from cocoindex_integration.connectors.cocoindex.property_graph import (  # noqa: PLC0415
        CocoFalkorDB,
        CocoNeo4j,
        CocoSurrealDB,
    )
    from cocoindex_integration.connectors.seam import (  # noqa: PLC0415
        is_coco_pg as _is_coco_pg,
        is_coco_vector as _is_coco_vector,
    )

    _vt = _pick_vector_target(cfg)
    if isinstance(_vt, CocoQdrant):
        await _mount_root_qdrant_collection(_vt, cfg)
    elif isinstance(_vt, CocoLanceDB):
        await _mount_root_lance_table(_vt, cfg)
    elif isinstance(_vt, CocoPostgres):
        await _mount_root_postgres_table(_vt, cfg)
    elif _vt is not None and not _is_coco_vector(_vt):
        _providers._get_or_create_vector_provider(_vt)

    _pt = _pick_pg_target(cfg)
    if isinstance(_pt, CocoNeo4j):
        await _mount_root_neo4j_tables(_pt, cfg)
    elif isinstance(_pt, CocoFalkorDB):
        await _mount_root_falkordb_tables(_pt, cfg)
    elif isinstance(_pt, CocoSurrealDB):
        await _mount_root_surrealdb_tables(_pt, cfg)
    elif _pt is not None and not _is_coco_pg(_pt):
        _providers._get_or_create_pg_provider(_pt)

    _providers._get_or_create_search_provider(_pick_search_target(cfg))
    _providers._get_or_create_rdf_provider(_pick_rdf_target(cfg))
