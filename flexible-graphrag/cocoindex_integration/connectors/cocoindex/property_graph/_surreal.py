"""Direct SurrealQL relation / MENTIONS writers for the native SurrealDB target.

Entity and chunk records are written via CocoIndex ``declare_record``; relations
and MENTIONS are written outside CocoIndex (plain SurrealDB client) so we can
use dynamic relationship types without mounting one relation target per predicate.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _build_surreal_config(db_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "url": str(db_cfg.get("url", "ws://localhost:8010/rpc")),
        "namespace": str(db_cfg.get("namespace", "test")),
        "database": str(db_cfg.get("database", "flexible_graphrag")),
        "username": str(db_cfg.get("username", "root")),
        "password": str(db_cfg.get("password", "root")),
        "chunk_table": str(db_cfg.get("chunk_table", "graph_chunk")),
        "entity_table": str(db_cfg.get("entity_table", "graph_entity")),
    }


def _build_surreal_client(db_cfg: Dict[str, Any]) -> Optional[Tuple[Dict[str, Any], Any]]:
    """Return (config dict, ConnectionFactory) for SurrealDB direct writes."""
    try:
        from cocoindex.connectors.surrealdb import ConnectionFactory  # noqa: PLC0415
    except ImportError:
        logger.warning("[native/surrealdb] cocoindex[surrealdb] not installed")
        return None
    cfg = _build_surreal_config(db_cfg)
    factory = ConnectionFactory(
        url=cfg["url"],
        namespace=cfg["namespace"],
        database=cfg["database"],
        credentials={"username": cfg["username"], "password": cfg["password"]},
    )
    return cfg, factory


def remove_table_sync(db_cfg: Dict[str, Any], table_name: str) -> None:
    """Drop a SurrealDB table definition (``REMOVE TABLE IF EXISTS``)."""
    try:
        from surrealdb.connections.blocking_ws import BlockingWsSurrealConnection  # noqa: PLC0415
    except ImportError:
        logger.warning("[native/surrealdb] surrealdb package not installed — cannot drop table")
        return
    cfg = _build_surreal_config(db_cfg)
    conn = BlockingWsSurrealConnection(cfg["url"])
    try:
        conn.signin({"username": cfg["username"], "password": cfg["password"]})
        conn.use(cfg["namespace"], cfg["database"])
        conn.query_raw(f"REMOVE TABLE IF EXISTS {table_name}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


async def _write_relations_surreal_async(
    client_tuple: Any,
    doc_id: str,
    relations: List[Dict[str, Any]],
    mentions: List[Dict[str, Any]],
    chunk_table: str,
    entity_table: str,
    mention_rel_type: str = "mentions",
) -> None:
    """Write relation and MENTIONS edges via SurrealDB RELATE."""
    try:
        cfg, factory = client_tuple
    except (TypeError, ValueError):
        return

    try:
        from surrealdb import AsyncSurreal  # noqa: PLC0415
    except ImportError:
        logger.warning("[native/surrealdb] surrealdb package not installed")
        return

    _chunk_table = chunk_table or cfg.get("chunk_table", "graph_chunk")
    _entity_table = entity_table or cfg.get("entity_table", "graph_entity")
    _mention_rel = mention_rel_type.lower()

    async with AsyncSurreal(cfg["url"]) as db:
        await db.signin({
            "username": cfg["username"],
            "password": cfg["password"],
        })
        await db.use(cfg["namespace"], cfg["database"])

        # Remove prior edges for this document (per-table — no generic "relation" table in v3).
        _rel_types = {
            str(_rel.get("rel_type", "related_to")).lower() for _rel in relations
        }
        for _rt in _rel_types:
            _edge_table = f"relation_{_rt}"
            try:
                await db.query(
                    f"DELETE {_edge_table} WHERE doc_id = $doc_id",
                    {"doc_id": doc_id},
                )
            except Exception as exc:  # noqa: BLE001
                if "does not exist" not in str(exc).lower():
                    logger.debug(
                        "[native/surrealdb] delete from %s skipped: %s",
                        _edge_table, exc,
                    )
        try:
            await db.query(
                f"DELETE {_mention_rel} WHERE doc_id = $doc_id",
                {"doc_id": doc_id},
            )
        except Exception as exc:  # noqa: BLE001
            if "does not exist" not in str(exc).lower():
                logger.debug(
                    "[native/surrealdb] delete from %s skipped: %s",
                    _mention_rel, exc,
                )

        for _rel in relations:
            _rel_type = str(_rel.get("rel_type", "related_to")).lower()
            _from_id = str(_rel["from_id"])
            _to_id = str(_rel["to_id"])
            _rel_id = str(_rel.get("rel_id", f"{doc_id}:rel"))
            try:
                _edge_table = f"relation_{_rel_type}"
                # SurrealDB v3: type::record() cannot appear directly in RELATE
                # targets — use LET variables first.
                await db.query(
                    f"LET $__from = type::record('{_entity_table}', $from_id); "
                    f"LET $__to   = type::record('{_entity_table}', $to_id); "
                    f"RELATE $__from->{_edge_table}->$__to "
                    "SET doc_id = $doc_id, chunk_id = $chunk_id, predicate = $predicate",
                    {
                        "from_id": _from_id,
                        "to_id": _to_id,
                        "doc_id": doc_id,
                        "chunk_id": _rel.get("chunk_id", ""),
                        "predicate": _rel.get("predicate", _rel_type),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "[native/surrealdb] relation write skipped (%s->%s): %s",
                    _from_id, _to_id, exc,
                )

        for _mention in mentions:
            _chunk_id = str(_mention.get("chunk_id", ""))
            _entity_id = str(_mention.get("entity_id", ""))
            if not _chunk_id or not _entity_id:
                continue
            try:
                # SurrealDB v3: use LET variables for type::record() in RELATE.
                await db.query(
                    f"LET $__from = type::record('{_chunk_table}', $chunk_id); "
                    f"LET $__to   = type::record('{_entity_table}', $entity_id); "
                    f"RELATE $__from->{_mention_rel}->$__to "
                    "SET doc_id = $doc_id, chunk_id = $chunk_id, entity_id = $entity_id",
                    {
                        "chunk_id": _chunk_id,
                        "entity_id": _entity_id,
                        "doc_id": doc_id,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "[native/surrealdb] MENTIONS write skipped (%s->%s): %s",
                    _chunk_id, _entity_id, exc,
                )


def write_relations_surreal_sync(
    client_tuple: Any,
    doc_id: str,
    relations: List[Dict[str, Any]],
    mentions: List[Dict[str, Any]],
    chunk_table: str,
    entity_table: str,
    mention_rel_type: str = "mentions",
) -> None:
    """Sync wrapper — run SurrealQL writes in a fresh event loop (thread pool safe)."""
    asyncio.run(
        _write_relations_surreal_async(
            client_tuple,
            doc_id,
            relations,
            mentions,
            chunk_table,
            entity_table,
            mention_rel_type,
        )
    )


def ensure_node_stubs_surreal_sync(
    client_tuple: Any,
    entity_ids: List[str],
    chunk_ids: List[str],
    entity_table: str,
    chunk_table: str,
) -> None:
    """Upsert bare chunk/entity records so RELATE endpoints exist before CocoIndex flush."""
    try:
        cfg, _factory = client_tuple
    except (TypeError, ValueError):
        return

    async def _stubs() -> None:
        try:
            from surrealdb import AsyncSurreal  # noqa: PLC0415
        except ImportError:
            return

        async with AsyncSurreal(cfg["url"]) as db:
            await db.signin({
                "username": cfg["username"],
                "password": cfg["password"],
            })
            await db.use(cfg["namespace"], cfg["database"])

            for _cid in chunk_ids:
                if not _cid:
                    continue
                await db.query(
                    f"UPSERT type::record('{chunk_table}', $id) SET id = $id",
                    {"id": str(_cid)},
                )
            for _eid in entity_ids:
                if not _eid:
                    continue
                await db.query(
                    f"UPSERT type::record('{entity_table}', $id) SET id = $id, name = $id",
                    {"id": str(_eid)},
                )

    asyncio.run(_stubs())


def set_entity_properties_surreal_sync(
    client_tuple: Any,
    entity_id_props: Dict[str, Dict[str, Any]],
    entity_table: str,
) -> None:
    """SurrealDB counterpart of ``_cypher.set_entity_properties_sync``.

    Applies ontology-declared entity properties (``KGEntity.properties``, which
    reach here as ``subject_properties_json`` / ``obj_properties_json`` on the
    triple rows) to entity records as real fields, so they are queryable:

        SELECT * FROM PERSON WHERE SALARY > 100000

    SurrealDB has no Cypher, so this uses ``UPSERT … MERGE $props``: MERGE adds
    and overwrites the given keys while leaving every other field — including
    the columns CocoIndex reconciliation writes — untouched.

    The entity table is SCHEMAFULL — CocoIndex defines it from the fixed
    ``TableSchema`` — so an undeclared key is rejected outright:

        Found field 'SALARY', but no such field exists for table 'graph_entity'

    Per-type ontology properties cannot be in that fixed schema (SALARY belongs
    to PERSON, BUDGET to PROJECT), so each key is declared first with
    ``DEFINE FIELD IF NOT EXISTS … TYPE any``.  ``any`` rather than a concrete
    kind because one key can hold different types across entity types, and
    because MERGE re-validates the *whole* record — a narrower kind would make a
    later write of the same key fail.

    Best-effort: nodes and relations are already written by the time this runs,
    so a failure here loses properties, not the graph.
    """
    try:
        cfg, _factory = client_tuple
    except (TypeError, ValueError):
        return
    rows = [(eid, props) for eid, props in (entity_id_props or {}).items() if eid and props]
    if not rows:
        return

    async def _apply() -> None:
        try:
            from surrealdb import AsyncSurreal  # noqa: PLC0415
        except ImportError:
            return
        async with AsyncSurreal(cfg["url"]) as db:
            await db.signin({
                "username": cfg["username"],
                "password": cfg["password"],
            })
            await db.use(cfg["namespace"], cfg["database"])

            # Declare every key once before any write — a single undeclared key
            # aborts the UPSERT that carries it.
            _keys = {k for _, p in rows for k in p if k and "`" not in k}
            for _key in sorted(_keys):
                await db.query(
                    f"DEFINE FIELD IF NOT EXISTS `{_key}` "
                    f"ON TABLE {entity_table} TYPE any"
                )

            for _eid, _props in rows:
                _clean = {k: v for k, v in _props.items() if k in _keys}
                if not _clean:
                    continue
                await db.query(
                    f"UPSERT type::record('{entity_table}', $id) MERGE $props",
                    {"id": str(_eid), "props": _clean},
                )

    try:
        asyncio.run(_apply())
        logger.debug(
            "set_entity_properties_surreal_sync: patched %d entity record(s)", len(rows)
        )
    except Exception as exc:  # noqa: BLE001 - properties are an enhancement
        logger.warning(
            "set_entity_properties_surreal_sync failed (%s: %s) — entities written "
            "without ontology properties", type(exc).__name__, exc,
        )


__all__ = [
    "_build_surreal_client",
    "write_relations_surreal_sync",
    "ensure_node_stubs_surreal_sync",
    "set_entity_properties_surreal_sync",
]
