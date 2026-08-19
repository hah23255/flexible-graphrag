"""Direct-Cypher relation / MENTIONS writers for Cypher property-graph stores.

Relations and MENTIONS are written outside CocoIndex (via a plain ``neo4j``
driver) rather than through ``mount_relation_target``.  This avoids one small
RANGE relationship index per relationship type while still producing clean,
bare relationship types in the graph.  Both functions are pure — they take a
``(driver, db_name)`` tuple — so ``app.py`` runs them via ``asyncio.to_thread``.

These helpers are Cypher-generic (Neo4j today; FalkorDB is also openCypher and
can reuse them), so they live in a shared module rather than in ``neo4j.py``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def write_relations_sync(
    driver_tuple: Any,
    doc_id: str,
    relations: List[Dict[str, Any]],
    mentions: List[Dict[str, Any]],
    entity_label: str,
    node_label: str,
    rel_prefix: str = "",
    mention_rel_type: str = "MENTIONS",
) -> None:
    """Write relation and MENTIONS edges to Neo4j using a direct sync driver.

    Called via ``asyncio.to_thread`` so it runs in a thread pool and never
    blocks the async event loop.

    1. Deletes all relationships where ``r.doc_id == doc_id`` (stale cleanup
       for file modifications — nodes are kept; only edges are removed here).
    2. Writes the new relations grouped by relationship type.
    3. Writes MENTIONS edges from Chunk nodes to entity nodes.

    Entity/chunk nodes are expected to already exist (written by CocoIndex
    ``declare_record`` just before this call); the ``MATCH`` clauses in the
    Cypher skip edges whose endpoints are missing.
    """
    try:
        driver, db_name = driver_tuple
    except (TypeError, ValueError):
        return

    el = f"`{entity_label}`"
    nl = f"`{node_label}`"

    with driver.session(database=db_name) as sess:
        # Remove stale edges for this document before re-writing
        sess.run(
            "MATCH ()-[r]-() WHERE r.doc_id = $doc_id DELETE r",
            doc_id=doc_id,
        )

        # Group relations by relationship type and write in batches
        _rel_by_type: Dict[str, List[Dict[str, Any]]] = {}
        for _rel in relations:
            _rt = f"{rel_prefix}{_rel['rel_type']}" if rel_prefix else _rel["rel_type"]
            _rel_by_type.setdefault(_rt, []).append(_rel)

        for _rel_type, _rels in _rel_by_type.items():
            _cypher = (
                f"MATCH (h:{el} {{id: $from_id}})"
                f" MATCH (t:{el} {{id: $to_id}})"
                f" MERGE (h)-[r:`{_rel_type}` {{relation_id: $rel_id}}]->(t)"
                f" SET r.doc_id = $doc_id, r.chunk_id = $chunk_id,"
                f"     r.predicate = $predicate"
            )
            for _rel in _rels:
                try:
                    sess.run(
                        _cypher,
                        from_id=_rel["from_id"],
                        to_id=_rel["to_id"],
                        rel_id=_rel["rel_id"],
                        doc_id=_rel["doc_id"],
                        chunk_id=_rel["chunk_id"],
                        predicate=_rel["predicate"],
                    )
                except Exception as _re:
                    logger.debug("[coco/neo4j] relation write skipped: %s", _re)

        # Write MENTIONS edges (:__Node__:Chunk)→(:__Entity__)
        _mq = (
            f"MATCH (chunk:{nl} {{id: $chunk_id}})"
            f" MATCH (entity:{el} {{id: $entity_id}})"
            f" MERGE (chunk)-[r:`{mention_rel_type}` {{mention_id: $mention_id}}]->(entity)"
            f" SET r.doc_id = $doc_id"
        )
        for _m in mentions:
            try:
                sess.run(
                    _mq,
                    chunk_id=_m["chunk_id"],
                    entity_id=_m["entity_id"],
                    mention_id=_m["mention_id"],
                    doc_id=_m["doc_id"],
                )
            except Exception as _me:
                logger.debug("[coco/neo4j] MENTIONS write skipped: %s", _me)


def set_entity_type_labels_sync(
    driver_tuple: Any,
    entity_id_labels: Dict[str, List[str]],
    entity_label: str,
) -> None:
    """Add type-specific Cypher labels to entity nodes.

    CocoIndex's ``mount_table_target`` creates every entity with a single
    shared label (``__Entity__``).  The semantic type (``PERSON``, ``COMPANY``,
    …) is stored as an ``entity_type`` property but is *not* applied as a
    graph label.  This function runs after ``ensure_node_stubs_sync`` to
    patch each entity node with its real type label(s) via:

        UNWIND $ids AS nid
        MATCH (n:`__Entity__` {id: nid})
        SET n:`PERSON`          ← label embedded in the Cypher string

    Cypher labels cannot be parameterised, so queries are grouped by label
    type and executed one per distinct type.

    Compatible with Neo4j, FalkorDB, Memgraph, and any other openCypher
    implementation that supports ``SET n:LABEL`` syntax.
    """
    try:
        driver, db_name = driver_tuple
    except (TypeError, ValueError):
        return

    # Group entity IDs by each label they should receive
    _label_to_ids: Dict[str, List[str]] = {}
    for entity_id, labels in entity_id_labels.items():
        for label in labels:
            _label_to_ids.setdefault(label, []).append(entity_id)

    if not _label_to_ids:
        return

    el = f"`{entity_label}`"

    with driver.session(database=db_name) as sess:
        for label, ids in _label_to_ids.items():
            safe_label = label.replace("`", "").replace("'", "").replace('"', "")
            if not safe_label:
                continue
            cypher = (
                f"UNWIND $ids AS nid "
                f"MATCH (n:{el} {{id: nid}}) "
                f"SET n:`{safe_label}`"
            )
            try:
                sess.run(cypher, ids=ids)
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "[coco/cypher] set label :%s on %d node(s) failed: %s",
                    label, len(ids), exc,
                )


def ensure_node_stubs_sync(
    driver_tuple: Any,
    entity_ids: List[str],
    chunk_ids: List[str],
    entity_label: str,
    node_label: str,
) -> None:
    """Pre-write bare entity and chunk stubs to Neo4j so that relation MATCHes work.

    CocoIndex's ``declare_record()`` only **queues** writes; the actual Neo4j
    MERGE runs during CocoIndex's reconciliation phase, which happens **after**
    ``_run_pipeline`` returns.  ``write_relations_sync`` uses MATCH — if
    the endpoint nodes don't exist yet, every MATCH returns nothing and all
    relations are silently dropped.

    This function pre-creates minimal stubs (id property only) via UNWIND MERGE
    so that MATCH finds them.  CocoIndex's reconciliation then MERGEs the full
    properties (name, embedding, entity_type, …) onto the same nodes later —
    idempotent, no data loss.

    Called via ``asyncio.to_thread`` before ``write_relations_sync``.
    """
    try:
        driver, db_name = driver_tuple
    except (TypeError, ValueError):
        return

    el = f"`{entity_label}`"
    nl = f"`{node_label}`"

    with driver.session(database=db_name) as sess:
        if entity_ids:
            sess.run(
                f"UNWIND $ids AS nid MERGE (n:{el} {{id: nid}})",
                ids=entity_ids,
            )
        if chunk_ids:
            sess.run(
                f"UNWIND $ids AS nid MERGE (n:{nl} {{id: nid}})",
                ids=chunk_ids,
            )


def set_entity_properties_sync(
    driver_tuple: Any,
    entity_id_props: Dict[str, Dict[str, Any]],
    entity_label: str,
) -> None:
    """Apply ontology-declared entity properties to entity nodes.

    Companion to :func:`set_entity_type_labels_sync`.  CocoIndex's
    ``mount_table_target`` writes the columns declared in the entity
    ``TableSchema`` — a fixed set — so per-entity-type properties (SALARY on
    PERSON, BUDGET on PROJECT) have nowhere to live in that schema.  The
    extractor does produce them (``KGEntity.properties``, from an ontology's
    ``owl:DatatypeProperty`` declarations) and they travel on the triple rows as
    ``subject_properties_json`` / ``obj_properties_json``.

    This patches them onto the nodes as REAL Cypher properties rather than an
    opaque JSON blob, so they are queryable:

        MATCH (n:PERSON) WHERE n.SALARY > 100000 RETURN n

    Unlike labels, properties CAN be parameterised, so one UNWIND handles every
    entity regardless of which keys it carries.  ``SET n += $props`` merges, so
    it never clears the columns CocoIndex reconciliation writes.

    Values are already primitives — ``parse_entity_props`` coerces anything
    nested before the row is built — because property graph stores reject
    map-valued properties.

    Idempotent and best-effort: a failure here must not fail the ingest, since
    the nodes and relations themselves are already written.
    """
    try:
        driver, db_name = driver_tuple
    except (TypeError, ValueError):
        return
    rows = [
        {"id": eid, "props": props}
        for eid, props in (entity_id_props or {}).items()
        if eid and props
    ]
    if not rows:
        return
    el = f"`{entity_label}`"
    try:
        with driver.session(database=db_name) as sess:
            sess.run(
                f"UNWIND $rows AS row "
                f"MATCH (n:{el} {{id: row.id}}) "
                f"SET n += row.props",
                rows=rows,
            )
        logger.debug("set_entity_properties_sync: patched %d entity node(s)", len(rows))
    except Exception as exc:  # noqa: BLE001 - properties are an enhancement
        logger.warning(
            "set_entity_properties_sync failed (%s: %s) — entities written without "
            "ontology properties", type(exc).__name__, exc,
        )
