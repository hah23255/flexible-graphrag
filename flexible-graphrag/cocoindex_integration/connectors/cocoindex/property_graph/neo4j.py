"""CocoIndex-native Neo4j property-graph connector.

Implements ``CocoNeo4j`` — the multi-label node + single-vector-index model that
reproduces LlamaIndex's ``Neo4jPropertyGraphStore`` footprint exactly.  Relations
and MENTIONS are written outside CocoIndex via the direct-Cypher writers in
``_cypher.py`` (imported by ``app.py``) to avoid per-relation-type indexes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Optional, Tuple

from cocoindex_integration.connectors.cocoindex.base import CocoPropertyGraph
from cocoindex_integration.connectors.cocoindex._runtime import (
    SHARED_ENTITY_LABEL,
    SHARED_NODE_LABEL,
    _get_neo4j_key,
    _safe_rel_type,
)

logger = logging.getLogger(__name__)


@dataclass
class CocoNeo4j(CocoPropertyGraph):
    """Descriptor for the native CocoIndex Neo4j target.

    Holds the ContextKey (for the ConnectionFactory) and configuration.

    Multi-label nodes (exact LlamaIndex model)
    ------------------------------------------
    Nodes carry a **labels array** exactly like LlamaIndex's
    ``Neo4jPropertyGraphStore``:

    * Chunk nodes  : ``:__Node__:Chunk``
    * Entity nodes : ``:__Node__:__Entity__:<Type>`` (e.g. ``:__Node__:__Entity__:Person``)

    ``:__Node__`` spans every node (query all nodes / chunks+entities uniformly);
    ``:__Entity__`` marks entities only (exclude chunks); the specific type label
    lets Neo4j Browser colour by type.

    Index footprint (matches LlamaIndex, + Neo4j base LOOKUP indexes)
    ----------------------------------------------------------------
    * RANGE constraint on ``:__Node__(id)``    (chunk table mount)
    * RANGE constraint on ``:__Entity__(id)``  (entity table mount)
    * ONE VECTOR index on ``:__Entity__(embedding)`` (name ``coco_vec_Entity_embedding``;
      read retriever patches LlamaIndex's hardcoded ``entity`` at query time)
    * (unavoidable CocoIndex extra: a small RANGE relationship index per rel
      type on its edge pk, needed for incremental edge deletes)

    How the labels array + clean relationship types are achieved
    -----------------------------------------------------------
    CocoIndex mounts one table per Neo4j label and MERGEs nodes on that single
    label; relationship endpoints reuse the endpoint tables' labels.  We mount:

    * the **chunk** table on ``:__Node__`` (pk ``id``) → adds ``:Chunk`` via APOC
    * the **entity** table on ``:__Entity__`` (pk ``id``) → adds ``:__Node__`` +
      the ``entity_type`` value via APOC

    Every relationship / MENTIONS endpoint therefore MERGEs on one single label
    (``:__Node__`` for chunks, ``:__Entity__`` for entities), so each predicate
    maps to exactly one endpoint pair and its Neo4j type is the **bare, clean
    predicate** (``WORKS_FOR``, ``HAS_SKILL``) — no endpoint qualification.
    ``MENTIONS`` is a single clean ``(:__Node__:Chunk)-[:MENTIONS]->(:__Entity__)``.

    Because each uniqueness constraint lives on exactly one MERGE label and chunk
    ids never collide with entity ids, node upserts and relation/MENTIONS
    endpoint MERGEs can never conflict regardless of apply order.

    ``app.py`` still assigns each entity a **single canonical type** (majority
    vote over the LLM's per-triple guesses) so the ``entity_type`` property —
    and thus the specific label added by APOC — is stable per node.
    """
    name = "neo4j"

    db_key: Any = None        # ContextKey[ConnectionFactory]
    # Base MERGE label for chunk nodes (LlamaIndex uses ``__Node__``); the
    # ``:Chunk`` display label is added on top via the APOC patch.
    chunk_table_name: str = SHARED_NODE_LABEL
    # Fallback label for entities whose ontology type is empty/unknown.
    base_entity_label: str = "Entity"
    # Prefix applied to every per-type entity label written to Neo4j.
    # e.g. "" → "Person", "Coco_" → "Coco_Person"
    entity_label_prefix: str = ""
    # Prefix applied to every predicate written as a Neo4j relationship type.
    # e.g. "" → "WORKS_FOR", "COCO_" → "COCO_WORKS_FOR"
    relation_type_prefix: str = ""
    # Node primary-key property.  LlamaIndex/LangChain both use ``id`` on nodes
    # (constraints are ``:__Node__(id)`` / ``:__Entity__(id)``) — match that.
    chunk_pk: str = "id"
    entity_pk: str = "id"
    relation_pk: str = "relation_id"
    # Cypher relationship type linking chunks to the entities they mention.
    mention_rel_type: str = "MENTIONS"
    mention_pk: str = "mention_id"
    # Neo4j vector-index similarity metric ("cosine" | "euclidean").
    vector_metric: str = "cosine"

    # lazily-created TableSchema objects (one entity/relation schema reused
    # for all per-type / per-predicate CocoIndex target instances)
    _chunk_schema: Any = field(default=None, repr=False)
    _entity_schema: Any = field(default=None, repr=False)
    _relation_schema: Any = field(default=None, repr=False)
    _mention_schema: Any = field(default=None, repr=False)
    #: Labels for which a vector index was already declared this process — guards
    #: against re-declaring the same attachment when declare_targets runs per file.
    _vec_indexed: set = field(default_factory=set, repr=False)

    def _build_schemas(self) -> None:
        """Build Neo4j TableSchema objects (synchronous; no network call)."""
        if self._chunk_schema is not None:
            return
        from cocoindex.connectors.neo4j import TableSchema, ColumnDef  # noqa: PLC0415
        # ``embedding`` holds the chunk/entity vector as a Neo4j LIST<FLOAT>; it
        # backs the CREATE VECTOR INDEX declared in declare_targets.  Nullable so
        # nodes without a computed embedding are still written.
        self._chunk_schema = TableSchema(
            columns={
                self.chunk_pk:    ColumnDef(type="STRING", nullable=False),
                # LlamaIndex tags its chunk nodes with _node_type="TextNode";
                # kept for parity (cosmetic — read path uses the vector store).
                "_node_type":     ColumnDef(type="STRING", nullable=True),
                "doc_id":         ColumnDef(type="STRING"),
                "chunk_index":    ColumnDef(type="INTEGER"),
                "text":           ColumnDef(type="STRING"),
                "file_name":      ColumnDef(type="STRING"),
                "file_path":      ColumnDef(type="STRING"),
                "file_type":      ColumnDef(type="STRING"),
                "modified_at":    ColumnDef(type="STRING"),
                "embedding":      ColumnDef(type="LIST<FLOAT>", nullable=True),
            },
            primary_key=self.chunk_pk,
        )
        self._entity_schema = TableSchema(
            columns={
                self.entity_pk:   ColumnDef(type="STRING", nullable=False),
                "name":           ColumnDef(type="STRING"),
                # Primary/canonical type for display; APOC multi-label uses entity_labels.
                "entity_type":    ColumnDef(type="STRING"),
                # All types the LLM assigned this entity (may be multiple).
                # The APOC patch applies every label in this list post-MERGE so nodes
                # receive labels like :__Entity__:__Node__:PERSON:EMPLOYEE simultaneously.
                "entity_labels":  ColumnDef(type="LIST<STRING>", nullable=True),
                "doc_id":         ColumnDef(type="STRING"),
                "ref_doc_id":     ColumnDef(type="STRING"),
                # LlamaIndex TRIPLET_SOURCE_KEY — links entity to source Chunk id for
                # PropertyGraphIndex.as_retriever(include_text=True) / add_source_text().
                "triplet_source_id": ColumnDef(type="STRING"),
                "file_name":      ColumnDef(type="STRING"),
                "source_type":    ColumnDef(type="STRING"),
                "embedding":      ColumnDef(type="LIST<FLOAT>", nullable=True),
            },
            primary_key=self.entity_pk,
        )
        self._relation_schema = TableSchema(
            columns={
                self.relation_pk:  ColumnDef(type="STRING", nullable=False),
                "predicate":       ColumnDef(type="STRING"),
                "doc_id":          ColumnDef(type="STRING"),
                "chunk_id":        ColumnDef(type="STRING"),
                "properties_json": ColumnDef(type="STRING"),
            },
            primary_key=self.relation_pk,
        )
        # MENTIONS edge (:Chunk)->(:<Type>): only bookkeeping properties so
        # CocoIndex can reconcile/delete edges by a stable mention_id.
        self._mention_schema = TableSchema(
            columns={
                self.mention_pk: ColumnDef(type="STRING", nullable=False),
                "doc_id":        ColumnDef(type="STRING"),
                "chunk_id":      ColumnDef(type="STRING"),
                "entity_id":     ColumnDef(type="STRING"),
            },
            primary_key=self.mention_pk,
        )

    async def declare_root_tables(self) -> Tuple[Any, Any]:
        """Mount the ``__Node__`` (chunks) and ``__Entity__`` (entities) tables at root scope.

        Must be called from the **root** CocoIndex component (``app_main``), not
        from within a per-file memoized function.  Mounting at the root means both
        ``TableTarget`` handles survive across update cycles — so when a source file
        is deleted CocoIndex can still look up the per-file records and issue Cypher
        ``DETACH DELETE`` for each one (which auto-removes all incident relationships).

        Relations and MENTIONS are written via a direct ``neo4j`` driver
        (``_root_neo4j_driver`` in ``app.py``) rather than through
        ``mount_relation_target``.  This avoids per-relation-type indexes while still
        giving clean, bare relationship types in the graph.

        The VECTOR index on ``:__Entity__(embedding)`` is declared by the caller
        (``_mount_root_neo4j_tables`` in ``app.py``) immediately after this method
        returns, using the flexible-graphrag entity embedding dimension resolved via
        ``_resolve_main_dim()``.  If the dimension cannot be determined at startup
        (unlikely), ``_declare_vector_index`` is also called from ``_run_pipeline``
        on the first actual ingest as a fallback — the call is idempotent.

        Returns
        -------
        chunk_table, entity_table
            The persistent ``TableTarget`` handles to use with ``declare_record()``.
        """
        self._build_schemas()
        from cocoindex.connectors.neo4j import mount_table_target  # noqa: PLC0415

        chunk_table = await mount_table_target(
            self.db_key,
            self.chunk_table_name,   # SHARED_NODE_LABEL == "__Node__"
            self._chunk_schema,
            primary_key=self.chunk_pk,
        )
        entity_table = await mount_table_target(
            self.db_key,
            SHARED_ENTITY_LABEL,
            self._entity_schema,
            primary_key=self.entity_pk,
        )
        # Vector index is declared by mount_root_tables() (or app.py fallback).
        logger.info(
            "[coco/neo4j] root tables ready: :%s (chunks) and :%s (entities)",
            self.chunk_table_name, SHARED_ENTITY_LABEL,
        )
        return chunk_table, entity_table

    async def mount_root_tables(self) -> Tuple[Any, Any, Optional[Any]]:
        """Mount root tables, declare the entity vector index, build the direct driver.

        Native CocoIndex Neo4j connector only.  This bundles everything ``app.py``
        needs at ``app_main`` scope so its ``_mount_root_neo4j_tables`` is a thin
        wrapper that just stores the returned handles in module globals.

        Steps
        -----
        1. ``begin_cycle()`` — reset the per-cycle vector-index guard so the fresh
           ``TableTarget`` handles created below re-attach their vector index
           (otherwise CocoIndex reconciles the new handle with no attachment and
           drops the existing Neo4j vector index on cycle 2+).
        2. ``declare_root_tables()`` — mount ``:__Node__`` (chunks) + ``:__Entity__``
           (entities) at root scope so both survive across update cycles (needed
           for per-record ``DETACH DELETE`` on file removal).
        3. Resolve the **entity** embedding dim via ``_resolve_main_dim()``
           (flexible-graphrag's main embedding model, e.g. 1536 — NOT CocoIndex's
           internal chunk dim) and declare the single vector index on
           ``:__Entity__(embedding)``.
        4. Build a direct ``neo4j`` driver for relation / MENTIONS writes (kept
           outside CocoIndex to avoid per-relation-type indexes).

        Returns
        -------
        (chunk_table, entity_table, driver_tuple)
            ``driver_tuple`` is ``(driver, db_name)`` or ``None`` when the direct
            driver could not be created.  On any failure ``(None, None, None)`` is
            returned and the error is logged.
        """
        import json as _json  # noqa: PLC0415
        import os as _os  # noqa: PLC0415

        try:
            self.begin_cycle()
            chunk_table, entity_table = await self.declare_root_tables()
        except Exception as exc:
            logger.error("[native/neo4j] root table mount failed: %s", exc)
            return None, None, None

        # Entity embedding dim (flexible-graphrag main model) → vector index.
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
                "[native/neo4j] could not resolve entity embedding dimension at startup "
                "— vector index will be created on first ingest. "
                "Set EMBEDDING_DIMENSION to declare it immediately."
            )

        # Direct neo4j driver for relation / MENTIONS writes (no CocoIndex tracking).
        driver_tuple: Optional[Any] = None
        try:
            import neo4j as _neo4j_mod  # noqa: PLC0415
            _raw = _os.getenv("NEO4J_GRAPH_DB_CONFIG", _os.getenv("GRAPH_DB_CONFIG", "{}")) or "{}"
            _cfg: Dict[str, Any] = _json.loads(_raw)
            _uri  = _cfg.get("url",      _cfg.get("uri",  "bolt://localhost:7687"))
            _user = _cfg.get("username", _cfg.get("user", "neo4j"))
            _pw   = _cfg.get("password", "password")
            _db   = _cfg.get("database", "neo4j")
            _drv  = _neo4j_mod.GraphDatabase.driver(_uri, auth=(_user, _pw))
            driver_tuple = (_drv, _db)
            logger.info("[native/neo4j] direct Cypher driver ready (uri=%s db=%s)", _uri, _db)
        except Exception as exc:
            logger.warning(
                "[native/neo4j] direct Cypher driver failed: %s — relations/MENTIONS skipped", exc
            )
            driver_tuple = None

        logger.info(
            "[native/neo4j] root tables mounted (entity vector index dim=%d)", entity_emb_dim
        )
        return chunk_table, entity_table, driver_tuple

    async def declare_targets(
        self,
        entity_types: FrozenSet[str] = frozenset(),
        relation_combos: FrozenSet[Tuple[str, str, str]] = frozenset(),
        embedding_dim: int = 0,
    ) -> Tuple[Any, Dict[str, Any], Dict[Tuple[str, str, str], Any], Dict[str, Any]]:
        """Declare CocoIndex target handles for chunks, entities, relations, MENTIONS.

        ``embedding_dim`` (> 0) enables a SINGLE Neo4j vector index — on the
        ``:__Entity__(embedding)`` property only (chunk vectors are served from
        the vector store, e.g. Qdrant, so chunks get no vector index).
        This keeps the Neo4j index footprint to 2 uniqueness constraints
        (``:__Node__(id)``, ``:__Entity__(id)``) + 1 vector index, exactly like
        LlamaIndex.

        Returns
        -------
        chunk_table :
            CocoIndex ``TableTarget`` for chunk nodes — MERGE'd on ``:__Node__``
            and given the ``:Chunk`` display label via the APOC patch.
        entity_tables : dict[entity_type → TableTarget]
            Maps EVERY entity type to the SAME single ``:__Entity__``
            ``TableTarget``.  The per-type mapping is kept only so ``app.py``'s
            ``entity_tables.get(type)`` lookups keep working; all entities are
            written to one shared table and gain their ``:__Node__`` +
            specific-type labels (``:Person`` …) via the APOC ``addLabels`` patch.
        relation_targets : dict[(from_type, predicate, to_type) → RelationTarget]
            One ``RelationTarget`` per predicate.  Endpoints are always
            ``:__Entity__``→``:__Entity__`` so the Neo4j relationship type is the
            clean, bare predicate (``WORKS_FOR``, ``HAS_SKILL``) — no endpoint
            qualification.  All combos sharing a predicate map to one target.
        mention_targets : dict[entity_type → RelationTarget]
            Maps EVERY entity type to the SAME single ``MENTIONS``
            ``(:__Node__:Chunk)-[:MENTIONS]->(:__Entity__)`` target — one clean
            relationship type for the whole graph (LlamaIndex style).

        Design notes
        ------------
        *  All entity nodes MERGE on the single ``:__Entity__`` base label; the
           specific type label is added post-MERGE by the APOC patch, so the
           uniqueness constraint lives on one label only and node vs.
           relation/MENTIONS endpoint MERGEs are always conflict-free.
        *  When a source file is deleted CocoIndex compares the set of declared
           records for that file with the previous set and issues Cypher DELETEs
           for every record that disappeared — nodes from the entity table and
           edges from the relation / MENTIONS targets alike.
        """
        self._build_schemas()
        from cocoindex.connectors.neo4j import (  # noqa: PLC0415
            mount_table_target,
            mount_relation_target,
        )

        # ── Chunk table — MERGE on :__Node__, +:Chunk via APOC ────────────────
        # Mounting on ``__Node__`` (not ``Chunk``) makes the uniqueness
        # constraint land on ``:__Node__(id)`` — exactly LlamaIndex's chunk
        # constraint.  The APOC patch then adds the ``:Chunk`` display label so
        # nodes end up ``:__Node__:Chunk``.
        chunk_table = await mount_table_target(
            self.db_key,
            self.chunk_table_name,     # == SHARED_NODE_LABEL ("__Node__")
            self._chunk_schema,
            primary_key=self.chunk_pk,
        )
        # NOTE: no vector index on chunks — chunk vectors are served from the
        # vector store (Qdrant) at query time.  Neo4j keeps exactly ONE vector
        # index (on :__Entity__ below), matching the LlamaIndex footprint:
        # 2 uniqueness constraints (:__Node__(id), :__Entity__(id)) + 1 vector
        # index, on top of Neo4j's own base indexes.  Chunk nodes still carry an
        # ``embedding`` property (just un-indexed).

        # ── Single shared __Entity__ table (LlamaIndex multi-label model) ──────
        # Every entity node is MERGE'd on the ONE base label ``__Entity__``; its
        # specific type label (Person, Company, …) is added afterwards by the
        # APOC ``addLabels`` patch (from the ``entity_type`` property).  Because
        # only ``__Entity__`` is ever used in a MERGE, the uniqueness constraint
        # lives on exactly one label and relation/MENTIONS endpoints (also
        # MERGE'd on ``__Entity__``) can never collide with node upserts.
        entity_table = await mount_table_target(
            self.db_key,
            SHARED_ENTITY_LABEL,
            self._entity_schema,
            primary_key=self.entity_pk,
        )
        if embedding_dim > 0:
            self._declare_vector_index(entity_table, SHARED_ENTITY_LABEL, embedding_dim)
        # All entity types resolve to the same shared table so app.py's
        # ``entity_tables.get(type)`` always returns the __Entity__ target.
        entity_tables: Dict[str, Any] = {
            etype: entity_table for etype in entity_types
        }

        # ── One relation target per predicate — CLEAN, unqualified type ───────
        # Since every endpoint is the single ``__Entity__`` label, a predicate
        # maps to exactly one (from_label, to_label) pair (__Entity__→__Entity__)
        # so no endpoint qualification is ever needed — the Neo4j relationship
        # type is the bare predicate (``WORKS_FOR``, ``HAS_SKILL``).
        relation_targets: Dict[Tuple[str, str, str], Any] = {}
        _rel_by_name: Dict[str, Any] = {}   # rel_name → RelationTarget
        for (from_type, pred, to_type) in sorted(relation_combos):
            rel_name = f"{self.relation_type_prefix}{_safe_rel_type(pred)}"
            rel_tgt = _rel_by_name.get(rel_name)
            if rel_tgt is None:
                rel_tgt = await mount_relation_target(
                    self.db_key,
                    rel_name,
                    entity_table,          # from :__Entity__
                    entity_table,          # to   :__Entity__
                    self._relation_schema,
                    primary_key=self.relation_pk,
                )
                _rel_by_name[rel_name] = rel_tgt
                logger.debug("[coco/neo4j] mounted relation type %r", rel_name)
            relation_targets[(from_type, pred, to_type)] = rel_tgt

        # ── Single MENTIONS: (:__Node__:Chunk)-[:MENTIONS]->(:__Entity__) ─────
        # One clean ``MENTIONS`` type for the whole graph (LlamaIndex style).
        # Both endpoints MERGE on single labels (__Node__ / __Entity__) so this
        # is constraint-safe.  Keyed by entity type only so app.py's per-type
        # lookup keeps working, but every type maps to the SAME shared target.
        mention_target = await mount_relation_target(
            self.db_key,
            f"{self.relation_type_prefix}{_safe_rel_type(self.mention_rel_type)}",
            chunk_table,               # from :__Node__ (chunk)
            entity_table,              # to   :__Entity__
            self._mention_schema,
            primary_key=self.mention_pk,
        )
        mention_targets: Dict[str, Any] = {
            etype: mention_target for etype in entity_types
        }

        return chunk_table, entity_tables, relation_targets, mention_targets

    def begin_cycle(self) -> None:
        """Reset per-cycle state so fresh ``TableTarget`` handles get re-attached.

        Must be called at the **start** of every CocoIndex ``app_main`` run,
        BEFORE ``declare_root_tables()`` creates new ``TableTarget`` handles.

        Why this is needed
        ------------------
        CocoIndex calls ``app_main`` on every update cycle and each call to
        ``mount_table_target`` returns a **fresh** ``TableTarget`` object.
        ``_declare_vector_index`` registers an *attachment* on the handle — if
        the guard ``_vec_indexed`` is not cleared, the new handle never gets its
        attachment and CocoIndex's reconciliation drops the existing Neo4j vector
        index to match the "no attachment" state of the new handle.

        Clearing ``_vec_indexed`` here lets ``_declare_vector_index`` re-attach
        to the new handle.  The within-cycle deduplication still works because
        ``_declare_vector_index`` repopulates ``_vec_indexed`` immediately after
        attaching, preventing redundant calls for the same label within one cycle
        (e.g. when ``_run_pipeline`` is also called as a fallback).
        """
        self._vec_indexed.clear()

    def _declare_vector_index(self, table: Any, label: str, dim: int) -> None:
        """Declare a Neo4j vector index on ``table.embedding`` once per label per cycle.

        ``TableTarget.declare_vector_index`` registers an attachment on the handle;
        ``_vec_indexed`` prevents duplicate attachments within a single update cycle
        (e.g. once from ``_mount_root_neo4j_tables`` and once as a fallback from
        ``_run_pipeline``).

        Call ``begin_cycle()`` at the start of each ``app_main`` run to reset
        ``_vec_indexed`` so fresh handles created by ``mount_table_target`` get
        the attachment — otherwise CocoIndex reconciles the handle with no
        attachment and drops the existing Neo4j vector index.
        """
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
                "[coco/neo4j] vector index declared on :%s(embedding) dim=%d metric=%s",
                label, dim, self.vector_metric,
            )
        except Exception as _vexc:  # noqa: BLE001
            logger.warning(
                "[coco/neo4j] could not declare vector index on :%s: %s", label, _vexc,
            )


def build_neo4j(db_cfg: Dict[str, Any]) -> Optional[CocoNeo4j]:  # noqa: ARG001
    """Build a :class:`CocoNeo4j` from a parsed JSON config dict (or None)."""
    key = _get_neo4j_key()
    if key is None:
        logger.warning("[coco] cocoindex not installed — cannot create Neo4j target")
        return None
    logger.info("[coco] CocoNeo4j: db_key=%s", key.key)
    return CocoNeo4j(db_key=key)
