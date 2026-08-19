"""
CocoIndex-native runtime: ContextKeys, resource lifespan, and connector patches.

This module holds the process-wide machinery shared by every CocoIndex-native
connector (``CocoQdrant``, ``CocoNeo4j``, …):

* Label sanitizers + shared Neo4j label constants.
* ``ContextKey`` singletons (Qdrant client, Neo4j / FalkorDB connection factory).
* Import-time monkey-patches of CocoIndex's Qdrant + Neo4j connectors
  (idempotent collection create; LlamaIndex-style multi-label nodes; clean
  index / constraint names).
* The ``@coco.lifespan`` resource provider that registers native DB clients.

Importing this module has the side effect of installing the patches and
registering the lifespan, so ``connectors/cocoindex/__init__.py`` imports it
eagerly and the concrete connector modules import from it.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Label sanitization helpers
#
# Neo4j node labels and relationship types may only contain letters, digits,
# and underscores.  These helpers normalise arbitrary LLM-generated strings
# so they are always valid.
# ─────────────────────────────────────────────────────────────────────────────

#: Entity type used when the LLM returns an empty or None type.
_FALLBACK_ENTITY_TYPE = "Entity"
#: Predicate used when the LLM returns an empty or None relation label.
_FALLBACK_PREDICATE = "RELATED_TO"


def _safe_label(name: str) -> str:
    """Sanitize *name* for use as a Neo4j node label (prefix will be added by caller)."""
    s = (name or "").strip()
    if not s:
        return "Unknown"
    s = re.sub(r"[^a-zA-Z0-9]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "Unknown"


def _safe_rel_type(name: str) -> str:
    """Sanitize *name* for use as a Neo4j relationship type (prefix added by caller)."""
    s = (name or "").strip().upper()
    if not s:
        return "RELATED_TO"
    s = re.sub(r"[^a-zA-Z0-9]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "RELATED_TO"


# ─────────────────────────────────────────────────────────────────────────────
# Shared Neo4j label constants (LlamaIndex-style multi-label model)
# ─────────────────────────────────────────────────────────────────────────────

#: Base label MERGE'd for every entity node; specific type added via APOC.
SHARED_ENTITY_LABEL = "__Entity__"
#: Base label shared by ALL nodes (chunks + entities), like LlamaIndex's
#: ``__Node__``.  Chunk nodes MERGE on this; entity nodes acquire it via APOC.
SHARED_NODE_LABEL = "__Node__"
#: Display label added to chunk nodes on top of ``__Node__`` (LlamaIndex uses
#: ``Chunk`` for its ``TextNode`` chunks).
CHUNK_DISPLAY_LABEL = "Chunk"


# ─────────────────────────────────────────────────────────────────────────────
# CocoIndex resource keys (module-level singletons)
#
# These ContextKey objects identify resources registered via @coco.lifespan.
# app.py passes them to declare_collection_target() / declare_table_target().
# ─────────────────────────────────────────────────────────────────────────────

_QDRANT_KEY: Optional[Any] = None    # ContextKey[QdrantClient]
_NEO4J_KEY: Optional[Any] = None     # ContextKey[ConnectionFactory]
_FALKORDB_KEY: Optional[Any] = None  # ContextKey[ConnectionFactory]
_LANCEDB_KEY: Optional[Any] = None   # ContextKey[LanceAsyncConnection]
_POSTGRES_KEY: Optional[Any] = None  # ContextKey[asyncpg.Pool]
_SURREALDB_KEY: Optional[Any] = None   # ContextKey[ConnectionFactory]


def _get_qdrant_key() -> Any:
    """Return (or lazily create) the global ContextKey for the Qdrant client."""
    global _QDRANT_KEY
    if _QDRANT_KEY is None:
        try:
            import cocoindex as coco
            _QDRANT_KEY = coco.ContextKey("flexible-graphrag/qdrant")
        except ImportError:
            pass
    return _QDRANT_KEY


def _get_neo4j_key() -> Any:
    """Return (or lazily create) the global ContextKey for the Neo4j factory."""
    global _NEO4J_KEY
    if _NEO4J_KEY is None:
        try:
            import cocoindex as coco
            _NEO4J_KEY = coco.ContextKey("flexible-graphrag/neo4j")
        except ImportError:
            pass
    return _NEO4J_KEY


def _get_falkordb_key() -> Any:
    """Return (or lazily create) the global ContextKey for the FalkorDB factory."""
    global _FALKORDB_KEY
    if _FALKORDB_KEY is None:
        try:
            import cocoindex as coco
            _FALKORDB_KEY = coco.ContextKey("flexible-graphrag/falkordb")
        except ImportError:
            pass
    return _FALKORDB_KEY


def _get_lancedb_key() -> Any:
    """Return (or lazily create) the global ContextKey for the LanceDB connection."""
    global _LANCEDB_KEY
    if _LANCEDB_KEY is None:
        try:
            import cocoindex as coco
            _LANCEDB_KEY = coco.ContextKey("flexible-graphrag/lancedb")
        except ImportError:
            pass
    return _LANCEDB_KEY


def _get_postgres_key() -> Any:
    """Return (or lazily create) the global ContextKey for the Postgres pool."""
    global _POSTGRES_KEY
    if _POSTGRES_KEY is None:
        try:
            import cocoindex as coco
            _POSTGRES_KEY = coco.ContextKey("flexible-graphrag/postgres")
        except ImportError:
            pass
    return _POSTGRES_KEY


def _get_surrealdb_key() -> Any:
    """Return (or lazily create) the global ContextKey for the SurrealDB factory."""
    global _SURREALDB_KEY
    if _SURREALDB_KEY is None:
        try:
            import cocoindex as coco
            _SURREALDB_KEY = coco.ContextKey("flexible-graphrag/surrealdb")
        except ImportError:
            pass
    return _SURREALDB_KEY


# ─────────────────────────────────────────────────────────────────────────────
# CocoIndex Qdrant connector — idempotent-create patch
#
# CocoIndex's _CollectionHandler calls _create_collection(if_not_exists=False)
# when the LMDB state is empty (action="insert", e.g. after cocoindex.db is
# deleted or on the very first run).  If the collection already exists in
# Qdrant (from a previous run or from any other tool), this raises HTTP 409.
#
# Patching to always use if_not_exists=True makes collection creation
# idempotent: CocoIndex checks whether the collection exists first and skips
# creation if it does.  This allows managed_by=SYSTEM (the default) to work
# correctly even when the Qdrant collection pre-exists, while still enabling
# CocoIndex's native delete reconciliation when source files are removed.
# ─────────────────────────────────────────────────────────────────────────────
try:
    from cocoindex.connectors.qdrant import _target as _qdrant_target  # type: ignore[import-untyped]

    _orig_create_collection = _qdrant_target._CollectionHandler._create_collection

    async def _idempotent_create_collection(  # type: ignore[no-untyped-def]
        self: Any,
        client: Any,
        collection_name: str,
        schema: Any,
        *,
        if_not_exists: bool,
    ) -> None:
        """Wrap CocoIndex's _create_collection to always skip if collection exists."""
        # Force if_not_exists=True regardless of action ("insert" or "upsert")
        # so the connector never raises HTTP 409 on a pre-existing collection.
        await _orig_create_collection(
            self, client, collection_name, schema, if_not_exists=True
        )

    _qdrant_target._CollectionHandler._create_collection = _idempotent_create_collection
    logger.debug(
        "[coco/qdrant] _CollectionHandler._create_collection patched "
        "(if_not_exists forced True — idempotent collection creation)"
    )
except (ImportError, AttributeError):
    pass  # cocoindex or the qdrant connector not installed — nothing to patch


# ─────────────────────────────────────────────────────────────────────────────
# CocoIndex Neo4j connector — multi-label node patch (LlamaIndex-style model)
#
# We reproduce LlamaIndex's ``Neo4jPropertyGraphStore`` label + index footprint
# EXACTLY:
#
#   Labels
#   ------
#   * Chunk nodes    : ``:__Node__:Chunk``     (base ``__Node__`` + ``Chunk``)
#   * Entity nodes   : ``:__Node__:__Entity__:<Type>``  (e.g. ``:__Node__:__Entity__:Person``)
#
#   Indexes (+ Neo4j's own base NODE/RELATIONSHIP LOOKUP indexes)
#   ------------------------------------------------------------
#   * RANGE constraint on ``:__Node__(id)``     (from the chunk table mount)
#   * RANGE constraint on ``:__Entity__(id)``   (from the entity table mount)
#   * ONE VECTOR index on ``:__Entity__(embedding)``
#   (CocoIndex additionally creates a small RANGE relationship index per
#    relationship type on its edge pk — required for incremental edge deletes;
#    LlamaIndex has no equivalent but these are unavoidable in CocoIndex.)
#
# How the labels array is produced
# --------------------------------
# CocoIndex's ``build_node_upsert`` emits a single-label MERGE
# (``MERGE (n:`Label` {id: $key_0}) SET n += $props``).  We append an APOC
# ``addLabels`` clause so each node gains its additional labels AFTER the MERGE:
#   * ``__Node__`` table (chunks)  → add ``Chunk``
#   * ``__Entity__`` table         → add ``__Node__`` + the ``entity_type`` value
#
# CRITICAL — why this is collision-free:  the additional labels are NEVER part
# of a MERGE.  Chunks always MERGE on the single label ``:__Node__`` and entities
# always MERGE on the single label ``:__Entity__`` (relationship / MENTIONS
# endpoints included, since endpoint labels come from the endpoint table names).
# So each uniqueness constraint lives on exactly one MERGE label and no
# cross-label MERGE conflict is possible regardless of the order CocoIndex
# applies node vs. relation targets.  Chunk ids (UUID / ``doc_id:chunk:N``) and
# entity ids (``doc_id:name``) never collide, so the shared ``:__Node__(id)``
# constraint (which entities also acquire via APOC) is always satisfied.
#
# Requires the APOC plugin (bundled with Neo4j 5 Docker images; the same
# requirement LlamaIndex's Neo4jPropertyGraphStore has).
# ─────────────────────────────────────────────────────────────────────────────

try:
    from cocoindex.connectors.neo4j import _cypher as _neo4j_cypher  # type: ignore[import-untyped]

    _orig_build_node_upsert = _neo4j_cypher.build_node_upsert

    def _build_node_upsert_multilabel(  # type: ignore[no-untyped-def]
        label: str,
        pk_fields: Any,
        has_value_fields: bool,
    ) -> str:
        """Wrap build_node_upsert to add LlamaIndex-style extra labels via APOC.

        * ``__Entity__`` table → ``:__Node__`` + the ``entity_type`` value
          → node becomes ``:__Entity__:__Node__:<Type>``.
        * ``__Node__`` table (chunks) → ``:Chunk`` → node becomes
          ``:__Node__:Chunk``.

        All other tables (relationship endpoints reuse these same labels) are
        unchanged.  The extra labels are added AFTER the MERGE so they never
        participate in uniqueness checks at merge time.
        """
        cypher = _orig_build_node_upsert(label, pk_fields, has_value_fields)
        if label == SHARED_ENTITY_LABEL:
            # Add __Node__ always; add every label from entity_labels (all types
            # the LLM assigned this entity).  Falls back to entity_type when
            # entity_labels is absent (older data / null property).
            cypher += (
                " WITH n CALL apoc.create.addLabels(n, "
                f"[x IN coalesce(n.entity_labels, [coalesce(n.entity_type, '')]) "
                f"+ ['{SHARED_NODE_LABEL}'] "
                "WHERE x IS NOT NULL AND x <> '']) YIELD node RETURN node"
            )
        elif label == SHARED_NODE_LABEL:
            cypher += (
                " WITH n CALL apoc.create.addLabels(n, "
                f"['{CHUNK_DISPLAY_LABEL}']) YIELD node RETURN node"
            )
        return cypher

    _neo4j_cypher.build_node_upsert = _build_node_upsert_multilabel

    # ── Index / constraint name cleanup ──────────────────────────────────────
    # The built-in naming functions interpolate the label directly, so labels
    # like ``__Entity__`` produce ugly names: ``coco_uniq___Entity____id``.
    # Two-step fix:
    #   1. Strip leading/trailing underscores from the label before delegating.
    #   2. Collapse any remaining run of 2+ underscores to a single underscore.
    # Result:
    #   coco_uniq_Entity_id       (was coco_uniq___Entity____id)
    #   coco_uniq_Node_id         (was coco_uniq___Node____id)
    #   coco_vec_Entity_embedding (was coco_vec___Entity____embedding)
    _MULTI_UNDER = re.compile(r"_{2,}")

    def _clean_lbl(label: str) -> str:
        cleaned = label.strip("_")
        return cleaned if cleaned else label

    def _squash(name: str) -> str:
        return _MULTI_UNDER.sub("_", name)

    _orig_constraint_name    = _neo4j_cypher.constraint_name
    _orig_index_name         = _neo4j_cypher.index_name
    _orig_vector_index_name  = _neo4j_cypher.vector_index_name

    def _constraint_name_clean(label: str, fields: Any) -> str:
        return _squash(_orig_constraint_name(_clean_lbl(label), fields))

    def _index_name_clean(kind: str, label: str, fields: Any) -> str:
        return _squash(_orig_index_name(kind, _clean_lbl(label), fields))

    def _vector_index_name_clean(label: str, field: str) -> str:
        return _squash(_orig_vector_index_name(_clean_lbl(label), field))

    _neo4j_cypher.constraint_name   = _constraint_name_clean
    _neo4j_cypher.index_name        = _index_name_clean
    _neo4j_cypher.vector_index_name = _vector_index_name_clean

    logger.debug(
        "[coco/neo4j] _cypher patched: multi-label APOC upsert + clean index/constraint names"
    )
except (ImportError, AttributeError):
    pass  # cocoindex or the neo4j connector not installed — nothing to patch


# ─────────────────────────────────────────────────────────────────────────────
# CocoIndex lifespan — provides native clients to the ContextProvider
#
# @coco.lifespan is called once when CocoIndex starts.  It receives an
# EnvironmentBuilder and must call builder.provide(key, value) for each
# resource that native target connectors need to look up.
#
# This function is a generator: code before `yield` runs on start, code
# after `yield` runs on shutdown.
# ─────────────────────────────────────────────────────────────────────────────

try:
    import cocoindex as _coco_for_lifespan
    from collections.abc import AsyncIterator as _AsyncIterator

    @_coco_for_lifespan.lifespan
    async def _provide_native_resources(builder: Any) -> _AsyncIterator[None]:  # type: ignore[type-arg]
        """Register native DB clients with CocoIndex's ContextProvider.

        Called once when the CocoIndex environment starts.  Only provides
        resources for backends that are actually configured as "cocoindex".
        Async generator: code before yield runs on start, after yield on shutdown.
        """
        import json as _json

        vector_backend = os.getenv("VECTOR_BACKEND", "llamaindex").lower()
        vector_db = os.getenv("VECTOR_DB", "none").lower()
        graph_backend = os.getenv("GRAPH_BACKEND", "llamaindex").lower()
        pg_graph_db = os.getenv("PG_GRAPH_DB", "none").lower()

        # ── Qdrant client ────────────────────────────────────────────────────
        if vector_backend == "cocoindex" and vector_db == "qdrant":
            try:
                from cocoindex.connectors.qdrant import create_client as _qdrant_create
                _vec_var = "QDRANT_VECTOR_DB_CONFIG"
                _db_cfg: Dict[str, Any] = _json.loads(
                    os.getenv(_vec_var, os.getenv("VECTOR_DB_CONFIG", "{}")) or "{}"
                )
                _host = _db_cfg.get("host", "localhost")
                _port = int(_db_cfg.get("port", 6333))
                _api_key: Optional[str] = _db_cfg.get("api_key") or os.getenv("QDRANT_API_KEY")
                _https = _db_cfg.get("https", False)
                _scheme = "https" if _https else "http"
                _url = _db_cfg.get("url") or f"{_scheme}://{_host}:{_port}"
                _qdrant_client_kwargs: Dict[str, Any] = {}
                if _api_key:
                    _qdrant_client_kwargs["api_key"] = _api_key
                _client = _qdrant_create(_url, prefer_grpc=False, **_qdrant_client_kwargs)
                builder.provide(_get_qdrant_key(), _client)
                logger.info("[coco] Qdrant client registered (url=%s)", _url)
            except Exception as _exc:
                logger.warning("[coco] Could not create Qdrant client: %s", _exc)

        # ── Neo4j connection factory ─────────────────────────────────────────
        if graph_backend == "cocoindex" and pg_graph_db == "neo4j":
            try:
                from cocoindex.connectors.neo4j import ConnectionFactory as _Neo4jConnFactory
                _pg_var = "NEO4J_GRAPH_DB_CONFIG"
                _pg_cfg: Dict[str, Any] = _json.loads(
                    os.getenv(_pg_var, os.getenv("GRAPH_DB_CONFIG", "{}")) or "{}"
                )
                _uri = _pg_cfg.get("url", _pg_cfg.get("uri", "bolt://localhost:7687"))
                _user = _pg_cfg.get("username", _pg_cfg.get("user", "neo4j"))
                _pw = _pg_cfg.get("password", "password")
                _db = _pg_cfg.get("database", "neo4j")
                _neo4j_factory = _Neo4jConnFactory(
                    uri=_uri,
                    auth=(_user, _pw),
                    database=_db,
                )
                builder.provide(_get_neo4j_key(), _neo4j_factory)
                logger.info("[coco] Neo4j ConnectionFactory registered (uri=%s db=%s)", _uri, _db)
            except Exception as _exc:
                logger.warning("[coco] Could not create Neo4j ConnectionFactory: %s", _exc)

        # ── FalkorDB connection factory ──────────────────────────────────────
        if graph_backend == "cocoindex" and pg_graph_db == "falkordb":
            try:
                from cocoindex.connectors.falkordb import ConnectionFactory as _FalkorConnFactory
                _pg_var = "FALKORDB_GRAPH_DB_CONFIG"
                _pg_cfg = _json.loads(
                    os.getenv(_pg_var, os.getenv("GRAPH_DB_CONFIG", "{}")) or "{}"
                )
                _uri = _pg_cfg.get("url", _pg_cfg.get("uri", "falkor://localhost:6379"))
                _graph_name = _pg_cfg.get(
                    "database", _pg_cfg.get("graph", "falkor")
                )
                _falkor_factory = _FalkorConnFactory(uri=_uri, graph=_graph_name)
                builder.provide(_get_falkordb_key(), _falkor_factory)
                logger.info(
                    "[coco] FalkorDB ConnectionFactory registered (uri=%s graph=%s)",
                    _uri, _graph_name,
                )
            except Exception as _exc:
                logger.warning("[coco] Could not create FalkorDB ConnectionFactory: %s", _exc)

        # ── LanceDB async connection ─────────────────────────────────────────
        if vector_backend == "cocoindex" and vector_db == "lancedb":
            try:
                from cocoindex.connectors import lancedb as _lancedb_mod
                _vec_var = "LANCEDB_VECTOR_DB_CONFIG"
                _db_cfg = _json.loads(
                    os.getenv(_vec_var, os.getenv("VECTOR_DB_CONFIG", "{}")) or "{}"
                )
                _uri = _db_cfg.get("uri", _db_cfg.get("path", "./lancedb_data"))
                _conn = await _lancedb_mod.connect_async(_uri)
                builder.provide(_get_lancedb_key(), _conn)
                logger.info("[coco] LanceDB connection registered (uri=%s)", _uri)
            except Exception as _exc:
                logger.warning("[coco] Could not create LanceDB connection: %s", _exc)

        # ── Postgres asyncpg pool ────────────────────────────────────────────
        if vector_backend == "cocoindex" and vector_db == "postgres":
            try:
                import asyncpg  # noqa: PLC0415
                _vec_var = "POSTGRES_VECTOR_DB_CONFIG"
                _db_cfg = _json.loads(
                    os.getenv(_vec_var, os.getenv("VECTOR_DB_CONFIG", "{}")) or "{}"
                )
                _host = _db_cfg.get("host", "localhost")
                _port = int(_db_cfg.get("port", 5433))
                _database = _db_cfg.get("database", "postgres")
                _user = _db_cfg.get("username", _db_cfg.get("user", "postgres"))
                _password = _db_cfg.get("password", "password")
                _dsn = _db_cfg.get("url") or _db_cfg.get("connection_string")
                if not _dsn:
                    _dsn = f"postgresql://{_user}:{_password}@{_host}:{_port}/{_database}"
                _pool = await asyncpg.create_pool(_dsn)
                builder.provide(_get_postgres_key(), _pool)
                logger.info("[coco] Postgres pool registered (dsn=%s)", _dsn.split("@")[-1])
            except Exception as _exc:
                logger.warning("[coco] Could not create Postgres pool: %s", _exc)

        # ── SurrealDB connection factory ─────────────────────────────────────
        if graph_backend == "cocoindex" and pg_graph_db == "surrealdb":
            try:
                from cocoindex.connectors.surrealdb import ConnectionFactory as _SurrealConnFactory
                _pg_var = "SURREALDB_GRAPH_DB_CONFIG"
                _pg_cfg = _json.loads(
                    os.getenv(_pg_var, os.getenv("GRAPH_DB_CONFIG", "{}")) or "{}"
                )
                _url = _pg_cfg.get("url", "ws://localhost:8010/rpc")
                _ns = _pg_cfg.get("namespace", "test")
                _db_name = _pg_cfg.get("database", "flexible_graphrag")
                _user = _pg_cfg.get("username", "root")
                _pw = _pg_cfg.get("password", "root")
                _surreal_factory = _SurrealConnFactory(
                    url=_url,
                    namespace=_ns,
                    database=_db_name,
                    credentials={"username": _user, "password": _pw},
                )
                builder.provide(_get_surrealdb_key(), _surreal_factory)
                logger.info(
                    "[coco] SurrealDB ConnectionFactory registered (url=%s ns=%s db=%s)",
                    _url, _ns, _db_name,
                )
            except Exception as _exc:
                logger.warning("[coco] Could not create SurrealDB ConnectionFactory: %s", _exc)

        yield  # CocoIndex environment is running; teardown happens after this

except ImportError:
    pass  # cocoindex not installed — lifespan not registered, that's fine
