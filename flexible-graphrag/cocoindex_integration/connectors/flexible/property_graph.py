"""``FlexiblePropertyGraph`` — CocoIndex target backed by flexible-graphrag PG adapters.

Supports all 15 flexible-graphrag property graph stores:

    Neo4j, FalkorDB, SurrealDB, ArcadeDB, Memgraph, NebulaGraph, HugeGraph,
    ArangoDB, Apache AGE, TigerGraph, LadybugDB, Cosmos Gremlin, Spanner,
    Neptune, Neptune Analytics.

Used when ``GRAPH_BACKEND`` is not ``cocoindex`` — the connector wraps
flexible-graphrag's own LlamaIndex / LangChain adapters and respects every
``PG_GRAPH_DB`` / ``GRAPH_BACKEND`` setting already in ``.env``.  For the
CocoIndex-native graph stores use the ``connectors.cocoindex`` family instead.

KG Triple target schema
-----------------------
Each row represents one KG triple.  CocoIndex tracks rows by (doc_id, triple_index)
so deletions propagate automatically when a document is removed or updated.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Optional

from cocoindex_integration.connectors.rows import ChunkRow, KGTripleRow
from cocoindex_integration.connectors.flexible.base import (
    FlexibleConnector,
    FlexibleReconcileHandler,
    content_fingerprint,
    graph_backend,
    parse_entity_props as _parse_entity_props,
)

logger = logging.getLogger(__name__)


def _resolve_lc_graph(adapter: Any) -> Any:
    """Return the LangChain graph object from a store adapter.

    The CocoIndex ``FlexiblePropertyGraph`` path uses ``create_property_graph_adapter``
    directly (e.g. ``Neo4jAdapter`` with ``get_graph()`` / ``.lc_graph``).  The main
    flexible-graphrag ingestion path wraps the same adapters in ``LangChainPGAdapter``
    (``get_lc_graph()``).  Accept either shape so LC graph writes do not silently no-op.
    """
    for attr in ("get_lc_graph", "get_graph"):
        fn = getattr(adapter, attr, None)
        if callable(fn):
            graph = fn()
            if graph is not None:
                return graph
    return getattr(adapter, "lc_graph", None)


def _create_llamaindex_graph_store(db_type: str, cfg: Dict[str, Any]) -> Any:
    """Create a LlamaIndex property graph store via the shared LI factory."""
    from llamaindex.graph.adapters.factory import create_graph_store  # type: ignore[import-untyped]
    return create_graph_store(db_type=db_type, config=cfg)


class FlexiblePropertyGraph(FlexibleConnector):
    """Custom CocoIndex target backed by flexible-graphrag's PG adapters.

    Supports the property graph stores CocoIndex does not natively provide
    (arcadedb, memgraph, nebula, hugegraph, arangodb, apache_age, tigergraph,
    ladybug, cosmos_gremlin, spanner, neptune, neptune_analytics) as well as the
    LlamaIndex/LangChain paths for neo4j / falkordb / surrealdb.
    """

    def __init__(self, app_config, backend: Optional[str] = None) -> None:
        super().__init__(app_config)
        self._adapter: Optional[Any] = None
        # Resolved backend from the pipeline config, when the caller has one.
        # _resolve_pipeline_config() downgrades llamaindex -> langchain for the
        # LangChain-only stores (arangodb, apache_age, hugegraph, tigergraph,
        # surrealdb, cosmos_gremlin).  Re-reading GRAPH_BACKEND from the env here
        # threw that decision away and asked LlamaIndex for a store it cannot
        # serve -- "Unsupported graph database: <db>" -- so the graph target was
        # skipped and nothing was indexed.
        self._resolved_backend: Optional[str] = (
            str(backend).lower() if backend else None
        )
        self._backend: str = "llamaindex"
        self._pending: Dict[str, List[KGTripleRow]] = {}  # doc_id -> triples
        self._pending_chunks: Dict[str, List[ChunkRow]] = {}  # doc_id -> chunks
        self._embed_model: Optional[Any] = None  # LlamaIndex BaseEmbedding for entity nodes

    # ------------------------------------------------------------------
    # CocoIndex lifecycle hooks
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        if self._adapter is not None:
            return  # idempotent — already initialised; shared across parallel files
        self._backend = self._resolved_backend or graph_backend()
        db_type = str(getattr(self.app_config, "pg_graph_db", "none"))
        cfg: Dict[str, Any] = getattr(self.app_config, "graph_db_config", {}) or {}
        try:
            if self._backend == "llamaindex":
                # LlamaIndex stores do not require langchain-neo4j or similar packages.
                self._adapter = await asyncio.to_thread(
                    _create_llamaindex_graph_store, db_type, cfg
                )
            else:
                # langchain or cocoindex backend — use flexible-graphrag's LC factory.
                from langchain.graph.pg_store_adapters import create_property_graph_adapter  # type: ignore[import-untyped]
                self._adapter = create_property_graph_adapter(db_type=db_type, config=cfg)
            logger.info(
                "FlexiblePropertyGraph: adapter ready for '%s' (backend=%s)",
                db_type, self._backend,
            )
        except Exception as exc:
            logger.error("FlexiblePropertyGraph: setup failed for '%s': %s", db_type, exc)
            raise

        # Build embedding model so entity nodes get vector properties written.
        # This is what makes pg_vector_search / GraphEntityVectorRetriever work.
        # Uses the same create_embedding_model path as _embed_chunks_cached in app.py.
        if self._backend == "llamaindex" and self._embed_model is None:
            try:
                from llamaindex.llm.embedding_factory import create_embedding_model
                from config import Settings, LLMProvider  # type: ignore[import-untyped]
                _fg_settings = Settings()
                _prov_str = getattr(_fg_settings, "llm_provider", "openai")
                try:
                    _prov = LLMProvider(_prov_str) if isinstance(_prov_str, str) else _prov_str
                except ValueError:
                    _prov = LLMProvider.OPENAI
                _llm_cfg = getattr(_fg_settings, "llm_config", {}) or {}
                self._embed_model = await asyncio.to_thread(
                    create_embedding_model, _prov, _llm_cfg, _fg_settings
                )
                if self._embed_model is not None:
                    logger.info("FlexiblePropertyGraph: embedding model ready for entity node vectors")
                else:
                    logger.warning("FlexiblePropertyGraph: create_embedding_model returned None — entity embeddings disabled")
            except Exception as exc:
                logger.warning(
                    "FlexiblePropertyGraph: entity embeddings skipped: %s", exc, exc_info=True,
                )

    async def declare_chunk_node(self, row: ChunkRow) -> None:
        """Buffer a chunk (TextNode) to be written alongside its entity triples."""
        self._pending_chunks.setdefault(row.doc_id, []).append(row)

    async def declare_row(self, row: KGTripleRow) -> None:
        self._pending.setdefault(row.doc_id, []).append(row)

    async def finalize(self) -> None:
        # Snapshot and clear atomically BEFORE any await point.  CocoIndex
        # calls declare_row / declare_chunk_node from concurrent coroutines;
        # if we iterated self._pending directly after an await, a concurrent
        # declare_row could add a new key and raise
        # "dictionary changed size during iteration".
        pending = self._pending
        pending_chunks = self._pending_chunks
        self._pending = {}
        self._pending_chunks = {}

        if not pending and not pending_chunks:
            return

        # ── Batch-embed all unique entity names across every pending document ──
        # Done once here (not per-chunk) so the API call count is minimised.
        entity_embeddings: Dict[str, List[float]] = {}
        if self._embed_model is not None and pending:
            all_names: List[str] = sorted({
                name
                for triples in pending.values()
                for t in triples
                for name in (t.subject, t.obj)
                if name
            })
            if all_names:
                try:
                    embeddings = await asyncio.to_thread(
                        self._embed_model.get_text_embedding_batch, all_names,
                    )
                    entity_embeddings = dict(zip(all_names, embeddings))
                    logger.info(
                        "FlexiblePropertyGraph: embedded %d entity names for Neo4j vector index",
                        len(entity_embeddings),
                    )
                except Exception as exc:
                    logger.warning("FlexiblePropertyGraph: entity embedding failed: %s", exc)

        for doc_id, triples in pending.items():
            chunks = pending_chunks.get(doc_id, [])
            await self._write_triples(doc_id, triples, chunks, entity_embeddings)
        # Write chunk-only docs (chunks with no KG triples — still need chunk nodes)
        for doc_id, chunks in pending_chunks.items():
            if doc_id not in pending:
                await self._write_triples(doc_id, [], chunks, entity_embeddings)

    async def teardown(self) -> None:
        self._adapter = None

    async def delete_row(self, doc_id: str) -> None:
        """Delete all graph nodes/edges for *doc_id* (called by CocoIndex on stale rows).

        Strategy (in order):
        1. LlamaIndex backend: run raw Cypher/Gremlin via ``structured_query()``
           — avoids calling ``adapter.delete(entity_names=...)`` which expects a
           *list* of entity names, not a doc_id string.
        2. LangChain backend: use ``get_lc_graph().query(cypher, params={...})``.
        3. Last resort: call the underlying ``delete()`` on the adapter (may not
           work for all stores but serves as a best-effort fallback).
        """
        if self._adapter is None:
            return
        cypher_by_ref = "MATCH (n) WHERE n.ref_doc_id = $rid DETACH DELETE n"
        deleted = False

        # ── Path 1: LlamaIndex backend via structured_query() ─────────────────
        _squery = getattr(self._adapter, "structured_query", None)
        if _squery is not None and not deleted:
            try:
                await asyncio.to_thread(
                    _squery,
                    cypher_by_ref,
                    param_map={"rid": doc_id},
                )
                logger.info(
                    "FlexiblePropertyGraph: deleted nodes/edges for doc '%s' via structured_query",
                    doc_id,
                )
                deleted = True
            except Exception as exc:
                logger.warning(
                    "FlexiblePropertyGraph: structured_query delete failed for '%s': %s "
                    "— will try LangChain fallback",
                    doc_id, exc,
                )

        # ── Path 2: LangChain backend via lc_graph.query() ──────────────
        if not deleted:
            try:
                lc_graph = _resolve_lc_graph(self._adapter) if self._adapter is not None else None
                if lc_graph is not None:
                    _q = getattr(lc_graph, "query", None)
                    if _q is not None:
                        await asyncio.to_thread(
                            _q, cypher_by_ref, params={"rid": doc_id}
                        )
                        logger.info(
                            "FlexiblePropertyGraph: deleted nodes/edges for doc '%s' via lc_graph.query",
                            doc_id,
                        )
                        deleted = True
            except Exception as exc:
                logger.warning(
                    "FlexiblePropertyGraph: lc_graph delete failed for '%s': %s",
                    doc_id, exc,
                )

        if not deleted:
            logger.warning(
                "FlexiblePropertyGraph: could not delete nodes for doc '%s' — no usable delete path found",
                doc_id,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _write_triples(
        self,
        doc_id: str,
        triples: List[KGTripleRow],
        chunks: Optional[List[ChunkRow]] = None,
        entity_embeddings: Optional[Dict[str, List[float]]] = None,
    ) -> None:
        if self._adapter is None or (not triples and not chunks):
            return
        if self._backend == "llamaindex":
            await asyncio.to_thread(
                self._write_triples_llamaindex, doc_id, triples, chunks or [],
                entity_embeddings,
            )
        else:
            await self._write_triples_langchain(doc_id, triples, chunks or [])

    def _write_triples_llamaindex(
        self,
        doc_id: str,
        triples: List[KGTripleRow],
        chunks: List[ChunkRow],
        entity_embeddings: Optional[Dict[str, List[float]]] = None,
    ) -> None:
        """Write chunk nodes + entity nodes + relations to a LlamaIndex PG store.

        Chunk nodes are written first as ``ChunkNode`` objects (LlamaIndex's
        property-graph chunk type) so the store creates proper ``__Chunk__``
        nodes with ``text`` content.  Entity nodes reference the chunk via
        ``TRIPLET_SOURCE_KEY`` so LlamaIndex creates ``MENTIONS`` edges.
        Entity embeddings (when provided) are stored on the entity node so that
        ``pg_vector_search`` / ``GraphEntityVectorRetriever`` work.
        """
        from llama_index.core.graph_stores.types import (  # type: ignore[import-untyped]
            EntityNode, Relation,
        )

        # ── ChunkNode type — proper property-graph chunk, not document TextNode ──
        # ChunkNode is stored as __Chunk__ in Neo4j with a `text` property.
        # upsert_nodes() understands it; TextNode (from llama_index.core.schema)
        # is a document-layer class that upsert_nodes() silently skips.
        try:
            from llama_index.core.graph_stores.types import ChunkNode  # type: ignore[import-untyped]
            _ChunkNode = ChunkNode
        except ImportError:
            _ChunkNode = None  # fallback handled below via structured_query

        # Helpers for writing _node_content so the standard LlamaIndex
        # get_llama_nodes() → metadata_dict_to_node() path can reconstruct chunks.
        try:
            from llama_index.core.schema import TextNode as _TextNode  # type: ignore[import-untyped]
            from llama_index.core.vector_stores.utils import (  # type: ignore[import-untyped]
                node_to_metadata_dict as _node_to_metadata_dict,
            )
            _HAS_NODE_CONTENT_HELPERS = True
        except ImportError:
            _HAS_NODE_CONTENT_HELPERS = False

        # ── TRIPLET_SOURCE_KEY constant ──────────────────────────────────────
        try:
            from llama_index.core.graph_stores.types import TRIPLET_SOURCE_KEY  # type: ignore[import-untyped]
        except ImportError:
            TRIPLET_SOURCE_KEY = "triplet_source_id"

        # ── Chunk nodes ───────────────────────────────────────────────────────
        chunk_nodes: List[Any] = []
        for chunk in chunks:
            props = {
                "ref_doc_id": doc_id,
                "doc_id": doc_id,
                "file_name": chunk.file_name,
                "file_path": chunk.file_path,
                "file_type": chunk.file_type,
                "modified_at": chunk.modified_at,
            }
            # Inject _node_content so that the base-class get_llama_nodes() →
            # metadata_dict_to_node() path can reconstruct a TextNode.  This
            # mirrors exactly what PropertyGraphStore.upsert_llama_nodes() does
            # and is required when GRAPH_BACKEND=llamaindex reads chunks back
            # for add_source_text() to attach paragraph context to triplets.
            if _HAS_NODE_CONTENT_HELPERS:
                try:
                    _tmp = _TextNode(
                        id_=chunk.chunk_id,
                        text=chunk.chunk_text,
                        metadata=dict(props),
                    )
                    props.update(_node_to_metadata_dict(_tmp, remove_text=True))
                except Exception as _nc_err:  # noqa: BLE001
                    logger.debug(
                        "FlexiblePropertyGraph: could not build _node_content for chunk %s: %s",
                        chunk.chunk_id,
                        _nc_err,
                    )
            if _ChunkNode is not None:
                cn = _ChunkNode(
                    id_=chunk.chunk_id,
                    text=chunk.chunk_text,
                    properties=props,
                )
                # Attach pre-computed embedding so Neo4j stores the chunk vector
                if chunk.embedding:
                    cn.embedding = chunk.embedding
                chunk_nodes.append(cn)

        if _ChunkNode is None and chunks:
            # Fallback: write __Chunk__ nodes directly via structured_query
            _squery = getattr(self._adapter, "structured_query", None)
            if _squery is not None:
                for chunk in chunks:
                    try:
                        _squery(
                            "MERGE (c:__Chunk__ {id: $cid}) "
                            "SET c.text = $text, c.ref_doc_id = $rid, "
                            "c.doc_id = $rid, c.file_name = $fname",
                            param_map={
                                "cid": chunk.chunk_id,
                                "text": chunk.chunk_text,
                                "rid": doc_id,
                                "fname": chunk.file_name,
                            },
                        )
                    except Exception as _ce:
                        logger.warning("FlexiblePropertyGraph: chunk Cypher failed: %s", _ce)

        # ── Entity nodes ──────────────────────────────────────────────────────
        entity_nodes: Dict[str, Any] = {}
        for triple in triples:
            for name, etype, cid, ent_props_json in (
                (triple.subject, triple.subject_type, triple.chunk_id,
                 getattr(triple, "subject_properties_json", "{}")),
                (triple.obj,     triple.obj_type,     triple.chunk_id,
                 getattr(triple, "obj_properties_json", "{}")),
            ):
                if name and name not in entity_nodes:
                    props = {"ref_doc_id": doc_id, "doc_id": doc_id}
                    if cid:
                        props[TRIPLET_SOURCE_KEY] = cid  # → MENTIONS edge in Neo4j
                    # Ontology-declared entity properties (TIME, NOTE, …).  The
                    # `not in entity_nodes` guard above already makes this
                    # first-occurrence-wins, matching how a conflicting entity
                    # TYPE is resolved.  getattr keeps older stored rows working.
                    props.update(_parse_entity_props(ent_props_json))
                    en = EntityNode(
                        name=name,
                        label=etype or "Entity",
                        properties=props,
                    )
                    # Set embedding so Neo4j stores the vector (enables pg_vector_search)
                    if entity_embeddings:
                        emb = entity_embeddings.get(name)
                        if emb:
                            en.embedding = emb
                    entity_nodes[name] = en

        # ── Relations ─────────────────────────────────────────────────────────
        rels: List[Any] = []
        for triple in triples:
            rel_props: Dict[str, Any] = {}
            try:
                rel_props = json.loads(triple.properties_json)
            except (json.JSONDecodeError, TypeError):
                pass
            rel_props["ref_doc_id"] = doc_id
            rels.append(Relation(
                source_id=triple.subject,
                target_id=triple.obj,
                label=triple.predicate.upper().replace(" ", "_"),
                properties=rel_props,
            ))

        # ── Upsert: ChunkNodes first, then EntityNodes, then Relations ─────────
        _upsert_nodes = getattr(self._adapter, "upsert_nodes", None)
        _upsert_rels = getattr(self._adapter, "upsert_relations", None)
        if _upsert_nodes is not None:
            if chunk_nodes:
                _upsert_nodes(chunk_nodes)
            if entity_nodes:
                _upsert_nodes(list(entity_nodes.values()))
        if _upsert_rels is not None and rels:
            _upsert_rels(rels)
        logger.info(
            "FlexiblePropertyGraph (LI): doc=%s chunks=%d entities=%d rels=%d embeddings=%d",
            doc_id, len(chunk_nodes or chunks), len(entity_nodes), len(rels),
            len(entity_embeddings) if entity_embeddings else 0,
        )

    async def _write_triples_langchain(
        self,
        doc_id: str,
        triples: List[KGTripleRow],
        chunks: Optional[List[ChunkRow]] = None,
    ) -> None:
        """Write triples via a LangChain property graph adapter.

        Triples are grouped by ``chunk_id`` so each source chunk becomes a
        separate ``Document`` node in the graph (matching the standard
        ``ingest_lc_graph.py`` behaviour where ``LLMGraphTransformer`` produces
        one ``GraphDocument`` per source ``LCDoc``).  Chunk text is used as
        ``page_content`` so the ``Document`` node carries actual content rather
        than an empty string.
        """
        if not triples:
            return
        try:
            import inspect as _inspect
            from collections import defaultdict

            from langchain_community.graphs.graph_document import (  # type: ignore[import-untyped]
                GraphDocument, Node as GNode, Relationship as GRel,
            )
            from langchain_core.documents import Document as LCDoc  # type: ignore[import-untyped]

            # Build chunk_id → chunk_text lookup from the ChunkRow list.
            chunk_text_map: Dict[str, str] = {}
            if chunks:
                for c in chunks:
                    chunk_text_map[c.chunk_id] = c.chunk_text

            # Group triples by their source chunk so we create one GraphDocument
            # per chunk (giving Neo4j one Document node per chunk, not one for
            # the whole file).  Triples with no chunk_id fall back to doc_id.
            triples_by_chunk: Dict[str, List[KGTripleRow]] = defaultdict(list)
            for triple in triples:
                triples_by_chunk[triple.chunk_id or doc_id].append(triple)

            # ── Build one GraphDocument per chunk ─────────────────────────────
            # Entity nodes are globally de-duplicated across chunks: the first
            # occurrence wins (same entity extracted from two chunks keeps the
            # type from the first chunk).  Neo4j MERGE semantics mean later
            # chunks only add new relationships without duplicating nodes.
            global_seen_nodes: Dict[str, Any] = {}
            graph_docs: List[Any] = []
            source_docs: Dict[str, Any] = {}  # chunk_id → LCDoc

            for cid, chunk_triples in triples_by_chunk.items():
                chunk_seen_nodes: Dict[str, Any] = {}
                chunk_g_rels: List[Any] = []

                for triple in chunk_triples:
                    # Resolve subject node — prefer an already-seen node so
                    # that two chunks mentioning the same entity share the same
                    # GNode object (consistent type label).
                    if triple.subject not in global_seen_nodes:
                        _sp = {"ref_doc_id": doc_id, "doc_id": doc_id}
                        # Ontology-declared entity properties; first chunk to
                        # mention the entity wins, same as the type label above.
                        _sp.update(_parse_entity_props(
                            getattr(triple, "subject_properties_json", "{}")))
                        global_seen_nodes[triple.subject] = GNode(
                            id=triple.subject,
                            type=triple.subject_type or "Entity",
                            properties=_sp,
                        )
                    chunk_seen_nodes[triple.subject] = global_seen_nodes[triple.subject]

                    if triple.obj not in global_seen_nodes:
                        _op = {"ref_doc_id": doc_id, "doc_id": doc_id}
                        _op.update(_parse_entity_props(
                            getattr(triple, "obj_properties_json", "{}")))
                        global_seen_nodes[triple.obj] = GNode(
                            id=triple.obj,
                            type=triple.obj_type or "Entity",
                            properties=_op,
                        )
                    chunk_seen_nodes[triple.obj] = global_seen_nodes[triple.obj]

                    rel_props: Dict[str, Any] = {}
                    try:
                        rel_props = json.loads(triple.properties_json)
                    except (json.JSONDecodeError, TypeError):
                        pass
                    rel_props["ref_doc_id"] = doc_id
                    chunk_g_rels.append(GRel(
                        source=chunk_seen_nodes[triple.subject],
                        target=chunk_seen_nodes[triple.obj],
                        type=triple.predicate.upper().replace(" ", "_"),
                        properties=rel_props,
                    ))

                chunk_text = chunk_text_map.get(cid, "")
                source_doc = LCDoc(
                    page_content=chunk_text,
                    metadata={
                        "doc_id": doc_id,
                        "ref_doc_id": doc_id,
                        "file_name": chunk_triples[0].file_name,
                        "chunk_id": cid,
                    },
                )
                source_docs[cid] = source_doc
                graph_docs.append(GraphDocument(
                    nodes=list(chunk_seen_nodes.values()),
                    relationships=chunk_g_rels,
                    source=source_doc,
                ))

            # Same store-aware sanitisation the default pipeline applies before
            # add_graph_documents -- LLM-produced ids/labels reach the store's
            # query text verbatim, and some stores reject characters the model
            # emits (see langchain/graph/id_sanitizer.py).
            try:
                from langchain.graph.id_sanitizer import (  # noqa: PLC0415
                    sanitize_graph_documents,
                )
                sanitize_graph_documents(
                    graph_docs, str(getattr(self.app_config, "pg_graph_db", "")),
                )
            except Exception as _san_exc:  # noqa: BLE001 - never block ingest
                logger.debug("FlexiblePropertyGraph: id sanitiser skipped: %s", _san_exc)

            adapter = self._adapter
            lc_graph = _resolve_lc_graph(adapter) if adapter is not None else None
            wrote = False

            def _get_entity_label_kwarg(fn) -> Dict[str, Any]:
                """Inspect *fn* for the base-entity-label parameter name."""
                try:
                    sig = _inspect.signature(fn)
                    if "base_entity_label" in sig.parameters:
                        return {"base_entity_label": True}
                    if "baseEntityLabel" in sig.parameters:
                        return {"baseEntityLabel": True}
                except (AttributeError, ValueError):
                    pass
                return {}

            # Prefer store-adapter override (FalkorDB, Memgraph, Nebula, …) —
            # same precedence as ingest/ingest_lc_graph.py.
            _add_adapter = getattr(adapter, "add_graph_documents", None)
            if callable(_add_adapter):
                _kw = _get_entity_label_kwarg(_add_adapter)
                await asyncio.to_thread(_add_adapter, graph_docs, include_source=True, **_kw)
                wrote = True
            elif lc_graph is not None:
                _add = getattr(lc_graph, "add_graph_documents", None)
                if callable(_add):
                    _kw = _get_entity_label_kwarg(_add)
                    await asyncio.to_thread(
                        _add,
                        graph_docs,
                        include_source=True,
                        **_kw,
                    )
                    wrote = True

            if not wrote:
                logger.warning(
                    "FlexiblePropertyGraph (LC): no add_graph_documents path for doc '%s' "
                    "(adapter=%s, lc_graph=%s) — graph write skipped",
                    doc_id,
                    type(adapter).__name__ if adapter is not None else None,
                    type(lc_graph).__name__ if lc_graph is not None else None,
                )
                return

            # Stamp ref_doc_id onto the Document (source) nodes written by
            # include_source=True so the delete Cypher can find them by doc_id.
            if lc_graph is not None and hasattr(lc_graph, "query"):
                for src_doc in source_docs.values():
                    _src_id = getattr(src_doc, "id", None)
                    if _src_id:
                        try:
                            await asyncio.to_thread(
                                lc_graph.query,
                                "MATCH (d:Document {id: $did}) "
                                "SET d.ref_doc_id = $rid, d.doc_id = $rid",
                                params={"did": _src_id, "rid": doc_id},
                            )
                        except Exception as stamp_exc:
                            logger.debug(
                                "FlexiblePropertyGraph (LC): Document ref_doc_id stamp failed: %s",
                                stamp_exc,
                            )

            _normalize = getattr(adapter, "normalize_entity_names", None)
            if callable(_normalize):
                await asyncio.to_thread(_normalize)

            # For Neo4j: ensure the VECTOR INDEX DDL exists and populate
            # embeddings on newly written __Entity__ nodes.  Mirrors what
            # ingest_lc_graph.py does in aingest_lc_graph / aingest_li_to_lc_graph.
            if lc_graph is not None and "Neo4j" in type(lc_graph).__name__:
                try:
                    from cocoindex_integration.pipeline.env_config import _load_app_settings
                    from ingest.ingest_lc_graph import (  # noqa: PLC0415
                        _ensure_lc_vector_index_ddl,
                        _populate_neo4j_embeddings,
                    )
                    _cfg = _load_app_settings()
                    if getattr(_cfg, "langchain_pg_vector_search", False):
                        await asyncio.to_thread(_ensure_lc_vector_index_ddl, lc_graph, _cfg)
                    await asyncio.to_thread(_populate_neo4j_embeddings, None, lc_graph, _cfg)
                except Exception as _idx_exc:
                    logger.warning(
                        "FlexiblePropertyGraph (LC): Neo4j index/embedding setup failed "
                        "(non-fatal): %s",
                        _idx_exc,
                    )

            total_nodes = len(global_seen_nodes)
            total_rels = sum(len(gd.relationships) for gd in graph_docs)
            logger.info(
                "FlexiblePropertyGraph (LC): wrote %d triple(s), %d node(s), %d rel(s), "
                "%d chunk doc(s) for doc %s",
                len(triples), total_nodes, total_rels, len(graph_docs), doc_id,
            )
        except Exception as exc:
            logger.error("FlexiblePropertyGraph: write failed for doc '%s': %s", doc_id, exc)
            raise


# ─────────────────────────────────────────────────────────────────────────────
# CocoIndex TargetHandler for property graph stores
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class _FilePGSpec:
    """All property-graph rows (triples + chunk nodes) for one source document."""
    doc_id: str
    triples: List[KGTripleRow]
    chunks: List[ChunkRow]


@dataclass(frozen=True)
class _FilePGTrackingRecord:
    """SHA-256 fingerprint of a document's chunk + triple content (change detection)."""
    fingerprint: bytes


class _FilePGAction(NamedTuple):
    """Action produced by reconcile() and consumed by _apply_actions()."""
    doc_id: str
    spec: Optional[_FilePGSpec]  # None → delete; non-None → upsert
    delete_first: bool = False   # True when modifying existing doc (purge stale nodes first)


def _pg_fingerprint(triples: List[KGTripleRow], chunks: List[ChunkRow]) -> bytes:
    """Stable SHA-256 over sorted chunk texts + sorted triple content."""
    def _units():
        for chunk in sorted(chunks, key=lambda c: c.chunk_index):
            yield chunk.chunk_text.encode("utf-8", errors="replace")
        for t in sorted(triples, key=lambda t: (t.subject, t.predicate, t.obj)):
            yield (
                # Entity properties are part of the desired state, so they must
                # be in the fingerprint — otherwise editing only a property
                # (same triple, new TIME) looks unchanged and is never written.
                f"{t.subject}|{t.predicate}|{t.obj}|{t.properties_json}"
                f"|{getattr(t, 'subject_properties_json', '')}"
                f"|{getattr(t, 'obj_properties_json', '')}".encode(
                    "utf-8", errors="replace"
                )
            )
    return content_fingerprint(_units())


class FlexiblePropertyGraphHandler(FlexibleReconcileHandler):
    """CocoIndex ``TargetHandler`` for flexible-graphrag property graph stores."""

    label = "PG data"

    def _fingerprint(self, desired: Any) -> bytes:
        return _pg_fingerprint(desired.triples, desired.chunks)

    def _make_delete_action(self, key: str) -> _FilePGAction:
        return _FilePGAction(doc_id=key, spec=None)

    def _make_upsert_action(self, desired: Any, delete_first: bool) -> _FilePGAction:
        return _FilePGAction(doc_id=desired.doc_id, spec=desired, delete_first=delete_first)

    def _make_tracking_record(self, fp: bytes) -> _FilePGTrackingRecord:
        return _FilePGTrackingRecord(fingerprint=fp)

    def _action_is_delete(self, action: Any) -> bool:
        return action.spec is None

    def _action_size(self, action: Any) -> str:
        return f"{len(action.spec.triples)} triple(s), {len(action.spec.chunks)} chunk(s)"

    async def _declare_upsert(self, action: Any) -> None:
        for chunk in action.spec.chunks:
            await self._target.declare_chunk_node(chunk)
        for triple in action.spec.triples:
            await self._target.declare_row(triple)


# Backwards-compatible alias (former public handler name).
FlexiblePGHandler = FlexiblePropertyGraphHandler
