"""``FlexibleVector`` — CocoIndex target backed by flexible-graphrag vector adapters.

Used inside the CocoIndex pipeline (``PIPELINE_BACKEND=cocoindex``) for any
vector store that does NOT have a native CocoIndex connector.  Activation:

* ``VECTOR_BACKEND=llamaindex`` or ``VECTOR_BACKEND=langchain`` (default) →
  this class is **not used**; the normal LI/LC adapter pipeline handles vectors
  directly (``ingest/update_vector.py``), bypassing CocoIndex entirely.

* ``PIPELINE_BACKEND=cocoindex`` with a non-native store (e.g. Elasticsearch,
  OpenSearch, Weaviate, Milvus, Chroma) → ``app.py._pick_vector_target``
  returns a ``FlexibleVector`` instance as the CocoIndex write target.

* ``PIPELINE_BACKEND=cocoindex`` with a CocoIndex-native store (Qdrant,
  LanceDB, Postgres/pgvector) → ``app.py`` uses the ``connectors.cocoindex``
  family (``CocoQdrant`` etc.) instead; this class is **not used**.

See: https://cocoindex.io/docs/advanced_topics/custom_target_connector/
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Optional

from cocoindex_integration.connectors.rows import VectorRow
from cocoindex_integration.connectors.flexible.base import (
    FlexibleConnector,
    FlexibleReconcileHandler,
    content_fingerprint,
    resolve_cocoindex_dim,
    resolve_main_dim,
    vector_backend,
)

logger = logging.getLogger(__name__)


class FlexibleVector(FlexibleConnector):
    """Custom CocoIndex target backed by flexible-graphrag's vector adapters.

    Used when ``VECTOR_BACKEND=cocoindex``.  Supports stores that CocoIndex
    does not natively provide: Elasticsearch, OpenSearch, Weaviate, Milvus,
    Chroma, and any other store in flexible-graphrag's adapter registry.
    """

    def __init__(self, app_config, embedding_dim: int = 1536) -> None:
        super().__init__(app_config)
        self.embedding_dim = embedding_dim
        self._adapter: Optional[Any] = None
        self._pending: List[VectorRow] = []
        self._setup_event: Optional[asyncio.Event] = None  # guards concurrent setup calls
        self._embed_model: Optional[Any] = None            # cached main-pipeline embed model
        # _li_managed controls whether CocoIndex's pre-computed embeddings are passed
        # through or discarded so that LI/LC re-embeds with its own model.
        #
        # True  → discard CocoIndex embeddings; LI/LC re-embeds (safe when dims differ)
        # False → forward CocoIndex embeddings directly (avoids double-embedding when dims match)
        #
        # Dimension-aware: if CocoIndex and the main pipeline produce the same dimension
        # (e.g. both EMBEDDING_KIND=huggingface → 384-dim), there is no mismatch and we
        # pass embeddings through directly.  Only set True when the dims genuinely differ
        # (e.g. COCOINDEX_EMBEDDING_KIND=sentence_transformer at 384 vs EMBEDDING_KIND=openai
        # at 1536) — in that case LI must re-embed to match the collection's index dimension.
        _is_li_lc = vector_backend() in ("llamaindex", "langchain")
        if _is_li_lc:
            try:
                _coco_dim = resolve_cocoindex_dim()
                _main_dim = resolve_main_dim()
                self._li_managed = (_coco_dim != _main_dim)
                if self._li_managed:
                    logger.info(
                        "FlexibleVector: dim mismatch (CocoIndex=%d, main=%d) "
                        "— LI will re-embed with EMBEDDING_KIND='%s'",
                        _coco_dim, _main_dim, os.getenv("EMBEDDING_KIND", "openai"),
                    )
                else:
                    logger.info(
                        "FlexibleVector: dims match (%d) "
                        "— CocoIndex embeddings forwarded directly (no double-embedding)",
                        _coco_dim,
                    )
            except Exception as _exc:
                logger.warning(
                    "FlexibleVector: dim comparison failed (%s) — defaulting to "
                    "_li_managed=True (safe fallback, LI will re-embed)",
                    _exc,
                )
                self._li_managed = True
        else:
            self._li_managed = False

    # ------------------------------------------------------------------
    # CocoIndex lifecycle hooks
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        # Fast path — already initialised (singleton shared across parallel files).
        if self._adapter is not None:
            return

        # If another coroutine is already initialising, wait for it then return.
        if self._setup_event is not None:
            await self._setup_event.wait()
            return

        # This is the first coroutine to call setup(); we own initialisation.
        self._setup_event = asyncio.Event()
        try:
            db_type = str(getattr(self.app_config, "vector_db", "none"))
            config: Dict[str, Any] = getattr(self.app_config, "vector_db_config", {}) or {}
            app_config = self.app_config
            backend = vector_backend()

            # Prefer the live HybridSearchSystem vector adapter when present.
            # A second Milvus/Weaviate/etc. client instance has its own field/
            # collection cache; writes via that instance leave the search-side
            # adapter stale (Milvus KeyError 'text' on similarity_search).
            reused = self._try_reuse_system_adapter(db_type, backend)
            if reused is not None:
                self._adapter = reused
                logger.info(
                    "FlexibleVector: reusing HybridSearchSystem vector adapter for '%s'",
                    db_type,
                )
            else:
                # Run heavy import + adapter creation in a thread so the event loop
                # stays responsive while LlamaIndex / PyTorch initialise (~27 s).
                def _build() -> Any:
                    from adapters.vector.vector_store_adapter import (  # noqa: PLC0415
                        build_vector_adapter,
                    )
                    return build_vector_adapter(
                        db_type=db_type,
                        config=config,
                        app_config=app_config,
                        vector_backend=backend,
                    )

                self._adapter = await asyncio.to_thread(_build)
                logger.info("FlexibleVector: adapter ready for '%s'", db_type)
        finally:
            self._setup_event.set()  # wake up any waiters even on exception

    @staticmethod
    def _try_reuse_system_adapter(db_type: str, backend: str) -> Optional[Any]:
        """Return ``system.vector_store`` when it matches this FlexibleVector target."""
        try:
            from backend import get_backend  # noqa: PLC0415

            system = getattr(get_backend(), "_system", None)
            if system is None:
                return None
            adapter = getattr(system, "vector_store", None)
            if adapter is None:
                return None
            cfg = getattr(system, "config", None)
            if cfg is None:
                return None
            sys_db = str(getattr(cfg, "vector_db", "none")).lower()
            sys_backend = str(
                getattr(cfg, "vector_backend", None) or vector_backend()
            ).lower()
            if sys_db != str(db_type).lower() or sys_backend != str(backend).lower():
                return None
            return adapter
        except Exception as exc:
            logger.debug("FlexibleVector: could not reuse system adapter: %s", exc)
            return None

    async def declare_row(self, row: VectorRow) -> None:
        self._pending.append(row)
        if len(self._pending) >= 100:
            await self._flush()

    async def finalize(self) -> None:
        await self._flush()

    async def teardown(self) -> None:
        self._adapter = None

    async def delete_row(self, doc_id: str) -> None:
        """Delete all vectors for *doc_id* (called by CocoIndex on stale rows)."""
        if self._adapter is None:
            return
        try:
            _delete = getattr(self._adapter, "delete", None)
            if _delete is not None:
                await asyncio.to_thread(_delete, doc_id)
        except Exception as exc:
            logger.error("FlexibleVector: delete failed for '%s': %s", doc_id, exc)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_embed_model(self) -> Any:
        """Return (and cache) the main pipeline's LI embedding model.

        Used when ``_li_managed=True`` (dim mismatch): CocoIndex embeddings cannot
        be forwarded, so we re-embed with ``EMBEDDING_KIND`` before calling
        ``insert_nodes``.  The model is created once and reused across flushes.
        """
        if self._embed_model is not None:
            return self._embed_model
        from llamaindex.llm.embedding_factory import create_embedding_model  # noqa: PLC0415
        from config import LLMProvider, Settings  # noqa: PLC0415
        _settings = Settings()
        _kind = (os.getenv("EMBEDDING_KIND") or "openai").lower()
        try:
            _prov = LLMProvider(_kind)
        except ValueError:
            _prov = LLMProvider.OPENAI
        self._embed_model = await asyncio.to_thread(create_embedding_model, _prov, {}, _settings)
        return self._embed_model

    async def _re_embed_nodes(self, nodes: List[Any]) -> None:
        """Embed *nodes* with the main pipeline's EMBEDDING_KIND model in-place.

        Called when ``_li_managed=True``: CocoIndex produced embeddings at a
        different dimension, so we must re-embed with the main pipeline's model
        before writing to the vector store (which was created with that dimension).
        """
        embed_model = await self._get_embed_model()
        texts = [n.get_content() for n in nodes]
        embeddings = await asyncio.to_thread(
            embed_model.get_text_embedding_batch, texts, False
        )
        for node, emb in zip(nodes, embeddings):
            node.embedding = list(emb)
        logger.info(
            "FlexibleVector: re-embedded %d node(s) with EMBEDDING_KIND='%s'",
            len(nodes), os.getenv("EMBEDDING_KIND", "openai"),
        )

    async def _re_embed_row_dicts(self, row_dicts: List[dict]) -> None:
        """Embed *row_dicts* with the main pipeline's EMBEDDING_KIND model in-place.

        LC path equivalent of ``_re_embed_nodes``.
        """
        embed_model = await self._get_embed_model()
        texts = [d.get("text", "") for d in row_dicts]
        embeddings = await asyncio.to_thread(
            embed_model.get_text_embedding_batch, texts, False
        )
        for d, emb in zip(row_dicts, embeddings):
            d["embedding"] = list(emb)
        logger.info(
            "FlexibleVector: re-embedded %d row(s) with EMBEDDING_KIND='%s'",
            len(row_dicts), os.getenv("EMBEDDING_KIND", "openai"),
        )

    async def _flush(self) -> None:
        if not self._pending or self._adapter is None:
            return
        rows, self._pending = self._pending, []
        try:
            if getattr(self._adapter, "is_langchain", lambda: False)():
                # LC path — pass rows as plain dicts; no LI TextNode created.
                # The CocoIndex row-ingestion contract lives in the connector
                # layer (_vector_writer) so the LC adapters stay framework-neutral.
                import dataclasses
                from cocoindex_integration.connectors.flexible._vector_writer import (
                    write_rows_langchain,
                )
                row_dicts = [dataclasses.asdict(row) for row in rows]
                if self._li_managed:
                    # Dim mismatch: re-embed with the main pipeline's model so the
                    # LC adapter receives correctly-dimensioned vectors.
                    await self._re_embed_row_dicts(row_dicts)
                await write_rows_langchain(self._adapter, row_dicts)
            else:
                # LI path — convert to TextNode then embed if needed.
                nodes = self._rows_to_nodes(rows)
                if self._li_managed:
                    # Dim mismatch: store.add() requires embeddings to be pre-set;
                    # it does NOT call the embedding model internally.
                    await self._re_embed_nodes(nodes)
                await self._adapter.insert_nodes(nodes)
        except Exception as exc:
            logger.error("FlexibleVector: write batch failed: %s", exc)
            raise

    def _rows_to_nodes(self, rows: List[VectorRow]) -> List[Any]:
        """Convert VectorRow → LlamaIndex TextNode (LI backend path only).

        Uses ``relationships[NodeRelationship.SOURCE]`` to set ``ref_doc_id`` —
        the read-only @property on BaseNode reads from there, NOT from a
        constructor kwarg.  The Qdrant (and other LI) stores write ``doc_id``
        payload from ``node.ref_doc_id``, so this must be correct for deletion
        to work: ``store.delete(ref_doc_id)`` filters on that payload field.

        When ``self._li_managed`` is True (dim mismatch), nodes are returned with
        ``embedding=None``.  ``_flush`` calls ``_re_embed_nodes`` immediately after
        to set correct-dimension embeddings before ``insert_nodes``.

        When ``self._li_managed`` is False (dims match), CocoIndex embeddings are
        forwarded directly — no second API call needed.
        """
        import uuid
        from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode

        return [
            TextNode(
                id_=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{row.doc_id}:{row.chunk_index}")),
                text=row.text,
                # None  → _flush will call _re_embed_nodes before insert_nodes
                # list  → CocoIndex dims match; forwarded directly (no second API call)
                embedding=None if self._li_managed else (row.embedding or None),
                relationships={
                    NodeRelationship.SOURCE: RelatedNodeInfo(node_id=row.doc_id),
                },
                # Canonical metadata built ONCE upstream (reader + parse +
                # provenance) — attached verbatim; no per-target rebuild.
                metadata=dict(row.metadata or {}),
            )
            for row in rows
        ]


# ─────────────────────────────────────────────────────────────────────────────
# CocoIndex TargetHandler for vector stores
#
# CocoIndex tracks *declared* target states across update cycles (LMDB-backed).
# When a source file disappears, CocoIndex passes NON_EXISTENCE to reconcile(),
# triggering an automatic delete — no custom tracking file needed.
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class _FileVectorSpec:
    """All vector rows that should exist for one source document."""
    doc_id: str
    rows: List[VectorRow]


@dataclass(frozen=True)
class _FileVectorTrackingRecord:
    """SHA-256 fingerprint of a document's text content (change detection)."""
    fingerprint: bytes


class _FileVectorAction(NamedTuple):
    """Action produced by reconcile() and consumed by _apply_actions()."""
    doc_id: str
    rows: Optional[List[VectorRow]]  # None → delete; non-None → upsert
    delete_first: bool = False       # True when modifying existing doc (purge old chunks first)


def _text_fingerprint(rows: List[VectorRow]) -> bytes:
    """Stable SHA-256 over ordered chunk texts — changes when content changes."""
    return content_fingerprint(
        row.text.encode("utf-8", errors="replace") for row in rows
    )


class FlexibleVectorHandler(FlexibleReconcileHandler):
    """CocoIndex ``TargetHandler`` for flexible-graphrag vector stores."""

    label = "vectors"

    def _fingerprint(self, desired: Any) -> bytes:
        return _text_fingerprint(desired.rows)

    def _make_delete_action(self, key: str) -> _FileVectorAction:
        return _FileVectorAction(doc_id=key, rows=None)

    def _make_upsert_action(self, desired: Any, delete_first: bool) -> _FileVectorAction:
        return _FileVectorAction(doc_id=desired.doc_id, rows=desired.rows, delete_first=delete_first)

    def _make_tracking_record(self, fp: bytes) -> _FileVectorTrackingRecord:
        return _FileVectorTrackingRecord(fingerprint=fp)

    def _action_is_delete(self, action: Any) -> bool:
        return action.rows is None

    def _action_size(self, action: Any) -> str:
        return f"{len(action.rows)} chunk(s)"

    async def _declare_upsert(self, action: Any) -> None:
        for row in action.rows:
            await self._target.declare_row(row)
