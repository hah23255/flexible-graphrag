"""``FlexibleSearch`` — CocoIndex target for flexible-graphrag full-text search stores.

Supported: Elasticsearch, OpenSearch, BM25 (in-memory / LlamaIndex or LangChain).
CocoIndex does not natively include these search stores, so this connector wraps
flexible-graphrag's SearchStoreAdapter.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Optional

from cocoindex_integration.connectors.rows import SearchRow
from cocoindex_integration.connectors.flexible.base import (
    FlexibleConnector,
    FlexibleReconcileHandler,
    content_fingerprint,
)

logger = logging.getLogger(__name__)


class FlexibleSearch(FlexibleConnector):
    """Custom CocoIndex target backed by flexible-graphrag's search adapters.

    Supports: ``"elasticsearch"``, ``"opensearch"``, ``"bm25"``.  The active
    store and its config are derived from ``app_config`` so the pipeline pickers
    can construct it uniformly with the other flexible targets.
    """

    def __init__(self, app_config) -> None:
        super().__init__(app_config)
        self.store_type: str = str(getattr(app_config, "search_db", "none"))
        raw_cfg = getattr(app_config, "search_db_config", None)
        self.store_config: Dict[str, Any] = (raw_cfg if isinstance(raw_cfg, dict) else {}) or {}
        self._adapter = None
        self._pending: List[SearchRow] = []

    async def setup(self) -> None:
        if self._adapter is not None:
            return  # idempotent — already initialised; shared across parallel files
        from adapters.search.search_store_adapter import build_search_adapter
        self._adapter = build_search_adapter(
            db_type=self.store_type,
            config=self.store_config,
            app_config=self.app_config,
        )
        logger.info("FlexibleSearch: adapter ready for '%s'", self.store_type)

    async def declare_row(self, row: SearchRow) -> None:
        self._pending.append(row)
        if len(self._pending) >= 200:
            await self._flush()

    async def _flush(self) -> None:
        if not self._pending or self._adapter is None:
            return
        rows = self._pending[:]
        self._pending.clear()
        try:
            store = self._adapter.get_store()
            if self._adapter.is_langchain():
                from langchain_core.documents import Document as LCDoc  # type: ignore[import-untyped]
                lc_docs = [
                    LCDoc(
                        page_content=r.text,
                        # Canonical metadata built ONCE upstream (reader + parse
                        # + provenance) — attached verbatim, no per-target rebuild.
                        metadata=dict(r.metadata or {}),
                    )
                    for r in rows
                ]
                _aadd = getattr(store, "aadd_documents", None)
                _add = getattr(store, "add_documents", None)
                if _aadd is not None:
                    await _aadd(lc_docs)
                elif _add is not None:
                    if asyncio.iscoroutinefunction(_add):
                        await _add(lc_docs)
                    else:
                        await asyncio.to_thread(_add, lc_docs)
            else:
                # LlamaIndex search store (Elasticsearch, OpenSearch, BM25 …)
                import uuid
                from llama_index.core.schema import (  # type: ignore[import-untyped]
                    TextNode, NodeRelationship, RelatedNodeInfo,
                )
                nodes = [
                    TextNode(
                        id_=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{r.doc_id}:{r.chunk_index}")),
                        text=r.text,
                        embedding=r.embedding or None,
                        # Canonical metadata built ONCE upstream (reader + parse
                        # + provenance) — attached verbatim, no per-target rebuild.
                        metadata=dict(r.metadata or {}),
                        # Set SOURCE relationship so node.ref_doc_id property
                        # returns r.doc_id — LlamaIndex ES stores read this
                        # property (not metadata["ref_doc_id"]) when indexing,
                        # and delete(ref_doc_id=...) queries that stored field.
                        relationships={
                            NodeRelationship.SOURCE: RelatedNodeInfo(node_id=r.doc_id),
                        },
                    )
                    for r in rows
                ]
                # Strategy:
                # 1. BM25 adapter → add_nodes() is pure-Python sync, safe for asyncio.to_thread
                # 2. ES / OS stores → use async_add() directly from this async method.
                #    DO NOT wrap in asyncio.to_thread — the LlamaIndex ES store calls
                #    asyncio.get_event_loop() internally, which raises RuntimeError inside
                #    a thread that has no event loop.
                # 3. Fallback to adapter's add() / store's add(); await if coroutine else thread.
                #
                # LlamaIndexSearchAdapter.__getattr__ delegates unknown attrs to the underlying
                # store, so getattr(self._adapter, "async_add") reaches ElasticsearchStore.async_add
                # without needing to unwrap via get_store() separately.
                _add_nodes = getattr(self._adapter, "add_nodes", None)
                _async_add = getattr(self._adapter, "async_add", None)
                _add_fn = getattr(self._adapter, "add", None)

                if _add_nodes is not None:
                    # BM25 — pure sync, no asyncio inside
                    await asyncio.to_thread(_add_nodes, nodes)
                elif _async_add is not None:
                    # LlamaIndex ES/OS async method — await directly in current loop
                    await _async_add(nodes)
                elif _add_fn is not None:
                    if asyncio.iscoroutinefunction(_add_fn):
                        await _add_fn(nodes)
                    else:
                        # Generic sync fallback — may fail if store uses asyncio internally
                        await asyncio.to_thread(_add_fn, nodes)
                else:
                    logger.warning(
                        "FlexibleSearch: no add/add_nodes/async_add method on %s — "
                        "search data not written to store",
                        type(self._adapter).__name__,
                    )
        except Exception as exc:
            logger.error("FlexibleSearch: write failed: %s", exc)
            raise

    async def delete_row(self, doc_id: str) -> None:
        """Delete all search documents for *doc_id*.

        Tries async delete first, then sync delete in a thread.
        LlamaIndex ES/OS: ``delete(ref_doc_id)`` on the underlying store works
        correctly because ``_flush()`` sets ``node.ref_doc_id`` via the
        ``relationships`` dict so the field is properly indexed in ES/OS.
        """
        if self._adapter is None:
            return
        try:
            _adelete = getattr(self._adapter, "adelete", None)
            _delete = getattr(self._adapter, "delete", None)
            if _adelete is not None:
                await _adelete(doc_id)
                logger.info(
                    "FlexibleSearch: deleted search docs for '%s' via adelete", doc_id
                )
            elif _delete is not None:
                if asyncio.iscoroutinefunction(_delete):
                    await _delete(doc_id)
                else:
                    await asyncio.to_thread(_delete, doc_id)
                logger.info(
                    "FlexibleSearch: deleted search docs for '%s' via delete", doc_id
                )
            else:
                logger.warning(
                    "FlexibleSearch: no delete method on adapter '%s' for doc '%s'",
                    type(self._adapter).__name__, doc_id,
                )
        except Exception as exc:
            logger.error("FlexibleSearch: delete failed for '%s': %s", doc_id, exc)

    async def finalize(self) -> None:
        await self._flush()

    async def teardown(self) -> None:
        self._adapter = None


# ─────────────────────────────────────────────────────────────────────────────
# CocoIndex TargetHandler for full-text search stores
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class _FileSearchSpec:
    """All search rows that should exist for one source document."""
    doc_id: str
    rows: List[SearchRow]


@dataclass(frozen=True)
class _FileSearchTrackingRecord:
    """SHA-256 fingerprint of a document's chunk text content (change detection)."""
    fingerprint: bytes


class _FileSearchAction(NamedTuple):
    """Action produced by reconcile() and consumed by _apply_actions()."""
    doc_id: str
    rows: Optional[List[SearchRow]]  # None → delete; non-None → upsert
    delete_first: bool = False        # True when modifying existing doc


def _search_fingerprint(rows: List[SearchRow]) -> bytes:
    """Stable SHA-256 over ordered chunk texts."""
    return content_fingerprint(
        row.text.encode("utf-8", errors="replace")
        for row in sorted(rows, key=lambda r: r.chunk_index)
    )


class FlexibleSearchHandler(FlexibleReconcileHandler):
    """CocoIndex ``TargetHandler`` for flexible-graphrag full-text search stores."""

    label = "search data"

    def _fingerprint(self, desired: Any) -> bytes:
        return _search_fingerprint(desired.rows)

    def _make_delete_action(self, key: str) -> _FileSearchAction:
        return _FileSearchAction(doc_id=key, rows=None)

    def _make_upsert_action(self, desired: Any, delete_first: bool) -> _FileSearchAction:
        return _FileSearchAction(doc_id=desired.doc_id, rows=desired.rows, delete_first=delete_first)

    def _make_tracking_record(self, fp: bytes) -> _FileSearchTrackingRecord:
        return _FileSearchTrackingRecord(fingerprint=fp)

    def _action_is_delete(self, action: Any) -> bool:
        return action.rows is None

    def _action_size(self, action: Any) -> str:
        return f"{len(action.rows)} chunk(s)"

    async def _declare_upsert(self, action: Any) -> None:
        for row in action.rows:
            await self._target.declare_row(row)
