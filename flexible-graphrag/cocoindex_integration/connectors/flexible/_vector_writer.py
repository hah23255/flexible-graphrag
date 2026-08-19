"""CocoIndex-aware vector row writer (connector layer).

This module owns the **CocoIndex row-ingestion contract** for LangChain-backed
vector stores.  It was moved OUT of the framework-neutral adapters
(``adapters/vector``, ``langchain/vector``, ``llamaindex/vector``) so those
adapters stay framework-neutral and free of CocoIndex-specific concerns.

Only the CocoIndex pipeline (``FlexibleVector._flush``) uses this writer.  The
non-CocoIndex ingestion pipeline (``ingest/update_vector.py``) still uses the
adapters' ``insert_nodes`` (LI) / ``add_documents`` (LC) directly.

Write strategy (no re-embed preserved)
--------------------------------------
Given pre-embedded ``VectorRow`` dicts and a LangChain-backed adapter:

1. **Qdrant raw-client fast path** — when the adapter exposes a raw
   ``qdrant_client`` and the rows carry embeddings, upsert ``PointStruct``
   objects with deterministic UUIDv5 point IDs (bypasses the LC store layer).
2. **Generic pre-computed path** — ``add_embeddings`` / ``aadd_embeddings``
   (``(text, vector)`` pairs + metadatas) so the store stores the given vectors
   without a second embed API call.
3. **Re-embed fallback** — build LC ``Document`` objects and call
   ``add_documents`` so the store embeds the text itself (only when no
   embeddings are present or the store lacks a pre-computed API).

The canonical per-chunk ``metadata`` dict (built once upstream in
``_run_pipeline``) is attached verbatim — see :func:`_row_payload`.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


def _row_payload(row: dict) -> Tuple[str, Dict[str, Any]]:
    """Return ``(page_content, metadata)`` from a VectorRow dict.

    The CocoIndex pipeline builds each row's canonical ``metadata`` dict ONCE
    (reader + parse + provenance), so we attach it verbatim.  A minimal
    provenance fallback is used only when ``metadata`` is absent.
    """
    meta = row.get("metadata")
    if meta:
        return (row["text"], dict(meta))
    return (
        row["text"],
        {
            "doc_id": row["doc_id"],
            "ref_doc_id": row.get("ref_doc_id") or row["doc_id"],
            "file_name": row.get("file_name", ""),
            "file_path": row.get("file_path", ""),
            "source_type": row.get("source_type", "cocoindex"),
            "chunk_index": row.get("chunk_index", 0),
        },
    )


def _qdrant_client(adapter: Any) -> Any:
    """Return the raw Qdrant client if *adapter* is Qdrant-backed, else ``None``."""
    return getattr(adapter, "_qdrant_client", None)


async def _qdrant_upsert(adapter: Any, rows: List[dict]) -> None:
    """Upsert pre-embedded rows via the raw Qdrant client (fast path)."""
    from qdrant_client.models import PointStruct  # noqa: PLC0415

    collection_name = getattr(adapter, "_collection_name", None) or "hybrid_search"
    client = adapter._qdrant_client
    points = [
        PointStruct(
            id=str(uuid.uuid5(
                uuid.NAMESPACE_DNS,
                row.get("node_id") or f"{row['doc_id']}:{row.get('chunk_index', 0)}",
            )),
            vector=row["embedding"],
            # Qdrant wraps metadata under "page_content"/"metadata" keys to match
            # LangChain's internal payload convention.
            payload={"page_content": pc, "metadata": meta},
        )
        for row in rows
        for pc, meta in (_row_payload(row),)
    ]
    await asyncio.to_thread(client.upsert, collection_name=collection_name, points=points)
    logger.info(
        "coco vector writer: upserted %d point(s) to Qdrant '%s' (pre-computed embeddings)",
        len(points), collection_name,
    )


def _store_prefers_sync(store: Any) -> bool:
    """True when async add_* APIs are stubs that require an async engine.

    ``langchain_postgres.PGVector`` exposes ``aadd_embeddings`` / ``aadd_documents``
    but constructs with ``async_mode=False`` by default — calling the async
    methods raises ``_async_engine not found`` / sync-mode errors.
    """
    if getattr(store, "async_mode", None) is False:
        return True
    name = type(store).__name__
    if name in ("PGVector",):
        return True
    # Sync engine present, async engine absent → prefer sync path.
    if getattr(store, "_async_engine", None) is None and (
        getattr(store, "_engine", None) is not None
        or getattr(store, "engine", None) is not None
    ):
        return True
    return False


async def _try_precomputed(store: Any, rows: List[dict]) -> bool:
    """Try ``add_embeddings`` / ``aadd_embeddings`` with pre-computed vectors.

    Returns ``True`` on success, ``False`` when the store lacks a pre-computed
    insert API (caller falls back to re-embedding).
    """
    text_embeddings = [(r["text"], r["embedding"]) for r in rows]
    metadatas = [_row_payload(r)[1] for r in rows]

    aadd_emb = getattr(store, "aadd_embeddings", None)
    add_emb = getattr(store, "add_embeddings", None)
    prefer_sync = _store_prefers_sync(store)
    try:
        if prefer_sync and add_emb is not None:
            await asyncio.to_thread(add_emb, text_embeddings, metadatas=metadatas)
        elif not prefer_sync and aadd_emb is not None:
            await aadd_emb(text_embeddings, metadatas=metadatas)
        elif add_emb is not None:
            await asyncio.to_thread(add_emb, text_embeddings, metadatas=metadatas)
        elif aadd_emb is not None:
            await aadd_emb(text_embeddings, metadatas=metadatas)
        else:
            return False
    except (NotImplementedError, TypeError, AssertionError, ValueError, RuntimeError) as exc:
        logger.debug(
            "coco vector writer: pre-computed path failed on %s (%s) — will fall back",
            type(store).__name__, exc,
        )
        return False
    logger.info(
        "coco vector writer: upserted %d docs to %s (pre-computed embeddings via add_embeddings)",
        len(rows), type(store).__name__,
    )
    return True


async def _add_documents(store: Any, rows: List[dict]) -> None:
    """Re-embed fallback: build LC Documents and let the store embed them."""
    from langchain_core.documents import Document  # noqa: PLC0415

    lc_docs = [Document(page_content=pc, metadata=meta)
               for pc, meta in (_row_payload(r) for r in rows)]
    store_name = type(store).__name__
    aadd = getattr(store, "aadd_documents", None)
    add = getattr(store, "add_documents", None)
    prefer_sync = _store_prefers_sync(store)
    try:
        if prefer_sync and add is not None:
            await asyncio.to_thread(add, lc_docs)
        elif not prefer_sync and aadd is not None:
            await aadd(lc_docs)
        elif add is not None:
            await asyncio.to_thread(add, lc_docs)
        elif aadd is not None:
            await aadd(lc_docs)
        else:
            logger.warning("coco vector writer: %s has no add_documents — skipped", store_name)
            return
    except (AssertionError, ValueError, RuntimeError) as exc:
        # Sync-mode stores that still expose aadd_* can fail; retry sync once.
        if add is not None and aadd is not None:
            logger.debug(
                "coco vector writer: async add_documents failed on %s (%s) — retrying sync",
                store_name, exc,
            )
            await asyncio.to_thread(add, lc_docs)
        else:
            raise
    logger.info("coco vector writer: added %d docs to %s (re-embedded by store)", len(lc_docs), store_name)


async def write_rows_langchain(adapter: Any, rows: List[dict]) -> None:
    """Write pre-embedded ``VectorRow`` dicts to a LangChain-backed store.

    Encapsulates the CocoIndex row-ingestion contract that previously lived in
    ``langchain/vector`` adapters — keeping those adapters framework-neutral.
    """
    if not rows:
        return

    has_embeddings = bool(rows[0].get("embedding"))

    # 1. Qdrant raw-client fast path (pre-computed vectors).
    if has_embeddings and _qdrant_client(adapter) is not None:
        await _qdrant_upsert(adapter, rows)
        return

    store = adapter.get_store()

    # 2. Generic pre-computed path via add_embeddings.
    if has_embeddings and await _try_precomputed(store, rows):
        return

    # 3. Re-embed fallback.
    await _add_documents(store, rows)


__all__ = ["write_rows_langchain", "_row_payload"]
