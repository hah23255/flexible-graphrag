"""Lazy, change-aware ``LiveMapView`` for detector-backed flexible sources.

``FlexibleMapView`` adapts a flexible-graphrag change **detector** (from
``incremental_updates.detectors``) plus its ``sources/`` reader to CocoIndex's
:class:`~cocoindex.LiveMapView` protocol so it can be consumed by
``coco.mount_each``.

Two halves of the protocol
--------------------------
* ``__aiter__`` — the **scannable current state**.  Lists file *metadata* via
  ``detector.list_all_files()`` (no bytes downloaded) and yields
  ``(stable_key, FlexibleFile)`` pairs.  Each :class:`FlexibleFile` carries the
  listing metadata (size, modified, ``etag``/``ordinal`` fingerprint) and a
  lazy single-key download callable; CocoIndex only pulls bytes for files whose
  ``modified_time`` (then fingerprint) actually changed.
* ``watch`` — the **change stream**.  Runs an initial ``update_all()`` +
  ``mark_ready()`` (so CocoIndex reconciles the current scan, including deletes
  of files that vanished between runs), then forwards ``detector.get_changes()``
  events to the subscriber as ``update(key, file)`` / ``delete(key)``.

Design notes
------------
* One detector instance per view; started lazily and reused for both the scan
  and the watch stream.  A separate ``sources/`` instance owns the download
  connection (built once, reused for every file's ``read_file_bytes``).
* ``SingleWatcherGuard`` enforces the protocol's single-active-``watch()``
  contract.
* All bytes are lazy: the map view never holds more than one file's content at
  a time (CocoIndex reads them one at a time via ``FlexibleFile.read()``).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from cocoindex._internal.live_component import LiveMapSubscriber
from cocoindex.connectorkits import SingleWatcherGuard

from ._file import FlexibleFile
from ._sources import (
    DETECTOR_BACKED,
    FileRecord,
    build_source,
    download_one,
    list_metadata,
    map_record,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers for direct flexible-target deletion on live DELETE events
# ---------------------------------------------------------------------------

def _doc_id_for_record(record: "FileRecord", default_source_type: str) -> str:
    """Compute the stable doc_id UUID for a file record.

    Uses the same formula as ``run._run_pipeline`` so the UUID produced here
    matches the one that was stored in Elasticsearch / GraphDB during insert.
    """
    source_type = record.source_type or default_source_type
    file_path = record.file_path or record.key
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{source_type}:{file_path}"))


async def _direct_delete_flexible_targets(record: "FileRecord", source_type: str) -> None:
    """Directly delete a document from all registered flexible targets.

    WHY THIS EXISTS
    ---------------
    CocoIndex's live-update path (``-L`` / ``cocoindex update -L``) propagates
    ``subscriber.delete(key)`` to the *direct pipeline components* (memoised
    functions such as ``process_flexible_file``), but it does **not** trigger
    the full reconcile cycle for ROOT ``TargetStateProvider`` registrations
    (``coco.register_root_target_states_provider``).

    Native CocoIndex targets (Qdrant, Neo4j) use CocoIndex's own collection
    management so their deletions are handled automatically.  Flexible targets
    (Elasticsearch, GraphDB RDF, …) use the root-provider mechanism and
    therefore miss live deletes.

    The FastAPI UI works because ``bridge.py._run_live`` calls
    ``app.update()`` periodically — that full reconcile cycle diffs LMDB
    previous-state against the current file listing and calls
    ``reconcile(NON_EXISTENCE, …)`` for vanished files.  The CLI ``-L`` mode
    has no periodic ``app.update()``, so we compensate here by calling
    ``delete_row(doc_id)`` directly.
    """
    # Lazy import to avoid circular-import at module load time.
    from cocoindex_integration.pipeline import state as _state  # noqa: PLC0415

    doc_id = _doc_id_for_record(record, source_type)

    _flexible_targets = [
        (_state._search_target_singleton, "search"),
        (_state._rdf_target_singleton,    "rdf"),
        (_state._vector_target_singleton, "vector"),
        (_state._pg_target_singleton,     "pg"),
    ]

    for target, label in _flexible_targets:
        if target is None:
            continue
        _delete = getattr(target, "delete_row", None)
        if _delete is None:
            # Not a flexible connector (e.g. native CocoIndex target) — skip.
            continue
        try:
            # Ensure the adapter is initialised (idempotent setup).
            _setup = getattr(target, "setup", None)
            if _setup is not None:
                await _setup()
            await _delete(doc_id)
            logger.info(
                "FlexibleMapView [live-delete]: removed %s data for doc='%s' key=%s",
                label, doc_id, record.key,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "FlexibleMapView [live-delete]: %s delete failed for doc='%s': %s",
                label, doc_id, exc,
            )


async def _delete_flexible_targets_with_doc_id(doc_id: str, description: str = "") -> None:
    """Delete from all flexible targets using a pre-computed doc_id.

    Unlike :func:`_direct_delete_flexible_targets`, this accepts the doc_id
    directly — no ``FileRecord`` or path computation needed.  Used by the
    CocoIndex-native live-source delete observer
    (``native_apps._DeleteObservingLiveMapView``), which derives the doc_id from
    the source's own view key/file at watch time.
    """
    from cocoindex_integration.pipeline import state as _state  # noqa: PLC0415

    _flexible_targets = [
        (_state._search_target_singleton, "search"),
        (_state._rdf_target_singleton,    "rdf"),
        (_state._vector_target_singleton, "vector"),
        (_state._pg_target_singleton,     "pg"),
    ]

    for target, label in _flexible_targets:
        if target is None:
            continue
        _delete = getattr(target, "delete_row", None)
        if _delete is None:
            continue
        try:
            _setup = getattr(target, "setup", None)
            if _setup is not None:
                await _setup()
            await _delete(doc_id)
            _suffix = f" ({description})" if description else ""
            logger.info(
                "NativeSource [live-delete]: removed %s data for doc='%s'%s",
                label, doc_id, _suffix,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "NativeSource [live-delete]: %s delete failed for doc='%s': %s",
                label, doc_id, exc,
            )


class FlexibleMapView:
    """A CocoIndex ``LiveMapView[str, FlexibleFile]`` over a detector-backed source.

    Parameters
    ----------
    source_type:
        One of the 10 detector-backed kinds (``filesystem``, ``s3``, ``gcs``,
        ``azure_blob``, ``google_drive``, ``onedrive``, ``sharepoint``, ``box``,
        ``alfresco``, ``nuxeo``).
    config:
        Source config dict (bucket, prefix, credentials, …) as assembled by
        ``_build_source_config_from_env``.
    """

    def __init__(self, source_type: str, config: Dict[str, Any]) -> None:
        if source_type not in DETECTOR_BACKED:
            raise ValueError(
                f"FlexibleMapView does not support source_type={source_type!r} "
                f"(supported: {sorted(DETECTOR_BACKED)})"
            )
        self._source_type = source_type
        self._config = dict(config)
        self._source: Optional[Any] = None  # cached sources/ instance (downloads)
        self._watch_guard = SingleWatcherGuard(f"flexible map view ({source_type})")

    # ── download binding ─────────────────────────────────────────────────────

    def _get_source(self) -> Any:
        """Build (once) and return the ``sources/`` instance used for downloads."""
        if self._source is None:
            self._source = build_source(self._source_type, self._config)
        return self._source

    def _make_file(self, record: FileRecord) -> FlexibleFile:
        """Build a lazy :class:`FlexibleFile` from a listing ``record``.

        The download callable closes over the cached source instance so file
        2..N reuse the same connection/reader (no per-file reconnection).
        """
        source_type = self._source_type

        def _download(download_key: str) -> bytes:
            return download_one(self._get_source(), source_type, download_key)

        return FlexibleFile(
            record.key,
            download=_download,
            download_key=record.download_key,
            size=record.size,
            modified=record.modified,
            etag=record.etag,
            ordinal=record.ordinal,
            reader_metadata=record.reader_metadata,
            file_name=record.file_name,
            display_path=record.file_path,
            file_type=record.file_type,
            source_type=record.source_type or self._source_type,
        )

    # ── LiveMapView.__aiter__ — scannable current state ──────────────────────

    def __aiter__(self) -> AsyncIterator[Tuple[str, FlexibleFile]]:
        return self._aiter_impl()

    async def _aiter_impl(self) -> AsyncIterator[Tuple[str, FlexibleFile]]:
        # INFO-level: CocoIndex's non-live path (_MountEachLiveComponent.process)
        # iterates the view and mounts one child component per item.  When a build
        # fails with the engine's opaque "Child component build cancelled", these
        # lines are what distinguish "the view was never read" from "the view
        # yielded items and a child mount was cancelled".
        logger.debug("FlexibleMapView[%s]: __aiter__ scan starting", self._source_type)
        try:
            records = await list_metadata(self._source_type, self._config)
        except BaseException as _exc:
            logger.error(
                "FlexibleMapView[%s]: list_metadata failed (%s)",
                self._source_type, type(_exc).__name__, exc_info=True,
            )
            raise
        logger.info(
            "FlexibleMapView[%s]: __aiter__ scan found %d record(s)",
            self._source_type, len(records),
        )
        for _i, record in enumerate(records):
            logger.debug(
                "FlexibleMapView[%s]: yielding item %d/%d key=%s",
                self._source_type, _i + 1, len(records), record.key,
            )
            yield (record.key, self._make_file(record))
        logger.debug("FlexibleMapView[%s]: __aiter__ exhausted", self._source_type)

    # ── LiveMapFeed.watch — change stream ────────────────────────────────────

    async def watch(self, subscriber: LiveMapSubscriber[str, FlexibleFile]) -> None:
        """Deliver an initial scan then live changes to *subscriber*."""
        logger.debug("FlexibleMapView[%s]: watch() entered", self._source_type)
        # watch() runs inside CocoIndex's own task, so anything raised here is
        # invisible to callers — the engine reports only "Child component build
        # cancelled".  Catch BaseException (CancelledError included) purely to
        # LOG it, then re-raise so the engine's own handling is unchanged.
        try:
            with self._watch_guard:
                logger.debug(
                    "FlexibleMapView[%s]: watch guard acquired, entering _watch",
                    self._source_type,
                )
                await self._watch(subscriber)
            logger.debug("FlexibleMapView[%s]: watch() returned normally", self._source_type)
        except asyncio.CancelledError:
            # Expected: CocoIndex cancels the watch task when an update cycle
            # ends or the live stream stops.  Logging this at ERROR (as the
            # original catch-all did) makes routine teardown look like a failure.
            logger.debug(
                "FlexibleMapView[%s]: watch() cancelled (normal teardown)",
                self._source_type,
            )
            raise
        except BaseException as _exc:
            logger.error(
                "FlexibleMapView[%s]: watch() raised %s",
                self._source_type, type(_exc).__name__, exc_info=True,
            )
            raise

    async def _watch(self, subscriber: LiveMapSubscriber[str, FlexibleFile]) -> None:
        from incremental_updates.detectors.base import ChangeType  # noqa: PLC0415
        from incremental_updates.detectors.factory import create_detector  # noqa: PLC0415

        logger.debug("FlexibleMapView[%s]: _watch: creating detector", self._source_type)
        detector = create_detector(self._source_type, self._config)
        logger.debug(
            "FlexibleMapView[%s]: _watch: detector=%s",
            self._source_type, type(detector).__name__ if detector else None,
        )
        if detector is None:
            logger.warning(
                "FlexibleMapView.watch: no detector for %s — reconciling scan only",
                self._source_type,
            )
            # No change stream available; still reconcile the current scan so
            # deletions of vanished files are handled, then finish (catch-up).
            await subscriber.update_all()
            await subscriber.mark_ready()
            return

        try:
            await detector.start()
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "FlexibleMapView.watch: detector.start() failed for %s: %s — "
                "reconciling scan only", self._source_type, exc,
            )
            try:
                await detector.stop()
            except Exception:  # noqa: BLE001
                pass
            await subscriber.update_all()
            await subscriber.mark_ready()
            return

        try:
            # Reconcile the current snapshot first (adds/modifies via __aiter__,
            # plus deletes of files that vanished between runs), then signal
            # readiness so catch-up mode can terminate.
            await subscriber.update_all()
            await subscriber.mark_ready()

            async for event in detector.get_changes():
                # Detectors yield None on idle timeouts (no event in N seconds) so
                # callers can check _running without blocking forever.
                if event is None:
                    continue
                if event.metadata is None:
                    logger.debug(
                        "FlexibleMapView.watch: skipping change event with no metadata"
                    )
                    continue
                meta = event.metadata
                record = map_record(self._source_type, meta, self._config)
                if record is None:
                    continue

                if event.change_type == ChangeType.DELETE:
                    # Directly remove from flexible targets (ES, GraphDB, …) BEFORE
                    # notifying CocoIndex.  CocoIndex's live mode does not propagate
                    # subscriber.delete() to root TargetStateProviders automatically;
                    # native targets (Qdrant, Neo4j) are fine — this only fires for
                    # connectors that expose delete_row().
                    await _direct_delete_flexible_targets(record, self._source_type)
                    handle = await subscriber.delete(record.key)
                    await handle.ready()
                else:  # CREATE / UPDATE
                    handle = await subscriber.update(
                        record.key, self._make_file(record)
                    )
                    await handle.ready()
        finally:
            try:
                await detector.stop()
            except Exception:  # noqa: BLE001
                pass


__all__ = ["FlexibleMapView"]
