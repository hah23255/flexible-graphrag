"""Lazy listing + single-key download helpers for detector-backed sources.

Phase 2 replaces the eager "list → download every file" iterators with a
two-step, change-aware flow:

1. ``list_metadata(source_type, config)`` — uses the existing
   ``incremental_updates`` **detector** for the source to list file metadata
   (path, ordinal, size, ``etag`` when available) *without downloading any
   bytes*.  This is the same listing the incremental-update system already
   performs, so we list once.
2. ``download_one(source, source_type, download_key)`` — downloads a single
   file's raw bytes via the source class's ``read_file_bytes`` method (which
   reuses the cached LlamaIndex reader / SDK client — no new SDK code).

Each ``FileRecord`` produced by ``list_metadata`` carries everything a
``FlexibleFile`` needs (stable key, download key, fingerprint inputs, and the
full reader/provenance metadata dict) so the map-view/file layer never has to
re-derive it.

Covers all 10 detector-backed sources: filesystem, s3, gcs, azure_blob,
google_drive, onedrive, sharepoint, box, alfresco, nuxeo.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone as _tz
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Short-lived scan result cache
# ---------------------------------------------------------------------------
# CocoIndex calls FlexibleMapView.__aiter__ (→ list_metadata) multiple times
# per app.update() cycle to build the key set for add/delete detection.
# Caching the result for a short window prevents redundant detector start/stop
# pairs within the same update tick.  The Time To Live (TTL) is well below any 
# poll interval so that each distinct cycle gets a fresh scan.
_scan_cache: Dict[str, tuple] = {}  # cache_key → (monotonic_timestamp, List[FileRecord])
_scan_locks: Dict[str, asyncio.Lock] = {}  # cache_key → asyncio.Lock
_SCAN_CACHE_TTL: float = 2.0  # Time To Live (TTL) in seconds


# Source class registry: source_type -> (module, class_name).
# These are the same classes the eager iterators and the standard
# get_documents() path use — no new SDK code.
_SOURCE_CLASSES: Dict[str, tuple] = {
    "filesystem": ("sources.filesystem", "FileSystemSource"),
    "s3": ("sources.s3", "S3Source"),
    "gcs": ("sources.gcs", "GCSSource"),
    "azure_blob": ("sources.azure_blob", "AzureBlobSource"),
    "google_drive": ("sources.google_drive", "GoogleDriveSource"),
    "onedrive": ("sources.onedrive", "OneDriveSource"),
    "sharepoint": ("sources.sharepoint", "SharePointSource"),
    "box": ("sources.box", "BoxSource"),
    "alfresco": ("sources.alfresco", "AlfrescoSource"),
    "nuxeo": ("sources.nuxeo", "NuxeoSource"),
}

# Detector-backed source types (the 10 that support change detection).
DETECTOR_BACKED = frozenset(_SOURCE_CLASSES.keys())


@dataclass
class FileRecord:
    """A single file's metadata from a detector listing (no bytes fetched).

    Attributes
    ----------
    key:
        Stable CocoIndex change-detection identity (e.g. ``s3://bucket/key``).
    download_key:
        Argument passed to the source's ``read_file_bytes`` (object key, blob
        name, file_id, or absolute path — may differ from ``key``).
    file_name:
        Human-readable filename.
    file_path:
        Human/display path.
    file_type:
        Extension without dot.
    size:
        Size in bytes (``0`` if unknown).
    modified:
        ISO-8601 last-modified string (``""`` if unknown).
    etag:
        Backend content fingerprint when the source provides one; else ``None``.
    ordinal:
        Microsecond-timestamp change token (fingerprint fallback when no etag).
    reader_metadata:
        Full provenance metadata dict (source, bucket/container/prefix/region,
        etc.) merged into chunk metadata downstream.
    source_type:
        The datasource kind string.
    """

    key: str
    download_key: str
    file_name: str
    file_path: str
    file_type: str = ""
    size: int = 0
    modified: str = ""
    etag: Optional[str] = None
    ordinal: Optional[int] = None
    reader_metadata: Dict[str, Any] = field(default_factory=dict)
    source_type: str = ""


def build_source(source_type: str, config: Dict[str, Any]):
    """Instantiate the ``sources/`` class for ``source_type`` (cached by caller).

    The returned instance owns credential/connection state and caches its
    LlamaIndex reader across ``read_file_bytes`` calls, so the map-view layer
    should build it once and reuse it for every file in a run.
    """
    if source_type not in _SOURCE_CLASSES:
        raise ValueError(f"No source class registered for '{source_type}'")

    # Google Drive: LlamaIndex's GoogleDriveReader interprets ``credentials_path``
    # as an OAuth2 client-secrets file.  When the path points to a service-account
    # key JSON instead (which is what the detector uses), the reader raises
    # "Client secrets must be for a web or installed app."  Pre-load the file and
    # convert it to the ``credentials`` JSON-string that GoogleDriveSource.__init__
    # already handles via its ``service_account_key`` path.
    if source_type == "google_drive" and not config.get("credentials") and config.get("credentials_path"):
        _cred_path = config["credentials_path"]
        try:
            with open(_cred_path, encoding="utf-8") as _fh:
                _cred_data = json.load(_fh)
            if _cred_data.get("type") == "service_account":
                config = dict(config)  # shallow copy — don't mutate caller's dict
                config["credentials"] = json.dumps(_cred_data)
                logger.debug(
                    "build_source(google_drive): pre-loaded service account from %s "
                    "into 'credentials' key for GoogleDriveSource",
                    _cred_path,
                )
        except Exception as _exc:
            logger.debug("build_source(google_drive): could not inspect %s: %s", _cred_path, _exc)

    mod_name, cls_name = _SOURCE_CLASSES[source_type]
    mod = importlib.import_module(mod_name)
    return getattr(mod, cls_name)(config)


def download_one(source: Any, source_type: str, download_key: str) -> bytes:
    """Download one file's raw bytes via the source's ``read_file_bytes``.

    Synchronous (reader/SDK I/O).  Callers run this off the event loop.
    """
    try:
        return source.read_file_bytes(download_key) or b""
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "download_one(%s) failed for key=%r: %s", source_type, download_key, exc
        )
        return b""


# ---------------------------------------------------------------------------
# Per-source FileMetadata -> FileRecord mapping
# ---------------------------------------------------------------------------

def _basename(path: str) -> str:
    return os.path.basename(path.rstrip("/")) if path else path


def _ext(name: str) -> str:
    _, e = os.path.splitext(name)
    return e.lstrip(".").lower()


def _map_record(source_type: str, m: Any, config: Dict[str, Any]) -> Optional[FileRecord]:
    """Convert a detector ``FileMetadata`` into a ``FileRecord``.

    ``m`` is ``incremental_updates.detectors.base.FileMetadata``.
    """
    extra = m.extra or {}
    modified = m.modified_timestamp or ""
    ordinal = m.ordinal
    size = m.size_bytes or 0

    if source_type == "filesystem":
        path = m.path
        # CocoIndex uses modified_time as its primary change-detection signal.
        # The filesystem detector sets modified_timestamp=None, so `modified`
        # above would be "" → FlexibleFile.modified_time = epoch for every file,
        # and CocoIndex never sees a change even when ordinal (mtime) differs.
        # Fix: derive modified from ordinal (microseconds since epoch) so that
        # mtime changes are visible to CocoIndex's memo comparison.
        if m.modified_timestamp:
            fs_modified = m.modified_timestamp.isoformat()
        elif ordinal:
            fs_modified = datetime.fromtimestamp(ordinal / 1_000_000, tz=_tz.utc).isoformat()
        else:
            fs_modified = ""
        return FileRecord(
            key=f"file://{path}",
            download_key=path,
            file_name=_basename(path),
            file_path=path,
            file_type=_ext(path),
            size=size,
            modified=fs_modified,
            etag=None,
            ordinal=ordinal,
            reader_metadata={"source": "filesystem", "file_path": path},
            source_type="filesystem",
        )

    if source_type == "s3":
        # detector path = "bucket/key"; extra has etag + s3_uri
        bucket = config.get("bucket") or config.get("bucket_name") or (
            m.path.split("/", 1)[0] if "/" in m.path else ""
        )
        object_key = m.path.split("/", 1)[1] if "/" in m.path else m.path
        s3_uri = extra.get("s3_uri") or f"s3://{bucket}/{object_key}"
        name = _basename(object_key)
        return FileRecord(
            key=s3_uri,
            download_key=object_key,
            file_name=name,
            file_path=s3_uri,
            file_type=_ext(name),
            size=size,
            modified=modified,
            etag=extra.get("etag"),
            ordinal=ordinal,
            reader_metadata={
                "source": "s3",
                "bucket_name": bucket,
                "prefix": config.get("prefix", "") or "",
                "region": config.get("region_name") or config.get("aws_region") or "",
                "source_type": "s3_object",
                "file_path": s3_uri,
            },
            source_type="s3",
        )

    if source_type == "gcs":
        bucket = extra.get("bucket") or config.get("bucket") or config.get("bucket_name") or ""
        object_key = extra.get("object_key") or (
            m.path.split("/", 1)[1] if "/" in m.path else m.path
        )
        name = _basename(object_key)
        key = f"gs://{bucket}/{object_key}"
        return FileRecord(
            key=key,
            download_key=object_key,
            file_name=name,
            file_path=key,
            file_type=_ext(name),
            size=size,
            modified=modified,
            etag=extra.get("etag"),
            ordinal=ordinal,
            reader_metadata={
                "source": "gcs",
                "bucket": bucket,
                "prefix": config.get("prefix", "") or "",
                "source_type": "gcs_blob",
                "file_path": key,
            },
            source_type="gcs",
        )

    if source_type == "azure_blob":
        container = extra.get("container") or config.get("container_name") or config.get("container") or ""
        blob_name = extra.get("blob_name") or (
            m.path.split("/", 1)[1] if "/" in m.path else m.path
        )
        # Change Feed events set blob_name = "container/blob" (full path) for the normal-pipeline
        # engine delete handler; strip the container prefix so download_key is just the blob name.
        if container and blob_name.startswith(f"{container}/"):
            blob_name = blob_name[len(container) + 1:]
        name = _basename(blob_name)
        key = f"{container}/{blob_name}"
        return FileRecord(
            key=key,
            download_key=blob_name,
            file_name=name,
            file_path=key,
            file_type=_ext(name),
            size=size,
            modified=modified,
            etag=extra.get("etag"),
            ordinal=ordinal,
            reader_metadata={
                "source": "azure_blob",
                "container_name": container,
                "source_type": "azure_blob_object",
                "file_path": key,
            },
            source_type="azure_blob",
        )

    if source_type == "google_drive":
        file_id = extra.get("file_id") or m.path
        name = extra.get("file_name") or file_id
        human_path = extra.get("file_path") or name
        return FileRecord(
            key=f"gdrive://{file_id}",
            download_key=file_id,
            file_name=name,
            file_path=human_path,
            file_type=_ext(name),
            size=size,
            modified=modified,
            etag=None,
            ordinal=ordinal,
            reader_metadata={
                "source": "google_drive",
                "file_id": file_id,
                "file_path": human_path,
                "source_type": "google_drive_file",
            },
            source_type="google_drive",
        )

    if source_type in ("onedrive", "sharepoint"):
        # msgraph detector: path = "onedrive://id" / "sharepoint://id"
        file_id = extra.get("file_id") or (
            m.path.split("://", 1)[1] if "://" in m.path else m.path
        )
        name = extra.get("file_name") or file_id
        human_path = extra.get("file_path") or name
        key = m.path if "://" in m.path else f"{source_type}://{file_id}"
        return FileRecord(
            key=key,
            download_key=file_id,
            file_name=name,
            file_path=human_path,
            file_type=_ext(name),
            size=size,
            modified=modified,
            etag=extra.get("etag"),
            ordinal=ordinal,
            reader_metadata={
                "source": source_type,
                "file_id": file_id,
                "file_path": human_path,
                "source_type": f"{source_type}_file",
            },
            source_type=source_type,
        )

    if source_type == "box":
        # detector path for box list = file.name (human filename); file_id in extra
        file_id = extra.get("file_id") or m.path
        name = m.path
        return FileRecord(
            key=f"box://{file_id}",
            download_key=file_id,
            file_name=name,
            file_path=name,
            file_type=_ext(name),
            size=size,
            modified=modified,
            etag=extra.get("etag"),
            ordinal=ordinal,
            reader_metadata={
                "source": "box",
                "file_id": file_id,
                "source_type": "box_file",
            },
            source_type="box",
        )

    if source_type == "alfresco":
        node_id = extra.get("node_id") or m.path
        human_path = m.path
        name = extra.get("name") or _basename(human_path)
        return FileRecord(
            key=f"alfresco://{node_id}",
            download_key=node_id,
            file_name=name,
            file_path=human_path,
            file_type=_ext(name),
            size=size,
            modified=modified or str(extra.get("modified", "")),
            etag=None,
            ordinal=ordinal,
            reader_metadata={
                "source": "alfresco",
                "node_id": node_id,
                "file_path": human_path,
                "source_type": "alfresco_document",
            },
            source_type="alfresco",
        )

    if source_type == "nuxeo":
        # NuxeoDetector emits path="nuxeo://<uid>" with the uid, display name and
        # repository path in ``extra`` — same shape as Alfresco.
        node_id = extra.get("node_id") or _basename(m.path)
        human_path = extra.get("file_path") or m.path
        name = extra.get("name") or _basename(human_path)
        return FileRecord(
            key=f"nuxeo://{node_id}",
            download_key=node_id,
            file_name=name,
            file_path=human_path,
            file_type=_ext(name),
            size=size,
            modified=modified or str(extra.get("modified", "")),
            etag=None,
            ordinal=ordinal,
            reader_metadata={
                "source": "nuxeo",
                "node_id": node_id,
                "file_path": human_path,
                "source_type": "nuxeo_document",
            },
            source_type="nuxeo",
        )

    logger.warning("No FileRecord mapping for source_type=%s", source_type)
    return None


def map_record(source_type: str, m: Any, config: Dict[str, Any]) -> Optional[FileRecord]:
    """Public wrapper around :func:`_map_record`.

    Converts a single ``incremental_updates.detectors.base.FileMetadata`` into a
    :class:`FileRecord`.  Used by the map-view ``watch()`` path to translate each
    ``ChangeEvent.metadata`` into a record the same way ``list_metadata`` does for
    the initial scan.
    """
    return _map_record(source_type, m, config)


async def list_metadata(source_type: str, config: Dict[str, Any]) -> List[FileRecord]:
    """List file metadata via the source's detector (no downloads).

    Starts the detector, calls ``list_all_files()``, stops the detector, and
    maps each ``FileMetadata`` to a ``FileRecord``.

    Results are cached for ``_SCAN_CACHE_TTL`` seconds so that concurrent
    calls within the same CocoIndex update cycle (CocoIndex calls
    ``__aiter__`` multiple times per ``update()``) reuse the first scan
    instead of starting and stopping the detector redundantly.
    """
    # --- cache key + per-key lock -------------------------------------------
    # Both calls within the same CocoIndex update() cycle start concurrently,
    # so a plain cache check races (both see a miss before either stores).
    # The lock serialises them: the second waiter finds the result already cached.
    try:
        _cache_key = f"{source_type}:{json.dumps(config, sort_keys=True, default=str)}"
    except Exception:  # noqa: BLE001
        _cache_key = source_type

    if _cache_key not in _scan_locks:
        _scan_locks[_cache_key] = asyncio.Lock()

    async with _scan_locks[_cache_key]:
        _now = time.monotonic()
        _cached = _scan_cache.get(_cache_key)
        if _cached is not None:
            _ts, _records = _cached
            if _now - _ts < _SCAN_CACHE_TTL:
                logger.debug(
                    "list_metadata(%s): %d cached file(s) (dedup within update cycle)",
                    source_type, len(_records),
                )
                return _records

        # --- fresh scan (lock held) -----------------------------------------
        from incremental_updates.detectors.factory import create_detector  # noqa: PLC0415

        try:
            detector = create_detector(source_type, config)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Detector creation failed for %s: %s — check source config env vars",
                source_type, exc,
            )
            return []
        if detector is None:
            logger.warning("No detector for source_type=%s", source_type)
            return []

        try:
            await detector.start()
            if source_type == "sharepoint":
                resolved_site_id = getattr(detector, "site_id", None)
                if resolved_site_id:
                    config["site_id"] = resolved_site_id
        except Exception as exc:  # noqa: BLE001
            logger.error("Detector start failed for %s: %s", source_type, exc)
            try:
                await detector.stop()
            except Exception:  # noqa: BLE001
                pass
            return []

        try:
            metas = await detector.list_all_files()
        except Exception as exc:  # noqa: BLE001
            logger.error("Detector list_all_files failed for %s: %s", source_type, exc)
            metas = []
        finally:
            try:
                await detector.stop()
            except Exception:  # noqa: BLE001
                pass

        records: List[FileRecord] = []
        for m in metas:
            rec = _map_record(source_type, m, config)
            if rec is not None:
                records.append(rec)
        logger.debug("list_metadata(%s): %d file(s)", source_type, len(records))

        _scan_cache[_cache_key] = (time.monotonic(), records)
        return records
