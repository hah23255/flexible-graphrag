"""Native CocoIndex source readers — localfs, S3, Azure Blob, Google Drive.

Used when ``SOURCE_BACKEND=cocoindex`` and the matching ``DATA_SOURCE`` is set.
There is **no per-source ``coco.App``** here anymore: the single
:func:`flexible_app.flexible_app_main` is the one app_main for every source.
It dispatches native sources through the :data:`NATIVE_READERS` registry below.

Each registry entry is a ``(lister, worker)`` pair:

* **lister** — an ``async`` function ``list_*(cfg) -> iterable | None`` that lists
  items using the native CocoIndex connector (``localfs.walk_dir``,
  ``amazon_s3.list_objects``, ``azure_blob.list_blobs``,
  ``GoogleDriveSource.items``).  Returns ``None`` when the connector's optional
  dependency is missing so the caller can fall back to ``FlexibleDataSource``.
* **worker** — the ``@coco.fn(memo=True)`` per-file processor that downloads the
  bytes (source-specific) and runs the full flexible-graphrag pipeline
  (parse → chunk → embed → KG → write to all configured targets).  Memoisation
  keys on the native file object so unchanged files are skipped.

Registry:
  filesystem / localfs → (``_list_localfs_items``,     ``process_localfs_file``)
  s3 / amazon_s3       → (``_list_s3_items``,           ``process_s3_file``)
  azure_blob           → (``_list_azure_blob_items``,   ``process_azure_blob_file``)
  google_drive         → (``_list_google_drive_items``, ``process_google_drive_file``)

All credentials and bucket/container/folder settings are read from the resolved
pipeline ``cfg`` (env vars + ``datasource_config`` ``connection_params``), which
:func:`flexible_app.build_app_for_config` prepares via
:func:`_prepare_native_source_cfg` before serialising to ``cfg_json``.
"""
from __future__ import annotations

import datetime
import json
import logging
import os
import uuid
import warnings
from pathlib import Path

# CocoIndex's Google Drive connector creates a new httplib2.Http() per request
# (intentional: httplib2 is not thread-safe).  Each Http() holds an SSL socket
# that Python's GC closes on the next collection cycle.  Suppress the cosmetic
# ResourceWarning so it does not flood the log; the sockets are not leaked.
warnings.filterwarnings(
    "ignore",
    message=r"unclosed.*ssl\.SSLSocket",
    category=ResourceWarning,
)
from typing import Any, Dict

logger = logging.getLogger(__name__)

import cocoindex as coco  # noqa: E402

from cocoindex_integration.pipeline import run as _run  # noqa: E402

# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

try:
    from cocoindex.resources.file import PatternFilePathMatcher
except ImportError:
    PatternFilePathMatcher = None  # type: ignore[assignment,misc]

_DOC_PATTERNS = [
    "**/*.pdf", "**/*.docx", "**/*.doc", "**/*.pptx", "**/*.ppt",
    "**/*.xlsx", "**/*.xls", "**/*.txt", "**/*.md", "**/*.html",
    "**/*.htm", "**/*.csv", "**/*.json", "**/*.xml",
]


def _build_path_matcher() -> Any:
    if PatternFilePathMatcher is None:
        return None
    return PatternFilePathMatcher(included_patterns=_DOC_PATTERNS)  # type: ignore[operator]


def _poll_interval_seconds() -> int:
    """Single CocoIndex refresh cadence (``COCOINDEX_POLL_INTERVAL``, seconds).

    Used two ways:
    * localfs ``walk_dir(rescan_interval=…)`` — periodic full rescan that backs
      up the file-system watcher (catches any events the watcher missed).
    * the bridge's backup poll for non-watchable sources (native S3/Azure/Drive).
    """
    try:
        return max(5, int(float(os.getenv("COCOINDEX_POLL_INTERVAL", "60"))))
    except Exception:
        return 60


def _emit_worker_failure(file: Any, exc: Exception) -> None:
    """Emit a terminal ``file_done`` (status=failed) for a failed worker.

    Native workers wrap ``_run._run_pipeline`` in a broad ``except`` that logs
    and returns ``error:...``.  When the failure happens *before* the pipeline
    reaches its own ``file_done`` finally block (e.g. a parse or chunk error, or
    a download failure), no terminal event is emitted.  In server / live mode the
    progress hook then never sees ``file_done`` for this file, ``_live_done`` is
    never set, and the REST ingest request hangs for its full 600 s timeout
    instead of failing fast.

    This helper derives a best-effort ``file_name`` / ``file_path`` from the
    native ``file`` object (whose exact shape varies per source) and emits the
    terminal event.  It never raises.
    """
    try:
        _fp = "source"
        try:
            _fp = str(file.file_path.resolve())
        except Exception:  # noqa: BLE001
            _fp = str(getattr(file, "file_path", file))
        try:
            _fn = str(file.file_path.path.name)
        except Exception:  # noqa: BLE001
            _fn = _fp.rsplit("/", 1)[-1].rsplit("\\", 1)[-1] or _fp
        _run._emit_progress(
            event="file_done", file_name=_fn, file_path=_fp,
            status="failed", detail=f"{type(exc).__name__}: {exc}",
        )
    except Exception:  # noqa: BLE001
        pass


# ─────────────────────────────────────────────────────────────────────────────
# App A — GraphRAGLocalfs
# ─────────────────────────────────────────────────────────────────────────────

try:
    from cocoindex.connectors import localfs as _localfs
    _LocalfsFile = _localfs.File
    _LOCALFS_AVAILABLE = True
except ImportError:
    _LOCALFS_AVAILABLE = False
    _localfs = None  # type: ignore[assignment]
    _LocalfsFile = Any  # type: ignore[misc,assignment]


@coco.fn(memo=True)  # type: ignore[misc]
async def process_localfs_file(file: _LocalfsFile, cfg_json: str) -> str:  # type: ignore[valid-type]
    """Process one local file — memoised so unchanged files are skipped.

    Retries up to 3 times on ``PermissionError`` / ``OSError`` with short
    backoffs (0.3 s → 1.0 s → 3.0 s).  On Windows, CocoIndex's live watcher
    fires the moment watchfiles sees the event — the file may still be locked
    by the process that is writing / copying it into the directory.  Without
    retrying inside the function the memo records a permanent failure (the memo
    key is the file's mtime, which won't change just because the lock was
    released, so CocoIndex would skip the file on the next scan).
    """
    import asyncio as _asyncio
    import traceback as _tb

    _RETRY_DELAYS = (0.3, 1.0, 3.0)

    async def _read_file() -> bytes:
        if hasattr(file, "read"):
            return await file.read()
        return file.file_path.resolve().read_bytes()

    try:
        # ── read with retry (Windows file-lock race on watcher events) ──────
        file_bytes: bytes = b""
        for _attempt, _delay in enumerate(_RETRY_DELAYS, 1):
            try:
                file_bytes = await _read_file()
                break
            except (PermissionError, OSError) as _lock_exc:
                if _attempt == len(_RETRY_DELAYS):
                    raise  # exhausted — caught by the outer except below
                logger.warning(
                    "[localfs] %s: read attempt %d/%d failed (%s: %s) — retrying in %.1fs",
                    getattr(getattr(file, "file_path", None), "path", file),
                    _attempt, len(_RETRY_DELAYS),
                    type(_lock_exc).__name__, _lock_exc, _delay,
                )
                await _asyncio.sleep(_delay)

        file_name = str(file.file_path.path.name)
        file_path = str(file.file_path.resolve())
        modified_at = str(getattr(file, "mtime", ""))
        _run._emit_progress(event="file_stage", file_name=file_name, file_path=file_path, stage="downloaded")
        cfg = json.loads(cfg_json)
        await _run._run_pipeline(file_bytes, file_name, file_path, "filesystem", modified_at, cfg)
        return f"ok:{file_name}"
    except Exception as _exc:
        _err_msg = (
            f"[CocoIndex pipeline ERROR] {type(_exc).__name__}: {_exc}\n"
            + _tb.format_exc()
        )
        logger.error(_err_msg)
        with open("cocoindex_pipeline_error.log", "a", encoding="utf-8") as _ef:
            _ef.write(_err_msg + "\n")
        # Guarantee a terminal file_done so live-mode progress hooks are released
        # even when the failure happened before _run_pipeline's own file_done.
        _emit_worker_failure(file, _exc)
        return f"error:{type(_exc).__name__}:{_exc}"


async def _list_localfs_items(cfg: Dict[str, Any]) -> "Any | None":
    """List local files as a live ``walk_dir`` view (or ``None`` if unavailable).

    Reads the directory from ``cfg["path"]`` / ``cfg["watch_dir"]`` (falling back
    to the ``WATCH_DIR`` env var, then ``./cocoindex-docs``).  Returns a
    ``LiveMapView`` in live mode: full scan first, then watch for file-system
    changes (via watchfiles); ``rescan_interval`` is the periodic full-rescan
    backup that catches any events the watcher missed.  In catch-up mode
    (``app.update()`` without live) the view does a one-time scan and exits.
    """
    if not _LOCALFS_AVAILABLE:
        logger.error("cocoindex.connectors.localfs not available — cannot walk dir")
        return None
    sourcedir = Path(
        cfg.get("path")
        or cfg.get("watch_dir")
        or os.getenv("WATCH_DIR", "./cocoindex-docs")
    )
    path_matcher = _build_path_matcher()
    walk_kwargs: Dict[str, Any] = {
        "recursive": True,
        "live": True,
        "rescan_interval": datetime.timedelta(seconds=_poll_interval_seconds()),
    }
    if path_matcher is not None:
        walk_kwargs["path_matcher"] = path_matcher
    files = _localfs.walk_dir(sourcedir, **walk_kwargs)  # type: ignore[union-attr]
    logger.info("[localfs] listing %s", sourcedir)
    return files.items()


# ─────────────────────────────────────────────────────────────────────────────
# App C — GraphRAGS3  (native CocoIndex amazon_s3 connector)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from cocoindex.connectors import amazon_s3 as _amazon_s3
    _S3_AVAILABLE = True
except ImportError:
    _S3_AVAILABLE = False
    _amazon_s3 = None  # type: ignore[assignment]


@coco.fn(memo=True)  # type: ignore[misc]
async def process_s3_file(file: Any, cfg_json: str) -> str:
    """Process one S3 object via CocoIndex's native ``amazon_s3`` connector."""
    import traceback as _tb
    try:
        resolved_key = str(file.file_path.resolve())
        file_name = resolved_key.rsplit("/", 1)[-1] or resolved_key
        bucket = str(file.file_path.bucket_name)
        cfg = json.loads(cfg_json)

        import aiobotocore.session as _aio_session  # noqa: PLC0415
        _session = _aio_session.get_session()
        _client_kwargs: Dict[str, Any] = {}
        _region = str(
            cfg.get("s3_region") or cfg.get("region") or cfg.get("region_name") or ""
        )
        if _region:
            _client_kwargs["region_name"] = _region
        # Accept all credential key name variants produced by different config paths.
        _access_key = str(
            cfg.get("s3_access_key") or cfg.get("access_key")
            or cfg.get("access_key_id") or cfg.get("aws_access_key_id") or ""
        )
        _secret_key = str(
            cfg.get("s3_secret_key") or cfg.get("secret_key")
            or cfg.get("secret_access_key") or cfg.get("aws_secret_access_key") or ""
        )
        if _access_key and _secret_key:
            _client_kwargs["aws_access_key_id"] = _access_key
            _client_kwargs["aws_secret_access_key"] = _secret_key
        _endpoint = str(cfg.get("s3_endpoint_url") or cfg.get("endpoint_url") or "")
        if _endpoint:
            _client_kwargs["endpoint_url"] = _endpoint
        # Honour OPENAI_VERIFY_SSL=false / AWS_CA_BUNDLE for corporate proxy environments.
        import os as _os2
        _ssl_env = _os2.getenv("OPENAI_VERIFY_SSL", "true").lower()
        if _ssl_env in ("false", "0", "no"):
            _client_kwargs["verify"] = False
        elif _s3ca := _os2.getenv("AWS_CA_BUNDLE") or _os2.getenv("REQUESTS_CA_BUNDLE"):
            _client_kwargs["verify"] = _s3ca

        logger.info("[S3] downloading s3://%s/%s ...", bucket, resolved_key)
        async with _session.create_client("s3", **_client_kwargs) as _client:
            file_bytes: bytes = await _amazon_s3.read(  # type: ignore[union-attr]
                _client, f"s3://{bucket}/{resolved_key}"
            )
        logger.info("[S3] downloaded %d bytes for %s", len(file_bytes), file_name)

        file_path = f"s3://{bucket}/{resolved_key}"
        try:
            _meta = file._metadata
            modified_at = str(_meta.modified_time) if _meta else ""
        except Exception:
            modified_at = ""

        source_metadata = {
            "bucket": bucket,
            "prefix": cfg.get("s3_prefix") or cfg.get("prefix") or "",
            "region": _region,
            "source": "s3",
        }
        _run._emit_progress(event="file_stage", file_name=file_name, file_path=file_path, stage="downloaded")
        await _run._run_pipeline(
            file_bytes, file_name, file_path, "s3", modified_at, cfg, source_metadata,
        )
        return f"ok:{file_name}"
    except Exception as _exc:
        _err_msg = (
            f"[CocoIndex S3 pipeline ERROR] {type(_exc).__name__}: {_exc}\n"
            + _tb.format_exc()
        )
        logger.error(_err_msg)
        with open("cocoindex_pipeline_error.log", "a", encoding="utf-8") as _ef:
            _ef.write(_err_msg + "\n")
        _emit_worker_failure(file, _exc)
        return f"error:{type(_exc).__name__}:{_exc}"


async def _list_s3_items(cfg: Dict[str, Any]) -> "Any | None":
    """List S3 objects via the native connector (or ``None`` if unavailable).

    Reads bucket/prefix/region/credentials from ``cfg`` with broad key
    fallbacks so both env-configured and ``connection_params``-configured
    sources work.  Materialises the async walker into a list within the client
    context and returns an iterator (the async client is closed by the time the
    per-file worker downloads bytes — the worker opens its own sync client).
    """
    if not _S3_AVAILABLE:
        logger.error("cocoindex.connectors.amazon_s3 not available — cannot list bucket")
        return None

    import aiobotocore.session  # noqa: PLC0415

    bucket = str(cfg.get("s3_bucket") or cfg.get("bucket") or cfg.get("bucket_name") or "")
    prefix = str(cfg.get("s3_prefix") or cfg.get("prefix") or "")
    session = aiobotocore.session.get_session()
    client_kwargs: Dict[str, Any] = {}
    endpoint_url = str(cfg.get("s3_endpoint_url") or cfg.get("endpoint_url") or "")
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    region = str(
        cfg.get("s3_region") or cfg.get("region_name") or cfg.get("region") or ""
    )
    if region:
        client_kwargs["region_name"] = region
    access_key = str(
        cfg.get("s3_access_key") or cfg.get("access_key")
        or cfg.get("access_key_id") or cfg.get("aws_access_key_id") or ""
    )
    secret_key = str(
        cfg.get("s3_secret_key") or cfg.get("secret_key")
        or cfg.get("secret_access_key") or cfg.get("aws_secret_access_key") or ""
    )
    if access_key and secret_key:
        client_kwargs["aws_access_key_id"] = access_key
        client_kwargs["aws_secret_access_key"] = secret_key
    # Honour OPENAI_VERIFY_SSL=false / AWS_CA_BUNDLE for corporate proxy environments.
    # aiobotocore's create_client accepts verify=False or verify="/path/ca.pem"
    # mirroring botocore / requests conventions.
    import os as _os
    _ssl_verify_env = _os.getenv("OPENAI_VERIFY_SSL", "true").lower()
    if _ssl_verify_env in ("false", "0", "no"):
        client_kwargs["verify"] = False
    elif _ca := _os.getenv("AWS_CA_BUNDLE") or _os.getenv("REQUESTS_CA_BUNDLE"):
        client_kwargs["verify"] = _ca

    path_matcher = _build_path_matcher()
    logger.info("[S3] listing s3://%s/%s (pattern filter: %s)",
                bucket, prefix or "", "yes" if path_matcher is not None else "no")
    _s3_items: list = []
    async with session.create_client("s3", **client_kwargs) as client:
        walker = _amazon_s3.list_objects(  # type: ignore[union-attr]
            client,
            bucket,
            prefix=prefix or "",
            path_matcher=path_matcher,
        )
        async for _path, _s3file in walker.items():
            _s3_items.append((_path, _s3file))
    logger.info("[S3] found %d object(s) in s3://%s/%s", len(_s3_items), bucket, prefix or "")
    return iter(_s3_items)


# ─────────────────────────────────────────────────────────────────────────────
# App D — GraphRAGAzureBlob  (native CocoIndex azure_blob connector)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from cocoindex.connectors import azure_blob as _azure_blob
    from azure.storage.blob.aio import ContainerClient as _AzureContainerClient
    _AZURE_BLOB_AVAILABLE = True
except ImportError:
    _AZURE_BLOB_AVAILABLE = False
    _azure_blob = None  # type: ignore[assignment]
    _AzureContainerClient = None  # type: ignore[assignment,misc]


def _azure_blob_credential(cfg: Dict[str, Any]) -> Any:
    """Build an Azure credential from config (key, SAS, or DefaultAzureCredential)."""
    _key = str(cfg.get("account_key", "") or "")
    if _key:
        return _key
    _sas = str(cfg.get("sas_token", "") or "")
    if _sas:
        return _sas
    from azure.identity.aio import DefaultAzureCredential  # noqa: PLC0415
    return DefaultAzureCredential()


@coco.fn(memo=True)  # type: ignore[misc]
async def process_azure_blob_file(file: Any, cfg_json: str) -> str:
    """Process one Azure Blob via CocoIndex's native ``azure_blob`` connector."""
    import asyncio as _asyncio
    import traceback as _tb
    try:
        cfg = json.loads(cfg_json)
        container = str(cfg.get("container_name", cfg.get("container", "")))
        resolved_name = str(file.file_path.resolve())
        file_name = str(file.file_path.path).replace("\\", "/").rsplit("/", 1)[-1] or resolved_name
        file_path = f"{container}/{resolved_name}" if container else resolved_name

        # Download via sync SDK in a thread pool.
        # Rationale: process_azure_blob_file is a @coco.fn component — CocoIndex defers
        # its execution until after app_azure_blob_main returns.  By that time the
        # ``async with ContainerClient`` block has exited and aiohttp's connector is
        # None, so ``await file.read()`` raises AssertionError.  Using the sync SDK in
        # asyncio.to_thread() creates a fresh HTTP session each time, avoiding the issue.
        _account_url = str(cfg.get("account_url", ""))
        _account_key = str(cfg.get("account_key", "") or "")
        _sas_token = str(cfg.get("sas_token", "") or "")
        _blob_name = resolved_name  # path within container (no container prefix)

        def _sync_download() -> bytes:
            from azure.storage.blob import BlobServiceClient as _SyncBSC  # noqa: PLC0415
            _cred: Any = _account_key or _sas_token or None
            if _cred is None:
                from azure.identity import DefaultAzureCredential as _DAC  # noqa: PLC0415
                _cred = _DAC()
            with _SyncBSC(account_url=_account_url, credential=_cred) as _svc:
                _bc = _svc.get_blob_client(container=container, blob=_blob_name)
                return _bc.download_blob().readall()

        file_bytes: bytes = await _asyncio.to_thread(_sync_download)
        modified_at = str(getattr(file, "mtime", getattr(file, "last_modified", "")))
        source_metadata = {
            "container": container,
            "prefix": cfg.get("prefix", ""),
            "account_url": cfg.get("account_url", ""),
            "source": "azure_blob",
        }
        _run._emit_progress(event="file_stage", file_name=file_name, file_path=file_path, stage="downloaded")
        await _run._run_pipeline(
            file_bytes, file_name, file_path, "azure_blob", modified_at, cfg, source_metadata,
        )
        return f"ok:{file_name}"
    except Exception as _exc:
        _err_msg = (
            f"[CocoIndex Azure Blob pipeline ERROR] {type(_exc).__name__}: {_exc}\n"
            + _tb.format_exc()
        )
        logger.error(_err_msg)
        with open("cocoindex_pipeline_error.log", "a", encoding="utf-8") as _ef:
            _ef.write(_err_msg + "\n")
        _emit_worker_failure(file, _exc)
        return f"error:{type(_exc).__name__}:{_exc}"


async def _list_azure_blob_items(cfg: Dict[str, Any]) -> "Any | None":
    """List Azure Blob blobs via the native connector (or ``None`` if unavailable).

    ``account_url`` is derived from ``account_name`` when missing.  The async
    walker is materialised into a list within the client context and returned as
    an iterator (the per-file worker opens its own sync SDK client to download).
    """
    if not _AZURE_BLOB_AVAILABLE or _AzureContainerClient is None:
        logger.error("cocoindex.connectors.azure_blob not available — cannot list container")
        return None

    account_url = str(cfg.get("account_url", ""))
    account_name = str(cfg.get("account_name", ""))
    if not account_url and account_name:
        account_url = f"https://{account_name}.blob.core.windows.net"
    container = str(cfg.get("container_name", cfg.get("container", "")))
    prefix = str(cfg.get("prefix", "") or "")
    credential = _azure_blob_credential(cfg)

    path_matcher = _build_path_matcher()
    logger.info("[azure_blob] listing %s/%s", container, prefix or "")
    _blob_items: list = []
    async with _AzureContainerClient(
        account_url=account_url,
        container_name=container,
        credential=credential,
    ) as client:
        walker = _azure_blob.list_blobs(  # type: ignore[union-attr]
            client,
            prefix=prefix,
            path_matcher=path_matcher,
        )
        async for _path, _blob in walker.items():
            _blob_items.append((_path, _blob))
    logger.info("[azure_blob] found %d blob(s) in %s", len(_blob_items), container)
    return iter(_blob_items)


# ─────────────────────────────────────────────────────────────────────────────
# App E — GraphRAGGoogleDrive  (native CocoIndex google_drive connector)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from cocoindex.connectors import google_drive as _google_drive
    _GOOGLE_DRIVE_AVAILABLE = True
except ImportError:
    _GOOGLE_DRIVE_AVAILABLE = False
    _google_drive = None  # type: ignore[assignment]


@coco.fn(memo=True)  # type: ignore[misc]
async def process_google_drive_file(file: Any, cfg_json: str) -> str:
    """Process one Google Drive file via CocoIndex's native connector."""
    import traceback as _tb
    try:
        file_bytes: bytes = await file.read()
        file_id = str(file.file_path.resolve())
        file_name = str(file.file_path.path).replace("\\", "/").rsplit("/", 1)[-1] or file_id
        cfg = json.loads(cfg_json)
        file_path = file_id
        modified_at = str(getattr(file, "mtime", getattr(file, "modified_time", "")))
        source_metadata = {
            "file_id": file_id,
            "folder_ids": cfg.get("root_folder_ids", cfg.get("folder_ids", [])),
            "source": "google_drive",
        }
        _run._emit_progress(event="file_stage", file_name=file_name, file_path=file_path, stage="downloaded")
        await _run._run_pipeline(
            file_bytes, file_name, file_path, "google_drive", modified_at, cfg, source_metadata,
        )
        return f"ok:{file_name}"
    except Exception as _exc:
        _err_msg = (
            f"[CocoIndex Google Drive pipeline ERROR] {type(_exc).__name__}: {_exc}\n"
            + _tb.format_exc()
        )
        logger.error(_err_msg)
        with open("cocoindex_pipeline_error.log", "a", encoding="utf-8") as _ef:
            _ef.write(_err_msg + "\n")
        _emit_worker_failure(file, _exc)
        return f"error:{type(_exc).__name__}:{_exc}"


async def _list_google_drive_items(cfg: Dict[str, Any]) -> "Any | None":
    """List Google Drive files via the native connector (or ``None`` if unavailable).

    Credential path and root folder ids are resolved from ``cfg`` (already
    populated by :func:`_prepare_native_source_cfg`), falling back to the raw
    ``connection_params`` keys.
    """
    if not _GOOGLE_DRIVE_AVAILABLE:
        logger.error("cocoindex.connectors.google_drive not available — cannot list Drive")
        return None
    cred_path = str(cfg.get("service_account_credential_path", "") or "")
    folder_ids = cfg.get("root_folder_ids") or cfg.get("folder_ids") or []
    if isinstance(folder_ids, str):
        try:
            folder_ids = json.loads(folder_ids)
        except Exception:
            folder_ids = [folder_ids] if folder_ids else []
    logger.info("[google_drive] listing %d folder(s)", len(folder_ids))
    source = _google_drive.GoogleDriveSource(  # type: ignore[union-attr]
        service_account_credential_path=cred_path,
        root_folder_ids=folder_ids,
    )
    return source.items()


# ─────────────────────────────────────────────────────────────────────────────
# Native-source config preparation + registry
# ─────────────────────────────────────────────────────────────────────────────

def _prepare_native_source_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Fill in derived native-source keys on *cfg* in place, then return it.

    Called once by :func:`flexible_app.build_app_for_config` before serialising
    ``cfg_json`` so both the lister and the per-file worker see the same values:

    * **azure_blob** — derive ``account_url`` from ``account_name`` when missing.
    * **google_drive** — resolve ``service_account_credential_path`` and
      ``root_folder_ids`` from the raw config / env.

    localfs and s3 need no derivation (all keys read directly with fallbacks).
    """
    data_source = str(cfg.get("data_source", "")).lower()
    if data_source == "azure_blob":
        account_url = str(cfg.get("account_url", ""))
        account_name = str(cfg.get("account_name", ""))
        if not account_url and account_name:
            cfg["account_url"] = f"https://{account_name}.blob.core.windows.net"
        cfg["container_name"] = str(cfg.get("container_name", cfg.get("container", "")))
    elif data_source == "google_drive":
        try:
            from cocoindex_integration.connectors.cocoindex.sources.google_drive import (  # noqa: PLC0415
                _resolve_gdrive_credential_path,
                _resolve_gdrive_folder_ids,
                _resolve_google_drive_config,
            )
            cfg.update(_resolve_google_drive_config(cfg))
            cfg["service_account_credential_path"] = _resolve_gdrive_credential_path(cfg)
            cfg["root_folder_ids"] = _resolve_gdrive_folder_ids(cfg)
        except Exception as _exc:  # pragma: no cover - defensive
            logger.warning("[google_drive] could not resolve credentials/folders: %s", _exc)
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# Flexible-target delete observation for CocoIndex-native live sources
#
# WHY THIS EXISTS
# ---------------
# A CocoIndex-native source (localfs today; S3/Azure/Drive if they ever become
# live-watchable) drives ``coco.mount_each`` from its OWN ``LiveMapView``.  On a
# live delete, CocoIndex's view calls ``subscriber.delete(key)``; native targets
# (Qdrant, Neo4j) reconcile automatically, but flexible root
# ``TargetStateProviders`` (Elasticsearch, GraphDB RDF, …) only reconcile during
# a full ``app.update()`` catch-up — which the FastAPI bridge runs periodically
# but the CLI ``-L`` live mode does not.
#
# The fix wraps the source's *own* ``LiveMapView`` and observes the SAME
# ``subscriber.delete(key)`` signal CocoIndex emits — no separate flexible-source
# detector, and source-agnostic (any ``LiveMapFeed`` is covered automatically).
# For non-live iterables (S3/Azure/Drive today) the wrapper is a no-op passthrough
# because deletions there are handled by the catch-up reconcile.
# ─────────────────────────────────────────────────────────────────────────────

#: NATIVE_READERS key → the ``source_type`` string ``_run_pipeline`` uses when it
#: computes ``doc_id`` (so the wrapper derives the identical UUID).
_DOCID_SOURCE_TYPE: Dict[str, str] = {
    "filesystem": "filesystem",
    "localfs": "filesystem",
    "s3": "s3",
    "amazon_s3": "s3",
    "azure_blob": "azure_blob",
    "google_drive": "google_drive",
}


def _native_file_path_for(source_type: str, file: Any, cfg: Dict[str, Any]) -> "str | None":
    """Recompute the ``file_path`` a per-file worker passes to ``_run_pipeline``.

    Mirrors each ``process_*_file`` worker exactly so the derived ``doc_id``
    matches what was written at ingest time.  Returns ``None`` if the file object
    doesn't expose the expected shape.
    """
    try:
        if source_type == "filesystem":
            return str(file.file_path.resolve())
        if source_type == "s3":
            bucket = str(file.file_path.bucket_name)
            resolved_key = str(file.file_path.resolve())
            return f"s3://{bucket}/{resolved_key}"
        if source_type == "azure_blob":
            container = str(cfg.get("container_name", cfg.get("container", "")))
            resolved_name = str(file.file_path.resolve())
            return f"{container}/{resolved_name}" if container else resolved_name
        if source_type == "google_drive":
            return str(file.file_path.resolve())
    except Exception:  # noqa: BLE001
        return None
    return None


def _native_doc_id_for_file(source_type: str, file: Any, cfg: Dict[str, Any]) -> "str | None":
    """Derive the ingest ``doc_id`` (``uuid5(source_type:file_path)``) for *file*."""
    fp = _native_file_path_for(source_type, file, cfg)
    if not fp:
        return None
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{source_type}:{fp}"))


class _DeleteObservingSubscriber:
    """Proxy ``LiveMapSubscriber`` that forwards deletes to flexible targets.

    Delegates every subscriber call to the real CocoIndex subscriber unchanged;
    additionally caches ``key → doc_id`` on ``update`` and, on ``delete``, calls
    ``delete_row(doc_id)`` on the flexible connectors before forwarding.
    """

    def __init__(self, inner: Any, view: "_DeleteObservingLiveMapView") -> None:
        self._inner = inner
        self._view = view

    async def update_all(self) -> Any:
        return await self._inner.update_all()

    async def mark_ready(self) -> Any:
        return await self._inner.mark_ready()

    async def update(self, key: Any, value: Any) -> Any:
        doc_id = _native_doc_id_for_file(self._view._source_type, value, self._view._cfg)
        if doc_id:
            self._view._key_to_doc[key] = doc_id
        return await self._inner.update(key, value)

    async def delete(self, key: Any) -> Any:
        doc_id = self._view._key_to_doc.pop(key, None)
        if doc_id:
            try:
                from cocoindex_integration.connectors.flexible._map_view import (  # noqa: PLC0415
                    _delete_flexible_targets_with_doc_id,
                )
                await _delete_flexible_targets_with_doc_id(doc_id, description=str(key))
            except Exception as _exc:  # noqa: BLE001
                logger.error(
                    "[native/del-observe] flexible delete failed for key=%s: %s",
                    key, _exc,
                )
        else:
            logger.debug(
                "[native/del-observe] no cached doc_id for deleted key=%s "
                "(not ingested this run) — flexible targets untouched", key,
            )
        return await self._inner.delete(key)

    # Optional subscriber methods used by some views — delegate transparently.
    async def read_committed_state(self, key: Any) -> Any:
        return await self._inner.read_committed_state(key)

    async def write_committed_state(self, key: Any, value: Any) -> Any:
        return await self._inner.write_committed_state(key, value)


class _DeleteObservingLiveMapView:
    """Wrap a CocoIndex-native ``LiveMapView`` to also delete flexible targets.

    Implements the ``LiveMapView`` protocol (``__aiter__`` + ``watch``) so
    ``coco.mount_each`` still treats it as a live source.  ``__aiter__`` and the
    proxied subscriber's ``update`` populate a ``key → doc_id`` cache; the proxied
    ``delete`` uses it to remove the doc from the flexible connectors.
    """

    def __init__(self, inner: Any, source_type: str, cfg: Dict[str, Any]) -> None:
        self._inner = inner
        self._source_type = source_type
        self._cfg = cfg
        self._key_to_doc: Dict[Any, str] = {}

    def __aiter__(self) -> Any:
        return self._aiter_impl()

    async def _aiter_impl(self) -> Any:
        async for key, file in self._inner:
            doc_id = _native_doc_id_for_file(self._source_type, file, self._cfg)
            if doc_id:
                self._key_to_doc[key] = doc_id
            yield key, file

    async def watch(self, subscriber: Any) -> None:
        await self._inner.watch(_DeleteObservingSubscriber(subscriber, self))


def wrap_native_view_for_deletes(
    items: Any, source_type: str, cfg: Dict[str, Any]
) -> Any:
    """Wrap a live-watchable native view so flexible targets receive deletes.

    Returns *items* unchanged unless it is a live ``LiveMapFeed`` (only those
    emit ``subscriber.delete``).  Plain iterables (S3/Azure/Drive today) are
    reconciled by the catch-up ``app.update()`` path and need no wrapper.
    """
    try:
        from cocoindex._internal.live_component import LiveMapFeed  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        return items
    if not isinstance(items, LiveMapFeed):
        return items
    docid_source = _DOCID_SOURCE_TYPE.get(source_type, source_type)
    logger.info(
        "[native/del-observe] wrapping live '%s' view — flexible targets will "
        "receive live delete signals from CocoIndex's own source",
        source_type,
    )
    return _DeleteObservingLiveMapView(items, docid_source, cfg)


# source_type → (async lister, @coco.fn per-file worker).  Consumed by
# flexible_app.flexible_app_main when ``source_backend == "cocoindex"``.
NATIVE_READERS: Dict[str, tuple] = {
    "filesystem": (_list_localfs_items, process_localfs_file),
    "localfs": (_list_localfs_items, process_localfs_file),
    "s3": (_list_s3_items, process_s3_file),
    "amazon_s3": (_list_s3_items, process_s3_file),
    "azure_blob": (_list_azure_blob_items, process_azure_blob_file),
    "google_drive": (_list_google_drive_items, process_google_drive_file),
}
