"""CocoIndex-native Google Drive source connector."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from cocoindex_integration.connectors.cocoindex.base import CocoSource

logger = logging.getLogger(__name__)


def _google_drive_native_available() -> bool:
    try:
        from cocoindex.connectors import google_drive  # noqa: F401, PLC0415
        return True
    except Exception:  # noqa: BLE001
        return False


@dataclass
class CocoGoogleDrive(CocoSource):
    """Descriptor for the native CocoIndex Google Drive source (read-only)."""

    name = "google_drive"
    can_read = True
    can_write = False

    credential_path: str = ""
    root_folder_ids: List[str] = field(default_factory=list)


def _resolve_gdrive_credential_path(cfg: Dict[str, Any]) -> str:
    """Return a filesystem path to service-account JSON credentials."""
    for _key in (
        "service_account_credential_path",
        "credentials_file",
        "credentials_path",
    ):
        _path = str(cfg.get(_key, "") or "")
        if _path and os.path.isfile(_path):
            return _path
    _env_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if _env_path and os.path.isfile(_env_path):
        return _env_path
    _creds = cfg.get("credentials", "")
    if not _creds:
        return ""
    try:
        if isinstance(_creds, str):
            _parsed = json.loads(_creds)
        else:
            _parsed = _creds
        _fd, _path = tempfile.mkstemp(suffix=".json", prefix="gdrive-sa-")
        with os.fdopen(_fd, "w", encoding="utf-8") as _fh:
            json.dump(_parsed, _fh)
        return _path
    except (json.JSONDecodeError, OSError, TypeError) as exc:
        logger.warning("[coco] google_drive: invalid inline credentials: %s", exc)
        return ""


def _resolve_gdrive_folder_ids(cfg: Dict[str, Any]) -> List[str]:
    _ids = cfg.get("root_folder_ids") or cfg.get("folder_ids")
    if isinstance(_ids, str):
        _ids = [_x.strip() for _x in _ids.split(",") if _x.strip()]
    if isinstance(_ids, list) and _ids:
        return [str(_x) for _x in _ids if str(_x).strip()]
    _folder = str(cfg.get("folder_id", "") or "")
    return [_folder] if _folder else []


def _resolve_google_drive_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(cfg or {})
    for _k, _v in os.environ.items():
        if _k.startswith("GOOGLE_DRIVE_"):
            merged[_k[13:].lower()] = _v
    merged.setdefault("credentials_file", os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""))
    return merged


def build_google_drive(cfg: Dict[str, Any]) -> Optional[CocoGoogleDrive]:
    """Build a :class:`CocoGoogleDrive` descriptor."""
    resolved = _resolve_google_drive_config(cfg)
    cred_path = _resolve_gdrive_credential_path(resolved)
    folder_ids = _resolve_gdrive_folder_ids(resolved)
    src = CocoGoogleDrive(credential_path=cred_path, root_folder_ids=folder_ids)
    src.native_available = (
        _google_drive_native_available()
        and bool(cred_path)
        and bool(folder_ids)
    )
    if src.native_available:
        logger.info(
            "[coco] google_drive: native CocoIndex Google Drive connector ready "
            "(folders=%s)",
            folder_ids,
        )
    elif _google_drive_native_available() and (not cred_path or not folder_ids):
        logger.warning(
            "[coco] google_drive: credentials or folder_id missing — "
            "using FlexibleDataSource('google_drive')"
        )
    else:
        logger.warning(
            "[coco] google_drive: native connector unavailable — "
            "using FlexibleDataSource('google_drive')"
        )
    return src
