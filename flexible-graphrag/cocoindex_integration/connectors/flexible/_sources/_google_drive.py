"""Google Drive source — Phase 2 lazy listing + single-key download.

Uses the Google Drive change detector for metadata listing (no downloads) and
``GoogleDriveSource.read_file_bytes(file_id)`` (reusing cached credentials and
the LlamaIndex ``GoogleDriveReader``) for single-file downloads.
"""

from __future__ import annotations

from typing import Any, Dict


async def list_metadata(config: Dict[str, Any]):
    """List Google Drive file metadata via the detector (no downloads)."""
    from ._lazy import list_metadata as _list_metadata
    return await _list_metadata("google_drive", config)


def build_source(config: Dict[str, Any]):
    """Build a cached ``GoogleDriveSource`` for single-key downloads."""
    from ._lazy import build_source as _build_source
    return _build_source("google_drive", config)


def download_one(source: Any, download_key: str) -> bytes:
    """Download one Drive file's raw bytes via ``read_file_bytes``."""
    from ._lazy import download_one as _download_one
    return _download_one(source, "google_drive", download_key)
