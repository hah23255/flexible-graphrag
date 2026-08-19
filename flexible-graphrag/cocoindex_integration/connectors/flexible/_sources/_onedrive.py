"""OneDrive source — Phase 2 lazy listing + single-key download.

Uses the Microsoft Graph change detector for metadata listing (no downloads)
and ``OneDriveSource.read_file_bytes(file_id)`` (reusing cached credentials and
the LlamaIndex ``OneDriveReader``) for single-file downloads.
"""

from __future__ import annotations

from typing import Any, Dict


async def list_metadata(config: Dict[str, Any]):
    """List OneDrive file metadata via the Microsoft Graph detector."""
    from ._lazy import list_metadata as _list_metadata
    return await _list_metadata("onedrive", config)


def build_source(config: Dict[str, Any]):
    """Build a cached ``OneDriveSource`` for single-key downloads."""
    from ._lazy import build_source as _build_source
    return _build_source("onedrive", config)


def download_one(source: Any, download_key: str) -> bytes:
    """Download one OneDrive file's raw bytes via ``read_file_bytes``."""
    from ._lazy import download_one as _download_one
    return _download_one(source, "onedrive", download_key)
