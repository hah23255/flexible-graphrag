"""Box source — Phase 2 lazy listing + single-key download.

Uses the Box change detector for metadata listing (no downloads) and
``BoxSource.read_file_bytes(file_id)`` (reusing the cached Box client and the
LlamaIndex ``BoxReader``) for single-file downloads.
"""

from __future__ import annotations

from typing import Any, Dict


async def list_metadata(config: Dict[str, Any]):
    """List Box file metadata via the Box detector (no downloads)."""
    from ._lazy import list_metadata as _list_metadata
    return await _list_metadata("box", config)


def build_source(config: Dict[str, Any]):
    """Build a cached ``BoxSource`` for single-key downloads."""
    from ._lazy import build_source as _build_source
    return _build_source("box", config)


def download_one(source: Any, download_key: str) -> bytes:
    """Download one Box file's raw bytes via ``BoxSource.read_file_bytes``."""
    from ._lazy import download_one as _download_one
    return _download_one(source, "box", download_key)
