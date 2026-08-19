"""Nuxeo source — lazy listing + single-key download.

Mirrors ``_alfresco.py``: the Nuxeo change detector supplies metadata (no
downloads) and ``NuxeoSource.read_file_bytes(uid)`` fetches one document's bytes
on demand — the main blob for File documents, the inline ``note:note`` text for
Note documents.
"""

from __future__ import annotations

from typing import Any, Dict


async def list_metadata(config: Dict[str, Any]):
    """List Nuxeo document metadata via the Nuxeo detector."""
    from ._lazy import list_metadata as _list_metadata
    return await _list_metadata("nuxeo", config)


def build_source(config: Dict[str, Any]):
    """Build a cached ``NuxeoSource`` for single-key downloads."""
    from ._lazy import build_source as _build_source
    return _build_source("nuxeo", config)


def download_one(source: Any, download_key: str) -> bytes:
    """Download one Nuxeo document's raw bytes via ``read_file_bytes``."""
    from ._lazy import download_one as _download_one
    return _download_one(source, "nuxeo", download_key)
