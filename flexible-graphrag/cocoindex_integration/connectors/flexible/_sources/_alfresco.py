"""Alfresco source — Phase 2 lazy listing + single-key download.

Uses the Alfresco change detector for metadata listing (no downloads) and
``AlfrescoSource.read_file_bytes(node_id)`` (reusing python-alfresco-api
``content_utils.download_file``) for single-file downloads.
"""

from __future__ import annotations

from typing import Any, Dict


async def list_metadata(config: Dict[str, Any]):
    """List Alfresco document metadata via the Alfresco detector."""
    from ._lazy import list_metadata as _list_metadata
    return await _list_metadata("alfresco", config)


def build_source(config: Dict[str, Any]):
    """Build a cached ``AlfrescoSource`` for single-key downloads."""
    from ._lazy import build_source as _build_source
    return _build_source("alfresco", config)


def download_one(source: Any, download_key: str) -> bytes:
    """Download one Alfresco document's raw bytes via ``read_file_bytes``."""
    from ._lazy import download_one as _download_one
    return _download_one(source, "alfresco", download_key)
