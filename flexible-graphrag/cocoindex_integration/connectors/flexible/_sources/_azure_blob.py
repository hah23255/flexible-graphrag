"""
Azure Blob Storage source iterator for the CocoIndex pipeline.

Uses the existing ``AzureBlobSource`` from ``sources/azure_blob.py`` (which
owns all the credential resolution and AzStorageBlobReader setup) together with
``BytesCaptureExtractor`` to collect raw file bytes.

Azure Blob caveat
-----------------
``AzStorageBlobReader`` downloads each blob to a **temporary directory** and
deletes that directory after the extractor is called.  ``BytesCaptureExtractor``
reads the bytes from the local temp file *inside* the extractor call, before the
temp dir is deleted — this is why we cannot use the two-step
"placeholder doc → process_documents_from_metadata" pattern used for S3/GCS.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, AsyncIterator, Dict

logger = logging.getLogger(__name__)


async def iter_azure_blob(config: Dict[str, Any]):
    """
    Yield ``SourceItem`` objects for every blob in the configured container.

    Flow
    ----
    1. ``AzureBlobSource(config)`` — resolves credentials, container.
    2. ``get_placeholder_docs(extractor=BytesCaptureExtractor())`` — calls
       ``AzStorageBlobReader.load_data()``.  For each blob the reader:
         a. Downloads the blob to a temp file on disk.
         b. Calls ``BytesCaptureExtractor.load_data(temp_path, extra_info)``.
            The extractor reads bytes from the temp file right now.
         c. Deletes the temp file.
       The placeholder doc carries ``metadata["raw_bytes"]`` = the blob bytes.
    3. Each placeholder doc's ``metadata["raw_bytes"]`` is placed in
       ``SourceItem._raw_bytes`` so ``parse_document`` is the *only* parse step.
    """
    from cocoindex_integration.connectors.flexible.source import SourceItem
    from sources.azure_blob import AzureBlobSource
    from sources.bytes_capture_extractor import BytesCaptureExtractor

    src = AzureBlobSource(config)
    extractor = BytesCaptureExtractor()

    try:
        placeholder_docs = await asyncio.to_thread(
            src.get_placeholder_docs, extractor
        )
    except Exception as exc:
        logger.error("Azure Blob source error: %s", exc)
        return

    container = src.container_name
    for doc in placeholder_docs:
        raw_bytes = doc.metadata.pop("raw_bytes", None)
        # AzStorageBlobReader stores the stable blob name in extra_info["file_path"]
        blob_name = doc.metadata.get("file_path", "")
        file_name = doc.metadata.get("file_name", os.path.basename(blob_name))
        stable_key = f"{container}/{blob_name}" if blob_name else file_name
        _, ext = os.path.splitext(file_name)
        modified_at = (
            doc.metadata.get("modified_at")
            or doc.metadata.get("last_modified")
            or ""
        )
        yield SourceItem(
            key=stable_key,
            file_name=file_name,
            file_path=stable_key,
            file_type=ext.lstrip(".").lower(),
            source_type="azure_blob",
            modified_at=modified_at,
            metadata={
                "source": "azure_blob",
                "container_name": container,
                "source_type": "azure_blob_object",
            },
            _raw_bytes=raw_bytes,
        )


# ---------------------------------------------------------------------------
# Phase 2 lazy API — list metadata (no download) + single-key download.
# ---------------------------------------------------------------------------

async def list_metadata(config: Dict[str, Any]):
    """List Azure blob metadata via the Azure Blob detector (no downloads)."""
    from ._lazy import list_metadata as _list_metadata
    return await _list_metadata("azure_blob", config)


def build_source(config: Dict[str, Any]):
    """Build a cached ``AzureBlobSource`` for single-key downloads."""
    from ._lazy import build_source as _build_source
    return _build_source("azure_blob", config)


def download_one(source: Any, download_key: str) -> bytes:
    """Download one blob's raw bytes via ``AzureBlobSource.read_file_bytes``."""
    from ._lazy import download_one as _download_one
    return _download_one(source, "azure_blob", download_key)
