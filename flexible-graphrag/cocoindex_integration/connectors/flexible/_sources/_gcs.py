"""
Google Cloud Storage source iterator for the CocoIndex pipeline.

Uses the existing ``GCSSource`` from ``sources/gcs.py`` (which owns all the
credential resolution and GCSReader setup) together with
``BytesCaptureExtractor`` to collect raw file bytes via the reader's fsspec
``_fs`` object — without calling DocumentProcessor or any parser.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, AsyncIterator, Dict

logger = logging.getLogger(__name__)


async def iter_gcs(config: Dict[str, Any]):
    """
    Yield ``SourceItem`` objects for every blob in the configured GCS bucket.

    Flow
    ----
    1. ``GCSSource(config)`` — resolves service account credentials, bucket, prefix.
    2. ``get_placeholder_docs(extractor=BytesCaptureExtractor())`` — calls
       ``GCSReader.load_data()``.  For each blob the reader opens a GCSFileSystem
       handle; ``BytesCaptureExtractor`` downloads raw bytes via
       ``fs.open(path, "rb").read()``.
    3. Each placeholder doc's ``metadata["raw_bytes"]`` is placed in
       ``SourceItem._raw_bytes`` so ``parse_document`` is the *only* parse step.
    """
    from cocoindex_integration.connectors.flexible.source import SourceItem
    from sources.bytes_capture_extractor import BytesCaptureExtractor
    from sources.gcs import GCSSource

    src = GCSSource(config)
    extractor = BytesCaptureExtractor()

    try:
        placeholder_docs = await asyncio.to_thread(
            src.get_placeholder_docs, extractor
        )
    except Exception as exc:
        logger.error("GCS source error: %s", exc)
        return

    bucket = src.bucket
    for doc in placeholder_docs:
        raw_bytes = doc.metadata.pop("raw_bytes", None)
        file_path = doc.metadata.get("file_path", "")
        file_name = doc.metadata.get("file_name", os.path.basename(file_path))
        # GCSReader returns paths as "bucket/blob_name"
        blob_name = (
            file_path.replace(f"{bucket}/", "", 1)
            if file_path.startswith(f"{bucket}/")
            else file_path
        )
        key = f"gs://{bucket}/{blob_name}"
        _, ext = os.path.splitext(file_name)
        modified_at = (
            doc.metadata.get("modified_at")
            or doc.metadata.get("updated")
            or ""
        )
        yield SourceItem(
            key=key,
            file_name=file_name,
            file_path=key,
            file_type=ext.lstrip(".").lower(),
            source_type="gcs",
            modified_at=modified_at,
            metadata={
                "source": "gcs",
                "bucket": bucket,
                "prefix": getattr(src, "prefix", "") or "",
                "source_type": "gcs_blob",
            },
            _raw_bytes=raw_bytes,
        )


# ---------------------------------------------------------------------------
# Phase 2 lazy API — list metadata (no download) + single-key download.
# ---------------------------------------------------------------------------

async def list_metadata(config: Dict[str, Any]):
    """List GCS object metadata via the GCS detector (no downloads)."""
    from ._lazy import list_metadata as _list_metadata
    return await _list_metadata("gcs", config)


def build_source(config: Dict[str, Any]):
    """Build a cached ``GCSSource`` for single-key downloads."""
    from ._lazy import build_source as _build_source
    return _build_source("gcs", config)


def download_one(source: Any, download_key: str) -> bytes:
    """Download one GCS object's raw bytes via ``GCSSource.read_file_bytes``."""
    from ._lazy import download_one as _download_one
    return _download_one(source, "gcs", download_key)
