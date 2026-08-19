"""
Amazon S3 source iterator for the CocoIndex pipeline.

Uses the existing ``S3Source`` from ``sources/s3.py`` (which owns all the
credential resolution, region logic, and S3Reader setup) together with
``BytesCaptureExtractor`` to collect raw file bytes via the reader's fsspec
``_fs`` object — without calling DocumentProcessor or any parser.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, AsyncIterator, Dict

logger = logging.getLogger(__name__)


async def iter_s3(config: Dict[str, Any]):
    """
    Yield ``SourceItem`` objects for every object in the configured S3 bucket.

    Flow
    ----
    1. ``S3Source(config)`` — resolves credentials, bucket, prefix.
    2. ``get_placeholder_docs(extractor=BytesCaptureExtractor())`` — calls
       ``S3Reader.load_data()``.  For each S3 object the reader opens an fsspec
       ``S3FileSystem`` handle; ``BytesCaptureExtractor`` downloads raw bytes
       via ``fs.open(path, "rb").read()`` *before* the reader closes the handle.
    3. Each placeholder doc's ``metadata["raw_bytes"]`` is placed in
       ``SourceItem._raw_bytes`` so ``parse_document`` in the pipeline is the
       *only* parse step.
    """
    from cocoindex_integration.connectors.flexible.source import SourceItem
    from sources.bytes_capture_extractor import BytesCaptureExtractor
    from sources.s3 import S3Source

    src = S3Source(config)
    extractor = BytesCaptureExtractor()

    try:
        placeholder_docs = await asyncio.to_thread(
            src.get_placeholder_docs, extractor
        )
    except Exception as exc:
        logger.error("S3 source error: %s", exc)
        return

    bucket = src.bucket_name
    for doc in placeholder_docs:
        raw_bytes = doc.metadata.pop("raw_bytes", None)
        file_path = doc.metadata.get("file_path", "")
        file_name = doc.metadata.get("file_name", os.path.basename(file_path))
        # S3Reader returns paths as "bucket/key"; strip bucket prefix to get key
        s3_key = (
            file_path.replace(f"{bucket}/", "", 1)
            if file_path.startswith(f"{bucket}/")
            else file_path
        )
        key = f"s3://{bucket}/{s3_key}"
        _, ext = os.path.splitext(file_name)
        modified_at = (
            doc.metadata.get("modified_at")
            or doc.metadata.get("last_modified")
            or ""
        )
        yield SourceItem(
            key=key,
            file_name=file_name,
            file_path=key,
            file_type=ext.lstrip(".").lower(),
            source_type="s3",
            modified_at=modified_at,
            metadata={
                "source": "s3",
                "bucket_name": bucket,
                "prefix": getattr(src, "prefix", "") or "",
                "region": getattr(src, "region_name", "") or "",
                "source_type": "s3_object",
            },
            _raw_bytes=raw_bytes,
        )


# ---------------------------------------------------------------------------
# Phase 2 lazy API — list metadata (no download) + single-key download.
# Delegates to the shared ``_lazy`` helper bound to source_type="s3".
# ---------------------------------------------------------------------------

async def list_metadata(config: Dict[str, Any]):
    """List S3 object metadata via the S3 detector (no downloads)."""
    from ._lazy import list_metadata as _list_metadata
    return await _list_metadata("s3", config)


def build_source(config: Dict[str, Any]):
    """Build a cached ``S3Source`` for single-key downloads."""
    from ._lazy import build_source as _build_source
    return _build_source("s3", config)


def download_one(source: Any, download_key: str) -> bytes:
    """Download one S3 object's raw bytes via ``S3Source.read_file_bytes``."""
    from ._lazy import download_one as _download_one
    return _download_one(source, "s3", download_key)
