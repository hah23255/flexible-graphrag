"""
Bytes-capture extractor for CocoIndex pipeline.

Reads raw file bytes at the point the LlamaIndex reader has the file available
(fsspec remote open for S3/GCS, local temp file for Azure Blob, local path for
filesystem) — WITHOUT calling DocumentProcessor or any parser.

This is the CocoIndex-specific counterpart of PassthroughExtractor:
  PassthroughExtractor  -> captures path + _fs, defers download to DocumentProcessor
  BytesCaptureExtractor -> captures raw bytes NOW, never calls DocumentProcessor

Usage in sources/_s3.py, _gcs.py, _azure_blob.py:
    extractor = BytesCaptureExtractor()
    placeholder_docs = source.get_placeholder_docs(extractor=extractor)
    # placeholder_docs[i].metadata["raw_bytes"] contains the file bytes
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

from llama_index.core import Document
from llama_index.core.readers.base import BaseReader
from llama_index.core.readers.file.base import is_default_fs

logger = logging.getLogger(__name__)


class BytesCaptureExtractor(BaseReader):
    """
    Drop-in replacement for PassthroughExtractor that captures raw file bytes
    immediately at reader time rather than deferring to DocumentProcessor.

    Called by LlamaIndex readers (S3Reader, GCSReader, AzStorageBlobReader,
    SimpleDirectoryReader) for each file via the ``file_extractor`` map.

    Behaviour per source type
    -------------------------
    * S3 / GCS (fsspec remote):
        Reader provides an ``fs`` AbstractFileSystem in kwargs.
        ``fs.open(path, "rb").read()`` downloads the bytes in-line.
    * Azure Blob (local temp file):
        Reader downloads to a temp dir, calls extractor, then deletes temp dir.
        We read from the local ``file_path`` immediately while the file exists.
    * Filesystem (local):
        Same as Azure Blob — ``Path(file_path).read_bytes()``.

    Returns
    -------
    A single ``Document`` with:
      * ``text = ""``   — no text extraction, no parsing
      * ``metadata["raw_bytes"]``  — the raw file bytes
      * ``metadata["file_path"]``  — stable cloud path if reader provides it,
                                    otherwise the local/temp path
      * ``metadata["file_name"]``  — original filename (not a temp name)
    """

    def load_data(
        self,
        file_path: Path,
        extra_info: Optional[Dict] = None,
        **kwargs,
    ) -> List[Document]:
        """Capture raw bytes; return a placeholder Document with metadata only."""
        fs = kwargs.get("fs", None)

        # ── Determine the stable file name ───────────────────────────────────
        file_name = _best_file_name(file_path, extra_info)

        # ── Read raw bytes ────────────────────────────────────────────────────
        raw_bytes: Optional[bytes] = None

        if fs is not None and not is_default_fs(fs):
            # Remote fsspec filesystem (S3 via S3FS, GCS via GCSFileSystem, …)
            try:
                with fs.open(str(file_path), "rb") as fh:
                    raw_bytes = fh.read()
                logger.debug(
                    "BytesCaptureExtractor: read %d bytes via fsspec from %s",
                    len(raw_bytes),
                    file_path,
                )
            except Exception as exc:
                logger.warning(
                    "BytesCaptureExtractor: fsspec read failed for %s: %s — returning empty bytes",
                    file_path,
                    exc,
                )
                raw_bytes = b""
        else:
            # Local path (filesystem source or Azure Blob temp file)
            local_path = Path(str(file_path))
            try:
                raw_bytes = local_path.read_bytes()
                logger.debug(
                    "BytesCaptureExtractor: read %d bytes from local %s",
                    len(raw_bytes),
                    local_path,
                )
            except Exception as exc:
                logger.warning(
                    "BytesCaptureExtractor: local read failed for %s: %s — returning empty bytes",
                    local_path,
                    exc,
                )
                raw_bytes = b""

        # ── Build stable file_path ────────────────────────────────────────────
        # Readers (AzStorageBlobReader, GCSReader) store the stable cloud path
        # in extra_info["file_path"].  Prefer that over the temp/fsspec path.
        stable_path = (
            (extra_info or {}).get("file_path")
            or str(file_path)
        )

        metadata: Dict = {
            "file_path": stable_path,
            "file_name": file_name,
            "raw_bytes": raw_bytes,
            **(extra_info or {}),
        }
        # raw_bytes is now in metadata; ensure the extra_info copy doesn't
        # overwrite it with a stale value if extra_info also had "raw_bytes".
        metadata["raw_bytes"] = raw_bytes

        return [Document(text="", metadata=metadata)]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _best_file_name(file_path: Path, extra_info: Optional[Dict]) -> str:
    """
    Pick the most human-readable filename from the available sources.

    Priority:
      1. ``extra_info["file_name"]``     — reader-supplied original name
      2. ``extra_info["file path"]``     — Google Drive style (with space)
      3. basename of ``extra_info["file_path"]`` — stable cloud path
      4. basename of ``file_path``       — fallback
    """
    if extra_info:
        if name := extra_info.get("file_name"):
            return name
        if path_with_space := extra_info.get("file path"):
            return os.path.basename(path_with_space)
        if stable := extra_info.get("file_path"):
            return os.path.basename(stable)
    return Path(str(file_path)).name
