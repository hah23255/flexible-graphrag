"""
Filesystem source iterator for the CocoIndex pipeline.

Uses ``pathlib.Path.read_bytes()`` directly — no LlamaIndex reader needed for
local files, no DocumentProcessor, no double-parse.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List

logger = logging.getLogger(__name__)


def _collect_files(path_cfg: Any) -> List[str]:
    """Return sorted absolute file paths from a path config (string, list, or None)."""
    if isinstance(path_cfg, list):
        roots = [Path(p).resolve() for p in path_cfg]
    elif path_cfg:
        roots = [Path(path_cfg).resolve()]
    else:
        roots = [Path(".").resolve()]

    result: List[str] = []
    for root in roots:
        if root.is_file():
            result.append(str(root))
        elif root.is_dir():
            result.extend(sorted(str(f) for f in root.rglob("*") if f.is_file()))
        else:
            logger.warning("Filesystem source: path does not exist: %s", root)
    return result


async def iter_filesystem(config: Dict[str, Any]):
    """
    Yield ``SourceItem`` objects for every file under the configured path(s).

    Reads raw bytes via ``Path.read_bytes()`` so ``parse_document`` in the
    CocoIndex pipeline is the *only* parse step — no double-parse.

    Accepts deep directory trees; all files are discovered recursively.
    """
    # Import here to avoid circular imports at module load time
    from cocoindex_integration.connectors.flexible.source import SourceItem

    path_cfg = config.get("path") or config.get("paths") or "."
    file_paths = await asyncio.to_thread(_collect_files, path_cfg)

    for abs_path in file_paths:
        fp = Path(abs_path)
        try:
            raw = await asyncio.to_thread(fp.read_bytes)
        except OSError as exc:
            logger.warning("Filesystem: cannot read %s: %s", fp, exc)
            continue

        try:
            stat = fp.stat()
            modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
        except OSError:
            modified_at = ""

        fname = fp.name
        _, ext = os.path.splitext(fname)
        yield SourceItem(
            key=f"file://{fp}",
            file_name=fname,
            file_path=str(fp),
            file_type=ext.lstrip(".").lower(),
            source_type="filesystem",
            modified_at=modified_at,
            _raw_bytes=raw,
        )


# ---------------------------------------------------------------------------
# Phase 2 lazy API — list metadata (no read) + single-key read.
# ---------------------------------------------------------------------------

async def list_metadata(config: Dict[str, Any]):
    """List local file metadata via the filesystem detector (no reads)."""
    from ._lazy import list_metadata as _list_metadata
    return await _list_metadata("filesystem", config)


def build_source(config: Dict[str, Any]):
    """Build a ``FileSystemSource`` for single-key reads."""
    from ._lazy import build_source as _build_source
    return _build_source("filesystem", config)


def download_one(source: Any, download_key: str) -> bytes:
    """Read one local file's raw bytes via ``FileSystemSource.read_file_bytes``."""
    from ._lazy import download_one as _download_one
    return _download_one(source, "filesystem", download_key)
