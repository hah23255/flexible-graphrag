"""CocoIndex-native local filesystem source connector.

``CocoLocalFileSystem`` wraps CocoIndex's own ``cocoindex.connectors.localfs``
reader, which gives CocoIndex-native change detection (add / modify / delete by
path + mtime).  The actual listing for the pipeline is done by
``native_apps._list_localfs_items`` (registered in ``NATIVE_READERS``);
the descriptor here is the registry-selectable, capability-flagged handle.

The local filesystem is a dual-role store: it can also be a *write* target, so
``can_write=True`` (store-to-store export can be built on the flags later).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from cocoindex_integration.connectors.cocoindex.base import CocoSource

logger = logging.getLogger(__name__)

#: Document file patterns accepted by flexible-graphrag's document processor.
#: Kept in sync with ``app.py``'s ``_DOC_PATTERNS`` (single source of truth is
#: ``app.py`` at execution time; this is the descriptor default).
_DEFAULT_DOC_PATTERNS: Tuple[str, ...] = (
    "**/*.pdf", "**/*.docx", "**/*.doc", "**/*.pptx", "**/*.ppt",
    "**/*.xlsx", "**/*.xls", "**/*.txt", "**/*.md", "**/*.html",
    "**/*.htm", "**/*.csv", "**/*.json", "**/*.xml",
)


def _localfs_available() -> bool:
    """True when CocoIndex's native localfs connector can be imported."""
    try:
        from cocoindex.connectors import localfs  # noqa: F401, PLC0415
        return True
    except Exception:  # noqa: BLE001
        return False


@dataclass
class CocoLocalFileSystem(CocoSource):
    """Descriptor for the native CocoIndex localfs source (read + write)."""
    name = "localfs"
    can_read = True
    can_write = True   # local filesystem is a valid write target too

    #: Directory to read/watch.  Defaults to WATCH_DIR then ./cocoindex-docs.
    source_dir: str = "./cocoindex-docs"
    recursive: bool = True
    patterns: Tuple[str, ...] = field(default_factory=lambda: _DEFAULT_DOC_PATTERNS)


def build_localfs(cfg: Dict[str, Any]) -> Optional[CocoLocalFileSystem]:
    """Build a :class:`CocoLocalFileSystem` (or None if native localfs missing).

    *cfg* is the pipeline config dict (``load_config_from_env`` output); the
    directory falls back to ``WATCH_DIR`` then ``./cocoindex-docs``.
    """
    src_dir = cfg.get("source_dir") or os.getenv("WATCH_DIR", "./cocoindex-docs")
    src = CocoLocalFileSystem(source_dir=src_dir)
    src.native_available = _localfs_available()
    if not src.native_available:
        logger.warning(
            "[coco] localfs: cocoindex.connectors.localfs unavailable — "
            "falling back to FlexibleDataSource('filesystem')"
        )
    return src
