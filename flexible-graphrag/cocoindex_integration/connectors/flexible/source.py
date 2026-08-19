"""flexible-graphrag data sources exposed as a CocoIndex source iterator.

``FlexibleDataSource`` wraps flexible-graphrag's 14 data sources behind a single
CocoIndex-compatible async iterator.  It is *standalone* (a reader) — it is not a
``FlexibleConnector`` target, since sources read and targets write.

Auto-sync support matrix
-------------------------
CocoIndex polls these iterators on a schedule and only reprocesses items whose
``key`` or content changed.  However, *auto-sync is only meaningful when the
underlying source actually returns updated content on re-poll*:

+-----------------+------------------------------------------------------------+
| Source          | Auto-sync / incremental support                            |
+=================+============================================================+
| filesystem      | Yes — CocoIndex detects new/modified/deleted files         |
| s3              | Yes — stable s3:// keys, CocoIndex detects changes         |
| gcs             | Yes — stable gs:// keys, CocoIndex detects changes         |
| azure_blob      | Yes — stable container/blob keys, CocoIndex detects changes|
| alfresco        | Yes — Alfresco node IDs are stable; cm:modified tracked    |
| nuxeo           | Yes — Nuxeo uids are stable; dc:modified tracked           |
| google_drive    | Yes — stable file_id keys                                  |
| onedrive        | Yes — stable file_id keys                                  |
| sharepoint      | Yes — stable file_id keys                                  |
| box             | Yes — stable path keys                                     |
+-----------------+------------------------------------------------------------+
| cmis            | **One-shot only** — no polling/change detection in fg      |
| web             | **One-shot only** — URLs fetched once; no change detection |
| wikipedia       | **One-shot only** — fetched once; articles not re-checked  |
| youtube         | **One-shot only** — transcripts fetched once               |
| file_upload     | **One-shot only** — uploaded via UI dialog; not a path     |
+-----------------+------------------------------------------------------------+

For one-shot sources, ``FlexibleDataSource`` still works for initial ingest
inside a CocoIndex pipeline, but re-running the pipeline will not detect
content changes (all items look unchanged to CocoIndex unless you change the
``key`` manually).

Phase 1 — raw-bytes passthrough (fixes double-parse)
------------------------------------------------------
The four highest-volume binary sources delegate to dedicated modules under
``_sources/``.  Each module uses the existing ``flexible-graphrag`` Source
class (``S3Source``, ``GCSSource``, ``AzureBlobSource``) together with
``BytesCaptureExtractor`` to capture raw file bytes at reader time — before
``DocumentProcessor`` or any parser is called.

* **filesystem** — ``_sources/_filesystem.py`` — ``Path.read_bytes()``
* **s3**          — ``_sources/_s3.py``          — ``S3Source`` + ``BytesCaptureExtractor``
* **gcs**         — ``_sources/_gcs.py``          — ``GCSSource`` + ``BytesCaptureExtractor``
* **azure_blob**  — ``_sources/_azure_blob.py``   — ``AzureBlobSource`` + ``BytesCaptureExtractor``

All other sources (Alfresco, CMIS, web, Wikipedia, YouTube, OneDrive,
SharePoint, Google Drive, Box) continue to use ``get_documents()`` for now
and fall back to UTF-8-encoded text in ``get_bytes()``.  Phase 2 will extend
raw-bytes passthrough to these sources.

Usage in a CocoIndex pipeline
------------------------------
    from cocoindex_integration.connectors.flexible.source import FlexibleDataSource

    @coco.fn
    async def main(sourcedir: pathlib.Path, tables) -> None:
        async for item in FlexibleDataSource("filesystem", {"path": str(sourcedir)}):
            await coco.mount_each(process_file, [(item.key, item)], tables)

Design notes
-------------
- Each source yields ``SourceItem`` objects with ``key`` (stable ID),
  ``file_name``, ``file_bytes`` (lazy-loaded), and ``metadata``.
- The ``key`` is deterministic (e.g. ``s3://bucket/key``, ``container/blob``)
  so CocoIndex can detect which items changed between runs.
- For cloud sources that already implement change detection in flexible-graphrag
  (GCS, Azure Blob, S3), CocoIndex's change detection is additive — it also
  tracks code/logic changes at the processing-function level.
- ``_raw_bytes`` (Phase 1): when set, ``get_bytes()`` returns the true raw
  file bytes without going through pre-parsed ``_doc.text``.  CocoIndex
  fingerprints these bytes for change detection — any byte change triggers a
  re-parse, and unchanged files are served from the memoization cache.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class SourceItem:
    """A single item yielded by a FlexibleDataSource iteration.

    Field design for CocoIndex sources
    ------------------------------------
    Each field is explicitly typed so CocoIndex can:
      1. Fingerprint the item for change detection (key + content_hash).
      2. Pass individual fields downstream to @coco.fn functions without
         unpacking a metadata dict.
      3. Fan-out specific fields to different downstream targets
         (e.g. modified_at → SearchRow, file_type → VectorRow).

    The ``key`` field is the stable CocoIndex change-detection key.
    CocoIndex compares keys between runs and only reprocesses items whose
    ``key`` or content has changed. Use the most stable identifier available:
      - S3 / GCS / Azure Blob: ``"s3://bucket/key"``, ``"gs://bucket/key"``
      - Alfresco / CMIS: document node ID (stable across edits)
      - OneDrive / SharePoint / Google Drive: file_id (stable across renames)
      - Filesystem: absolute path

    Attributes
    ----------
    key:
        Stable unique identifier. CocoIndex uses this for change detection.
    file_name:
        Human-readable filename (e.g. ``"report.pdf"``).
        Separate from key so it can be stored as a named field in targets.
    file_path:
        Full source path (e.g. ``"s3://my-bucket/docs/report.pdf"``).
    file_type:
        File extension without dot (e.g. ``"pdf"``, ``"docx"``, ``"pptx"``).
        Separate field enables file_type filtering in vector/search stores.
    source_type:
        Datasource kind string: ``"s3"``, ``"gcs"``, ``"azure_blob"``,
        ``"alfresco"``, ``"filesystem"``, etc.
    modified_at:
        ISO-8601 last-modified timestamp. CocoIndex uses this as part of the
        content fingerprint — if modified_at changes, the item is reprocessed.
        Empty string when the source does not provide modification timestamps.
    content_hash:
        Optional SHA-256 hex of the raw file bytes. When provided, CocoIndex
        uses this for byte-level change detection (more reliable than modified_at
        for sources that update timestamps without changing content).
    metadata:
        Source-specific extra metadata dict.  Populated by ``_iter_*`` methods
        with the same fields that ``get_documents()`` adds after processing
        (e.g. ``bucket_name``, ``prefix``, ``region`` for S3; ``container_name``
        for Azure Blob; ``bucket`` for GCS).  Merged into Document metadata by
        ``app.py`` so downstream stores see the same enrichment as the standard
        ingestion path.
    _raw_bytes:
        Phase 1: true raw file bytes (binary content) when available.
        When set, ``get_bytes()`` returns these directly — no encoding of
        pre-parsed text.  CocoIndex fingerprints these for change detection.
    _doc:
        Raw LlamaIndex Document object — internal use for text-only sources.
        Access content via ``await item.get_bytes()`` or ``await item.get_text()``.
    """
    # ── Change detection key ─────────────────────────────────────────────────
    key: str              # stable unique identifier for CocoIndex change detection

    # ── Explicit source metadata fields ──────────────────────────────────────
    file_name: str        # human-readable filename
    file_path: str        # full source path
    file_type: str = ""   # file extension (pdf, docx, pptx, txt, xlsx, ...)
    source_type: str = "" # datasource kind (s3, gcs, azure_blob, filesystem, ...)
    modified_at: str = "" # ISO-8601 last-modified; empty if source doesn't provide it
    content_hash: str = "" # SHA-256 hex of file bytes (optional, for byte-level diffing)

    # ── Source-specific extra metadata ───────────────────────────────────────
    # Carries source-specific fields that the standard get_documents() flow adds
    # after processing (e.g. bucket_name / prefix / region for S3; container_name
    # for Azure Blob; bucket for GCS).  Downstream pipeline code (app.py) merges
    # this into the Document metadata so the same fields are available in vector
    # and search stores regardless of whether the CocoIndex or standard path is used.
    metadata: Dict[str, Any] = field(default_factory=dict)

    # ── Internal ─────────────────────────────────────────────────────────────
    # Phase 1: true raw binary bytes for filesystem/S3/GCS/Azure Blob sources.
    # When set, get_bytes() returns this directly without encoding _doc.text.
    _raw_bytes: Optional[bytes] = field(default=None, repr=False)
    # Legacy: LlamaIndex Document for text-only sources (web, wikipedia, etc.)
    _doc: Any = field(default=None, repr=False)

    async def get_bytes(self) -> bytes:
        """Return the raw file bytes for this item.

        Phase 1 priority:
          1. ``_raw_bytes`` — true binary file bytes (filesystem/S3/GCS/Azure).
             CocoIndex fingerprints these for change detection.
          2. ``_doc.extra_info["raw_bytes"]`` — reader-injected bytes.
          3. ``_doc.text.encode("utf-8")`` — fallback for text-only sources.
        """
        # Phase 1 fast path: true raw bytes available
        if self._raw_bytes is not None:
            return self._raw_bytes
        if self._doc is None:
            return b""
        # Reader-injected raw bytes (metadata preferred; extra_info is legacy)
        meta = getattr(self._doc, "metadata", None) or {}
        if "raw_bytes" in meta:
            return meta["raw_bytes"]
        extra = getattr(self._doc, "extra_info", None)
        if isinstance(extra, dict) and "raw_bytes" in extra:
            return extra["raw_bytes"]
        # Fall back to UTF-8 encoded text (text-only sources: web, wikipedia, youtube)
        text = getattr(self._doc, "text", "") or ""
        return text.encode("utf-8", errors="replace")

    async def get_text(self) -> str:
        """Return the document text (already extracted by the reader)."""
        if self._doc is None:
            return ""
        return getattr(self._doc, "text", "") or ""



# ---------------------------------------------------------------------------
# FlexibleDataSource
# ---------------------------------------------------------------------------

class FlexibleDataSource:
    """Async iterator over documents from any flexible-graphrag data source.

    Parameters
    ----------
    source_type:
        One of: ``"alfresco"``, ``"azure_blob"``, ``"box"``, ``"cmis"``,
        ``"filesystem"``, ``"gcs"``, ``"google_drive"``, ``"onedrive"``,
        ``"s3"``, ``"sharepoint"``, ``"web"``, ``"wikipedia"``, ``"youtube"``.
    config:
        Source configuration dict (same schema as flexible-graphrag's
        datasource_config JSON blobs).
    use_doc_processor:
        If True, run documents through flexible-graphrag's DocumentProcessor
        (Docling/LlamaParse) before yielding. Set False when you want raw
        placeholder docs and will call a @coco.fn parse function yourself.
    """

    _SOURCE_MAP = {
        "alfresco": "_iter_alfresco",
        "azure_blob": "_iter_azure_blob",
        "box": "_iter_box",
        "cmis": "_iter_cmis",
        "filesystem": "_iter_filesystem",
        "gcs": "_iter_gcs",
        "google_drive": "_iter_google_drive",
        "nuxeo": "_iter_nuxeo",
        "onedrive": "_iter_onedrive",
        "s3": "_iter_s3",
        "sharepoint": "_iter_sharepoint",
        "web": "_iter_web",
        "wikipedia": "_iter_wikipedia",
        "youtube": "_iter_youtube",
    }

    def __init__(
        self,
        source_type: str,
        config: Dict[str, Any],
        use_doc_processor: bool = False,
    ):
        self.source_type = source_type.lower()
        self.config = config
        self.use_doc_processor = use_doc_processor
        if self.source_type not in self._SOURCE_MAP:
            raise ValueError(
                f"Unknown source type '{source_type}'. "
                f"Supported: {', '.join(sorted(self._SOURCE_MAP))}"
            )

    def __aiter__(self) -> AsyncIterator[SourceItem]:
        method = getattr(self, self._SOURCE_MAP[self.source_type])
        return method()

    # ------------------------------------------------------------------
    # Helper: build a SourceItem from a LlamaIndex Document (text-only)
    # ------------------------------------------------------------------

    def _make_item(
        self,
        key: str,
        file_name: str,
        file_path: str,
        doc: Any,
        *,
        modified_at: str = "",
    ) -> SourceItem:
        """Build a SourceItem from a pre-loaded LlamaIndex Document.

        Used for text-only / legacy sources (web, wikipedia, youtube, alfresco,
        onedrive, etc.) where ``get_bytes()`` falls back to ``_doc.text.encode()``.

        Extracts file_type from the file extension and modified_at from the
        document metadata (both ``modified_at`` and ``modified at`` styles).
        """
        meta = getattr(doc, "metadata", {}) or {}
        fname = file_name or meta.get("file_name", os.path.basename(file_path))

        # File type from extension
        _, ext = os.path.splitext(fname)
        file_type = ext.lstrip(".").lower()

        # modified_at: prefer explicit arg, then metadata (both key styles).
        # Normalize to empty string — callers that write to Elasticsearch must
        # omit the field (or use None) when the value is empty to avoid
        # "failed to parse field [metadata.modified_at] of type [date]" errors.
        mod = (
            modified_at
            or meta.get("modified_at", "")
            or meta.get("modified at", "")
            or meta.get("last_modified", "")
            or ""
        )
        # Strip whitespace; keep only non-empty values.
        mod = mod.strip() if mod else ""

        return SourceItem(
            key=key,
            file_name=fname,
            file_path=file_path,
            file_type=file_type,
            source_type=self.source_type,
            modified_at=mod,
            _doc=doc,
        )

    # ------------------------------------------------------------------
    # Binary-source iterators — delegate to _sources/ modules
    # Each module owns its own credential resolution and byte capture
    # (BytesCaptureExtractor + existing flexible-graphrag Source class).
    # ------------------------------------------------------------------

    async def _iter_filesystem(self) -> AsyncIterator[SourceItem]:
        """Delegate to _sources._filesystem — raw bytes via Path.read_bytes()."""
        from cocoindex_integration.connectors.flexible._sources._filesystem import iter_filesystem
        async for item in iter_filesystem(self.config):
            yield item

    async def _iter_s3(self) -> AsyncIterator[SourceItem]:
        """Delegate to _sources._s3 — raw bytes via BytesCaptureExtractor + S3Source."""
        from cocoindex_integration.connectors.flexible._sources._s3 import iter_s3
        async for item in iter_s3(self.config):
            yield item

    async def _iter_gcs(self) -> AsyncIterator[SourceItem]:
        """Delegate to _sources._gcs — raw bytes via BytesCaptureExtractor + GCSSource."""
        from cocoindex_integration.connectors.flexible._sources._gcs import iter_gcs
        async for item in iter_gcs(self.config):
            yield item

    async def _iter_azure_blob(self) -> AsyncIterator[SourceItem]:
        """Delegate to _sources._azure_blob — raw bytes via BytesCaptureExtractor + AzureBlobSource."""
        from cocoindex_integration.connectors.flexible._sources._azure_blob import iter_azure_blob
        async for item in iter_azure_blob(self.config):
            yield item

    # ------------------------------------------------------------------
    # Remaining sources: text-only / Phase 2 (use _doc + text fallback)
    # ------------------------------------------------------------------

    async def _iter_google_drive(self) -> AsyncIterator[SourceItem]:
        from sources.google_drive import GoogleDriveSource  # type: ignore[import]
        src = GoogleDriveSource(self.config)
        docs = await asyncio.to_thread(src.get_documents)
        for doc in docs:
            file_id = doc.metadata.get("file_id", doc.metadata.get("file_path", ""))
            file_name = doc.metadata.get("file_name", file_id)
            yield self._make_item(
                key=f"gdrive://{file_id}",
                file_name=file_name,
                file_path=f"gdrive://{file_id}",
                doc=doc,
            )

    async def _iter_onedrive(self) -> AsyncIterator[SourceItem]:
        from sources.onedrive import OneDriveSource  # type: ignore[import]
        src = OneDriveSource(self.config)
        docs = await asyncio.to_thread(src.get_documents)
        for doc in docs:
            file_id = doc.metadata.get("file_id", doc.metadata.get("file_path", ""))
            file_name = doc.metadata.get("file_name", file_id)
            yield self._make_item(
                key=f"onedrive://{file_id}",
                file_name=file_name,
                file_path=f"onedrive://{file_id}",
                doc=doc,
            )

    async def _iter_sharepoint(self) -> AsyncIterator[SourceItem]:
        from sources.sharepoint import SharePointSource  # type: ignore[import]
        src = SharePointSource(self.config)
        docs = await asyncio.to_thread(src.get_documents)
        for doc in docs:
            file_id = doc.metadata.get("file_id", doc.metadata.get("file_path", ""))
            file_name = doc.metadata.get("file_name", file_id)
            yield self._make_item(
                key=f"sharepoint://{file_id}",
                file_name=file_name,
                file_path=f"sharepoint://{file_id}",
                doc=doc,
            )

    async def _iter_box(self) -> AsyncIterator[SourceItem]:
        from sources.box import BoxSource  # type: ignore[import]
        src = BoxSource(self.config)
        docs = await asyncio.to_thread(src.get_documents)
        for doc in docs:
            path = doc.metadata.get("file_path", doc.metadata.get("file_name", ""))
            yield self._make_item(
                key=f"box://{path}",
                file_name=doc.metadata.get("file_name", os.path.basename(path)),
                file_path=f"box://{path}",
                doc=doc,
            )

    async def _iter_alfresco(self) -> AsyncIterator[SourceItem]:
        from sources.alfresco import AlfrescoSource  # type: ignore[import]
        src = AlfrescoSource(self.config)
        docs = await asyncio.to_thread(src.get_documents)
        for doc in docs:
            path = doc.metadata.get("file_path", doc.metadata.get("file_name", ""))
            yield self._make_item(
                key=f"alfresco://{path}",
                file_name=doc.metadata.get("file_name", os.path.basename(path)),
                file_path=f"alfresco://{path}",
                doc=doc,
                modified_at=doc.metadata.get("cm:modified", ""),
            )

    async def _iter_nuxeo(self) -> AsyncIterator[SourceItem]:
        """Eager fallback for Nuxeo.

        Nuxeo is detector-backed, so ``flexible_app_main`` normally takes the
        lazy ``FlexibleMapView`` path instead of this one.  Kept so a Nuxeo
        source still ingests when the detector cannot be built (bad credentials,
        missing Kafka audit stream, …).
        """
        from sources.nuxeo import NuxeoSource  # type: ignore[import]
        src = NuxeoSource(self.config)
        docs = await asyncio.to_thread(src.get_documents)
        for doc in docs:
            path = doc.metadata.get("file_path", doc.metadata.get("file_name", ""))
            yield self._make_item(
                key=f"nuxeo://{path}",
                file_name=doc.metadata.get("file_name", os.path.basename(path)),
                file_path=f"nuxeo://{path}",
                doc=doc,
                modified_at=doc.metadata.get("modified_at", ""),
            )

    async def _iter_cmis(self) -> AsyncIterator[SourceItem]:
        # One-shot only — CMIS has no polling/change-detection in flexible-graphrag.
        from sources.cmis import CmisSource  # type: ignore[import]
        src = CmisSource(self.config)
        docs = await asyncio.to_thread(src.get_documents)
        for doc in docs:
            path = doc.metadata.get("file_path", doc.metadata.get("file_name", ""))
            yield self._make_item(
                key=f"cmis://{path}",
                file_name=doc.metadata.get("file_name", os.path.basename(path)),
                file_path=f"cmis://{path}",
                doc=doc,
                modified_at=doc.metadata.get("cmis:lastModificationDate", ""),
            )

    async def _iter_web(self) -> AsyncIterator[SourceItem]:
        # One-shot only — URLs are fetched once; no change detection on re-poll.
        from sources.web import WebSource  # type: ignore[import]
        src = WebSource(self.config)
        docs = await asyncio.to_thread(src.get_documents)
        for doc in docs:
            url = doc.metadata.get("url", doc.metadata.get("file_path", ""))
            slug = (url or "").rstrip("/").split("/")[-1].split("?")[0] or "page"
            file_name = doc.metadata.get("file_name") or slug
            if not os.path.splitext(file_name)[1]:
                file_name = f"{file_name}.txt"
            yield self._make_item(
                key=url,
                file_name=file_name,
                file_path=url,
                doc=doc,
            )

    async def _iter_wikipedia(self) -> AsyncIterator[SourceItem]:
        # One-shot only — Wikipedia articles are fetched once; not re-checked on re-poll.
        from sources.wikipedia import WikipediaSource  # type: ignore[import]
        src = WikipediaSource(self.config)
        docs = await asyncio.to_thread(src.get_documents)
        for doc in docs:
            title = doc.metadata.get("title", doc.metadata.get("file_name", "article"))
            yield self._make_item(
                key=f"wikipedia://{title}",
                file_name=f"{title}.txt",
                file_path=f"wikipedia://{title}",
                doc=doc,
            )

    async def _iter_youtube(self) -> AsyncIterator[SourceItem]:
        # One-shot only — transcripts are fetched once; no change detection on re-poll.
        from sources.youtube import YouTubeSource  # type: ignore[import]
        src = YouTubeSource(self.config)
        docs = await asyncio.to_thread(src.get_documents)
        for idx, doc in enumerate(docs):
            video_id = doc.metadata.get("video_id", "")
            start_ts = doc.metadata.get("start_timestamp", str(idx))
            file_name = doc.metadata.get("file_name") or f"{video_id}_{start_ts}.txt"
            if not os.path.splitext(file_name)[1]:
                file_name = f"{file_name}.txt"
            file_path = doc.metadata.get("file_path") or f"youtube://{video_id}/{start_ts}"
            yield self._make_item(
                key=f"youtube://{video_id}#{start_ts}",
                file_name=file_name,
                file_path=file_path,
                doc=doc,
            )
