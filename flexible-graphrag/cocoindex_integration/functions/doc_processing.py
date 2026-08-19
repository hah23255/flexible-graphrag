"""
@coco.fn decorated document processing functions.

These wrap flexible-graphrag's DocumentProcessor (Docling / LlamaParse) so that
CocoIndex can memoize the expensive parse step. An unchanged file hash -> cache
hit -> no re-billing, no re-parse time.

Usage in a CocoIndex pipeline
------------------------------
    from cocoindex_integration.functions.doc_processing import (
        parse_document,
        build_parse_cfg_json,
    )

    @coco.fn(memo=True)
    async def process_file(file: localfs.File, cfg_json: str) -> str:
        content = await file.read_bytes()
        parse_cfg = build_parse_cfg_json(json.loads(cfg_json))
        return await parse_document(content, file.file_path.name, parse_cfg)

Notes
-----
- ``memo=True`` means CocoIndex caches the parsed text keyed by (file bytes,
  file name, parse config JSON). Unchanged files with unchanged parse config
  are never re-parsed — no re-billing.
- *cfg_json* includes **all** parse-affecting settings so the cache correctly
  invalidates when the parse configuration changes (e.g. enabling OCR, switching
  from docling to llamaparse, changing the tier, or rotating the API key).
- Both parsers go through flexible-graphrag's ``DocumentProcessor`` — this is
  the single source of truth for docling and LlamaParse v2, and ensures every
  config option (device, OCR engine, tier, language, custom prompt …) is
  honored consistently with the default (non-CocoIndex) pipeline.
- ``liteparse`` (future) will slot in automatically once
  ``DocumentProcessor`` supports it — no changes needed here.
- Return type annotation is required for proper CocoIndex deserialization.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any, Dict

import cocoindex as coco

logger = logging.getLogger(__name__)

# Corporate proxy SSL bypass ─────────────────────────────────────────────────
# Docling downloads ML models (DocumentFigureClassifier) from huggingface.co
# via the ``requests`` library.  When OPENAI_VERIFY_SSL=false we patch
# requests.Session.send globally so huggingface_hub downloads work in
# environments where the proxy re-signs TLS traffic with a private CA.
if os.getenv("OPENAI_VERIFY_SSL", "true").strip().lower() in ("false", "0", "no"):
    try:
        import requests
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        _orig_session_send = requests.Session.send

        def _no_verify_session_send(self, req, **kwargs):  # type: ignore[override]
            # Force override: requests.Session.request() pre-populates verify=True
            # so setdefault() would not help here.
            kwargs["verify"] = False
            return _orig_session_send(self, req, **kwargs)

        requests.Session.send = _no_verify_session_send  # type: ignore[method-assign]
        logger.info(
            "doc_processing: OPENAI_VERIFY_SSL=false — patched requests.Session "
            "to skip TLS verification (huggingface_hub model downloads)"
        )
    except ImportError:
        pass


# ---------------------------------------------------------------------------
# Parse-affecting config keys
# ---------------------------------------------------------------------------
# All keys listed here are extracted from the pipeline cfg dict and included
# in the memo key JSON.  The cache therefore invalidates when *any* parse
# setting changes — not just when file bytes change.

_PARSE_CFG_KEYS: tuple = (
    "parser_type",
    # Docling options
    "docling_device",
    "docling_ocr",
    "docling_ocr_engine",
    "docling_timeout",
    "docling_cancel_check_interval",
    "parser_format_for_extraction",
    "save_parsing_output",
    # LlamaParse options (tier, language, custom_prompt read from env inside
    # DocumentProcessor for now; still included in key so cache invalidates
    # when they change in env_config.py / the pipeline cfg)
    "llamaparse_tier",
    "llama_cloud_api_key",
    "llamaparse_language",
    "llamaparse_custom_prompt",
)

# Mapping from pipeline cfg key → Settings field name for config we can thread
# into DocumentProcessor via the Settings constructor.
_CFG_TO_SETTINGS: Dict[str, str] = {
    "docling_device":                "docling_device",
    "docling_ocr":                   "docling_ocr",
    "docling_ocr_engine":            "docling_ocr_engine",
    "docling_timeout":               "docling_timeout",
    "docling_cancel_check_interval": "docling_cancel_check_interval",
    "parser_format_for_extraction":  "parser_format_for_extraction",
    "save_parsing_output":           "save_parsing_output",
    # LlamaParse API key — threaded via Settings so DocumentProcessor picks it up
    # without an additional os.getenv call inside the thread.
    "llama_cloud_api_key":           "llamaparse_api_key",
}


def build_parse_cfg_json(cfg: Dict[str, Any]) -> str:
    """Extract parse-affecting config from *cfg* into a stable JSON memo key.

    Only the keys listed in ``_PARSE_CFG_KEYS`` are included — all other
    pipeline keys (graph DB, vector DB, chunker settings, …) are ignored so
    they do not pollute the parse cache and trigger unnecessary re-parses.

    Parameters
    ----------
    cfg:
        Full pipeline config dict as returned by ``load_config_from_env()``.

    Returns
    -------
    str
        Compact, deterministic JSON string safe to pass as a CocoIndex
        ``@coco.fn(memo=True)`` argument.
    """
    return json.dumps(
        {k: cfg.get(k) for k in _PARSE_CFG_KEYS},
        sort_keys=True,
        separators=(",", ":"),
    )


# ---------------------------------------------------------------------------
# Core parse implementation (sync, runs in thread pool)
# ---------------------------------------------------------------------------

# Parse-metadata keys derived from the temp file that must NOT leak downstream
# (they would clobber the real source provenance carried by the pipeline).
_PARSE_META_STRIP: frozenset = frozenset({
    "file_path", "file_name", "file_type", "source",
    "file_size", "creation_date", "last_modified_date", "last_accessed_date",
})


def _extract_parse_metadata(docs: list) -> Dict[str, Any]:
    """Collect parse-derived metadata (e.g. ``conversion_method``) from *docs*.

    Only keeps parse-provenance keys — temp-file-derived keys (``file_path``,
    ``file_name`` …) and private keys are stripped so they never overwrite the
    real source provenance that the pipeline attaches post-parse.
    """
    meta: Dict[str, Any] = {}
    for d in docs:
        for k, v in (getattr(d, "metadata", None) or {}).items():
            if not isinstance(k, str) or k.startswith("_"):
                continue
            if k in _PARSE_META_STRIP:
                continue
            # First non-empty value wins (docs are pages of the same file).
            if k not in meta and v is not None and v != "":
                meta[k] = v
    return meta


def _parse_sync(file_bytes: bytes, file_name: str, cfg_json: str) -> str:
    """Parse *file_bytes* synchronously via DocumentProcessor.

    Runs in a thread pool (via ``asyncio.to_thread``) so it never blocks the
    async event loop.  Both docling and llamaparse routes go through
    flexible-graphrag's ``DocumentProcessor`` — the single source of truth for
    all parse options.

    Parameters
    ----------
    file_bytes:
        Raw file content to parse.
    file_name:
        Original filename with extension — used by DocumentProcessor to select
        the correct converter and by LlamaParse to determine the file type.
    cfg_json:
        JSON string produced by ``build_parse_cfg_json()``.  Supplies parse
        config overrides that take precedence over environment variables.

    Returns
    -------
    str
        Compact JSON string ``{"text": <str>, "metadata": <dict>}`` where
        ``metadata`` holds parse-derived provenance (``conversion_method`` …).
        Returning a JSON string keeps the CocoIndex memo value a plain ``str``
        while carrying parse metadata forward (no longer discarded).
    """
    import asyncio
    from process.document_processor import DocumentProcessor
    from config import Settings

    cfg = json.loads(cfg_json)

    # Normalise parser_type.  Three parsers are supported (DOCUMENT_PARSER):
    # docling, llamaparse, liteparse.  The `else` here used to collapse
    # everything that was not llamaparse to docling, silently discarding
    # liteparse — so DOCUMENT_PARSER=liteparse was honoured by the default
    # pipeline but ignored by this one, and .md/.txt went through docling
    # (which strips markdown markers) instead of being read through unchanged.
    parser_type = cfg.get("parser_type", "docling").lower()
    if parser_type in ("llamaparse", "llama_parse"):
        parser_type = "llamaparse"
    elif parser_type != "liteparse":
        parser_type = "docling"

    # Build a Settings object seeded from the environment, then override with
    # parse-affecting values from the pipeline cfg so UI / REST datasource
    # config is honored (not just env vars).
    settings_kwargs: Dict[str, Any] = {}
    for cfg_key, settings_key in _CFG_TO_SETTINGS.items():
        v = cfg.get(cfg_key)
        if v is not None:
            settings_kwargs[settings_key] = v

    app_cfg = Settings(**settings_kwargs)
    proc = DocumentProcessor(config=app_cfg, parser_type=parser_type)

    ext = os.path.splitext(file_name)[1] or ".txt"
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tf:
        tf.write(file_bytes)
        tmp_path = tf.name

    try:
        # process_documents is async; run it in a fresh event loop for this
        # thread (asyncio.to_thread gives us a clean thread with no loop).
        docs = asyncio.run(proc.process_documents([tmp_path]))
        texts = [d.text or "" for d in docs if d.text]
        parse_metadata = _extract_parse_metadata(docs)
        return json.dumps(
            {"text": "\n\n".join(texts), "metadata": parse_metadata},
            separators=(",", ":"),
        )
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Public memoized parse function
# ---------------------------------------------------------------------------

@coco.fn(memo=True)
async def parse_document(
    file_bytes: bytes,
    file_name: str,
    cfg_json: str = "{}",
) -> str:
    """Parse *file_bytes* using the parser selected by *cfg_json*.

    This is the single memoized parse entry point for the CocoIndex pipeline.
    Unchanged files with unchanged parse config are returned from cache
    (no re-billing, no re-parse time).

    Parameters
    ----------
    file_bytes:
        Raw file content.  CocoIndex hashes this as part of the memo key, so
        any content change automatically triggers a re-parse.
    file_name:
        Original filename with extension.  Passed to ``DocumentProcessor`` for
        converter selection and to LlamaParse for file-type detection.
    cfg_json:
        JSON string from ``build_parse_cfg_json()``.  All parse-affecting
        settings are encoded here so that changing OCR, tier, language, API
        key, etc. correctly invalidates the cache.  Pass ``"{}"`` to use
        environment defaults only.

    Returns
    -------
    str
        Compact JSON string ``{"text": <str>, "metadata": <dict>}``.  Use
        :func:`decode_parse_result` to split it into ``(text, parse_metadata)``.
        Returning a JSON string (rather than bare text) lets parse-derived
        metadata flow forward while keeping the memo value a plain ``str``.
    """
    import asyncio
    return await asyncio.to_thread(_parse_sync, file_bytes, file_name, cfg_json)


def decode_parse_result(result: str) -> "tuple[str, Dict[str, Any]]":
    """Split a :func:`parse_document` result into ``(text, parse_metadata)``.

    Backward compatible: if *result* is a legacy plain-text value (from a cache
    entry written before parse metadata was added), it is returned as the text
    with empty metadata.
    """
    if not result:
        return "", {}
    try:
        obj = json.loads(result)
    except (ValueError, TypeError):
        return result, {}
    if isinstance(obj, dict) and "text" in obj:
        meta = obj.get("metadata") or {}
        if not isinstance(meta, dict):
            meta = {}
        return str(obj.get("text") or ""), meta
    # Some other JSON shape — treat the original string as text.
    return result, {}
