"""
@coco.fn decorated chunking / splitting functions.

Chunking is deliberately NOT memoized (memo=False, the default) because:
- It is CPU-cheap (regex splits, no API calls).
- Return values (list of chunk dicts) are large; storing them wastes the CocoIndex
  LMDB cache.
- Downstream memoized callers (embedding, KG extraction) are keyed on individual
  chunk text, so they still skip unchanged chunks even without chunk-level memos.

The @coco.fn decorator (without memo=True) still participates in CocoIndex's
logic-change detection: if you change the splitter settings, CocoIndex propagates
the change fingerprint to memoized callers (embed, KG extract) and invalidates
only the chunks whose text changed.

Dataclass return types
----------------------
CocoIndex requires a return type annotation on memoized functions for correct
deserialization. Chunking functions return a plain list of dicts which is
fine since chunking itself is not memoized.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)

# Both LI and LC splitter imports are lazy (inside the split functions) so that
# only the selected backend's packages are imported at call time.  CocoIndex
# caches results in LMDB, so the per-call import cost is amortised after the
# first invocation and is not a hot path.

try:
    import cocoindex as coco
    _COCO_AVAILABLE = True
except ImportError:
    _COCO_AVAILABLE = False
    class _FnFallback:
        """No-op stand-in for ``coco.fn`` when CocoIndex is not installed."""
        def __call__(self, fn=None, **kw):
            if fn is not None:
                return fn
            def decorator(f):
                return f
            return decorator
    class _CocoFallback:
        fn = _FnFallback()
    coco = _CocoFallback()  # type: ignore[assignment]


@dataclass
class TextChunk:
    """A single text chunk with its position metadata."""
    text: str
    chunk_index: int
    start_char: int
    end_char: int


# ---------------------------------------------------------------------------
# LlamaIndex — delegates to LlamaIndexChunkerAdapter
# ---------------------------------------------------------------------------

def _split_llamaindex_sync(
    text: str,
    chunk_size: int,
    chunk_overlap: int,
) -> List[TextChunk]:
    # Lazy import — only loaded when this path is actually taken.
    # LlamaIndexChunkerAdapter itself imports llama_index lazily inside __init__.
    from llamaindex.process.chunker_adapter import LlamaIndexChunkerAdapter
    adapter = LlamaIndexChunkerAdapter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    chunks_text = adapter.split_text(text)
    return [
        TextChunk(text=ch, chunk_index=i, start_char=0, end_char=len(ch))
        for i, ch in enumerate(chunks_text)
    ]


if _COCO_AVAILABLE:
    @coco.fn
    def split_with_llamaindex(
        text: str,
        chunk_size: int = 1024,
        chunk_overlap: int = 128,
    ) -> List[TextChunk]:
        """Split text using LlamaIndex SentenceSplitter.

        Not memoized — cheap CPU operation. @coco.fn without memo=True still
        makes the function's logic visible to CocoIndex for change propagation.
        """
        return _split_llamaindex_sync(text, chunk_size, chunk_overlap)
else:
    def split_with_llamaindex(
        text: str,
        chunk_size: int = 1024,
        chunk_overlap: int = 128,
    ) -> List[TextChunk]:
        return _split_llamaindex_sync(text, chunk_size, chunk_overlap)


# ---------------------------------------------------------------------------
# LangChain splitters — delegates to LangChainChunkerAdapter
# ---------------------------------------------------------------------------

import os as _os

#: All splitter types supported, matching ``LC_SPLITTER_TYPE`` in flexible-graphrag.
#: Kept here for documentation; the full list with splitting logic lives in
#: :class:`langchain.process.chunker_adapter.LangChainChunkerAdapter`.
LC_SPLITTER_TYPES = (
    "recursive",             # RecursiveCharacterTextSplitter (default)
    "character",             # CharacterTextSplitter
    "token",                 # TokenTextSplitter (requires tiktoken)
    "markdown",              # MarkdownTextSplitter
    "python",                # PythonCodeTextSplitter
    "sentence_transformers", # SentenceTransformersTokenTextSplitter
)


def _split_langchain_sync(
    text: str,
    chunk_size: int,
    chunk_overlap: int,
    splitter_type: str,
) -> List[TextChunk]:
    # Lazy import — only loaded when this path is actually taken.
    # LangChainChunkerAdapter imports langchain_text_splitters lazily in _build_splitter().
    from langchain.process.chunker_adapter import LangChainChunkerAdapter
    adapter = LangChainChunkerAdapter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        splitter_type=splitter_type,
    )
    raw_chunks = adapter.split_text(text)
    return [
        TextChunk(text=ch, chunk_index=i, start_char=0, end_char=len(ch))
        for i, ch in enumerate(raw_chunks)
    ]


def _lc_splitter_type_from_env() -> str:
    return _os.getenv("LC_SPLITTER_TYPE", "recursive")


# ---------------------------------------------------------------------------
# CocoIndex-native splitter — cocoindex.ops.text.RecursiveSplitter
# ---------------------------------------------------------------------------

#: All splitter types supported for CHUNKER_BACKEND=cocoindex, matching COCOINDEX_SPLITTER_TYPE.
COCOINDEX_SPLITTER_TYPES = (
    "recursive",   # RecursiveSplitter (default) — syntax-aware via tree-sitter for 30+ languages
    "separator",   # SeparatorSplitter — split on regex separators, then pack into chunk_size groups
)

# Default separators for SeparatorSplitter (paragraph → sentence → clause boundary).
_DEFAULT_SEPARATORS = [r"\n{2,}", r"[.!?…]\s+", r"[:;]\s+"]


def _parse_cocoindex_separators(raw: str) -> List[str]:
    """Parse COCOINDEX_SEPARATORS env var into a list of regex strings.

    Accepts two formats so users can include patterns that contain commas
    (e.g. ``\\n{2,}``):

    **JSON array** (preferred when patterns contain commas)::

        COCOINDEX_SEPARATORS=["\\\\n{2,}", "[.!?]\\\\s+"]

    **Pipe-separated list** (simple patterns)::

        COCOINDEX_SEPARATORS=\\\\n{2,}|[.!?]\\\\s+

    Returns ``_DEFAULT_SEPARATORS`` if ``raw`` is empty.
    """
    raw = raw.strip()
    if not raw:
        return _DEFAULT_SEPARATORS
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list) and all(isinstance(s, str) for s in parsed):
                return parsed
        except json.JSONDecodeError:
            pass
    # Pipe-separated fallback
    return [s.strip() for s in raw.split("|") if s.strip()] or _DEFAULT_SEPARATORS


def _cocoindex_separators_from_env() -> List[str]:
    raw = _os.getenv("COCOINDEX_SEPARATORS", "")
    return _parse_cocoindex_separators(raw)


def _pack_into_chunks(
    fragments: list,
    chunk_size: int,
    chunk_overlap: int,
    text: str = "",
) -> List[TextChunk]:
    """Group CocoIndex ``Chunk`` fragments into ``TextChunk``s respecting ``chunk_size``.

    ``SeparatorSplitter`` returns individual sentence/paragraph fragments with no
    size limit.  This function packs consecutive fragments into groups whose total
    character length stays within ``chunk_size``, then prepends up to
    ``chunk_overlap`` characters from the previous group as overlap.

    A group's text is taken by **slicing the original document** between the first
    and last fragment's offsets, not by joining fragment texts.  ``SeparatorSplitter``
    consumes the separators, so joining with a space silently rewrites the
    document: every blank line, newline and indent between fragments collapses to
    one space.  That makes the chunk text differ from the source it claims to
    quote, makes ``start_char``/``end_char`` useless for locating it, and defeats
    anything downstream that reads structure — a markdown-heading split arrives
    with no ``\\n\\n`` left to split on.
    """
    result: List[TextChunk] = []
    current: list = []       # accumulated fragment objects
    current_len: int = 0

    def _slice(frags: list) -> str:
        if not text:
            return " ".join(f.text for f in frags)
        try:
            start, end = frags[0].start.char_offset, frags[-1].end.char_offset
        except AttributeError:
            return " ".join(f.text for f in frags)
        body = text[start:end]
        # Offsets should always be usable; fall back rather than emit an empty
        # chunk if a future splitter reports them differently.
        return body if body else " ".join(f.text for f in frags)

    def _flush(frags: list) -> TextChunk:
        return TextChunk(
            text=_slice(frags),
            chunk_index=len(result),
            start_char=frags[0].start.char_offset,
            end_char=frags[-1].end.char_offset,
        )

    def _overlap_tail(frags: list) -> list:
        tail: list = []
        length = 0
        for f in reversed(frags):
            need = len(f.text) + (1 if tail else 0)
            if length + need > chunk_overlap:
                break
            tail.insert(0, f)
            length += need
        return tail

    for frag in fragments:
        add_len = len(frag.text) + (1 if current else 0)
        if current and current_len + add_len > chunk_size:
            result.append(_flush(current))
            current = _overlap_tail(current)
            current_len = sum(len(f.text) for f in current) + max(0, len(current) - 1)
        current.append(frag)
        current_len += add_len

    if current:
        result.append(_flush(current))

    _full = text or " ".join(f.text for f in fragments)
    return result or [TextChunk(text=_full, chunk_index=0, start_char=0, end_char=len(_full))]


def _split_cocoindex_sync(
    text: str,
    chunk_size: int,
    chunk_overlap: int,
    splitter_type: str = "recursive",
    language: str = "",
    separators: Optional[List[str]] = None,
) -> List[TextChunk]:
    """Split using CocoIndex's native splitters from ``cocoindex.ops.text``.

    ``splitter_type`` selects the splitter (mirrors ``COCOINDEX_SPLITTER_TYPE``):

    ``recursive``  RecursiveSplitter (default) — syntax-aware tree-sitter splitting
                   for 30+ languages; ``language`` enables the grammar (auto-detected
                   from file extension by the pipeline).
    ``separator``  SeparatorSplitter — splits on regex boundaries, then packs
                   fragments into ``chunk_size`` groups.
                   ``separators`` overrides the regex list (``COCOINDEX_SEPARATORS``
                   env var); defaults to ``_DEFAULT_SEPARATORS``.

    Raises ``ImportError`` if ``cocoindex.ops.text`` is not installed.
    Set ``CHUNKER_BACKEND=llamaindex`` or ``langchain`` for those backends.
    """
    try:
        from cocoindex.ops.text import RecursiveSplitter, SeparatorSplitter
    except ImportError:
        raise ImportError(
            "CHUNKER_BACKEND=cocoindex requires cocoindex.ops.text. "
            "Install with: uv pip install \"cocoindex[text]\""
        ) from None

    st = (splitter_type or "recursive").lower()

    if st == "separator":
        # SeparatorSplitter takes separators_regex in __init__ and split(text) only —
        # no chunk_size support.  Pack the resulting fragments ourselves.
        _seps = separators if separators else _cocoindex_separators_from_env()
        splitter = SeparatorSplitter(separators_regex=_seps)
        fragments = splitter.split(text)
        return _pack_into_chunks(fragments, chunk_size, chunk_overlap, text)
    else:  # "recursive" (default)
        splitter = RecursiveSplitter()
        kwargs: dict = {"chunk_size": chunk_size, "chunk_overlap": chunk_overlap}
        if language:
            kwargs["language"] = language
        raw_chunks = splitter.split(text, **kwargs)
        return [
            TextChunk(
                text=chunk.text,
                chunk_index=i,
                start_char=chunk.start.char_offset,
                end_char=chunk.end.char_offset,
            )
            for i, chunk in enumerate(raw_chunks)
        ]


def _cocoindex_splitter_type_from_env() -> str:
    return _os.getenv("COCOINDEX_SPLITTER_TYPE", "recursive")


if _COCO_AVAILABLE:
    @coco.fn
    def split_with_cocoindex(
        text: str,
        chunk_size: int = 1024,
        chunk_overlap: int = 128,
        splitter_type: str = "",
        language: str = "",
        separators_json: str = "",
    ) -> List[TextChunk]:
        """Split text using CocoIndex's native splitters from ``cocoindex.ops.text``.

        ``splitter_type`` values (mirrors ``COCOINDEX_SPLITTER_TYPE`` env var):

        ``recursive``  RecursiveSplitter (default) — syntax-aware tree-sitter
                       splitting for 30+ languages (Markdown, Python, Rust, SQL, …).
                       ``language`` selects the grammar; auto-detected from file
                       extension by the pipeline when left empty.
        ``separator``  SeparatorSplitter — splits on regex separators.
                       ``separators_json`` is a JSON array of regex strings
                       (from ``COCOINDEX_SEPARATORS``); falls back to
                       ``_DEFAULT_SEPARATORS`` when empty.

        If ``splitter_type`` is empty, the ``COCOINDEX_SPLITTER_TYPE`` env var is
        read; defaults to ``"recursive"``.

        ``separators_json`` must be a JSON array string so CocoIndex can correctly
        track cache invalidation when the separator list changes.

        No LlamaIndex or LangChain dependency required.
        ``start_char`` / ``end_char`` in returned :class:`TextChunk` objects are
        populated from CocoIndex's position-tracked ``Chunk`` objects.

        ``chunk_size`` and ``chunk_overlap`` are in **characters** (same convention
        as ``CHUNK_SIZE`` / ``CHUNK_OVERLAP`` env vars).

        Not memoized — cheap CPU operation, same policy as the other chunkers.
        """
        _seps: Optional[List[str]] = json.loads(separators_json) if separators_json else None
        return _split_cocoindex_sync(
            text, chunk_size, chunk_overlap,
            splitter_type=splitter_type or _cocoindex_splitter_type_from_env(),
            language=language,
            separators=_seps,
        )
else:
    def split_with_cocoindex(
        text: str,
        chunk_size: int = 1024,
        chunk_overlap: int = 128,
        splitter_type: str = "",
        language: str = "",
        separators_json: str = "",
    ) -> List[TextChunk]:
        _seps: Optional[List[str]] = json.loads(separators_json) if separators_json else None
        return _split_cocoindex_sync(
            text, chunk_size, chunk_overlap,
            splitter_type=splitter_type or _cocoindex_splitter_type_from_env(),
            language=language,
            separators=_seps,
        )


if _COCO_AVAILABLE:
    @coco.fn
    def split_with_langchain(
        text: str,
        chunk_size: int = 1024,
        chunk_overlap: int = 128,
        splitter_type: str = "",
    ) -> List[TextChunk]:
        """Split text with any LangChain splitter, selected by ``LC_SPLITTER_TYPE``.

        ``splitter_type`` values (mirrors ``LC_SPLITTER_TYPE`` env var):

        ``recursive``             RecursiveCharacterTextSplitter (default)
        ``character``             CharacterTextSplitter
        ``token``                 TokenTextSplitter — requires tiktoken
        ``markdown``              MarkdownTextSplitter
        ``python``                PythonCodeTextSplitter
        ``sentence_transformers`` SentenceTransformersTokenTextSplitter

        If ``splitter_type`` is empty, the ``LC_SPLITTER_TYPE`` env var is read.
        Falls back to ``"recursive"`` when neither is set.

        Not memoized — cheap CPU operation.  Downstream callers (embed, KG extract)
        are memoized per individual chunk text and skip unchanged chunks automatically.
        """
        return _split_langchain_sync(
            text, chunk_size, chunk_overlap,
            splitter_type or _lc_splitter_type_from_env(),
        )
else:
    def split_with_langchain(
        text: str,
        chunk_size: int = 1024,
        chunk_overlap: int = 128,
        splitter_type: str = "",
    ) -> List[TextChunk]:
        return _split_langchain_sync(
            text, chunk_size, chunk_overlap,
            splitter_type or _lc_splitter_type_from_env(),
        )
