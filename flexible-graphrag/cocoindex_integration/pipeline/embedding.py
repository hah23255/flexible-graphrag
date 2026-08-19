"""Memoized embedding helper for the CocoIndex pipeline.

Keyed on ``(chunk_texts_json, embed_cfg_json)`` so the OpenAI/Ollama/etc. API is
only called when file content or embedding config actually changes.  Returns a
JSON string ``[[float, …], …]`` (one list per chunk, same order).

Imports ``cocoindex`` for the ``@coco.fn(memo=True)`` decorator, so it must only
be imported after :mod:`bootstrap` has run (which ``pipeline/app.py`` guarantees).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List

import cocoindex as coco

logger = logging.getLogger(__name__)


def _build_embed_cfg_json(cfg: Dict[str, Any]) -> str:
    """Extract only embedding-relevant keys from cfg into a stable JSON string."""
    keys = (
        "llm_provider", "llm_config_json", "embedding_kind", "embedding_model",
        "embedding_dimension", "cocoindex_embedding_model",
    )
    return json.dumps({k: cfg.get(k) for k in keys if k in cfg}, sort_keys=True)


@coco.fn(memo=True)
async def _embed_chunks_cached(
    chunk_texts_json: str,   # JSON list of chunk text strings
    embed_cfg_json: str,     # JSON subset of cfg controlling the embed model
) -> str:
    """Memoized batch embedding.

    Returns a JSON-encoded ``List[List[float]]`` — one embedding per chunk.
    CocoIndex stores the result in LMDB keyed by (chunk_texts, embed_config);
    on unchanged files + unchanged embedding model the API is never called again.

    When ``embedding_kind == "sentence_transformer"`` in ``embed_cfg_json``, embeddings
    are produced by ``cocoindex.ops.sentence_transformers.SentenceTransformerEmbedder``
    (local sentence-transformers model, thread-safe GPU, ``VectorSchemaProvider``).
    All other values delegate to the flexible-graphrag embedding factory
    (OpenAI, Ollama, Google, Bedrock, etc.).
    """
    chunk_texts: List[str] = json.loads(chunk_texts_json)
    if not chunk_texts:
        return "[]"

    embed_cfg: Dict[str, Any] = json.loads(embed_cfg_json)
    _kind = (embed_cfg.get("embedding_kind") or "").lower()

    # ── CocoIndex-native SentenceTransformerEmbedder ────────────────────
    if _kind == "sentence_transformer":
        try:
            from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
        except ImportError:
            raise ImportError(
                "COCOINDEX_EMBEDDING_KIND=sentence_transformer requires sentence-transformers "
                "via CocoIndex. Install with: uv pip install \"cocoindex[sentence-transformers]\""
            ) from None
        # Resolve model: COCOINDEX_EMBEDDING_MODEL > EMBEDDING_MODEL > default
        _model = (
            embed_cfg.get("cocoindex_embedding_model")
            or embed_cfg.get("embedding_model")
            or "all-MiniLM-L6-v2"
        )
        # SentenceTransformerEmbedder caches the model in memory and handles
        # thread-safe GPU access automatically (CocoIndex internal pool).
        embedder = SentenceTransformerEmbedder(_model)
        embeddings: List[List[float]] = []
        for _txt in chunk_texts:
            _arr = await embedder.embed(_txt)  # returns numpy ndarray
            embeddings.append(_arr.tolist())
        logger.info(
            "[embed] sentence_transformer SentenceTransformerEmbedder(%s): %d chunk(s)",
            _model, len(chunk_texts),
        )
        return json.dumps(embeddings)

    # ── CocoIndex LiteLLM (100+ cloud providers, no proxy server needed) ──
    # COCOINDEX_EMBEDDING_KIND=litellm uses cocoindex.ops.litellm.LiteLLMEmbedder
    # which calls the litellm Python library directly — no proxy process, no
    # Windows antivirus issues (unlike EMBEDDING_KIND=litellm which needs the
    # LiteLLM proxy server running in WSL2).
    if _kind == "litellm":
        try:
            from cocoindex.ops.litellm import LiteLLMEmbedder  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "COCOINDEX_EMBEDDING_KIND=litellm requires cocoindex[litellm]. "
                "Install with: uv pip install 'cocoindex[litellm]'"
            ) from None
        _model = (
            embed_cfg.get("cocoindex_embedding_model")
            or embed_cfg.get("embedding_model")
            or "text-embedding-3-small"
        )
        embedder = LiteLLMEmbedder(_model)
        embeddings = []  # List[List[float]]
        for _txt in chunk_texts:
            _arr = await embedder.embed(_txt)
            embeddings.append(_arr.tolist())
        logger.info(
            "[embed] litellm LiteLLMEmbedder(%s): %d chunk(s)",
            _model, len(chunk_texts),
        )
        return json.dumps(embeddings)

    # ── flexible-graphrag embedding factory (OpenAI, Ollama, Google, …) ─
    try:
        from llamaindex.llm.embedding_factory import create_embedding_model
        from config import Settings, LLMProvider  # type: ignore[import-untyped]
        _fg_settings = Settings()
        _llm_prov_str = embed_cfg.get("llm_provider", "openai").lower()
        try:
            _llm_prov = LLMProvider(_llm_prov_str)
        except ValueError:
            _llm_prov = LLMProvider.OPENAI
        _llm_cfg: Dict[str, Any] = json.loads(embed_cfg.get("llm_config_json") or "{}")
        embed_model = await asyncio.to_thread(
            create_embedding_model, _llm_prov, _llm_cfg, _fg_settings
        )
        batch = await asyncio.to_thread(
            embed_model.get_text_embedding_batch,
            chunk_texts,
            show_progress=False,
        )
        embeddings = [list(e) for e in batch]
        _emb_model_name = embed_cfg.get("embedding_model") or "(default)"
        logger.info(
            "[embed] %s/%s: %d chunk(s)",
            _llm_prov_str, _emb_model_name, len(chunk_texts),
        )
        return json.dumps(embeddings)
    except Exception as exc:
        logger.warning("[embed] embedding failed (%s) — returning empty embeddings", exc)
        return json.dumps([[] for _ in chunk_texts])
