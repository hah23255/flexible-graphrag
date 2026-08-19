"""@coco.fn decorated functions for the flexible-graphrag / CocoIndex integration.

Submodules
----------
llm
    LLM text-generation provider factories.
    Reads ``LLM_PROVIDER`` env var.
    ``get_llama_index_llm()``, ``get_langchain_llm()``,
    ``DYNAMIC_LLM_PROVIDERS``, ``supports_embeddings()``.

embedding
    Embedding provider factories — independent of LLM_PROVIDER.
    Reads ``EMBEDDING_KIND`` env var.
    ``get_llamaindex_embedding()``, ``get_langchain_embedding()``.
    For in-pipeline embeddings prefer CocoIndex's built-in
    ``coco.functions.LiteLLM.text_embedding()`` or
    ``coco.functions.SentenceTransformer.encode()``.

kg_extraction
    @coco.fn ``extract_kg_llamaindex`` / ``extract_kg_langchain``, plus
    ``extract_kg_custom`` (the dispatcher for registered custom extractors).
    Honours: USE_ONTOLOGY, KG_EXTRACTOR_TYPE, SCHEMA_NAME,
    STRICT_SCHEMA_VALIDATION, DISABLE_PROPERTIES, MAX_TRIPLETS_PER_CHUNK.

kg_extractors
    Bring-your-own extraction: ``KGExtractor`` base class,
    ``KGExtractionContext``, ``register_kg_extractor``.  Point
    ``KG_EXTRACTOR_BACKEND`` at a registered name, ``module:Class``, or
    ``/path/mod.py:Class``.  Bump ``KGExtractor.version`` when logic changes or
    memoised results from the old implementation keep being served.

doc_processing
    @coco.fn ``parse_document(file_bytes, file_name, cfg_json)`` — single memoized
    entry point for both Docling and LlamaParse v2 via ``DocumentProcessor``.
    ``build_parse_cfg_json(cfg)`` extracts parse-affecting config into the memo key.

chunking
    @coco.fn ``split_with_llamaindex`` / ``split_with_langchain``.

Independence note
-----------------
``LLM_PROVIDER`` and ``EMBEDDING_KIND`` are completely independent —
a pipeline can use ``LLM_PROVIDER=ollama`` for KG extraction while
using ``EMBEDDING_KIND=openai`` for vector embeddings.
"""
