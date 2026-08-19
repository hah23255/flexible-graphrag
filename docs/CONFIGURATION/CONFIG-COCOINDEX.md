# CocoIndex Configuration

Every `.env` setting that affects the CocoIndex pipeline is covered here. For what the pipeline *is* and when to
use it, start with [CocoIndex Integration](../GETTING-STARTED/COCOINDEX-INTEGRATION.md).

All of these are read only when `PIPELINE_BACKEND=cocoindex`, except the shared
`CHUNKER_BACKEND` / `GRAPH_BACKEND` / `VECTOR_BACKEND` settings and the database selectors,
which apply to both pipelines.

---

## Turning it on

```bash
PIPELINE_BACKEND=cocoindex
```

Ingest requests (`/api/ingest*`, UI, MCP) route through a `coco.App` instead of the default
LlamaIndex/LangChain pipeline. Search and QA are unaffected — they read the same indexes.

!!! warning "Do not combine with these"
    `ENABLE_INCREMENTAL_UPDATES=true` and `ENABLE_LANGFLOW_FLOWS=true` are both mutually
    exclusive with CocoIndex mode. Startup disables them and logs a warning rather than running
    two ingest systems against one set of indexes.

---

## Core settings

```bash
# LMDB state — what CocoIndex has already processed, plus its step-level memo cache.
# Delete this to force a full reprocess.  Separate from Postgres document_state
# (which CocoIndex mode does not use at all).
COCOINDEX_DB=./cocoindex.db

# Watch directory for the primary filesystem source, and the staging area for UI
# file uploads.  Relative paths resolve against the PROCESS working directory —
# "./cocoindex-docs" means something different when run from the repo root than
# from flexible-graphrag/.  Use an absolute path if that is ever ambiguous.
WATCH_DIR=./cocoindex-docs

# Refresh cadence in seconds (default 60).  Change detection is always live; this
# is the BACKUP cadence:
#   - periodic re-scan for sources with no change stream (web, wikipedia, youtube,
#     cmis, and the native S3 / Azure / Drive connectors)
#   - the localfs watcher's full-rescan backstop
#   - restart backoff for a live stream that ends or errors
# Set to 0 to disable the backup poll for sources that have no change stream.
COCOINDEX_POLL_INTERVAL=60

# Telemetry opt-out (any non-empty value other than "0" disables it).
COCOINDEX_DISABLE_USAGE_TRACKING=1
```

### Logging

```bash
# Python-side logging for Flexible GraphRAG and the integration code.
LOG_LEVEL=INFO
FLEXIBLE_GRAPHRAG_LOG=flexible-graphrag-cocoindex.log

# CocoIndex's own Rust-level tracing (terminal).  Independent of LOG_LEVEL.
COCOINDEX_LOG_LEVEL=INFO
```

---

## Choosing the source

```bash
# Which source starts with the server.
#   unset       -> filesystem (watching WATCH_DIR)
#   <name>      -> that source starts at boot
#   "" or none  -> no primary source; the UI / REST / MCP supplies one
DATA_SOURCE=filesystem
```

All 14 sources are supported: `filesystem`, `s3`, `gcs`, `azure_blob`, `google_drive`,
`onedrive`, `sharepoint`, `box`, `alfresco`, `nuxeo`, `cmis`, `web`, `wikipedia`, `youtube`.
Each uses the same credentials as in the default pipeline (`S3_CONFIG`, `ALFRESCO_CONFIG`,
`NUXEO_CONFIG`, …), so nothing source-specific changes when you switch pipelines.

```bash
# How the pipeline READS documents.
#   flexible  (default) - Flexible GraphRAG adapters for all 14 sources
#   cocoindex           - native CocoIndex connectors where one exists
#                         (localfs, s3, azure_blob, google_drive); every other
#                         source transparently falls back to flexible
SOURCE_BACKEND=flexible
```

The 10 detector-backed sources (filesystem, S3, GCS, Azure Blob, Google Drive, OneDrive,
SharePoint, Box, Alfresco, Nuxeo) run a live change stream and download only changed files. The
other 4 are snapshot-only and re-scan on `COCOINDEX_POLL_INTERVAL`.

For standalone `cocoindex update`, use a real `DATA_SOURCE` — `""` / `none` is for UI-driven
ingest and leaves the CLI with no source to run.

---

## Choosing targets

The pipeline uses the same database selectors as the default pipeline:

```bash
PG_GRAPH_DB=neo4j            # 15 options, or none
RDF_GRAPH_DB=graphdb         # fuseki | graphdb | oxigraph | neptune_rdf | none
VECTOR_DB=qdrant             # 10 options, or none
SEARCH_DB=elasticsearch      # elasticsearch | opensearch | bm25 | none
```

Connection details come from the usual per-store variables (`NEO4J_GRAPH_DB_CONFIG`,
`QDRANT_VECTOR_DB_CONFIG`, …) — see
[Database Configuration](../DATABASES/DATABASE-CONFIGURATION.md).

### Native vs. Flexible target connectors

```bash
GRAPH_BACKEND=llamaindex     # llamaindex (default) | langchain | cocoindex
VECTOR_BACKEND=llamaindex    # llamaindex (default) | langchain | cocoindex
SEARCH_BACKEND=llamaindex    # llamaindex (default) | langchain
```

| | `*_BACKEND=cocoindex` covers | everything else |
|---|---|---|
| Property graph | Neo4j, FalkorDB, SurrealDB | Flexible adapters (all 15) |
| Vector | Qdrant, LanceDB, Postgres (pgvector) | Flexible adapters (all 10) |
| RDF | — | Flexible adapters (all 4) |
| Search | — | Flexible adapters (all 3) |

Setting `*_BACKEND=cocoindex` for an unsupported store is not an error: the pipeline logs the
downgrade and uses the Flexible adapter.

---

## Pipeline stages

### Document processing

```bash
DOCUMENT_PARSER=docling      # docling (default, local) | llamaparse (cloud) | liteparse (local)
```

All three work inside the pipeline and are memoized per file, so unchanged files are never
re-parsed. Parser-specific settings (`LLAMAPARSE_MODE`, `LITEPARSE_OCR`, `DOCLING_*`) behave
exactly as in the default pipeline.

`parser_type` is part of the memo key, so switching parsers re-parses everything — as it must,
since the text changes.

!!! note "Parsers rewrite markdown differently"
    `docling` normalises `.md`/`.txt` — it keeps blank lines but strips `#` heading markers.
    `liteparse` reads both through unchanged. That only matters if something downstream keys on
    markup, such as a heading-based chunker or a custom extractor that splits on headings.

### Chunking

```bash
CHUNK_SIZE=1024
CHUNK_OVERLAP=128

CHUNKER_BACKEND=llamaindex   # llamaindex (default) | langchain | cocoindex

# Applies when CHUNKER_BACKEND=langchain
#   recursive (default) | character | token | markdown | python | sentence_transformers
#LC_SPLITTER_TYPE=recursive

# Applies when CHUNKER_BACKEND=cocoindex — requires: uv pip install "cocoindex[text]"
#   recursive  RecursiveSplitter (default); syntax-aware tree-sitter splits for 30+ languages
#   separator  SeparatorSplitter; splits on regex separators, then packs to CHUNK_SIZE
COCOINDEX_SPLITTER_TYPE=recursive

# recursive only. Leave empty to auto-detect from the file extension.
#COCOINDEX_LANGUAGE=markdown        # markdown, python, typescript, rust, sql, …

# separator only. Two accepted formats:
#   JSON array          use when patterns contain commas, like \n{2,}
#     COCOINDEX_SEPARATORS=["\\n{2,}", "[.!?…]\\s+", "[:;]\\s+"]
#   Pipe-separated      simpler; splits on | so patterns must not contain one
#     COCOINDEX_SEPARATORS=\n{2,}|\. |;
# Default when unset: ["\\n{2,}", "[.!?…]\\s+", "[:;]\\s+"]  (paragraph → sentence → clause)
#COCOINDEX_SEPARATORS=
```

`SeparatorSplitter` emits one fragment per separator match and then **packs** consecutive
fragments up to `CHUNK_SIZE`. A separator alone therefore does not guarantee one chunk per
match: if the fragments fit, they are packed back together. Size the two together when you want
a specific granularity.

### Embedding

`COCOINDEX_EMBEDDING_KIND` overrides `EMBEDDING_KIND` for this pipeline only, so both pipelines
can use different embedders from one `.env`.

```bash
# Leave unset to inherit EMBEDDING_KIND from the main pipeline (the usual choice).
#COCOINDEX_EMBEDDING_KIND=

# Local sentence-transformers — GPU-accelerated, no API key.
#   Requires: uv pip install "cocoindex[sentence-transformers]"
#COCOINDEX_EMBEDDING_KIND=sentence_transformer
#COCOINDEX_EMBEDDING_MODEL=all-MiniLM-L6-v2                 # 384 dims, fastest (default)
#COCOINDEX_EMBEDDING_MODEL=all-mpnet-base-v2                # 768 dims, higher quality
#COCOINDEX_EMBEDDING_MODEL=BAAI/bge-small-en-v1.5           # 384 dims, strong retrieval
#COCOINDEX_EMBEDDING_MODEL=nomic-ai/nomic-embed-text-v1.5   # 768 dims, long context

# LiteLLM in-process (100+ providers, no proxy server).
#   Requires: uv pip install "cocoindex[litellm]"
#COCOINDEX_EMBEDDING_KIND=litellm
```

Any other value (`openai`, `ollama`, `google`, `vertex`, `azure`, `bedrock`, `fireworks`,
`openai_like`) delegates to the same provider the main pipeline uses.

#### Which implementation actually runs

Only two settings switch to a CocoIndex-native implementation. Everything else — including
every LLM call — uses Flexible GraphRAG's own LlamaIndex / LangChain factories, the same ones
the default pipeline uses.

| | CocoIndex's own | Flexible GraphRAG's (LlamaIndex / LangChain) |
|---|---|---|
| Embeddings | `COCOINDEX_EMBEDDING_KIND=sentence_transformer` or `litellm` | unset (inherits `EMBEDDING_KIND`), or any provider name |
| Chunking | `CHUNKER_BACKEND=cocoindex` | `llamaindex` (default) or `langchain` |
| LLM (KG extraction, entity resolution) | — | always |

So `LLM_PROVIDER` and its credentials behave exactly as in the default pipeline no matter how
this pipeline is configured, and an all-CocoIndex embedding setup still calls your normal LLM
for extraction.

!!! danger "Dimensions must match the collection"
    If this pipeline embeds with a different model than the one that created your vector
    collection, writes fail on a dimension mismatch. Either use the same embedder in both
    pipelines, or give the CocoIndex pipeline its own collection.

    ```bash
    # Only set this when COCOINDEX_EMBEDDING_KIND is also set.
    #COCOINDEX_EMBEDDING_DIMENSION=384
    ```

### KG extraction

```bash
ENABLE_KNOWLEDGE_GRAPH=true
KG_EXTRACTOR_BACKEND=llamaindex   # llamaindex (default) | langchain | a custom extractor
#KG_EXTRACTION_TIMEOUT=120        # seconds, per chunk

# Ontology-guided extraction: entity/relation types come from your .ttl files.
USE_ONTOLOGY=false                # false (default) | true
```

The two built-ins run through Flexible GraphRAG — LlamaIndex `SchemaLLMPathExtractor` /
`DynamicLLMPathExtractor`, or LangChain `LLMGraphTransformer`. There is no native CocoIndex
extractor that produces the ontology-guided multi-label entity graphs this project builds.
Results are memoized per chunk *and* per schema, so editing an ontology only re-extracts the
chunks it affects.

#### Custom extractors

`KG_EXTRACTOR_BACKEND` also accepts your own extractor — subclass `KGExtractor` from
`cocoindex_integration.functions.kg_extractors` and point the variable at it. **CocoIndex
pipeline only**; the default pipeline uses the two built-ins.

```bash
#KG_EXTRACTOR_BACKEND=meeting_notes                          # a registered name
#KG_EXTRACTOR_BACKEND=my_package.extractors:MyExtractor      # importable module + class
#KG_EXTRACTOR_BACKEND=./extractor.py:MyExtractor             # file path + class, no install

# Imported before resolution, so @register_kg_extractor names become reachable
# without naming the class. Comma-separated.
#KG_EXTRACTOR_MODULES=my_package.extractors
```

An extractor takes one chunk and returns a `KGResult` — plain triples and entities — so the
graph is written by whichever target `PG_GRAPH_DB` / `GRAPH_BACKEND` select. It can call
`ctx.builtin()` to hand content it does not recognise back to the built-in extractor, which is
how one run covers a source that is not uniform.

!!! warning "Bump `version` when you edit an extractor"
    Extraction is memoized on `(chunk_text, spec, version)`. Editing the class does **not**
    re-extract on its own — you keep reading the previous implementation's cached triples until
    you bump its `version` attribute or delete the LMDB state.

#### Entity resolution

```bash
# Applied to one document's triples after all its chunks are extracted.
#   none      (default) unchanged behaviour, no extra dependency
#   normalize folds accents/case/punctuation ("bob smith" -> "Bob Smith")
#   llm       also merges "Bob" -> "Bob Smith", "Acme Corp" -> "Acme Corporation"
#             Requires: uv pip install "cocoindex[entity_resolution]" (faiss)
#             The CORE extra, not entity_resolution_llm — see below.
#             Without it, degrades to normalize with a warning — never fails.
ENTITY_RESOLUTION=none
```

`llm` uses Flexible GraphRAG's own `PairResolver` and `Embedder` implementations, wired to the
`LLM_PROVIDER` and embedding model you already configured — so resolution uses the same models
as the rest of the pipeline, and CocoIndex's
[built-in LLM resolver](https://cocoindex.io/docs/ops/entity_resolution/)
(`cocoindex[entity_resolution_llm]`) is not needed.

Per document, not per corpus: two different files are never merged together. Extraction is per
chunk, so this runs afterwards — `Bob` in one chunk and `Bob Smith` in the next are only
comparable once both exist.

!!! danger "Resolution rewrites entity ids"
    Enabling it against a corpus already ingested without it leaves old and new nodes
    disagreeing. Turn it on from the start, or re-ingest clean.

---

## Monitoring

Optional Postgres tables in the incremental-updates database (`POSTGRES_INCREMENTAL_URL`).
Observability only — it never drives change detection.

```bash
COCOINDEX_ENABLE_PG_MONITORING=true

# Row volume for cocoindex_ingest_log:
#   summary (default) - one row per file: the terminal event and its status
#   stages            - one row per pipeline stage (~13 per file); use when
#                       debugging where a file stalls
#COCOINDEX_MONITOR_DETAIL=summary
```

**`cocoindex_run_log`** — one row per update cycle:

```sql
SELECT trigger, adds, deletes, unchanged, errors, elapsed_s, logged_at
FROM cocoindex_run_log ORDER BY logged_at DESC LIMIT 20;
```

`adds` / `deletes` / `unchanged` are **document** counts. CocoIndex's own totals sum every
component it ran — the per-file worker plus parsing plus embedding plus one KG extraction per
*chunk* — so a single 5-chunk file would otherwise read as `adds=8`. These columns are filtered
to the per-document components; the raw per-component breakdown is kept in the `note` column.

**`cocoindex_ingest_log`** — per-file rows from the pipeline's progress hook. Memo hits and pure
deletes produce no rows, so no rows for a file means "nothing needed doing".

```sql
SELECT trigger, file_path, stage, status, detail, logged_at
FROM cocoindex_ingest_log ORDER BY logged_at DESC LIMIT 50;
```

---

## A complete example

Mixed native and Flexible connectors:

```bash
# Pipeline
PIPELINE_BACKEND=cocoindex
COCOINDEX_DB=./cocoindex.db
WATCH_DIR=./cocoindex-docs
COCOINDEX_POLL_INTERVAL=60
COCOINDEX_ENABLE_PG_MONITORING=true

# Stage backends
CHUNKER_BACKEND=cocoindex          # native syntax-aware splitter
GRAPH_BACKEND=cocoindex            # native Neo4j
VECTOR_BACKEND=cocoindex           # native Qdrant
KG_EXTRACTOR_BACKEND=llamaindex    # always Flexible

# Databases
PG_GRAPH_DB=neo4j
NEO4J_GRAPH_DB_CONFIG={"url": "bolt://localhost:7687", "username": "neo4j", "password": "password"}
VECTOR_DB=qdrant
QDRANT_VECTOR_DB_CONFIG={"host": "localhost", "port": 6333}
SEARCH_DB=elasticsearch
ELASTICSEARCH_SEARCH_DB_CONFIG={"hosts": ["http://localhost:9200"]}

# LLM + embeddings
LLM_PROVIDER=openai
OPENAI_MODEL=gpt-4.1-mini
OPENAI_API_KEY=sk-...
EMBEDDING_KIND=openai
OPENAI_EMBEDDING_MODEL=text-embedding-3-small

# Document processing / chunking
DOCUMENT_PARSER=docling
CHUNK_SIZE=1024
CHUNK_OVERLAP=128
```

---

## Related documentation

- [CocoIndex Integration](../GETTING-STARTED/COCOINDEX-INTEGRATION.md) — overview and quick start
- [CocoIndex Developer Guide](../DEVELOPER/DEVELOPER-COCOINDEX.md) — internals and extension points
- [Environment Configuration](../GETTING-STARTED/ENVIRONMENT-CONFIGURATION.md) — general `.env` reference
- [Database Configuration](../DATABASES/DATABASE-CONFIGURATION.md) — connection settings per store
