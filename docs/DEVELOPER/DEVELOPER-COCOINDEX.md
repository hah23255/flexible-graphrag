# CocoIndex Developer Guide

How `cocoindex_integration/` is put together, the APIs you would call to build on it, and the
places where behaviour is easy to get wrong.

For setup and settings, see [CocoIndex Integration](../GETTING-STARTED/COCOINDEX-INTEGRATION.md)
and [CocoIndex Configuration](../CONFIGURATION/CONFIG-COCOINDEX.md).

**If you are here to build something**, jump to what you want to change:

| I want to… | go to |
|---|---|
| replace how entities are extracted | [Custom extractors](#the-processing-functions) |
| add a data source — Flexible adapter or CocoIndex-native connector | [Extending it](#extending-it) |
| add a target store — Flexible adapter or CocoIndex-native connector | [Extending it](#extending-it) |
| add a processing stage (`@coco.fn`) of my own | [The processing functions](#the-processing-functions) |
| assemble my own pipeline / `coco.App` | [Extending it](#extending-it) |
| understand what runs, in order | [How a document flows through](#how-a-document-flows-through) |
| find out why my change did not re-run | [Gotchas](#gotchas) |

None of that is limited to what this integration already wires. A CocoIndex connector, function
or flow you write yourself drops in the same way — this package is a set of adapters and
conventions around CocoIndex, not a fence around it.

The fastest way in is a worked example:
[`examples/cocoindex/meeting_notes_graph_any/`](../../examples/cocoindex/meeting_notes_graph_any/README.md)
is a port of a CocoIndex example that exercises most of this — a custom `KGExtractor`, entity
resolution, the flexible and native target paths, and three ways to run the same code (a small
purpose-built app, the standard pipeline, and the server). CocoIndex's own
[examples](https://cocoindex.io/docs/examples/)
([source](https://github.com/cocoindex-io/cocoindex/tree/main/examples)) are worth reading
alongside it for the plain-CocoIndex idioms.

---

## How a document flows through

There is **one** `app_main` for every source — `flexible_app_main` — and one `coco.App` per
configured data source.

```
REST / MCP / UI  ──►  bridge.py  ──►  coco.App(flexible_app_main, cfg_json)
                                             │
                                             ├── source_backend=cocoindex?
                                             │     └── NATIVE_READERS[source] → (lister, worker)
                                             │           localfs · s3 · azure_blob · google_drive
                                             │
                                             ├── detector-backed source?  (10 of 14)
                                             │     └── FlexibleMapView  → lazy, change-aware
                                             │
                                             └── otherwise
                                                   └── FlexibleDataSource → eager snapshot

                            coco.mount_each(worker, items, cfg_json)
                                             │
                                             ▼
                            worker  @coco.fn(memo=True)   ← one component per document
                                             │
                                             ▼
                                     run._run_pipeline()
                    parse → chunk → embed → KG extract → declare target states
```

`_run_pipeline` is the single implementation of the pipeline body; every source path funnels
into it, which is why adding a source never means touching the processing stages.

**Where change detection happens** depends on the path:

| Path | Listing | Change signal |
|---|---|---|
| `FlexibleMapView` (10 detector-backed sources) | `detector.list_all_files()` — metadata only, no downloads | `detector.get_changes()` streamed to CocoIndex's subscriber |
| Native CocoIndex localfs | `walk_dir(live=True)` | OS file watcher + `rescan_interval` backstop |
| Native S3 / Azure / Drive | connector `list_*` | none — reconciled by the backup poll |
| `FlexibleDataSource` (web, wikipedia, youtube, cmis) | eager scan | none — reconciled by the backup poll |

Bytes are fetched lazily on the `FlexibleMapView` path: the fingerprint is the `etag` when the
source provides one, otherwise the detector's `ordinal` (mtime/version), so an unchanged file is
never downloaded.

---

## Package layout

```
cocoindex_integration/
  bridge.py            FastAPI ↔ CocoIndex: app lifecycle, live streams, backup poll,
                       ingest_files() / ingest_source(), datasource_config persistence
  retriever_bridge.py  read path — vector/graph retrievers when *_BACKEND=cocoindex
  surreal_chain.py     QA chain for CocoIndex's flat SurrealDB schema
  entity_resolution.py ENTITY_RESOLUTION=normalize|llm — our PairResolver + Embedder
                       plugged into CocoIndex's protocols, using the configured models
  monitoring.py        optional Postgres monitoring tables
  _compat.py           Python 3.14 asyncio / sniffio / anyio patches

  pipeline/
    app.py             thin entry point; module-level `app` for `cocoindex update`
    flexible_app.py    flexible_app_main (the one app_main) + build_app_for_config()
                       + _resolve_pipeline_config() backend resolution & fallbacks
    native_apps.py     native per-file workers + NATIVE_READERS registry
    run.py             _run_pipeline() — parse → chunk → embed → KG → targets
    selectors.py       target pickers (vector / pg / rdf / search) + native root mounts
    providers.py       registers flexible TargetStateProviders with CocoIndex
    state.py           process-wide singletons and runtime flags
    env_config.py      .env → config dict (also the memo key)
    embedding.py       _embed_chunks_cached — memoized batch embedding
    bootstrap.py       import-time setup that MUST run before `import cocoindex`

  functions/           @coco.fn processing stages
    doc_processing.py  parse_document (memo=True)
    chunking.py        split_with_llamaindex / _langchain / _cocoindex (not memoized)
    embedding.py       embedding-provider construction helpers
    kg_extraction.py   extract_kg_llamaindex / _langchain / _custom (memo=True)
    kg_extractors.py   KGExtractor base class + registry — bring your own extractor
    llm.py             LLM provider construction

  connectors/
    seam.py            is_coco_vector / is_coco_pg — the native-vs-flexible fork
    rows.py            VectorRow / ChunkRow / KGTripleRow / SearchRow (the shared contract)
    flexible/          targets + sources backed by Flexible GraphRAG adapters
      base.py            FlexibleConnector, FlexibleReconcileHandler, flush barrier
      vector.py  property_graph.py  rdf.py  search.py
      source.py          FlexibleDataSource — eager iterator over all 14 sources
      _map_view.py       FlexibleMapView — LiveMapView over a detector
      _file.py           FlexibleFile — lazy bytes + fingerprint
      _sources/          per-source lazy listing / single-key download
    cocoindex/         native CocoIndex connectors
      _runtime.py        ContextKeys, @coco.lifespan, connector patches
      vector/            qdrant.py  lancedb.py  postgres.py  (+ retrievers)
      property_graph/    neo4j.py  falkordb.py  surrealdb.py  (+ Cypher/SurrealQL helpers)
      sources/           localfs.py  amazon_s3.py  azure_blob.py  google_drive.py
```

---

## The processing functions

These are the real signatures. Every one is a plain `@coco.fn`, so you can compose them into
your own app.

### Parsing — `functions/doc_processing.py`

One entry point covers all three parsers; `DOCUMENT_PARSER` selects which runs.

```python
from cocoindex_integration.functions.doc_processing import (
    parse_document, build_parse_cfg_json, decode_parse_result,
)

result = await parse_document(file_bytes, file_name, build_parse_cfg_json(cfg))
text, metadata = decode_parse_result(result)
```

`memo=True`. Parsing is the most expensive per-document step (Docling CPU work, or a paid
LlamaParse call), so unchanged bytes are never re-parsed — and changing chunking or embedding
config does not invalidate it. The config is passed as a JSON *string* so CocoIndex can
fingerprint it; that is the pattern throughout.

### Chunking — `functions/chunking.py`

```python
from cocoindex_integration.functions.chunking import (
    split_with_llamaindex, split_with_langchain, split_with_cocoindex, TextChunk,
)

chunks = split_with_llamaindex(text, chunk_size=1024, chunk_overlap=128)
chunks = split_with_langchain(text, 1024, 128, splitter_type="markdown")
chunks = split_with_cocoindex(text, 1024, 128, splitter_type="recursive", language="python")
```

Empty `splitter_type` falls back to `LC_SPLITTER_TYPE` / `COCOINDEX_SPLITTER_TYPE`. For
`splitter_type="separator"`, pass `separators_json` — a JSON *array string*, not a list, so the
separator set participates in the memo key.

**Not memoized, deliberately.** Chunking is cheap CPU work that returns large lists; caching it
would cost more than it saves. The steps downstream are memoized *per chunk*, so only changed
chunks trigger API calls anyway.

### Embedding — `pipeline/embedding.py`

```python
from cocoindex_integration.pipeline.embedding import (
    _build_embed_cfg_json, _embed_chunks_cached,
)
import json

embeddings = json.loads(await _embed_chunks_cached(
    json.dumps([c.text for c in chunks]),
    _build_embed_cfg_json(cfg),
))
```

`memo=True`, keyed on the chunk texts plus the embedding config, so re-running only re-embeds
text that changed — and switching models invalidates exactly the affected entries. Dispatches to
CocoIndex's `SentenceTransformerEmbedder` or `LiteLLMEmbedder` when `COCOINDEX_EMBEDDING_KIND`
selects them, and otherwise to Flexible GraphRAG's embedding factory.

(`functions/embedding.py` is a different thing: `get_llamaindex_embedding()` /
`get_langchain_embedding()` build provider objects. It is not the pipeline entry point.)

### KG extraction — `functions/kg_extraction.py`

```python
from cocoindex_integration.functions.kg_extraction import (
    extract_kg_llamaindex, extract_kg_langchain,
    load_ontology_schema_json, load_extractor_config_json, _kg_result_from_json,
)

raw = await extract_kg_llamaindex(
    chunk_text,
    schema_json=load_ontology_schema_json(use_ontology=True, ontology_dir="ontologies/"),
    llm_provider="openai",
    llm_config_json="{}",
    extractor_config_json=load_extractor_config_json(),
)
result = _kg_result_from_json(raw)   # -> KGResult(triples=[KGTriple, ...], entities=[...])
```

**Returns a JSON string, not a `KGResult`.** `KGResult` holds `Dict[str, Any]` fields that
CocoIndex's type introspection cannot serialise into LMDB, so the memoized boundary is a string;
`_kg_result_from_json()` rebuilds the object.

`memo=True`, keyed on chunk text *and* schema. Editing an ontology re-extracts only the chunks it
affects, which is the single biggest cost saving in the pipeline.

`extractor_config_json` carries `KG_EXTRACTOR_TYPE`, `MAX_TRIPLETS_PER_CHUNK`,
`DISABLE_PROPERTIES`, `STRICT_SCHEMA_VALIDATION`. All default from env; pass it explicitly only
when you want config changes to invalidate the cache.

### Custom extractors — `functions/kg_extractors.py`

`KG_EXTRACTOR_BACKEND` accepts your own extractor instead of either built-in. Subclass
`KGExtractor`: one chunk in, a `KGResult` out.

```python
from cocoindex_integration.functions.kg_extraction import KGResult, KGTriple, KGEntity
from cocoindex_integration.functions.kg_extractors import (
    KGExtractor, KGExtractionContext, register_kg_extractor,
)

@register_kg_extractor("meeting_notes")
class MeetingNotesExtractor(KGExtractor):
    version = "1"                      # bump when behaviour changes — see below

    async def extract(self, chunk_text: str, ctx: KGExtractionContext) -> KGResult:
        if not looks_like_mine(chunk_text):
            return await ctx.builtin(chunk_text)      # delegate what you don't recognise
        llm = ctx.llamaindex_llm()                    # or ctx.langchain_llm()
        ...
        return KGResult(triples=[...], entities=[...])
```

Point `KG_EXTRACTOR_BACKEND` at it three ways — a registered name, `module:Class`, or
`/path/to/file.py:Class` (no install, no `sys.path` setup). `KG_EXTRACTOR_MODULES` imports
modules first so registered names resolve.

Three things that are easy to get wrong:

- **Memoized on `(chunk_text, spec, version)`.** The two built-ins are separate `@coco.fn`
  objects with separate keyspaces; custom extractors share one dispatcher, so the spec and
  `version` are passed as real arguments to keep them apart. Editing your class without bumping
  `version` keeps serving the old implementation's triples.
- **No document provenance in `ctx`** — no `file_name`, no `doc_id`, deliberately. An id built
  from a filename mints a different node when the same document arrives from another source.
  Derive ids from content; provenance rides on `KGTripleRow` and reaches the nodes from there.
- **`ctx.builtin()` runs the built-in extractor** with this run's ontology and provider, and
  memoizes in its own keyspace — so delegation costs nothing extra on re-runs.

CocoIndex pipeline only. The default pipeline's seam
(`adapters/process/kg_extractor_adapter.py`) speaks in framework objects rather than `KGResult`,
so it uses the two built-ins.

### Entity resolution — `entity_resolution.py`

Runs after every chunk of a document is extracted, rewriting `subject`/`obj` across its
`KGTripleRow`s and RDF rows in place — the earliest point at which `Bob` in one chunk and
`Bob Smith` in the next are comparable. Per document, never across the corpus.

`normalize` folds accents/case/punctuation in pure Python. `llm` adds semantic merges and needs
`cocoindex[entity_resolution_llm]` (or the smaller `cocoindex[entity_resolution]`; the
difference is only `instructor`). `FlexibleEmbedder` and `LLMPairResolver` implement CocoIndex's
`Embedder` and `PairResolver` protocols using the configured models, so CocoIndex's own LLM
resolver is never called on our path. Missing extra degrades to `normalize` with a warning.

`LLMPairResolver` refuses ambiguous bare first names before consulting the model: it counts, over
the whole corpus, how many full names share each first name. `Priya` merges into `Priya Raman`
only when no other `Priya …` exists. The check has to be code rather than a prompt rule — the
resolver shows the model an embedding-filtered candidate list that often omits the competing
name — and it applies to both sides of the comparison, since the scan asks
`("Priya Patel", ["Priya"])` as readily as the reverse.

---

## Targets

Both connector families share one contract — the row dataclasses in `connectors/rows.py` and the
lifecycle method *names* — but nothing else. Flexible targets write through LlamaIndex/LangChain
adapters; native targets go through CocoIndex's own reconciliation. `connectors/seam.py` is the
single place that classifies which is which:

```python
from cocoindex_integration.connectors.seam import is_coco_vector, is_coco_pg
```

### Flexible targets

```python
from cocoindex_integration.connectors.rows import VectorRow
from cocoindex_integration.connectors.flexible.vector import FlexibleVector

vector = FlexibleVector(app_config, embedding_dim=1536)
await vector.setup()                       # idempotent
await vector.declare_row(VectorRow(doc_id=..., chunk_index=0, text=..., embedding=[...]))
await vector.finalize()                    # flush buffered rows
await vector.delete_row(doc_id)            # called by the reconciler on delete
```

The same shape applies to `FlexiblePropertyGraph`, `FlexibleRDFGraph`, and `FlexibleSearch`.

`FlexibleReconcileHandler` in `flexible/base.py` supplies the CocoIndex `TargetHandler` these
share: content fingerprinting, the upsert/delete decision, and the batch apply loop. A subclass
only provides small hooks (fingerprint, action factories, declare). If you add a target, subclass
it rather than reimplementing `reconcile()`.

### Native targets

Selected by `VECTOR_BACKEND` / `GRAPH_BACKEND=cocoindex`:

| Kind | Classes |
|---|---|
| Vector | `CocoQdrant`, `CocoLanceDB`, `CocoPostgres` |
| Property graph | `CocoNeo4j`, `CocoFalkorDB`, `CocoSurrealDB` |

These mount their root collection/table once per run at `app_main` scope
(`selectors._mount_native_target_roots`). Root scope is load-bearing: the handles must outlive an
update cycle so CocoIndex can look up a deleted file's previous records and issue row-level
deletes.

`CocoNeo4j` reproduces LlamaIndex's `Neo4jPropertyGraphStore` footprint exactly — `:__Node__:Chunk`
for chunks, `:__Node__:__Entity__:<Type>` for entities, two uniqueness constraints and one vector
index — so the graph is readable by the same retrievers either way. Multi-label nodes come from an
APOC `addLabels` patch applied in `connectors/cocoindex/_runtime.py`; relations are written through
a direct Bolt driver to avoid a per-relation-type index.

### The completion barrier

Flexible targets declare state during processing, but CocoIndex calls `reconcile()` →
`_apply_actions()` *after* the file emits `file_done`. Marking a job complete on `file_done` alone
lets a search run before the vectors land.

```python
note_target_pending(doc_id)   # at declare time
note_target_flushed(doc_id)   # when the sink finishes (also on failure)
await wait_targets_flushed(timeout=180.0)   # main.py, before reporting completion
```

---

## Extending it

**A new data source.** Add it to `FlexibleDataSource._SOURCE_MAP` with an `_iter_*` method
(eager). If it has a change detector, also add it to `_SOURCE_CLASSES` in
`_sources/_lazy.py`, give its `sources/<name>.py` class a `read_file_bytes()`, add a
`_map_record()` branch, and add its env prefix to `_SOURCE_ENV_PREFIX`. `DETECTOR_BACKED` is
derived from `_SOURCE_CLASSES`, so registering there is what turns the live path on. Nuxeo is the
most recent worked example.

**A new native CocoIndex source** with no custom config mapping: call
`build_native_passthrough()` in `connectors/cocoindex/sources/__init__.py` — no new module. With
custom mapping, follow `sources/localfs.py` and add a `(lister, worker)` pair to `NATIVE_READERS`.

**A new target store.** Flexible: subclass `FlexibleReconcileHandler` and follow
`flexible/vector.py`. Native: add a builder to `COCO_VECTOR_REGISTRY` / `COCO_PG_REGISTRY` (a
`None` entry marks a store as known-but-unimplemented, which makes the fallback explicit).

**A whole custom pipeline.** Import the pieces above into your own `coco.App`. Start from
`pipeline/flexible_app.py` — `build_app_for_config()` shows how a config dict becomes a running
app, and `run._run_pipeline()` shows the full stage sequence.

---

## Running and driving it

### CLI

```bash
cd flexible-graphrag
cocoindex update cocoindex_integration/pipeline/app.py       # one catch-up pass
cocoindex update -L cocoindex_integration/pipeline/app.py    # live
cocoindex update --full-reprocess cocoindex_integration/pipeline/app.py
```

Other flags: `--reset`, `--preview`, `-f/--force`, `-q/--quiet`; global `--env-file`, `--app-dir`.

There is also `python -m cocoindex_integration.pipeline.app [dir]`, which takes an optional
filesystem directory but none of the CocoIndex CLI flags.

### REST (server mode)

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/api/cocoindex/status` | bridge status, app list, last update result |
| `POST` | `/api/cocoindex/sync-now` | manual reconcile; body `{"full_reprocess": false}` |
| `GET` | `/api/cocoindex/config` | the resolved pipeline config |

Normal ingest still goes through `/api/ingest*`; the bridge routes it into CocoIndex.

### Progress events

The pipeline emits per-file events through a hook the bridge installs:

```
downloading → downloaded → parsing → parsed → chunked → embedded →
kg_extracting → kg_extracted → vector_indexing → graph_indexing →
search_indexing → rdf_indexing → indexing_complete → file_done
```

`main.py` turns these into UI progress; `monitoring.py` optionally writes them to Postgres.
Every terminal path emits `file_done`, including failures — a missing one would hang the REST
request until its timeout.

---

## Gotchas

**`from cocoindex_integration.pipeline import app` does not give you the module.**
`pipeline/__init__.py` rebinds that name to the `coco.App` instance. Import from the defining
submodule (`pipeline.run`, `pipeline.state`) or use
`sys.modules["cocoindex_integration.pipeline.app"]`, which is what
`bridge._get_pipeline_module()` does.

**`bootstrap.py` must run before `import cocoindex`.** It neutralises `nest_asyncio.apply()` on
Python 3.14 and patches `asyncio.wait_for` / sniffio / asyncpg. `pipeline/app.py` imports it
first for exactly this reason.

**CocoIndex's stats are component-level, not document-level.** `UpdateStats.total` sums every
component — the per-file worker, parsing, embedding, and one KG extraction per *chunk*. Use
`bridge._document_level_counters()` for any number a human will read.

**`handle.watch()` yields `UpdateSnapshot`, not `UpdateStats`.** The counters live on
`.stats`; reading the snapshot directly returns zeros silently.

**`WATCH_DIR` is relative to the process working directory.** A CLI run and a server run started
from different directories watch different folders.

**`skip_graph` is a runtime flag, not a config change.** CocoIndex registers each app name once,
so rebuilding an app to change `cfg_json` raises. `set_runtime_skip_graph()` toggles KG
extraction for a cycle instead; the bridge clears it afterwards.

---

## Known limitations

1. **Mutually exclusive with `ENABLE_INCREMENTAL_UPDATES` and `ENABLE_LANGFLOW_FLOWS`.** Startup
   disables both and logs a warning.
2. **Parsing and KG extraction are always Flexible.** No native CocoIndex equivalent produces the
   ontology-guided multi-label entity graphs this project builds.
3. **Snapshot-only sources** (web, Wikipedia, YouTube, CMIS) have no change stream and are
   reconciled by the backup poll.
4. **Native S3 / Azure Blob / Google Drive connectors are scan-only.** For live change detection
   on those, use `SOURCE_BACKEND=flexible` and the detector path.
5. **CLI runs open their own store clients.** In server mode the targets reuse the adapters
   `HybridSearchSystem` has already built for querying. Under `cocoindex update` there is no
   server, so `FlexibleVector` and the other targets construct their own. Same writes either
   way — the difference is one extra connection per store, and any adapter-level caching starts
   cold.

---

## Related documentation

- [CocoIndex Integration](../GETTING-STARTED/COCOINDEX-INTEGRATION.md) — overview and quick start
- [CocoIndex Configuration](../CONFIGURATION/CONFIG-COCOINDEX.md) — every `.env` setting
- [REST API](REST-API.md) — general endpoint reference
- [Meeting notes example](../../examples/cocoindex/meeting_notes_graph_any/README.md) — a
  worked example: custom extractor, three ways to run it
- CocoIndex's own docs: [overview](https://cocoindex.io/docs/getting_started/overview/) ·
  [examples](https://cocoindex.io/docs/examples/) ·
  [example source](https://github.com/cocoindex-io/cocoindex/tree/main/examples)
