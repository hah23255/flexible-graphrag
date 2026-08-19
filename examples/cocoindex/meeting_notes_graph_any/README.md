# Meeting Notes Knowledge Graph — ported to Flexible GraphRAG's CocoIndex integration

This is a port of the CocoIndex [`meeting_notes_graph_neo4j`](https://github.com/cocoindex-io/cocoindex/tree/main/examples/meeting_notes_graph_neo4j)
example to **Flexible GraphRAG's CocoIndex integration** — still a CocoIndex
app, with the same change detection, memoisation and live watching, but reaching
its sources and stores through Flexible GraphRAG. So it now runs against **any**
of Flexible GraphRAG's 15 supported property graph databases and **any** of the
10 auto-sync capable data sources — hence `meeting_notes_graph_any`. The store,
the source, and how each is read and written are all config values rather than
code:

* `PG_GRAPH_DB` — which of the 15 graph stores
* `NOTES_SOURCE` — which of the 10 sources (filesystem, S3, GCS, Azure Blob,
  Google Drive, OneDrive, SharePoint, Box, Alfresco, Nuxeo)
* `GRAPH_BACKEND` — how it is written: `llamaindex` or `langchain` for Flexible
  GraphRAG's adapter layer, covering all 15 stores, or `cocoindex` for
  CocoIndex's own native connectors (Neo4j, FalkorDB, SurrealDB — 3 of the 15)
* `SOURCE_BACKEND` — how it is read: `flexible` for all 10 sources, or
  `cocoindex` for CocoIndex's native source connectors (filesystem, S3, Azure
  Blob, Google Drive — 4 of the 10; of those only filesystem streams changes,
  the rest are polled)

## Before you start

This example runs *against* Flexible GraphRAG, so install and configure it
first — see [Prerequisites](../../../README.md#prerequisites),
[Setup](../../../README.md#setup) and
[Python Backend Setup](../../../README.md#python-backend-setup-standalone) in
the main README. You need:

* the Python environment installed, with the CocoIndex extra:
  `uv pip install "flexible-graphrag[cocoindex]"`
* a working `.env` in `flexible-graphrag/` — at minimum an LLM
  (`LLM_PROVIDER` + its API key) and a property graph store (`PG_GRAPH_DB`)
* that store actually running (see [docker/](../../../docker/))

Optional but used by the defaults here:
`uv pip install "cocoindex[entity_resolution]"` for `ENTITY_RESOLUTION=llm` —
without it the run degrades to `normalize` with a warning rather than failing.

Everything below runs from this directory.

## Differences

CocoIndex has two meeting-notes examples — one for Neo4j, one for
FalkorDB — and the two files differ by about 59 lines, all of it graph-database
plumbing. It is also Google Drive only.

| |CocoIndex| This port |
|---|---|---|
| Source | Google Drive only | any of 10 (`NOTES_SOURCE`) |
| Graph store | one file per store | any of 15 (`PG_GRAPH_DB`) configured |
| Read / write path | CocoIndex's own connectors | `SOURCE_BACKEND` / `GRAPH_BACKEND`: Flexible GraphRAG's adapter layer, or CocoIndex's native connectors (4 of the sources, 3 of the stores) |
| Extraction schema | fixed pydantic models | same models, unchanged |
| LLM | LiteLLM | project's configured `LLM_PROVIDER` |
| Entity resolution | LLM pair resolver | same machinery, configured LLM |

## The graph

Same shape as the original CocoIndex example:

```
(Person)-[:ATTENDED {is_organizer}]->(Meeting)
(Meeting)-[:DECIDED]->(Task)
(Task)-[:ASSIGNED_TO]->(Person)
```

## Files

| | |
|---|---|
| `extractor.py` | the extraction as a registered `KGExtractor` — the domain logic, as a plugin |
| `mini_app.py` | a short CocoIndex app — source, custom extraction, property graph |
| `pipeline_app.py` | the **standard** flexible-graphrag pipeline with this extractor plugged in — config only, no pipeline copy |
| `run_backend.py` | starts the app server configured for this example, so the UI can drive it |
| `example_config.py` | the settings both runners share — one definition, so they cannot drift |
| `meeting_notes.py` | shared library: extraction schema, prompt, section splitter, `.env` loading |
| `meeting_notes_ontology.ttl` | the graph described in RDF — read by the RDF graph retriever's LangChain GraphDB adapter. Under `USE_ONTOLOGY=true` it would also reach the built-in extractor, shaping the *delegated* non-note content rather than the meetings |

### Three ways to run the same extractor

```bash
cocoindex update mini_app.py       # short pipeline
cocoindex update pipeline_app.py   # the full pipeline
uv run run_backend.py              # the full pipeline, as a server for the UI
```

All three use the same `MeetingNotesExtractor`, chunk one meeting at a time, and
produce the same graph — 31 triples, 4 `Meeting`, 4 `Person`, 11 `Task` with the
example file. What differs is how much runs around that.

| | `mini_app.py` | `pipeline_app.py` | `run_backend.py` |
|---|---|---|---|
| source | any of 10 | any of 10 | any of 10, choosable in the UI |
| document conversion | none | PDF, DOCX, PPTX … | PDF, DOCX, PPTX … |
| chunking | per meeting section — splits directly | per meeting section — configured splitter does it | same |
| KG extraction | `MeetingNotesExtractor` | `MeetingNotesExtractor` | `MeetingNotesExtractor` |
| property graph | yes | yes | yes |
| vector, search, RDF | no | **when configured** | **when configured** |
| driven by | the CocoIndex CLI | the CocoIndex CLI | the app UI |

`mini_app.py` is deliberately short: source → extract → property graph. The full
pipeline also writes whichever of `VECTOR_DB` / `SEARCH_DB` / `RDF_GRAPH_DB` you
have configured — anything set to `none` is skipped — and those are what make
the notes searchable (hybrid search, ai-query, ai-chat) rather than only
traversable as a graph.

`run_backend.py` runs that same full pipeline, just as a server rather than a
one-shot command, so the UI can upload, search and ask against it.

`pipeline_app.py` contains no copy of the Flexible GraphRAG CocoIndex pipeline
app — it sets the settings the meeting-notes format needs and then imports it, so
the equivalent is a pure environment delta:

| setting | why |
|---|---|
| `DOCUMENT_PARSER=liteparse` | conversion with docling strips markdown `#` markers, so heading splitting finds nothing and the whole file becomes one meeting. liteparse passes `.md`/`.txt` through unchanged. |
| `CHUNKER_BACKEND=cocoindex` + `COCOINDEX_SPLITTER_TYPE=separator` + `COCOINDEX_SEPARATORS=\n{2,}#{1,2}\s+` | chunk per meeting, so the memo is per meeting |
| `CHUNK_SIZE=600` | the separator splitter emits one fragment per section then **packs** them up to `CHUNK_SIZE`; at 2048 all four meetings pack back into one chunk |
| `ENTITY_RESOLUTION=llm` | extraction is per chunk, so `Bob` and `Bob Smith` are only comparable afterwards |
| `KG_EXTRACTOR_BACKEND=./extractor.py:MeetingNotesExtractor` | the extractor |
| `ONTOLOGY_PATHS=./meeting_notes_ontology.ttl` | the RDF graph retriever needs a local ontology to build its schema; without one it logs `RDF graph retriever not available` and GraphDB drops out of the fusion set. `USE_ONTOLOGY` stays `false`, so this does not turn on ontology-guided extraction. |

Precedence in `pipeline_app.py` is shell variable > the file > `.env`. That
ordering matters: `.env` legitimately sets `DOCUMENT_PARSER=docling` and
`CHUNKER_BACKEND=llamaindex`, and those have to lose to the file or the example
silently reverts to whole-file chunking.

### When not every document is a meeting note

`KG_EXTRACTOR_BACKEND` selects one extractor for the whole run, but a real
source is rarely all meeting notes. So each meeting section declares itself:

```markdown
## Ingestion Rewrite Design Review

Type: MeetingNote
Date: 2026-07-13
```

Anything **without** that tag is handed to the built-in extractor via
`ctx.builtin()` rather than being dropped or force-fitted. Drop a
`vendor-invoice.md` next to the notes and it comes out as an ordinary graph —
`Globex Ltd` as `ORGANIZATION`, `Linda Torres` as `PERSON` — with no `Meeting`
node invented for it.

That matters because `ExtractedMeeting` *requires* a date, a note and an
organiser: handed an invoice, the LLM would dutifully fabricate a meeting rather
than decline. The tag is what stops it.

The tag is per section rather than one marker per file because extraction is per
**chunk** — a file-level tag would be visible only in the first chunk and every
later chunk would look untagged. The `Type:` value is parsed rather than matched
against one literal, so the same convention extends to other document types
later.

### The extractor is a plugin

`extractor.py` subclasses `KGExtractor` and registers itself. Nothing imports it
directly — all three runners name it by spec and it is resolved through the
registry, so the same class serves all three unchanged:

```bash
KG_EXTRACTOR_BACKEND=./extractor.py:MeetingNotesExtractor
```

`mini_app.py` hardcodes that spec instead of reading the environment, but it goes
through the same dispatcher.

Custom extractors are honoured by the **CocoIndex pipeline** only
(`PIPELINE_BACKEND=cocoindex`); the default pipeline uses the two built-ins.

It produces generic `KGResult` triples, so the graph is written by whichever
target `PG_GRAPH_DB` / `GRAPH_BACKEND` select — the example contains no
store-specific write code at all.

Two things follow from that:

**Extraction is memoised on `(chunk_text, spec, version)`.** Edit `extractor.py`
and bump its `version`, or you keep reading the old triples.

**The extractor gets no document provenance** — no `file_name`, no `doc_id`, so
an id cannot drift when the same note arrives from a different source. Meeting
ids are content-derived (`date#title`), and provenance is attached a layer out:
on every row, and as readable properties on `Meeting` nodes. Only
document-scoped types get those (`PROVENANCE_TYPES`) — a `Person` appears across
many notes, so stamping one filename on them would be a lie.

A `Meeting` node ends up with:

| | |
|---|---|
| `title` | the heading, **verbatim** — not round-tripped through the LLM |
| `text` | the section body, **verbatim** — nothing lost to summarisation |
| `time` | meeting date, LLM-extracted and normalised to ISO |
| `note` | the LLM's summary — reworded by design; `text` is the original |
| `note_file`, `note_path`, `source_type`, `note_modified_at` | where it came from |

## Running it

Needs a configured LLM (`LLM_PROVIDER` in the project `.env`) and a running
target store.

```bash
cocoindex update mini_app.py         # one pass over ./sample_notes
cocoindex update -L mini_app.py      # …and keep watching

cocoindex update pipeline_app.py     # the full pipeline; -L works here too

uv run run_backend.py --notes        # the server — always watching, no -L
```

`-L` is a CocoIndex CLI flag, so it applies to the two `cocoindex update`
commands. `run_backend.py` is a server: it watches continuously either way.

While watching, edit `sample_notes/team-meetings.md` and the change flows
through: only the edited section is re-extracted (the rest are memo hits that
never reach the LLM), and only the changed file's worker re-runs.

The source is a config value too — `FlexibleMapView` is a CocoIndex
`LiveMapView` over any of the 10 detector-backed sources, so this file contains
no source-handling code at all:

```bash
NOTES_SOURCE=google_drive cocoindex update -L mini_app.py
NOTES_PATH=/some/other/folder cocoindex update -L mini_app.py

# Windows: set NOTES_SOURCE=google_drive          (then cocoindex update -L mini_app.py)
#          set NOTES_PATH=C:\some\other\folder
```

All 10 stream changes, so live watching works whichever you pick: filesystem,
s3, gcs, azure_blob, google_drive, onedrive, sharepoint, box, alfresco, nuxeo.

| env var | |
|---|---|
| `NOTES_SOURCE` | source to watch (default `filesystem` → `sample_notes/`) |
| `NOTES_PATH` | folder, when the source is `filesystem` |
| `NOTES_RESOLVE` | entity resolution: `llm` (default), `normalize`, `none` |
| `NOTES_VERBOSE` | `1` to keep the backend's own INFO logging |

By default the console shows only this app's progress. Importing the backend
pulls in a few dozen modules that log at INFO — adapter construction, embedding
factories, source detectors — none of it about your notes, so it is silenced
once the noisy imports are done.

### Clearing state

The two `cocoindex update` runners keep their memo in a `cocoindex.db/`
**directory** here — gitignored, so it will not show up in a git client.
`run_backend.py` does not create one: the server uses the backend's own
`flexible-graphrag/cocoindex.db`.

That split matters when clearing: `scripts/cleanup.py` reaches the backend's memo
(so the server's), but not this directory. Delete `cocoindex.db/` here by hand.

## Using it from the app UI

The same extractor works when the **server** runs rather than the CLI, which is
what makes the notes searchable from the UI — upload, hybrid search, ai-query
and chat all read the stores this pipeline fills.

**1. Start the server** configured for this example:

```bash
uv run run_backend.py            # start it
uv run run_backend.py --notes    # …and seed the watch dir with sample_notes/

python run_backend.py            # same, if your venv is already active
```

Anything you set in your shell still wins, so a one-off override works:

```bash
PG_GRAPH_DB=arcadedb uv run run_backend.py

# Windows: set PG_GRAPH_DB=arcadedb   (then uv run run_backend.py)
```

**2. Start a frontend** in another terminal — see
[Frontend Setup (Standalone)](../../../README.md#frontend-setup-standalone) in
the main README for `npm install` and the per-framework commands:

| | | |
|---|---|---|
| React | `npm run dev` | http://localhost:5174 |
| Angular | `npm start` | http://localhost:4200 |
| Vue | `npm run dev` | http://localhost:3000 |

(`run_backend.py` serves the API on http://localhost:8000 — open the frontend
URL, not that one.)

Try asking **"what meeting did Bob lead?"** — the answer comes back as
*Ingestion Rewrite Design Review, 2026-07-13*, which exercises the verbatim
`title` property, the `is_organizer` edge and `Bob` → `Bob Smith` resolution all
at once.

### Getting notes in

Two ways, both watched live:

* **Upload in the UI** — `/api/ingest` copies from `./uploads/` into `WATCH_DIR`,
  where the localfs connector picks it up.
* **Drop a `.md` file into the watch directory** — the live stream notices within
  seconds.

The watch directory is `./watch/` beside the example, created on demand.

> **Why its own directory, set per process.** `scripts/cleanup.py` deletes every
> file in whatever `WATCH_DIR` points at. Keeping it separate keeps the example
> clear of the app's normal corpus and keeps `cleanup.py` away from
> `sample_notes/` — which is exactly why `run_backend.py` sets it for its own
> process and the path never goes in `.env`.

### Several sources at once

Each configured source becomes its own CocoIndex app, so they run side by side.
After one UI upload the server reports two:

```json
"app_names": ["GraphRAG_filesystem", "GraphRAG_upload_37b68883-…"], "num_apps": 2
```

* `GraphRAG_filesystem` — the default source, watching `WATCH_DIR`
* `GraphRAG_upload_…` — created by the UI file upload

Add more from the UI's data source configuration and they join the same run.
Picking **Google Drive** there puts this example on the source the original
CocoIndex example uses, with the filesystem source still running alongside it.

`KG_EXTRACTOR_BACKEND` is global, so every source uses this extractor — fine
here, because anything untagged is delegated to the built-in one.

### If search returns nothing after cleaning the databases

CocoIndex still has the documents memoised, so re-ingesting is a cache hit that
never refills the emptied stores — ingest reports success and search stays empty.
Clear the memos too, then start fresh. `cleanup.py` empties the stores and the
server's own memo; the two directories it does not reach are this example's CLI
memo (see [Clearing state](#clearing-state)) and the watch directory, which may
still hold earlier uploads — `--notes` reseeds it.

```bash
cd ../../../flexible-graphrag
python ../scripts/cleanup.py

cd ../examples/cocoindex/meeting_notes_graph_any
rm -rf cocoindex.db watch          # Windows: rmdir /s /q cocoindex.db  &  rmdir /s /q watch
uv run run_backend.py --notes
```

Everything is reprocessed on startup, with nothing to trigger by hand.

### Seeing the graph

```cypher
MATCH (n:Meeting)--(m) RETURN n, m
MATCH (n) RETURN n
```

## The sample notes

`sample_notes/team-meetings.md` is written to exercise three specific things:

| In the notes | What it tests |
|---|---|
| `Zoë Café-Lange` | id sanitisation (rewritten on FalkorDB/Cosmos, kept on Neo4j) |
| `bob smith` vs `Bob Smith` | typographic resolution — though in practice the LLM usually fixes this during extraction |
| `Bob` vs `Bob Smith` | **semantic** resolution: extraction runs per section, so only a corpus-level pass can know these are one person |

A useful thing to try: add a bullet like `- Budget Cuts` and nothing happens —
it is a bare noun phrase, so the extractor correctly does not treat it as a
task. `- Marcus Webb to model the budget cuts for Q4.` has a verb and an owner,
and produces a `Task` node with an `ASSIGNED_TO` edge.

## Entity resolution

The original CocoIndex example uses `resolve_entities` with an `LlmPairResolver`.
This uses the same machinery through
`cocoindex_integration/entity_resolution.py`, which plugs the project's
configured LLM and embedding model into CocoIndex's `PairResolver` and `Embedder`
protocols.

| strategy | merges |
|---|---|
| `normalize` | accents, case, punctuation — `bob smith` → `Bob Smith` |
| `llm` | also `Bob` → `Bob Smith`, `Acme Corp` → `Acme Corporation` |

The shipped default is `none`. **This example** overrides it to `llm` in
`example_config.py`, since that is the strategy that does the interesting merge.

```bash
uv pip install "cocoindex[entity_resolution]"   # core only — pulls faiss-cpu
```

That is the **core** extra, not
[`entity_resolution_llm`](https://cocoindex.io/docs/ops/entity_resolution/): the
LLM comparison here is Flexible GraphRAG's own `LLMPairResolver`, wired to
whatever `LLM_PROVIDER` you have configured, so CocoIndex's built-in LLM
resolver is not used. Without the extra, `llm` falls back to `normalize` with a
warning rather than failing.

Two things to know:

* **Bare first names merge only when unambiguous.** `Priya` merges into
  `Priya Raman`, but if the corpus also held `Priya Patel` it would merge into
  neither — fusing two real people is worse than leaving one unmerged.
* **Resolution rewrites entity ids.** Enabling it against a corpus already
  ingested without it leaves old and new nodes disagreeing. Turn it on from the
  start, or re-ingest clean.

## Known limits

**Chunking is by heading, not by size.** Keeps the extraction unit comparable to
the original CocoIndex example, and keeps the memo per *meeting*: edit one
meeting and only that section reaches the LLM again. A converted PDF or DOCX
arrives with the `#` markers gone, so section detection falls back to one meeting
per chunk.

**Resolution is per document.** `Bob` merges into `Bob Smith` only when both
spellings appear in the *same* note — neither runner sees the whole corpus.

**Tasks are keyed by their description.** The same task worded slightly
differently in two meetings becomes two nodes, and the LLM does not punctuate
identically between runs. `normalize` merges those within a run but cannot merge
against what an earlier run already wrote. The original example has the same
property.

**Harmless noise on exit.** The neo4j driver's destructor runs after interpreter
shutdown (`sys.meta_path is None`) and aiohttp reports an unclosed session. Exit
code is 0.
