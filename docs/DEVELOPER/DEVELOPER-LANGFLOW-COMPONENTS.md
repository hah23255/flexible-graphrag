# Langflow Components

Flexible GraphRAG provides **12 custom Langflow components** under the **Flexible GraphRAG** node category. They are **thin nodes over the real backend machinery** — the `ingest/*` pipeline functions (`run_chunk_pipeline`, `update_vector` / `update_search` / `update_pg_graph` / `update_rdf_graph`), `retriever_setup`, and the full adapter/store layer (both LangChain and LlamaIndex). Nothing about the pipeline is reimplemented in the components; they marshal inputs and call the same code the FastAPI backend does.

For **setup and app-driven flow mode**, see [Langflow Integration](../GETTING-STARTED/LANGFLOW-INTEGRATION.md). This page is the developer reference for the components themselves.

Source: [`flexible-graphrag/langflow_components/flexible_graphrag/`](https://github.com/stevereiner/flexible-graphrag/tree/main/flexible-graphrag/langflow_components/flexible_graphrag).

---

## Architecture

- **Driven by `.env`.** Each component reads the backend `Settings()`. UI fields on a node override the corresponding setting as an environment variable before the system is built, so a flow can tweak per-node behavior without editing `.env`.
- **One shared system per run.** The system class is always `HybridSearchSystem` (the full LangChain + LlamaIndex adapter layer). The **Data Source** node builds it and stashes it in a process-level **run cache**; only a short **run-key string** travels in the `Data` payloads along the edges (the live system is not serializable). Downstream nodes look the system back up by run-key.
- **Ingestion threads the shared system along the edges; query nodes are independent** (Hybrid Search and AI Query each build their own system and reconnect to the persistent stores — they are not wired to the ingestion graph).
- **Imports.** Components import the shared helper by absolute path — `from langflow_components.flexible_graphrag._fg_shared import ...` — because Langflow's component loader cannot resolve a bare local import.

---

## The 12 components

| # | Node (display name) | `name` / type | Icon | In → Out | Role |
|---|---|---|---|---|---|
| 1 | **Flexible: Data Source** | `FlexibleDataSource` | FolderOpen | Trigger *(Message)* → Data | Entry point: builds the shared system (`.env` + UI overrides) and resolves a source to file paths. |
| 2 | **Flexible: Document Processor** | `FlexibleDocProcessor` | FileText | Source *(Data)* → Data | Parse source files into documents via the backend `DocumentProcessor` (Docling / LlamaParse per `.env`). |
| 3 | **Flexible: Text Splitter** | `FlexibleTextSplitter` | Scissors | Documents *(Data)* → Data | Chunk documents (size/overlap and chunker backend from `.env`). |
| 4 | **Flexible: Vector Store** | `FlexibleVectorStore` | Database | Chunks *(Data)* → Data | Index chunks into the vector DB (Qdrant / Neo4j / Elasticsearch / … per `.env`). |
| 5 | **Flexible: KG Extraction** | `FlexibleKGExtraction` | Sparkles | Chunks *(Data)* → Data | Extract entities/relations **once**; feeds Property Graph + RDF in parallel. |
| 6 | **Flexible: Property Graph** | `FlexibleGraphStore` | Workflow | KG *(Data)* → Data | Store the extracted KG into the property-graph DB (Neo4j / … per `.env`). |
| 7 | **Flexible: Search Store** | `FlexibleSearchStore` | Search | Chunks *(Data)* → Data | Build the fulltext/BM25 index (BM25 / Elasticsearch / OpenSearch per `.env`). |
| 8 | **Flexible: RDF Graph** | `FlexibleRDFStore` | Share2 | KG *(Data)* → Data | Write RDF triples (Fuseki / Oxigraph / GraphDB / Neptune per `.env`). |
| 9 | **Flexible: Ingestion Summary** | `FlexibleIngestSummary` | ListChecks | Vector/Search/PG/RDF *(Data)* → Message | Join the store nodes; print a one-shot ingestion summary (feeds ChatOutput for the Playground). |
| 10 | **Flexible: Hybrid Search** | `FlexibleHybridSearch` | Zap | Query *(Message)* → Data | Hybrid search over all configured modalities (vector, property graph, fulltext, RDF). Standalone. |
| 11 | **Flexible: AI Query** | `FlexibleAIQuery` | MessageSquare | Question *(Message)* → Data | Answer a question via RAG over the ingested stores. Independent of Hybrid Search. |
| 12 | **Flexible: Query Summary** | `FlexibleQuerySummary` | ListChecks | AI Answer + Search Results *(Data)* → Message | Combine the AI Query answer + Hybrid Search results into one chat message. |

---

## Ingestion flow topology

```text
[ChatInput]
   │ (input_value: {source_type, source_config, config_path, …})
   ▼
Data Source ──► Document Processor ──► Text Splitter ──┬──► Vector Store ─────────────┐
                                                       │                              │
                                                       ├──► Search Store ─────────────┤
                                                       │                              │
                                                       └──► KG Extraction ──┬──► Property Graph ─┤
                                                                            │                    │
                                                                            └──► RDF Graph ──────┤
                                                                                                 ▼
                                                                                        Ingestion Summary ──► [ChatOutput]
```

- **KG Extraction runs once** and fans out to both **Property Graph** and **RDF Graph**, so entities/relations aren't extracted twice.
- Any store node whose backend is disabled in `.env` is a no-op — the flow still runs end-to-end.
- The **Ingestion Summary** joins whichever store outputs are present (all four are optional inputs) and emits the human-readable summary.

## Query flow topology

**Hybrid Search** and **AI Query** are independent — each builds its own system and reconnects to the persistent stores. There are **three** query-side flows:

```text
Combined (Playground only — fg_query_flow.json):
  [ChatInput: question] ──┬──► Hybrid Search ──► results ─┐
                          │                                ├──► Query Summary ──► [ChatOutput]
                          └──► AI Query ──────► answer ────┘

App search (fg_search_flow.json):     [ChatInput] ──► Hybrid Search
App AI query (fg_aiquery_flow.json):  [ChatInput] ──► AI Query
```

The **app uses the dedicated single-branch flows** because **Langflow runs every branch present in a flow** — if the app ran the combined flow for a search it would also fire an AI Query LLM call (and vice-versa). The backend requests the target component's structured output via `output_component`, so `run_search_flow` / `run_aiquery_flow` return the same shapes as direct mode. The combined flow (with **Query Summary** merging both into one chat message) is kept for the Langflow **Playground**.

Each query node reuses a **warm cached system** (`run_with_query_system` in `_fg_shared.py`) instead of rebuilding it every request; the cache is invalidated on each ingest (`start_run`) so queries always reflect the latest data.

---

## Component loading & Langflow 1.10.1 constraints

Components are loaded by pointing Langflow at the package directory:

```ini
LANGFLOW_COMPONENTS_PATH=…/flexible-graphrag/langflow_components
```

Langflow's loader (`lfx.custom.validate.prepare_global_scope`) only executes **top-level** `import` / `class` / `def` AST nodes in a component file. Three patterns are required for the components to register on Langflow 1.10.1:

1. **Secret fields** use `SecretStrInput(...)`, not `MessageTextInput(..., password=True)` — API keys must be secret inputs.
2. **Imports must be flat and top-level.** `try/except` import blocks are silently dropped (they caused `NameError: Component is not defined`). Langflow auto-falls-back `langflow.* → lfx.*`.
3. **Input ports** are declared with `HandleInput(..., input_types=["Data"])` — declaring an input as `Output(...)` inside `inputs=[...]` does not work.

Other loader details:

- **Package layout drives discovery.** Each component is its own file, and an `__init__.py` must exist at **every level** (`langflow_components/` and `langflow_components/flexible_graphrag/`) — a missing `__init__.py` makes components **silently not appear**. The subfolder name (`flexible_graphrag`) becomes the sidebar **category**.
- **Icons are [Lucide](https://lucide.dev/) icon names** (`icon = "FileText"`), and an optional `priority: int` orders the components within the palette (lower first).

### Fast iteration without a restart

To debug a single component without the regen + Langflow-restart cycle, use Langflow's **"New Custom Component"** button at the bottom of the left sidebar and **paste the component's Python** directly — it registers immediately. (Use `LANGFLOW_COMPONENTS_PATH` for the permanent, whole-package install.)

### Mixing with stock Langflow components

The `Flexible: *` nodes speak **LlamaIndex** `Data`/documents; stock Langflow nodes are **LangChain**-native. If you wire a Flexible component directly to a built-in Langflow node, the document shape differs — LlamaIndex `Document(text=…, metadata=…)` vs LangChain `Document(page_content=…, metadata=…)`. Convert at the boundary (`page_content` ↔ `text`, pass metadata through). The backend already provides these converters (`langchain/utils.py`) for the ingest pipeline.

---

## Regenerating the bundled flow JSONs

Regeneration is only needed when you **change internal flexible-graphrag code** — the flow JSONs *embed* the component code, so an edit to a component (or the pipeline it calls) isn't picked up in flow mode until the flows are regenerated. Normal use never needs it: the app drives the existing flows over the Langflow **REST API**, passing per-run config as the flow's `input_value`.

The **four** flows in `flows/` are generated from the **real registered component templates** (via `lfx.custom.utils.build_custom_component_template`), then assembled into nodes + edges:

- `fg_ingestion_flow.json` — the full ingest pipeline.
- `fg_search_flow.json` — ChatInput → Hybrid Search (the app's search).
- `fg_aiquery_flow.json` — ChatInput → AI Query (the app's AI query).
- `fg_query_flow.json` — combined Hybrid Search + AI Query → Query Summary (Langflow **Playground** only; the app doesn't run it).

The dedicated search / AI-query flows exist because **Langflow runs every branch present in a flow** — running the combined flow for a search would also fire an AI Query LLM call. `run_search_flow` / `run_aiquery_flow` target the dedicated flows (falling back to the combined one if a file is missing).

Regenerate all four from the live components:

```powershell
python flexible-graphrag/langflow_components/generate_flows.py   # run in the Langflow venv
```

It validates each with `lfx.graph.graph.base.Graph.from_payload(data).prepare()` before writing. In app-driven flow mode the backend **deletes and re-uploads** the flow matching the JSON's `name` on every startup (see [Langflow Integration](../GETTING-STARTED/LANGFLOW-INTEGRATION.md#customizing-the-flows)), so regenerate rather than hand-editing the running flow in the UI. **Re-run the generator whenever you edit a component** (its code is embedded in the flow JSON), then restart the backend.

---

## File map

| Path | Contents |
|---|---|
| `langflow_components/flexible_graphrag/*.py` | The 12 component classes (one per file) + `__init__.py` exports. |
| `langflow_components/flexible_graphrag/_fg_shared.py` | Run cache, system builder (`build_system`), the warm query-system cache (`get_query_system` / `run_with_query_system`, cleared on ingest by `start_run`), `has_ingest_chunks` / `ingest_chunk_count` helpers. |
| `langflow_components/generate_flows.py` | Regenerates all four flow JSONs from the live component classes (run in the Langflow venv). |
| `flows/fg_ingestion_flow.json`, `fg_search_flow.json`, `fg_aiquery_flow.json`, `fg_query_flow.json` | The bundled flows the app uploads (ingest, search, AI query, + combined Playground query). |
| `flow_service.py` (backend) | `FlowService` — drives the flows over the Langflow REST API in app-driven mode (uses the dedicated search / AI-query flows). |
