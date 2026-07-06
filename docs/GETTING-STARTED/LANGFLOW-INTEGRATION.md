# Langflow Integration

Flexible GraphRAG ships **12 custom [Langflow](https://www.langflow.org/) components** (the **Flexible GraphRAG** node category) and four ready-made flows. There are two ways to use them, and they can be used together:

1. **App-driven flow mode** *(optional)* — the Flexible GraphRAG app runs its **ingest pipeline, hybrid search, and AI query through the Langflow visual flows** (via the Langflow REST API) instead of calling the system directly. The flows execute the **same backend machinery** driven by the same `.env`, so you get identical results — but the pipeline becomes a visual flow you can **customize**. Enabled with `ENABLE_LANGFLOW_FLOWS=true` (off by default).
2. **Visual flow building** — drag the Flexible components onto the Langflow canvas to build or customize your own ingest/query flows. See the developer reference: [Langflow Components](../DEVELOPER/DEVELOPER-LANGFLOW-COMPONENTS.md).

!!! info "All your existing configuration is reused"
    The components are thin nodes over the real backend (`ingest/*` pipeline + retriever + adapter/store layer, both LlamaIndex and LangChain). **Every database, LLM/embedding, chunking, KG-extraction, RDF, and framework (LI/LC per-stage) setting in your backend `.env` applies unchanged.** You do not reconfigure anything for Langflow — the flow just orchestrates the same pipeline.

---

## How it works (app-driven flow mode)

When `ENABLE_LANGFLOW_FLOWS=true`, on startup the backend:

1. **Uploads the flow JSONs** to Langflow (replacing any existing flow of the same name, so a restart always runs the current file). Four flows are used:
   - `flows/fg_ingestion_flow.json` — the ingest pipeline.
   - `flows/fg_search_flow.json` — **search** (ChatInput → Hybrid Search only).
   - `flows/fg_aiquery_flow.json` — **AI query** (ChatInput → AI Query only).
   - `flows/fg_query_flow.json` — combined Hybrid Search + AI Query → Query Summary, for the Langflow **Playground** (the app doesn't run it).
2. Routes each **ingest / hybrid-search / AI-query** request to `POST /api/v1/run/{flow}` on the Langflow server, passing the per-run config as the flow's `input_value` (the ChatInput → Data Source / query edge).
3. Reads the flow's component outputs back and returns them to the UI/API — so flow mode renders identically to direct mode.

!!! note "Why separate search and AI-query flows"
    Langflow runs **every branch present in a flow**. If the app ran the combined query flow for a search, it would also fire an AI Query LLM call (and vice-versa). The dedicated single-branch `fg_search_flow` / `fg_aiquery_flow` avoid that wasted work.

The backend and Langflow communicate **only over HTTP** (`LANGFLOW_URL`), which is why they run as two separate processes (see below).

---

## Setup: two venvs, two terminals

Langflow executes the flow's component code **in its own process**, so it needs Flexible GraphRAG installed in its environment. The app runs in its own environment and just talks to Langflow over REST. Use **two virtual environments in two terminal windows**:

| | Langflow venv (Terminal 1) | Backend venv (Terminal 2) |
|---|---|---|
| Runs | `langflow run` (the flow server) | the Flexible GraphRAG app (`uvicorn`/`python main.py`) |
| Needs | Langflow **+** an editable install of `flexible-graphrag` (so it can import the components + pipeline) | the normal backend install (Langflow **not** required) |
| Reads `.env`? | uses `.env` when it executes the pipeline | yes — sets `ENABLE_LANGFLOW_FLOWS`, `LANGFLOW_URL` |

!!! warning "Python build matters for Langflow"
    Create the Langflow venv on **Python 3.14.5 or newer** (or 3.13) — use an explicit patch version: `uv venv --python 3.14.5` (or greater). Plain `uv venv --python 3.14` resolves to **3.14.0**, whose OpenSSL build aborts on SSL and makes Langflow crash on startup. Litmus test for any venv: `python -c "import ssl; ssl.create_default_context(); print('OK')"`.

### Terminal 1 — Langflow venv

```powershell
# Create the venv on Python 3.14.5 or newer (NOT plain --python 3.14, which resolves to 3.14.0)
uv venv --python 3.14.5 venv-langflow

# Two-step install (the combined flexible-graphrag[langflow] extra is unsatisfiable).
# --native-tls is needed behind an SSL-inspecting corporate proxy.
uv pip install --native-tls langflow==1.10.1
uv pip install --native-tls --override extras-overrides.txt -e ".[langchain,langchain-extras]"   # LlamaIndex + fuller LangChain backends (e.g. Neo4j)

# Point Langflow at the custom components, then run it FROM the flexible-graphrag backend dir
$env:LANGFLOW_COMPONENTS_PATH = "C:/newdev3/flexible-graphrag-flow/flexible-graphrag/langflow_components"
langflow run --port 7860 --log-level WARNING --log-file langflow.log
```

Wait for Langflow to **fully start** — after the purple "Welcome to Langflow" box it prints `Launching Langflow...`; once that finishes, the **Flexible GraphRAG** category with the 12 `Flexible: *` nodes appears in the sidebar. Open <http://localhost:7860> to confirm.

!!! note "cmd.exe instead of PowerShell"
    In Command Prompt, set the components path **without quotes** (cmd stores the quotes as part of the value): `set LANGFLOW_COMPONENTS_PATH=C:\newdev3\flexible-graphrag-flow\flexible-graphrag\langflow_components`. Either `/` or `\` works.

### Terminal 2 — backend venv

Enable flow mode in the backend `.env`:

```ini
ENABLE_LANGFLOW_FLOWS=true
LANGFLOW_URL=http://localhost:7860
# LANGFLOW_API_KEY=            # only if your Langflow has auth enabled (see below)
# INGEST_FLOW_PATH=flows/fg_ingestion_flow.json   # defaults shown; override to customize
# QUERY_FLOW_PATH=flows/fg_query_flow.json
```

Then start the backend as usual. Ingest, hybrid search, and AI query now run through the Langflow flows.

---

## Configuration reference

| Variable | Default | Purpose |
|---|---|---|
| `ENABLE_LANGFLOW_FLOWS` | `false` | Run ingest/search/AI-query through Langflow instead of calling the system directly. |
| `LANGFLOW_URL` | `http://localhost:7860` | URL of the running Langflow server. |
| `LANGFLOW_API_KEY` | *(unset)* | Only needed if Langflow has authentication enabled. Not required for a default local single-user Langflow (`AUTO_LOGIN` on = no auth). Create one in the Langflow UI under **Settings → Langflow API Keys → Add New**, or via the `langflow api-key` CLI. |
| `INGEST_FLOW_PATH` | `flows/fg_ingestion_flow.json` | Path to the ingestion flow JSON. |
| `SEARCH_FLOW_PATH` | `flows/fg_search_flow.json` | Dedicated search-only flow the app runs (no AI Query branch). |
| `AIQUERY_FLOW_PATH` | `flows/fg_aiquery_flow.json` | Dedicated AI-query-only flow the app runs (no Hybrid Search branch). |
| `QUERY_FLOW_PATH` | `flows/fg_query_flow.json` | Combined Hybrid Search + AI Query flow — Langflow **Playground** only (the app doesn't run it). |

All other backend settings (data sources, vector/search/graph/RDF DBs, LLM & embeddings, chunking, KG extraction, LI/LC framework pickers) are read from the same `.env` and apply unchanged.

---

## Customizing the flows

The bundled flows cover the full pipeline, but you can edit them visually:

1. In the Langflow UI, open the flow you want to change (Ingestion / Search / AI Query / combined Query), **duplicate** it, and adjust the canvas.
2. **Export** your edited flow to a JSON file.
3. Point the matching `*_FLOW_PATH` (`INGEST_FLOW_PATH` / `SEARCH_FLOW_PATH` / `AIQUERY_FLOW_PATH` / `QUERY_FLOW_PATH`) at your exported copy.

Regenerating the bundled flows from the components is a developer task — see [Langflow Components → Regenerating the bundled flow JSONs](../DEVELOPER/DEVELOPER-LANGFLOW-COMPONENTS.md#regenerating-the-bundled-flow-jsons).

!!! warning "The app replaces flows by name on startup"
    In app-driven mode the backend deletes and re-uploads the flow that matches the configured JSON's `name` every time it starts — so hand-edits made *in the Langflow UI* to the bundled flow are overwritten. Persist changes by **exporting to your own JSON** and pointing the `*_FLOW_PATH` at it (give it a distinct `name`), or by regenerating the bundled JSON.

If you change **backend code that a component imports at runtime** (e.g. `_fg_shared.py`), **restart the Langflow server** — it imported the old module.

### Tips for building flows in the UI

- **Connect nodes** by dragging from an **output handle** (right-side circle) to an **input handle** (left-side circle). A line appears when connected.
- **Use forward slashes in file paths** on Windows inside node fields (`C:/data/file.pdf`) — backslashes can fail in the flow JSON.
- **Red icon on a node = error.** Click the node to inspect its config; open the browser console (F12) for details. If two nodes won't connect, it's usually a port **type mismatch** — delete the edge and reconnect (or refresh the page).

---

## Troubleshooting

- **`Flexible: *` nodes don't appear** — `LANGFLOW_COMPONENTS_PATH` must point at the **parent `langflow_components` folder, not the `flexible_graphrag/` subfolder** (the subfolder name becomes the sidebar category). Confirm `flexible-graphrag` is installed editable (`-e`) in the **same** venv as Langflow, then check the Langflow logs for import errors and restart. Quick checklist: `echo $env:LANGFLOW_COMPONENTS_PATH` → verify the path → confirm the components import in that venv.
- **SSL abort on Langflow start** — the interpreter's OpenSSL is 3.5.x (uv-standalone). Recreate the venv on a python.org / pythoncore 3.14.5 (or 3.13) interpreter. See the warning above.
- **401 from Langflow in flow mode** — set `LANGFLOW_API_KEY` (x-api-key) if your Langflow requires auth; leave it unset for a default local single-user Langflow.
- **Nothing ingests / long hang** — ingestion runs synchronously over one HTTP call and can take minutes (KG extraction is LLM-bound). This is expected; the connect timeout is short but the read timeout is unlimited. If you drive the API directly (or via a node with its own timeout), raise the request timeout to 300–600s.

---

## See also

- [Langflow Components (developer reference)](../DEVELOPER/DEVELOPER-LANGFLOW-COMPONENTS.md) — the 12 components, flow topology, and how to add/edit them.
- [Environment Configuration](ENVIRONMENT-CONFIGURATION.md) — the full backend `.env` reference.
- [Framework Configuration](../CONFIGURATION/LANGCHAIN-CONFIGURATION.md) — LlamaIndex / LangChain per-stage pickers.
