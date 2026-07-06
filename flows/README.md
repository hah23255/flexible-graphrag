# Flexible GraphRAG — Langflow Flows

Bundled Langflow flows the app uploads in **app-driven flow mode** (`ENABLE_LANGFLOW_FLOWS=true`):

- `fg_ingestion_flow.json` — ingestion pipeline.
- `fg_search_flow.json` — app search (Hybrid Search only).
- `fg_aiquery_flow.json` — app AI query (AI Query only).
- `fg_query_flow.json` — combined Hybrid Search + AI Query → Query Summary (Langflow Playground only).

Documentation:

- **Setup, flow mode, configuration, and customizing the flows** → [docs/GETTING-STARTED/LANGFLOW-INTEGRATION.md](../docs/GETTING-STARTED/LANGFLOW-INTEGRATION.md)
- **The 12 components, flow topology, and regenerating these JSONs** → [docs/DEVELOPER/DEVELOPER-LANGFLOW-COMPONENTS.md](../docs/DEVELOPER/DEVELOPER-LANGFLOW-COMPONENTS.md)
