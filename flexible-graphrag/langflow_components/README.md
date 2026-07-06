# Flexible GraphRAG — Langflow Components

The Langflow integration documentation lives in the main documentation site (it was previously duplicated across `README.md` / `QUICKSTART.md` / `build_flow_manually.md` here and had gone stale — those are now consolidated into this one pointer):

- **Setup, quickstart, app-driven flow mode, and configuration** → [docs/GETTING-STARTED/LANGFLOW-INTEGRATION.md](../../docs/GETTING-STARTED/LANGFLOW-INTEGRATION.md)
- **The 12 components, flow topology, building/customizing flows, and how to add/edit components** → [docs/DEVELOPER/DEVELOPER-LANGFLOW-COMPONENTS.md](../../docs/DEVELOPER/DEVELOPER-LANGFLOW-COMPONENTS.md)

The component source lives in [`flexible_graphrag/`](flexible_graphrag/) — one file per component, plus the shared run-cache helper `flexible_graphrag/_fg_shared.py`. The bundled flows the app uploads are in [`../../flows/`](../../flows/).
