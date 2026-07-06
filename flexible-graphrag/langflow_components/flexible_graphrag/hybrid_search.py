"""Flexible GraphRAG Hybrid Search Component for Langflow.

Independent (not wired to AI Query). Builds its own system from .env and runs the real
hybrid search across all configured modalities (vector + property graph + fulltext + RDF)
via system.search(), which lazily reconnects to the persistent stores.
"""

from langflow_components.flexible_graphrag._fg_shared import run_with_query_system

from langflow.custom import Component
from langflow.io import MessageTextInput, StrInput, IntInput, Output
from langflow.schema import Data


class FlexibleHybridSearchComponent(Component):
    """Hybrid retrieval across vector + property graph + fulltext + RDF (per .env)."""

    display_name = "Flexible: Hybrid Search"
    description = "Hybrid search over all configured modalities (vector, property graph, fulltext, RDF) — standalone"
    icon = "Zap"
    name = "FlexibleHybridSearch"

    inputs = [
        MessageTextInput(name="query", display_name="Query", info="Search query"),
        IntInput(name="top_k", display_name="Top K", value=10, info="Number of results"),
        StrInput(name="config_path", display_name="Config (.env) path", value="",
                 advanced=True, info="Blank = backend default .env (StrInput so API tweaks apply)"),
    ]

    outputs = [
        Output(display_name="Results", name="results", method="run_search"),
    ]

    async def run_search(self) -> Data:
        query = (self.query or "").strip()
        if not query:
            raise ValueError("Query is required")

        # Reuse a warm cached system (built once per .env, like the backend) instead of
        # rebuilding it every search — build_system does full LLM/embedding/store/retriever
        # setup and was the main reason flow-mode search was slower than direct mode.
        # Return the real system.search() result dicts unchanged — same shape the non-flow
        # backend returns ({rank, content, score, source, file_type, file_name}), so the app
        # renders flow-mode search identically to direct mode.
        top_k = int(self.top_k or 10)
        results = await run_with_query_system(
            self.config_path, lambda s: s.search(query, top_k=top_k)
        ) or []
        self.status = f"{len(results)} result(s) for: {query[:60]}"
        return Data(data={"query": query, "count": len(results), "results": results})
