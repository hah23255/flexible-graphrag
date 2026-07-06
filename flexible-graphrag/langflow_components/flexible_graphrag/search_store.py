"""Flexible GraphRAG Fulltext Search Component for Langflow.

Builds the fulltext / BM25 index via the real backend update_search (per SEARCH_DB /
SEARCH_BACKEND), using the shared system. Pass-through.
"""

from langflow_components.flexible_graphrag._fg_shared import get_run, make_payload, get_loop, has_ingest_chunks, ingest_chunk_count

from langflow.custom import Component
from langflow.io import HandleInput, Output
from langflow.schema import Data


class FlexibleSearchStoreComponent(Component):
    """Build a fulltext/BM25 search index (per .env: SEARCH_DB)."""

    display_name = "Flexible: Search Store"
    description = "Build fulltext/BM25 index (BM25/Elasticsearch/OpenSearch per .env) via the backend pipeline"
    icon = "Search"
    name = "FlexibleSearchStore"

    inputs = [
        HandleInput(name="chunks", display_name="Chunks", input_types=["Data"],
                    info="From Text Splitter: shared system + chunk nodes."),
    ]

    outputs = [
        Output(display_name="Result", name="result", method="create_index"),
    ]

    async def create_index(self) -> Data:
        from ingest.update_search import update_search

        run = get_run(self.chunks)
        system, nodes = run["system"], run.get("nodes")
        if not has_ingest_chunks(run):
            raise ValueError("No chunk nodes in the ingestion run")

        # insert (not refresh): preserves the chunks' stable ref_doc_id so incremental-sync
        # delete can find them (refresh re-wraps nodes with random ids). Matches the non-flow
        # path; sync modify = delete + add so the engine handles replacement.
        duration = await update_search(system, nodes, get_loop(), ingest_mode="insert")
        self.status = f"Search indexed {ingest_chunk_count(run)} chunk(s) into {system.config.search_db} in {duration:.2f}s."
        return make_payload(run["_key"], "search", num_nodes=ingest_chunk_count(run))
