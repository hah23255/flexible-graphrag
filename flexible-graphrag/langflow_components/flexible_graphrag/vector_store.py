"""Flexible GraphRAG Vector Store Component for Langflow.

Indexes chunk nodes into the configured vector DB via the real backend update_vector
(LlamaIndex or LangChain per VECTOR_BACKEND), using the shared system. Pass-through.
"""

from langflow_components.flexible_graphrag._fg_shared import get_run, make_payload, get_loop, has_ingest_chunks, ingest_chunk_count

from langflow.custom import Component
from langflow.io import HandleInput, Output
from langflow.schema import Data


class FlexibleVectorStoreComponent(Component):
    """Store embeddings in the configured vector database (per .env: VECTOR_DB)."""

    display_name = "Flexible: Vector Store"
    description = "Index chunks into the vector DB (Qdrant/Neo4j/Elasticsearch/… per .env) via the backend pipeline"
    icon = "Database"
    name = "FlexibleVectorStore"

    inputs = [
        HandleInput(name="chunks", display_name="Chunks", input_types=["Data"],
                    info="From Text Splitter: shared system + chunk nodes."),
    ]

    outputs = [
        Output(display_name="Result", name="result", method="store_vectors"),
    ]

    async def store_vectors(self) -> Data:
        from ingest.update_vector import update_vector

        run = get_run(self.chunks)
        system, nodes = run["system"], run.get("nodes")
        if not has_ingest_chunks(run):
            raise ValueError("No chunk nodes in the ingestion run")

        # insert (not refresh): insert_nodes/async_add preserve the chunks' stable ref_doc_id,
        # so incremental-sync delete (delete by {config_id}:identity) can find them. refresh
        # re-wraps nodes into fresh LIDocuments with random ids, breaking that. Matches the
        # non-flow path (ingest_from_source uses default INSERT). Sync modify = delete + add,
        # so the engine handles replacement.
        duration = await update_vector(system, nodes, get_loop(), ingest_mode="insert")
        self.status = f"Vector indexed {ingest_chunk_count(run)} chunk(s) into {system.config.vector_db} in {duration:.2f}s."
        return make_payload(run["_key"], "vector", num_nodes=ingest_chunk_count(run))
