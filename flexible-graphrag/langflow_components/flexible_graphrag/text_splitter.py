"""Flexible GraphRAG Text Splitter Component for Langflow.

Runs the real backend chunk pipeline (run_chunk_pipeline; LlamaIndex or LangChain per
CHUNKER_BACKEND) on the shared system. Threads documents + chunk nodes downstream.
"""

from langflow_components.flexible_graphrag._fg_shared import get_run, make_payload, get_loop

from langflow.custom import Component
from langflow.io import HandleInput, Output
from langflow.schema import Data


class FlexibleSplitterComponent(Component):
    """Split documents into chunks via the backend chunk pipeline."""

    display_name = "Flexible: Text Splitter"
    description = "Chunk documents using the backend pipeline (chunk size/overlap and backend from .env)"
    icon = "Scissors"
    name = "FlexibleTextSplitter"

    inputs = [
        HandleInput(name="documents", display_name="Documents", input_types=["Data"],
                    info="From Document Processor: shared system + documents."),
    ]

    outputs = [
        Output(display_name="Chunks", name="chunks", method="split_documents"),
    ]

    async def split_documents(self) -> Data:
        from ingest.run_chunk_pipeline import run_chunk_pipeline

        run = get_run(self.documents)
        documents = run.get("documents")
        if not documents:
            raise ValueError("No documents in the ingestion run")

        nodes, chunk_duration = await run_chunk_pipeline(run["system"], documents, get_loop())
        run["nodes"] = nodes
        self.status = f"Chunked {len(documents)} document(s) into {len(nodes)} node(s) in {chunk_duration:.2f}s."
        return make_payload(run["_key"], "chunks", num_chunks=len(nodes))
