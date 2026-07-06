"""Flexible GraphRAG Ingestion Summary Component for Langflow.

Joins the four store branches (Vector, Search, Property Graph, RDF) so the whole ingestion
runs once, then prints a single summary. Output is a Message so it can feed a ChatOutput —
which lets the ingest flow run from the langflow Playground in one shot (no per-node reruns).
"""

from langflow_components.flexible_graphrag._fg_shared import get_run, ingest_chunk_count

from langflow.custom import Component
from langflow.io import HandleInput, Output
from langflow.schema.message import Message


class FlexibleIngestSummaryComponent(Component):
    """Join all store branches and print a one-shot ingestion summary."""

    display_name = "Flexible: Ingestion Summary"
    description = "Join Vector/Search/Property Graph/RDF; print a one-shot summary (feeds ChatOutput for the Playground)"
    icon = "ListChecks"
    name = "FlexibleIngestSummary"

    inputs = [
        HandleInput(name="vector", display_name="Vector", input_types=["Data"], required=False),
        HandleInput(name="search", display_name="Search", input_types=["Data"], required=False),
        HandleInput(name="kg", display_name="KG Extraction", input_types=["Data"], required=False),
        HandleInput(name="graph", display_name="Property Graph", input_types=["Data"], required=False),
        HandleInput(name="rdf", display_name="RDF", input_types=["Data"], required=False),
    ]

    outputs = [
        Output(display_name="Summary", name="summary", method="summarize"),
    ]

    async def summarize(self) -> Message:
        run = get_run([self.vector, self.search, self.kg, self.graph, self.rdf])
        cfg = run["system"].config
        files = run.get("file_paths") or []
        docs = run.get("documents") or []
        # chunk count: LI nodes if present, else stashed LangChain chunks (all-LC backend has 0 LI nodes)
        chunks = ingest_chunk_count(run)
        ents, rels = run.get("entities", 0), run.get("relations", 0)
        rdf_db = getattr(cfg, "rdf_graph_db", "none")

        lines = [
            "Ingestion complete.",
            f"  Files:           {len(files)}",
            f"  Documents:       {len(docs)}",
            f"  Chunks:          {chunks}",
            f"  KG Extraction:   {ents} entities, {rels} relations",
            f"  Vector DB:       {cfg.vector_db}  ({chunks} vectors)",
            f"  Search DB:       {cfg.search_db}  ({chunks} docs)",
            f"  Property graph:  {cfg.pg_graph_db}  ({ents} entities, {rels} relations)",
            f"  RDF graph:       {rdf_db}  ({ents} entities, {rels} relations)",
        ]
        text = "\n".join(lines)
        self.status = text
        return Message(text=text)
