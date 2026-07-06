"""Flexible GraphRAG KG Extraction Component for Langflow.

Extracts entities/relations from chunks ONCE (LLM-intensive) so the Property Graph node
and the RDF node can both store from the same extraction in parallel.

- LlamaIndex backend: extraction only here (PG node stores via update_pg_graph(pre_extracted=True)).
- LangChain backend: extraction is coupled with the graph write inside LLMGraphTransformer,
  so this node performs extraction + property-graph write and the PG node passes through.
"""

from langflow_components.flexible_graphrag._fg_shared import get_run, make_payload, get_loop, has_ingest_chunks

from langflow.custom import Component
from langflow.io import HandleInput, BoolInput, Output
from langflow.schema import Data


class FlexibleKGExtractionComponent(Component):
    """Extract a knowledge graph from chunks (per .env); feeds Property Graph + RDF."""

    display_name = "Flexible: KG Extraction"
    description = "Extract entities/relations once; feeds Property Graph + RDF in parallel (per .env)"
    icon = "Sparkles"
    name = "FlexibleKGExtraction"

    inputs = [
        HandleInput(name="chunks", display_name="Chunks", input_types=["Data"],
                    info="From Text Splitter: shared system + chunk nodes."),
        BoolInput(name="skip_graph", display_name="Skip", value=False,
                  info="Skip KG extraction for this run."),
    ]

    outputs = [
        Output(display_name="KG", name="kg", method="extract"),
    ]

    async def extract(self) -> Data:
        run = get_run(self.chunks)
        system, nodes, documents = run["system"], run.get("nodes"), run.get("documents")
        if not has_ingest_chunks(run):
            raise ValueError("No chunk nodes in the ingestion run")

        cfg = system.config
        # skip_graph may come from the node's own field (Playground) or threaded through the
        # run-cache by the Data Source (app/flow mode passes it via the input_value payload).
        skip = (bool(self.skip_graph) or bool(run.get("skip_graph"))
                or not getattr(cfg, "enable_knowledge_graph", True))
        kg_backend = (getattr(cfg, "kg_extractor_backend", "llamaindex") or "llamaindex").lower()

        if skip:
            run["kg_extracted"] = False
            run["pg_done"] = False
            run["entities"] = 0
            run["relations"] = 0
            self.status = "KG extraction skipped (per flag or ENABLE_KNOWLEDGE_GRAPH=false)."
            return make_payload(run["_key"], "kg", entities=0, relations=0)

        if kg_backend == "langchain":
            # LangChain couples extraction + property-graph write — do both here.
            from ingest.update_pg_graph import update_pg_graph
            nodes, extracted, _kg_dur, _graph_dur, num_e, num_r = await update_pg_graph(
                system, nodes, documents, get_loop(), skip_graph=False
            )
            run["nodes"] = nodes
            run["kg_extracted"] = bool(extracted)
            run["pg_done"] = True
            self.status = (f"KG (LangChain) extracted + property graph written: "
                           f"{num_e} entities, {num_r} relations.")
        else:
            # LlamaIndex: extraction only.
            from process.kg_extractor import run_kg_extractors_on_nodes
            from ingest._helpers import make_kg_extractor
            nodes, num_e, num_r, _ = await run_kg_extractors_on_nodes(
                nodes, [make_kg_extractor(system)], cfg
            )
            run["nodes"] = nodes
            run["kg_extracted"] = True
            run["pg_done"] = False
            self.status = f"KG extracted: {num_e} entities, {num_r} relations (feeds Property Graph + RDF)."

        run["entities"] = num_e
        run["relations"] = num_r
        return make_payload(run["_key"], "kg", entities=num_e, relations=num_r)
