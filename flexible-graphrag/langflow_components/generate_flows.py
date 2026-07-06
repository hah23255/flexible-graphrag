"""Generate the Flexible GraphRAG Langflow flows from the live component classes.

Writes ``fg_ingestion_flow.json`` and ``fg_query_flow.json`` — built by importing the
12 ``Flexible*`` component classes and rendering each via Langflow's
``build_custom_component_template``, so the flows always embed the CURRENT component code.
Because app-driven flow mode runs the flow's EMBEDDED code, you must re-run this whenever you
edit a component (e.g. the query-node warm-system caching) and then restart the backend so it
re-uploads the flow.

Run it in the LANGFLOW venv (needs ``langflow``/``lfx`` + an editable ``flexible-graphrag``):

    python flexible-graphrag/langflow_components/generate_flows.py            # writes to <repo>/flows
    python flexible-graphrag/langflow_components/generate_flows.py <out_dir>  # custom output dir

The Langflow starter template is resolved from the installed ``langflow`` package, so this
works regardless of which venv/repo you run it from.
"""
import json, copy, importlib, sys, os

# Make the flexible-graphrag dir importable (so `langflow_components...` resolves) regardless
# of the current working directory.
_FG_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _FG_DIR not in sys.path:
    sys.path.insert(0, _FG_DIR)

from lfx.custom.utils import build_custom_component_template

# Resolve the bundled Langflow starter project from the INSTALLED langflow package (not a
# hardcoded venv path) so this runs in any venv/repo.
import langflow as _lf
STARTER = os.path.join(os.path.dirname(_lf.__file__),
                       "initial_setup", "starter_projects", "Basic Prompting.json")

COMP = {
 "FlexibleDataSource": ("data_source", "FlexibleDataSourceComponent"),
 "FlexibleDocProcessor": ("document_processor", "FlexibleDocProcessorComponent"),
 "FlexibleTextSplitter": ("text_splitter", "FlexibleSplitterComponent"),
 "FlexibleVectorStore": ("vector_store", "FlexibleVectorStoreComponent"),
 "FlexibleKGExtraction": ("kg_extraction", "FlexibleKGExtractionComponent"),
 "FlexibleGraphStore": ("graph_store", "FlexibleGraphStoreComponent"),
 "FlexibleSearchStore": ("search_store", "FlexibleSearchStoreComponent"),
 "FlexibleRDFStore": ("rdf_store", "FlexibleRDFStoreComponent"),
 "FlexibleIngestSummary": ("ingest_summary", "FlexibleIngestSummaryComponent"),
 "FlexibleHybridSearch": ("hybrid_search", "FlexibleHybridSearchComponent"),
 "FlexibleAIQuery": ("ai_query", "FlexibleAIQueryComponent"),
 "FlexibleQuerySummary": ("query_summary", "FlexibleQuerySummaryComponent"),
}

_tpl = {}
def our(type_name, nid, x, y):
    if type_name not in _tpl:
        mod, cls = COMP[type_name]
        m = importlib.import_module(f"langflow_components.flexible_graphrag.{mod}")
        _tpl[type_name], _ = build_custom_component_template(getattr(m, cls)())
    fn = json.loads(json.dumps(_tpl[type_name]))
    return {"id": nid, "type": "genericNode", "position": {"x": x, "y": y},
            "data": {"id": nid, "type": type_name, "node": fn}}

def starter(node_type, nid, x, y):
    d = json.load(open(STARTER, encoding="utf-8"))
    for n in d["data"]["nodes"]:
        if n["data"].get("type") == node_type:
            n = copy.deepcopy(n)
            n["id"] = nid; n["position"] = {"x": x, "y": y}; n["data"]["id"] = nid
            for k in ("measured", "positionAbsolute", "dragging", "selected"):
                n.pop(k, None)
            return n
    raise KeyError(node_type)

def scape(d):
    return json.dumps(d, sort_keys=True, separators=(",", ":")).replace('"', "œ")

def out_types(node, name):
    for o in node["data"]["node"].get("outputs", []):
        if o.get("name") == name:
            return o.get("types", [])
    return []

def in_info(node, field):
    f = node["data"]["node"]["template"][field]
    return f.get("input_types") or [], f.get("type", "other")

def edge(src, sout, tgt, tfield):
    st = out_types(src, sout)
    tt, tft = in_info(tgt, tfield)
    sh = {"dataType": src["data"]["type"], "id": src["id"], "name": sout, "output_types": st}
    th = {"fieldName": tfield, "id": tgt["id"], "inputTypes": tt, "type": tft}
    sH, tH = scape(sh), scape(th)
    return {"animated": False, "className": "", "selected": False,
            "source": src["id"], "target": tgt["id"], "sourceHandle": sH, "targetHandle": tH,
            "data": {"sourceHandle": sh, "targetHandle": th},
            "id": f"reactflow__edge-{src['id']}{sH}-{tgt['id']}{tH}"}

def flow(name, desc, nodes, edges, zoom=0.7):
    return {"name": name, "description": desc,
            "data": {"nodes": nodes, "edges": edges, "viewport": {"x": 0, "y": 0, "zoom": zoom}},
            "is_component": False}

def validate(payload, fid):
    from lfx.graph.graph.base import Graph
    g = Graph.from_payload(payload["data"], flow_id=fid); g.prepare()
    return len(g.vertices), len(g.edges)

def build_ingestion():
    ci = starter("ChatInput", "ChatInput-0001", 60, 60)
    ds = our("FlexibleDataSource", "FlexibleDataSource-0001", 60, 380)
    dp = our("FlexibleDocProcessor", "FlexibleDocProcessor-0001", 380, 380)
    sp = our("FlexibleTextSplitter", "FlexibleTextSplitter-0001", 700, 380)
    vs = our("FlexibleVectorStore", "FlexibleVectorStore-0001", 1020, 120)
    ss = our("FlexibleSearchStore", "FlexibleSearchStore-0001", 1020, 320)
    kg = our("FlexibleKGExtraction", "FlexibleKGExtraction-0001", 1020, 560)
    pg = our("FlexibleGraphStore", "FlexibleGraphStore-0001", 1340, 460)
    rd = our("FlexibleRDFStore", "FlexibleRDFStore-0001", 1340, 680)
    sm = our("FlexibleIngestSummary", "FlexibleIngestSummary-0001", 1700, 380)
    co = starter("ChatOutput", "ChatOutput-0001", 2040, 380)
    nodes = [ci, ds, dp, sp, vs, ss, kg, pg, rd, sm, co]
    edges = [
        edge(ci, "message", ds, "trigger"),
        edge(ds, "source", dp, "source"),
        edge(dp, "documents", sp, "documents"),
        edge(sp, "chunks", vs, "chunks"),
        edge(sp, "chunks", ss, "chunks"),
        edge(sp, "chunks", kg, "chunks"),
        edge(kg, "kg", pg, "kg"),
        edge(kg, "kg", rd, "kg"),
        edge(vs, "result", sm, "vector"),
        edge(ss, "result", sm, "search"),
        edge(kg, "kg", sm, "kg"),
        edge(pg, "result", sm, "graph"),
        edge(rd, "result", sm, "rdf"),
        edge(sm, "summary", co, "input_value"),
    ]
    return flow("FG Ingestion Flow",
                "Data Source -> Doc Processor -> Splitter -> {Vector, Search, KG Extraction}; KG Extraction -> {Property Graph, RDF}; all -> Ingestion Summary -> ChatOutput. Run via Play or the Playground (one shot).",
                nodes, edges)

def build_query():
    ci = starter("ChatInput", "ChatInput-0001", 60, 320)
    hs = our("FlexibleHybridSearch", "FlexibleHybridSearch-0001", 460, 140)
    aq = our("FlexibleAIQuery", "FlexibleAIQuery-0001", 460, 480)
    qs = our("FlexibleQuerySummary", "FlexibleQuerySummary-0001", 900, 320)
    co = starter("ChatOutput", "ChatOutput-0001", 1280, 320)
    edges = [
        edge(ci, "message", hs, "query"),
        edge(ci, "message", aq, "query"),
        edge(hs, "results", qs, "search"),
        edge(aq, "answer", qs, "answer"),
        edge(qs, "summary", co, "input_value"),
    ]
    return flow("FG Query Flow",
                "Playground: ChatInput fans out to Hybrid Search + AI Query (independent — neither feeds the other); Query Summary joins both -> ChatOutput.",
                [ci, hs, aq, qs, co], edges, zoom=0.75)

def build_search():
    # Dedicated search-only flow for the app: ChatInput -> Hybrid Search. Keeping AI Query out
    # of this flow means a search runs ONLY Hybrid Search (langflow runs every branch present,
    # so the combined query flow would also fire an AI Query LLM call on each search).
    ci = starter("ChatInput", "ChatInput-0001", 60, 320)
    hs = our("FlexibleHybridSearch", "FlexibleHybridSearch-0001", 460, 320)
    edges = [edge(ci, "message", hs, "query")]
    return flow("FG Search Flow",
                "App search: ChatInput -> Hybrid Search (no AI Query branch). Backend reads the Hybrid Search output.",
                [ci, hs], edges, zoom=0.8)

def build_aiquery():
    # Dedicated AI-query-only flow for the app: ChatInput -> AI Query (no Hybrid Search branch).
    ci = starter("ChatInput", "ChatInput-0001", 60, 320)
    aq = our("FlexibleAIQuery", "FlexibleAIQuery-0001", 460, 320)
    edges = [edge(ci, "message", aq, "query")]
    return flow("FG AI Query Flow",
                "App AI query: ChatInput -> AI Query (no Hybrid Search branch). Backend reads the AI Query output.",
                [ci, aq], edges, zoom=0.8)

if __name__ == "__main__":
    # Default output: <repo_root>/flows (repo_root is the parent of the flexible-graphrag dir).
    out_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(_FG_DIR), "flows")
    os.makedirs(out_dir, exist_ok=True)
    for builder, fname, fid in ((build_ingestion, "fg_ingestion_flow.json", "ing"),
                                (build_query, "fg_query_flow.json", "qry"),
                                (build_search, "fg_search_flow.json", "sch"),
                                (build_aiquery, "fg_aiquery_flow.json", "aiq")):
        f = builder()
        nv, ne = validate(f, fid)
        json.dump(f, open(os.path.join(out_dir, fname), "w"), indent=2)
        print(f"VALID {fname}: {nv} vertices, {ne} edges  ->  {os.path.join(out_dir, fname)}")
