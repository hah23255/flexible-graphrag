"""
flexible-graphrag LangChain Integration Module

Provides LangChain-based graph retrieval, QA chains, and unified adapters
for all subsystems (property graph, RDF, vector, search, LLM, embedding,
chunker, KG extractor).

Subpackages
-----------
graph/
  Property graph and RDF retrieval components and unified store adapters:
  GraphQueryRetriever             — Layer-1 LI wrapper (canonical name, new)
  TextToGraphQueryRetriever       — backward-compat alias for GraphQueryRetriever
  LCGraphQARetriever              — Layer-0 pure LC retriever (LangChain only)
  GraphEntityVectorRetriever      — entity embedding similarity search
  GraphNeighborhoodRetriever      — k-hop graph expansion from seed entity nodes
  SynonymExpander                 — LLM query rewriter (adds synonyms)
  SynonymExpanderRetriever        — wraps any retriever with SynonymExpander
  PropertyGraphStoreAdapter       — unified PG store ABC (LlamaIndex or LangChain)
  LlamaIndexPGAdapter             — wraps LlamaIndex PropertyGraphStore
  LangChainPGAdapter              — wraps LangChain graph + add_graph_documents()
  build_pg_store_adapter()        — factory for PG adapters
  nodes_to_graph_documents()      — convert LlamaIndex nodes -> LangChain GraphDocuments
  RdfGraphStoreAdapter            — unified RDF store ABC
  FusekiGraphAdapter              — thin wrapper over FusekiRDFStoreAdapter
  OxigraphGraphAdapter            — thin wrapper over OxigraphRDFStoreAdapter
  OntotextGraphAdapter            — thin wrapper over OntotextGraphDBRDFStoreAdapter
  build_rdf_store_adapter()       — factory for RDF adapters

  langchain_adapters/             — per-store adapters supplying chain backends
    RDF stores (SPARQL QA):
      GraphDBLangChainAdapter   — Ontotext GraphDB (RDF4J)
      FusekiLangChainAdapter    — Apache Jena Fuseki
      OxigraphLangChainAdapter  — Oxigraph
      NeptuneRDFAdapter         — Amazon Neptune RDF
    Property graph stores (Cypher/AQL/GQL QA):
      ArangoDBAdapter, ApacheAGEAdapter, CosmosGremlinAdapter
      SpannerGraphAdapter, SurrealDBAdapter
      NeptunePropertyGraphAdapter, NeptuneAnalyticsAdapter

  logging_retriever.py        — LoggingRetriever / wrap_with_logging()
  synonym_fusion.py           — SynonymFusion.from_config() / .wrap() / .wrap_all()

llm/
  LLM and embedding adapters:
  get_langchain_llm()             — returns ChatModel matching active LLM_PROVIDER
  LLMAdapter / EmbeddingAdapter   — unified ABCs (llamaindex | langchain | both)
  build_llm_adapter()             — factory for LLM adapters
  build_embedding_adapter()       — factory for embedding adapters

process/
  Chunker and KG extractor adapters:
  ChunkerAdapter                  — ABC (LlamaIndex SentenceSplitter or LangChain splitters)
  build_chunker_adapter()         — factory for chunker adapters
  KGExtractorAdapter              — ABC (LlamaIndex extractors or LLMGraphTransformer)
  LangChainKGExtractorAdapter     — ontology-aware LLMGraphTransformer wrapper
  build_kg_extractor_adapter()    — factory for KG extractor adapters

Namespace coexistence with the real PyPI ``langchain``
------------------------------------------------------
This package is deliberately named ``langchain`` and, in dev/editable and
Docker layouts, wins ``import langchain`` over the installed PyPI ``langchain``
library (our subpackages ``graph``/``llm``/``process``/``search``/``vector``
are imported as ``langchain.*``). That shadowing would otherwise starve code
that needs the *real* langchain submodules — notably Langflow 1.10.2+, whose
core startup does ``from langchain.agents import create_agent`` (uncaught, so a
miss is fatal). ``extend_path`` appends the real langchain's directory to our
``__path__`` as a fallback: our own submodules still resolve first (our dir
stays first in ``__path__``), and any name we don't define (``agents``,
``chains``, ``tools``, …) falls through to the real library.
"""

import pkgutil as _pkgutil

# Keep our subpackages first; append the real langchain dir(s) found elsewhere
# on sys.path so `langchain.agents` etc. remain importable despite the shadow.
__path__ = _pkgutil.extend_path(__path__, __name__)

