"""langchain.graph.rdf_store_adapters — per-backend LC RDF/SPARQL adapters.

Adapters available
------------------
graphdb_langchain_adapter    GraphDBLangChainAdapter   — Ontotext GraphDB (RDF4J)
fuseki_langchain_adapter     FusekiLangChainAdapter    — Apache Jena Fuseki
oxigraph_langchain_adapter   OxigraphLangChainAdapter  — Oxigraph
neptune_rdf_adapter          NeptuneRDFAdapter         — Amazon Neptune RDF

Each adapter module is loaded lazily by :func:`build_rdf_store_adapter`
(in :mod:`adapters.graph.rdf_store_adapter`) based on the configured
``RDF_GRAPH_DB`` value.  Nothing is imported here at package load time.
"""

__all__: list = []
