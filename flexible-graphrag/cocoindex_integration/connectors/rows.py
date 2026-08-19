"""Shared row schemas for the flexible-graphrag CocoIndex connectors.

These dataclasses are the *convention seam* between the two connector families
(``connectors.flexible`` and ``connectors.cocoindex``).  The pipeline in
``pipeline/app.py`` builds these rows once per chunk/triple and both families
consume the same types — no cross-family base class or Protocol is involved.

Field design for CocoIndex targets
----------------------------------
Each field is a separate, individually queryable column — NOT packed into a
single ``metadata`` dict.  This gives fine-grained control over filtering,
aggregation, and faceted search, and lets CocoIndex track rows by their primary
key for automatic stale-row deletion when a document is updated or removed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class VectorRow:
    """Schema for a single row in any flexible-graphrag vector store.

    CocoIndex tracks rows by (doc_id, chunk_index) for automatic stale-row
    deletion — no manual delete bookkeeping needed.
    """
    # ── Primary key ─────────────────────────────────────────────────────────
    doc_id: str              # config_id:source_path (stable across re-ingests)
    chunk_index: int         # 0-based position within the document

    # ── Core content ────────────────────────────────────────────────────────
    text: str                # chunk text (NOT the full document)
    embedding: List[float]   # dense vector — dimension matches EMBEDDING_DIMENSION

    # ── Source provenance ────────────────────────────────────────────────────
    file_name: str = ""      # human-readable filename (e.g. "report.pdf")
    file_path: str = ""      # full source path (e.g. "s3://bucket/path/report.pdf")
    file_type: str = ""      # extension: "pdf", "docx", "pptx", "txt", etc.
    source_type: str = ""    # datasource kind: "s3", "gcs", "azure_blob", etc.
    modified_at: str = ""    # ISO-8601 last-modified timestamp

    # ── Document-level IDs ───────────────────────────────────────────────────
    ref_doc_id: str = ""     # same as doc_id — kept for flexible-graphrag compat

    # ── Chunking metadata ────────────────────────────────────────────────────
    start_char_idx: int = 0  # start character offset within the original document
    end_char_idx: int = 0    # end character offset within the original document
    total_chunks: int = 0    # total number of chunks for this document

    # ── Canonical per-chunk metadata (merged ONCE upstream) ───────────────────
    # Full metadata dict built once in the pipeline: reader/placeholder metadata
    # (bucket_name/prefix/region for S3, container_name for Azure Blob, …) +
    # parse-derived metadata (conversion_method, …) + provenance (doc_id,
    # file_name, file_path, file_type, source_type, modified_at, chunk_index,
    # total_chunks, …).  Targets attach this verbatim — they do NOT rebuild it
    # from the explicit columns above (those columns exist for CocoIndex-native
    # per-column stores that need individually queryable fields).
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChunkRow:
    """Schema for a text chunk (source node) in the property graph.

    One ``ChunkRow`` per chunk per document.  ``FlexiblePropertyGraph`` writes
    these as ``ChunkNode`` objects which become ``__Chunk__`` nodes in Neo4j (and
    equivalent chunk nodes in other stores).  Entity nodes refer back to their
    source chunk via ``chunk_id``, which LlamaIndex uses to create ``MENTIONS``
    edges automatically during ``upsert_nodes()``.
    """
    # ── Primary key ─────────────────────────────────────────────────────────
    doc_id: str              # config_id:source_path
    chunk_index: int         # 0-based position within the document

    # ── Content ─────────────────────────────────────────────────────────────
    chunk_id: str            # stable UUID — LlamaIndex TextNode.node_id
    chunk_text: str          # full chunk text (written to the Chunk node)

    # ── Source provenance ────────────────────────────────────────────────────
    file_name: str = ""
    file_path: str = ""
    file_type: str = ""
    modified_at: str = ""

    # ── Optional pre-computed embedding ──────────────────────────────────────
    # When provided, written as the chunk node's vector property in Neo4j
    # so that the neighborhood retriever can return chunk text with similarity.
    embedding: Optional[List[float]] = None

    # ── Canonical per-chunk metadata (merged ONCE upstream) ───────────────────
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class KGTripleRow:
    """Schema for a single KG triple row in any flexible-graphrag property graph store.

    CocoIndex uses (doc_id, triple_index) as the primary key for stale-row
    deletion when a document is updated or removed.  Relation properties from
    the ontology are serialized to ``properties_json`` (a JSON string) so
    CocoIndex can fingerprint them as part of the row identity — this avoids
    schema issues with variable-depth property dicts in graph stores.
    """
    # ── Primary key ─────────────────────────────────────────────────────────
    doc_id: str              # config_id:source_path
    triple_index: int        # 0-based position within the chunk's extracted triples

    # ── Triple content ───────────────────────────────────────────────────────
    subject: str             # entity label (e.g. "Acme Corp")
    subject_type: str        # entity type from ontology (e.g. "Company")
    predicate: str           # relation label (e.g. "WORKS_FOR")
    obj: str                 # object label (e.g. "James Okafor")
    obj_type: str            # object entity type (e.g. "Person")

    # ── Source chunk linkage ─────────────────────────────────────────────────
    # chunk_id links entities to their source TextNode so LlamaIndex can create
    # MENTIONS edges (e.g. (chunk)-[:MENTIONS]->(entity) in Neo4j).
    chunk_id: str = ""       # LlamaIndex TextNode.node_id of the source chunk

    # ── Source provenance ────────────────────────────────────────────────────
    file_name: str = ""      # human-readable source filename
    file_path: str = ""      # full source path
    source_type: str = ""    # datasource kind
    ref_doc_id: str = ""     # same as doc_id for compatibility

    # ── Ontology-sourced relation properties ─────────────────────────────────
    # Stored as JSON string so CocoIndex can fingerprint them cleanly.
    # Example: '{"EMPLOYMENT_ROLE": "Software Engineer", "SALARY": "145000.0"}'
    properties_json: str = "{}"

    # ── Ontology-sourced ENTITY properties ───────────────────────────────────
    # The extractor produces these (KGEntity.properties) from
    # ``possible_entity_props`` / an ontology's owl:DatatypeProperty declarations,
    # e.g. '{"TIME": "2026-07-06", "NOTE": "Q3 roadmap review"}'.  They travel on
    # the triple because the triple is the unit CocoIndex fingerprints and
    # reconciles; the writers apply them to the endpoint nodes.
    #
    # The same entity is an endpoint of several triples, so its properties ride
    # along redundantly and two chunks can disagree.  Writers resolve that the
    # same way they already resolve a conflicting entity TYPE — first occurrence
    # wins (see the note near the node cache in flexible/property_graph.py).
    #
    # Empty default keeps every existing producer and stored row valid.
    subject_properties_json: str = "{}"
    obj_properties_json: str = "{}"


@dataclass
class SearchRow:
    """Schema for a full-text search document in Elasticsearch, OpenSearch, or BM25.

    CocoIndex tracks rows by (doc_id, chunk_index) for stale-row auto-deletion.
    """
    # ── Primary key ─────────────────────────────────────────────────────────
    doc_id: str              # config_id:source_path
    chunk_index: int         # 0-based chunk position

    # ── Core content ────────────────────────────────────────────────────────
    text: str                # chunk text (full-text indexed)

    # ── Source provenance ────────────────────────────────────────────────────
    file_name: str = ""          # human-readable filename (keyword field for filtering)
    file_path: str = ""          # full source path
    file_type: str = ""          # extension: "pdf", "docx", etc.
    source_type: str = ""        # datasource: "s3", "gcs", "azure_blob", etc.
    modified_at: str = ""        # ISO-8601 last-modified (date field for range queries)
    ref_doc_id: str = ""         # same as doc_id for compatibility

    # ── Optional embedding ───────────────────────────────────────────────────
    # When the underlying search store is a vector store (Elasticsearch,
    # OpenSearch), pass the pre-computed chunk embedding so the store's add()
    # call succeeds without triggering a second embed API call.
    embedding: List[float] = field(default_factory=list)

    # ── Canonical per-chunk metadata (merged ONCE upstream) ───────────────────
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RDFTripleRow:
    """Schema for a single RDF triple target row."""
    doc_id: str
    subject_label: str
    subject_type: str
    predicate_label: str
    obj_label: str
    obj_type: str
    file_name: str = ""
    properties: Dict[str, Any] = field(default_factory=dict)
