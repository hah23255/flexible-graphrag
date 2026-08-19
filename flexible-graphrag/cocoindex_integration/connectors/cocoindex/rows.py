"""
Explicit field schemas for CocoIndex-native targets.

Using explicit typed fields (not a generic "metadata" dict) gives CocoIndex
fine-grained column-level tracking and lets each connector store only the
columns it actually needs.

These are the *CocoIndex-native* row schemas (written straight into Qdrant /
Neo4j / etc. via CocoIndex's own connectors).  They are intentionally separate
from ``connectors.rows`` — that module holds the *flexible-family* row types
(the convention seam shared with LlamaIndex / LangChain adapters).

Schemas defined here:
    CocoVectorRow    — vector-store targets (Qdrant, LanceDB, pgvector, …)
    CocoKGTripleRow  — property-graph targets (Neo4j, FalkorDB, …)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class CocoVectorRow:
    """Schema for vector-store targets (Qdrant, LanceDB, Turbopuffer, zvec)."""
    doc_id: str                     # config_id:stable_path
    chunk_index: int
    text: str                       # raw chunk text
    embedding: List[float]
    file_name: str
    file_path: str                  # cloud path (bucket/key) or local path
    file_type: str                  # pdf, docx, txt …
    source_type: str                # s3, gcs, filesystem, alfresco …
    modified_at: str                # ISO-8601 or ""
    ref_doc_id: str                 # same as doc_id (LlamaIndex convention)
    start_char_idx: int = 0
    end_char_idx: int = 0
    total_chunks: int = 0
    properties_json: str = "{}"     # JSON of extra node metadata


@dataclass
class CocoKGTripleRow:
    """Schema for property-graph targets (Neo4j, FalkorDB, SurrealDB)."""
    doc_id: str
    triple_index: int
    head: str                       # subject entity name
    relation: str                   # predicate / relationship type
    tail: str                       # object entity name
    head_type: str = ""             # ontology entity type of subject
    tail_type: str = ""             # ontology entity type of object
    file_name: str = ""
    source_type: str = ""
    ref_doc_id: str = ""
    properties_json: str = "{}"     # JSON of relation properties


