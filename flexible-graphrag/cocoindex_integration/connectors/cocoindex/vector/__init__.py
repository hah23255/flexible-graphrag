"""CocoIndex-native vector-store connectors (one module per store).

Selection is table-driven via ``COCO_VECTOR_REGISTRY`` (name → builder).

Implemented (CocoIndex native + flexible-graphrag LI/LC adapters):
    qdrant (``qdrant.py``), lancedb (``lancedb.py``), postgres (``postgres.py`` / pgvector)

Registered placeholder — native CocoIndex connector, not yet wired in this pipeline.
These stores are NOT available as flexible-graphrag LI/LC adapters; to use them,
extend the CocoIndex pipeline to call ``cocoindex.connectors.<name>`` directly:
    turbopuffer   single/named-vector namespaces
    valkey        Redis-compatible search with HNSW/FLAT vector indexes
    zvec          embedded in-process dense + sparse vectors
    doris         Apache Doris analytical DB — HNSW/IVF vector + inverted FTS
    sqlite        embedded SQLite — optional vector search via sqlite-vec

Stores not in this registry (e.g. weaviate, chroma, milvus, elasticsearch-vector,
pinecone, opensearch, pgvector-langchain) are flexible-graphrag LI/LC only —
set ``VECTOR_BACKEND=llamaindex`` or ``langchain`` to use them.

Note: ``doris`` and ``sqlite`` are dual-purpose — vector-capable targets here,
and relational/row targets in ``relational.py``.  ``postgres`` is likewise split:
vector (pgvector) role here; relational-row role in ``relational.py``.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from cocoindex_integration.connectors.cocoindex.base import CocoVector
from cocoindex_integration.connectors.cocoindex.vector.qdrant import (
    CocoQdrant,
    build_qdrant,
)
from cocoindex_integration.connectors.cocoindex.vector.lancedb import (
    CocoLanceDB,
    build_lancedb,
)
from cocoindex_integration.connectors.cocoindex.vector.postgres import (
    CocoPostgres,
    build_postgres,
)

logger = logging.getLogger(__name__)

#: name → builder(db_cfg) -> Optional[CocoVector].  ``None`` = recognised name
#: without a native v1.x implementation.
COCO_VECTOR_REGISTRY: Dict[str, Optional[Callable[[Dict[str, Any]], Optional[CocoVector]]]] = {
    # ── Implemented ──────────────────────────────────────────────────────────
    "qdrant": build_qdrant,
    "lancedb": build_lancedb,
    "postgres": build_postgres,
    # ── Placeholder: native CocoIndex connectors, not yet wired in this pipeline ──
    # These are NOT in flexible-graphrag's LI/LC adapters.  To use one, extend
    # the CocoIndex pipeline to call cocoindex.connectors.<name> directly.
    "turbopuffer": None,   # Turbopuffer — namespaced single/named vectors
    "valkey": None,        # Valkey (Redis-compatible) — HNSW/FLAT vector search
    "zvec": None,          # zvec — embedded in-process dense + sparse vectors
    "doris": None,         # Apache Doris — HNSW/IVF vector + inverted FTS (also relational.py)
    "sqlite": None,        # SQLite + sqlite-vec — embedded vector search (also relational.py)
}

#: Vector stores that have a native CocoIndex target connector (names).
COCO_VECTOR_TARGETS = frozenset(COCO_VECTOR_REGISTRY)


def coco_vector_target(vector_db: str, db_cfg: Dict[str, Any]) -> Optional[Any]:
    """Return a CocoVector connector (or None) for *vector_db*.

    Two distinct ``None`` outcomes:
    - Store is in ``COCO_VECTOR_REGISTRY`` with a ``None`` builder: native
      CocoIndex connector not yet wired in this pipeline.  Extend the pipeline
      to call ``cocoindex.connectors.<name>`` directly.
    - Store is not in the registry at all: flexible-graphrag LI/LC only — the
      caller falls back to ``FlexibleVector`` and ``VECTOR_BACKEND`` controls
      which adapter is used.
    """
    name_lower = vector_db.lower()
    if name_lower not in COCO_VECTOR_REGISTRY:
        # Not a CocoIndex native connector — flexible-graphrag LI/LC path.
        return None
    builder = COCO_VECTOR_REGISTRY[name_lower]
    if builder is None:
        logger.warning(
            "[coco] %s: native CocoIndex connector not yet wired in this pipeline — "
            "extend the CocoIndex pipeline to use cocoindex.connectors.%s directly",
            vector_db, name_lower,
        )
        return None
    return builder(db_cfg)


__all__ = [
    "CocoQdrant",
    "CocoLanceDB",
    "CocoPostgres",
    "build_qdrant",
    "build_lancedb",
    "build_postgres",
    "COCO_VECTOR_REGISTRY",
    "COCO_VECTOR_TARGETS",
    "coco_vector_target",
]
