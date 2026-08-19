"""CocoIndex-native relational and analytical target/source connectors (placeholders).

CocoIndex v1 supports the following relational / analytical stores:

    Postgres      — relational rows (as source via PgTableSource AND as target);
                    its pgvector role is a separate vector target (``vector/postgres.py``)
    Apache Doris  — OLAP columnar store; HNSW/IVF vector + inverted full-text
    SQLite        — embedded relational DB; optional sqlite-vec vector extension
    Snowflake     — cloud data warehouse; managed DDL, MERGE upserts, deletes
    BigQuery      — Google Cloud data warehouse; managed DDL, MERGE, deletes

These are **row-oriented stores** — they exchange structured rows with a
CocoIndex flow (relational tables, extracted entities, chunk metadata, analytics
events, tabular exports).  This is distinct from the document → chunk → embed
pipeline flexible-graphrag drives for Q&A: those workloads use the vector /
property-graph / search targets.  So these row stores are registered here as
recognised names but left unimplemented — they matter mainly for someone
building a **custom CocoIndex flow** (standalone or alongside the app pipeline).

Dual-capability note:
* ``doris`` and ``sqlite`` also appear in ``vector/__init__.py`` — the same
  engine can act as a vector target (HNSW/IVF, sqlite-vec) OR a relational-row
  target depending on what the flow writes.
* ``postgres`` is split three ways: relational-row source/target (here),
  pgvector vector target (``vector/postgres.py``), implemented; and it can also
  feed rows into a flow via ``PgTableSource``.

When are they useful?
---------------------
* Read relational tables into a flow (Postgres ``PgTableSource``) for enrichment.
* Export extracted entities or KG triples to a data warehouse for BI / SQL analytics.
* Feed a downstream pipeline that consumes structured rows (Snowflake, BigQuery).
* Lightweight local storage via SQLite (e.g. unit tests, edge deployments).
* Apache Doris hybrid workloads (full-text + vector) in high-throughput settings.

Usage
-----
These stores are already available as native CocoIndex connectors
(``cocoindex.connectors.*``).  To use one, extend or customise the CocoIndex
pipeline — for example, add a target or source step in ``pipeline/app.py`` that
calls the connector directly — and register its builder in
``COCO_RELATIONAL_REGISTRY`` below.

``COCO_RELATIONAL_REGISTRY`` mirrors the shape of ``COCO_VECTOR_REGISTRY``:
``None`` builder = recognised store name without a native implementation.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from cocoindex_integration.connectors.cocoindex.base import CocoConnector

logger = logging.getLogger(__name__)


class CocoRelational(CocoConnector):
    """Kind-base for CocoIndex-native relational / analytical connectors.

    Row-oriented stores exchange structured rows with a flow.  Postgres can act
    as both source and target; the warehouses are write targets.
    """
    kind = "relational"
    can_read = True   # Postgres PgTableSource can read rows into a flow
    can_write = True


#: name → builder(db_cfg) -> Optional[CocoRelational].
#: ``None`` = recognised store name without a native v1.x implementation here.
COCO_RELATIONAL_REGISTRY: Dict[
    str, Optional[Callable[[Dict[str, Any]], Optional[CocoRelational]]]
] = {
    # Relational (source + target) — pgvector role lives in vector/postgres.py
    "postgres": None,    # PostgreSQL rows — PgTableSource (read) + table target (write)
    # Analytical stores (warehouse / OLAP)
    "doris": None,       # Apache Doris — HNSW/IVF vector + inverted full-text (also vector/)
    "snowflake": None,   # Snowflake — cloud data warehouse
    "bigquery": None,    # Google BigQuery — cloud data warehouse
    # Embedded / lightweight relational
    "sqlite": None,      # SQLite — embedded DB with optional sqlite-vec extension (also vector/)
}

#: Store names that have a CocoIndex relational target descriptor.
COCO_RELATIONAL_TARGETS = frozenset(COCO_RELATIONAL_REGISTRY)


def coco_relational_target(
    store_name: str, db_cfg: Dict[str, Any]
) -> Optional[CocoRelational]:
    """Return a CocoRelational connector (always None — placeholders only).

    These stores are already available as native CocoIndex connectors.  When
    ``None`` is returned, extend the CocoIndex pipeline in ``pipeline/app.py``
    to call the relevant ``cocoindex.connectors.*`` connector directly.
    *db_cfg* is reserved for future builders.
    """
    name_lower = store_name.lower()
    if name_lower not in COCO_RELATIONAL_REGISTRY:
        return None
    builder = COCO_RELATIONAL_REGISTRY[name_lower]
    if builder is None:
        logger.warning(
            "[coco] %s: relational connector not yet wired — "
            "extend the CocoIndex pipeline to call the native "
            "cocoindex.connectors.%s connector directly", store_name, name_lower,
        )
        return None
    return builder(db_cfg)


__all__ = [
    "CocoRelational",
    "COCO_RELATIONAL_REGISTRY",
    "COCO_RELATIONAL_TARGETS",
    "coco_relational_target",
]
