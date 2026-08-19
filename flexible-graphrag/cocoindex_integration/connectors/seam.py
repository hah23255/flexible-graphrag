"""Adapter seam — convention-based classification of pipeline targets.

The CocoIndex pipeline in ``pipeline/app.py`` handles two connector families that
are unified *by convention only* (shared method names + shared ``connectors.rows``
data types), NOT via a cross-family base class or Protocol:

* ``connectors.cocoindex`` — ``CocoConnector`` targets that write through
  CocoIndex's own native connectors (``declare_record`` / ``declare_point``
  reconciliation).
* ``connectors.flexible``  — ``FlexibleConnector`` targets that write through
  flexible-graphrag's own LlamaIndex / LangChain adapters.

Every place the pipeline forks "native vs flexible" used to test the *concrete*
class (``isinstance(x, CocoQdrant)`` / ``isinstance(x, CocoNeo4j)``).  That is
fragile: adding a second native vector store (LanceDB, pgvector) or graph store
(FalkorDB) would silently mis-route it into the flexible provider path.

This module is the single seam that classifies a target by its *family / kind
base class* instead.  The concrete store-specific write bodies in ``app.py`` stay
keyed to the concrete class (their mechanics differ per store); only the fork
decision is centralised here.

Design constraints (per project owner):
* The four target pickers (vector / pg / rdf / search) stay separate to mirror
  the ``.env`` config vars — this module does NOT collapse them.
* ``_run_pipeline`` keeps its small coco-vs-flexible branch — this module only
  supplies the predicates that branch reads.
* Names use ``Coco`` or ``Flexible``, never both, never ``Native``.

All predicates are ``None``-safe (return ``False`` for ``None``) and never raise
if a family's base class cannot be imported (e.g. cocoindex not installed).
"""

from __future__ import annotations

from typing import Any

try:  # cocoindex-native family (safe even if cocoindex itself is absent —
    # base.py only imports logging).
    from cocoindex_integration.connectors.cocoindex.base import (
        CocoConnector,
        CocoPropertyGraph,
        CocoSource,
        CocoVector,
    )
except Exception:  # noqa: BLE001 - defensive: keep predicates usable regardless
    CocoConnector = CocoVector = CocoPropertyGraph = CocoSource = ()  # type: ignore[assignment]

try:  # flexible family (base.py only imports stdlib — safe to import here).
    from cocoindex_integration.connectors.flexible.base import FlexibleConnector
except Exception:  # noqa: BLE001
    FlexibleConnector = ()  # type: ignore[assignment]


# ─────────────────────────────────────────────────────────────────────────────
# Family-level predicates
# ─────────────────────────────────────────────────────────────────────────────

def is_coco_native(target: Any) -> bool:
    """True when *target* is a CocoIndex-native connector (any kind)."""
    return isinstance(target, CocoConnector)


def is_flexible(target: Any) -> bool:
    """True when *target* is a flexible (LI/LC-wrapping) connector."""
    return isinstance(target, FlexibleConnector)


# ─────────────────────────────────────────────────────────────────────────────
# Kind-level predicates (used by the vector / pg forks in app.py)
# ─────────────────────────────────────────────────────────────────────────────

def is_coco_vector(target: Any) -> bool:
    """True when *target* is a CocoIndex-native vector-store target.

    Covers every current and future native vector store (Qdrant today; LanceDB /
    pgvector when wired) so the flexible fallback fork never mis-routes them.
    """
    return isinstance(target, CocoVector)


def is_coco_pg(target: Any) -> bool:
    """True when *target* is a CocoIndex-native property-graph target.

    Covers every current and future native graph store (Neo4j today; FalkorDB
    when wired).
    """
    return isinstance(target, CocoPropertyGraph)


def is_coco_source(target: Any) -> bool:
    """True when *target* is a CocoIndex-native source connector."""
    return isinstance(target, CocoSource)


# ─────────────────────────────────────────────────────────────────────────────
# Diagnostics
# ─────────────────────────────────────────────────────────────────────────────

def describe_target(target: Any) -> str:
    """Human-readable one-line description for logging (``None``-safe)."""
    if target is None:
        return "none"
    if is_coco_native(target):
        describe = getattr(target, "describe", None)
        if callable(describe):
            try:
                return describe()
            except Exception:  # noqa: BLE001
                pass
        return f"Coco {getattr(target, 'kind', 'connector')} '{getattr(target, 'name', '')}'"
    if is_flexible(target):
        return f"Flexible {type(target).__name__}"
    return type(target).__name__


__all__ = [
    "is_coco_native",
    "is_flexible",
    "is_coco_vector",
    "is_coco_pg",
    "is_coco_source",
    "describe_target",
]
