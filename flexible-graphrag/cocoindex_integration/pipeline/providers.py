"""CocoIndex TargetStateProvider registration — one provider per store family.

Each ``_get_or_create_*_provider`` function is idempotent: the first call
registers a ``TargetHandler`` with CocoIndex (which issues upsert / delete
actions for the flexible-graphrag adapters); subsequent calls return the
cached provider stored on :mod:`state`.

Import this module and call the helpers; do **not** use ``global`` here —
always mutate ``state.*_provider`` directly.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Lazy import of cocoindex so that modules that import providers without a
# running CocoIndex environment don't crash at import time.
import cocoindex as coco  # noqa: E402

from cocoindex_integration.pipeline import state as _state  # noqa: E402


def _get_or_create_vector_provider(target: Any) -> Optional[Any]:
    """Register FlexibleVectorHandler with CocoIndex (idempotent).

    Returns the ``coco.TargetStateProvider`` created by
    ``register_root_target_states_provider``.  Subsequent calls return the
    cached provider without re-registering.  Returns *None* when CocoIndex is
    unavailable or *target* is None.
    """
    if _state._vector_provider is not None or target is None:
        return _state._vector_provider
    try:
        from cocoindex_integration.connectors.flexible.vector import FlexibleVectorHandler  # noqa: PLC0415
        handler = FlexibleVectorHandler(target)
        _state._vector_provider = coco.register_root_target_states_provider(
            "flexible-graphrag/vector/v1",
            handler,
        )
        logger.info(
            "Vector: CocoIndex TargetStateProvider registered (FlexibleVectorHandler)"
        )
    except Exception as exc:
        logger.warning(
            "Vector: could not register CocoIndex TargetStateProvider — "
            "deletions will not be auto-handled: %s", exc,
        )
    return _state._vector_provider


def _get_or_create_pg_provider(target: Any) -> Optional[Any]:
    """Register FlexiblePGHandler with CocoIndex (idempotent)."""
    if _state._pg_provider is not None or target is None:
        return _state._pg_provider
    try:
        from cocoindex_integration.connectors.flexible.property_graph import FlexiblePGHandler  # noqa: PLC0415
        handler = FlexiblePGHandler(target)
        _state._pg_provider = coco.register_root_target_states_provider(
            "flexible-graphrag/pg/v1",
            handler,
        )
        logger.info("PG: CocoIndex TargetStateProvider registered (FlexiblePGHandler)")
    except Exception as exc:
        logger.warning(
            "PG: could not register CocoIndex TargetStateProvider — "
            "deletions will not be auto-handled: %s", exc,
        )
    return _state._pg_provider


def _get_or_create_search_provider(target: Any) -> Optional[Any]:
    """Register FlexibleSearchHandler with CocoIndex (idempotent)."""
    if _state._search_provider is not None or target is None:
        return _state._search_provider
    try:
        from cocoindex_integration.connectors.flexible.search import FlexibleSearchHandler  # noqa: PLC0415
        handler = FlexibleSearchHandler(target)
        _state._search_provider = coco.register_root_target_states_provider(
            "flexible-graphrag/search/v1",
            handler,
        )
        logger.info("Search: CocoIndex TargetStateProvider registered (FlexibleSearchHandler)")
    except Exception as exc:
        logger.warning(
            "Search: could not register CocoIndex TargetStateProvider — "
            "deletions will not be auto-handled: %s", exc,
        )
    return _state._search_provider


def _get_or_create_rdf_provider(target: Any) -> Optional[Any]:
    """Register FlexibleRDFHandler with CocoIndex (idempotent)."""
    if _state._rdf_provider is not None or target is None:
        return _state._rdf_provider
    try:
        from cocoindex_integration.connectors.flexible.rdf import FlexibleRDFHandler  # noqa: PLC0415
        handler = FlexibleRDFHandler(target)
        _state._rdf_provider = coco.register_root_target_states_provider(
            "flexible-graphrag/rdf/v1",
            handler,
        )
        logger.info("RDF: CocoIndex TargetStateProvider registered (FlexibleRDFHandler)")
    except Exception as exc:
        logger.warning(
            "RDF: could not register CocoIndex TargetStateProvider — "
            "deletions will not be auto-handled: %s", exc,
        )
    return _state._rdf_provider
