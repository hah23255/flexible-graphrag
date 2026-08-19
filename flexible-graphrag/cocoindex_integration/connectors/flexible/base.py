"""Base classes and shared helpers for the ``flexible`` connector family.

Two small pieces of shared machinery live here:

``FlexibleConnector``
    Thin base establishing the *convention* every flexible target follows:
    ``setup() / declare_row() / finalize() / teardown() / delete_row()``.
    It only holds ``app_config`` — each target keeps its own store-specific
    write logic (the write mechanisms differ too much to share safely).

``FlexibleReconcileHandler``
    Generic CocoIndex ``TargetHandler`` that removes the real duplication:
    the reconcile() change-detection logic and the _apply_actions() batch loop
    were byte-for-byte identical across the four targets.  Subclasses only
    supply a handful of tiny hooks (fingerprint, action factories, declare).

Plus the embedding-dimension resolvers shared by the pipeline and the vector
target (``_resolve_cocoindex_dim`` / ``_resolve_main_dim``) and the backend
env-var readers.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import threading
from typing import Any, Dict, Iterable, Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Target-sink flush barrier
#
# Flexible TargetStateProviders declare state during process_file, then CocoIndex
# calls reconcile() → _apply_actions() *after* process_file emits file_done.
# REST ingest used to mark the job complete on file_done, so hybrid search raced
# ahead of the actual vector/search/graph writes.  These helpers let main.py wait
# until every queued sink action has finished.
# ─────────────────────────────────────────────────────────────────────────────

_pending_counts: Dict[str, int] = {}
_pending_lock = threading.Lock()
_pending_empty: Optional[asyncio.Event] = None
_pending_loop: Optional[asyncio.AbstractEventLoop] = None


def _ensure_pending_empty_event() -> asyncio.Event:
    """Return the process-wide idle Event bound to the current running loop."""
    global _pending_empty, _pending_loop
    loop = asyncio.get_running_loop()
    if _pending_empty is None or _pending_loop is not loop:
        _pending_empty = asyncio.Event()
        _pending_loop = loop
        with _pending_lock:
            if not _pending_counts:
                _pending_empty.set()
            else:
                _pending_empty.clear()
    return _pending_empty


def note_target_pending(doc_id: str) -> None:
    """Record that a reconcile action was queued for *doc_id* (refcount +1)."""
    key = str(doc_id)
    with _pending_lock:
        _pending_counts[key] = _pending_counts.get(key, 0) + 1
        idle = _pending_empty
    if idle is not None:
        idle.clear()


def note_target_flushed(doc_id: str) -> None:
    """Record that a queued sink action for *doc_id* finished (refcount -1)."""
    key = str(doc_id)
    with _pending_lock:
        n = _pending_counts.get(key, 0) - 1
        if n <= 0:
            _pending_counts.pop(key, None)
        else:
            _pending_counts[key] = n
        empty = not _pending_counts
        idle = _pending_empty
    if empty and idle is not None:
        idle.set()


async def wait_targets_flushed(timeout: float = 120.0) -> None:
    """Await until every pending flexible-target sink action has finished."""
    idle = _ensure_pending_empty_event()
    with _pending_lock:
        if not _pending_counts:
            idle.set()
            return
    try:
        await asyncio.wait_for(idle.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        with _pending_lock:
            leftover = dict(_pending_counts)
        logger.warning(
            "wait_targets_flushed: timed out after %.0fs with pending=%s",
            timeout, leftover,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Backend env-var readers
# ─────────────────────────────────────────────────────────────────────────────

def parse_entity_props(raw: Any) -> Dict[str, Any]:
    """Decode a row's ``*_properties_json`` into a property dict.

    Shared by the property-graph and RDF connectors: both receive
    ``KGTripleRow`` and both need the ontology-declared entity properties the
    extractor put on ``KGEntity.properties``.

    Only primitive values survive.  Property-graph stores reject dict/list
    values ("Property values can only be of primitive types or arrays
    thereof"), and these come from LLM output, so a nested object is entirely
    possible — it is stringified rather than dropped.

    Never raises: malformed JSON means no extra properties, not a failed ingest.
    """
    if not raw or raw == "{}":
        return {}
    try:
        import json as _json
        data = _json.loads(raw)
    except (ValueError, TypeError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, Any] = {}
    for key, value in data.items():
        if not key:
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            out[str(key)] = value
        elif isinstance(value, (list, tuple)) and all(
            isinstance(v, (str, int, float, bool)) for v in value
        ):
            out[str(key)] = list(value)
        else:
            out[str(key)] = str(value)
    return out


def vector_backend() -> str:
    """Read VECTOR_BACKEND from the environment (default: ``"llamaindex"``)."""
    return os.environ.get("VECTOR_BACKEND", "llamaindex").lower()


def graph_backend() -> str:
    """Read GRAPH_BACKEND from the environment (default: ``"llamaindex"``)."""
    return os.environ.get("GRAPH_BACKEND", "llamaindex").lower()


# ─────────────────────────────────────────────────────────────────────────────
# Embedding-dimension resolution
# ─────────────────────────────────────────────────────────────────────────────

def resolve_cocoindex_dim() -> int:
    """Return the embedding dimension the CocoIndex pipeline will produce.

    Resolution order:
    1. ``COCOINDEX_EMBEDDING_DIMENSION`` (explicit override for CocoIndex)
    2. ``{RESOLVED_KIND}_EMBEDDING_DIMENSION`` (e.g. ``OPENAI_EMBEDDING_DIMENSION``)
    3. ``EMBEDDING_DIMENSION`` (generic fallback)
    4. Model-name inference via the embedding factory

    When ``COCOINDEX_EMBEDDING_KIND`` is not set CocoIndex falls back to the
    main ``EMBEDDING_KIND`` (e.g. ``openai``).  In that case the relevant
    dimension env-var is ``OPENAI_EMBEDDING_DIMENSION``, not the CocoIndex-only
    ``COCOINDEX_EMBEDDING_DIMENSION``.  Steps 2–3 pick that up so that the
    reported dimension matches the actual embeddings CocoIndex will write.
    """
    from llamaindex.llm.embedding_factory import get_embedding_dimension  # noqa: PLC0415
    coco_kind = os.getenv("COCOINDEX_EMBEDDING_KIND", "").lower()
    main_kind = os.getenv("EMBEDDING_KIND", "openai").lower()
    resolved_kind = coco_kind or main_kind
    model = os.getenv("COCOINDEX_EMBEDDING_MODEL") or os.getenv("EMBEDDING_MODEL") or None
    explicit_dim = (
        int(os.getenv("COCOINDEX_EMBEDDING_DIMENSION", "0") or "0")
        or int(os.getenv(f"{resolved_kind.upper()}_EMBEDDING_DIMENSION", "0") or "0")
        or int(os.getenv("EMBEDDING_DIMENSION", "0") or "0")
    ) or None
    return get_embedding_dimension(resolved_kind, model, explicit_dim)


def resolve_main_dim() -> int:
    """Return the embedding dimension configured for the main flexible-graphrag pipeline.

    Resolution order:
    1. ``{KIND}_EMBEDDING_DIMENSION`` (per-kind override)
    2. ``EMBEDDING_DIMENSION`` (generic override)
    3. Model-name inference per embedding kind
    """
    from llamaindex.llm.embedding_factory import get_embedding_dimension  # noqa: PLC0415
    kind = os.getenv("EMBEDDING_KIND", "openai").lower()
    model = os.getenv(f"{kind.upper()}_EMBEDDING_MODEL") or os.getenv("EMBEDDING_MODEL") or None
    kind_dim = int(os.getenv(f"{kind.upper()}_EMBEDDING_DIMENSION", "0") or "0")
    generic_dim = int(os.getenv("EMBEDDING_DIMENSION", "0") or "0")
    explicit_dim = kind_dim or generic_dim or None
    return get_embedding_dimension(kind, model, explicit_dim)


# Backwards-compatible aliases (the leading-underscore names used elsewhere).
_vector_backend = vector_backend
_graph_backend = graph_backend
_resolve_cocoindex_dim = resolve_cocoindex_dim
_resolve_main_dim = resolve_main_dim


# ─────────────────────────────────────────────────────────────────────────────
# Fingerprint helper
# ─────────────────────────────────────────────────────────────────────────────

def content_fingerprint(chunks: Iterable[bytes]) -> bytes:
    """Stable SHA-256 over an ordered iterable of byte chunks.

    Callers are responsible for ordering (e.g. sorting rows by chunk_index) and
    for encoding each unit of content before passing it in.  Centralising the
    hasher keeps every target's change-detection consistent.
    """
    h = hashlib.sha256()
    for chunk in chunks:
        h.update(chunk)
    return h.digest()


# ─────────────────────────────────────────────────────────────────────────────
# FlexibleConnector — thin convention base for LI/LC-wrapping targets
# ─────────────────────────────────────────────────────────────────────────────

class FlexibleConnector:
    """Base for flexible-graphrag CocoIndex *targets*.

    Establishes the shared lifecycle convention (method names + data types) that
    the CocoIndex pipeline relies on.  Deliberately thin: each concrete target
    (``FlexibleVector``, ``FlexiblePropertyGraph``, ``FlexibleSearch``,
    ``FlexibleRDFGraph``) keeps its own store-specific write logic because the
    underlying write mechanisms differ too much to unify without risk.

    Lifecycle (all async, implemented by subclasses):
        setup()        — build/attach the underlying store adapter (idempotent)
        declare_row()  — buffer one row for the next flush
        finalize()     — flush all buffered rows
        teardown()     — release the adapter
        delete_row()   — remove all data for a doc_id (CocoIndex stale-row path)
    """

    def __init__(self, app_config: Any) -> None:
        self.app_config = app_config


# ─────────────────────────────────────────────────────────────────────────────
# FlexibleReconcileHandler — generic CocoIndex TargetHandler
# ─────────────────────────────────────────────────────────────────────────────

class FlexibleReconcileHandler:
    """Generic CocoIndex ``TargetHandler`` shared by all flexible targets.

    Register once with ``coco.register_root_target_states_provider()``.
    CocoIndex then:
    - Calls ``reconcile()`` for every *declared* target state (upsert path)
    - Calls ``reconcile()`` with ``NON_EXISTENCE`` for states that were declared
      in a previous cycle but are now absent (delete path — no tracking file needed)

    The reconcile output is batched into ``_apply_actions()``, which performs the
    actual I/O via the shared target adapter.

    Subclasses supply these hooks (everything else is identical across targets):
        label                 — noun used in log messages (e.g. "vectors")
        _fingerprint(desired) — content SHA-256 for change detection
        _make_delete_action(key)
        _make_upsert_action(desired, delete_first)
        _make_tracking_record(fp)
        _action_is_delete(action)
        _action_size(action)  — short human string for logging
        _declare_upsert(action) — declare the action's rows/chunks on the target
    """

    label: str = "data"

    def __init__(self, target: Any) -> None:
        self._target = target
        import cocoindex as _coco  # noqa: PLC0415
        self._sink: Any = _coco.TargetActionSink.from_async_fn(self._apply_actions)

    # ── hooks (subclasses override) ──────────────────────────────────────────
    def _fingerprint(self, desired: Any) -> bytes:  # pragma: no cover - abstract
        raise NotImplementedError

    def _make_delete_action(self, key: str) -> Any:  # pragma: no cover - abstract
        raise NotImplementedError

    def _make_upsert_action(self, desired: Any, delete_first: bool) -> Any:  # pragma: no cover
        raise NotImplementedError

    def _make_tracking_record(self, fp: bytes) -> Any:  # pragma: no cover - abstract
        raise NotImplementedError

    def _action_is_delete(self, action: Any) -> bool:  # pragma: no cover - abstract
        raise NotImplementedError

    def _action_size(self, action: Any) -> str:
        return ""

    async def _declare_upsert(self, action: Any) -> None:  # pragma: no cover - abstract
        raise NotImplementedError

    # ── shared machinery ─────────────────────────────────────────────────────
    async def _apply_actions(self, context_provider: Any, actions: Any) -> None:
        """Write/delete all actions for one batch to the target store."""
        cls = type(self).__name__
        action_list = list(actions)
        try:
            try:
                await self._target.setup()
            except Exception as exc:
                logger.error("%s: adapter setup failed: %s", cls, exc)
                return

            for action in action_list:
                doc_id = action.doc_id
                if self._action_is_delete(action):
                    logger.info("%s: deleting %s for doc '%s'", cls, self.label, doc_id)
                    await self._target.delete_row(doc_id)
                else:
                    if getattr(action, "delete_first", False):
                        logger.info(
                            "%s: replacing %s for doc '%s' (%s)",
                            cls, self.label, doc_id, self._action_size(action),
                        )
                        # Modified doc — purge stale data first so shrinking documents
                        # don't leave orphan rows/nodes behind.
                        await self._target.delete_row(doc_id)
                    else:
                        logger.info(
                            "%s: inserting %s for doc '%s' (%s)",
                            cls, self.label, doc_id, self._action_size(action),
                        )
                    await self._declare_upsert(action)

            await self._target.finalize()
        finally:
            # Always release the flush barrier — even when setup/write fails —
            # so REST ingest does not hang waiting for sinks that will never run.
            for action in action_list:
                note_target_flushed(str(action.doc_id))

    def reconcile(
        self,
        key: Any,
        desired: Any,
        prev_records: Any,
        prev_may_be_missing: bool,
        /,
    ) -> Any:
        """Determine what action (if any) is needed to reconcile desired state."""
        import cocoindex as _coco  # noqa: PLC0415

        if _coco.is_non_existence(desired):
            # Source file was deleted — nothing in prev_records means already gone.
            if not prev_records and not prev_may_be_missing:
                return None
            # Delete path is not preceded by declare_target_state in process_file.
            note_target_pending(str(key))
            return _coco.TargetReconcileOutput(
                action=self._make_delete_action(str(key)),
                sink=self._sink,
                tracking_record=_coco.NON_EXISTENCE,
            )

        fp = self._fingerprint(desired)

        # CocoIndex deserializes tracking records from LMDB as plain dicts; handle both.
        def _get_fp(r: Any) -> bytes:
            return r["fingerprint"] if isinstance(r, dict) else r.fingerprint

        # Skip if content hasn't changed (fingerprints match all previous records).
        # process_file already called note_target_pending() at declare time — release
        # that refcount here so wait_targets_flushed does not hang on a no-op.
        if not prev_may_be_missing and all(_get_fp(r) == fp for r in prev_records):
            note_target_flushed(str(key))
            return None

        # delete_first=True when updating an existing doc (prev_records present).
        # First ingest (prev_records empty or prev_may_be_missing) → insert only.
        # Pending refcount was already taken at declare_target_state time.
        is_update = bool(prev_records) and not prev_may_be_missing
        return _coco.TargetReconcileOutput(
            action=self._make_upsert_action(desired, is_update),
            sink=self._sink,
            tracking_record=self._make_tracking_record(fp),
        )
