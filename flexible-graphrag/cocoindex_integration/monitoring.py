"""Optional multi-stage Postgres monitor for the CocoIndex pipeline.

This is **observability only** — it records per-file / per-stage rows in a
``cocoindex_ingest_log`` table and per-run summary rows in a
``cocoindex_run_log`` table so operators can watch ingest progress in
pgAdmin (or any SQL client) across a run.  It is *never* used for change
detection (that stays in CocoIndex's LMDB memo layer + the incremental
``document_state`` table).

Enable with ``COCOINDEX_ENABLE_PG_MONITORING=true`` in ``.env``.  The monitor
reuses the incremental-updates Postgres database (``POSTGRES_INCREMENTAL_URL``)
via its own small ``asyncpg`` pool.  When the flag is off, ``get_monitor()``
returns ``None`` and the whole feature is a no-op with zero overhead.

Tables
------
cocoindex_run_log
    One row per update cycle: adds / deletes / unchanged / reprocesses / errors,
    elapsed time, and the trigger (startup / ui / poll / live / manual).

    **These are DOCUMENT counts.**  CocoIndex's own ``UpdateStats.total`` sums
    every component it ran — the per-file worker plus ``parse_document`` plus
    ``_embed_chunks_cached`` plus one ``extract_kg_*`` per *chunk* — so a single
    5-chunk file could report "adds=8", which is neither a file count nor a
    chunk count.  ``bridge._document_level_counters()`` filters to the per-file
    components before anything is written here, and the full per-component
    breakdown is kept in the ``note`` column when you need it.

cocoindex_ingest_log
    Per-file rows, written from the pipeline's progress hook (so memo hits and
    pure deletes produce nothing).  Volume is controlled by
    ``COCOINDEX_MONITOR_DETAIL``:

    ``summary`` (default)
        ONE row per file — the terminal event, with status
        (completed / skipped / failed) and detail.
    ``stages``
        A row per stage transition (~13 per file).  Use when debugging where a
        file stalls; it swamps the table on a real corpus.

    Exactly one writer: the bridge's monitor hook.  Callers pass their own
    ``progress_cb`` for UI updates and must not log here as well.

Stages (as emitted by the pipeline progress hook):
    downloading → downloaded → parsing → parsed → chunked → embedded →
    kg_extracting → kg_extracted → vector_indexing → graph_indexing →
    search_indexing → rdf_indexing → indexing_complete → done

The design mirrors ``StateManager``: a lazy ``asyncpg`` pool, an idempotent
``CREATE TABLE IF NOT EXISTS``, and best-effort inserts that never raise into
the pipeline.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

try:
    import asyncpg  # type: ignore[import-untyped]
    _ASYNCPG_AVAILABLE = True
except ImportError:  # pragma: no cover - asyncpg is a core dep, guard anyway
    asyncpg = None  # type: ignore[assignment]
    _ASYNCPG_AVAILABLE = False


def is_enabled() -> bool:
    """Return True if ``COCOINDEX_ENABLE_PG_MONITORING`` is truthy in the env."""
    return os.getenv("COCOINDEX_ENABLE_PG_MONITORING", "false").lower() in (
        "1", "true", "yes", "on",
    )


#: Stage names that always earn a row, even at ``summary`` detail.  These are the
#: terminal / decision points a human actually reads.
_SUMMARY_STAGES = frozenset({"done", "failed", "skipped", "completed"})


def detail_level() -> str:
    """Return the ``cocoindex_ingest_log`` verbosity: ``"summary"`` or ``"stages"``.

    ``summary`` (default) writes ONE row per file — the terminal ``file_done``
    event with its status and detail.  ``stages`` writes a row for every stage
    transition (parsing → parsed → chunked → embedded → …), which is ~13 rows
    per file and swamps the table on any real corpus.

    Set ``COCOINDEX_MONITOR_DETAIL=stages`` when debugging where a file stalls.
    """
    value = os.getenv("COCOINDEX_MONITOR_DETAIL", "summary").strip().lower()
    return "stages" if value in ("stages", "all", "full", "verbose") else "summary"


def should_log_stage(stage: str, is_terminal: bool) -> bool:
    """True when a per-file event should produce a ``cocoindex_ingest_log`` row."""
    if detail_level() == "stages":
        return True
    return is_terminal or (stage or "").lower() in _SUMMARY_STAGES


def _extract_total(stats: Any) -> dict:
    """Pull scalar counters out of a CocoIndex UpdateStats (or ComponentStats).

    Also accepts a plain dict with the same keys so callers that aggregate
    counters themselves (e.g. CocoIndexBridge multi-app loops) can pass
    pre-computed totals without constructing a fake stats object.

    Returns a dict with keys: adds, deletes, unchanged, reprocesses, errors,
    in_progress, finished.  All values default to 0 so callers never KeyError.
    """
    _zero = dict(adds=0, deletes=0, unchanged=0, reprocesses=0,
                 errors=0, in_progress=0, finished=0)
    if stats is None:
        return _zero
    if isinstance(stats, dict):
        return {k: int(stats.get(k, 0)) for k in _zero}
    # UpdateStats has a .total (ComponentStats); ComponentStats is used directly.
    cs = getattr(stats, "total", stats)
    return {
        "adds":        getattr(cs, "num_adds", 0),
        "deletes":     getattr(cs, "num_deletes", 0),
        "unchanged":   getattr(cs, "num_unchanged", 0),
        "reprocesses": getattr(cs, "num_reprocesses", 0),
        "errors":      getattr(cs, "num_errors", 0),
        "in_progress": getattr(cs, "num_in_progress", 0),
        "finished":    getattr(cs, "num_finished", 0),
    }


class CocoIngestMonitor:
    """Best-effort per-stage / per-run ingest logger backed by Postgres.

    All methods swallow errors (logging at DEBUG) so a monitoring failure can
    never break the ingest pipeline.
    """

    def __init__(self, postgres_url: str) -> None:
        self.postgres_url = postgres_url
        self.pool: Optional["asyncpg.Pool"] = None  # type: ignore[name-defined]

    async def initialize(self) -> bool:
        """Create the pool + tables.  Returns True on success, False otherwise."""
        if not _ASYNCPG_AVAILABLE:
            logger.debug("CocoIngestMonitor: asyncpg unavailable — disabled")
            return False
        try:
            self.pool = await asyncpg.create_pool(self.postgres_url, min_size=1, max_size=2)
            await self._create_schema()
            logger.info("CocoIngestMonitor: enabled (cocoindex_ingest_log + cocoindex_run_log)")
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "CocoIngestMonitor: could not initialize (%s) — monitoring disabled",
                exc,
            )
            self.pool = None
            return False

    async def _create_schema(self) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            # Per-file / per-stage event log (fine-grained, driven by progress hook)
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cocoindex_ingest_log (
                    id          BIGSERIAL PRIMARY KEY,
                    run_id      TEXT NOT NULL,
                    trigger     TEXT,
                    file_name   TEXT,
                    file_path   TEXT,
                    stage       TEXT NOT NULL,
                    status      TEXT,
                    detail      TEXT,
                    logged_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cocoindex_ingest_log_run
                ON cocoindex_ingest_log(run_id, logged_at)
                """
            )
            # Per-run summary log (CocoIndex native UpdateStats counters)
            # Captures EVERY update cycle — including live-mode watch-folder changes.
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cocoindex_run_log (
                    id              BIGSERIAL PRIMARY KEY,
                    run_id          TEXT NOT NULL,
                    trigger         TEXT NOT NULL,
                    adds            INT  NOT NULL DEFAULT 0,
                    deletes         INT  NOT NULL DEFAULT 0,
                    unchanged       INT  NOT NULL DEFAULT 0,
                    reprocesses     INT  NOT NULL DEFAULT 0,
                    errors          INT  NOT NULL DEFAULT 0,
                    elapsed_s       FLOAT,
                    update_count    INT,
                    note            TEXT,
                    logged_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cocoindex_run_log_logged
                ON cocoindex_run_log(logged_at DESC)
                """
            )
            # Add trigger column to ingest_log if it was created by an older version
            await conn.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='cocoindex_ingest_log'
                          AND column_name='trigger'
                    ) THEN
                        ALTER TABLE cocoindex_ingest_log ADD COLUMN trigger TEXT;
                    END IF;
                END $$;
                """
            )

    async def log(
        self,
        run_id: str,
        stage: str,
        *,
        trigger: Optional[str] = None,
        file_name: Optional[str] = None,
        file_path: Optional[str] = None,
        status: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> None:
        """Insert one per-stage row (best-effort; never raises)."""
        if self.pool is None:
            return
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO cocoindex_ingest_log
                        (run_id, trigger, file_name, file_path, stage, status, detail)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    run_id, trigger, file_name, file_path, stage, status, detail,
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug("CocoIngestMonitor.log error (ignored): %s", exc)

    async def log_run_summary(
        self,
        run_id: str,
        trigger: str,
        stats: Any,
        elapsed_s: Optional[float] = None,
        update_count: Optional[int] = None,
        note: Optional[str] = None,
    ) -> None:
        """Insert one per-run summary row from a CocoIndex UpdateStats object.

        Called after every update cycle — one-shot updates, poll cycles, and
        live-mode incremental snapshots (watch-folder adds/deletes).  Rows with
        adds=0, deletes=0, unchanged=0 are skipped to avoid flooding the table
        with empty-cycle heartbeats.
        """
        if self.pool is None:
            return
        try:
            c = _extract_total(stats)
            # Skip silent no-op cycles (nothing processed at all)
            if c["adds"] == 0 and c["deletes"] == 0 and c["unchanged"] == 0 and c["reprocesses"] == 0 and c["errors"] == 0:
                return
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO cocoindex_run_log
                        (run_id, trigger, adds, deletes, unchanged,
                         reprocesses, errors, elapsed_s, update_count, note)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    """,
                    run_id, trigger,
                    c["adds"], c["deletes"], c["unchanged"],
                    c["reprocesses"], c["errors"],
                    round(elapsed_s, 3) if elapsed_s is not None else None,
                    update_count,
                    note,
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug("CocoIngestMonitor.log_run_summary error (ignored): %s", exc)

    async def close(self) -> None:
        if self.pool is not None:
            try:
                await self.pool.close()
            except Exception:  # noqa: BLE001
                pass
            self.pool = None


async def get_monitor() -> Optional[CocoIngestMonitor]:
    """Return an initialized monitor when enabled + reachable, else ``None``.

    Reads ``POSTGRES_INCREMENTAL_URL`` for the connection (same database the
    incremental update system uses).  Returns ``None`` when the feature flag is
    off, the URL is missing, or the DB is unreachable.
    """
    if not is_enabled():
        return None
    postgres_url = os.getenv("POSTGRES_INCREMENTAL_URL")
    if not postgres_url:
        logger.warning(
            "COCOINDEX_ENABLE_PG_MONITORING=true but POSTGRES_INCREMENTAL_URL "
            "is not set — monitoring disabled"
        )
        return None
    monitor = CocoIngestMonitor(postgres_url)
    if await monitor.initialize():
        return monitor
    return None
