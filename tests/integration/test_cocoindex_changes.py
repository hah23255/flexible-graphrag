"""
Integration tests — CocoIndex-native change detection (add / modify / delete).

Naming: this file is deliberately NOT called ``test_cocoindex_incremental.py`` and
carries no ``incremental`` marker.  ``tests/conftest.py`` auto-applies
``pytest.mark.incremental`` to any test whose *nodeid* contains "incremental", and
``run_profile.py`` defaults to ``-m "integration and not incremental"`` — so an
``…_incremental.py`` filename would silently deselect every test here.  In this
repo the ``incremental`` marker means "needs the FG orchestrator"
(``ENABLE_INCREMENTAL_UPDATES=true``), which is the exact opposite of what these
tests require.

This is the CocoIndex counterpart to ``test_incremental.py``.  The two use
**completely different machinery** and are mutually exclusive:

``test_incremental.py``  (ENABLE_INCREMENTAL_UPDATES=true)
    FG orchestrator + watchdog detectors + Postgres ``document_state``;
    re-ingests changed files through ``hybrid_system`` (the default LI/LC
    pipeline).  Manual trigger: ``POST /api/sync/sync-now``.

``test_cocoindex_changes.py``  (PIPELINE_BACKEND=cocoindex)
    CocoIndex owns change processing — no ``document_state``, no FG
    orchestrator.  Registering a source with ``enable_sync=true`` gives the app
    a dedicated live stream (``app.update(live=True)``):

    * ``SOURCE_BACKEND=cocoindex`` — native localfs connector,
      ``walk_dir(recursive=True, live=True, rescan_interval=COCOINDEX_POLL_INTERVAL)``
      (OS file watcher plus a periodic full rescan as backup).
    * ``SOURCE_BACKEND=flexible`` — ``FlexibleMapView.watch()`` forwarding
      ``detector.get_changes()`` from the flexible filesystem detector.

    Deletes are reconciled by CocoIndex's LMDB reconciler, which calls
    ``delete_row(doc_id)`` on every declared target.
    Manual trigger: ``POST /api/cocoindex/sync-now``.

Setting ``ENABLE_INCREMENTAL_UPDATES=true`` alongside
``PIPELINE_BACKEND=cocoindex`` is an unsupported combination — the backend logs
a warning and refuses to start the FG orchestrator.  These tests skip in that
case rather than silently exercising a half-configured stack, so the invalid
combination cannot produce a green run.

Requires:
  - Backend started with ``PIPELINE_BACKEND=cocoindex`` and
    ``ENABLE_INCREMENTAL_UPDATES`` unset / false.
  - ``INTEGRATION_WATCH_DIR`` pointing at a dedicated folder (read from .env).
  - A low ``COCOINDEX_POLL_INTERVAL`` keeps the run short — the rescan cadence
    bounds how quickly a change is noticed.  10-15 s is a good test value.

Keep it fast: run with ``--pg none`` first.  Change detection, reconciliation and
``delete_row`` on the vector target are what these tests actually exercise; adding
a property graph pulls LLM KG extraction into every add/modify and dominates the
wall-clock.  Add ``--pg`` only when you specifically want to cover graph deletes.

Run via matrix — native CocoIndex localfs source (fast, vector only):
    uv run tests/integration/run_matrix.py --clean --pipeline cocoindex \\
        --source-backend cocoindex --pg none --vector qdrant \\
        --test-path tests/integration/test_cocoindex_changes.py

Run via matrix — flexible detector-backed source (fast, vector only):
    uv run tests/integration/run_matrix.py --clean --pipeline cocoindex \\
        --source-backend flexible --pg none --vector qdrant \\
        --test-path tests/integration/test_cocoindex_changes.py

Run directly (backend already running):
    pytest tests/integration/test_cocoindex_changes.py -m cocoindex -s

NOTE: do **not** pass ``--incremental`` — it sets
``ENABLE_INCREMENTAL_UPDATES=true`` and every test here will skip.
"""
from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.integration.api_client import APIClient
from tests.integration.env_helpers import normalized_integration_watch_dir

logger = logging.getLogger(__name__)

_watch = normalized_integration_watch_dir()
HAS_INTEGRATION_WATCH_DIR = bool(_watch)


def _pipeline_backend() -> str:
    return os.getenv("PIPELINE_BACKEND", "default").strip().lower()


def _source_backend() -> str:
    return os.getenv("SOURCE_BACKEND", "flexible").strip().lower()


# CocoIndex refresh cadence — the live stream's backup rescan interval.  A change
# is guaranteed to be noticed within roughly one interval, so the wait has to
# comfortably exceed it (plus parse + embed + any KG time for the new content).
COCO_POLL_INTERVAL = int(float(os.getenv("COCOINDEX_POLL_INTERVAL", "60")))

# SOURCE_BACKEND=flexible does NOT use CocoIndex's own walk_dir watcher — change
# detection goes through the flexible filesystem detector, which is watchdog-based
# (``FlexibleMapView.watch()`` forwards ``detector.get_changes()``).  The
# watchdog's own latency therefore applies on top of the CocoIndex rescan, so the
# wait needs the same allowance test_incremental.py makes for that path.
WATCHDOG_WAIT = int(os.getenv("INTEGRATION_SYNC_WAIT", "60"))


def _default_sync_wait() -> int:
    wait = max(120, COCO_POLL_INTERVAL * 2 + 45)
    if _source_backend() == "flexible":
        wait = max(wait, WATCHDOG_WAIT + COCO_POLL_INTERVAL + 45)
    return wait


COCO_SYNC_WAIT = int(os.getenv("COCOINDEX_SYNC_WAIT", str(_default_sync_wait())))

REGISTER_MAX_WAIT = int(os.getenv("INTEGRATION_REGISTER_MAX_WAIT", "600"))

# Distinct names/phrases from test_incremental.py so both suites can share one
# watch directory without cross-contaminating each other's assertions.
SEED_FILE_NAME   = "coco_seed_baseline.txt"
SEED_PHRASE      = "coco_seed_baseline_phrase_vt3"

ADD_FILE_NAME    = "coco_incremental_add.txt"
ADD_PHRASE       = "coco_add_phrase_wn9"

MODIFY_FILE_NAME = "coco_incremental_modify.txt"
MODIFY_PHRASE    = "coco_modify_baseline_phrase_hp4"
MODIFIED_PHRASE  = "coco_modify_updated_phrase_zx6"

DELETE_FILE_NAME = "coco_incremental_delete.txt"
DELETE_PHRASE    = "coco_delete_phrase_qm2"

_CLOUD_ONLY_SOURCES: frozenset[str] = frozenset({"s3", "azure_blob", "google_drive"})


# ──────────────────────────────────────────────────────────────────────────────
# Guards
# ──────────────────────────────────────────────────────────────────────────────

def _truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


def _skip_unless_coco_change_detection() -> None:
    """Skip unless the backend is configured for CocoIndex-driven change detection."""
    if _pipeline_backend() not in ("cocoindex", "coco"):
        pytest.skip(
            "PIPELINE_BACKEND != cocoindex — run via "
            "run_matrix.py --pipeline cocoindex"
        )
    if _truthy("ENABLE_INCREMENTAL_UPDATES"):
        pytest.skip(
            "ENABLE_INCREMENTAL_UPDATES=true is mutually exclusive with "
            "PIPELINE_BACKEND=cocoindex — the backend disables the FG orchestrator "
            "in that case. Unset it (do not pass --incremental) and re-run."
        )
    if not HAS_INTEGRATION_WATCH_DIR:
        pytest.skip("INTEGRATION_WATCH_DIR not set — nothing to watch")
    ds = os.getenv("DATA_SOURCE", "").strip().lower()
    if ds in _CLOUD_ONLY_SOURCES:
        pytest.skip(
            f"DATA_SOURCE={ds!r} watches a remote source — local file changes in "
            f"INTEGRATION_WATCH_DIR are never seen by the pipeline"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _write_file(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    logger.info("Written: %s (%d bytes)", path, len(content))


def _wait_for_file_gone(path: Path, timeout: float = 20.0, poll: float = 0.5) -> None:
    """Block until *path* is absent from both stat and the parent directory listing.

    On Windows ``unlink()`` can return before the deletion shows up in directory
    listings, so a rescan started immediately afterwards may still see the file
    and treat it as unchanged instead of deleted.
    """
    deadline = time.time() + timeout
    parent = path.parent
    name_lower = path.name.lower()
    while time.time() < deadline:
        if path.exists():
            time.sleep(poll)
            continue
        try:
            entries = {e.lower() for e in os.listdir(parent)}
        except OSError:
            entries = set()
        if name_lower not in entries:
            time.sleep(1.0)  # let the VFS cache converge before the rescan
            return
        time.sleep(poll)
    logger.warning(
        "_wait_for_file_gone: %s still visible in directory after %.1fs", path, timeout
    )


def _trigger_coco_sync(client: APIClient) -> None:
    """Nudge CocoIndex to reconcile now instead of waiting for the rescan."""
    try:
        client.cocoindex_sync_now()
    except Exception as exc:  # non-fatal — the live stream will still catch up
        logger.debug("cocoindex_sync_now failed (ignored): %s", exc)


def _wait_for_results(
    client: APIClient,
    query: str,
    expected_term: str,
    max_wait: int = COCO_SYNC_WAIT,
    poll: float = 5.0,
) -> bool:
    """Poll search until *expected_term* appears in a result, or *max_wait* expires."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        _trigger_coco_sync(client)
        results = client.search(query, top_k=5)
        logger.info(
            "_wait_for_results: %d result(s) for %r; snippets: %s",
            len(results.results),
            query,
            [r.get("content", "")[:60] for r in results.results],
        )
        for r in results.results:
            text = (r.get("content") or r.get("text") or "").lower()
            if expected_term.lower() in text:
                logger.info(
                    "Found %r after %.1fs", expected_term,
                    max_wait - (deadline - time.time()),
                )
                return True
        time.sleep(poll)
    return False


def _wait_for_no_results(
    client: APIClient,
    query: str,
    absent_term: str,
    max_wait: int = COCO_SYNC_WAIT,
    poll: float = 5.0,
) -> bool:
    """Poll search until *absent_term* is gone from every result."""
    deadline = time.time() + max_wait

    def _still_present() -> bool:
        results = client.search(query, top_k=5)
        return any(
            absent_term.lower() in (r.get("content") or r.get("text") or "").lower()
            for r in results.results
        )

    while time.time() < deadline:
        _trigger_coco_sync(client)
        if not _still_present():
            logger.info(
                "Term %r gone after %.1fs", absent_term,
                max_wait - (deadline - time.time()),
            )
            return True
        time.sleep(poll)
    # The final sync may have landed just as the deadline expired.
    try:
        if not _still_present():
            logger.info("Term %r gone on final post-deadline check", absent_term)
            return True
    except Exception:
        pass
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Session: register the watch directory with enable_sync (starts the live stream)
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def coco_watch_root(client: APIClient) -> Iterator[Path]:
    """Seed the watch dir, register it with ``enable_sync=true``, return the path.

    Registering a *directory* is what starts the CocoIndex live stream.  It also
    regression-tests the completion barrier: the per-file barrier in ``main.py``
    keys on filename, and a directory has no ``file_done`` event of its own, so
    an unfixed backend hangs here for the full live-ingest timeout instead of
    completing.
    """
    _skip_unless_coco_change_detection()

    watch_root = Path(str(_watch)).resolve()
    watch_root.mkdir(parents=True, exist_ok=True)

    # Pre-place the seed, modify, and delete files so they are indexed by the
    # registration pass.  Only the "add" test creates its file afterwards.
    _write_file(
        watch_root / SEED_FILE_NAME,
        f"CocoIndex baseline seed document. Unique phrase: {SEED_PHRASE}.\n"
        "Indexed by the registration pass of POST /api/ingest with enable_sync.\n",
    )
    _write_file(
        watch_root / MODIFY_FILE_NAME,
        f"Document for the CocoIndex modify test. Unique phrase: {MODIFY_PHRASE}.\n",
    )
    _write_file(
        watch_root / DELETE_FILE_NAME,
        f"Document for the CocoIndex delete test. Unique phrase: {DELETE_PHRASE}.\n",
    )

    logger.info(
        "[coco-inc] registering watch dir (source_backend=%s, poll_interval=%ds, "
        "sync_wait=%ds): %s",
        _source_backend(), COCO_POLL_INTERVAL, COCO_SYNC_WAIT, watch_root,
    )
    result = client.ingest_filesystem_paths_with_sync([str(watch_root)], enable_sync=True)
    assert result.processing_id, "No processing_id from /api/ingest (enable_sync)"
    status = client.wait_for_completion(result.processing_id, max_wait=REGISTER_MAX_WAIT)
    assert status.status == "completed", (
        f"CocoIndex watch registration did not complete: {status}. "
        "A directory registration that hangs at ~10% means the live-ingest "
        "completion barrier never matched a file_done event."
    )
    logger.info("[coco-inc] watch registration completed: %s", status.message)

    yield watch_root

    for name in (SEED_FILE_NAME, ADD_FILE_NAME, MODIFY_FILE_NAME, DELETE_FILE_NAME):
        (watch_root / name).unlink(missing_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.cocoindex
class TestCocoIndexChangeDetection:
    """add / modify / delete driven by CocoIndex's own live stream + reconciler."""

    def test_seed_is_indexed(self, client: APIClient, coco_watch_root: Path) -> None:
        """The pre-placed seed must be searchable after the registration pass."""
        assert _wait_for_results(client, SEED_PHRASE, SEED_PHRASE), (
            f"Seed phrase {SEED_PHRASE!r} not found within {COCO_SYNC_WAIT}s. "
            "The registration ingest reported success but nothing was indexed."
        )

    @pytest.mark.slow
    def test_add_file_is_indexed(self, client: APIClient, coco_watch_root: Path) -> None:
        """A new file in the watch dir must be picked up by the live stream."""
        target = coco_watch_root / ADD_FILE_NAME
        _write_file(
            target,
            f"Newly added CocoIndex document. Unique phrase: {ADD_PHRASE}.\n"
            "Detected by the live stream / rescan, not by a manual ingest call.\n",
        )
        try:
            assert _wait_for_results(client, ADD_PHRASE, ADD_PHRASE), (
                f"Added file phrase {ADD_PHRASE!r} not indexed within "
                f"{COCO_SYNC_WAIT}s (source_backend={_source_backend()}, "
                f"COCOINDEX_POLL_INTERVAL={COCO_POLL_INTERVAL}s, "
                f"watchdog allowance={WATCHDOG_WAIT}s). "
                "Check that the app has a live stream (enable_sync); raise "
                "COCOINDEX_SYNC_WAIT or lower COCOINDEX_POLL_INTERVAL if the "
                "detector is simply slower than the wait."
            )
        finally:
            target.unlink(missing_ok=True)

    @pytest.mark.slow
    def test_modify_file_updates_index(
        self, client: APIClient, coco_watch_root: Path
    ) -> None:
        """Overwriting an indexed file must replace its content in the index.

        Both halves matter: the new phrase has to appear *and* the old one has to
        disappear.  Only checking the new phrase would pass even if the modify
        left the previous version's chunks behind as duplicates.
        """
        target = coco_watch_root / MODIFY_FILE_NAME
        assert _wait_for_results(client, MODIFY_PHRASE, MODIFY_PHRASE), (
            f"Baseline phrase {MODIFY_PHRASE!r} was never indexed — cannot test modify"
        )

        _write_file(
            target,
            f"Rewritten CocoIndex document. Unique phrase: {MODIFIED_PHRASE}.\n"
            "The previous phrase must no longer be retrievable.\n",
        )
        assert _wait_for_results(client, MODIFIED_PHRASE, MODIFIED_PHRASE), (
            f"Updated phrase {MODIFIED_PHRASE!r} not indexed within {COCO_SYNC_WAIT}s"
        )
        assert _wait_for_no_results(client, MODIFY_PHRASE, MODIFY_PHRASE), (
            f"Old phrase {MODIFY_PHRASE!r} still present after modify — the "
            "reconciler left the previous version's rows behind"
        )

    @pytest.mark.slow
    def test_delete_file_removes_from_index(
        self, client: APIClient, coco_watch_root: Path
    ) -> None:
        """Deleting a file must remove its rows from every declared target."""
        target = coco_watch_root / DELETE_FILE_NAME
        assert _wait_for_results(client, DELETE_PHRASE, DELETE_PHRASE), (
            f"Baseline phrase {DELETE_PHRASE!r} was never indexed — cannot test delete"
        )

        target.unlink(missing_ok=True)
        _wait_for_file_gone(target)

        assert _wait_for_no_results(client, DELETE_PHRASE, DELETE_PHRASE), (
            f"Phrase {DELETE_PHRASE!r} still retrievable after the file was "
            f"deleted — CocoIndex's reconciler did not call delete_row for it"
        )


@pytest.mark.cocoindex
def test_coco_change_detection_config_reported(client: APIClient) -> None:
    """Informational — record the configuration this run exercised."""
    _skip_unless_coco_change_detection()
    logger.info(
        "[coco-inc] pipeline=%s  source_backend=%s  data_source=%s  "
        "poll_interval=%ds  sync_wait=%ds  vec=%s  pg=%s  search=%s",
        _pipeline_backend(), _source_backend(),
        os.getenv("DATA_SOURCE", "(unset)"),
        COCO_POLL_INTERVAL, COCO_SYNC_WAIT,
        os.getenv("VECTOR_DB", "none"),
        os.getenv("PG_GRAPH_DB", "none"),
        os.getenv("SEARCH_DB", "none"),
    )
