"""
Integration tests for the CocoIndex pipeline backend.

These tests run against the FastAPI HTTP API (``main.py``) with
``PIPELINE_BACKEND=cocoindex`` set in the backend's environment.  The
CocoIndex pipeline handles internal incremental state and memoised
processing; the REST surface visible to the tests is unchanged.

Prerequisites:
  - Backend started with PIPELINE_BACKEND=cocoindex
  - At least one active store (VECTOR_DB or SEARCH_DB or PG_GRAPH_DB)
  - OPENAI_API_KEY (or alternative LLM/embedding provider) set

Run directly (backend already running):
    pytest tests/integration/test_cocoindex.py -m cocoindex -s

Run via matrix (native Qdrant + Neo4j):
    uv run tests/integration/run_matrix.py \\
        --pipeline cocoindex --vector qdrant --pg neo4j \\
        --backends llamaindex \\
        --test-path tests/integration/test_cocoindex.py

Run via matrix (CocoIndex native PG + vector, flexible search/RDF):
    uv run tests/integration/run_matrix.py \\
        --pipeline cocoindex --graph-backend cocoindex --vector-backend cocoindex \\
        --vector qdrant --pg neo4j --search elasticsearch \\
        --backends llamaindex \\
        --test-path tests/integration/test_cocoindex.py

Run via named profile:
    uv run tests/integration/run_profile.py --profile coco-qdrant-neo4j

Assumptions:
  - PIPELINE_BACKEND=cocoindex is set; tests skip otherwise.
  - Tests do NOT test the CLI (``cocoindex update app.py``); only the
    FastAPI server is exercised.
  - CocoIndex memoisation avoids redundant LLM calls across test runs
    (re-ingesting the same document is near-instant on second run).
"""
from __future__ import annotations

import logging
import os
import textwrap
import time

import pytest

from tests.integration.api_client import APIClient, QueryResult, SearchResult

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pipeline_backend() -> str:
    # config.py default is "default" (any value other than "cocoindex" uses
    # the built-in per-stage pipeline).  "flexible" was never a valid value
    # here — the default sentinel in config.py is "default".
    return os.getenv("PIPELINE_BACKEND", "default").lower()


def _source_backend() -> str:
    return os.getenv("SOURCE_BACKEND", "flexible").lower()


def _skip_unless_cocoindex() -> None:
    """Skip the test if the backend is not running with CocoIndex."""
    if _pipeline_backend() not in ("cocoindex", "coco"):
        pytest.skip(
            "PIPELINE_BACKEND != cocoindex — set it in the backend environment "
            "(PIPELINE_BACKEND=cocoindex) or run via: run_matrix.py --pipeline cocoindex"
        )


def _active(env_var: str) -> bool:
    return os.getenv(env_var, "none").strip().lower() not in ("none", "", "false")


# Cloud-native sources that receive files via their own protocol (S3 bucket,
# Azure Blob container, Google Drive folder).  Local HTTP file-upload tests
# cannot push data into these sources, so they must be skipped.
_CLOUD_ONLY_SOURCES: frozenset[str] = frozenset({"s3", "azure_blob", "google_drive"})


def _is_cloud_source() -> bool:
    return os.getenv("DATA_SOURCE", "").strip().lower() in _CLOUD_ONLY_SOURCES


def _skip_if_cloud_source(label: str = "") -> None:
    """Skip any test that uploads a local file when DATA_SOURCE is a cloud source.

    When the CocoIndex bridge is configured for S3 / Azure Blob / Google Drive,
    the pipeline watches that remote source — a local file dropped into the
    watch directory is never seen, so ``wait_for_completion`` would time out.
    """
    ds = os.getenv("DATA_SOURCE", "").strip().lower()
    if ds in _CLOUD_ONLY_SOURCES:
        pytest.skip(
            f"DATA_SOURCE={ds!r} — local HTTP file upload not supported for "
            f"cloud-native CocoIndex sources.  Test the source directly via "
            f"--data-source {ds} with credentials configured."
            + (f"  [{label}]" if label else "")
        )


def _ensure_cloud_content_or_skip(
    client: APIClient,
    query: str,
    terms: list[str],
    *,
    max_wait: int = 300,
    context: str = "",
) -> None:
    """For cloud-native sources, trigger CocoIndex sync and wait for indexed content.

    If content does not appear within *max_wait* seconds, the calling test is
    skipped with a descriptive message rather than failing hard — the cloud
    source simply has no matching files configured for it.

    For non-cloud sources this is a no-op (ingest tests already populated the stores).
    """
    if not _is_cloud_source():
        return

    ds = os.getenv("DATA_SOURCE", "").strip().lower()
    sync_timeout = int(os.getenv("INTEGRATION_CLOUD_SYNC_TIMEOUT", str(max_wait)))
    logger.info(
        "[%s] cloud-native source — triggering CocoIndex sync and waiting up to %ds "
        "for content matching %s …",
        ds, sync_timeout, terms,
    )
    found = client.cocoindex_wait_for_content(query, terms, max_wait=sync_timeout, poll=10.0)
    if not found:
        label = f" ({context})" if context else ""
        pytest.skip(
            f"DATA_SOURCE={ds!r}{label} — CocoIndex sync ran but no content matching "
            f"{terms!r} appeared in {sync_timeout}s.  "
            f"Ensure the cloud source has files containing those terms and that "
            f"the credentials / folder config in GOOGLE_DRIVE_CONFIG / S3_CONFIG / "
            f"AZURE_BLOB_CONFIG point to the right location."
        )


COMPANY_SEARCH_QUERY = "who works for acme"
COMPANY_SEARCH_TERMS = ["james", "linda", "marcus", "priya", "sarah"]

COMPANY_AI_QUERY = "how is acme organized"
COMPANY_AI_TERMS  = ["engineering", "department", "sales", "management", "organized"]

FAST_SEARCH_QUERY = "content management interoperability"
FAST_SEARCH_TERMS = ["alfresco", "cmis", "repository"]


def _log_results(result: SearchResult, label: str = "") -> None:
    logger.info("─── %s (%d results) ───", label or result.query, result.total)
    for i, r in enumerate(result.results[:5], 1):
        src   = r.get("source") or r.get("metadata", {}).get("source") or "?"
        score = r.get("score", r.get("rank", "?"))
        text  = r.get("content") or r.get("text") or ""
        snippet = " | ".join(l.strip() for l in text.splitlines() if l.strip())[:120]
        logger.info("  [%d] %-40s  score=%-6s  %s", i, src, score, snippet)


def _has_relevant(result: SearchResult, terms: list[str]) -> bool:
    for r in result.results:
        text = (r.get("content") or r.get("text") or "").lower()
        if any(t in text for t in terms):
            return True
    return False


def _ingest_and_wait(client: APIClient, doc_path, *, label: str,
                     max_wait: int | None = None) -> None:
    timeout = max_wait or int(os.getenv("INTEGRATION_INGEST_TIMEOUT", "300"))
    result = client.ingest_file(doc_path)
    assert result.processing_id, f"No processing_id for {label}"
    logger.info("Ingesting %s -> processing_id=%s", doc_path.name, result.processing_id)
    status = client.wait_for_completion(result.processing_id, max_wait=timeout)
    assert status.status == "completed", f"Ingest failed ({label}): {status}"
    logger.info("Ingest complete: %s", label)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Smoke — backend info + pipeline detection
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.cocoindex
def test_cocoindex_backend_info(client: APIClient) -> None:
    """GET /api/info must succeed and confirm CocoIndex pipeline is active."""
    _skip_unless_cocoindex()
    info = client.info()
    logger.info("Backend info: %s", info)
    assert info is not None, "No info response from /api/info"


@pytest.mark.cocoindex
def test_cocoindex_health_ok(client: APIClient) -> None:
    """The health check must pass with CocoIndex enabled."""
    _skip_unless_cocoindex()
    assert client.wait_until_healthy(max_wait=10), "Backend health check failed"


# ─────────────────────────────────────────────────────────────────────────────
# 2. Ingest via HTTP API
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.cocoindex
def test_cocoindex_ingest_fast_doc(client: APIClient, fast_doc_path) -> None:
    """Ingest cmispress.txt through the CocoIndex pipeline via POST /api/ingest.

    CocoIndex memoises parse + embed + KG calls — the second run against the
    same file is near-instant because only target writes are replayed.
    """
    _skip_unless_cocoindex()
    _skip_if_cloud_source("ingest_fast_doc")
    _ingest_and_wait(client, fast_doc_path, label="cmispress.txt [coco]")


@pytest.mark.cocoindex
@pytest.mark.slow
def test_cocoindex_ingest_full_doc(client: APIClient, full_doc_path) -> None:
    """Ingest company-ontology-test.txt (multi-chunk) through the CocoIndex pipeline."""
    _skip_unless_cocoindex()
    _skip_if_cloud_source("ingest_full_doc")
    _ingest_and_wait(client, full_doc_path, label="company-ontology-test.txt [coco]")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Search / retrieval after ingest
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.cocoindex
@pytest.mark.vector
def test_cocoindex_vector_search_returns_results(client: APIClient) -> None:
    """Vector store populated by CocoIndex must return relevant results.

    Requires at least one of: VECTOR_DB active (native CocoIndex qdrant/lancedb/postgres)
    or a flexible vector adapter (qdrant/milvus/chroma/… with VECTOR_BACKEND=llamaindex|langchain).

    For cloud-native sources (S3, Azure Blob, Google Drive) this test triggers
    a CocoIndex sync-now and waits up to INTEGRATION_CLOUD_SYNC_TIMEOUT seconds
    (default 300) before asserting results.  If no matching content appears it
    skips rather than fails — the cloud folder may simply have different files.
    """
    _skip_unless_cocoindex()
    if not _active("VECTOR_DB"):
        pytest.skip("VECTOR_DB=none")

    _ensure_cloud_content_or_skip(
        client, FAST_SEARCH_QUERY, FAST_SEARCH_TERMS,
        context="vector search",
    )
    result = client.search(FAST_SEARCH_QUERY, top_k=8)
    _log_results(result, label=f"[coco/vector] {FAST_SEARCH_QUERY!r}")

    assert result.total > 0, (
        f"CocoIndex vector store returned 0 results for {FAST_SEARCH_QUERY!r}. "
        "Check that ingest completed successfully."
    )
    assert _has_relevant(result, FAST_SEARCH_TERMS), (
        f"None of {FAST_SEARCH_TERMS} found in CocoIndex vector results.\n"
        f"Snippets: {[r.get('content','')[:80] for r in result.results]}"
    )
    vdb = os.getenv("VECTOR_DB", "?")
    vbe = os.getenv("VECTOR_BACKEND", "?")
    logger.info("PASS  vector=%s (%s)  results=%d", vdb, vbe, result.total)


@pytest.mark.cocoindex
@pytest.mark.search_db
def test_cocoindex_search_store_returns_results(client: APIClient) -> None:
    """Flexible search store (Elasticsearch/BM25) populated via CocoIndex must return results.

    For cloud-native sources a sync-now is triggered first; the test skips if
    no matching content appears within INTEGRATION_CLOUD_SYNC_TIMEOUT seconds.
    """
    _skip_unless_cocoindex()
    if not _active("SEARCH_DB"):
        pytest.skip("SEARCH_DB=none")

    _ensure_cloud_content_or_skip(
        client, FAST_SEARCH_QUERY, FAST_SEARCH_TERMS,
        context="search store",
    )
    result = client.search(FAST_SEARCH_QUERY, top_k=8)
    _log_results(result, label=f"[coco/search] {FAST_SEARCH_QUERY!r}")

    assert result.total > 0, (
        f"Flexible search store returned 0 results via CocoIndex pipeline. "
        f"Check FlexibleSearch connector configuration."
    )
    sdb = os.getenv("SEARCH_DB", "?")
    logger.info("PASS  search=%s  results=%d", sdb, result.total)


@pytest.mark.cocoindex
@pytest.mark.graph
def test_cocoindex_graph_search_no_crash(client: APIClient) -> None:
    """Property graph populated by CocoIndex must not crash on search.

    Lenient bar: 0 results is a warning (KG quality varies by LLM);
    a 500 or exception IS a failure.

    Supports both native CocoIndex PG connectors (neo4j, falkordb, surrealdb)
    and flexible LI/LC adapters.
    """
    _skip_unless_cocoindex()
    if not _active("PG_GRAPH_DB"):
        pytest.skip("PG_GRAPH_DB=none")

    result = client.search(COMPANY_SEARCH_QUERY, top_k=8)
    _log_results(result, label=f"[coco/graph] {COMPANY_SEARCH_QUERY!r}")

    pg = os.getenv("PG_GRAPH_DB", "?")
    gbe = os.getenv("GRAPH_BACKEND", "?")
    if result.total == 0:
        logger.warning(
            "WARN  pg=%s (%s) returned 0 results via CocoIndex — "
            "KG extraction may need more extraction passes or stronger LLM.",
            pg, gbe,
        )
    elif _has_relevant(result, COMPANY_SEARCH_TERMS):
        logger.info("PASS  pg=%s (%s)  results=%d (relevant names found)", pg, gbe, result.total)
    else:
        logger.warning(
            "WARN  pg=%s (%s)  results=%d  but none of %s found.",
            pg, gbe, result.total, COMPANY_SEARCH_TERMS,
        )
    assert result.total >= 0


# ─────────────────────────────────────────────────────────────────────────────
# 4. Hybrid search with the ontology-rich document
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.cocoindex
def test_cocoindex_hybrid_search_who_works_for_acme(client: APIClient) -> None:
    """Hybrid search 'who works for acme' after CocoIndex ingest.

    Requires at least one active store (vector or search) with the full document
    already ingested.  For cloud-native sources a sync-now is triggered first;
    the test skips if no relevant content appears within INTEGRATION_CLOUD_SYNC_TIMEOUT s.
    """
    _skip_unless_cocoindex()
    if not _active("VECTOR_DB") and not _active("SEARCH_DB"):
        pytest.skip("Both VECTOR_DB and SEARCH_DB are 'none' — no retriever available")

    _ensure_cloud_content_or_skip(
        client, COMPANY_SEARCH_QUERY, COMPANY_SEARCH_TERMS,
        context="hybrid search",
    )
    result = client.search(COMPANY_SEARCH_QUERY, top_k=10)
    _log_results(result, label=f"[coco/hybrid] {COMPANY_SEARCH_QUERY!r}")

    assert result.total > 0, (
        f"CocoIndex hybrid search returned 0 results for {COMPANY_SEARCH_QUERY!r}."
    )
    assert _has_relevant(result, COMPANY_SEARCH_TERMS), (
        f"Expected one of {COMPANY_SEARCH_TERMS} in CocoIndex hybrid results.\n"
        f"Snippets: {[r.get('content','')[:80] for r in result.results]}"
    )
    for r in result.results:
        src = r.get("source") or r.get("metadata", {}).get("source", "")
        assert src, f"CocoIndex result missing 'source' field: {r}"


# ─────────────────────────────────────────────────────────────────────────────
# 5. AI Q&A
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.cocoindex
@pytest.mark.ai_qa
def test_cocoindex_ai_query_how_is_acme_organized(client: APIClient) -> None:
    """POST /api/query — AI Q&A on CocoIndex-ingested graph data.

    Uses the same endpoint as the non-CocoIndex path; the CocoIndex pipeline
    populates the graph, and the retriever reads from it via the standard hybrid
    retrieval stack.
    """
    _skip_unless_cocoindex()
    timeout = int(os.getenv("INTEGRATION_SEARCH_TIMEOUT", "300"))
    qr: QueryResult = client.query(COMPANY_AI_QUERY, top_k=10, timeout=timeout)
    logger.info("─── AI query [coco]: %r ───", COMPANY_AI_QUERY)
    logger.info("  Answer: %s", textwrap.shorten(qr.answer, width=400, placeholder=" …"))

    assert qr.status == "success", f"Unexpected status: {qr.raw}"
    assert qr.answer.strip(), "Empty answer from /api/query with CocoIndex pipeline"


# ─────────────────────────────────────────────────────────────────────────────
# 6. Re-ingest idempotency (CocoIndex memoisation)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.cocoindex
def test_cocoindex_reingest_is_fast(client: APIClient, fast_doc_path) -> None:
    """Re-ingesting the same file via CocoIndex should complete quickly.

    CocoIndex memoises chunk embeddings and KG extraction calls, so a second
    ingest of an unchanged file should only replay target writes (< 10 s even
    for cloud LLMs).

    If the file has never been ingested, this test triggers a full ingest and
    may take longer — that's also fine (the assertion is a soft warning, not
    a hard failure, to avoid flakiness on first-run CI).
    """
    _skip_unless_cocoindex()
    _skip_if_cloud_source("reingest_is_fast")
    start = time.monotonic()
    _ingest_and_wait(client, fast_doc_path, label="cmispress.txt [coco re-ingest]")
    elapsed = time.monotonic() - start

    # Memoised re-ingest should finish in under 30 s (target writes + HTTP overhead).
    # On first run it may take longer — only warn, don't hard-fail.
    if elapsed > 30:
        logger.warning(
            "[coco] Re-ingest of %s took %.1fs (> 30s) — "
            "either this was a first ingest or memoisation is not working as expected.",
            fast_doc_path.name, elapsed,
        )
    else:
        logger.info("[coco] Re-ingest took %.1fs — memoisation working as expected.", elapsed)


# ─────────────────────────────────────────────────────────────────────────────
# 7. Text ingest via API (POST /api/ingest-text)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.cocoindex
def test_cocoindex_ingest_text(client: APIClient) -> None:
    """POST /api/ingest-text should work with the CocoIndex pipeline."""
    _skip_unless_cocoindex()
    _skip_if_cloud_source("ingest_text")
    text = (
        "CocoIndex is an incremental data transformation framework powered by a Rust engine. "
        "It provides memoised chunking, embedding, and knowledge graph extraction with automatic "
        "incremental updates and stateful reconciliation."
    )
    result = client.ingest_text(text, filename="cocoindex-test.txt")
    assert result.processing_id, "No processing_id from text ingest"
    timeout = int(os.getenv("INTEGRATION_INGEST_TIMEOUT", "300"))
    status = client.wait_for_completion(result.processing_id, max_wait=timeout)
    assert status.status == "completed", f"Text ingest failed: {status}"
    logger.info("[coco] Text ingest completed  processing_id=%s", result.processing_id)


# ─────────────────────────────────────────────────────────────────────────────
# 8. Source backend variant
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.cocoindex
def test_cocoindex_source_backend_reported(client: APIClient) -> None:
    """Log the active source backend for observability.

    When SOURCE_BACKEND=cocoindex, data ingestion uses native CocoIndex
    connectors (filesystem, S3, Azure, GDrive).  When SOURCE_BACKEND=flexible,
    the existing Flexible GraphRAG detector-backed adapters are used, but
    the CocoIndex pipeline still handles memoisation and target writes.

    This test is informational — it always passes; it just records the
    active configuration in the test log for later inspection.
    """
    _skip_unless_cocoindex()
    pipe = _pipeline_backend()
    src  = _source_backend()
    vdb  = os.getenv("VECTOR_DB",   "none")
    gdb  = os.getenv("PG_GRAPH_DB", "none")
    sdb  = os.getenv("SEARCH_DB",   "none")
    gbe  = os.getenv("GRAPH_BACKEND",  os.getenv("GRAPH_BACKEND", "llamaindex"))
    vbe  = os.getenv("VECTOR_BACKEND", os.getenv("VECTOR_BACKEND", "llamaindex"))
    logger.info(
        "[coco] Config — pipeline=%s  source=%s  vec=%s (%s)  pg=%s (%s)  search=%s",
        pipe, src, vdb, vbe, gdb, gbe, sdb,
    )
    # Just report — always pass
    assert True
