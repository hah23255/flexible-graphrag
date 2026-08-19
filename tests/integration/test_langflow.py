"""
Integration tests for the Langflow-backed pipeline.

When ``ENABLE_LANGFLOW_FLOWS=true``, the backend routes ingest and query
requests through Langflow flows instead of the direct in-process pipeline.
The REST surface (endpoints, request/response shape) is **unchanged** — only
the execution path differs.

These tests verify that the standard API still behaves correctly end-to-end
when Langflow is the execution engine, and provide a baseline for comparing
Langflow output quality against the direct pipeline.

Prerequisites:
  - A running Langflow instance (default: http://localhost:7860 or as configured
    by ``LANGFLOW_URL``).
  - Backend started with ``ENABLE_LANGFLOW_FLOWS=true``.
  - At least one active store (VECTOR_DB or SEARCH_DB).
  - Langflow flows for ingest and query already imported/configured in Langflow
    (see ``docs/GETTING-STARTED/LANGFLOW-INTEGRATION.md``).

Run directly (backend already running with Langflow):
    pytest tests/integration/test_langflow.py -m langflow -s

Run via matrix (Langflow + LlamaIndex Neo4j):
    uv run tests/integration/run_matrix.py \\
        --langflow true --pg neo4j --vector qdrant --backends llamaindex \\
        --test-path tests/integration/test_langflow.py

Run via named profile:
    uv run tests/integration/run_profile.py --profile langflow-neo4j-llamaindex

Notes:
  - These tests do NOT spin up Langflow — it must be running independently.
  - Langflow flows must be configured before the backend starts (or within
    the LANGFLOW_RETRY_ATTEMPTS window at startup).
  - If Langflow is unreachable, tests are skipped with a clear message.
"""
from __future__ import annotations

import logging
import os
import textwrap
import time

import pytest
import requests

from tests.integration.api_client import APIClient, QueryResult, SearchResult

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _langflow_enabled() -> bool:
    return os.getenv("ENABLE_LANGFLOW_FLOWS", "false").strip().lower() in ("true", "1", "yes")


def _langflow_url() -> str:
    return os.getenv("LANGFLOW_URL", "http://localhost:7860").rstrip("/")


def _skip_unless_langflow() -> None:
    """Skip the test if Langflow is not enabled or not reachable."""
    if not _langflow_enabled():
        pytest.skip(
            "ENABLE_LANGFLOW_FLOWS != true — set it in the backend environment "
            "or run via: run_matrix.py --langflow true"
        )
    # Quick reachability check on the Langflow server itself
    url = _langflow_url()
    try:
        r = requests.get(f"{url}/api/v1/version", timeout=5)
        if r.status_code not in (200, 401):  # 401 = auth required but reachable
            pytest.skip(f"Langflow at {url} returned unexpected status {r.status_code}")
    except requests.ConnectionError:
        pytest.skip(
            f"Langflow server at {url} is not reachable. "
            "Start Langflow first: python -m langflow run --host 0.0.0.0 --port 7860"
        )


def _active(env_var: str) -> bool:
    return os.getenv(env_var, "none").strip().lower() not in ("none", "", "false")


def _ingest_timeout() -> int:
    override = os.getenv("INTEGRATION_INGEST_TIMEOUT")
    if override:
        return int(override)
    # Langflow adds round-trip overhead; allow extra time
    return int(os.getenv("INTEGRATION_INGEST_TIMEOUT", "600"))


COMPANY_SEARCH_QUERY = "who works for acme"
COMPANY_SEARCH_TERMS = ["james", "linda", "marcus", "priya", "sarah"]

COMPANY_AI_QUERY = "how is acme organized"
COMPANY_AI_TERMS = ["engineering", "department", "sales", "management", "organized"]

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


def _ingest_and_wait(client: APIClient, doc_path, *, label: str) -> None:
    result = client.ingest_file(doc_path)
    assert result.processing_id, f"No processing_id for {label}"
    logger.info("[langflow] Ingesting %s -> processing_id=%s", doc_path.name, result.processing_id)
    status = client.wait_for_completion(result.processing_id, max_wait=_ingest_timeout())
    assert status.status == "completed", f"Langflow ingest failed ({label}): {status}"
    logger.info("[langflow] Ingest complete: %s", label)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Smoke — Langflow server + backend connectivity
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.langflow
def test_langflow_server_reachable() -> None:
    """Langflow server must be reachable before other Langflow tests run."""
    _skip_unless_langflow()
    url = _langflow_url()
    r = requests.get(f"{url}/api/v1/version", timeout=10)
    assert r.status_code in (200, 401), (
        f"Langflow at {url} returned unexpected status {r.status_code}"
    )
    try:
        version_info = r.json()
        logger.info("[langflow] Server version: %s", version_info.get("version", r.text[:80]))
    except Exception:
        logger.info("[langflow] Server reachable at %s (status %d)", url, r.status_code)


@pytest.mark.langflow
def test_langflow_backend_health(client: APIClient) -> None:
    """FastAPI backend must be healthy with ENABLE_LANGFLOW_FLOWS=true."""
    _skip_unless_langflow()
    assert client.wait_until_healthy(max_wait=10), (
        "Backend health check failed with ENABLE_LANGFLOW_FLOWS=true"
    )
    info = client.health()
    logger.info("[langflow] Backend health: %s", info)


@pytest.mark.langflow
def test_langflow_flows_listed() -> None:
    """Langflow /api/v1/flows/ endpoint should return flows (informational — non-fatal).

    This test lists the flows visible in LangFlow for diagnostic purposes only.
    The backend manages flow upload/bind automatically at startup; the test does
    NOT need to verify specific flow IDs.  A 0-flow result or a non-200 response
    is logged as a warning rather than failing the suite.
    """
    _skip_unless_langflow()
    url = _langflow_url()
    api_key = os.getenv("LANGFLOW_API_KEY", "")
    headers = {"x-api-key": api_key} if api_key else {}

    # LangFlow 1.x requires the trailing slash on /api/v1/flows/
    r: requests.Response | None = None
    for endpoint in ("/api/v1/flows/", "/api/v1/flows"):
        try:
            r = requests.get(f"{url}{endpoint}", headers=headers, timeout=15)
            if r.status_code in (200, 307, 308):  # 307/308 = redirect (follow)
                break
            if r.status_code == 401:
                pytest.skip("Langflow requires authentication — set LANGFLOW_API_KEY in .env")
        except requests.ConnectionError:
            pytest.skip(f"Langflow at {url} not reachable")
    else:
        # Both endpoints returned non-200 — just warn, don't fail
        _status = r.status_code if r is not None else "unreachable"
        logger.warning(
            "[langflow] Could not list flows from %s (status %s) — "
            "the backend manages flow uploads automatically; this is informational only.",
            url, _status,
        )
        return

    if r is None:
        return  # connection error path (should not reach here after pytest.skip above)

    try:
        flows = r.json()
    except Exception:
        logger.warning("[langflow] /api/v1/flows/ did not return JSON — status %s", r.status_code)
        return

    if isinstance(flows, list):
        flow_names = [f.get("name", "?") for f in flows[:10]]
    elif isinstance(flows, dict):
        items = flows.get("items") or flows.get("flows") or []
        flow_names = [f.get("name", "?") for f in items[:10]]
    else:
        flow_names = []

    if flow_names:
        logger.info("[langflow] Flows available: %s", flow_names)
    else:
        logger.warning(
            "[langflow] No flows visible — the backend uploads flows at startup "
            "via ENABLE_LANGFLOW_FLOWS=true.  Ingest/search tests will verify "
            "that the flows are active via the standard /api/* endpoints."
        )
    # Intentionally non-fatal — flow presence is verified implicitly by ingest tests


# ─────────────────────────────────────────────────────────────────────────────
# 2. Ingest via Langflow
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.langflow
def test_langflow_ingest_fast_doc(client: APIClient, fast_doc_path) -> None:
    """POST /api/ingest via Langflow flow must complete for cmispress.txt.

    The backend transparently routes this through the Langflow ingest flow
    when ENABLE_LANGFLOW_FLOWS=true.
    """
    _skip_unless_langflow()
    _ingest_and_wait(client, fast_doc_path, label="cmispress.txt [langflow]")


@pytest.mark.langflow
@pytest.mark.slow
def test_langflow_ingest_full_doc(client: APIClient, full_doc_path) -> None:
    """Ingest company-ontology-test.txt through Langflow (multi-chunk, ontology-rich)."""
    _skip_unless_langflow()
    _ingest_and_wait(client, full_doc_path, label="company-ontology-test.txt [langflow]")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Search after Langflow ingest
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.langflow
@pytest.mark.vector
def test_langflow_vector_search_returns_results(client: APIClient) -> None:
    """Vector search must return results when ingest ran through Langflow."""
    _skip_unless_langflow()
    if not _active("VECTOR_DB"):
        pytest.skip("VECTOR_DB=none")

    result = client.search(FAST_SEARCH_QUERY, top_k=8)
    _log_results(result, label=f"[langflow/vector] {FAST_SEARCH_QUERY!r}")

    assert result.total > 0, (
        f"Langflow vector store returned 0 results for {FAST_SEARCH_QUERY!r}."
    )
    assert _has_relevant(result, FAST_SEARCH_TERMS), (
        f"None of {FAST_SEARCH_TERMS} found in Langflow vector results.\n"
        f"Snippets: {[r.get('content','')[:80] for r in result.results]}"
    )
    vdb = os.getenv("VECTOR_DB", "?")
    logger.info("PASS  [langflow] vector=%s  results=%d", vdb, result.total)


@pytest.mark.langflow
def test_langflow_hybrid_search_who_works_for_acme(client: APIClient) -> None:
    """Hybrid search via Langflow pipeline must surface employee names."""
    _skip_unless_langflow()
    if not _active("VECTOR_DB") and not _active("SEARCH_DB"):
        pytest.skip("Both VECTOR_DB and SEARCH_DB are 'none' — no retriever available")

    result = client.search(COMPANY_SEARCH_QUERY, top_k=10)
    _log_results(result, label=f"[langflow/hybrid] {COMPANY_SEARCH_QUERY!r}")

    assert result.total > 0, (
        f"Langflow hybrid search returned 0 results for {COMPANY_SEARCH_QUERY!r}."
    )
    assert _has_relevant(result, COMPANY_SEARCH_TERMS), (
        f"Expected one of {COMPANY_SEARCH_TERMS} in Langflow hybrid results.\n"
        f"Snippets: {[r.get('content','')[:80] for r in result.results]}"
    )
    for r in result.results:
        src = r.get("source") or r.get("metadata", {}).get("source", "")
        assert src, f"Langflow result missing 'source' field: {r}"


@pytest.mark.langflow
@pytest.mark.graph
def test_langflow_graph_search_no_crash(client: APIClient) -> None:
    """Property graph search must not crash when Langflow is the pipeline.

    Lenient bar: 0 results is a warning; a 500 or exception IS a failure.
    """
    _skip_unless_langflow()
    if not _active("PG_GRAPH_DB"):
        pytest.skip("PG_GRAPH_DB=none")

    result = client.search(COMPANY_SEARCH_QUERY, top_k=8)
    _log_results(result, label=f"[langflow/graph] {COMPANY_SEARCH_QUERY!r}")

    pg = os.getenv("PG_GRAPH_DB", "?")
    if result.total == 0:
        logger.warning("[langflow] pg=%s returned 0 results — Langflow KG extraction may differ.", pg)
    else:
        logger.info("PASS  [langflow] pg=%s  results=%d", pg, result.total)
    assert result.total >= 0


# ─────────────────────────────────────────────────────────────────────────────
# 4. AI Q&A via Langflow
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.langflow
@pytest.mark.ai_qa
def test_langflow_ai_query_how_is_acme_organized(client: APIClient) -> None:
    """POST /api/query via Langflow query flow must return a non-empty answer."""
    _skip_unless_langflow()
    timeout = int(os.getenv("INTEGRATION_SEARCH_TIMEOUT", "600"))
    # Langflow adds round-trip overhead; double the normal timeout
    qr: QueryResult = client.query(COMPANY_AI_QUERY, top_k=10, timeout=timeout)

    logger.info("─── AI query [langflow]: %r ───", COMPANY_AI_QUERY)
    logger.info("  Answer: %s", textwrap.shorten(qr.answer, width=400, placeholder=" …"))

    assert qr.status == "success", f"Unexpected status from Langflow query: {qr.raw}"
    assert qr.answer.strip(), "Empty answer from /api/query with ENABLE_LANGFLOW_FLOWS=true"


# ─────────────────────────────────────────────────────────────────────────────
# 5. Text ingest via Langflow
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.langflow
def test_langflow_ingest_text(client: APIClient) -> None:
    """POST /api/ingest-text via Langflow flow must complete successfully."""
    _skip_unless_langflow()
    text = (
        "Langflow is a visual framework for building multi-agent and RAG applications. "
        "It integrates with LangChain components and supports flexible workflows "
        "for document ingestion, retrieval, and AI-powered question answering."
    )
    result = client.ingest_text(text, filename="langflow-test.txt")
    assert result.processing_id, "No processing_id from Langflow text ingest"
    timeout = _ingest_timeout()
    status = client.wait_for_completion(result.processing_id, max_wait=timeout)
    assert status.status == "completed", f"Langflow text ingest failed: {status}"
    logger.info("[langflow] Text ingest completed  processing_id=%s", result.processing_id)


# ─────────────────────────────────────────────────────────────────────────────
# 6. Configuration report
# ─────────────────────────────────────────────────────────────────────────────
#
# NOTE — Langflow and CocoIndex pipeline are MUTUALLY EXCLUSIVE.
#
# When both ENABLE_LANGFLOW_FLOWS=true and PIPELINE_BACKEND=cocoindex are set,
# the CocoIndex bridge does not activate — the server falls through to the
# regular ingest path (which Langflow then intercepts).  The two integrations
# share no overlap in the current implementation.
#
# Future investigation: exposing Flexible GraphRAG CocoIndex components as
# Langflow custom components (so a CocoIndex pipeline *runs inside* a Langflow
# flow) is a possible future direction.  For now, test each separately.
#   - Langflow only:  ENABLE_LANGFLOW_FLOWS=true, PIPELINE_BACKEND=default
#   - CocoIndex only: PIPELINE_BACKEND=cocoindex, ENABLE_LANGFLOW_FLOWS=false
#   - Use named profiles: langflow-neo4j-llamaindex / coco-qdrant-neo4j

@pytest.mark.langflow
def test_langflow_config_report(client: APIClient) -> None:
    """Log the active Langflow + pipeline configuration for observability.

    Always passes — informational only.
    """
    _skip_unless_langflow()
    langflow_url  = _langflow_url()
    pipeline      = os.getenv("PIPELINE_BACKEND", "default")
    vdb   = os.getenv("VECTOR_DB",   "none")
    gdb   = os.getenv("PG_GRAPH_DB", "none")
    sdb   = os.getenv("SEARCH_DB",   "none")
    gbe   = os.getenv("GRAPH_BACKEND",  "llamaindex")
    vbe   = os.getenv("VECTOR_BACKEND", "llamaindex")
    logger.info(
        "[langflow] Config — url=%s  pipeline=%s  "
        "vec=%s (%s)  pg=%s (%s)  search=%s",
        langflow_url, pipeline, vdb, vbe, gdb, gbe, sdb,
    )
    assert True
