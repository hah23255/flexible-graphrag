"""Deterministic per-store test for hostile entity ids / labels / relationship types.

Why this exists
---------------
The LangChain KG path writes ``LLMGraphTransformer`` output straight into store
queries, so a model that happens to emit a backslash or an apostrophe in an
entity name can break ingest.  Those failures showed up in the overnight matrix
as **intermittent** — Azure Cosmos DB for Gremlin died on
``A graph element 'id' cannot contain invalid character '\\'`` one night and
passed the next, purely because extraction differs run to run.

This test removes the LLM from the loop.  It builds ``GraphDocument`` objects
with hand-picked hostile values and writes them through the *same*
``add_graph_documents`` path ingest uses, so a store either handles a given
character class or it does not — same answer every run.

It is therefore both:
  * a regression test for ``langchain/graph/id_sanitizer.py``, and
  * a probe that tells you, per store, which characters are genuinely a problem.

Scope
-----
Runs against whatever ``PG_GRAPH_DB`` the backend was started with, so the
matrix drives coverage:

    uv run tests/integration/run_matrix.py --clean --pg neo4j,arcadedb,cosmos_gremlin,arangodb,apache_age,hugegraph,tigergraph,surrealdb,falkordb,memgraph,nebula,ladybug --graph-backend langchain --vector qdrant --test-path tests/integration/test_pg_id_sanitizer.py

Unlike the rest of the integration suite this talks to the store adapter
directly rather than through the REST API — that is the only way to control the
exact bytes that reach the store.  ``tests/integration/conftest.py`` already puts
``flexible-graphrag`` on ``sys.path``.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List

import pytest

logger = logging.getLogger(__name__)


# ── The hostile inputs, one class per case ───────────────────────────────────
# Each entry: (case_id, entity_id, entity_label, relationship_type)
#
# "backslash" and "apostrophe" are the two that actually bit us: Cosmos rejects
# a backslash in an id outright, and langchain_community's GremlinGraph builds
# g.V().has('id','<value>') by interpolation, so an apostrophe closes the
# literal early (a Gremlin-injection hazard as much as a crash).
HOSTILE_CASES = [
    ("plain",        "Acme Corporation",      "Company",      "WORKS_FOR"),
    ("backslash",    "Acme\\Corp",            "Company",      "WORKS_FOR"),
    ("apostrophe",   "Zoe's Cafe",            "Company",      "OWNS"),
    ("double_quote", 'The "Big" Co',          "Company",      "OWNS"),
    ("slash",        "Research/Development",  "Department",   "PART_OF"),
    ("hash_question","Item #42 ?",            "Product",      "HAS_FEATURE"),
    ("space_label",  "Jane Doe",              "Staff Member", "REPORTS TO"),
    ("unicode",      "Zoë Café Ltd",          "Company",      "LOCATED_IN"),
    ("newline",      "Line1\nLine2",          "Note",         "MENTIONS"),
    ("leading_digit","3M Company",            "3Company",     "3RELATED"),
]


def _pg_graph_db() -> str:
    return (os.getenv("PG_GRAPH_DB", "none") or "none").strip().lower()


def _skip_unless_pg_configured() -> str:
    db = _pg_graph_db()
    if db in ("none", ""):
        pytest.skip("PG_GRAPH_DB not configured — nothing to write to")
    return db


def _build_graph_document(entity_id: str, label: str, rel_type: str) -> Any:
    """Build a one-edge GraphDocument, same shape LLMGraphTransformer produces."""
    from langchain_community.graphs.graph_document import (  # type: ignore[import-untyped]
        GraphDocument, Node, Relationship,
    )
    from langchain_core.documents import Document  # type: ignore[import-untyped]

    src = Node(id=entity_id, type=label)
    tgt = Node(id="Target Entity", type="Company")
    return GraphDocument(
        nodes=[src, tgt],
        relationships=[Relationship(source=src, target=tgt, type=rel_type)],
        source=Document(page_content="sanitizer probe", metadata={"source": "id-sanitizer-test"}),
    )


def _write(graph_docs: List[Any], db: str) -> None:
    """Write through the same adapter path ingest uses.  Raises on store rejection."""
    from langchain.graph.pg_store_adapters import create_property_graph_adapter  # type: ignore[import-untyped]
    from config import Settings  # type: ignore[import-untyped]

    settings = Settings()
    cfg = getattr(settings, "graph_db_config", {}) or {}
    adapter = create_property_graph_adapter(db_type=db, config=cfg)

    try:
        add = getattr(adapter, "add_graph_documents", None)
        inner = None
        if not callable(add):
            for attr in ("get_lc_graph", "get_graph"):
                fn = getattr(adapter, attr, None)
                if callable(fn):
                    inner = fn()
                    if inner is not None:
                        break
            add = getattr(inner, "add_graph_documents", None) if inner is not None else None
        if not callable(add):
            pytest.skip(f"{db}: no add_graph_documents path on the adapter")
        add(graph_docs, include_source=False)
    finally:
        # Close the driver/session explicitly.  The bolt-protocol stores (neo4j,
        # falkordb, memgraph, arcadedb, nebula) otherwise leave it to the
        # destructor, which the neo4j driver warns about:
        #   "Relying on Driver's destructor to close the session is deprecated"
        for obj in (adapter, locals().get("inner")):
            if obj is None:
                continue
            for closer in ("close", "aclose"):
                fn = getattr(obj, closer, None)
                if callable(fn):
                    try:
                        fn()
                    except Exception:  # noqa: BLE001 - teardown must not fail the test
                        pass
                    break


# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.parametrize("case_id,entity_id,label,rel_type", HOSTILE_CASES,
                         ids=[c[0] for c in HOSTILE_CASES])
def test_hostile_id_survives_sanitizer(case_id, entity_id, label, rel_type) -> None:
    """The sanitiser must make every hostile value safe for the configured store.

    Asserts on the SANITISED value rather than the store round-trip so the
    failure message says which character class the sanitiser missed, not merely
    that some driver raised.
    """
    db = _skip_unless_pg_configured()
    from langchain.graph.id_sanitizer import (
        sanitize_graph_documents, sanitize_label, sanitize_rel_type,
    )

    doc = _build_graph_document(entity_id, label, rel_type)
    sanitize_graph_documents([doc], db)

    node = doc.nodes[0]
    # Read the per-store rules from the sanitiser itself rather than restating
    # them: a duplicated literal here silently stopped asserting FalkorDB's
    # apostrophe rule the moment that store was added to the real table.
    from langchain.graph.id_sanitizer import _STORE_FORBIDDEN_ID_CHARS
    forbidden = set(_STORE_FORBIDDEN_ID_CHARS.get(db, ""))

    assert not (set(node.id) & forbidden), (
        f"{db}: sanitised id {node.id!r} still contains forbidden character(s) "
        f"{sorted(set(node.id) & forbidden)} (case: {case_id})"
    )
    assert not any(ord(c) < 0x20 for c in node.id), (
        f"{db}: sanitised id {node.id!r} still contains a control character (case: {case_id})"
    )
    assert node.id, f"{db}: sanitiser produced an empty id (case: {case_id})"

    # Labels and relationship types are identifiers in every supported query
    # language, so they must come out as bare identifiers.
    assert node.type == sanitize_label(label)
    assert node.type.replace("_", "").isalnum(), f"label {node.type!r} is not identifier-safe"
    rel = doc.relationships[0]
    assert rel.type == sanitize_rel_type(rel_type)
    assert rel.type.replace("_", "").isalnum(), f"rel type {rel.type!r} is not identifier-safe"
    assert not rel.type[0].isdigit(), f"rel type {rel.type!r} starts with a digit"


@pytest.mark.integration
@pytest.mark.slow
def test_type_name_used_as_both_relationship_and_label() -> None:
    """The same name as a relationship type, then as a node label, must still write.

    ArcadeDB (and Apache AGE) share ONE namespace between vertex and edge types.
    ``LLMGraphTransformer`` routinely emits the same token both ways across
    different documents — ``GOAL`` as a relationship in one, an entity label in
    the next — and each ``add_graph_documents`` call only saw its own batch.
    The second call then created nothing (the name existed, as an edge) and its
    MERGE died with a bare ``Neo.DatabaseError.General.UnknownError`` naming no
    cause.

    That made it look intermittent in the overnight matrix: it only fired when
    extraction happened to produce that ordering.  Two fixed batches reproduce
    it every run.
    """
    db = _skip_unless_pg_configured()
    from langchain.graph.id_sanitizer import sanitize_graph_documents

    from langchain_community.graphs.graph_document import (  # type: ignore[import-untyped]
        GraphDocument, Node, Relationship,
    )
    from langchain_core.documents import Document  # type: ignore[import-untyped]

    name = "COLLIDE_PROBE"
    src = Document(page_content="type collision probe", metadata={"source": "collision-test"})

    # Batch 1: the name is a RELATIONSHIP type.
    a, b = Node(id="Probe A", type="Thing"), Node(id="Probe B", type="Thing")
    batch1 = GraphDocument(
        nodes=[a, b],
        relationships=[Relationship(source=a, target=b, type=name)],
        source=src,
    )
    # Batch 2: the same name is now a NODE label.
    c = Node(id="Probe C", type="Thing")
    n = Node(id="Probe Node", type=name)
    batch2 = GraphDocument(
        nodes=[c, n],
        relationships=[Relationship(source=c, target=n, type="HAS_PROBE")],
        source=src,
    )

    for step, batch in ((1, batch1), (2, batch2)):
        sanitize_graph_documents([batch], db)
        try:
            _write([batch], db)  # a fresh adapter each time, as ingest does
        except Exception as exc:
            msg = str(exc).lower()
            if any(t in msg for t in ("connection", "refused", "timeout", "unreachable", "not available")):
                pytest.skip(f"{db}: store unreachable — {type(exc).__name__}: {exc}")
            pytest.fail(
                f"{db}: batch {step} failed writing {name!r} used as "
                f"{'a relationship type' if step == 1 else 'a node label'}: "
                f"{type(exc).__name__}: {exc}\n"
                f"On a store that shares one type namespace, the adapter must "
                f"consult the live schema and rename the collision."
            )
    logger.info("[type-collision] %s accepted %r as both a relationship and a label", db, name)


@pytest.mark.integration
@pytest.mark.slow
def test_hostile_ids_write_to_store() -> None:
    """End-to-end: every hostile case actually writes to the configured store.

    This is the test that reproduces the original defects.  With the sanitiser
    removed it should fail for Cosmos Gremlin on the backslash/apostrophe cases;
    with it in place every store should accept all of them.

    Skips (rather than fails) when the store is unreachable, so a matrix run
    against a partially-started docker stack stays honest about what it proved.
    """
    db = _skip_unless_pg_configured()
    from langchain.graph.id_sanitizer import sanitize_graph_documents

    graph_docs = [
        _build_graph_document(entity_id, label, rel_type)
        for _cid, entity_id, label, rel_type in HOSTILE_CASES
    ]
    sanitize_graph_documents(graph_docs, db)

    try:
        _write(graph_docs, db)
    except Exception as exc:
        msg = str(exc).lower()
        if any(tok in msg for tok in ("connection", "refused", "timeout", "unreachable", "not available")):
            pytest.skip(f"{db}: store unreachable — {type(exc).__name__}: {exc}")
        pytest.fail(
            f"{db}: writing sanitised hostile ids still failed with "
            f"{type(exc).__name__}: {exc}\n"
            f"If the message names a character, add it to _STORE_FORBIDDEN_ID_CHARS "
            f"for {db!r} in langchain/graph/id_sanitizer.py."
        )
    logger.info("[id-sanitizer] %s accepted all %d hostile cases", db, len(HOSTILE_CASES))
