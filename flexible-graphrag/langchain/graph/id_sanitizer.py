"""Store-aware sanitisation of LangChain ``GraphDocument`` ids, labels and types.

Why this exists
---------------
``LLMGraphTransformer`` puts raw model output straight into ``Node.id``,
``Node.type`` and ``Relationship.type``, and the LangChain store integrations
interpolate those values into queries.  Some stores then reject or mis-parse
them, and because the values come from an LLM the failures are
**non-deterministic** — the same document can ingest cleanly one run and fail
the next.

Two failures observed in this project (2026-08-13 overnight matrix):

* **Azure Cosmos DB for Gremlin** — ``A graph element 'id' cannot contain
  invalid character '\\'``.  ``langchain_community``'s ``GremlinGraph`` builds
  ``g.V().has('id','{node.id}')`` by **string interpolation with no escaping**,
  so a backslash breaks the id rules and an apostrophe would terminate the
  quoted literal outright (a Gremlin-injection hazard, not just a crash).
* **ArcadeDB** — a relationship MERGE failed with a generic
  ``Neo.DatabaseError.General.UnknownError``.  Whether that is id/type-related
  is NOT established; ArcadeDB returns no detail over Bolt.  The sanitiser is
  applied there too because it is harmless, but see ``tests/integration/
  test_pg_id_sanitizer.py`` for the empirical per-store answer.

The CocoIndex *native* connectors already do this for their own writes
(``_safe_label`` / ``_safe_rel_type`` in
``cocoindex_integration/connectors/cocoindex/_runtime.py``).  Those only apply
when ``GRAPH_BACKEND=cocoindex``, i.e. the native Neo4j/FalkorDB/SurrealDB
targets — they never touch the LangChain path, which is what this module covers.

Design: minimal by default
--------------------------
Node ids are **content**, not identifiers: entity names legitimately contain
spaces, accents and punctuation ("Acme Corporation", "Zoë's Café").  Rewriting
them aggressively would change every doc_id and silently fragment existing
graphs.  So the universal rules only remove characters that cannot survive
anywhere (control characters, newlines), and each store opts in to the extra
characters *it* cannot handle.

Labels and relationship types are different — they ARE identifiers in every
supported query language, so those get the strict identifier treatment.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

#: Fallbacks when sanitisation leaves nothing usable.
FALLBACK_ID = "unknown"
FALLBACK_LABEL = "Entity"
FALLBACK_REL_TYPE = "RELATED_TO"

#: Control characters are never valid in any store's id, label or type.
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
_WHITESPACE_RUN = re.compile(r"\s+")

#: Characters each store cannot accept inside a node id.
#:
#: cosmos_gremlin: Cosmos DB forbids ``/ \\ ? #`` in an id outright, and
#: langchain_community's GremlinGraph interpolates ids into a single-quoted
#: Gremlin literal without escaping, so ``'`` and ``"`` must go too.
#: gremlin/tinkerpop: same interpolation issue, without the Cosmos id rules.
_STORE_FORBIDDEN_ID_CHARS: Dict[str, str] = {
    "cosmos_gremlin": "/\\?#'\"",
    "gremlin": "'\"",
    "tigergraph": "'\"",
    # falkordb: langchain_community's FalkorDB integration also interpolates the
    # id into a quoted Cypher literal and its parser rejects the backslash
    # escaping that produces:
    #   Invalid input ' ': expected STARTS WITH ... errCtx: MERGE (n:`Company` {id: 'Zoe\'s Caf
    # Found by tests/integration/test_pg_id_sanitizer.py, NOT by the overnight
    # matrix -- the LLM had simply never emitted an apostrophe in an entity name.
    "falkordb": "'\"",
}

#: Stores whose ids are safest kept ASCII (non-ASCII is silently mangled or
#: rejected by some drivers).  Empty for now — recorded as the extension point.
_STORE_ASCII_ONLY_IDS: frozenset = frozenset()


def _strip_control(value: str) -> str:
    """Remove control characters and collapse whitespace runs to single spaces."""
    cleaned = _CONTROL_CHARS.sub("", value or "")
    return _WHITESPACE_RUN.sub(" ", cleaned).strip()


def sanitize_node_id(value: Any, store_type: str = "") -> str:
    """Return *value* usable as a node id for *store_type*.

    Universal: drop control characters, collapse whitespace.  Per store: drop
    the characters that store cannot represent.  Deliberately preserves spaces,
    case and accents — entity names are content and changing them would alter
    node identity across every store.
    """
    text = _strip_control(str(value if value is not None else ""))

    forbidden = _STORE_FORBIDDEN_ID_CHARS.get((store_type or "").lower(), "")
    if forbidden:
        text = text.translate({ord(ch): None for ch in forbidden})
        text = _WHITESPACE_RUN.sub(" ", text).strip()

    if (store_type or "").lower() in _STORE_ASCII_ONLY_IDS:
        text = (
            unicodedata.normalize("NFKD", text)
            .encode("ascii", "ignore")
            .decode("ascii")
            .strip()
        )

    return text or FALLBACK_ID


def sanitize_label(value: Any, fallback: str = FALLBACK_LABEL) -> str:
    """Return *value* usable as a node label in Cypher / Gremlin / SPARQL.

    Labels are identifiers everywhere, so this is the strict treatment: letters,
    digits and underscores only, never leading with a digit.
    """
    text = _strip_control(str(value if value is not None else ""))
    text = re.sub(r"[^a-zA-Z0-9]", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if text and text[0].isdigit():
        text = f"_{text}"
    return text or fallback


def sanitize_rel_type(value: Any, fallback: str = FALLBACK_REL_TYPE) -> str:
    """Return *value* usable as a relationship type (upper-cased identifier)."""
    text = _strip_control(str(value if value is not None else "")).upper()
    text = re.sub(r"[^A-Z0-9]", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if text and text[0].isdigit():
        text = f"_{text}"
    return text or fallback


def sanitize_graph_documents(
    graph_docs: Iterable[Any], store_type: str = "",
) -> Tuple[int, int]:
    """Sanitise every node id/type and relationship type, in place.

    Relationship endpoints are rewritten with the SAME rules as the node ids, so
    an edge still resolves to its endpoints after both have been cleaned.

    Returns ``(nodes_changed, rels_changed)`` for logging.  Never raises: a
    malformed GraphDocument is skipped rather than failing the whole ingest.
    """
    nodes_changed = 0
    rels_changed = 0

    for doc in graph_docs or []:
        try:
            for node in getattr(doc, "nodes", None) or []:
                original_id = getattr(node, "id", None)
                clean_id = sanitize_node_id(original_id, store_type)
                if clean_id != original_id:
                    node.id = clean_id
                    nodes_changed += 1
                original_type = getattr(node, "type", None)
                clean_type = sanitize_label(original_type)
                if clean_type != original_type:
                    node.type = clean_type

            for rel in getattr(doc, "relationships", None) or []:
                changed = False
                for endpoint in ("source", "target"):
                    ep = getattr(rel, endpoint, None)
                    if ep is None:
                        continue
                    ep_id = getattr(ep, "id", None)
                    clean_ep = sanitize_node_id(ep_id, store_type)
                    if clean_ep != ep_id:
                        ep.id = clean_ep
                        changed = True
                    ep_type = getattr(ep, "type", None)
                    clean_ep_type = sanitize_label(ep_type)
                    if clean_ep_type != ep_type:
                        ep.type = clean_ep_type
                        changed = True
                rel_type = getattr(rel, "type", None)
                clean_rel = sanitize_rel_type(rel_type)
                if clean_rel != rel_type:
                    rel.type = clean_rel
                    changed = True
                if changed:
                    rels_changed += 1
        except Exception as exc:  # noqa: BLE001 - never break ingest over this
            logger.debug("sanitize_graph_documents: skipped a document: %s", exc)

    if nodes_changed or rels_changed:
        logger.info(
            "Graph id/label sanitiser (%s): %d node(s), %d relationship(s) adjusted",
            store_type or "generic", nodes_changed, rels_changed,
        )
    return nodes_changed, rels_changed


__all__ = [
    "sanitize_node_id",
    "sanitize_label",
    "sanitize_rel_type",
    "sanitize_graph_documents",
    "FALLBACK_ID",
    "FALLBACK_LABEL",
    "FALLBACK_REL_TYPE",
]
