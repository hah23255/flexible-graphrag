"""Shared normalization for LangChain ``GraphDocument`` batches.

``LLMGraphTransformer`` emits relationships whose ``source`` / ``target`` nodes
are frequently **absent** from ``document.nodes`` (e.g. a ``Place`` that only
ever appears as the target of ``LOCATED_IN``).  Every store adapter that writes
those relationships has to cope with the same three consequences:

1. The endpoint vertex/type is never declared, so the edge write fails.
2. The endpoint node carries no ``name``, so QA chains that filter on ``name``
   cannot match it.
3. The endpoint node carries no ``ref_doc_id``, so incremental delete misses it
   and leaves orphan entities behind that graph QA keeps answering from.

:func:`normalize_graph_documents` fixes all three **once**, in a
framework-neutral way, by folding endpoints into ``doc.nodes`` and stamping the
shared properties.  Adapters then only need their own store-specific schema
DDL, write syntax, and delete queries — the per-store loops that used to
re-derive endpoints are no longer needed.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, FrozenSet, Iterable, List, Optional

logger = logging.getLogger(__name__)

__all__ = ["source_ref_doc_id", "normalize_graph_documents"]


def source_ref_doc_id(doc: Any) -> str:
    """Return the stable document id for *doc*, or ``""`` when absent.

    Reads ``ref_doc_id`` first, then ``doc_id``, from ``doc.source.metadata``.
    This is the key incremental delete filters on, so both spellings are
    accepted — ``ingest_lc_graph`` writes ``doc_id`` while the RDF/vector paths
    use ``ref_doc_id``.
    """
    source = getattr(doc, "source", None)
    metadata = (getattr(source, "metadata", None) if source is not None else None) or {}
    return str(metadata.get("ref_doc_id") or metadata.get("doc_id") or "")


def normalize_graph_documents(
    graph_documents: Iterable[Any],
    *,
    metadata_skip: FrozenSet[str] = frozenset(),
    stamp_doc_id: bool = False,
    name_property: str = "name",
    rid_property: str = "ref_doc_id",
) -> Dict[int, str]:
    """Fold relationship endpoints into ``doc.nodes`` and stamp shared props.

    Mutates *graph_documents* in place:

    - Relationship endpoints missing from ``doc.nodes`` are appended, so any
      adapter loop over ``doc.nodes`` covers them (schema collection, property
      declaration, vertex writes, ``MENTIONS`` links).
    - ``name_property`` defaults to ``str(node.id)`` on every node.
    - ``rid_property`` is stamped from ``doc.source.metadata`` on every node,
      making rid-based incremental delete complete.
    - ``doc.source.metadata`` gets the rid under both spellings, so stores that
      persist source metadata verbatim (ArangoDB's ``*_SOURCE`` collection) can
      be filtered on it.
    - Keys in *metadata_skip* are removed from node and relationship properties.

    Parameters
    ----------
    metadata_skip:
        Ingestion-metadata keys to strip (e.g. ``file_path``, ``modified_at``).
        These are not graph-model properties and would otherwise flood the
        schema with filesystem columns on every write.
    stamp_doc_id:
        Also write the rid to a ``doc_id`` property.  Stores whose delete query
        checks both spellings (Apache AGE) need this.
    name_property, rid_property:
        Property names to use, for stores that differ from the defaults.

    Returns
    -------
    Mapping of ``id(doc)`` to the resolved rid, so callers can reuse it without
    re-reading ``doc.source.metadata``.
    """
    rids: Dict[int, str] = {}

    for doc in graph_documents:
        rid = source_ref_doc_id(doc)
        rids[id(doc)] = rid

        source = getattr(doc, "source", None)
        if rid and source is not None:
            if getattr(source, "metadata", None) is None:
                source.metadata = {}
            source.metadata.setdefault("ref_doc_id", rid)
            source.metadata.setdefault("doc_id", rid)

        nodes: List[Any] = doc.nodes if doc.nodes is not None else []
        if doc.nodes is None:
            doc.nodes = nodes

        # Identity is (id, type): the same name can legitimately exist under two
        # types, and stores key vertices on both.
        known = {(str(n.id), getattr(n, "type", None)) for n in nodes}

        for rel in doc.relationships or []:
            if getattr(rel, "properties", None) is None:
                rel.properties = {}
            _strip(rel.properties, metadata_skip)
            for endpoint in (rel.source, rel.target):
                if endpoint is None or not getattr(endpoint, "type", None):
                    continue
                key = (str(endpoint.id), endpoint.type)
                if key in known:
                    continue
                known.add(key)
                nodes.append(endpoint)

        for node in nodes:
            if getattr(node, "properties", None) is None:
                node.properties = {}
            _strip(node.properties, metadata_skip)
            node.properties.setdefault(name_property, str(node.id))
            if rid:
                node.properties.setdefault(rid_property, rid)
                if stamp_doc_id:
                    node.properties.setdefault("doc_id", rid)

    return rids


def _strip(properties: Optional[Dict[str, Any]], skip: FrozenSet[str]) -> None:
    """Remove *skip* keys from *properties* in place."""
    if not skip or not properties:
        return
    for key in [k for k in properties if k in skip]:
        del properties[key]
