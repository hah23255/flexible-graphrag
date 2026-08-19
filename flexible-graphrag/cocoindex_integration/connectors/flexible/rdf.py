"""``FlexibleRDFGraph`` — CocoIndex target for flexible-graphrag's RDF graph stores.

Supported RDF stores: Fuseki, GraphDB (Ontotext), Oxigraph, Neptune RDF.

Each declared row is a KG triple.  CocoIndex tracks rows per document; on
document deletion all triples for that document are removed via SPARQL
DELETE WHERE.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Optional

from cocoindex_integration.connectors.rows import RDFTripleRow  # noqa: F401 (re-exported)
from cocoindex_integration.connectors.flexible.base import (
    FlexibleConnector,
    FlexibleReconcileHandler,
    content_fingerprint,
    parse_entity_props,
)

logger = logging.getLogger(__name__)


class FlexibleRDFGraph(FlexibleConnector):
    """Custom CocoIndex target connector backed by flexible-graphrag's RDF adapters.

    Supported stores: ``"fuseki"``, ``"graphdb"``, ``"oxigraph"``, ``"neptune_rdf"``.
    The active store is determined by ``app_config.rdf_graph_db``.
    """

    def __init__(self, app_config) -> None:
        super().__init__(app_config)
        self._rdf_store = None
        self._graph_uri: Optional[str] = None
        self._pending: Dict[str, List[Any]] = {}

    async def setup(self) -> None:
        if self._rdf_store is not None:
            return  # idempotent — already initialised; shared across parallel files
        from adapters.graph.rdf_store_adapter import build_rdf_store_adapter
        self._rdf_store = build_rdf_store_adapter(config=self.app_config)

        # Named graph URI: base_namespace without trailing slash.
        # All adapters' store() accepts graph_uri=None (→ default context), so
        # we must pass this explicitly to land in the correct named graph.
        _base_ns: str = (
            getattr(self.app_config, "rdf_base_namespace", None)
            or "https://integratedsemantics.org/flexible-graphrag/kg/"
        )
        self._graph_uri = _base_ns.rstrip("/")

        store_type = getattr(self.app_config, "rdf_graph_db", "unknown")
        logger.info(
            "FlexibleRDFGraph: adapter ready for '%s', named graph URI: %s",
            store_type, self._graph_uri,
        )

    async def declare_row(self, row: Any) -> None:
        if row.doc_id not in self._pending:
            self._pending[row.doc_id] = []
        self._pending[row.doc_id].append(row)

    async def finalize(self) -> None:
        # Snapshot and clear before any await so concurrent declare_row calls
        # cannot cause "dictionary changed size during iteration".
        pending = self._pending
        self._pending = {}
        for doc_id, rows in pending.items():
            await self._export_to_rdf(doc_id, rows)

    async def _export_to_rdf(self, doc_id: str, rows) -> None:
        """Export buffered triple rows to the RDF store.

        Accepts either ``RDFTripleRow`` (subject_label/predicate_label/obj_label)
        or ``KGTripleRow`` (subject/predicate/obj) — whichever was declared.
        """
        if not self._rdf_store or not rows:
            return
        try:
            from rdf.kg_to_rdf_converter import KGToRDFConverter
            from llama_index.core.graph_stores.types import EntityNode, Relation

            entity_nodes = []
            relations = []
            seen: Dict[str, EntityNode] = {}

            for row in rows:
                # Accept both KGTripleRow (subject/predicate/obj) and
                # RDFTripleRow (subject_label/predicate_label/obj_label).
                subj = getattr(row, "subject", None) or getattr(row, "subject_label", "")
                pred = getattr(row, "predicate", None) or getattr(row, "predicate_label", "")
                obj  = getattr(row, "obj", None) or getattr(row, "obj_label", "")
                subj_type = getattr(row, "subject_type", "") or ""
                obj_type  = getattr(row, "obj_type", "") or ""

                # properties: KGTripleRow stores JSON string; RDFTripleRow stores dict
                _props_raw = getattr(row, "properties_json", None)
                if _props_raw is not None:
                    import json as _json
                    try:
                        row_props: Dict[str, Any] = _json.loads(_props_raw) if _props_raw else {}
                    except Exception:
                        row_props = {}
                else:
                    row_props = dict(getattr(row, "properties", {}) or {})

                # Ontology-declared entity properties become datatype triples
                # downstream (kg_to_rdf_converter), which is their natural home
                # in RDF: owl:DatatypeProperty is exactly what they came from.
                # First occurrence wins, matching the property-graph writers.
                if subj not in seen:
                    _sp = {"ref_doc_id": doc_id}
                    _sp.update(parse_entity_props(
                        getattr(row, "subject_properties_json", "{}")))
                    seen[subj] = EntityNode(
                        name=subj,
                        label=subj_type or "Entity",
                        properties=_sp,
                    )
                if obj not in seen:
                    _op = {"ref_doc_id": doc_id}
                    _op.update(parse_entity_props(
                        getattr(row, "obj_properties_json", "{}")))
                    seen[obj] = EntityNode(
                        name=obj,
                        label=obj_type or "Entity",
                        properties=_op,
                    )
                relations.append(Relation(
                    source_id=subj,
                    target_id=obj,
                    label=pred,
                    properties={**row_props, "ref_doc_id": doc_id},
                ))

            entity_nodes = list(seen.values())

            # KGToRDFConverter.convert() expects LlamaIndex nodes whose metadata
            # carries KG_NODES_KEY and KG_RELATIONS_KEY — build one synthetic node.
            from llama_index.core.schema import TextNode
            from llama_index.core.graph_stores.types import KG_NODES_KEY, KG_RELATIONS_KEY

            synthetic_node = TextNode(text="")
            synthetic_node.metadata[KG_NODES_KEY] = entity_nodes
            synthetic_node.metadata[KG_RELATIONS_KEY] = relations
            synthetic_node.metadata["ref_doc_id"] = doc_id

            converter = KGToRDFConverter()
            _, turtle_data = converter.convert(nodes=[synthetic_node])

            # Pass graph_uri so data goes into the named graph, not the default context.
            await asyncio.to_thread(self._rdf_store.store, turtle_data, self._graph_uri)
            logger.info(
                "FlexibleRDFGraph: exported %d triples for doc %s (graph=%s)",
                len(rows), doc_id, self._graph_uri,
            )
        except Exception as exc:
            logger.error("FlexibleRDFGraph: export failed for doc '%s': %s", doc_id, exc)
            raise

    async def delete_row(self, doc_id: str) -> None:
        """Delete all RDF triples for a doc_id via SPARQL DELETE WHERE."""
        if self._rdf_store is not None:
            try:
                await asyncio.to_thread(self._rdf_store.delete, doc_id, self._graph_uri)
            except Exception as exc:
                logger.error("FlexibleRDFGraph: delete failed for '%s': %s", doc_id, exc)

    async def teardown(self) -> None:
        self._rdf_store = None


# ─────────────────────────────────────────────────────────────────────────────
# CocoIndex TargetHandler for RDF graph stores
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class _FileRDFSpec:
    """All RDF triple rows that should exist for one source document.

    Rows may be either ``RDFTripleRow`` or ``KGTripleRow`` objects — the
    underlying ``FlexibleRDFGraph._export_to_rdf()`` uses duck-typed getattr
    access so both are accepted.
    """
    doc_id: str
    rows: List[Any]  # List[KGTripleRow | RDFTripleRow]


@dataclass(frozen=True)
class _FileRDFTrackingRecord:
    """SHA-256 fingerprint of a document's RDF triple content (change detection)."""
    fingerprint: bytes


class _FileRDFAction(NamedTuple):
    """Action produced by reconcile() and consumed by _apply_actions()."""
    doc_id: str
    rows: Optional[List[Any]]  # None → delete; non-None → upsert
    delete_first: bool = False  # True when modifying existing doc


def _rdf_fingerprint(rows: List[Any]) -> bytes:
    """Stable SHA-256 over sorted triple content (subject|predicate|obj)."""
    def _key(r: Any):
        return (
            getattr(r, "subject", None) or getattr(r, "subject_label", ""),
            getattr(r, "predicate", None) or getattr(r, "predicate_label", ""),
            getattr(r, "obj", None) or getattr(r, "obj_label", ""),
        )

    def _units():
        for row in sorted(rows, key=_key):
            subj = getattr(row, "subject", None) or getattr(row, "subject_label", "")
            pred = getattr(row, "predicate", None) or getattr(row, "predicate_label", "")
            obj  = getattr(row, "obj", None) or getattr(row, "obj_label", "")
            yield f"{subj}|{pred}|{obj}".encode("utf-8", errors="replace")

    return content_fingerprint(_units())


class FlexibleRDFHandler(FlexibleReconcileHandler):
    """CocoIndex ``TargetHandler`` for flexible-graphrag RDF graph stores."""

    label = "RDF data"

    def _fingerprint(self, desired: Any) -> bytes:
        return _rdf_fingerprint(desired.rows)

    def _make_delete_action(self, key: str) -> _FileRDFAction:
        return _FileRDFAction(doc_id=key, rows=None)

    def _make_upsert_action(self, desired: Any, delete_first: bool) -> _FileRDFAction:
        return _FileRDFAction(doc_id=desired.doc_id, rows=desired.rows, delete_first=delete_first)

    def _make_tracking_record(self, fp: bytes) -> _FileRDFTrackingRecord:
        return _FileRDFTrackingRecord(fingerprint=fp)

    def _action_is_delete(self, action: Any) -> bool:
        return action.rows is None

    def _action_size(self, action: Any) -> str:
        return f"{len(action.rows)} triple(s)"

    async def _declare_upsert(self, action: Any) -> None:
        for row in action.rows:
            await self._target.declare_row(row)
