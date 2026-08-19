"""Meeting-notes knowledge graph — a CocoIndex App with live source sync.

What this is
------------
The upstream CocoIndex example (``meeting_notes_graph_neo4j``) is Google Drive ->
Neo4j, with a second near-identical file for FalkorDB.  This produces the same
graph, but neither the source nor the target store is code:

* the **extraction** is a registered ``KGExtractor`` (``extractor.py``), so the
  same class also runs under the standard flexible-graphrag pipeline —
  ``KG_EXTRACTOR_BACKEND=./extractor.py:MeetingNotesExtractor``
* the **target** is whatever ``PG_GRAPH_DB`` / ``GRAPH_BACKEND`` select, written
  through the pipeline's own property-graph target — all 15 stores, no
  store-specific code here
* the **source** is ``NOTES_SOURCE`` — any of the 10 detector-backed sources

so this file is only the CocoIndex wiring, which is where CocoIndex's value is:

* **change detection** — only files whose content changed are reprocessed
* **memoisation** — an unchanged meeting section never reaches the LLM again
* **live watching** — ``cocoindex update -L`` keeps running and picks up edits
* **target-state reconciliation** — because writes go through the pipeline's
  target, deleting a note now retracts its meetings instead of leaving them
  behind (the earlier version of this file wrote with MERGE and could not)

Run
---
    # one pass over ./sample_notes
    cocoindex update mini_app.py

    # watch and keep the graph current; edit/add/delete a note and see it react
    cocoindex update -L mini_app.py

    # same code against a different source
    NOTES_SOURCE=google_drive cocoindex update -L mini_app.py

    # show the backend's own INFO logging as well
    NOTES_VERBOSE=1 cocoindex update mini_app.py

Environment: the same ``.env`` as everything else — target store from
``PG_GRAPH_DB``, LLM from ``LLM_PROVIDER``, source credentials from the usual
per-source variables.

===================  =====================================================
``NOTES_SOURCE``     source to watch (default ``filesystem`` → ``sample_notes/``)
``NOTES_PATH``       folder, when the source is ``filesystem``
``NOTES_RESOLVE``    entity resolution: ``llm`` (default), ``normalize``, ``none``
``NOTES_VERBOSE``    ``1`` to keep the backend's INFO logging
===================  =====================================================
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional

# The backend must be FIRST on sys.path: it ships its own ``langchain``
# package (langchain.graph.*) that must win over the installed distribution.
# An editable install already puts this directory on sys.path, but AFTER
# site-packages — so "add it if missing" silently does nothing.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_BACKEND = _REPO_ROOT / "flexible-graphrag"
_BACKEND_PATH = str(_BACKEND)
while _BACKEND_PATH in sys.path:
    sys.path.remove(_BACKEND_PATH)
sys.path.insert(0, _BACKEND_PATH)

import cocoindex as coco  # noqa: E402

# Expected, not a problem: importing the backend registers its own lifespan on
# top of ours.  Ours has already run and provided everything by then.  See the
# import note below for why the backend is imported at all.
warnings.filterwarnings(
    "ignore", message=".*Overriding the default lifespan function.*"
)

# IMPORTANT: nothing from ``cocoindex_integration`` may be imported at module
# level here.
#
# That package's ``__init__`` imports eagerly (deliberately — the import side
# effect is what registers the CocoIndex-native target types), and one of those
# imports builds the backend's own ``coco.App``, "GraphRAG_filesystem".  The CLI
# discovers apps by scanning loaded modules, so a module-level import makes it
# see two and refuse to run:
#
#     Error: Multiple apps found in 'mini_app.py':
#            GraphRAG_filesystem, MeetingNotesGraphAny
#
# The same applies to ``extractor.py``, which imports the base class from that
# package — so it is resolved by path inside app_main(), not imported here.
# Importing inside the app keeps discovery clean: at scan time this module
# defines exactly one app.  The backend's lifespan is registered later and
# CocoIndex logs "Overriding the default lifespan function" — harmless, because
# by then our lifespan has already run.

logger = logging.getLogger("meeting-notes-mini")

NOTES_DIR = Path(__file__).parent / "sample_notes"

#: The extractor, addressed the same way ``KG_EXTRACTOR_BACKEND`` would address
#: it.  A path rather than a module name because this directory is not an
#: installed package, and path specs need no sys.path setup.
EXTRACTOR_SPEC = f"{Path(__file__).parent / 'extractor.py'}:MeetingNotesExtractor"

# This directory is on sys.path (it is where the script lives), and the module
# is named meeting_notes rather than main precisely so this can be an ordinary
# import — a module called `main` here would be shadowed by the backend's
# FastAPI main.py, which is first on sys.path and would boot the API server.
import meeting_notes as _S1  # noqa: E402


# ---------------------------------------------------------------------------
# Context: config and the resolved target, established once per app run
# ---------------------------------------------------------------------------

#: Store name only — enough for change detection.  The target object itself is
#: not a ContextKey value: it holds live driver connections and is not
#: serialisable, so it lives in a module global built by the lifespan.
GRAPH_DB = coco.ContextKey[str]("graph_db", detect_change=True)

#: Extractor version, so a bumped extractor invalidates downstream work too.
EXTRACTOR = coco.ContextKey[str]("extractor", detect_change=True)

_CFG: Dict[str, Any] = {}
_PG_TARGET: Any = None
_EXTRACTOR_VERSION: str = ""


def _quiet_backend_logging() -> None:
    """Keep the console to this app's own progress.  ``NOTES_VERBOSE=1`` disables.

    Importing the backend pulls in a few dozen modules that log at INFO —
    adapter construction, embedding factories, the source detector, target
    registration — none of which is about *your* notes.

    Two mechanisms are needed.  ``logging.disable()`` is consulted when a record
    is emitted, so it also covers loggers that do not exist yet (the noisy ones
    are created during the imports below).  Lowering the existing loggers
    afterwards is what makes it stick, because the backend gives its modules
    explicit levels that a root-level change would not override.
    """
    if os.getenv("NOTES_VERBOSE", "").strip().lower() in ("1", "true", "yes"):
        return
    logging.disable(logging.INFO)


def _restore_own_logging() -> None:
    if os.getenv("NOTES_VERBOSE", "").strip().lower() in ("1", "true", "yes"):
        return
    logging.disable(logging.NOTSET)
    logging.getLogger().setLevel(logging.WARNING)  # covers loggers created later
    for _name in list(logging.root.manager.loggerDict):
        logging.getLogger(_name).setLevel(logging.WARNING)
    logger.setLevel(logging.INFO)


@coco.lifespan  # type: ignore[misc]
async def coco_lifespan(builder: Any):
    """Resolve pipeline config, the extractor, and the property-graph target."""
    global _CFG, _PG_TARGET, _EXTRACTOR_VERSION

    _quiet_backend_logging()

    from cocoindex_integration.functions.kg_extractors import resolve_kg_extractor
    from cocoindex_integration.pipeline import selectors as _sel
    from cocoindex_integration.pipeline.env_config import load_config_from_env
    from cocoindex_integration.pipeline.flexible_app import _resolve_pipeline_config

    # Exactly the config the standard pipeline would use, including its
    # backend downgrades (GRAPH_BACKEND=llamaindex is forced to langchain for
    # the LangChain-only stores).  Re-deriving it here would drift.
    _S1.load_settings()  # exports .env into os.environ
    _CFG = _resolve_pipeline_config(load_config_from_env())

    _cls = resolve_kg_extractor(EXTRACTOR_SPEC)
    _EXTRACTOR_VERSION = str(getattr(_cls, "version", "") or "")

    graph_db = str(_CFG.get("pg_graph_db", "none"))
    _PG_TARGET = _sel._pick_pg_target(_CFG)
    if _PG_TARGET is not None:
        # Creates constraints/indexes/labels.  The pipeline does this per run
        # too, and it is the noisiest step — adapters and embedding factories.
        await _PG_TARGET.setup()

    # Everything chatty has now been imported and constructed.
    _restore_own_logging()

    if _PG_TARGET is None:
        logger.warning(
            "mini_app: no property-graph target (PG_GRAPH_DB=%s) — nothing will be written",
            graph_db,
        )
    else:
        logger.info(
            "mini_app: target=%s backend=%s extractor=%s v%s",
            graph_db, _CFG.get("graph_backend"), _cls.__name__, _EXTRACTOR_VERSION,
        )

    builder.provide(GRAPH_DB, graph_db)
    builder.provide(EXTRACTOR, f"{_cls.__name__}@{_EXTRACTOR_VERSION}")
    yield


# ---------------------------------------------------------------------------
# Row building — KGResult (generic) -> KGTripleRow (what every target consumes)
# ---------------------------------------------------------------------------


#: Entity types that belong to ONE source document, and so can carry that
#: document's provenance as node properties.  A Person or Task appears across
#: many notes, so stamping a single note's filename on them would be a lie.
PROVENANCE_TYPES = {"Meeting"}


def _rows_from_kg(
    kg: Any,
    doc_id: str,
    file_name: str,
    file_path: str,
    source_type: str,
    modified_at: str = "",
) -> List[Any]:
    """Turn one ``KGResult`` into ``KGTripleRow``s.

    This is the only place provenance is attached, and it is attached here
    rather than inside the extractor — which is what lets the same extractor run
    unchanged whether the notes came from disk, S3 or SharePoint.

    It goes on in two forms:

    * on **every row** — ``doc_id``, ``file_name``, ``file_path``,
      ``source_type`` — which is what the pipeline uses to reconcile and delete
      a document's triples, and what the writers put on chunk nodes.
    * as **node properties** on document-scoped entities (``PROVENANCE_TYPES``).
      Entity nodes otherwise get only ``doc_id``/``ref_doc_id``, which are
      opaque uuid5 values — so without this a Meeting node carries no readable
      trace of which note it came from.  ``mini_app`` writes no chunk nodes, so
      there is nowhere else for it to live.

    Entity properties ride on both endpoints of every triple the entity appears
    in; the writers apply them to the nodes and resolve disagreements
    first-occurrence-wins.
    """
    from cocoindex_integration.connectors.rows import KGTripleRow

    ent_props: Dict[str, Dict[str, Any]] = {}
    ent_types: Dict[str, str] = {}
    for ent in (getattr(kg, "entities", None) or []):
        label = (getattr(ent, "label", "") or "").strip()
        if not label:
            continue
        etype = getattr(ent, "entity_type", "") or ""
        props = dict(getattr(ent, "properties", None) or {})
        if etype in PROVENANCE_TYPES:
            props.update({
                "note_file": file_name,
                "note_path": file_path,
                "source_type": source_type,
                "note_modified_at": modified_at,
            })
        ent_props.setdefault(label, props)
        ent_types.setdefault(label, etype)

    def props_json(label: str) -> str:
        p = ent_props.get(label)
        return json.dumps(p, sort_keys=True) if p else "{}"

    rows: List[Any] = []
    for i, t in enumerate(getattr(kg, "triples", None) or []):
        rel_props = getattr(t, "relation_properties", None) or {}
        rows.append(KGTripleRow(
            doc_id=doc_id,
            triple_index=i,
            subject=t.subject,
            subject_type=t.subject_type or ent_types.get(t.subject, ""),
            predicate=t.predicate,
            obj=t.obj,
            obj_type=t.obj_type or ent_types.get(t.obj, ""),
            file_name=file_name,
            file_path=file_path,
            source_type=source_type,
            ref_doc_id=doc_id,
            properties_json=json.dumps(rel_props, sort_keys=True) if rel_props else "{}",
            subject_properties_json=props_json(t.subject),
            obj_properties_json=props_json(t.obj),
        ))
    return rows


# ---------------------------------------------------------------------------
# Per-file worker, mounted once per source file
# ---------------------------------------------------------------------------


@coco.fn  # type: ignore[misc]
async def process_file(file: Any, resolve: str) -> None:
    """Extract every meeting in one note file and declare its slice of the graph.

    *file* is a ``FlexibleFile``, so this same worker serves every source —
    CocoIndex fingerprints it via ``__coco_memo_state__`` (mtime + etag) BEFORE
    calling ``read()``, so unchanged files never download.
    """
    from cocoindex_integration.connectors.flexible.base import note_target_pending
    from cocoindex_integration.connectors.flexible.property_graph import _FilePGSpec
    from cocoindex_integration.functions.kg_extraction import (
        _kg_result_from_json, extract_kg_custom,
    )
    from cocoindex_integration.pipeline import providers as _providers

    name = (
        getattr(file, "display_file_name", None)
        or getattr(file, "name", None)
        or str(getattr(file, "display_path", "note"))
    )
    path = str(getattr(file, "display_path", None) or name)
    # From the file, not from NOTES_SOURCE: the view knows what it actually
    # yielded, and the env var is only the *request*.
    source_type = str(getattr(file, "source_type", "") or
                      os.getenv("NOTES_SOURCE", "filesystem")).lower()
    modified_at = str(getattr(file, "modified_at", "") or "")

    try:
        raw = await file.read()
    except Exception as exc:  # noqa: BLE001 - one unreadable file must not stop the run
        logger.warning("mini_app: cannot read %s: %s", name, exc)
        return

    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)

    # Split here, not in the extractor, so the memo is per *meeting*: editing one
    # meeting re-extracts that section only, and the rest are cache hits that
    # never reach the LLM.  (The extractor splits again defensively, which is
    # what lets the standard pipeline feed it size-based chunks instead.)
    sections = _S1.split_meetings(text)
    if not sections:
        return

    triples: List[Any] = []
    entities: List[Any] = []
    for section in sections:
        try:
            # The same memoised dispatcher the standard pipeline calls.  The
            # version is an argument so a bumped extractor re-extracts rather
            # than serving the old implementation's cached triples.
            raw_json = await extract_kg_custom(
                section,
                extractor_spec=EXTRACTOR_SPEC,
                extractor_version=_EXTRACTOR_VERSION,
                llm_provider=str(_CFG.get("llm_provider", "") or ""),
                llm_config_json=str(_CFG.get("llm_config_json", "{}") or "{}"),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("mini_app: extraction failed in %s: %s", name, exc)
            continue
        kg = _kg_result_from_json(raw_json)
        triples.extend(kg.triples)
        entities.extend(kg.entities)

    if not triples:
        return

    if resolve != "none":
        # In a thread on purpose: resolve_entity_names() uses asyncio.run() for
        # the 'llm' strategy, which raises inside a running loop and is caught as
        # a silent downgrade to 'normalize'.  A worker thread has no running
        # loop, so llm resolution actually runs.
        await asyncio.to_thread(_resolve_in_place, triples, entities, resolve)

    # Same doc_id formula as the standard pipeline, so a file ingested either way
    # occupies one reconciliation slot rather than two.
    doc_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{source_type}:{path}"))

    rows = _rows_from_kg(
        _Merged(triples, entities), doc_id, name, path, source_type, modified_at,
    )

    if _PG_TARGET is None:
        logger.info("mini_app: %s -> %d triple(s), no target configured", name, len(rows))
        return

    provider = _providers._get_or_create_pg_provider(_PG_TARGET)
    if provider is None:
        logger.warning("mini_app: property-graph provider unavailable")
        return

    # declare_target_state (rather than a direct write) is what buys deletion:
    # CocoIndex diffs this document's declared state against what is stored, so
    # removing a meeting — or the whole file — retracts exactly its own triples
    # and leaves entities shared with other notes alone.
    note_target_pending(doc_id)
    coco.declare_target_state(
        provider.target_state(
            doc_id, _FilePGSpec(doc_id=doc_id, triples=rows, chunks=[]),
        )
    )
    logger.info(
        "mini_app: %s -> %d triple(s), %d entity(ies) into %s",
        name, len(rows), len({e.label for e in entities}), _CFG.get("pg_graph_db"),
    )


class _Merged:
    """Minimal KGResult-shaped holder for the merged per-file result."""

    def __init__(self, triples: List[Any], entities: List[Any]) -> None:
        self.triples = triples
        self.entities = entities


def _resolve_in_place(triples: List[Any], entities: List[Any], strategy: str) -> None:
    """Merge entity spellings within one file.  Runs in a worker thread.

    ``normalize`` folds accents, case and punctuation — it merges ``bob smith``
    into ``Bob Smith``.  Only ``llm`` merges ``Bob`` into ``Bob Smith``, and it
    needs ``uv pip install "cocoindex[entity_resolution]"`` (which pulls
    faiss-cpu); without it, or without both models, it falls back to
    ``normalize``.

    Per file either way, so a name that appears in two different notes can only
    be merged if both spellings occur in the same note.
    """
    from cocoindex_integration.entity_resolution import resolve_entity_names

    # Only Person and Task: Meeting ids are content-derived keys, and rewriting
    # them would break the link between a meeting and its own tasks.
    mergeable = {"Person", "Task"}
    names = [
        lbl
        for e in entities
        if (e.entity_type or "") in mergeable and (lbl := (e.label or "").strip())
    ]
    if not names:
        return

    llm = embed_model = None
    if strategy == "llm":
        # Both are required — resolve_entity_names downgrades to 'normalize' with
        # a warning if either is missing, which looks like "llm did nothing".
        from cocoindex_integration.functions.embedding import get_llamaindex_embedding
        from cocoindex_integration.functions.llm import get_llama_index_llm

        provider = str(_CFG.get("llm_provider", "") or os.getenv("LLM_PROVIDER", "openai"))
        try:
            llm = get_llama_index_llm(provider, json.loads(_CFG.get("llm_config_json", "{}") or "{}"))
            embed_model = get_llamaindex_embedding()
        except Exception as exc:  # noqa: BLE001 - resolution is an enhancement
            logger.warning("mini_app: cannot build models for llm resolution: %s", exc)

    canonical = resolve_entity_names(
        names, strategy=strategy, llm=llm, embed_model=embed_model
    )
    if not any(k != v for k, v in canonical.items()):
        return

    for t in triples:
        if (t.subject_type or "") in mergeable:
            t.subject = canonical.get(t.subject, t.subject)
        if (t.obj_type or "") in mergeable:
            t.obj = canonical.get(t.obj, t.obj)

    merged: Dict[str, Any] = {}
    for e in entities:
        e.label = canonical.get(e.label, e.label)
        existing = merged.get(e.label)
        if existing is None:
            merged[e.label] = e
        else:
            for k, v in (e.properties or {}).items():
                existing.properties.setdefault(k, v)
    entities[:] = list(merged.values())


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------


@coco.fn  # type: ignore[misc]
async def app_main(resolve: str = "") -> None:
    """Mount one worker per source file.

    The source is a config value, not code.  ``FlexibleMapView`` is a CocoIndex
    ``LiveMapView`` over any of the 10 detector-backed flexible-graphrag
    sources, so pointing this example at Google Drive instead of a local folder
    is ``NOTES_SOURCE=google_drive`` — no source-handling code here at all.
    Every one of them streams changes, so ``cocoindex update -L`` keeps the
    graph current whichever you pick.
    """
    # Lazy — see the import note at the top of this file.
    from cocoindex_integration.connectors.flexible._map_view import FlexibleMapView
    from cocoindex_integration.pipeline.flexible_app import (
        _build_source_config_from_env,
    )

    # Env rather than a CLI flag: `cocoindex update` owns the command line, so
    # NOTES_RESOLVE is how you switch strategy without editing this file.
    #
    # Default 'llm' because it is the one that actually merges "Bob" into
    # "Bob Smith"; 'normalize' only folds accents/case/punctuation and leaves
    # the graph with both. It degrades to 'normalize' with a warning when
    # cocoindex[entity_resolution] (faiss) is missing.
    resolve = (resolve or os.getenv("NOTES_RESOLVE", "llm")).lower()

    source_type = os.getenv("NOTES_SOURCE", "filesystem").lower()
    source_cfg = _build_source_config_from_env(source_type)
    if source_type == "filesystem":
        # This example watches ITS OWN notes folder, deliberately overriding the
        # backend's SOURCE_PATHS.  Inheriting that would silently point the
        # example at whatever the app happens to ingest (an Alfresco share, a
        # cloud bucket) and it would just report "nothing found".
        notes_path = os.getenv("NOTES_PATH") or str(NOTES_DIR)
        source_cfg = {**source_cfg, "paths": [notes_path]}

    logger.info("mini_app: source=%s", source_type)
    view = FlexibleMapView(source_type, source_cfg)
    await coco.mount_each(process_file, view, resolve)  # type: ignore[attr-defined]


app = coco.App(
    coco.AppConfig(name="MeetingNotesGraphAny"),
    app_main,
)
