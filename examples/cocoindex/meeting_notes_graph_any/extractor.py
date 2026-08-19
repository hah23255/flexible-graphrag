"""The meeting-notes extraction, as a registered ``KGExtractor``.

This is the phase-1 shape of the example: the domain logic is a *plugin*, not an
app.  It produces generic ``KGResult`` triples, so it can be driven by

* ``mini_app.py`` — this example's own CocoIndex app, or
* the standard flexible-graphrag CocoIndex pipeline, via
  ``KG_EXTRACTOR_BACKEND=<path to this file>:MeetingNotesExtractor``

and in both cases the graph is written by whichever target ``PG_GRAPH_DB`` /
``GRAPH_BACKEND`` select — no store-specific code here.

Graph shape, matching upstream::

    (Person)-[:ATTENDED {is_organizer}]->(Meeting)
    (Meeting)-[:DECIDED]->(Task)
    (Task)-[:ASSIGNED_TO]->(Person)
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

# This file is loadable *by path* (KG_EXTRACTOR_BACKEND=/…/extractor.py:Class).
# importlib does not put a file's own directory on sys.path, so `import
# meeting_notes` below would fail with ModuleNotFoundError when loaded that way.
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

# Import this BEFORE anything from cocoindex_integration: meeting_notes' module
# body moves the backend directory to the FRONT of sys.path, which is what makes
# `cocoindex_integration` importable when this file is loaded standalone (and
# what keeps the backend's own `langchain` package winning over the installed
# distribution — see the long comment in meeting_notes.py).
import meeting_notes as _mn  # noqa: E402

from cocoindex_integration.functions.kg_extraction import (  # noqa: E402
    KGEntity,
    KGResult,
    KGTriple,
)
from cocoindex_integration.functions.kg_extractors import (  # noqa: E402
    KGExtractionContext,
    KGExtractor,
    register_kg_extractor,
)

logger = logging.getLogger("meeting-notes-extractor")

PERSON = "Person"
MEETING = "Meeting"
TASK = "Task"


#: Documents opt in explicitly with a ``Type:`` line.  Every meeting section
#: carries one, rather than a single marker at the top of the file, because
#: extraction is per *chunk*: a file-level tag would be visible only in the first
#: chunk and every later one would look untagged.  A plain text line also
#: survives document conversion, where the ``##`` heading marker does not.
_TYPE_RE = re.compile(r"^\s*Type:\s*(\S+)\s*$", re.MULTILINE | re.IGNORECASE)

#: The type this extractor claims.  The line is *parsed* rather than matched
#: against one literal so the same convention extends to other document types
#: later — ``Type: Invoice`` and friends can be claimed by their own extractors,
#: with anything unclaimed still falling through to the built-in.
DOC_TYPE = "MeetingNote"


def document_type(text: str) -> str:
    """The declared ``Type:`` of this text, or ``""`` when it declares none."""
    match = _TYPE_RE.search(text or "")
    return match.group(1).strip() if match else ""


def looks_like_meeting_notes(text: str) -> bool:
    """True when this text opts in to this extractor.

    An explicit tag rather than a heuristic: guessing from shape would claim
    documents that merely mention a date, and this extractor's schema *requires*
    a date, a note and an organiser — so on anything else the LLM invents a
    meeting rather than declining.
    """
    return document_type(text).lower() == DOC_TYPE.lower()


def section_title(section_text: str) -> str:
    """The meeting's own heading, verbatim.

    ``split_meetings`` consumes the ``##`` marker, so the title is simply the
    first non-empty line.  Taken from the text rather than asked of the LLM: the
    heading is *data*, and round-tripping it through a model rewords it
    ("Q3 Planning Kickoff" comes back as "Q3 planning kickoff meeting").
    """
    for line in (section_text or "").splitlines():
        line = line.strip().lstrip("#").strip()
        if line:
            return line
    return ""


def section_body(section_text: str) -> str:
    """Everything after the heading, verbatim."""
    lines = (section_text or "").splitlines()
    for i, line in enumerate(lines):
        if line.strip():
            return "\n".join(lines[i + 1:]).strip()
    return ""


def meeting_id(time_iso: str, title_or_organizer: str) -> str:
    """A stable, **content-derived** id for one meeting.

    Upstream keys meetings by ``(note_file, time)``.  That cannot be done here on
    purpose: an extractor gets no document provenance, because an id built from
    ``file_name`` mints a different node when the same note arrives from
    SharePoint rather than the filesystem — duplicate meetings for one meeting.
    Provenance still reaches the graph, just from ``KGTripleRow`` rather than
    from the id (see the note in ``KGExtractionContext``).

    Date + title instead — the title is verbatim from the heading, so unlike an
    LLM-extracted organiser name it cannot drift between runs.  Falls back to the
    organiser when there is no heading, which happens when the standard pipeline
    feeds a size-based chunk that starts mid-section.

    Editing a note's body keeps the same Meeting node and updates its
    properties, which is what you want for "same meeting, corrected notes".  The
    trade is that two genuinely different meetings with the same date and title
    merge into one.
    """
    return f"{(time_iso or 'undated').strip()}#{(title_or_organizer or 'unknown').strip()}"


@register_kg_extractor("meeting_notes")
class MeetingNotesExtractor(KGExtractor):
    """Extract Meeting/Person/Task triples from a chunk of meeting notes.

    Ontology-independent by design: this *is* the schema, so ``ctx.schema`` is
    ignored.  Point ``USE_ONTOLOGY`` at the bundled ``.ttl`` instead if you want
    the ontology-guided graph — the two coexist (``Meeting`` vs ``MEETING``).
    """

    #: Bump on any behaviour change — extraction is memoised on
    #: (chunk_text, spec, version), so edits to this class are otherwise
    #: invisible and you keep reading the previous version's triples.
    version = "1"

    async def extract(self, chunk_text: str, ctx: KGExtractionContext) -> KGResult:
        # A chunk may hold one section (mini_app splits first, to keep the memo
        # per meeting) or several (the standard pipeline chunks by size).  Split
        # again so both callers behave the same; falling back to the whole chunk
        # covers a chunk that starts mid-section and has no heading.
        sections = _mn.split_meetings(chunk_text) or [chunk_text]

        # Only sections that opt in are ours.  The rest go to the built-in
        # extractor rather than being dropped, so a mixed corpus — notes beside
        # specs beside invoices — still gets a graph for all of it, and a chunk
        # holding both is handled correctly rather than all-or-nothing.
        mine = [s for s in sections if looks_like_meeting_notes(s)]
        theirs = [s for s in sections if not looks_like_meeting_notes(s)]

        triples: List[KGTriple] = []
        # Keyed by label so the same Person across sections is one entity, and
        # first-write-wins on properties (matching how the writers reconcile a
        # conflicting entity type).
        entities: Dict[str, KGEntity] = {}

        def add_entity(label: str, etype: str, props: Dict[str, Any] | None = None) -> str:
            label = (label or "").strip()
            if not label:
                return ""
            existing = entities.get(label)
            if existing is None:
                entities[label] = KGEntity(
                    label=label, entity_type=etype, properties=dict(props or {})
                )
            elif props:
                for k, v in props.items():
                    existing.properties.setdefault(k, v)
            return label

        if theirs:
            # One call for everything not claimed here.  ctx.builtin() runs the
            # ordinary llamaindex/langchain extraction with this run's ontology
            # and provider, so those passages come out exactly as they would
            # have with no custom extractor configured.
            try:
                other = await ctx.builtin("\n\n".join(theirs))
                triples.extend(other.triples)
                for ent in (other.entities or []):
                    add_entity(ent.label, ent.entity_type, ent.properties)
            except Exception as exc:  # noqa: BLE001 - delegation must not break ours
                logger.warning("MeetingNotesExtractor: built-in delegation failed: %s", exc)

        # LlamaIndex on purpose, whatever the pipeline uses elsewhere: this needs
        # structured_predict() for pydantic output, and ctx.llamaindex_llm()
        # builds from LLM_PROVIDER directly rather than from the configured
        # framework — so an all-LangChain run still gets a working LLM here.
        # (ctx.langchain_llm() is there for extractors that prefer the other side.)
        llm = ctx.llamaindex_llm() if mine else None
        if mine and llm is None:
            logger.warning("MeetingNotesExtractor: no LLM for provider=%s", ctx.llm_provider)

        for section in (mine if llm is not None else []):
            # extract_meeting is sync (llm.structured_predict) — keep the event
            # loop free so concurrent files still overlap.
            extracted = await asyncio.to_thread(_mn.extract_meeting, llm, section)
            if extracted is None:
                continue

            organizer = (extracted.organizer.name or "").strip()
            title = section_title(section)
            mid = meeting_id(extracted.time, title or organizer)
            add_entity(mid, MEETING, {
                "title": title,                    # verbatim heading
                "time": extracted.time,            # LLM, normalised to ISO
                "note": extracted.note,            # LLM summary — reworded by design
                "text": section_body(section),     # verbatim body, nothing lost
            })

            if organizer:
                add_entity(organizer, PERSON)
                triples.append(KGTriple(
                    subject=organizer, subject_type=PERSON,
                    predicate="ATTENDED",
                    obj=mid, obj_type=MEETING,
                    relation_properties={"is_organizer": True},
                ))

            for p in extracted.participants:
                name = (p.name or "").strip()
                if not name or name == organizer:
                    continue  # the model sometimes repeats the organiser
                add_entity(name, PERSON)
                triples.append(KGTriple(
                    subject=name, subject_type=PERSON,
                    predicate="ATTENDED",
                    obj=mid, obj_type=MEETING,
                    relation_properties={"is_organizer": False},
                ))

            for t in extracted.tasks:
                desc = (t.description or "").strip()
                if not desc:
                    continue
                add_entity(desc, TASK)
                triples.append(KGTriple(
                    subject=mid, subject_type=MEETING,
                    predicate="DECIDED",
                    obj=desc, obj_type=TASK,
                ))
                for a in t.assigned_to:
                    assignee = (a.name or "").strip()
                    if not assignee:
                        continue
                    add_entity(assignee, PERSON)
                    triples.append(KGTriple(
                        subject=desc, subject_type=TASK,
                        predicate="ASSIGNED_TO",
                        obj=assignee, obj_type=PERSON,
                    ))

        return KGResult(triples=triples, entities=list(entities.values()))
