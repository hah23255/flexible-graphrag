"""Phase 2 — the **standard** flexible-graphrag CocoIndex pipeline, with this
example's extractor plugged in.

Where ``mini_app.py`` is a small purpose-built app (source → extract → graph),
this runs the real production pipeline: parse (PDF/DOCX/…), chunk, embed, and
write to the vector store, the search index, the RDF store and the property
graph — with ``MeetingNotesExtractor`` supplying the triples instead of the
built-in LLM extractor.

    cocoindex update pipeline_app.py
    cocoindex update -L pipeline_app.py

The point of this file is that it is *only configuration*.  There is no copy of
the pipeline here: it sets the handful of settings the meeting-notes format
needs, then re-exports the pipeline's own app.  Running the same thing purely
from the environment is equivalent —

    ENTITY_RESOLUTION=llm DOCUMENT_PARSER=liteparse CHUNKER_BACKEND=cocoindex \\
    COCOINDEX_SPLITTER_TYPE=separator COCOINDEX_SEPARATORS='\\n{2,}#{1,2}\\s+' \\
    CHUNK_SIZE=600 CHUNK_OVERLAP=0 \\
    KG_EXTRACTOR_BACKEND=./extractor.py:MeetingNotesExtractor \\
    cocoindex update cocoindex_integration/pipeline/app.py

— which is exactly the delta this file encodes, and why each setting is needed:

``DOCUMENT_PARSER=liteparse``
    The pipeline *parses* documents, and docling normalises markdown away —
    it keeps the blank lines but strips the ``#`` markers, so a heading-based
    split finds nothing and the whole file becomes one meeting.  liteparse reads
    ``.md``/``.txt`` through unchanged.  ``mini_app.py`` never hits this because
    it reads raw bytes and does not parse at all.

``CHUNKER_BACKEND=cocoindex`` + ``separator`` + the heading regex
    Chunk per meeting rather than per 2048 characters, so the memo is per
    meeting: edit one and only that section is re-extracted.

``CHUNK_SIZE=600``
    The separator splitter emits one fragment per section and then *packs*
    fragments up to ``CHUNK_SIZE``.  Leave it at 2048 and all four meetings pack
    back into a single chunk.

``ENTITY_RESOLUTION=llm``
    Extraction is per chunk, so "Bob" in one meeting and "Bob Smith" in another
    are only comparable afterwards.  Needs
    ``uv pip install "cocoindex[entity_resolution]"``; without it this degrades
    to ``normalize`` with a warning rather than failing.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# The backend must be FIRST on sys.path — it ships its own ``langchain`` package
# that must win over the installed distribution.  See meeting_notes.py.
_HERE = Path(__file__).resolve().parent
_BACKEND_PATH = str(_HERE.parents[2] / "flexible-graphrag")
while _BACKEND_PATH in sys.path:
    sys.path.remove(_BACKEND_PATH)
sys.path.insert(0, _BACKEND_PATH)


sys.path.insert(0, str(_HERE))          # so `import example_config` resolves
import example_config  # noqa: E402

# Shared with run_backend.py so the two entry points cannot drift.  Precedence is
# shell variable > example_config > .env; see apply_in_process() for why that
# ordering has to be established before anything reads configuration.
#
# WATCH_DIR is sample_notes here because the CLI only ever *reads* it.  The
# server uses its own directory instead — /api/ingest copies uploads into
# WATCH_DIR, and scripts/cleanup.py deletes every file in it.
example_config.apply_in_process(example_config.SAMPLE_NOTES)

# Keep this example's memo beside the example rather than in the backend
# directory, so running it never invalidates the backend's own cached work.
os.environ.setdefault("COCOINDEX_DB", str(_HERE / "cocoindex.db"))


# Import AFTER the environment is set: the pipeline reads its configuration at
# import time and builds the app as a side effect.  Re-export that app rather
# than building a second one — the CocoIndex CLI discovers apps by scanning
# loaded modules and refuses to run when it finds more than one.
#
# import_module by name, not ``from ... import app``: the package's __init__
# re-exports the App *instance* under the name ``app``, shadowing the submodule
# of the same name, so the ``from`` form hands back an App and ``.app`` on it
# raises AttributeError.
import importlib  # noqa: E402

app = importlib.import_module("cocoindex_integration.pipeline.app").app
