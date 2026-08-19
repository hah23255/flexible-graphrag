"""The settings this example needs, defined once.

Both entry points read this, so they cannot drift apart:

* ``pipeline_app.py``  — ``cocoindex update pipeline_app.py`` (CLI)
* ``run_backend.py``   — starts the app server so the UI can drive it

Each setting exists for a reason; see the table in the README, and
``pipeline_app.py``'s docstring for the long form.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

HERE = Path(__file__).resolve().parent

#: The notes that ship with the example.  Read-only.
SAMPLE_NOTES = HERE / "sample_notes"

#: Where the *server* watches for notes.  Deliberately NOT ``sample_notes`` —
#: ``/api/ingest`` copies uploads into ``WATCH_DIR``, and ``scripts/cleanup.py``
#: deletes every file in it.  Pointing it at ``sample_notes`` would put the
#: shipped notes one cleanup away from deletion.
BACKEND_WATCH_DIR = HERE / "watch"

#: The extractor, addressed the way ``KG_EXTRACTOR_BACKEND`` addresses it: a
#: file path plus a class, which needs no install and no sys.path setup.
EXTRACTOR_SPEC = f"{HERE / 'extractor.py'}:MeetingNotesExtractor"


def settings(watch_dir: Path | str) -> Dict[str, str]:
    """The environment delta for running this example."""
    return {
        "PIPELINE_BACKEND":        "cocoindex",
        # Conversion with docling strips markdown '#' markers, so heading
        # splitting finds nothing and the whole file becomes one meeting.
        "DOCUMENT_PARSER":         "liteparse",
        # Chunk per meeting, so the memo is per meeting.
        "CHUNKER_BACKEND":         "cocoindex",
        "COCOINDEX_SPLITTER_TYPE": "separator",
        "COCOINDEX_SEPARATORS":    r"\n{2,}#{1,2}\s+",
        # The separator splitter packs fragments up to CHUNK_SIZE; leave it at
        # 2048 and all four meetings pack back into a single chunk.
        "CHUNK_SIZE":              "600",
        "CHUNK_OVERLAP":           "0",
        # Extraction is per chunk, so "Bob" and "Bob Smith" are only comparable
        # afterwards.  Needs cocoindex[entity_resolution]; degrades to
        # 'normalize' with a warning without it.
        "ENTITY_RESOLUTION":       "llm",
        "KG_EXTRACTOR_BACKEND":    EXTRACTOR_SPEC,
        # Extraction is NOT ontology-guided — this extractor is the schema.
        "USE_ONTOLOGY":            "false",
        # …but the GraphDB RDF retriever is a separate consumer of the same
        # setting, and it is not gated on USE_ONTOLOGY (retriever_setup.py reads
        # ONTOLOGY_DIR / ONTOLOGY_PATHS / ONTOLOGY_PATH directly).  Without a
        # local ontology file OntotextGraphDBGraph cannot build its schema and
        # logs "RDF graph retriever not available", dropping GraphDB out of the
        # fusion retriever.  Pointing it at this example's own .ttl means the
        # SPARQL it generates is described in Meeting/Person/Task terms rather
        # than whatever unrelated ontology .env happens to name.
        "ONTOLOGY_PATHS":          str(HERE / "meeting_notes_ontology.ttl"),
        "DATA_SOURCE":             "filesystem",
        "WATCH_DIR":               str(watch_dir),
    }


def apply_in_process(watch_dir: Path | str, load_env: bool = True) -> Dict[str, str]:
    """Put those settings in ``os.environ``.  Returns what was applied.

    Precedence is **shell variable > this file > .env**.

    The shell snapshot has to be taken *before* ``.env`` is loaded, because
    loading it puts its values into ``os.environ`` too and afterwards there is no
    way to tell the two apart.  ``setdefault`` alone would be wrong: ``.env``
    legitimately sets ``DOCUMENT_PARSER=docling`` and
    ``CHUNKER_BACKEND=llamaindex``, and those must lose to this file or the
    example silently reverts to whole-file chunking.
    """
    shell_keys = frozenset(os.environ)

    if load_env:
        import meeting_notes as _mn  # noqa: PLC0415 - also applies the SSL patch

        _mn.load_settings()          # exports the backend .env into os.environ

    applied = settings(watch_dir)
    for key, value in applied.items():
        if key not in shell_keys:
            os.environ[key] = value
    return applied
