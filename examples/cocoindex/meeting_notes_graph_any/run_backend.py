"""Start the flexible-graphrag server configured for this example.

    uv run run_backend.py

Then open the app UI and use it normally — upload notes, search them, ask
questions. Ingest runs the standard pipeline with this example's
``MeetingNotesExtractor``, so the meetings land in the property graph while the
vector and search stores get the text, which is what hybrid search, ai-query and
chat read.

Why a script rather than a documented command: the settings are a handful of
environment variables that must all be right together (see ``example_config``),
and one of them — ``WATCH_DIR`` — is genuinely dangerous to put in ``.env``.

    scripts/cleanup.py DELETES every file in WATCH_DIR.

So this points the server at ``./watch/`` beside the example, created on demand
and safe to lose, and sets it for this process only.  The shipped
``sample_notes/`` are never the watch directory and are never at risk.

Anything you set in your shell still wins, so a one-off override works:

    PG_GRAPH_DB=arcadedb uv run run_backend.py

Make path overrides **absolute**.  The server runs with its own directory as cwd
(start.py resolves ./uploads, ./cocoindex.db and .env relative to itself), so a
relative path set here resolves against flexible-graphrag/, not the directory you
launched from:

    ONTOLOGY_PATHS=meeting_notes_ontology.ttl     # -> flexible-graphrag/meeting_notes_ontology.ttl
                                                  #    ...which is not where it lives

Because shell variables win over this file, that also silently replaces the
absolute path example_config.py would have supplied.  Unset it to get the
example's own default back.

Add ``--notes`` to seed the watch directory with the sample notes so there is
something to search before you upload anything:

    uv run run_backend.py --notes
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

# The backend must be FIRST on sys.path — it ships its own ``langchain`` package
# that must win over the installed distribution.  See meeting_notes.py.
_HERE = Path(__file__).resolve().parent
_BACKEND = _HERE.parents[2] / "flexible-graphrag"
_BACKEND_PATH = str(_BACKEND)
while _BACKEND_PATH in sys.path:
    sys.path.remove(_BACKEND_PATH)
sys.path.insert(0, _BACKEND_PATH)
sys.path.insert(0, str(_HERE))          # so `import example_config` resolves

import example_config  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--notes", action="store_true",
        help="copy sample_notes/ into the watch directory before starting",
    )
    parser.add_argument(
        "--watch-dir", default=str(example_config.BACKEND_WATCH_DIR),
        help="directory the server watches (default: ./watch beside this file)",
    )
    args = parser.parse_args()

    watch_dir = Path(args.watch_dir).resolve()
    watch_dir.mkdir(parents=True, exist_ok=True)

    if args.notes:
        for note in sorted(example_config.SAMPLE_NOTES.glob("*.md")):
            shutil.copy2(note, watch_dir / note.name)
            print(f"  seeded {note.name}")

    # Build the child environment rather than mutating this process: the server
    # is a subprocess, and this keeps the same shell > script > .env precedence
    # the CLI entry point uses.
    env = dict(os.environ)
    for key, value in example_config.settings(watch_dir).items():
        if key not in os.environ:       # a shell variable still wins
            env[key] = value

    # flush: stdout is block-buffered when it is not a terminal, so without this
    # the banner appears *after* the server's own output.
    print("\nflexible-graphrag — meeting notes example")
    print(f"  extractor : {example_config.EXTRACTOR_SPEC}")
    print(f"  watching  : {watch_dir}")
    print(f"  graph     : {env.get('PG_GRAPH_DB', '(from .env)')}")
    print("  React UI  : http://localhost:5174")
    print("  Angular UI: http://localhost:4200")
    print("  Vue UI    : http://localhost:3000")
    print("              (start one in another terminal — see Frontend Setup in")
    print("               the main README)\n")
    print("  Drop a .md file into the watch directory, or upload one in the UI.")
    print("  Ctrl-C to stop.\n", flush=True)

    try:
        # cwd is the backend: start.py resolves ./uploads, ./cocoindex.db and
        # .env relative to it.  sys.executable, so `uv run run_backend.py` keeps
        # the server on the very same interpreter.
        return subprocess.call([sys.executable, "start.py"], cwd=str(_BACKEND), env=env)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
