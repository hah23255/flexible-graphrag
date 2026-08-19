"""Meeting-notes extraction — the shared library for this example.

Three things live here, and nothing else:

* the extraction schema (identical to the upstream CocoIndex example) and its
  prompt — ``ExtractedMeeting`` and friends
* ``split_meetings()``, the ``\\n\\n##?`` section splitter
* ``load_settings()``, which makes the backend ``.env`` reachable whatever
  directory you launch from

Turning extracted meetings into a graph is **not** here.  ``extractor.py`` emits
generic ``KGResult`` triples and the pipeline's own writers put them in whichever
store is configured, so this file no longer builds ``GraphDocument``s or talks to
a store adapter.

This is a plain module, not an entry point — run ``mini_app.py`` (short app) or
``pipeline_app.py`` (the full pipeline).

Graph shape, matching upstream:

    (Person)-[:ATTENDED {is_organizer}]->(Meeting)
    (Meeting)-[:DECIDED]->(Task)
    (Task)-[:ASSIGNED_TO]->(Person)
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from typing import Any, List, Optional

# flexible-graphrag lives beside the repo root, not on sys.path by default.
#
# It must come FIRST, not merely be present.  The backend ships its own
# ``langchain/`` package (``langchain.graph.id_sanitizer``, the store adapters)
# which deliberately shadows the installed ``langchain`` distribution — that
# works for the backend because it runs with its own directory as cwd.
#
# An editable install already puts this directory on sys.path via a .pth file,
# but *after* site-packages.  So a simple "add it if missing" check silently
# does nothing and the installed langchain wins, producing
# ``ModuleNotFoundError: No module named 'langchain.graph'`` the moment
# anything (LiteLLM, imported via the LLM factory) pulls in plain ``langchain``
# first.  Move it to the front instead of just ensuring membership.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_BACKEND = _REPO_ROOT / "flexible-graphrag"
_BACKEND_PATH = str(_BACKEND)
while _BACKEND_PATH in sys.path:
    sys.path.remove(_BACKEND_PATH)
sys.path.insert(0, _BACKEND_PATH)

# Neutralise nest_asyncio on Python 3.14+, BEFORE anything imports LlamaIndex.
#
# llama_index/core/async_utils.py calls nest_asyncio.apply() unconditionally.  Its
# patched loop.run_until_complete() runs coroutines outside a Task, and from 3.12
# asyncio.wait_for()/asyncio.timeout() require one — so document parsing dies with
#
#     RuntimeError: Timeout should be used inside a task
#     -> "No documents were successfully processed"
#
# and asyncio.run() then fails again on the way out, in
# Runner.close() -> shutdown_default_executor().
#
# The FastAPI backend guards this the same way (backend.py: only apply below
# 3.14) and so does the Langflow bundle (_fg_shared.py).  Neither covers a plain
# script, which is what these examples are.  The backend also gets away with it
# because uvicorn keeps a long-lived loop and never calls asyncio.run().
if sys.version_info >= (3, 14):
    try:
        import nest_asyncio as _nest_asyncio

        _nest_asyncio.apply = lambda *a, **kw: None  # type: ignore[assignment]
    except ImportError:
        pass

import pydantic  # noqa: E402

# Accept the re-signed certificates that TLS-inspecting antivirus / corporate
# proxies present.  The backend does this on import; these examples deliberately
# do NOT import the backend's main.py (it would boot the whole FastAPI app), so
# they call the shared helper directly.  Without it, LLM calls die with
# "APIConnectionError: Connection error." whose real cause is
# "certificate verify failed: Basic Constraints of CA cert not marked critical".
# Intermittent by nature — it depends on whether the inspecting software is on
# the path for a given connection — so the app can work while a script fails.
try:
    from ssl_compat import patch_ssl_context as _patch_ssl  # noqa: E402

    _patch_ssl()
except Exception:  # noqa: BLE001 - never block the example from starting
    pass

logger = logging.getLogger("meeting-notes")

#: The backend's ``.env``.  ``Settings`` declares ``env_file=".env"`` as a
#: *relative* path, which pydantic resolves against the process working
#: directory — so running this script from its own directory would silently load
#: pydantic defaults (neo4j, no API keys) instead of your configuration, with no
#: error to tell you.  Passing the absolute path makes the example work from any
#: directory.
_ENV_FILE = _BACKEND / ".env"


_env_loaded = False


def _ensure_env_loaded() -> None:
    """Export the backend .env into ``os.environ`` (what the backend's main.py does).

    Two separate mechanisms read that file and BOTH are needed:

    * ``Settings(_env_file=...)`` parses it into pydantic fields.
    * ``os.environ`` — because ``Settings`` builds ``llm_config`` with
      ``os.getenv("OPENAI_API_KEY")`` (config.py), and the store adapters read
      credentials the same way.  Pydantic does *not* export what it parses, so
      without this the API key silently comes back ``None`` and every LLM call
      fails with "Missing credentials".

    ``flexible-graphrag/main.py`` calls ``load_dotenv()`` for exactly this
    reason; it just relies on the cwd being the backend directory.
    """
    global _env_loaded
    if _env_loaded or not _ENV_FILE.is_file():
        return
    try:
        from dotenv import load_dotenv  # type: ignore[import-untyped]

        # override=False so real shell variables still win, matching main.py.
        load_dotenv(str(_ENV_FILE), override=False)
        _env_loaded = True
    except ImportError:
        logger.warning("python-dotenv not installed — .env will not reach os.environ")


def load_settings() -> Any:
    """Backend ``Settings``, loaded from the backend .env whatever the cwd is."""
    from config import Settings  # type: ignore[import-untyped]

    _ensure_env_loaded()
    if _ENV_FILE.is_file():
        return Settings(_env_file=str(_ENV_FILE))
    logger.warning("No .env at %s — using defaults", _ENV_FILE)
    return Settings()


# ---------------------------------------------------------------------------
# Extraction schema — identical to upstream, so the comparison stays honest
# ---------------------------------------------------------------------------


class ExtractedPerson(pydantic.BaseModel):
    name: str = pydantic.Field(
        description="Full name of the person, as written in the note."
    )


class ExtractedTask(pydantic.BaseModel):
    description: str = pydantic.Field(
        description="Concise, standalone description of the task or action item."
    )
    assigned_to: List[ExtractedPerson] = pydantic.Field(
        default_factory=list, description="People the task is assigned to."
    )


class ExtractedMeeting(pydantic.BaseModel):
    time: str = pydantic.Field(
        description="Date of the meeting in ISO format (YYYY-MM-DD)."
    )
    note: str = pydantic.Field(
        description="A brief summary or notes from the meeting section."
    )
    organizer: ExtractedPerson = pydantic.Field(
        description="The person who organized or led the meeting."
    )
    participants: List[ExtractedPerson] = pydantic.Field(
        default_factory=list,
        description=(
            "People who attended the meeting other than the organizer. "
            "Do not include the organizer here."
        ),
    )
    tasks: List[ExtractedTask] = pydantic.Field(
        default_factory=list,
        description="Action items or tasks decided in the meeting.",
    )


EXTRACT_PROMPT = """\
You are an expert at reading meeting notes and extracting structured information.

Given a single meeting section (Markdown), extract:
- The meeting date (look for a date in the heading or body; required, ISO YYYY-MM-DD).
- A brief note summarizing what the meeting was about.
- The organizer (the person who ran the meeting). If unclear, pick the person
  who appears most central to the meeting.
- Participants other than the organizer.
- Tasks or action items decided, including who they are assigned to.

Return only what is supported by the text. Use full names where available.

Meeting section:
---------------
{section_text}
"""


# ---------------------------------------------------------------------------
# Splitting — same `\n\n##? ` heading rule as upstream
# ---------------------------------------------------------------------------

_HEADING_RE = re.compile(r"\n\n##?\s+")


def split_meetings(text: str) -> List[str]:
    # Normalise line endings first.  The heading rule is ``\n\n##``, so a CRLF
    # file — the Windows default, and what most editors save — matches nothing
    # and the entire file comes back as ONE section.  That fails silently: you
    # get a single meeting with the last date it can find and no error, and
    # per-section memoisation collapses to per-file.  Text-mode ``open()`` hides
    # this via universal newlines; reading bytes from a source and decoding them
    # (what every non-filesystem source does) does not.
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    parts = _HEADING_RE.split("\n\n" + text)
    return [p.strip() for p in parts if p.strip()]


# ---------------------------------------------------------------------------
# LLM extraction — uses the project's configured provider, not a hard-wired one
# ---------------------------------------------------------------------------


def extract_meeting(llm: Any, section_text: str) -> Optional[ExtractedMeeting]:
    """Extract one meeting from a Markdown section, or None if the LLM refuses.

    The caller supplies the LLM — ``extractor.py`` passes
    ``KGExtractionContext.llamaindex_llm()``, so extraction follows whatever
    ``LLM_PROVIDER`` the run is configured with (upstream hard-wires LiteLLM +
    instructor instead).  ``structured_predict`` is a LlamaIndex API, so the LLM
    must be a LlamaIndex one even in an otherwise LangChain run.
    """
    from llama_index.core.prompts import PromptTemplate  # type: ignore[import-untyped]

    try:
        return llm.structured_predict(
            ExtractedMeeting,
            PromptTemplate(EXTRACT_PROMPT),
            section_text=section_text,
        )
    except Exception as exc:  # noqa: BLE001 - one bad section must not kill the run
        logger.warning("Extraction failed for a section: %s: %s", type(exc).__name__, exc)
        return None


