"""
Custom KG extractor registration — bring your own extraction logic.

The built-in extractors (``extract_kg_llamaindex`` / ``extract_kg_langchain``)
drive an LLM with an ontology-derived schema.  When you want something else —
a fixed pydantic schema, a regex/deterministic pass, a domain-specific prompt,
a non-LLM model — subclass :class:`KGExtractor` and point
``KG_EXTRACTOR_BACKEND`` at it.

Why this seam and not the app-level one
----------------------------------------
``adapters/process/kg_extractor_adapter.py`` is the other extractor seam, but it
speaks in framework objects (LlamaIndex ``TextNode`` / LangChain
``GraphDocument``) over whole documents.  This one is chunk-in / ``KGResult``-out
— a pure function over plain dataclasses.  ``KGResult`` is already what *both*
built-in backends normalise into, so implementing against it is not a third
contract, and the pipeline's writers apply entity + relation properties from it
across every property-graph and RDF target.

Writing one
-----------
::

    from cocoindex_integration.functions.kg_extraction import (
        KGResult, KGTriple, KGEntity,
    )
    from cocoindex_integration.functions.kg_extractors import (
        KGExtractor, KGExtractionContext, register_kg_extractor,
    )

    @register_kg_extractor("meeting_notes")
    class MeetingNotesExtractor(KGExtractor):
        version = "1"          # bump when behaviour changes — see caching below

        async def extract(self, chunk_text: str, ctx: KGExtractionContext) -> KGResult:
            llm = ctx.llamaindex_llm()          # or ctx.langchain_llm()
            ...
            return KGResult(triples=[...], entities=[...])

Selecting one
-------------
``KG_EXTRACTOR_BACKEND`` accepts a registered name or a direct class reference:

===================================================  ===============================
``KG_EXTRACTOR_BACKEND=llamaindex`` / ``langchain``   built-in (unchanged default)
``KG_EXTRACTOR_BACKEND=meeting_notes``                registered name
``KG_EXTRACTOR_BACKEND=my.pkg.mod:MyExtractor``       importable module + class
``KG_EXTRACTOR_BACKEND=/path/to/mod.py:MyExtractor``  file path + class
===================================================  ===============================

A registered *name* only resolves once the module defining it has been imported.
For a class that is not on ``sys.path`` (an example directory, a scratch script)
use the file-path form, which needs no install and no ``sys.path`` surgery.
``KG_EXTRACTOR_MODULES`` (comma-separated) is imported before resolution, which
is how a registered name can be made reachable without naming the class.

Caching — the part that bites
-----------------------------
Extraction is memoised by CocoIndex.  The two built-ins are separate
``@coco.fn`` objects, so they memoise into separate keyspaces; custom extractors
all share **one** dispatching function, so the spec *and* the ``version`` are
passed as real arguments and become part of the memo key.  That means:

* switching extractors re-extracts (the spec changed), and
* **editing an extractor's logic does not re-extract unless you bump
  ``version``** — you would keep reading the previous implementation's triples
  out of the cache.

Bump ``version`` whenever output could change.  It is a plain string, so
``"2"``, ``"1.1"``, or a date all work.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, Optional, Type

from cocoindex_integration.functions.kg_extraction import (  # noqa: F401 - re-exported
    KGEntity,
    KGResult,
    KGTriple,
)

logger = logging.getLogger(__name__)

# The two built-in backends.  Anything else is treated as a custom extractor
# spec.  Kept here rather than in pipeline/flexible_app.py so the registry and
# the config resolver cannot disagree about what "built-in" means.
BUILTIN_KG_EXTRACTOR_BACKENDS = frozenset({"llamaindex", "langchain"})


@dataclass
class KGExtractionContext:
    """Everything an extractor needs, already parsed.

    Deliberately plain data plus two LLM accessors — an extractor never has to
    know how the pipeline resolves providers or loads ontologies.
    """

    #: Ontology-derived schema, or ``{}`` when no ontology is configured.
    #: Same structure the built-in extractors receive: ``entities``,
    #: ``relations``, ``entity_props``, ``relation_props``, ``validation_schema``.
    schema: Dict[str, Any] = field(default_factory=dict)

    #: Parsed ``load_extractor_config_json()`` — ``KG_EXTRACTOR_TYPE``,
    #: ``DISABLE_PROPERTIES``, ``MAX_TRIPLETS_PER_CHUNK``, …
    extractor_config: Dict[str, Any] = field(default_factory=dict)

    #: Provider name (openai, ollama, gemini, bedrock, …).
    llm_provider: str = ""

    #: Extra LLM kwargs (model, temperature, …).
    llm_config: Dict[str, Any] = field(default_factory=dict)

    # ── Deliberately absent: document provenance ──────────────────────────────
    # No doc_id / file_name / file_path / source_type here.  An extractor is
    # text-in, triples-out, independent of where the text came from.
    #
    # Nodes DO need provenance — it is what makes a document's triples
    # deletable/reconcilable and what the stores expect — but that is already
    # carried separately: KGTripleRow has doc_id, file_name, file_path,
    # source_type and ref_doc_id, the pipeline fills them in per document, and
    # the writers tag the nodes.  An extractor adding them would duplicate a
    # solved concern.
    #
    # It would also be actively harmful for *ids*.  A meeting keyed
    # f"{file_name}#{date}" mints a different id when the same note arrives from
    # SharePoint instead of the filesystem, producing duplicate nodes; for cloud
    # sources file_name may be a bare name while file_path is s3://bucket/key, so
    # the id silently changes meaning per source.  Derive ids from the *content*
    # instead (a date plus the organiser, a hash of the section) — stable
    # regardless of which source delivered the bytes.

    @property
    def use_ontology(self) -> bool:
        """True when an ontology was loaded.  Custom extractors may ignore it."""
        return bool(self.schema)

    def llamaindex_llm(self) -> Any:
        """The configured LLM as a LlamaIndex ``LLM``.  May return ``None``."""
        from cocoindex_integration.functions.llm import get_llama_index_llm
        return get_llama_index_llm(
            self.llm_provider or os.getenv("LLM_PROVIDER", "openai"),
            self.llm_config,
        )

    def langchain_llm(self) -> Any:
        """The configured LLM as a LangChain chat model.  May return ``None``."""
        from cocoindex_integration.functions.llm import get_langchain_llm
        return get_langchain_llm(
            self.llm_provider or os.getenv("LLM_PROVIDER", "openai"),
            self.llm_config,
        )

    async def builtin(self, chunk_text: str, backend: str = ""):
        """Run the **built-in** extractor over *chunk_text* and return its ``KGResult``.

        ``KG_EXTRACTOR_BACKEND`` selects one extractor for the whole run, but a
        corpus is rarely uniform: an extractor written for meeting notes will
        happily invent a meeting when handed an invoice.  Rather than emit
        nothing for everything it does not recognise, a custom extractor can
        recognise its own documents and hand the rest back::

            async def extract(self, chunk_text, ctx):
                if not looks_like_mine(chunk_text):
                    return await ctx.builtin(chunk_text)
                ...

        The built-in runs with this context's ontology, provider and extractor
        config, so delegated chunks behave exactly as they would have with no
        custom extractor configured.  ``backend`` defaults to
        ``KG_EXTRACTOR_FALLBACK`` and then ``llamaindex``.

        Memoised independently of the custom extractor: the built-ins are their
        own ``@coco.fn`` objects, so delegating does not disturb their cache.
        """
        from cocoindex_integration.functions.kg_extraction import (  # noqa: PLC0415
            _kg_result_from_json,
            extract_kg_langchain,
            extract_kg_llamaindex,
        )

        which = (backend or os.getenv("KG_EXTRACTOR_FALLBACK", "llamaindex")).lower()
        fn = extract_kg_langchain if which == "langchain" else extract_kg_llamaindex
        # sort_keys so the memo key is stable across runs — dict ordering would
        # otherwise produce a different cache entry for identical config.
        raw = await fn(
            chunk_text,
            schema_json=json.dumps(self.schema, sort_keys=True, default=str),
            llm_provider=self.llm_provider,
            llm_config_json=json.dumps(self.llm_config, sort_keys=True, default=str),
            extractor_config_json=json.dumps(
                self.extractor_config, sort_keys=True, default=str
            ),
        )
        return _kg_result_from_json(raw)


class KGExtractor(ABC):
    """Base interface for a custom knowledge-graph extractor.

    One instance is built per resolution and reused across chunks, so treat
    ``self`` as read-only — chunks are extracted concurrently.  Put per-chunk
    state in locals.
    """

    #: Registry name.  Set by :func:`register_kg_extractor`, or declare it here.
    name: ClassVar[str] = ""

    #: Bump when behaviour changes, or memoised results from the previous
    #: implementation keep being served.  See the module docstring.
    version: ClassVar[str] = "1"

    @abstractmethod
    async def extract(self, chunk_text: str, ctx: KGExtractionContext) -> KGResult:
        """Extract a :class:`KGResult` from one chunk.

        Entity properties belong on ``KGResult.entities[*].properties`` and
        relation properties on ``KGResult.triples[*].relation_properties``; the
        pipeline writes both to every configured graph target.

        Raising is allowed — the pipeline logs the chunk and continues — but
        returning an empty ``KGResult()`` is usually better, since a raise is
        not memoised and will be retried on every run.
        """


# ─────────────────────────────────────────────────────────────────────────────
# Registry
# ─────────────────────────────────────────────────────────────────────────────

_REGISTRY: Dict[str, Type[KGExtractor]] = {}


def register_kg_extractor(name: Optional[str] = None):
    """Class decorator registering an extractor under *name*.

    ``name`` defaults to the class's own ``name`` attribute, else a snake-ish
    form of the class name.  Re-registering the same name is allowed (module
    reload) but logged when it changes the target class.
    """

    def _decorate(cls: Type[KGExtractor]) -> Type[KGExtractor]:
        if not (isinstance(cls, type) and issubclass(cls, KGExtractor)):
            raise TypeError(
                f"register_kg_extractor: {cls!r} is not a KGExtractor subclass"
            )
        key = (name or getattr(cls, "name", "") or cls.__name__).strip()
        if not key:
            raise ValueError("register_kg_extractor: name must not be empty")
        if key.lower() in BUILTIN_KG_EXTRACTOR_BACKENDS:
            raise ValueError(
                f"register_kg_extractor: {key!r} is a built-in backend name"
            )
        existing = _REGISTRY.get(key)
        if existing is not None and existing is not cls:
            logger.info(
                "register_kg_extractor: %r now resolves to %s (was %s)",
                key, cls.__name__, existing.__name__,
            )
        cls.name = key
        _REGISTRY[key] = cls
        return cls

    return _decorate


def registered_kg_extractors() -> List[str]:
    """Names registered so far.  Only reflects modules already imported."""
    return sorted(_REGISTRY)


def is_builtin_kg_extractor(spec: str) -> bool:
    """True for ``llamaindex`` / ``langchain`` (case-insensitive)."""
    return (spec or "").strip().lower() in BUILTIN_KG_EXTRACTOR_BACKENDS


def _import_registration_modules() -> None:
    """Import ``KG_EXTRACTOR_MODULES`` so decorators run.  Failures are logged."""
    raw = os.getenv("KG_EXTRACTOR_MODULES", "").strip()
    if not raw:
        return
    for mod in (m.strip() for m in raw.split(",")):
        if not mod:
            continue
        try:
            importlib.import_module(mod)
        except Exception as exc:  # noqa: BLE001 - one bad module must not kill the run
            logger.warning(
                "KG_EXTRACTOR_MODULES: importing %r failed (%s: %s)",
                mod, type(exc).__name__, exc,
            )


def _split_class_spec(spec: str) -> Optional[tuple[str, str]]:
    """Split ``"module:Class"`` / ``"/path/mod.py:Class"`` into its two halves.

    Splits on the *last* colon so Windows drive letters (``C:\\...``) survive.
    Returns ``None`` when *spec* is a bare name rather than a class reference.
    """
    if ":" not in spec:
        return None
    head, _, tail = spec.rpartition(":")
    if not head or not tail.isidentifier():
        return None
    return head, tail


def _load_class(head: str, attr: str) -> Type[KGExtractor]:
    """Import *head* (module path or ``.py`` file) and return its *attr*."""
    if head.endswith(".py") or os.sep in head or (os.altsep and os.altsep in head):
        path = os.path.abspath(head)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"KG extractor file not found: {path}")
        # A stable module name keeps repeated resolutions pointing at one
        # module object, so the class identity (and its memo key) is stable.
        mod_name = f"_fg_kg_extractor_{abs(hash(path)):x}"
        existing = sys.modules.get(mod_name)
        if existing is not None:
            module = existing
        else:
            spec_obj = importlib.util.spec_from_file_location(mod_name, path)
            if spec_obj is None or spec_obj.loader is None:
                raise ImportError(f"cannot load KG extractor from {path}")
            module = importlib.util.module_from_spec(spec_obj)
            # Register before exec so a decorator inside can import its own module.
            sys.modules[mod_name] = module
            try:
                spec_obj.loader.exec_module(module)
            except Exception:
                sys.modules.pop(mod_name, None)
                raise
    else:
        module = importlib.import_module(head)

    try:
        cls = getattr(module, attr)
    except AttributeError as exc:
        raise ImportError(f"{head!r} has no attribute {attr!r}") from exc
    if not (isinstance(cls, type) and issubclass(cls, KGExtractor)):
        raise TypeError(
            f"{head}:{attr} is not a KGExtractor subclass "
            f"(got {type(cls).__name__})"
        )
    return cls


def resolve_kg_extractor(spec: str) -> Type[KGExtractor]:
    """Resolve *spec* to a :class:`KGExtractor` subclass.

    Accepts a registered name, ``module:Class``, or ``/path/to/mod.py:Class``.
    Raises ``LookupError`` / ``ImportError`` / ``TypeError`` on failure — callers
    decide whether that is fatal or falls back to a built-in.
    """
    spec = (spec or "").strip()
    if not spec:
        raise LookupError("KG extractor spec is empty")
    if is_builtin_kg_extractor(spec):
        raise LookupError(f"{spec!r} is a built-in backend, not a custom extractor")

    if spec in _REGISTRY:
        return _REGISTRY[spec]

    parts = _split_class_spec(spec)
    if parts is not None:
        cls = _load_class(*parts)
        # Make it reachable by name too, for logging and later lookups.
        if not getattr(cls, "name", ""):
            cls.name = cls.__name__
        _REGISTRY.setdefault(cls.name, cls)
        return cls

    # Bare name that is not registered yet — a defining module may still be
    # pending import.
    _import_registration_modules()
    if spec in _REGISTRY:
        return _REGISTRY[spec]

    known = registered_kg_extractors()
    raise LookupError(
        f"unknown KG extractor {spec!r}; registered: {known or '(none)'}. "
        "Use module:Class or /path/to/mod.py:Class, or set KG_EXTRACTOR_MODULES "
        "to a module that registers it."
    )


def kg_extractor_version(spec: str) -> str:
    """``version`` of the extractor *spec* resolves to (``""`` if unresolvable)."""
    try:
        return str(getattr(resolve_kg_extractor(spec), "version", "") or "")
    except Exception:  # noqa: BLE001 - callers treat this as "unknown"
        return ""


__all__ = [
    "BUILTIN_KG_EXTRACTOR_BACKENDS",
    "KGEntity",
    "KGExtractionContext",
    "KGExtractor",
    "KGResult",
    "KGTriple",
    "is_builtin_kg_extractor",
    "kg_extractor_version",
    "register_kg_extractor",
    "registered_kg_extractors",
    "resolve_kg_extractor",
]
