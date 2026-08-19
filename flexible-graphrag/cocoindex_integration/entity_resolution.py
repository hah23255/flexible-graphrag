"""Entity resolution for extracted KG entities, backed by CocoIndex.

Why this lives in ``cocoindex_integration`` and not in the base package
----------------------------------------------------------------------
KG extraction names entities however the source text happened to spell them, so
the same real-world person or company arrives as several distinct nodes:
"Bob Smith", "bob smith", "Robert Smith", "Acme Corp", "Acme Corporation".
Flexible GraphRAG de-duplicates by exact match on entity id, so every variant
becomes its own node and the graph fragments.

The good semantic solution is CocoIndex's ``cocoindex.ops.entity_resolution``:
it embeds every name, uses FAISS to partition them into connected components of
a similarity graph, then asks a *pair resolver* to make the final call on each
candidate pair.  Both plug-in points are plain ``Protocol`` classes —

    Embedder      async embed(text) -> float32 vector
    PairResolver  async __call__(entity, candidates) -> PairDecision

— so the machinery is reusable with **our** LLM and **our** embedding model
instead of the LiteLLM ones upstream uses.

    https://cocoindex.io/docs/ops/entity_resolution/#custom-resolvers

Because that is a CocoIndex dependency, the whole feature is scoped to this
package: the base flexible-graphrag install never imports it, and nothing here
is required for a default-pipeline ingest.

Frameworks
----------
Resolution is a property of the *names*, not of the graph library, so the core
(:func:`resolve_entity_names`) takes and returns plain strings.  Two thin
appliers write the result back into whichever representation is in play:

* :func:`resolve_entity_nodes`     — LlamaIndex ``EntityNode`` / ``Relation``
                                     (rewrites ``name`` and ``source_id`` /
                                     ``target_id``)
* :func:`resolve_graph_documents`  — LangChain ``GraphDocument``
                                     (rewrites ``Node.id`` and both endpoints)

Neither framework is imported at module level; both appliers duck-type.

Strategies
----------
``none`` (default)
    Nothing is merged.

``normalize``
    Typographic variance only — accent folding, case, punctuation, whitespace.
    No dependencies beyond the standard library.  Merges "Zoë Café" with
    "Zoe Cafe", but never "Bob" with "Robert Smith".

``llm``
    Full CocoIndex resolution driven by the supplied LLM and embedding model.
    Merges "Acme Corp" with "Acme Corporation".  Falls back to ``normalize``
    when the dependency is missing::

        uv pip install "cocoindex[entity_resolution]"   # pulls faiss-cpu

Safety: this changes entity identity
------------------------------------
Merging "Acme Corp" into "Acme Corporation" **rewrites entity ids**.  Running it
over a corpus that was already ingested without it produces a graph where old
and new nodes disagree — the same fragmentation hazard called out in
``langchain/graph/id_sanitizer.py``.  So the default strategy is ``none``:
nothing happens unless a caller opts in.  Enable it for a corpus from the
start, or re-ingest clean.
"""

from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

#: Strategy names accepted by :func:`resolve_entity_names`.
STRATEGY_NONE = "none"
STRATEGY_NORMALIZE = "normalize"
STRATEGY_LLM = "llm"

#: Entity labels worth resolving by default.  Resolving *every* label is
#: usually wrong: two Tasks with similar wording are generally different tasks,
#: whereas two similar Person or Organization names usually are one entity.
DEFAULT_RESOLVE_TYPES: Tuple[str, ...] = ("Person", "Organization", "Company")

_NOISE = re.compile(r"[^a-z0-9 ]+")
_WS = re.compile(r"\s+")


# ---------------------------------------------------------------------------
# Strategy 1: normalisation (standard library only)
# ---------------------------------------------------------------------------


def normalize_key(name: str) -> str:
    """Normalised comparison key: accent-folded, lowercased, punctuation to space.

    Accents are **folded** (``é`` -> ``e``), not deleted: deleting them turns
    "Zoë Café" into "zo caf", which then fails to match the very "Zoe Cafe" it
    was supposed to match.  Punctuation becomes a space rather than vanishing so
    that "Jean-Luc" and "Jean Luc" agree.
    """
    text = unicodedata.normalize("NFKD", (name or "").strip().lower())
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = _NOISE.sub(" ", text)
    return _WS.sub(" ", text).strip()


def _noise_score(value: str) -> int:
    """How 'untidy' a surface form is — punctuation plus redundant whitespace."""
    punct = sum(1 for c in value if not (c.isalnum() or c.isspace()))
    extra_ws = len(value) - len(" ".join(value.split()))
    return punct + extra_ws


class NormalizedResolver:
    """Groups names whose normalised keys are identical.

    Catches ``bob smith`` / ``Bob Smith`` / ``Bob Smith.`` / ``Zoe Cafe`` vs
    ``Zoë Café``.  Does **not** catch ``Bob`` / ``Robert Smith`` or ``Acme
    Corp`` / ``Acme Corporation`` — those are semantic, so they need ``llm``.
    """

    def resolve(self, names: Iterable[str]) -> Dict[str, str]:
        name_list = [(n or "").strip() for n in names if (n or "").strip()]
        counts: Dict[str, int] = defaultdict(int)
        by_key: Dict[str, List[str]] = defaultdict(list)
        for name in name_list:
            counts[name] += 1
            key = normalize_key(name)
            if key:
                by_key[key].append(name)

        canonical: Dict[str, str] = {}
        for variants in by_key.values():
            # Pick the spelling the corpus actually favours, not merely the
            # longest: "Bob  Smith." is longer than "Bob Smith" but worse.
            # Order: most frequent, then tidiest, then longest, then alphabetical
            # (the last purely so the result is deterministic).
            best = min(
                set(variants),
                key=lambda v: (-counts[v], _noise_score(v), -len(v), v),
            )
            for v in variants:
                canonical[v] = best
        return canonical


# ---------------------------------------------------------------------------
# Strategy 2: CocoIndex resolution driven by our own LLM + embedder
# ---------------------------------------------------------------------------


class FlexibleEmbedder:
    """Adapts our embedding model to CocoIndex's ``Embedder`` protocol.

    Works with LlamaIndex embedding models (``aget_text_embedding``) and with
    LangChain ones (``aembed_query`` / ``embed_query``), since both appear in
    this project depending on ``GRAPH_BACKEND``.
    """

    def __init__(self, embed_model: Any) -> None:
        self._embed_model = embed_model

    async def embed(self, text: str) -> Any:
        import numpy as np

        for attr in ("aget_text_embedding", "aembed_query"):
            fn = getattr(self._embed_model, attr, None)
            if callable(fn):
                return np.asarray(await fn(text), dtype=np.float32)

        for attr in ("get_text_embedding", "embed_query"):
            fn = getattr(self._embed_model, attr, None)
            if callable(fn):
                # Sync-only model: keep it off the event loop.
                return np.asarray(await asyncio.to_thread(fn, text), dtype=np.float32)

        raise TypeError(
            f"{type(self._embed_model).__name__} exposes no known embedding method"
        )


_PAIR_PROMPT = """\
You are deciding whether two names refer to the same real-world entity.

Candidate name: {entity}

Existing entities:
{candidate_list}

If the candidate name refers to the SAME real-world entity as one of the
existing entities, reply with that existing entity's name EXACTLY as written
above, on a single line with no other text.

If it refers to a different entity, reply with exactly: NONE

Treat spelling variants, capitalisation, abbreviations, nicknames and
legal-suffix differences ("Acme Corp" vs "Acme Corporation", "Bob" vs
"Robert Smith") as the same entity. A lone first name refers to the same person
as a fuller name beginning with it ("Priya" and "Priya Raman").

Treat genuinely different entities that merely share a common word as different.
"""

# The bare-first-name rule above is only safe because of the code-level guard in
# LLMPairResolver._bare_name_is_ambiguous.  Asking the model to judge ambiguity
# itself was tried and failed: it merged "Priya" into "Priya Patel" with
# "Priya Raman" also in the corpus, because the resolver shows it an
# embedding-filtered candidate list that often omits the competing name.  The
# guard counts over the whole corpus and refuses before the model is consulted.


def _as_text(response: Any) -> str:
    """Best-effort text out of an LLM response from either framework."""
    for attr in ("text", "content"):
        value = getattr(response, attr, None)
        if isinstance(value, str):
            return value
    return str(response)


class LLMPairResolver:
    """CocoIndex ``PairResolver`` backed by the project's configured LLM.

    Stateless, so it is safe under the concurrent ``__call__`` invocations
    ``resolve_entities`` makes across components.  Accepts LlamaIndex LLMs
    (``acomplete`` / ``complete``) and LangChain ones (``ainvoke`` / ``invoke``).
    """

    def __init__(self, llm: Any, all_names: Optional[Sequence[str]] = None) -> None:
        self._llm = llm
        # first name -> how many DISTINCT multi-word names in the corpus start
        # with it.  See _bare_name_is_ambiguous for why this is built from the
        # whole corpus rather than from each call's candidate list.
        # Distinct full names per first name.  A bare name is skipped: it is not
        # itself evidence that a *second* person shares the first name.
        _buckets: Dict[str, set] = {}
        for raw in (all_names or ()):
            parts = (raw or "").strip().split()
            if len(parts) < 2:
                continue
            _buckets.setdefault(normalize_key(parts[0]), set()).add(normalize_key(raw))
        self._first_name_counts: Dict[str, int] = {k: len(v) for k, v in _buckets.items()}

    def _bare_name_is_ambiguous(self, entity: str) -> bool:
        """True when *entity* is a lone first name shared by several full names.

        ``Priya`` with both ``Priya Raman`` and ``Priya Patel`` in the corpus
        must not merge into either — picking one silently fuses two real people.

        This is decided in code, not in the prompt, because the resolver hands
        the LLM only an *embedding-filtered* candidate list: the second
        ``Priya …`` is frequently absent from it, so the model cannot see that
        the case is ambiguous and confidently merges. Counting over the full
        corpus is the only view that can tell.
        """
        parts = (entity or "").strip().split()
        if len(parts) != 1:
            return False  # already a full name; the LLM can judge it
        return self._first_name_counts.get(normalize_key(parts[0]), 0) > 1

    async def _complete(self, prompt: str) -> str:
        for attr in ("acomplete", "ainvoke"):
            fn = getattr(self._llm, attr, None)
            if callable(fn):
                return _as_text(await fn(prompt))
        for attr in ("complete", "invoke"):
            fn = getattr(self._llm, attr, None)
            if callable(fn):
                return _as_text(await asyncio.to_thread(fn, prompt))
        raise TypeError(f"{type(self._llm).__name__} exposes no known completion method")

    async def __call__(self, entity: str, candidates: List[str]) -> Any:
        from cocoindex.ops.entity_resolution import CanonicalSide, PairDecision

        if not candidates:
            return PairDecision(matched=None)

        # The guard has to apply to BOTH sides.  resolve_entities drives the scan
        # either way round — it calls (entity="Priya Patel", candidates=["Priya"])
        # just as readily as (entity="Priya", candidates=["Priya Patel"]) — and in
        # the first form the ambiguous name is a *candidate*.  Checking only
        # `entity` let "Priya" merge into "Priya Patel" with "Priya Raman" also
        # present, which is the exact failure this guard exists to prevent.
        if self._bare_name_is_ambiguous(entity):
            logger.debug(
                "Entity resolution: %r is a first name shared by %d full names — "
                "ambiguous, not merging", entity,
                self._first_name_counts.get(normalize_key(entity), 0),
            )
            return PairDecision(matched=None)

        candidates = [c for c in candidates if not self._bare_name_is_ambiguous(c)]
        if not candidates:
            return PairDecision(matched=None)

        prompt = _PAIR_PROMPT.format(
            entity=entity, candidate_list="\n".join(f"- {c}" for c in candidates)
        )

        try:
            raw = await self._complete(prompt)
        except Exception as exc:  # noqa: BLE001 - a failed compare must not abort
            logger.warning(
                "Entity-resolution LLM call failed for %r: %s: %s",
                entity, type(exc).__name__, exc,
            )
            return PairDecision(matched=None)

        answer = raw.strip().splitlines()[0].strip() if raw.strip() else ""
        if not answer or answer.upper() == "NONE":
            return PairDecision(matched=None)

        # The contract requires `matched` to be one of the supplied candidates,
        # or resolve_entities raises.  Models paraphrase, so map back defensively.
        matched = None
        if answer in candidates:
            matched = answer
        else:
            lowered = {c.lower(): c for c in candidates}
            if answer.lower() in lowered:
                matched = lowered[answer.lower()]

        if matched is not None:
            # Which name survives?  CanonicalSide.MATCHED (the default) keeps the
            # candidate, which is whichever the scan happened to reach first — so
            # "Bob Smith" merging into an existing "Bob" would leave the graph
            # labelled "Bob".  Prefer the longer surface form: it is the more
            # complete name ("Bob Smith" over "Bob", "Acme Corporation" over
            # "Acme Corp") and matches what the normalize strategy picks.
            side = CanonicalSide.NEW if len(entity) > len(matched) else CanonicalSide.MATCHED
            return PairDecision(matched=matched, canonical=side)

        logger.debug(
            "Entity-resolution LLM returned %r, not among candidates %r — treating as NONE",
            answer, candidates,
        )
        return PairDecision(matched=None)


async def _resolve_with_cocoindex(
    names: Sequence[str],
    llm: Any,
    embed_model: Any,
    *,
    max_distance: float,
    top_n: int,
) -> Dict[str, str]:
    from cocoindex.ops.entity_resolution import resolve_entities

    resolved = await resolve_entities(
        names,
        embedder=FlexibleEmbedder(embed_model),
        # The full name set, not just each call's candidates: the ambiguity
        # guard needs the whole-corpus view that the per-call list lacks.
        resolve_pair=LLMPairResolver(llm, all_names=names),
        max_distance=max_distance,
        top_n=top_n,
    )

    # ResolvedEntities exposes canonical_of() / canonicals() / groups() /
    # to_dict() — NOT .get().  canonical_of() is the one that chain-walks, so a
    # name merged into a name that was itself merged still resolves to the final
    # canonical.  (An earlier version called .get() inside a bare except, which
    # turned the resulting AttributeError into "nothing merged" on every call —
    # the resolution ran, the LLM answered correctly, and the answer was thrown
    # away.  Hence the narrow except here.)
    canonical: Dict[str, str] = {}
    for name in names:
        try:
            target = resolved.canonical_of(name)
        except (AttributeError, KeyError) as exc:
            logger.warning(
                "Entity resolution: could not read canonical for %r (%s: %s)",
                name, type(exc).__name__, exc,
            )
            target = None
        canonical[name] = target or name
    return canonical


# ---------------------------------------------------------------------------
# Core: names in, canonical names out (framework-neutral)
# ---------------------------------------------------------------------------


def resolve_entity_names(
    names: Iterable[str],
    *,
    strategy: str = STRATEGY_NONE,
    llm: Any = None,
    embed_model: Any = None,
    max_distance: float = 0.5,
    top_n: int = 5,
) -> Dict[str, str]:
    """Return a ``raw name -> canonical name`` map.

    ``strategy`` is ``none`` (default), ``normalize`` or ``llm``.  The ``llm``
    strategy falls back to ``normalize`` when CocoIndex or faiss is missing, or
    when either model is not supplied.

    Never raises: resolution is an enhancement, and failing it must not fail an
    ingest.
    """
    # Materialise once (``names`` may be a generator) and keep the duplicates:
    # NormalizedResolver uses the frequency of each spelling to choose which one
    # becomes canonical, so de-duplicating here would throw that signal away.
    all_names = [(n or "").strip() for n in names if (n or "").strip()]
    unique = sorted(set(all_names))
    if not unique or strategy == STRATEGY_NONE:
        return {n: n for n in unique}

    if strategy == STRATEGY_LLM:
        if llm is None or embed_model is None:
            logger.warning(
                "Entity resolution: strategy 'llm' needs both an LLM and an "
                "embedding model — falling back to 'normalize'"
            )
        else:
            try:
                return asyncio.run(
                    _resolve_with_cocoindex(
                        unique, llm, embed_model,
                        max_distance=max_distance, top_n=top_n,
                    )
                )
            except ImportError as exc:
                logger.warning(
                    "Entity resolution: CocoIndex resolution unavailable (%s) — "
                    'falling back to "normalize". Install with: '
                    'uv pip install "cocoindex[entity_resolution]"', exc,
                )
            except RuntimeError as exc:
                # asyncio.run() called from inside a running loop.
                logger.warning(
                    "Entity resolution: cannot start an event loop (%s) — "
                    "falling back to 'normalize'", exc,
                )
            except Exception as exc:  # noqa: BLE001 - never fail an ingest
                logger.warning(
                    "Entity resolution failed (%s: %s) — falling back to 'normalize'",
                    type(exc).__name__, exc,
                )

    return NormalizedResolver().resolve(all_names)


async def aresolve_entity_names(
    names: Iterable[str],
    *,
    strategy: str = STRATEGY_NONE,
    llm: Any = None,
    embed_model: Any = None,
    max_distance: float = 0.5,
    top_n: int = 5,
) -> Dict[str, str]:
    """Async form of :func:`resolve_entity_names`.

    Use this from inside a running event loop — the sync version cannot call
    ``asyncio.run`` there and would silently degrade to ``normalize``.
    """
    all_names = [(n or "").strip() for n in names if (n or "").strip()]
    unique = sorted(set(all_names))
    if not unique or strategy == STRATEGY_NONE:
        return {n: n for n in unique}

    if strategy == STRATEGY_LLM and llm is not None and embed_model is not None:
        try:
            return await _resolve_with_cocoindex(
                unique, llm, embed_model, max_distance=max_distance, top_n=top_n
            )
        except Exception as exc:  # noqa: BLE001 - never fail an ingest
            logger.warning(
                "Entity resolution failed (%s: %s) — falling back to 'normalize'",
                type(exc).__name__, exc,
            )

    return NormalizedResolver().resolve(all_names)


# ---------------------------------------------------------------------------
# Applier: LlamaIndex EntityNode / Relation
# ---------------------------------------------------------------------------


def _label_of(obj: Any) -> str:
    return str(getattr(obj, "label", "") or getattr(obj, "type", "") or "").lower()


def resolve_entity_nodes(
    nodes: Iterable[Any],
    relations: Optional[Iterable[Any]] = None,
    *,
    strategy: str = STRATEGY_NONE,
    entity_types: Optional[Sequence[str]] = None,
    llm: Any = None,
    embed_model: Any = None,
    canonical_map: Optional[Dict[str, str]] = None,
    max_distance: float = 0.5,
    top_n: int = 5,
) -> Tuple[int, int]:
    """Merge duplicate LlamaIndex ``EntityNode`` entities, in place.

    LlamaIndex identifies an entity by ``EntityNode.name`` (``.id`` is derived
    from it) and ``Relation`` refers to endpoints by ``source_id`` /
    ``target_id``, i.e. by those same names — so both must be rewritten together
    or the relations dangle.

    Pass *canonical_map* to apply a map computed elsewhere, in which case
    *strategy* is ignored.

    Returns ``(names_merged, references_rewritten)``.
    """
    node_list = list(nodes or [])
    rel_list = list(relations or [])
    if strategy == STRATEGY_NONE and canonical_map is None:
        return (0, 0)

    types = tuple(entity_types) if entity_types else DEFAULT_RESOLVE_TYPES
    type_set = {t.lower() for t in types}

    def _is_target(node: Any) -> bool:
        return _label_of(node) in type_set

    targets = [n for n in node_list if _is_target(n)]
    if not targets and canonical_map is None:
        return (0, 0)

    if canonical_map is None:
        names = [str(getattr(n, "name", "") or "") for n in targets]
        canonical_map = resolve_entity_names(
            names, strategy=strategy, llm=llm, embed_model=embed_model,
            max_distance=max_distance, top_n=top_n,
        )

    merged = sum(1 for raw, canon in canonical_map.items() if raw != canon)
    if not merged:
        return (0, 0)

    # Only rewrite relation endpoints that named a resolved entity, so a Task
    # and a Person that happen to share a string are not conflated.
    resolved_names = {str(getattr(n, "name", "") or "").strip() for n in targets}

    rewritten = 0
    for node in targets:
        try:
            old = str(getattr(node, "name", "") or "").strip()
            new = canonical_map.get(old)
            if new and new != old:
                node.name = new
                rewritten += 1
        except Exception as exc:  # noqa: BLE001 - never break ingest over this
            logger.debug("resolve_entity_nodes: skipped a node: %s", exc)

    for rel in rel_list:
        for attr in ("source_id", "target_id"):
            try:
                old = str(getattr(rel, attr, "") or "").strip()
                if old not in resolved_names:
                    continue
                new = canonical_map.get(old)
                if new and new != old:
                    setattr(rel, attr, new)
                    rewritten += 1
            except Exception as exc:  # noqa: BLE001
                logger.debug("resolve_entity_nodes: skipped a relation: %s", exc)

    logger.info(
        "Entity resolution (%s, llamaindex): merged %d name variant(s), "
        "rewrote %d reference(s)", strategy, merged, rewritten,
    )
    return (merged, rewritten)


# ---------------------------------------------------------------------------
# Applier: LangChain GraphDocument
# ---------------------------------------------------------------------------


def resolve_graph_documents(
    graph_docs: Iterable[Any],
    *,
    strategy: str = STRATEGY_NONE,
    entity_types: Optional[Sequence[str]] = None,
    llm: Any = None,
    embed_model: Any = None,
    canonical_map: Optional[Dict[str, str]] = None,
    max_distance: float = 0.5,
    top_n: int = 5,
) -> Tuple[int, int]:
    """Merge duplicate entities across LangChain ``GraphDocument`` objects, in place.

    Relationship endpoints are rewritten with the same map so edges still
    resolve after their endpoints move.

    Pass *canonical_map* to apply a map computed elsewhere, in which case
    *strategy* is ignored.

    Returns ``(names_merged, references_rewritten)``.
    """
    docs = list(graph_docs or [])
    if not docs or (strategy == STRATEGY_NONE and canonical_map is None):
        return (0, 0)

    types = tuple(entity_types) if entity_types else DEFAULT_RESOLVE_TYPES
    type_set = {t.lower() for t in types}

    def _is_target(node: Any) -> bool:
        return _label_of(node) in type_set

    if canonical_map is None:
        # Count each entity *mention* once.  A Relationship's endpoints are
        # normally the very same Node objects listed in doc.nodes, so collecting
        # from both without de-duplicating by object identity would count one
        # mention twice — and the frequency tie-break then picks the wrong
        # canonical spelling.
        names: List[str] = []
        seen_objs: set = set()
        for doc in docs:
            candidates = list(getattr(doc, "nodes", None) or [])
            for rel in getattr(doc, "relationships", None) or []:
                for endpoint in ("source", "target"):
                    ep = getattr(rel, endpoint, None)
                    if ep is not None:
                        candidates.append(ep)
            for node in candidates:
                if not _is_target(node) or id(node) in seen_objs:
                    continue
                seen_objs.add(id(node))
                names.append(str(getattr(node, "id", "") or ""))
        if not names:
            return (0, 0)
        canonical_map = resolve_entity_names(
            names, strategy=strategy, llm=llm, embed_model=embed_model,
            max_distance=max_distance, top_n=top_n,
        )

    merged = sum(1 for raw, canon in canonical_map.items() if raw != canon)
    if not merged:
        return (0, 0)

    rewritten = 0
    for doc in docs:
        try:
            for node in getattr(doc, "nodes", None) or []:
                if not _is_target(node):
                    continue
                old = str(getattr(node, "id", "") or "").strip()
                new = canonical_map.get(old)
                if new and new != old:
                    node.id = new
                    rewritten += 1
            for rel in getattr(doc, "relationships", None) or []:
                for endpoint in ("source", "target"):
                    ep = getattr(rel, endpoint, None)
                    if ep is None or not _is_target(ep):
                        continue
                    old = str(getattr(ep, "id", "") or "").strip()
                    new = canonical_map.get(old)
                    if new and new != old:
                        ep.id = new
                        rewritten += 1
        except Exception as exc:  # noqa: BLE001 - never break ingest over this
            logger.debug("resolve_graph_documents: skipped a document: %s", exc)

    logger.info(
        "Entity resolution (%s, langchain): merged %d name variant(s), "
        "rewrote %d reference(s)", strategy, merged, rewritten,
    )
    return (merged, rewritten)


__all__ = [
    "STRATEGY_NONE",
    "STRATEGY_NORMALIZE",
    "STRATEGY_LLM",
    "DEFAULT_RESOLVE_TYPES",
    "normalize_key",
    "NormalizedResolver",
    "FlexibleEmbedder",
    "LLMPairResolver",
    "resolve_entity_names",
    "aresolve_entity_names",
    "resolve_entity_nodes",
    "resolve_graph_documents",
]
