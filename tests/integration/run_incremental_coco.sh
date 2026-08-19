#!/usr/bin/env bash
# ============================================================
#  run_incremental_coco.sh - incremental (add/modify/delete) testing
#  for the CocoIndex pipeline, tier by tier.
#
#  Answers one question per store: does it correctly handle a document being
#  added, modified and deleted while the pipeline watches a directory?
#  Every tier runs tests/integration/test_cocoindex_changes.py with
#  PIPELINE_BACKEND=cocoindex and a filesystem watch directory registered
#  through the REST API with enable_sync.
#
#  Usage (from repo root):
#      tests/integration/run_incremental_coco.sh            # all tiers
#      tests/integration/run_incremental_coco.sh 2          # one tier
#      tests/integration/run_incremental_coco.sh 3a         # RDF only
#      tests/integration/run_incremental_coco.sh quick      # tier 0 + seed test only
#
#  Tiers:
#    0   smoke, both source backends             2 jobs
#    1   10 vector stores        (src flexible) 10 jobs
#    2   12 property graph stores               12 jobs   <- slowest, KG extraction
#    3a  4 RDF stores            (+ qdrant)      4 jobs
#    3b  3 search stores         (+ qdrant)      3 jobs
#    4   10 vector stores        (src cocoindex) 10 jobs  <- native delete path
#
#  Tier 2 is the long one: every add and modify runs LLM KG extraction.
#  Tiers 1/3a/3b/4 pass --pg none to keep the LLM out of the loop.
#
#  Prerequisites:
#    - INTEGRATION_WATCH_DIR set to a dedicated folder
#    - ENABLE_INCREMENTAL_UPDATES unset/false (mutually exclusive with cocoindex)
#    - docker stack up for whichever stores the tier touches
#
#  Tuning:
#    export COCOINDEX_SYNC_WAIT=30   # shorten failure waits while debugging
#
#  Cloud PG stores (spanner, neptune, neptune_analytics) are omitted from
#  tier 2 - add them to PG_ALL below when those instances are available.
# ============================================================
set -uo pipefail

TIER="${1:-all}"
cd "$(dirname "$0")/../.."

MATRIX=(uv run tests/integration/run_matrix.py)
TESTPATH="tests/integration/test_cocoindex_changes.py"
COMMON=(--clean --pipeline cocoindex --test-path "$TESTPATH")

VEC_ALL="qdrant,lancedb,postgres,chroma,milvus,weaviate,elasticsearch,opensearch,pinecone,neo4j"
PG_ALL="neo4j,falkordb,memgraph,arcadedb,nebula,ladybug,arangodb,apache_age,hugegraph,tigergraph,surrealdb,cosmos_gremlin"
RDF_ALL="fuseki,graphdb,oxigraph"
SEARCH_ALL="elasticsearch,opensearch,bm25"

LOGDIR="$(dirname "$0")/logs"
mkdir -p "$LOGDIR"
STAMP="$(date +%Y%m%d-%H%M)"
LOG="$LOGDIR/incremental-coco-$TIER-$STAMP.log"

echo "Incremental CocoIndex run (tier $TIER) started $(date)" > "$LOG"
echo "Log: $LOG"
echo

PASS=0
FAIL=0

run_group() {   # run_group "<label>" <matrix args...>
    local label="$1"; shift
    echo "[$label]"
    if "${MATRIX[@]}" "$@" >> "$LOG" 2>&1; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  -> FAILED (see $LOG)"
    fi
}

want() {  # want <tier-name>...  -> true when $TIER selects this group
    local t
    for t in "$@"; do
        [[ "$TIER" == "$t" ]] && return 0
    done
    [[ "$TIER" == "all" ]] && return 0
    return 1
}

# ---- Tier 0: smoke, both source backends -------------------------------
if want 0 quick; then
    if [[ "$TIER" == "quick" ]]; then
        run_group "tier 0 smoke - src flexible"  "${COMMON[@]}" --source-backend flexible  --pg none --vector qdrant -k test_seed_is_indexed
        run_group "tier 0 smoke - src cocoindex" "${COMMON[@]}" --source-backend cocoindex --pg none --vector qdrant -k test_seed_is_indexed
    else
        run_group "tier 0 smoke - src flexible"  "${COMMON[@]}" --source-backend flexible  --pg none --vector qdrant
        run_group "tier 0 smoke - src cocoindex" "${COMMON[@]}" --source-backend cocoindex --pg none --vector qdrant
    fi
fi

# ---- Tier 1: vector stores, flexible source ----------------------------
if want 1; then
    run_group "tier 1: 10 vector stores - src flexible" \
        "${COMMON[@]}" --source-backend flexible --pg none --vector "$VEC_ALL"
fi

# ---- Tier 2: property graph stores -------------------------------------
# Only tier that keeps KG extraction on, so it is by far the slowest.
if want 2; then
    run_group "tier 2: 12 property graph stores - src flexible [SLOW: KG extraction]" \
        "${COMMON[@]}" --source-backend flexible --vector qdrant --pg "$PG_ALL"
fi

# ---- Tier 3a: RDF stores (each also gets qdrant) -----------------------
if want 3 3a; then
    run_group "tier 3a: 4 RDF stores + qdrant" \
        "${COMMON[@]}" --source-backend flexible --pg none --vector qdrant --rdf "$RDF_ALL"
fi

# ---- Tier 3b: search stores (each also gets qdrant) --------------------
if want 3 3b; then
    run_group "tier 3b: 3 search stores + qdrant" \
        "${COMMON[@]}" --source-backend flexible --pg none --vector qdrant --search "$SEARCH_ALL"
fi

# ---- Tier 4: vector stores via the NATIVE cocoindex source -------------
# The only tier that exercises native_apps._DeleteObservingLiveMapView -
# CocoIndex's own localfs watcher forwarding deletes to flexible targets.
if want 4; then
    run_group "tier 4: 10 vector stores - src cocoindex [native delete path]" \
        "${COMMON[@]}" --source-backend cocoindex --pg none --vector "$VEC_ALL"
fi

# ---- Summary -----------------------------------------------------------
# Per-job detail comes from run_matrix's own "[matrix] PASS/FAIL" lines, so the
# tier counters above and this list cannot disagree.
JOBFAIL=$(grep -c '\[matrix\] FAIL' "$LOG" 2>/dev/null || echo 0)
JOBPASS=$(grep -c '\[matrix\] PASS' "$LOG" 2>/dev/null || echo 0)

{
    echo
    echo "============================================================"
    echo " INCREMENTAL COCOINDEX SUMMARY (tier $TIER)"
    echo "   Tier groups passed: $PASS   failed: $FAIL"
    echo "   Individual jobs   : $JOBPASS passed, $JOBFAIL failed"
    if [[ "$JOBFAIL" -gt 0 ]]; then
        echo
        echo " Failed jobs:"
        grep '\[matrix\] FAIL' "$LOG" || true
    fi
    echo "============================================================"
} >> "$LOG"

echo
echo "============================================================"
echo " Tier groups: $PASS passed, $FAIL failed"
echo " Jobs:        $JOBPASS passed, $JOBFAIL failed"
echo " Log:         $LOG"
echo "============================================================"

if [[ "$FAIL" -gt 0 || "$JOBFAIL" -gt 0 ]]; then
    exit 1
fi
exit 0
