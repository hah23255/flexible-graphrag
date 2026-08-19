@echo off
REM ============================================================
REM  run_incremental_coco.bat - incremental (add/modify/delete) testing
REM  for the CocoIndex pipeline, tier by tier.
REM
REM  Answers one question per store: does it correctly handle a document being
REM  added, modified and deleted while the pipeline watches a directory?
REM  Every tier runs tests/integration/test_cocoindex_changes.py with
REM  PIPELINE_BACKEND=cocoindex and a filesystem watch directory registered
REM  through the REST API with enable_sync.
REM
REM  Usage (from repo root):
REM      tests\integration\run_incremental_coco.bat            - all tiers
REM      tests\integration\run_incremental_coco.bat 2          - one tier
REM      tests\integration\run_incremental_coco.bat 3a         - RDF only
REM      tests\integration\run_incremental_coco.bat quick      - tier 0 + seed test only
REM
REM  Tiers:
REM    0   smoke, both source backends            2 jobs
REM    1   10 vector stores        (src flexible) 10 jobs
REM    2   12 property graph stores               12 jobs   <- slowest, KG extraction
REM    3a  4 RDF stores            (+ qdrant)     4 jobs
REM    3b  3 search stores         (+ qdrant)     3 jobs
REM    4   10 vector stores        (src cocoindex) 10 jobs  <- native delete path
REM
REM  Tier 2 is the long one: every add and modify runs LLM KG extraction.
REM  Tiers 1/3a/3b/4 pass --pg none to keep the LLM out of the loop.
REM
REM  Prerequisites:
REM    - .env with PIPELINE_BACKEND=cocoindex supported (the matrix sets it)
REM    - INTEGRATION_WATCH_DIR set to a dedicated folder
REM    - ENABLE_INCREMENTAL_UPDATES unset/false (mutually exclusive with cocoindex)
REM    - docker stack up for whichever stores the tier touches
REM
REM  Tuning:
REM    set COCOINDEX_SYNC_WAIT=30   before running to shorten failure waits
REM                                 (default is derived from COCOINDEX_POLL_INTERVAL)
REM
REM  Cloud PG stores (spanner, neptune, neptune_analytics) are omitted from
REM  tier 2 - add them to PG_ALL below when those instances are available.
REM ============================================================

setlocal enabledelayedexpansion
cd /d "%~dp0..\.."

set TIER=%1
if "%TIER%"=="" set TIER=all

set MATRIX=uv run tests/integration/run_matrix.py
set TESTPATH=tests/integration/test_cocoindex_changes.py
set COMMON=--clean --pipeline cocoindex --test-path %TESTPATH%

set VEC_ALL=qdrant,lancedb,postgres,chroma,milvus,weaviate,elasticsearch,opensearch,pinecone,neo4j
set PG_ALL=neo4j,falkordb,memgraph,arcadedb,nebula,ladybug,arangodb,apache_age,hugegraph,tigergraph,surrealdb,cosmos_gremlin
set RDF_ALL=fuseki,graphdb,oxigraph
set SEARCH_ALL=elasticsearch,opensearch,bm25

set LOGDIR=%~dp0logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
for /f "delims=" %%d in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd-HHmm"') do set STAMP=%%d
set LOG=%LOGDIR%\incremental-coco-%TIER%-%STAMP%.log

echo Incremental CocoIndex run (tier %TIER%) started %DATE% %TIME% > "%LOG%"
echo Log: %LOG%
echo.

set PASS=0
set FAIL=0

REM ---- Tier 0: smoke, both source backends -------------------------------
if /I "%TIER%"=="all"   set DO_T0=1
if /I "%TIER%"=="0"     set DO_T0=1
if /I "%TIER%"=="quick" set DO_T0=1
if defined DO_T0 (
    echo [tier 0] smoke - src flexible
    if /I "%TIER%"=="quick" (
        %MATRIX% %COMMON% --source-backend flexible --pg none --vector qdrant -k test_seed_is_indexed >> "%LOG%" 2>&1
    ) else (
        %MATRIX% %COMMON% --source-backend flexible --pg none --vector qdrant >> "%LOG%" 2>&1
    )
    if !ERRORLEVEL! EQU 0 (set /a PASS+=1) else (set /a FAIL+=1)

    echo [tier 0] smoke - src cocoindex
    if /I "%TIER%"=="quick" (
        %MATRIX% %COMMON% --source-backend cocoindex --pg none --vector qdrant -k test_seed_is_indexed >> "%LOG%" 2>&1
    ) else (
        %MATRIX% %COMMON% --source-backend cocoindex --pg none --vector qdrant >> "%LOG%" 2>&1
    )
    if !ERRORLEVEL! EQU 0 (set /a PASS+=1) else (set /a FAIL+=1)
)

REM ---- Tier 1: vector stores, flexible source ----------------------------
if /I "%TIER%"=="all" set DO_T1=1
if /I "%TIER%"=="1"   set DO_T1=1
if defined DO_T1 (
    echo [tier 1] 10 vector stores - src flexible
    %MATRIX% %COMMON% --source-backend flexible --pg none --vector %VEC_ALL% >> "%LOG%" 2>&1
    if !ERRORLEVEL! EQU 0 (set /a PASS+=1) else (set /a FAIL+=1)
)

REM ---- Tier 2: property graph stores -------------------------------------
REM  Only tier that keeps KG extraction on, so it is by far the slowest.
if /I "%TIER%"=="all" set DO_T2=1
if /I "%TIER%"=="2"   set DO_T2=1
if defined DO_T2 (
    echo [tier 2] 12 property graph stores - src flexible  [SLOW: KG extraction]
    %MATRIX% %COMMON% --source-backend flexible --vector qdrant --pg %PG_ALL% >> "%LOG%" 2>&1
    if !ERRORLEVEL! EQU 0 (set /a PASS+=1) else (set /a FAIL+=1)
)

REM ---- Tier 3a: RDF stores (each also gets qdrant) -----------------------
if /I "%TIER%"=="all" set DO_T3A=1
if /I "%TIER%"=="3"   set DO_T3A=1
if /I "%TIER%"=="3a"  set DO_T3A=1
if defined DO_T3A (
    echo [tier 3a] 4 RDF stores + qdrant
    %MATRIX% %COMMON% --source-backend flexible --pg none --vector qdrant --rdf %RDF_ALL% >> "%LOG%" 2>&1
    if !ERRORLEVEL! EQU 0 (set /a PASS+=1) else (set /a FAIL+=1)
)

REM ---- Tier 3b: search stores (each also gets qdrant) --------------------
if /I "%TIER%"=="all" set DO_T3B=1
if /I "%TIER%"=="3"   set DO_T3B=1
if /I "%TIER%"=="3b"  set DO_T3B=1
if defined DO_T3B (
    echo [tier 3b] 3 search stores + qdrant
    %MATRIX% %COMMON% --source-backend flexible --pg none --vector qdrant --search %SEARCH_ALL% >> "%LOG%" 2>&1
    if !ERRORLEVEL! EQU 0 (set /a PASS+=1) else (set /a FAIL+=1)
)

REM ---- Tier 4: vector stores via the NATIVE cocoindex source -------------
REM  The only tier that exercises native_apps._DeleteObservingLiveMapView -
REM  CocoIndex's own localfs watcher forwarding deletes to flexible targets.
if /I "%TIER%"=="all" set DO_T4=1
if /I "%TIER%"=="4"   set DO_T4=1
if defined DO_T4 (
    echo [tier 4] 10 vector stores - src cocoindex  [native delete path]
    %MATRIX% %COMMON% --source-backend cocoindex --pg none --vector %VEC_ALL% >> "%LOG%" 2>&1
    if !ERRORLEVEL! EQU 0 (set /a PASS+=1) else (set /a FAIL+=1)
)

REM ---- Summary -----------------------------------------------------------
REM  Per-job detail comes from run_matrix's own "[matrix] PASS/FAIL" lines, so
REM  the tier counters above and this list cannot disagree.
set "TIERFAILS=%TEMP%\inc-coco-fails-%RANDOM%.txt"
findstr /c:"[matrix] FAIL" "%LOG%" > "%TIERFAILS%" 2>nul
set JOBFAIL=0
for /f %%c in ('type "%TIERFAILS%" ^| find /c /v ""') do set JOBFAIL=%%c
set JOBPASS=0
for /f %%c in ('findstr /c:"[matrix] PASS" "%LOG%" ^| find /c /v ""') do set JOBPASS=%%c

echo. >> "%LOG%"
echo ============================================================ >> "%LOG%"
echo  INCREMENTAL COCOINDEX SUMMARY (tier %TIER%) >> "%LOG%"
echo    Tier groups passed: %PASS%   failed: %FAIL% >> "%LOG%"
echo    Individual jobs   : %JOBPASS% passed, %JOBFAIL% failed >> "%LOG%"
if %JOBFAIL% GTR 0 (
    echo. >> "%LOG%"
    echo  Failed jobs: >> "%LOG%"
    type "%TIERFAILS%" >> "%LOG%"
)
echo ============================================================ >> "%LOG%"
del "%TIERFAILS%" 2>nul

echo.
echo ============================================================
echo  Tier groups: %PASS% passed, %FAIL% failed
echo  Jobs:        %JOBPASS% passed, %JOBFAIL% failed
echo  Log:         %LOG%
echo ============================================================

if %FAIL% GTR 0 exit /b 1
if %JOBFAIL% GTR 0 exit /b 1
exit /b 0
