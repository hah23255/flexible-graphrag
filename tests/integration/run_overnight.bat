@echo off
REM ============================================================
REM  Overnight integration test suite for flexible-graphrag
REM  Run from repo root:  tests\integration\run_overnight.bat
REM  Results logged to:   tests\integration\logs\overnight-<date>.log
REM
REM  Note on --backends: this is a run_matrix.py convenience shorthand
REM  that sets GRAPH_BACKEND + VECTOR_BACKEND + SEARCH_BACKEND at once.
REM  It is NOT a real flexible-graphrag config key.  Use the explicit
REM  --graph-backend / --vector-backend / --search-backend flags when
REM  you need to set only one dimension independently.
REM ============================================================

cd /d "%~dp0..\.."
REM Build a safe date string using PowerShell (avoids locale-dependent %%DATE%% format)
for /f "delims=" %%d in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd-HHmm"') do set YYYYMMDD=%%d
REM Use absolute path so the log works regardless of where the bat is called from
set LOG=%~dp0logs\overnight-%YYYYMMDD%.log
echo Overnight run started %DATE% %TIME% > "%LOG%"
echo. >> "%LOG%"

set MATRIX=uv run tests/integration/run_matrix.py --clean

REM ============================================================
REM  1. All LLM providers - LlamaIndex backend (qdrant only)
REM     DONE: 12/12 PASS
REM ============================================================
echo [%TIME%] 1. All LLM providers - LI backend >> "%LOG%"
%MATRIX% --pg none --vector qdrant --llm openai,ollama,gemini,anthropic,vertex_ai,bedrock,fireworks,openai_like,vllm,litellm,openrouter,azure_openai >> "%LOG%" 2>&1

REM ============================================================
REM  1b. All LLM providers - LangChain backend (qdrant only, no PG)
REM      LC LLMs are used by: QA synthesis, Synonym Expander
REM      No PG — skips slow KG extraction; graph retriever tested separately in section 6
REM ============================================================
echo [%TIME%] 1b. All LLM providers - LC backend >> "%LOG%"
%MATRIX% --pg none --vector qdrant --backends langchain --fusion langchain --llm openai,ollama,gemini,anthropic,vertex_ai,bedrock,fireworks,openai_like,vllm,litellm,openrouter,azure_openai >> "%LOG%" 2>&1

REM ============================================================
REM  2. All embedding providers - LlamaIndex backend (qdrant only)
REM     PREV: 4/7 PASS. FIXED: cross-provider API key, google dim, fireworks endpoint
REM     Expecting 7+/8 PASS after embedding factory fixes
REM ============================================================
echo [%TIME%] 2. All embedding providers - LI backend >> "%LOG%"
%MATRIX% --vector qdrant --embedding openai,ollama,google,vertex,azure,bedrock,fireworks,openai_like >> "%LOG%" 2>&1

REM ============================================================
REM  2b. All embedding providers - LangChain backend (qdrant only)
REM ============================================================
echo [%TIME%] 2b. All embedding providers - LC backend >> "%LOG%"
%MATRIX% --vector qdrant --backends langchain --embedding openai,ollama,google,vertex,azure,bedrock,fireworks,openai_like >> "%LOG%" 2>&1

REM ============================================================
REM  2c. LiteLLM embedding - both LI and LC backends
REM      Requires LiteLLM proxy running: litellm --config litellm_config.yaml --port 4000
REM      Skip if proxy not running (will error with connection refused, not 403)
REM ============================================================
echo [%TIME%] 2c. LiteLLM embedding - both backends >> "%LOG%"
%MATRIX% --vector qdrant --backends both --embedding litellm >> "%LOG%" 2>&1

REM ============================================================
REM  3. All vector DBs - LlamaIndex backend
REM     DONE: 10/10 PASS
REM ============================================================
echo [%TIME%] 3. All vector DBs - LlamaIndex backend >> "%LOG%"
%MATRIX% --vector all --backends llamaindex --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  4. All vector DBs - LangChain backend
REM     DONE: 10/10 PASS
REM ============================================================
echo [%TIME%] 4. All vector DBs - LangChain backend >> "%LOG%"
%MATRIX% --vector all --backends langchain --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  5. All PG stores - LlamaIndex backend
REM     DONE: 6/6 PASS (local only - neo4j, arcadedb, falkordb, memgraph, nebula, ladybug)
REM     Cloud: neptune, neptune_analytics, spanner (run separately when available)
REM ============================================================
echo [%TIME%] 5. All PG stores - LI backend - general tests >> "%LOG%"
%MATRIX% --pg neo4j,arcadedb,falkordb,memgraph,nebula,ladybug --vector qdrant --backends llamaindex --llm openai >> "%LOG%" 2>&1
echo [%TIME%] 5a. All PG stores - LI backend - graph query tests >> "%LOG%"
%MATRIX% --pg neo4j,arcadedb,falkordb,memgraph,nebula,ladybug --vector none --search none --rdf none --backends llamaindex --test-path tests/integration/test_graph_query.py >> "%LOG%" 2>&1

REM ============================================================
REM  5b. PG stores - LI backend - cloud (TEMP commented out — restore when available)
REM ============================================================
REM echo [%TIME%] 5b. PG stores - LI backend (cloud) - general tests >> "%LOG%"
REM %MATRIX% --pg neptune,neptune_analytics,spanner --vector qdrant --backends llamaindex --llm openai >> "%LOG%" 2>&1
REM echo [%TIME%] 5c. PG stores - LI backend (cloud) - graph query tests >> "%LOG%"
REM %MATRIX% --pg neptune,neptune_analytics,spanner --vector none --search none --rdf none --backends llamaindex --test-path tests/integration/test_graph_query.py >> "%LOG%" 2>&1

REM ============================================================
REM  6. All PG stores - LangChain backend (local only; cloud commented below)
REM ============================================================
echo [%TIME%] 6. All PG stores - LC backend - general tests >> "%LOG%"
%MATRIX% --pg neo4j,arcadedb,falkordb,memgraph,nebula,tigergraph,arangodb,apache_age,hugegraph,surrealdb,cosmos_gremlin,ladybug --vector qdrant --backends langchain --llm openai >> "%LOG%" 2>&1
echo [%TIME%] 6a. All PG stores - LC backend - graph query tests >> "%LOG%"
%MATRIX% --pg neo4j,arcadedb,falkordb,memgraph,nebula,tigergraph,arangodb,apache_age,hugegraph,surrealdb,cosmos_gremlin,ladybug --vector none --search none --rdf none --backends langchain --test-path tests/integration/test_graph_query.py >> "%LOG%" 2>&1
REM Restore cloud LC PG when available:
REM %MATRIX% --pg ...,neptune,neptune_analytics --vector qdrant --backends langchain --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  7. All RDF stores - local (fuseki, graphdb, oxigraph)
REM ============================================================
echo [%TIME%] 7. RDF stores (local) - general search tests >> "%LOG%"
%MATRIX% --rdf fuseki,graphdb,oxigraph --vector qdrant --backends langchain --fusion langchain --llm openai >> "%LOG%" 2>&1
echo [%TIME%] 7a. RDF stores (local) - RDF-specific endpoint tests >> "%LOG%"
%MATRIX% --rdf fuseki,graphdb,oxigraph --pg none --vector none --search none --test-path tests/integration/test_rdf.py >> "%LOG%" 2>&1
echo [%TIME%] 7b. RDF stores (local) - graph/query SPARQL fallback >> "%LOG%"
%MATRIX% --rdf fuseki,graphdb,oxigraph --pg none --vector none --search none --test-path tests/integration/test_graph_query.py >> "%LOG%" 2>&1

REM ============================================================
REM  7c. RDF stores - Neptune (cloud) — TEMP commented out
REM ============================================================
REM echo [%TIME%] 7c. RDF stores - Neptune RDF - general tests >> "%LOG%"
REM %MATRIX% --rdf neptune_rdf --vector qdrant --backends langchain --fusion langchain --llm openai >> "%LOG%" 2>&1
REM echo [%TIME%] 7d. RDF stores - Neptune RDF - RDF-specific + graph query tests >> "%LOG%"
REM %MATRIX% --rdf neptune_rdf --pg none --vector none --search none --test-path tests/integration/test_rdf.py >> "%LOG%" 2>&1
REM %MATRIX% --rdf neptune_rdf --pg none --vector none --search none --test-path tests/integration/test_graph_query.py >> "%LOG%" 2>&1

REM ============================================================
REM  8. All search DBs - LlamaIndex backend
REM ============================================================
echo [%TIME%] 8. All search DBs - LlamaIndex backend >> "%LOG%"
%MATRIX% --vector qdrant --search all --backends llamaindex --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  9. All search DBs - LangChain backend
REM ============================================================
echo [%TIME%] 9. All search DBs - LangChain backend >> "%LOG%"
%MATRIX% --vector qdrant --search all --backends langchain --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  10. Chunker backends - LI chunker vs LC chunker
REM ============================================================
echo [%TIME%] 10a. Chunker - LlamaIndex >> "%LOG%"
%MATRIX% --vector qdrant --chunker llamaindex --llm openai >> "%LOG%" 2>&1
echo [%TIME%] 10b. Chunker - LangChain >> "%LOG%"
%MATRIX% --vector qdrant --chunker langchain --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  11. KG extractor backends - LI extractor vs LC extractor
REM ============================================================
echo [%TIME%] 11a. KG extractor - LlamaIndex >> "%LOG%"
%MATRIX% --pg neo4j --vector qdrant --backends llamaindex --llm openai >> "%LOG%" 2>&1
echo [%TIME%] 11b. KG extractor - LangChain >> "%LOG%"
%MATRIX% --pg neo4j --vector qdrant --backends langchain --chunker llamaindex --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  12. Full 4-DB stack - LI pipeline + LI fusion
REM ============================================================
echo [%TIME%] 12. Full stack - LI pipeline + LI fusion >> "%LOG%"
%MATRIX% --pg neo4j --rdf fuseki --vector qdrant --search elasticsearch --backends llamaindex --fusion llamaindex --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  13. Full 4-DB stack - LC pipeline + LC fusion
REM ============================================================
echo [%TIME%] 13. Full stack - LC pipeline + LC fusion >> "%LOG%"
%MATRIX% --pg neo4j --rdf fuseki --vector qdrant --search elasticsearch --backends langchain --fusion langchain --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  14. Incremental updates - all vector DBs (LI + LC backends)
REM      Primary verification: add/modify/delete via search()
REM ============================================================
echo [%TIME%] 14. Incremental - all vector DBs - LI backend >> "%LOG%"
%MATRIX% --vector all --search none --backends llamaindex --incremental --inc-ops ingest,add,modify,delete --llm openai >> "%LOG%" 2>&1

echo [%TIME%] 14b. Incremental - all vector DBs - LC backend >> "%LOG%"
%MATRIX% --vector all --search none --backends langchain --incremental --inc-ops ingest,add,modify,delete --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  14c. Incremental updates - all search DBs (LI + LC backends)
REM ============================================================
echo [%TIME%] 14c. Incremental - all search DBs - LI backend >> "%LOG%"
%MATRIX% --vector none --search all --backends llamaindex --incremental --inc-ops ingest,add,modify,delete --llm openai >> "%LOG%" 2>&1

echo [%TIME%] 14d. Incremental - all search DBs - LC backend >> "%LOG%"
%MATRIX% --vector none --search all --backends langchain --incremental --inc-ops ingest,add,modify,delete --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  15. Incremental updates - all PG stores (LI backend)
REM      Verifies graph nodes/edges deleted on file remove
REM ============================================================
echo [%TIME%] 15. Incremental - all local PG stores - LI backend >> "%LOG%"
%MATRIX% --pg neo4j,arcadedb,falkordb,memgraph,nebula,ladybug --vector qdrant --search none --backends llamaindex --incremental --inc-ops ingest,add,modify,delete --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  15b. Incremental updates - all PG stores (LC backend)
REM ============================================================
echo [%TIME%] 15b. Incremental - all local PG stores - LC backend >> "%LOG%"
%MATRIX% --pg neo4j,arcadedb,falkordb,memgraph,nebula,tigergraph,arangodb,apache_age,hugegraph,surrealdb,ladybug --vector qdrant --search none --backends langchain --incremental --inc-ops ingest,add,modify,delete --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  15c. Incremental updates - all 4 RDF stores (local + Neptune RDF)
REM      Verifies triples deleted on file remove
REM ============================================================
echo [%TIME%] 15c. Incremental - all local RDF stores (fuseki, graphdb, oxigraph) >> "%LOG%"
%MATRIX% --rdf fuseki,graphdb,oxigraph --pg none --vector qdrant --search none --backends llamaindex --incremental --inc-ops ingest,add,modify,delete --llm openai >> "%LOG%" 2>&1
REM Restore Neptune RDF when available:
REM %MATRIX% --rdf fuseki,graphdb,oxigraph,neptune_rdf --pg none --vector qdrant --search none --backends llamaindex --incremental --inc-ops ingest,add,modify,delete --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  15d. Incremental updates - cloud PG stores (LI) — TEMP commented out
REM       neptune, neptune_analytics, spanner
REM ============================================================
REM echo [%TIME%] 15d. Incremental - cloud PG stores - LI backend >> "%LOG%"
REM %MATRIX% --pg neptune,neptune_analytics,spanner --vector qdrant --search none --backends llamaindex --incremental --inc-ops ingest,add,modify,delete --llm openai >> "%LOG%" 2>&1

REM ============================================================
REM  15e. Incremental updates - cloud PG stores (LC)
REM       cosmos_gremlin kept (local Gremlin server); neptunes commented out
REM ============================================================
echo [%TIME%] 15e. Incremental - cloud/local PG stores - LC backend (cosmos_gremlin) >> "%LOG%"
%MATRIX% --pg cosmos_gremlin --vector qdrant --search none --backends langchain --incremental --inc-ops ingest,add,modify,delete --llm openai >> "%LOG%" 2>&1
REM Restore neptunes when available:
REM %MATRIX% --pg neptune,neptune_analytics,cosmos_gremlin --vector qdrant --search none --backends langchain --incremental --inc-ops ingest,add,modify,delete --llm openai >> "%LOG%" 2>&1

REM  Single cleanup after all incremental sections — each run's --clean already
REM  wipes stores before starting; this just leaves postgres tables tidy at the end.
echo [%TIME%] Post-incremental cleanup (document_state + datasource_config) >> "%LOG%"
set ENABLE_INCREMENTAL_UPDATES=true
uv run scripts/cleanup.py --matrix-clean >> "%LOG%" 2>&1
set ENABLE_INCREMENTAL_UPDATES=

REM ============================================================
REM  16. Ontology: true vs false across key PG stores
REM      Tests KG extraction quality with and without ontology guidance.
REM      Run against a few representative stores (LI + LC each).
REM ============================================================
echo [%TIME%] 16. Ontology true vs false - LI (neo4j, arcadedb) >> "%LOG%"
%MATRIX% --pg neo4j,arcadedb --vector qdrant --backends llamaindex --llm openai --ontology both >> "%LOG%" 2>&1
echo [%TIME%] 16b. Ontology true vs false - LC (neo4j, arangodb) >> "%LOG%"
%MATRIX% --pg neo4j,arangodb --vector qdrant --backends langchain --llm openai --ontology both >> "%LOG%" 2>&1

REM ============================================================
REM  17. Multi-format folder ingest - Docling parser
REM      Ingests all files in sample-docs: .txt, .pdf, .docx, .xlsx, .pptx
REM      Only runs when --test-dir is explicitly provided here.
REM      NOT auto-activated by INTEGRATION_TEST_DIR env var in other sections.
REM ============================================================
echo [%TIME%] 17. Folder ingest - Docling parser - LI backend >> "%LOG%"
%MATRIX% --vector qdrant --backends llamaindex --llm openai --doc-parser docling --test-dir sample-docs --test-path tests/integration/test_folder_ingest.py >> "%LOG%" 2>&1
echo [%TIME%] 17b. Folder ingest - Docling parser - LC backend >> "%LOG%"
%MATRIX% --vector qdrant --backends langchain --llm openai --doc-parser docling --test-dir sample-docs --test-path tests/integration/test_folder_ingest.py >> "%LOG%" 2>&1

REM ============================================================
REM  18. Multi-format folder ingest - LlamaParse parser
REM      Requires LLAMA_CLOUD_API_KEY in .env.
REM      Skip if key not configured (run_matrix will error with auth failure).
REM ============================================================
echo [%TIME%] 18. Folder ingest - LlamaParse - LI backend >> "%LOG%"
%MATRIX% --vector qdrant --backends llamaindex --llm openai --doc-parser llamaparse --test-dir sample-docs --test-path tests/integration/test_folder_ingest.py >> "%LOG%" 2>&1
echo [%TIME%] 18b. Folder ingest - LlamaParse - LC backend >> "%LOG%"
%MATRIX% --vector qdrant --backends langchain --llm openai --doc-parser llamaparse --test-dir sample-docs --test-path tests/integration/test_folder_ingest.py >> "%LOG%" 2>&1

echo. >> "%LOG%"
echo [%TIME%] Overnight run finished >> "%LOG%"

REM ============================================================
REM  Summary + exit code.
REM  Derived from the log rather than from ERRORLEVEL after each of the ~36
REM  matrix calls above: those were never checked, so this script always exited
REM  0 and run_overnight_all.bat recorded the phase as PASS even when individual
REM  jobs had failed (two did on 2026-08-13 and went unnoticed).  run_matrix.py
REM  prints one "[matrix] PASS/FAIL <label>" line per job, so counting them here
REM  cannot drift out of sync with the call sites.
REM ============================================================
set "SUMTMP=%TEMP%\overnight-fails-%RANDOM%.txt"
findstr /c:"[matrix] FAIL" "%LOG%" > "%SUMTMP%" 2>nul
set OVN_FAIL=0
for /f %%c in ('type "%SUMTMP%" ^| find /c /v ""') do set OVN_FAIL=%%c
set OVN_PASS=0
for /f %%c in ('findstr /c:"[matrix] PASS" "%LOG%" ^| find /c /v ""') do set OVN_PASS=%%c

echo. >> "%LOG%"
echo ============================================================ >> "%LOG%"
echo  OVERNIGHT SUMMARY >> "%LOG%"
echo    Jobs passed: %OVN_PASS% >> "%LOG%"
echo    Jobs failed: %OVN_FAIL% >> "%LOG%"
if %OVN_FAIL% GTR 0 (
    echo. >> "%LOG%"
    echo  Failed jobs: >> "%LOG%"
    type "%SUMTMP%" >> "%LOG%"
)
echo ============================================================ >> "%LOG%"
del "%SUMTMP%" 2>nul

echo.
echo Overnight run: %OVN_PASS% job(s) passed, %OVN_FAIL% failed.
echo Done. Results in %LOG%
if %OVN_FAIL% GTR 0 exit /b 1
exit /b 0
