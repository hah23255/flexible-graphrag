@echo off
REM ============================================================
REM  Full overnight chain for flexible-graphrag
REM  Run from repo root:
REM      tests\integration\run_overnight_all.bat
REM
REM  Phases (always in this order; each continues after the previous):
REM    1. run_full_suite.bat all
REM         - CocoIndex native suites (1-3) + Flexible default pipeline (4-12)
REM    2. run_full_suite.bat coco-flex
REM         - Flexible suites (4-12) again with PIPELINE_BACKEND=cocoindex
REM    3. run_incremental_coco.bat
REM         - Incremental add/modify/delete under the CocoIndex pipeline,
REM           tiers 0-4 (vector / property graph / RDF / search, both source
REM           backends).  Tier 2 is the slow one - it keeps KG extraction on.
REM    4. run_overnight.bat
REM         - LLM / embedding / incremental / matrix overnight suites
REM
REM  Four phase logs (full detail - look at these separately):
REM    tests\integration\logs\full_suite-all-<date>.log
REM    tests\integration\logs\full_suite-coco-flex-<date>.log
REM    tests\integration\logs\incremental-coco-all-<date>.log  (written by that script)
REM    tests\integration\logs\overnight-<date>.log             (same as a normal overnight run)
REM
REM  Short index only:
REM    tests\integration\logs\overnight-all-<date>.log
REM ============================================================

setlocal enabledelayedexpansion

cd /d "%~dp0..\.."

for /f "delims=" %%d in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd-HHmm"') do set STAMP=%%d
set LOGDIR=%~dp0logs
set MASTER_LOG=%LOGDIR%\overnight-all-%STAMP%.log
set LOG1=%LOGDIR%\full_suite-all-%STAMP%.log
set LOG2=%LOGDIR%\full_suite-coco-flex-%STAMP%.log
REM Phase 3 console output; its per-job detail goes to the script's own
REM incremental-coco-all-<date>.log (it uses setlocal, so its LOG cannot leak here).
set LOG3=%LOGDIR%\incremental-coco-phase-%STAMP%.log
REM Phase 4 uses run_overnight.bat's own overnight-<date>.log (written by that script).
set LOG4=

if not exist "%LOGDIR%" mkdir "%LOGDIR%"

echo Overnight-all started %DATE% %TIME% > "%MASTER_LOG%"
echo. >> "%MASTER_LOG%"
echo Phase logs (full detail): >> "%MASTER_LOG%"
echo   1. %LOG1% >> "%MASTER_LOG%"
echo   2. %LOG2% >> "%MASTER_LOG%"
echo   3. %LOG3% ^(plus incremental-coco-all-^<date^>.log^) >> "%MASTER_LOG%"
echo   4. ^(overnight-^<date^>.log created by run_overnight.bat^) >> "%MASTER_LOG%"
echo. >> "%MASTER_LOG%"

set FAIL=0
set RC1=0
set RC2=0
set RC3=0
set RC4=0

REM ------------------------------------------------------------
REM  Phase 1 - full suite (native coco + flexible default pipeline)
REM ------------------------------------------------------------
echo.
echo ============================================================
echo  [%TIME%] PHASE 1/4: run_full_suite.bat all
echo  Log: %LOG1%
echo ============================================================
echo [%TIME%] PHASE 1/4 START: run_full_suite.bat all >> "%MASTER_LOG%"
echo [%TIME%] PHASE 1/4 Log: %LOG1% >> "%MASTER_LOG%"
call "%~dp0run_full_suite.bat" all > "%LOG1%" 2>&1
set RC1=!ERRORLEVEL!
if !RC1! EQU 0 (
    echo [%TIME%] PHASE 1/4 PASS >> "%MASTER_LOG%"
) else (
    echo [%TIME%] PHASE 1/4 FAIL exit=!RC1! >> "%MASTER_LOG%"
    set /a FAIL+=1
)
echo. >> "%MASTER_LOG%"

REM ------------------------------------------------------------
REM  Phase 2 - flexible suites under PIPELINE_BACKEND=cocoindex
REM ------------------------------------------------------------
echo.
echo ============================================================
echo  [%TIME%] PHASE 2/4: run_full_suite.bat coco-flex
echo  Log: %LOG2%
echo ============================================================
echo [%TIME%] PHASE 2/4 START: run_full_suite.bat coco-flex >> "%MASTER_LOG%"
echo [%TIME%] PHASE 2/4 Log: %LOG2% >> "%MASTER_LOG%"
call "%~dp0run_full_suite.bat" coco-flex > "%LOG2%" 2>&1
set RC2=!ERRORLEVEL!
if !RC2! EQU 0 (
    echo [%TIME%] PHASE 2/4 PASS >> "%MASTER_LOG%"
) else (
    echo [%TIME%] PHASE 2/4 FAIL exit=!RC2! >> "%MASTER_LOG%"
    set /a FAIL+=1
)
echo. >> "%MASTER_LOG%"

REM ------------------------------------------------------------
REM  Phase 3 - incremental add/modify/delete under CocoIndex (tiers 0-4)
REM  Needs INTEGRATION_WATCH_DIR set and ENABLE_INCREMENTAL_UPDATES off
REM  (mutually exclusive with PIPELINE_BACKEND=cocoindex); the tests skip
REM  rather than fail when that is not the case.
REM ------------------------------------------------------------
echo.
echo ============================================================
echo  [%TIME%] PHASE 3/4: run_incremental_coco.bat
echo  Log: %LOG3%
echo ============================================================
echo [%TIME%] PHASE 3/4 START: run_incremental_coco.bat >> "%MASTER_LOG%"
echo [%TIME%] PHASE 3/4 Log: %LOG3% >> "%MASTER_LOG%"
call "%~dp0run_incremental_coco.bat" > "%LOG3%" 2>&1
set RC3=!ERRORLEVEL!
if !RC3! EQU 0 (
    echo [%TIME%] PHASE 3/4 PASS >> "%MASTER_LOG%"
) else (
    echo [%TIME%] PHASE 3/4 FAIL exit=!RC3! >> "%MASTER_LOG%"
    set /a FAIL+=1
)
echo. >> "%MASTER_LOG%"

REM ------------------------------------------------------------
REM  Phase 4 - overnight matrix (LLM / emb / incremental / ...)
REM  run_overnight.bat writes its normal overnight-<date>.log itself.
REM ------------------------------------------------------------
echo.
echo ============================================================
echo  [%TIME%] PHASE 4/4: run_overnight.bat
echo  Log: overnight-^<date^>.log ^(written by run_overnight.bat^)
echo ============================================================
echo [%TIME%] PHASE 4/4 START: run_overnight.bat >> "%MASTER_LOG%"
REM Clear LOG so overnight.bat sets its own path; capture it after return.
set LOG=
call "%~dp0run_overnight.bat"
set RC4=!ERRORLEVEL!
REM overnight.bat does not use setlocal, so LOG is the overnight log path.
if defined LOG (
    set LOG4=!LOG!
) else (
    set LOG4=%LOGDIR%\overnight-*.log
)
echo [%TIME%] PHASE 4/4 Log: !LOG4! >> "%MASTER_LOG%"
if !RC4! EQU 0 (
    echo [%TIME%] PHASE 4/4 PASS >> "%MASTER_LOG%"
) else (
    echo [%TIME%] PHASE 4/4 FAIL exit=!RC4! >> "%MASTER_LOG%"
    set /a FAIL+=1
)
echo. >> "%MASTER_LOG%"

REM Roll the individual job failures up from every phase log, so the index
REM says WHICH jobs failed rather than only how many phases did.  Phase exit
REM codes alone hid two failing matrix jobs on 2026-08-13.
set "ALLFAILS=%TEMP%\overnight-all-fails-%RANDOM%.txt"
type nul > "%ALLFAILS%"
for %%L in ("%LOG1%" "%LOG2%" "%LOG3%") do (
    if exist %%L findstr /c:"[matrix] FAIL" %%L >> "%ALLFAILS%" 2>nul
)
REM Phase 3 writes per-job detail to its own log, not the console we captured.
if exist "%LOGDIR%\incremental-coco-*.log" (
    for %%L in ("%LOGDIR%\incremental-coco-*.log") do findstr /c:"[matrix] FAIL" "%%L" >> "%ALLFAILS%" 2>nul
)
if exist "%LOGDIR%\overnight-*.log" (
    for %%L in ("%LOGDIR%\overnight-*.log") do findstr /c:"[matrix] FAIL" "%%L" >> "%ALLFAILS%" 2>nul
)
set JOBFAIL=0
for /f %%c in ('type "%ALLFAILS%" ^| find /c /v ""') do set JOBFAIL=%%c

echo ============================================================ >> "%MASTER_LOG%"
echo Overnight-all finished %DATE% %TIME% >> "%MASTER_LOG%"
echo   Phase 1 exit=!RC1!  log=%LOG1% >> "%MASTER_LOG%"
echo   Phase 2 exit=!RC2!  log=%LOG2% >> "%MASTER_LOG%"
echo   Phase 3 exit=!RC3!  log=%LOG3% >> "%MASTER_LOG%"
echo   Phase 4 exit=!RC4!  log=!LOG4! >> "%MASTER_LOG%"
echo   Failed phases: !FAIL! / 4 >> "%MASTER_LOG%"
echo   Failed jobs:   %JOBFAIL% >> "%MASTER_LOG%"
if %JOBFAIL% GTR 0 (
    echo. >> "%MASTER_LOG%"
    echo   Failed job detail: >> "%MASTER_LOG%"
    type "%ALLFAILS%" >> "%MASTER_LOG%"
)
del "%ALLFAILS%" 2>nul
echo ============================================================ >> "%MASTER_LOG%"
echo.
echo Overnight-all finished. Failed phases: !FAIL! / 4
echo.
echo Phase logs:
echo   1. %LOG1%
echo   2. %LOG2%
echo   3. %LOG3%
echo   4. !LOG4!
echo Index:  %MASTER_LOG%
echo.

if !FAIL! GTR 0 exit /b 1
exit /b 0
