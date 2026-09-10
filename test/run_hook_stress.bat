@REM Hook-system stability scenarios (h* plugin family). Each scenario is a
@REM standalone plugin that builds its own private pools and asserts PASS/FAIL
@REM invariants. Every scenario runs even when an earlier one fails, so one
@REM broken area never hides another; the script exits 1 if any scenario FAILed.
@REM Run with the race detector:  run_hook_stress.bat race
@REM   (race builds a -race binary; slower, use it in CI to catch hook races)
@REM Per-plugin knobs: see `agilepool_test.exe --help hcount` etc.
if "%1"=="race" (
    go build -race -o agilepool_test.exe .
) else (
    go build -o agilepool_test.exe .
)
if errorlevel 1 exit /b 1

set FAILED=0
call :scenario --hcount
call :scenario --hpanic num=3000 stage=all
call :scenario --hpanic num=200 level=callback stage=all 2>nul
call :scenario --horder
call :scenario --hctx
call :scenario --hblock num=2000 delay-us=100
call :scenario --hchurn
call :scenario --hreenter
call :scenario --hclose
call :scenario --henqueue

if not "%FAILED%"=="0" (
    @echo hook-stability suite: FAILURES
    exit /b 1
)
@echo hook-stability suite: all PASS
exit /b 0

:scenario
.\agilepool_test.exe %*
if errorlevel 1 set FAILED=1
exit /b 0
