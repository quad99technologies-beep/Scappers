@echo off
REM Netherlands Pipeline Runner - Standardized
REM Supports Resume/Checkpoint functionality

REM Enable unbuffered output for real-time console updates
set PYTHONUNBUFFERED=1

cd /d "%~dp0"

REM Setup logging
setlocal enabledelayedexpansion

REM Create timestamp
for /f "usebackq tokens=*" %%a in (`powershell -NoProfile -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"` ) do set timestamp=%%a

set log_file=..\..\output\Netherlands\Netherlands_run_%timestamp%.log

REM Create output directory if it doesn't exist
if not exist "..\..\output\Netherlands" mkdir "..\..\output\Netherlands"

echo ================================================================================
echo Netherlands Pipeline - Starting at %date% %time%
echo ================================================================================
echo.
echo Log file: !log_file!
echo.

REM Delegates execution to the Python resume runner
REM This runner handles:
REM 1. Step skipping (checkpoints)
REM 2. Output verification
REM 3. Error handling
REM 4. Integration with platform config

python -u run_pipeline_resume.py %* 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '!log_file!' -Append"

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Pipeline failed with exit code %ERRORLEVEL%
    echo Details can be found in !log_file!
    exit /b %ERRORLEVEL%
)

echo.
echo ================================================================================
echo Netherlands Pipeline - Completed at %date% %time%
echo ================================================================================
echo.
pause
