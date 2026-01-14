@echo off
REM Belarus Pipeline Runner
REM Runs all workflow steps in sequence with resume/checkpoint support
REM By default, resumes from last completed step
REM Use run_pipeline_resume.py --fresh to start fresh

REM Enable unbuffered output for real-time console updates
set PYTHONUNBUFFERED=1

cd /d "%~dp0"

REM Use resume script if available, otherwise fall back to original behavior
if exist "run_pipeline_resume.py" (
    python -u "run_pipeline_resume.py" %*
    exit /b %errorlevel%
)

REM Setup logging - create log file with timestamp
setlocal enabledelayedexpansion
echo Get-Date -Format 'yyyyMMdd_HHmmss' > "%TEMP%\get_timestamp.ps1"
for /f %%I in ('powershell -NoProfile -ExecutionPolicy Bypass -File "%TEMP%\get_timestamp.ps1"') do set timestamp=%%I
del "%TEMP%\get_timestamp.ps1" 2>nul
set log_file=..\..\output\Belarus\Belarus_run_%timestamp%.log

REM Create output directory if it doesn't exist
if not exist "..\..\output\Belarus" mkdir "..\..\output\Belarus"

REM Output to both console (for real-time GUI) and log file
REM Use a simple approach: redirect to log file AND display in real-time using PowerShell Tee-Object

REM Initialize log file with header
(
echo ================================================================================
echo Belarus Pipeline - Starting at %date% %time%
echo ================================================================================
echo.
) > "%log_file%"

REM Step 0: Backup and Clean
echo [Step 0/1] Backup and Clean... >> "%log_file%"
echo [Step 0/1] Backup and Clean...
python -u "00_backup_and_clean.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Backup and Clean failed >> "%log_file%"
    echo ERROR: Backup and Clean failed
    exit /b 1
)

REM Step 1: Extract RCETH Data
echo. >> "%log_file%"
echo [Step 1/1] Extract RCETH Data... >> "%log_file%"
echo.
echo [Step 1/1] Extract RCETH Data...
python -u "01_belarus_rceth_extract.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Extract RCETH Data failed >> "%log_file%"
    echo ERROR: Extract RCETH Data failed
    exit /b 1
)

echo. >> "%log_file%"
echo ================================================================================
echo Belarus Pipeline - Completed successfully at %date% %time% >> "%log_file%"
echo ================================================================================
echo.
echo ================================================================================
echo Belarus Pipeline - Completed successfully at %date% %time%
echo ================================================================================

REM Clean up lock file after successful completion
python "cleanup_lock.py" 2>nul

echo. >> "%log_file%"
echo Log file saved to: %log_file% >> "%log_file%"
echo.
echo Log file saved to: %log_file%
