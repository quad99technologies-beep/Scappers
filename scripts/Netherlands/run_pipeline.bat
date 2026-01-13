@echo off
REM Netherlands Pipeline Runner
REM Runs all workflow steps in sequence

REM Enable unbuffered output for real-time console updates
set PYTHONUNBUFFERED=1

cd /d "%~dp0"

REM Check if run_pipeline_resume.py exists and use it if available
if exist "run_pipeline_resume.py" (
    echo [PIPELINE] Using checkpoint-aware pipeline runner...
    python -u "run_pipeline_resume.py" %*
    exit /b %errorlevel%
)

REM Setup logging - create log file with timestamp
setlocal enabledelayedexpansion
echo Get-Date -Format 'yyyyMMdd_HHmmss' > "%TEMP%\get_timestamp.ps1"
for /f %%I in ('powershell -NoProfile -ExecutionPolicy Bypass -File "%TEMP%\get_timestamp.ps1"') do set timestamp=%%I
del "%TEMP%\get_timestamp.ps1" 2>nul
set log_file=..\..\output\Netherlands\Netherlands_run_%timestamp%.log

REM Create output directory if it doesn't exist
if not exist "..\..\output\Netherlands" mkdir "..\..\output\Netherlands"

REM Output to both console (for real-time GUI) and log file
REM Initialize log file with header
(
echo ================================================================================
echo Netherlands Pipeline - Starting at %date% %time%
echo ================================================================================
echo.
) > "%log_file%"

echo ================================================================================
echo Netherlands Pipeline - Starting at %date% %time%
echo ================================================================================
echo.

REM Step 0: Backup and Clean
echo [PROGRESS] Pipeline Step: 0/2 (0.0%%) - Preparing: Backing up previous results and cleaning output directory >> "%log_file%"
echo [PROGRESS] Pipeline Step: 0/2 (0.0%%) - Preparing: Backing up previous results and cleaning output directory
echo [Step 0/2] Backup and Clean... >> "%log_file%"
echo [Step 0/2] Backup and Clean...
python -u "00_backup_and_clean.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Backup and Clean failed >> "%log_file%"
    echo ERROR: Backup and Clean failed
    exit /b 1
)

REM Step 1: Collect URLs
echo. >> "%log_file%"
echo [PROGRESS] Pipeline Step: 1/2 (50.0%%) - Collecting: Gathering product URLs from search terms >> "%log_file%"
echo [PROGRESS] Pipeline Step: 1/2 (50.0%%) - Collecting: Gathering product URLs from search terms
echo [Step 1/2] Collect URLs... >> "%log_file%"
echo.
echo [Step 1/2] Collect URLs...
python -u "01_collect_urls.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Collect URLs failed >> "%log_file%"
    echo ERROR: Collect URLs failed
    exit /b 1
)

REM Step 2: Reimbursement Extraction
echo. >> "%log_file%"
echo [PROGRESS] Pipeline Step: 2/2 (100.0%%) - Extracting: Processing reimbursement data from collected URLs >> "%log_file%"
echo [PROGRESS] Pipeline Step: 2/2 (100.0%%) - Extracting: Processing reimbursement data from collected URLs
echo [Step 2/2] Reimbursement Extraction... >> "%log_file%"
echo.
echo [Step 2/2] Reimbursement Extraction...
python -u "02_reimbursement_extraction.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Reimbursement Extraction failed >> "%log_file%"
    echo ERROR: Reimbursement Extraction failed
    exit /b 1
)

echo. >> "%log_file%"
echo [PROGRESS] Pipeline Step: 2/2 (100.0%%) - Pipeline completed successfully >> "%log_file%"
echo [PROGRESS] Pipeline Step: 2/2 (100.0%%) - Pipeline completed successfully
echo ================================================================================ >> "%log_file%"
echo Netherlands Pipeline - Completed successfully at %date% %time% >> "%log_file%"
echo ================================================================================ >> "%log_file%"
echo.
echo ================================================================================
echo Netherlands Pipeline - Completed successfully at %date% %time%
echo ================================================================================

echo. >> "%log_file%"
echo Log file saved to: %log_file% >> "%log_file%"
echo.
echo Log file saved to: %log_file%
