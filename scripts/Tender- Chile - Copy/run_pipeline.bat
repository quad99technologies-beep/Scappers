@echo off
REM Tender Chile Pipeline Runner
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
set log_file=..\..\output\Tender_Chile\Tender_Chile_run_%timestamp%.log

REM Create output directory if it doesn't exist
if not exist "..\..\output\Tender_Chile" mkdir "..\..\output\Tender_Chile"

REM Initialize log file with header
(
echo ================================================================================
echo Tender Chile Pipeline - Starting at %date% %time%
echo ================================================================================
echo.
) > "%log_file%"

REM Step 0: Backup and Clean
echo [Step 0/4] Backup and Clean... >> "%log_file%"
echo [Step 0/4] Backup and Clean...
python -u "00_backup_and_clean.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Backup and Clean failed >> "%log_file%"
    echo ERROR: Backup and Clean failed
    exit /b 1
)

REM Step 1: Get Redirect URLs
echo [Step 1/4] Get Redirect URLs... >> "%log_file%"
echo [Step 1/4] Get Redirect URLs...
python -u "01_get_redirect_urls.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Get Redirect URLs failed >> "%log_file%"
    echo ERROR: Get Redirect URLs failed
    exit /b 1
)

REM Step 2: Extract Tender Details
echo [Step 2/4] Extract Tender Details... >> "%log_file%"
echo [Step 2/4] Extract Tender Details...
python -u "02_extract_tender_details.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Extract Tender Details failed >> "%log_file%"
    echo ERROR: Extract Tender Details failed
    exit /b 1
)

REM Step 3: Extract Tender Awards
echo [Step 3/4] Extract Tender Awards... >> "%log_file%"
echo [Step 3/4] Extract Tender Awards...
python -u "03_extract_tender_awards.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Extract Tender Awards failed >> "%log_file%"
    echo ERROR: Extract Tender Awards failed
    exit /b 1
)

REM Step 4: Merge Final CSV
echo [Step 4/4] Merge Final CSV... >> "%log_file%"
echo [Step 4/4] Merge Final CSV...
python -u "04_merge_final_csv.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Merge Final CSV failed >> "%log_file%"
    echo ERROR: Merge Final CSV failed
    exit /b 1
)

REM Clean up lock file after successful completion
python "cleanup_lock.py" 2>nul

echo. >> "%log_file%"
echo ================================================================================
echo Pipeline completed successfully!
echo ================================================================================
echo Log file saved to: %log_file% >> "%log_file%"
echo.
echo Log file saved to: %log_file%
