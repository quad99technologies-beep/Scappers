@echo off
REM Argentina Pipeline Runner
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
set log_file=..\..\output\Argentina\Argentina_run_%timestamp%.log

REM Create output directory if it doesn't exist
if not exist "..\..\output\Argentina" mkdir "..\..\output\Argentina"

REM Output to both console (for real-time GUI) and log file
REM Initialize log file with header
(
echo ================================================================================
echo Argentina Pipeline - Starting at %date% %time%
echo ================================================================================
echo.
) > "%log_file%"

echo ================================================================================
echo Argentina Pipeline - Starting at %date% %time%
echo ================================================================================
echo.

REM Step 0: Backup and Clean
echo [Step 0/6] Backup and Clean... >> "%log_file%"
echo [Step 0/6] Backup and Clean...
python -u "00_backup_and_clean.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Backup and Clean failed >> "%log_file%"
    echo ERROR: Backup and Clean failed
    exit /b 1
)

REM Step 1: Get Product List
echo. >> "%log_file%"
echo [Step 1/6] Get Product List... >> "%log_file%"
echo.
echo [Step 1/6] Get Product List...
python -u "01_getProdList.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Get Product List failed >> "%log_file%"
    echo ERROR: Get Product List failed
    exit /b 1
)

REM Step 2: Prepare URLs
echo. >> "%log_file%"
echo [Step 2/6] Prepare URLs and Determine Sources... >> "%log_file%"
echo.
echo [Step 2/6] Prepare URLs and Determine Sources...
python -u "02_prepare_urls.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Prepare URLs failed >> "%log_file%"
    echo ERROR: Prepare URLs failed
    exit /b 1
)

REM Step 3: Scrape Products
echo. >> "%log_file%"
echo [Step 3/6] Scrape Products... >> "%log_file%"
echo.
echo [Step 3/6] Scrape Products...
python -u "03_alfabeta_scraper_labs.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Scrape Products failed >> "%log_file%"
    echo ERROR: Scrape Products failed
    exit /b 1
)

REM Step 4: Translate Using Dictionary
echo. >> "%log_file%"
echo [Step 4/6] Translate Using Dictionary... >> "%log_file%"
echo.
echo [Step 4/6] Translate Using Dictionary...
python -u "04_TranslateUsingDictionary.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Translate Using Dictionary failed >> "%log_file%"
    echo ERROR: Translate Using Dictionary failed
    exit /b 1
)

REM Step 5: Generate Output
echo. >> "%log_file%"
echo [Step 5/6] Generate Output... >> "%log_file%"
echo.
echo [Step 5/6] Generate Output...
python -u "05_GenerateOutput.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Generate Output failed >> "%log_file%"
    echo ERROR: Generate Output failed
    exit /b 1
)

REM Step 6: PCID Missing
echo. >> "%log_file%"
echo [Step 6/6] PCID Missing... >> "%log_file%"
echo.
echo [Step 6/6] PCID Missing...
python -u "06_PCIDmissing.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo WARNING: PCID Missing step failed (continuing...) >> "%log_file%"
    echo WARNING: PCID Missing step failed (continuing...)
)

echo. >> "%log_file%"
echo ================================================================================ >> "%log_file%"
echo Argentina Pipeline - Completed successfully at %date% %time% >> "%log_file%"
echo ================================================================================ >> "%log_file%"
echo.
echo ================================================================================
echo Argentina Pipeline - Completed successfully at %date% %time%
echo ================================================================================

REM Clean up lock file after successful completion
python "cleanup_lock.py" 2>nul

echo. >> "%log_file%"
echo Log file saved to: %log_file% >> "%log_file%"
echo.
echo Log file saved to: %log_file%

