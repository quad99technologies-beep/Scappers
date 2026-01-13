@echo off
REM Canada Ontario Pipeline Runner
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
set log_file=..\..\output\CanadaOntario\CanadaOntario_run_%timestamp%.log

REM Create output directory if it doesn't exist
if not exist "..\..\output\CanadaOntario" mkdir "..\..\output\CanadaOntario"

REM Output to both console (for real-time GUI) and log file
REM Initialize log file with header
(
echo ================================================================================
echo Canada Ontario Pipeline - Starting at %date% %time%
echo ================================================================================
echo.
) > "%log_file%"

echo ================================================================================
echo Canada Ontario Pipeline - Starting at %date% %time%
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

REM Step 1: Extract Product Details
echo. >> "%log_file%"
echo [PROGRESS] Pipeline Step: 1/2 (50.0%%) - Scraping: Extracting product details from Ontario Formulary >> "%log_file%"
echo [PROGRESS] Pipeline Step: 1/2 (50.0%%) - Scraping: Extracting product details from Ontario Formulary
echo [Step 1/2] Extract Product Details... >> "%log_file%"
echo.
echo [Step 1/2] Extract Product Details...
python -u "01_extract_product_details.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Extract Product Details failed >> "%log_file%"
    echo ERROR: Extract Product Details failed
    exit /b 1
)

echo. >> "%log_file%"
echo [PROGRESS] Pipeline Step: 2/2 (100.0%%) - Pipeline completed successfully >> "%log_file%"
echo [PROGRESS] Pipeline Step: 2/2 (100.0%%) - Pipeline completed successfully
echo ================================================================================ >> "%log_file%"
echo Canada Ontario Pipeline - Completed successfully at %date% %time% >> "%log_file%"
echo ================================================================================ >> "%log_file%"
echo.
echo ================================================================================
echo Canada Ontario Pipeline - Completed successfully at %date% %time%
echo ================================================================================

echo. >> "%log_file%"
echo Log file saved to: %log_file% >> "%log_file%"
echo.
echo Log file saved to: %log_file%
