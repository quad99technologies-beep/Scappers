@echo off
REM CanadaQuebec Pipeline Runner
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
set log_file=..\..\output\CanadaQuebec\CanadaQuebec_run_%timestamp%.log

REM Create output directory if it doesn't exist
if not exist "..\..\output\CanadaQuebec" mkdir "..\..\output\CanadaQuebec"

REM Output to both console (for real-time GUI) and log file
REM Use a simple approach: redirect to log file AND display in real-time using PowerShell Tee-Object

REM Initialize log file with header
(
echo ================================================================================
echo CanadaQuebec Pipeline - Starting at %date% %time%
echo ================================================================================
echo.
) > "%log_file%"

REM Function to run command with tee to both console and log file
REM Note: We'll pipe Python commands through PowerShell Tee-Object for real-time output + logging

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

REM Step 1: Split PDF into Annexes
echo. >> "%log_file%"
echo [Step 1/6] Split PDF into Annexes... >> "%log_file%"
echo.
echo [Step 1/6] Split PDF into Annexes...
python -u "01_split_pdf_into_annexes.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Split PDF failed >> "%log_file%"
    echo ERROR: Split PDF failed
    exit /b 1
)

REM Step 2: Validate PDF Structure (optional)
echo. >> "%log_file%"
echo [Step 2/6] Validate PDF Structure... >> "%log_file%"
echo.
echo [Step 2/6] Validate PDF Structure...
python -u "02_validate_pdf_structure.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo WARNING: PDF validation failed (continuing...) >> "%log_file%"
    echo WARNING: PDF validation failed (continuing...)
)

REM Step 3: Extract Annexe IV.1
echo. >> "%log_file%"
echo [Step 3/6] Extract Annexe IV.1... >> "%log_file%"
echo.
echo [Step 3/6] Extract Annexe IV.1...
python -u "03_extract_annexe_iv1.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Extract Annexe IV.1 failed >> "%log_file%"
    echo ERROR: Extract Annexe IV.1 failed
    exit /b 1
)

REM Step 4: Extract Annexe IV.2
echo. >> "%log_file%"
echo [Step 4/6] Extract Annexe IV.2... >> "%log_file%"
echo.
echo [Step 4/6] Extract Annexe IV.2...
python -u "04_extract_annexe_iv2.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Extract Annexe IV.2 failed >> "%log_file%"
    echo ERROR: Extract Annexe IV.2 failed
    exit /b 1
)

REM Step 5: Extract Annexe V
echo. >> "%log_file%"
echo [Step 5/6] Extract Annexe V... >> "%log_file%"
echo.
echo [Step 5/6] Extract Annexe V...
python -u "05_extract_annexe_v.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Extract Annexe V failed >> "%log_file%"
    echo ERROR: Extract Annexe V failed
    exit /b 1
)

REM Step 6: Merge All Annexes
echo. >> "%log_file%"
echo [Step 6/6] Merge All Annexes... >> "%log_file%"
echo.
echo [Step 6/6] Merge All Annexes...
python -u "06_merge_all_annexes.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Merge All Annexes failed >> "%log_file%"
    echo ERROR: Merge All Annexes failed
    exit /b 1
)

echo. >> "%log_file%"
echo ================================================================================ >> "%log_file%"
echo CanadaQuebec Pipeline - Completed successfully at %date% %time% >> "%log_file%"
echo ================================================================================ >> "%log_file%"
echo.
echo ================================================================================
echo CanadaQuebec Pipeline - Completed successfully at %date% %time%
echo ================================================================================

REM Clean up lock file after successful completion
python "cleanup_lock.py" 2>nul

echo. >> "%log_file%"
echo Log file saved to: %log_file% >> "%log_file%"
echo.
echo Log file saved to: %log_file%
