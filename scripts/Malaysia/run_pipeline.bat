@echo off
REM Malaysia Pipeline Runner
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
set log_file=..\..\output\Malaysia\Malaysia_run_%timestamp%.log

REM Create output directory if it doesn't exist
if not exist "..\..\output\Malaysia" mkdir "..\..\output\Malaysia"

REM Output to both console (for real-time GUI) and log file
REM Use a simple approach: redirect to log file AND display in real-time using PowerShell Tee-Object

REM Initialize log file with header
(
echo ================================================================================
echo Malaysia Pipeline - Starting at %date% %time%
echo ================================================================================
echo.
) > "%log_file%"

REM Step 0: Backup and Clean
echo [Step 0/5] Backup and Clean... >> "%log_file%"
echo [Step 0/5] Backup and Clean...
python -u "00_backup_and_clean.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Backup and Clean failed >> "%log_file%"
    echo ERROR: Backup and Clean failed
    exit /b 1
)

REM Step 1: Product Registration Number
echo. >> "%log_file%"
echo [Step 1/5] Product Registration Number... >> "%log_file%"
echo.
echo [Step 1/5] Product Registration Number...
python -u "01_Product_Registration_Number.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Product Registration Number failed >> "%log_file%"
    echo ERROR: Product Registration Number failed
    exit /b 1
)

REM Step 2: Product Details
echo. >> "%log_file%"
echo [Step 2/5] Product Details... >> "%log_file%"
echo.
echo [Step 2/5] Product Details...
python -u "02_Product_Details.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Product Details failed >> "%log_file%"
    echo ERROR: Product Details failed
    exit /b 1
)

REM Step 3: Consolidate Results
echo. >> "%log_file%"
echo [Step 3/5] Consolidate Results... >> "%log_file%"
echo.
echo [Step 3/5] Consolidate Results...
python -u "03_Consolidate_Results.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Consolidate Results failed >> "%log_file%"
    echo ERROR: Consolidate Results failed
    exit /b 1
)

REM Step 4: Get Fully Reimbursable
echo. >> "%log_file%"
echo [Step 4/5] Get Fully Reimbursable... >> "%log_file%"
echo.
echo [Step 4/5] Get Fully Reimbursable...
python -u "04_Get_Fully_Reimbursable.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Get Fully Reimbursable failed >> "%log_file%"
    echo ERROR: Get Fully Reimbursable failed
    exit /b 1
)

REM Step 5: Generate PCID Mapped
echo. >> "%log_file%"
echo [Step 5/5] Generate PCID Mapped... >> "%log_file%"
echo.
echo [Step 5/5] Generate PCID Mapped...
python -u "05_Generate_PCID_Mapped.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Generate PCID Mapped failed >> "%log_file%"
    echo ERROR: Generate PCID Mapped failed
    exit /b 1
)

echo. >> "%log_file%"
echo ================================================================================ >> "%log_file%"
echo Malaysia Pipeline - Completed successfully at %date% %time% >> "%log_file%"
echo ================================================================================ >> "%log_file%"
echo.
echo ================================================================================
echo Malaysia Pipeline - Completed successfully at %date% %time%
echo ================================================================================

REM Clean up lock file after successful completion
python "cleanup_lock.py" 2>nul

echo. >> "%log_file%"
echo Log file saved to: %log_file% >> "%log_file%"
echo.
echo Log file saved to: %log_file%

