@echo off
REM North Macedonia Pipeline Runner - Enterprise Edition
REM Wraps logic in scraper.py

set PYTHONUNBUFFERED=1
cd /d "%~dp0"

REM Setup logging
setlocal enabledelayedexpansion
for /f "usebackq tokens=*" %%a in (`powershell -NoProfile -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"` ) do set timestamp=%%a
set log_dir=..\..\output\NorthMacedonia
set log_file=%log_dir%\NorthMacedonia_run_%timestamp%.log

if not exist "%log_dir%" mkdir "%log_dir%"

echo ================================================================================
echo North Macedonia Pipeline - Starting at %date% %time%
echo ================================================================================
echo Log: %log_file%

python -u "scraper.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Pipeline failed with exit code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)

echo.
echo ================================================================================
echo North Macedonia Pipeline - Completed at %date% %time%
echo ================================================================================
pause
