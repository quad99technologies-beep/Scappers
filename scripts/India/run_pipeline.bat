@echo off
REM India Pipeline Runner - Enterprise Edition
REM Wraps logic in scraper.py (which delegates to run_pipeline_scrapy.py)

set PYTHONUNBUFFERED=1
cd /d "%~dp0"

REM Setup logging
setlocal enabledelayedexpansion
for /f "usebackq tokens=*" %%a in (`powershell -NoProfile -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"` ) do set timestamp=%%a
set log_dir=..\..\output\India
set log_file=%log_dir%\India_run_%timestamp%.log

if not exist "%log_dir%" mkdir "%log_dir%"

echo ================================================================================
echo India Pipeline (Scrapy) - Starting at %date% %time%
echo ================================================================================
echo Log: %log_file%

python -u "scraper.py" %* 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Pipeline failed with exit code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)

echo.
echo ================================================================================
echo India Pipeline - Completed at %date% %time%
echo ================================================================================
pause
