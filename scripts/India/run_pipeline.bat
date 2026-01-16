@echo off
REM India NPPA Pharma Sahi Daam Scraper Pipeline
REM Runs all steps in sequence with checkpoint support

echo ============================================================
echo India NPPA Scraper Pipeline
echo ============================================================

cd /d "%~dp0"
python run_pipeline_resume.py %*

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Pipeline failed with error code %ERRORLEVEL%
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo Pipeline completed successfully!
pause
