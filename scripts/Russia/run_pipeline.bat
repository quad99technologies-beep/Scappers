@echo off
REM Russia Scraper Pipeline Runner
REM This script runs the Russia VED pricing scraper pipeline

echo ============================================================
echo Russia VED Pricing Scraper Pipeline
echo ============================================================
echo.

REM Change to script directory
cd /d "%~dp0"

REM Run the pipeline with resume support
python run_pipeline_resume.py %*

echo.
echo ============================================================
echo Pipeline execution completed
echo ============================================================

pause
