@echo off
REM Malaysia Pipeline Runner (DB-backed, resume support)
REM Runs run_pipeline_resume.py - all data in PostgreSQL; use --fresh to start from step 0

set PYTHONUNBUFFERED=1
cd /d "%~dp0"

python -u "run_pipeline_resume.py" %*
exit /b %errorlevel%
