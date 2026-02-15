@echo off
REM Canada Quebec Pipeline Runner (DB-backed, resume support)
set PYTHONUNBUFFERED=1
cd /d "%~dp0"
python -u "run_pipeline_resume.py" %*
exit /b %errorlevel%
