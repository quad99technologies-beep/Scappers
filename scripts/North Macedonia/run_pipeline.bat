@echo off
REM North Macedonia Pipeline Runner
REM Runs all workflow steps in sequence with resume/checkpoint support
REM By default, resumes from last completed step
REM Use run_pipeline_resume.py --fresh to start fresh

REM Enable unbuffered output for real-time console updates
set PYTHONUNBUFFERED=1

cd /d "%~dp0"

REM Use resume script if available
if exist "run_pipeline_resume.py" (
    python -u "run_pipeline_resume.py" %*
    exit /b %errorlevel%
)
