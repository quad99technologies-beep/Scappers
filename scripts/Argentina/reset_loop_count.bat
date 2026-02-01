@echo off
setlocal
REM Wrapper to run reset_loop_count.py even if .py is associated with another app (e.g., Cursor).
python "%~dp0reset_loop_count.py" %*
