@echo off
setlocal
REM Wrapper to run requeue_missing_products.py even if .py is associated with another app (e.g., Cursor).
python "%~dp0requeue_missing_products.py" %*
