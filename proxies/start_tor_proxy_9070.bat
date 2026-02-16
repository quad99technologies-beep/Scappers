@echo off
REM Start Tor Proxy (Port 9070)
REM Wrapper for Python script
python "%~dp0start_tor.py" --port 9070 --control-port 9071
if %ERRORLEVEL% NEQ 0 (
  echo [ERROR] Python script failed.
  pause
)
