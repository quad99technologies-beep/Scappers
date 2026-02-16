@echo off
REM Start Tor Proxy (Port 9080)
REM Wrapper for Python script
python "%~dp0start_tor.py" --port 9080 --control-port 9081
if %ERRORLEVEL% NEQ 0 (
  echo [ERROR] Python script failed.
  pause
)
