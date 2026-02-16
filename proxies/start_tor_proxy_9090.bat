@echo off
REM Start Tor Proxy (Port 9090)
REM Wrapper for Python script
python "%~dp0start_tor.py" --port 9090 --control-port 9091
if %ERRORLEVEL% NEQ 0 (
  echo [ERROR] Python script failed.
  pause
)
