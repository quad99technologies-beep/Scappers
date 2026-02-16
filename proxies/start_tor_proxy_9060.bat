@echo off
REM Start Tor Proxy (Port 9060)
REM Wrapper for Python script
python "%~dp0start_tor.py" --port 9060 --control-port 9061
if %ERRORLEVEL% NEQ 0 (
  echo [ERROR] Python script failed.
  pause
)
