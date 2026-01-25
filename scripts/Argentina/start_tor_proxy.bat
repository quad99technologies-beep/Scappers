@echo off
setlocal

REM Start a standalone Tor daemon for scraping (stable NEWNYM + no Tor Browser UI).
REM Prereq: Tor Browser installed (we reuse its tor.exe).
REM Config file created at: C:\TorProxy\torrc
REM Data directory:         C:\TorProxy\data
REM SocksPort:              9050
REM ControlPort:            9051

set "TOR_EXE=%USERPROFILE%\OneDrive\Desktop\Tor Browser\Browser\TorBrowser\Tor\tor.exe"
if not exist "%TOR_EXE%" (
  REM Fallback: try a common non-OneDrive desktop path
  set "TOR_EXE=%USERPROFILE%\Desktop\Tor Browser\Browser\TorBrowser\Tor\tor.exe"
)
if not exist "%TOR_EXE%" (
  echo [ERROR] tor.exe not found. Update TOR_EXE in this .bat to your tor.exe path.
  exit /b 1
)

if not exist "C:\TorProxy" mkdir "C:\TorProxy" >nul 2>&1
if not exist "C:\TorProxy\data" mkdir "C:\TorProxy\data" >nul 2>&1

REM Write a minimal torrc (overwrite).
> "C:\TorProxy\torrc" (
  echo DataDirectory C:\TorProxy\data
  echo SocksPort 9050
  echo ControlPort 9051
  echo CookieAuthentication 1
)

echo [INFO] Starting Tor proxy...
echo [INFO]   tor.exe:  %TOR_EXE%
echo [INFO]   torrc:    C:\TorProxy\torrc
echo [INFO]   socks:    127.0.0.1:9050
echo [INFO]   control:  127.0.0.1:9051
echo.
echo [INFO] Keep this window open while scraping. Wait for "Bootstrapped 100%%" before starting the pipeline.
echo.

"%TOR_EXE%" -f "C:\TorProxy\torrc"

endlocal
