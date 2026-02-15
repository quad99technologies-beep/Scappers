@echo off
setlocal

REM ================================================================
REM Start Tor Proxy — shared by all scrapers (Argentina, Netherlands, Chile)
REM ================================================================
REM Starts a standalone Tor daemon for anonymous scraping.
REM Prereq: Tor Browser installed (we reuse its tor.exe).
REM
REM Endpoints:
REM   SOCKS5:   127.0.0.1:9050  (for httpx proxy / Firefox proxy)
REM   Control:  127.0.0.1:9051  (for NEWNYM identity rotation)
REM
REM Cookie auth file: C:\TorProxy\data\control_auth_cookie
REM ================================================================

set "TOR_EXE=%USERPROFILE%\OneDrive\Desktop\Tor Browser\Browser\TorBrowser\Tor\tor.exe"
if not exist "%TOR_EXE%" (
  set "TOR_EXE=%USERPROFILE%\Desktop\Tor Browser\Browser\TorBrowser\Tor\tor.exe"
)
if not exist "%TOR_EXE%" (
  echo [ERROR] tor.exe not found. Update TOR_EXE in this .bat to your tor.exe path.
  exit /b 1
)

if not exist "C:\TorProxy" mkdir "C:\TorProxy" >nul 2>&1
if not exist "C:\TorProxy\data" mkdir "C:\TorProxy\data" >nul 2>&1

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
echo [INFO] Keep this window open while scraping.
echo [INFO] Wait for "Bootstrapped 100%%" before starting any pipeline.
echo.

"%TOR_EXE%" -f "C:\TorProxy\torrc"

endlocal
