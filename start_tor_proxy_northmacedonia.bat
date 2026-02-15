@echo off
setlocal

REM ================================================================
REM Start Tor Proxy for North Macedonia Scraper
REM ================================================================
REM Starts a standalone Tor daemon for North Macedonia scraper.
REM Uses different ports from the main Tor proxy (9050/9051) to avoid conflicts.
REM
REM Endpoints:
REM   SOCKS5:   127.0.0.1:9060  (for httpx proxy)
REM   Control:  127.0.0.1:9061  (for NEWNYM identity rotation)
REM
REM Cookie auth file: C:\TorProxyNM\data\control_auth_cookie
REM ================================================================

set "TOR_EXE=%USERPROFILE%\OneDrive\Desktop\Tor Browser\Browser\TorBrowser\Tor\tor.exe"
if not exist "%TOR_EXE%" (
  set "TOR_EXE=%USERPROFILE%\Desktop\Tor Browser\Browser\TorBrowser\Tor\tor.exe"
)
if not exist "%TOR_EXE%" (
  echo [ERROR] tor.exe not found. Update TOR_EXE in this .bat to your tor.exe path.
  exit /b 1
)

if not exist "C:\TorProxyNM" mkdir "C:\TorProxyNM" >nul 2>&1
if not exist "C:\TorProxyNM\data" mkdir "C:\TorProxyNM\data" >nul 2>&1

> "C:\TorProxyNM\torrc" (
  echo DataDirectory C:\TorProxyNM\data
  echo SocksPort 9060
  echo ControlPort 9061
  echo CookieAuthentication 1
)

echo [INFO] Starting Tor proxy for North Macedonia...
echo [INFO]   tor.exe:  %TOR_EXE%
echo [INFO]   torrc:    C:\TorProxyNM\torrc
echo [INFO]   socks:    127.0.0.1:9060
echo [INFO]   control:  127.0.0.1:9061
echo.
echo [INFO] Keep this window open while scraping.
echo [INFO] Wait for "Bootstrapped 100%%" before starting any pipeline.
echo.

"%TOR_EXE%" -f "C:\TorProxyNM\torrc"

endlocal
