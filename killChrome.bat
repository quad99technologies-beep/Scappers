@echo off
echo Killing browser and sync processes...

:: Kill Google Chrome
taskkill /F /IM chrome.exe /T

:: Kill Mozilla Firefox
taskkill /F /IM firefox.exe /T

:: Kill tor
taskkill /F /IM tor.exe /T

:: Kill Tor Browser (Firefox-based)
taskkill /F /IM tor.exe /T
taskkill /F /IM firefox.exe /T

:: Kill Google Drive (new and legacy)
taskkill /F /IM GoogleDriveFS.exe /T
taskkill /F /IM googledrivesync.exe /T
taskkill /F /IM drive.exe /T

echo Done. Chrome, Firefox, Tor, and Google Drive processes terminated.
pause
