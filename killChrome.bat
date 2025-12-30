 @echo off
echo Killing Chrome and Google Drive processes...

:: Kill all Chrome processes
taskkill /F /IM chrome.exe /T

:: Kill Google Drive (new and legacy versions)
taskkill /F /IM GoogleDriveFS.exe /T
taskkill /F /IM googledrivesync.exe /T
taskkill /F /IM drive.exe /T
taskkill /F /IM python.exe
taskkill /F /IM pythonw.exe

echo Done. All Chrome and Drive processes have been terminated.
pause
