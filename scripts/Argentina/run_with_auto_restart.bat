@echo off
REM Argentina Scraper with Auto-Restart
REM This wrapper automatically restarts the scraper if it hangs or crashes

echo ==========================================
echo Argentina Scraper - Auto-Restart Mode
echo ==========================================
echo.
echo This will restart the scraper if:
echo   - No progress for 10 minutes
echo   - Memory exceeds 2GB
echo   - Process crashes
echo.
echo Press Ctrl+C to stop
echo.

python auto_restart_wrapper.py --max-runtime-hours 8 --no-progress-timeout 600 --memory-limit-mb 2048

echo.
echo Scraper stopped. Press any key to exit...
pause > nul
