@echo off
REM ============================================================================
REM Scraper Management GUI Launcher
REM ============================================================================

echo Starting Scraper Management GUI...
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python and try again.
    pause
    exit /b 1
)

REM Run the GUI
python scraper_gui.py

if errorlevel 1 (
    echo.
    echo ERROR: Failed to start GUI
    pause
    exit /b 1
)

