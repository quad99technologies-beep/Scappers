@echo off
setlocal enabledelayedexpansion
REM Setup batch file to install required dependencies

echo ========================================
echo Setup Script - Installing Dependencies
echo ========================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python and try again.
    pause
    exit /b 1
)

echo Python found: 
python --version
echo.

REM Determine which Python command to use for pip
set "PYTHON_CMD=python"
set "PIP_CMD=python -m pip"

REM Try python -m pip first
python -m pip --version >nul 2>&1
if errorlevel 1 (
    REM Try py launcher as fallback
    py -m pip --version >nul 2>&1
    if errorlevel 1 (
        REM Try direct pip command
        pip --version >nul 2>&1
        if errorlevel 1 (
            echo ERROR: pip is not available
            echo.
            echo Trying to install pip...
            python -m ensurepip --upgrade
            if errorlevel 1 (
                echo ERROR: Could not install pip automatically.
                echo Please install pip manually or reinstall Python with pip included.
                pause
                exit /b 1
            )
        ) else (
            set "PIP_CMD=pip"
        )
    ) else (
        set "PYTHON_CMD=py"
        set "PIP_CMD=py -m pip"
    )
)

echo Using: !PIP_CMD!
!PIP_CMD! --version
echo.

echo Installing Python packages...
echo.

REM Install playwright
echo [1/3] Installing Playwright...
!PIP_CMD! install playwright
if errorlevel 1 (
    echo ERROR: Failed to install Playwright
    pause
    exit /b 1
)
echo Playwright installed successfully!
echo.

REM Install selenium and webdriver-manager
echo [2/4] Installing Selenium and dependencies...
!PIP_CMD! install selenium webdriver-manager pandas openpyxl
if errorlevel 1 (
    echo ERROR: Failed to install Selenium dependencies
    pause
    exit /b 1
)
echo Selenium dependencies installed successfully!
echo.

REM Install web scraping dependencies
echo [3/4] Installing web scraping dependencies...
!PIP_CMD! install requests beautifulsoup4 lxml
if errorlevel 1 (
    echo ERROR: Failed to install web scraping dependencies
    pause
    exit /b 1
)
echo Web scraping dependencies installed successfully!
echo.

REM Install Playwright browsers
echo [4/4] Installing Playwright browsers (this may take a few minutes)...
!PYTHON_CMD! -m playwright install chromium
if errorlevel 1 (
    echo ERROR: Failed to install Playwright browsers
    echo Please run 'playwright install' manually.
    pause
    exit /b 1
)
echo Playwright browsers installed successfully!
echo.

echo ========================================
echo Setup Complete!
echo ========================================
echo.
echo All dependencies have been installed.
echo You can now run 'run_scripts.bat' to execute the scripts.
echo.
endlocal
pause

