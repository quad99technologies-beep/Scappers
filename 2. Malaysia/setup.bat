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

REM Upgrade pip first
echo Upgrading pip to latest version...
!PIP_CMD! install --upgrade pip
if errorlevel 1 (
    echo WARNING: Failed to upgrade pip, continuing anyway...
    echo.
)

echo Installing Python packages...
echo.

REM Install playwright
echo [1/5] Installing Playwright...
!PIP_CMD! install playwright
if errorlevel 1 (
    echo ERROR: Failed to install Playwright
    echo Please check your internet connection and try again.
    pause
    exit /b 1
)
echo Playwright installed successfully!
echo.

REM Install selenium and webdriver-manager
echo [2/5] Installing Selenium and dependencies...
!PIP_CMD! install selenium webdriver-manager
if errorlevel 1 (
    echo ERROR: Failed to install Selenium dependencies
    echo Please check your internet connection and try again.
    pause
    exit /b 1
)
echo Selenium dependencies installed successfully!
echo.

REM Install data processing dependencies
echo [3/5] Installing data processing dependencies...
!PIP_CMD! install pandas openpyxl
if errorlevel 1 (
    echo ERROR: Failed to install data processing dependencies
    echo Please check your internet connection and try again.
    pause
    exit /b 1
)
echo Data processing dependencies installed successfully!
echo.

REM Install web scraping dependencies
echo [4/5] Installing web scraping dependencies...
!PIP_CMD! install requests beautifulsoup4 lxml
if errorlevel 1 (
    echo ERROR: Failed to install web scraping dependencies
    echo Please check your internet connection and try again.
    pause
    exit /b 1
)
echo Web scraping dependencies installed successfully!
echo.

REM Install Playwright browsers
echo [5/5] Installing Playwright browsers (this may take a few minutes)...
echo This will download Chromium browser (~200MB)...
!PYTHON_CMD! -m playwright install chromium
if errorlevel 1 (
    echo ERROR: Failed to install Playwright browsers
    echo You can try running manually: python -m playwright install chromium
    pause
    exit /b 1
)
echo Playwright browsers installed successfully!
echo.

REM Verify installations
echo ========================================
echo Verifying Installations
echo ========================================
echo.

echo Checking installed packages...
!PIP_CMD! show playwright >nul 2>&1
if errorlevel 1 (
    echo WARNING: Playwright verification failed
) else (
    echo   [OK] Playwright
)

!PIP_CMD! show selenium >nul 2>&1
if errorlevel 1 (
    echo WARNING: Selenium verification failed
) else (
    echo   [OK] Selenium
)

!PIP_CMD! show pandas >nul 2>&1
if errorlevel 1 (
    echo WARNING: Pandas verification failed
) else (
    echo   [OK] Pandas
)

!PIP_CMD! show requests >nul 2>&1
if errorlevel 1 (
    echo WARNING: Requests verification failed
) else (
    echo   [OK] Requests
)

!PIP_CMD! show beautifulsoup4 >nul 2>&1
if errorlevel 1 (
    echo WARNING: BeautifulSoup4 verification failed
) else (
    echo   [OK] BeautifulSoup4
)

echo.
echo Checking Playwright browsers...
!PYTHON_CMD! -m playwright --version >nul 2>&1
if errorlevel 1 (
    echo WARNING: Playwright browser verification failed
) else (
    echo   [OK] Playwright browsers
)

echo.
echo ========================================
echo Setup Complete!
echo ========================================
echo.
echo All dependencies have been installed successfully.
echo.
echo Next steps:
echo   1. Ensure input/Malaysia_PCID.csv exists
echo   2. Run 'run_scripts.bat' to execute the scripts
echo.
echo For help, see docs/USER_MANUAL.md
echo.
endlocal
pause

