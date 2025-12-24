@echo off
REM ============================================================================
REM Canada Quebec RAMQ Scraper - Setup Script
REM ============================================================================
REM This script sets up the environment for the Canada Quebec RAMQ scraper
REM It creates necessary directories and installs Python dependencies
REM ============================================================================

setlocal enabledelayedexpansion

echo ========================================
echo Canada Quebec RAMQ Scraper - Setup
echo ========================================
echo.

REM Change to script directory
cd /d "%~dp0"

REM ============================================================================
REM Check Python Installation
REM ============================================================================
echo [1/4] Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH!
    echo Please install Python 3.8 or higher from https://www.python.org/
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo [OK] Python !PYTHON_VERSION! found
echo.

REM ============================================================================
REM Create Required Directories
REM ============================================================================
echo [2/4] Creating required directories...

if not exist "input" (
    mkdir "input"
    echo [OK] Created directory: input
) else (
    echo [SKIP] Directory already exists: input
)

if not exist "output\csv" (
    mkdir "output\csv"
    echo [OK] Created directory: output\csv
) else (
    echo [SKIP] Directory already exists: output\csv
)

if not exist "output\split_pdf" (
    mkdir "output\split_pdf"
    echo [OK] Created directory: output\split_pdf
) else (
    echo [SKIP] Directory already exists: output\split_pdf
)

if not exist "backups" (
    mkdir "backups"
    echo [OK] Created directory: backups
) else (
    echo [SKIP] Directory already exists: backups
)

echo.

REM ============================================================================
REM Install Python Dependencies
REM ============================================================================
echo [3/4] Installing Python dependencies...
echo.

REM Check if requirements.txt exists in doc folder
if exist "doc\requirements.txt" (
    echo Installing from doc\requirements.txt...
    python -m pip install --upgrade pip
    if errorlevel 1 (
        echo WARNING: Failed to upgrade pip, continuing anyway...
    )
    
    python -m pip install -r "doc\requirements.txt"
    if errorlevel 1 (
        echo ERROR: Failed to install dependencies from requirements.txt!
        pause
        exit /b 1
    )
    echo [OK] Core dependencies installed
    
    REM Install additional dependencies used by annexe extraction scripts
    echo Installing additional dependencies (pandas, openai)...
    python -m pip install pandas openai
    if errorlevel 1 (
        echo WARNING: Failed to install pandas/openai, but continuing...
        echo NOTE: Some scripts may require these packages
    ) else (
        echo [OK] Additional dependencies installed
    )
) else (
    echo WARNING: doc\requirements.txt not found!
    echo Installing all required dependencies...
    
    python -m pip install --upgrade pip
    python -m pip install PyPDF2 pdfplumber pandas openai
    if errorlevel 1 (
        echo ERROR: Failed to install dependencies!
        pause
        exit /b 1
    )
    echo [OK] All dependencies installed
)

echo.

REM ============================================================================
REM Verify Installation
REM ============================================================================
echo [4/4] Verifying installation...
echo.

python -c "import PyPDF2; print('[OK] PyPDF2 installed')" 2>nul || echo [FAIL] PyPDF2 not installed
python -c "import pdfplumber; print('[OK] pdfplumber installed')" 2>nul || echo [FAIL] pdfplumber not installed
python -c "import pandas; print('[OK] pandas installed')" 2>nul || echo [WARN] pandas not installed
python -c "import openai; print('[OK] openai installed')" 2>nul || echo [WARN] openai not installed

echo.

REM ============================================================================
REM Setup Complete
REM ============================================================================
echo ========================================
echo Setup completed successfully!
echo ========================================
echo.
echo Next steps:
echo   1. Place your PDF file in the 'input' folder
echo      (Default expected name: liste-med.pdf)
echo   2. Run 'run_annexe_extraction.bat' to start extraction
echo.
echo Directory structure:
echo   - input\              : Place your PDF files here
echo   - output\csv\         : Extracted CSV files will be saved here
echo   - output\split_pdf\   : Split PDF files will be saved here
echo   - backups\            : Automatic backups will be saved here
echo.
echo Important files:
echo   - run_annexe_extraction.bat : Main execution script
echo   - Script\                  : Contains all extraction scripts
echo   - doc\                     : Documentation and utilities
echo.
pause

