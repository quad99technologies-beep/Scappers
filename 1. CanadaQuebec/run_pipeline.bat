@echo off
REM ============================================================================
REM Canada Quebec RAMQ - Complete Annexe Extraction Pipeline
REM ============================================================================
REM This script:
REM   step_00 - Backs up output folder before each run (in doc/)
REM   step_01 - Splits PDF into annexes (IV.1, IV.2, V)
REM   step_02 - Validates PDF structure (optional)
REM   step_03 - Extracts Annexe IV.1 data
REM   step_04 - Extracts Annexe IV.2 data
REM   step_05 - Extracts Annexe V data
REM   step_06 - Merges all annexe outputs into final CSV
REM ============================================================================

setlocal enabledelayedexpansion

echo ========================================
echo Canada Quebec RAMQ - Complete Pipeline
echo ========================================
echo.

REM Change to script directory
cd /d "%~dp0"

REM ============================================================================
REM Step 00: Backup Output Folder
REM ============================================================================
echo [Step 00/06] Backing up output folder...
python doc\step_01_backup_and_clean.py
if errorlevel 1 (
    echo ERROR: Backup failed!
    pause
    exit /b 1
)
echo.

REM ============================================================================
REM Step 01: Split PDF into Annexes
REM ============================================================================
echo [Step 01/06] Splitting PDF into annexes (IV.1, IV.2, V)...
python Script\step_01_split_pdf_into_annexes.py
if errorlevel 1 (
    echo ERROR: PDF splitting failed!
    pause
    exit /b 1
)
echo.

REM ============================================================================
REM Step 02: Validate PDF Structure (Optional - can be skipped)
REM ============================================================================
echo [Step 02/06] Validating PDF structure...
python Script\step_02_validate_pdf_structure.py
if errorlevel 1 (
    echo WARNING: PDF validation failed, but continuing...
)
echo.

REM ============================================================================
REM Step 03: Extract Annexe IV.1
REM ============================================================================
echo [Step 03/06] Extracting Annexe IV.1...
python Script\step_03_extract_annexe_iv1.py
if errorlevel 1 (
    echo ERROR: Annexe IV.1 extraction failed!
    pause
    exit /b 1
)
echo.

REM ============================================================================
REM Step 04: Extract Annexe IV.2
REM ============================================================================
echo [Step 04/06] Extracting Annexe IV.2...
python Script\step_04_extract_annexe_iv2.py
if errorlevel 1 (
    echo ERROR: Annexe IV.2 extraction failed!
    pause
    exit /b 1
)
echo.

REM ============================================================================
REM Step 05: Extract Annexe V
REM ============================================================================
echo [Step 05/06] Extracting Annexe V...
python Script\step_05_extract_annexe_v.py
if errorlevel 1 (
    echo ERROR: Annexe V extraction failed!
    pause
    exit /b 1
)
echo.

REM ============================================================================
REM Step 06: Merge All Annexes
REM ============================================================================
echo [Step 06/06] Merging all annexe outputs...
python Script\step_06_merge_all_annexes.py
if errorlevel 1 (
    echo ERROR: Merge failed!
    pause
    exit /b 1
)
echo.

echo ========================================
echo Pipeline completed successfully!
echo ========================================
echo.
echo Output files:
echo   - Annexe IV.1: output\csv\annexe_iv1_extracted.csv
echo   - Annexe IV.2: output\csv\annexe_iv2_extracted.csv
echo   - Annexe V:    output\csv\annexe_v_extracted.csv
echo   - Final Report: output\csv\canadaquebecreport_ddmmyyyy.csv
echo     (Filename includes today's date in ddmmyyyy format)
echo.
echo Backups saved in: backups\
echo.
pause

