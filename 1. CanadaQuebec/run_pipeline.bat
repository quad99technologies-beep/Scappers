@echo off
REM ============================================================================
REM Enterprise PDF Processing Pipeline - Canada Quebec RAMQ Scraper
REM ============================================================================
REM Executes all extraction, normalization, and verification steps in sequence
REM
REM Pipeline Steps:
REM   step_00_* - Utility modules (encoding, database) - imported by other steps
REM   step_01   - Backup and clean output folder
REM   step_02   - Extract legend section from PDF
REM   step_03   - Validate PDF structure
REM   step_04   - Extract DIN data to CSV
REM   step_05   - Normalize CSV data
REM   step_06   - Verify encoding
REM   step_07   - Transform to standard format
REM ============================================================================

echo ========================================
echo Enterprise PDF Processing Pipeline
echo Canada Quebec RAMQ Scraper
echo ========================================
echo.

echo [Step 1/7] Backing up and cleaning output folder...
python Script\step_01_backup_and_clean.py
if errorlevel 1 (
    echo ERROR: Backup failed!
    pause
    exit /b 1
)
echo.

echo [Step 2/7] Extracting Annexe IV.1 section from PDF...
python Script\step_02_extract_legend_section.py
if errorlevel 1 (
    echo ERROR: Annexe IV.1 extraction failed!
    pause
    exit /b 1
)
echo.

echo [Step 3/7] Validating PDF structure...
python Script\step_03_validate_pdf_structure.py
if errorlevel 1 (
    echo ERROR: PDF validation failed!
    pause
    exit /b 1
)
echo.

echo [Step 4/7] Extracting DIN data to CSV...
python Script\step_04_extract_din_data.py
if errorlevel 1 (
    echo ERROR: DIN extraction failed!
    pause
    exit /b 1
)
echo.

echo [Step 5/7] Normalizing CSV data...
python Script\step_05_normalize_csv_data.py
if errorlevel 1 (
    echo ERROR: CSV normalization failed!
    pause
    exit /b 1
)
echo.

echo [Step 6/7] Verifying encoding...
python Script\step_06_verify_encoding.py
if errorlevel 1 (
    echo ERROR: Encoding verification failed!
    pause
    exit /b 1
)
echo.

echo [Step 7/7] Transforming to standard format...
python Script\step_07_transform_to_standard_format.py
if errorlevel 1 (
    echo ERROR: Format transformation failed!
    pause
    exit /b 1
)
echo.

echo ========================================
echo Pipeline completed successfully!
echo ========================================
echo.
echo Output files are in: output\csv\
echo Documentation is in: Script\doc\
echo.
pause

