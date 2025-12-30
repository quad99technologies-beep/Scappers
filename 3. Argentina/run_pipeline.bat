@echo off
REM ============================================================================
REM Argentina AlfaBeta Scraper - Complete Pipeline
REM ============================================================================
REM This script runs all steps of the Argentina scraper pipeline.
REM Updated to use platform paths (Documents/ScraperPlatform/)
REM
REM Features:
REM - State tracking and resume capability
REM - Automatic backups after each step
REM - Human-readable logging
REM - Max row extraction limits
REM
REM Steps:
REM   00 - Backup and Clean
REM   01 - Get Product List
REM   02 - Scrape Products (supports --max-rows)
REM   03 - Translate Using Dictionary
REM   04 - Generate Output (FINAL REPORT)
REM   05 - PCID Missing
REM ============================================================================

setlocal enabledelayedexpansion

REM Change to script directory (ensure CWD is scraper root)
cd /d "%~dp0"

REM Platform paths (scripts now write here automatically via config_loader)
set "PLATFORM_ROOT=%USERPROFILE%\Documents\ScraperPlatform"
set "SCRIPT_DIR=%~dp0"
echo Platform Root: %PLATFORM_ROOT%
echo.

REM Configuration
set STATE_FILE=%SCRIPT_DIR%pipeline_state.txt
set LOG_DIR=%PLATFORM_ROOT%\logs
set BACKUP_DIR=%PLATFORM_ROOT%\output\backups
set MAX_ROWS=0
set LOOP_MODE=0

REM Load MAX_ROWS from .env file if it exists
if exist "%SCRIPT_DIR%.env" (
    for /f "usebackq tokens=2 delims==" %%I in (`findstr /i "^MAX_ROWS=" "%SCRIPT_DIR%.env"`) do set MAX_ROWS=%%I
)

REM Create directories
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"

REM Parse command line arguments
:parse_args
if "%~1"=="" goto args_done
if /i "%~1"=="--max-rows" (
    set MAX_ROWS=%~2
    shift
    shift
    goto parse_args
)
if /i "%~1"=="--loop" (
    set LOOP_MODE=1
    shift
    goto parse_args
)
if /i "%~1"=="--help" (
    echo Usage: run_pipeline.bat [--max-rows N] [--loop]
    echo   --max-rows N  : Maximum rows to extract in step 2 (0 = unlimited)
    echo   --loop        : Run pipeline in continuous loop
    exit /b 0
)
shift
goto parse_args

:args_done

REM Get timestamp for this run (using PowerShell for modern Windows compatibility)
for /f "usebackq tokens=*" %%I in (`powershell -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"`) do set TIMESTAMP=%%I
set LOG_FILE=%LOG_DIR%\pipeline_%TIMESTAMP%.log

echo ============================================================================ >> "%LOG_FILE%"
echo Pipeline Execution Started: %date% %time% >> "%LOG_FILE%"
echo Configuration: MAX_ROWS=%MAX_ROWS%, LOOP_MODE=%LOOP_MODE% >> "%LOG_FILE%"
echo ============================================================================ >> "%LOG_FILE%"
echo.

:main_loop
    REM Read last completed step from state file
    set LAST_STEP=0
    if exist "%STATE_FILE%" (
        for /f %%i in (%STATE_FILE%) do set LAST_STEP=%%i
    )
    
    REM Step 00: Backup and Clean (MANDATORY FIRST STEP - only on fresh start)
    if %LAST_STEP% EQU 0 (
        echo. >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 00: Backup and Clean Output Folder >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 00: Backup and Clean Output Folder...
        python "%SCRIPT_DIR%script\00_backup_and_clean.py" >> "%LOG_FILE%" 2>&1
        if errorlevel 1 (
            echo [%date% %time%] ERROR: Step 00 (Backup) failed! >> "%LOG_FILE%"
            echo [%date% %time%] ERROR: Step 00 (Backup) failed!
            goto error_handler
        )
        echo [%date% %time%] STEP 00: Completed successfully >> "%LOG_FILE%"
        echo [%date% %time%] STEP 00: Completed successfully
    )

    echo [%date% %time%] Starting pipeline from step %LAST_STEP% >> "%LOG_FILE%"
    echo [%date% %time%] Starting pipeline from step %LAST_STEP%

    REM Step 1: Get Product List
    if %LAST_STEP% LSS 1 (
        echo. >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 01: Getting Product List >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 01: Getting Product List...
        python "%SCRIPT_DIR%script\01_getProdList.py" >> "%LOG_FILE%" 2>&1
        if errorlevel 1 (
            echo [%date% %time%] ERROR: Step 1 failed! >> "%LOG_FILE%"
            echo [%date% %time%] ERROR: Step 1 failed!
            goto error_handler
        )
        echo [%date% %time%] STEP 01: Completed successfully >> "%LOG_FILE%"
        echo [%date% %time%] STEP 01: Completed successfully
        call :backup_step 1
        timeout /t 1 /nobreak >nul
        echo 1 > "%STATE_FILE%"
    )

    REM Step 2: Scrape Products (with max-rows limit)
    if %LAST_STEP% LSS 2 (
        echo. >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 02: Scraping Products (Max Rows: %MAX_ROWS%) >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 02: Scraping Products (Max Rows: %MAX_ROWS%)...
        if %MAX_ROWS% EQU 0 (
            python "%SCRIPT_DIR%script\02_alfabeta_scraper_labs.py" --headless >> "%LOG_FILE%" 2>&1
        ) else (
            python "%SCRIPT_DIR%script\02_alfabeta_scraper_labs.py" --headless --max-rows %MAX_ROWS% >> "%LOG_FILE%" 2>&1
        )
        if errorlevel 1 (
            echo [%date% %time%] ERROR: Step 2 failed! >> "%LOG_FILE%"
            echo [%date% %time%] ERROR: Step 2 failed!
            goto error_handler
        )
        echo [%date% %time%] STEP 02: Completed successfully >> "%LOG_FILE%"
        echo [%date% %time%] STEP 02: Completed successfully
        call :backup_step 2
        timeout /t 1 /nobreak >nul
        echo 2 > "%STATE_FILE%"
    )

    REM Step 3: Translate Using Dictionary
    if %LAST_STEP% LSS 3 (
        echo. >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 03: Translating Using Dictionary >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 03: Translating Using Dictionary...
        python "%SCRIPT_DIR%script\03_TranslateUsingDictionary.py" >> "%LOG_FILE%" 2>&1
        if errorlevel 1 (
            echo [%date% %time%] ERROR: Step 3 failed! >> "%LOG_FILE%"
            echo [%date% %time%] ERROR: Step 3 failed!
            goto error_handler
        )
        echo [%date% %time%] STEP 03: Completed successfully >> "%LOG_FILE%"
        echo [%date% %time%] STEP 03: Completed successfully
        call :backup_step 3
        timeout /t 1 /nobreak >nul
        echo 3 > "%STATE_FILE%"
    )

    REM Step 4: Generate Output (FINAL REPORT)
    if %LAST_STEP% LSS 4 (
        echo. >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 04: Generating Output (FINAL REPORT) >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 04: Generating Output (FINAL REPORT)...
        python "%SCRIPT_DIR%script\04_GenerateOutput.py" >> "%LOG_FILE%" 2>&1
        if errorlevel 1 (
            echo [%date% %time%] ERROR: Step 4 failed! >> "%LOG_FILE%"
            echo [%date% %time%] ERROR: Step 4 failed!
            goto error_handler
        )
        echo [%date% %time%] STEP 04: Completed successfully >> "%LOG_FILE%"
        echo [%date% %time%] STEP 04: Completed successfully
        call :backup_step 4
        timeout /t 1 /nobreak >nul
        echo 4 > "%STATE_FILE%"
    )

    REM Step 5: PCID Missing
    if %LAST_STEP% LSS 5 (
        echo. >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 05: Processing PCID Missing >> "%LOG_FILE%"
        echo ============================================================================ >> "%LOG_FILE%"
        echo [%date% %time%] STEP 05: Processing PCID Missing...
        python "%SCRIPT_DIR%script\05_PCIDmissing.py" >> "%LOG_FILE%" 2>&1
        if errorlevel 1 (
            echo [%date% %time%] ERROR: Step 5 failed! >> "%LOG_FILE%"
            echo [%date% %time%] ERROR: Step 5 failed!
            goto error_handler
        )
        echo [%date% %time%] STEP 05: Completed successfully >> "%LOG_FILE%"
        echo [%date% %time%] STEP 05: Completed successfully
        call :backup_step 5
        timeout /t 1 /nobreak >nul
        echo 5 > "%STATE_FILE%"
    )

    REM Pipeline completed successfully
    echo. >> "%LOG_FILE%"
    echo ============================================================================ >> "%LOG_FILE%"
    echo [%date% %time%] PIPELINE COMPLETED SUCCESSFULLY >> "%LOG_FILE%"
    echo ============================================================================ >> "%LOG_FILE%"
    echo [%date% %time%] PIPELINE COMPLETED SUCCESSFULLY
    
    REM Create final backup
    call :backup_final
    
    REM Reset state for next run
    timeout /t 1 /nobreak >nul
    echo 0 > "%STATE_FILE%"
    
    REM If loop mode, wait and restart
    if %LOOP_MODE% EQU 1 (
        echo [%date% %time%] Waiting 60 seconds before next run... >> "%LOG_FILE%"
        echo [%date% %time%] Waiting 60 seconds before next run...
        timeout /t 60 /nobreak >nul
        goto main_loop
    )
    
    goto end

:backup_step
    setlocal enabledelayedexpansion
    set STEP_NUM=%~1
    REM Get timestamp for backup directory
    for /f "usebackq tokens=*" %%T in (`powershell -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"`) do set BACKUP_TS=%%T
    set BACKUP_STEP_DIR=%BACKUP_DIR%\step%STEP_NUM%_!BACKUP_TS!
    if not exist "!BACKUP_STEP_DIR!" mkdir "!BACKUP_STEP_DIR!"
    if not exist "!BACKUP_STEP_DIR!\Input" mkdir "!BACKUP_STEP_DIR!\Input"
    if not exist "!BACKUP_STEP_DIR!\Output" mkdir "!BACKUP_STEP_DIR!\Output"
    
    echo [%date% %time%] Creating backup for step %STEP_NUM%... >> "%LOG_FILE%"
    echo [%date% %time%] Creating backup for step %STEP_NUM%...
    
    REM Backup Input files
    if exist "%SCRIPT_DIR%Input\*.csv" (
        xcopy /Y /I "%SCRIPT_DIR%Input\*.csv" "!BACKUP_STEP_DIR!\Input\" >nul 2>&1
    )
    
    REM Backup Output files
    if exist "%SCRIPT_DIR%Output\*.csv" (
        xcopy /Y /I "%SCRIPT_DIR%Output\*.csv" "!BACKUP_STEP_DIR!\Output\" >nul 2>&1
    )
    if exist "%SCRIPT_DIR%Output\*.xlsx" (
        xcopy /Y /I "%SCRIPT_DIR%Output\*.xlsx" "!BACKUP_STEP_DIR!\Output\" >nul 2>&1
    )
    
    echo [%date% %time%] Backup created: !BACKUP_STEP_DIR! >> "%LOG_FILE%"
    echo [%date% %time%] Backup created: !BACKUP_STEP_DIR!
    endlocal
    exit /b

:backup_final
    setlocal enabledelayedexpansion
    REM Get timestamp for final backup directory
    for /f "usebackq tokens=*" %%T in (`powershell -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"`) do set BACKUP_TS=%%T
    set BACKUP_FINAL_DIR=%BACKUP_DIR%\final_!BACKUP_TS!
    if not exist "!BACKUP_FINAL_DIR!" mkdir "!BACKUP_FINAL_DIR!"
    
    echo [%date% %time%] Creating final backup... >> "%LOG_FILE%"
    echo [%date% %time%] Creating final backup...
    
    REM Backup all Input files
    if exist "%SCRIPT_DIR%Input" (
        xcopy /E /I /Y "%SCRIPT_DIR%Input" "!BACKUP_FINAL_DIR!\Input\" >nul 2>&1
    )
    
    REM Backup all Output files
    if exist "%SCRIPT_DIR%Output" (
        xcopy /E /I /Y "%SCRIPT_DIR%Output" "!BACKUP_FINAL_DIR!\Output\" >nul 2>&1
    )
    
    echo [%date% %time%] Final backup created: !BACKUP_FINAL_DIR! >> "%LOG_FILE%"
    echo [%date% %time%] Final backup created: !BACKUP_FINAL_DIR!
    endlocal
    exit /b

:pre_startup_cleanup
    setlocal enabledelayedexpansion
    REM Get timestamp for pre-startup backup
    for /f "usebackq tokens=*" %%T in (`powershell -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"`) do set BACKUP_TS=%%T
    set PRE_BACKUP_DIR=%BACKUP_DIR%\pre_startup_!BACKUP_TS!
    if not exist "!PRE_BACKUP_DIR!" mkdir "!PRE_BACKUP_DIR!"
    
    echo [%date% %time%] Backing up Output folder... >> "%LOG_FILE%"
    echo [%date% %time%] Backing up Output folder...
    
    REM Backup all Output files
    if exist "%SCRIPT_DIR%Output" (
        xcopy /E /I /Y "%SCRIPT_DIR%Output" "!PRE_BACKUP_DIR!\Output\" >nul 2>&1
        echo [%date% %time%] Output backup created: !PRE_BACKUP_DIR!\Output\ >> "%LOG_FILE%"
        echo [%date% %time%] Output backup created: !PRE_BACKUP_DIR!\Output\
    ) else (
        echo [%date% %time%] No Output folder to backup >> "%LOG_FILE%"
    )
    
    echo [%date% %time%] Cleaning Input files (Productlist.csv)... >> "%LOG_FILE%"
    echo [%date% %time%] Cleaning Input files (Productlist.csv)...
    
    REM Delete Productlist.csv
    if exist "%SCRIPT_DIR%Input\Productlist.csv" (
        del /F /Q "%SCRIPT_DIR%Input\Productlist.csv" >nul 2>&1
        echo [%date% %time%] Deleted: Input\Productlist.csv >> "%LOG_FILE%"
        echo [%date% %time%] Deleted: Input\Productlist.csv
    )
    
    echo [%date% %time%] Pre-startup cleanup completed >> "%LOG_FILE%"
    echo [%date% %time%] Pre-startup cleanup completed
    endlocal
    exit /b

:error_handler
    echo. >> "%LOG_FILE%"
    echo ============================================================================ >> "%LOG_FILE%"
    echo [%date% %time%] PIPELINE HALTED - State saved for resume >> "%LOG_FILE%"
    echo ============================================================================ >> "%LOG_FILE%"
    echo [%date% %time%] PIPELINE HALTED - State saved for resume
    echo [%date% %time%] To resume, run the script again. It will continue from step %LAST_STEP% >> "%LOG_FILE%"
    echo [%date% %time%] To resume, run the script again. It will continue from step %LAST_STEP%
    exit /b 1

:end
    echo. >> "%LOG_FILE%"
    echo ============================================================================ >> "%LOG_FILE%"
    echo Pipeline Execution Ended: %date% %time% >> "%LOG_FILE%"
    echo ============================================================================ >> "%LOG_FILE%"
    echo.
    echo Log file: %LOG_FILE%
    exit /b 0

