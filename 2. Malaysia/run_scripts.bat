@echo off
setlocal enabledelayedexpansion
REM Batch file to run all numbered scripts in sequence with stats tracking

echo ========================================
echo Running Scripts in Sequence
echo ========================================
echo.

REM Get the directory where the batch file is located
set "SCRIPT_DIR=%~dp0"
set "PYTHON_SCRIPT_DIR=%SCRIPT_DIR%Script"
set "OUTPUT_DIR=%SCRIPT_DIR%Output"
set "BACKUP_DIR=%SCRIPT_DIR%Backup"
set "LOG_FILE=%OUTPUT_DIR%\execution_log.txt"

REM Create Output directory if it doesn't exist
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

REM Initialize log file
echo ======================================== > "%LOG_FILE%"
echo Script Execution Log >> "%LOG_FILE%"
echo Execution Date: %date% %time% >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

REM Check if script folder exists
if not exist "%PYTHON_SCRIPT_DIR%" (
    echo ERROR: Script folder not found: %PYTHON_SCRIPT_DIR%
    echo ERROR: Script folder not found >> "%LOG_FILE%"
    pause
    exit /b 1
)

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python and try again.
    echo ERROR: Python not found >> "%LOG_FILE%"
    pause
    exit /b 1
)

echo Python found.
python --version >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

REM Check if Playwright browsers are installed
echo Checking for Playwright browsers...
if exist "%LOCALAPPDATA%\ms-playwright" (
    REM Check if chromium folder exists (version may vary)
    dir /b "%LOCALAPPDATA%\ms-playwright\chromium-*" >nul 2>&1
    if errorlevel 1 (
        goto :install_playwright
    )
) else (
    goto :install_playwright
)
goto :playwright_ok

:install_playwright
echo.
echo WARNING: Playwright browsers not found!
echo.
echo Would you like to install Playwright browsers now? (Y/N)
set /p INSTALL_CHOICE="Choice: "
if /i "!INSTALL_CHOICE!"=="Y" (
    echo.
    echo Installing Playwright browsers (this may take a few minutes)...
    playwright install chromium
    if errorlevel 1 (
        echo ERROR: Failed to install Playwright browsers.
        echo Please run 'playwright install' manually or run 'setup.bat'.
        pause
        exit /b 1
    )
    echo Playwright browsers installed successfully!
    echo.
) else (
    echo.
    echo Please run 'playwright install' or 'setup.bat' to install browsers before running scripts.
    pause
    exit /b 1
)

:playwright_ok

REM ========================================
REM CLEAR OLD OUTPUTS
REM ========================================
echo.
echo ========================================
echo Preparing Output Folder
echo ========================================
echo.

REM Create Backup directory if it doesn't exist
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"

if exist "%OUTPUT_DIR%" (
    REM Check if output folder has any files (excluding execution_log.txt)
    dir /b "%OUTPUT_DIR%\*.*" 2>nul | findstr /v /i "execution_log.txt" >nul 2>&1
    if not errorlevel 1 (
        REM Create backup with timestamp
        set "BACKUP_NAME=backup_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%%time:~6,2%"
        set "BACKUP_NAME=!BACKUP_NAME: =0!"
        set "BACKUP_NAME=!BACKUP_NAME:/=!"
        set "BACKUP_NAME=!BACKUP_NAME::=!"
        set "BACKUP_PATH=%BACKUP_DIR%\!BACKUP_NAME!"

        echo Creating backup: !BACKUP_NAME!
        echo Creating backup at: !BACKUP_PATH! >> "%LOG_FILE%"

        REM Create backup folder
        mkdir "!BACKUP_PATH!" 2>nul

        REM Copy files to backup (exclude execution_log.txt)
        for %%f in ("%OUTPUT_DIR%\*.*") do (
            if /i not "%%~nxf"=="execution_log.txt" (
                copy "%%f" "!BACKUP_PATH!\" >nul 2>&1
            )
        )

        REM Copy subdirectories
        for /d %%d in ("%OUTPUT_DIR%\*") do (
            xcopy "%%d" "!BACKUP_PATH!\%%~nxd\" /E /I /H /Y >nul 2>&1
        )

        echo Backup created successfully at: !BACKUP_PATH!
        echo Backup created successfully >> "%LOG_FILE%"
        echo.

        REM Clear output folder (keep execution_log.txt)
        echo Clearing output folder...
        echo Clearing output folder >> "%LOG_FILE%"

        REM Delete all files except execution_log.txt
        for %%f in ("%OUTPUT_DIR%\*.*") do (
            if /i not "%%~nxf"=="execution_log.txt" (
                del /q "%%f" >nul 2>&1
            )
        )

        REM Delete all subdirectories
        for /d %%d in ("%OUTPUT_DIR%\*") do rmdir /s /q "%%d" >nul 2>&1

        echo Output folder cleared.
        echo. >> "%LOG_FILE%"
    ) else (
        echo Output folder is already empty.
        echo Output folder is already empty >> "%LOG_FILE%"
    )
) else (
    echo Creating output folder...
    echo Creating output folder >> "%LOG_FILE%"
    mkdir "%OUTPUT_DIR%"
)

echo.
echo ========================================
echo Starting Script Execution
echo ========================================
echo.

REM Change to script directory
cd /d "%PYTHON_SCRIPT_DIR%"

REM ========================================
REM RUN SCRIPTS IN ORDER
REM ========================================

REM Script 01: Get Drug Prices from MyPriMe
echo.
echo ========================================
echo [1/5] Getting Drug Prices from MyPriMe
echo ========================================
set "SCRIPT_01=01_Product_Registration_Number.py"
if exist "%SCRIPT_01%" (
    echo Running: %SCRIPT_01%
    echo [1/5] Running: %SCRIPT_01% >> "%LOG_FILE%"
    echo Start Time: %time% >> "%LOG_FILE%"

    python "%SCRIPT_01%"
    if errorlevel 1 (
        echo [FAILED] Script 01 failed! >> "%LOG_FILE%"
        echo ERROR: Script 01 failed!
        echo End Time: %time% >> "%LOG_FILE%"
        echo. >> "%LOG_FILE%"
        goto :script_failed
    ) else (
        echo [SUCCESS] Script 01 completed >> "%LOG_FILE%"
        echo End Time: %time% >> "%LOG_FILE%"

        REM Record stats
        if exist "%OUTPUT_DIR%\malaysia_drug_prices_view_all.csv" (
            for /f %%a in ('find /c /v "" ^< "%OUTPUT_DIR%\malaysia_drug_prices_view_all.csv"') do set LINE_COUNT=%%a
            echo Output file: malaysia_drug_prices_view_all.csv (approx. !LINE_COUNT! lines) >> "%LOG_FILE%"
            echo Created malaysia_drug_prices_view_all.csv
        )
        echo. >> "%LOG_FILE%"
    )
) else (
    echo Script not found: %SCRIPT_01%
    echo [SKIPPED] Script not found: %SCRIPT_01% >> "%LOG_FILE%"
    echo. >> "%LOG_FILE%"
)

REM Script 02: Get Product Details from QUEST3+
echo.
echo ========================================
echo [2/5] Getting Product Details from QUEST3+
echo ========================================
set "SCRIPT_02=02_Product_Details.py"
if exist "%SCRIPT_02%" (
    echo Running: %SCRIPT_02%
    echo [2/5] Running: %SCRIPT_02% >> "%LOG_FILE%"
    echo Start Time: %time% >> "%LOG_FILE%"

    python "%SCRIPT_02%"
    if errorlevel 1 (
        echo [FAILED] Script 02 failed! >> "%LOG_FILE%"
        echo ERROR: Script 02 failed!
        echo End Time: %time% >> "%LOG_FILE%"
        echo. >> "%LOG_FILE%"
        goto :script_failed
    ) else (
        echo [SUCCESS] Script 02 completed >> "%LOG_FILE%"
        echo End Time: %time% >> "%LOG_FILE%"

        REM Record stats
        if exist "%OUTPUT_DIR%\quest3_product_details.csv" (
            for /f %%a in ('find /c /v "" ^< "%OUTPUT_DIR%\quest3_product_details.csv"') do set LINE_COUNT=%%a
            echo Output file: quest3_product_details.csv (approx. !LINE_COUNT! lines) >> "%LOG_FILE%"
            echo Created quest3_product_details.csv
        )
        echo. >> "%LOG_FILE%"
    )
) else (
    echo Script not found: %SCRIPT_02%
    echo [SKIPPED] Script not found: %SCRIPT_02% >> "%LOG_FILE%"
    echo. >> "%LOG_FILE%"
)

REM Script 03: Consolidate Results
echo.
echo ========================================
echo [3/5] Running Consolidate Results
echo ========================================
set "SCRIPT_03=03_Consolidate_Results.py"
if exist "%SCRIPT_03%" (
    echo Running: %SCRIPT_03%
    echo [3/5] Running: %SCRIPT_03% >> "%LOG_FILE%"
    echo Start Time: %time% >> "%LOG_FILE%"

    python "%SCRIPT_03%"
    if errorlevel 1 (
        echo [FAILED] Script 03 failed! >> "%LOG_FILE%"
        echo ERROR: Script 03 failed!
        echo End Time: %time% >> "%LOG_FILE%"
        echo. >> "%LOG_FILE%"
        goto :script_failed
    ) else (
        echo [SUCCESS] Script 03 completed >> "%LOG_FILE%"
        echo End Time: %time% >> "%LOG_FILE%"

        REM Record stats
        if exist "%OUTPUT_DIR%\consolidated_products.csv" (
            for /f %%a in ('find /c /v "" ^< "%OUTPUT_DIR%\consolidated_products.csv"') do set LINE_COUNT=%%a
            echo Output file: consolidated_products.csv (approx. !LINE_COUNT! lines) >> "%LOG_FILE%"
            echo Created consolidated_products.csv
        )
        echo. >> "%LOG_FILE%"
    )
) else (
    echo Script not found: %SCRIPT_03%
    echo [SKIPPED] Script not found: %SCRIPT_03% >> "%LOG_FILE%"
    echo. >> "%LOG_FILE%"
)

REM Script 04: Get Fully Reimbursable
echo.
echo ========================================
echo [4/5] Running Get Fully Reimbursable
echo ========================================
set "SCRIPT_04=04_Get_Fully_Reimbursable.py"
if exist "%SCRIPT_04%" (
    echo Running: %SCRIPT_04%
    echo [4/5] Running: %SCRIPT_04% >> "%LOG_FILE%"
    echo Start Time: %time% >> "%LOG_FILE%"

    python "%SCRIPT_04%"
    if errorlevel 1 (
        echo [FAILED] Script 04 failed! >> "%LOG_FILE%"
        echo ERROR: Script 04 failed!
        echo End Time: %time% >> "%LOG_FILE%"
        echo. >> "%LOG_FILE%"
        goto :script_failed
    ) else (
        echo [SUCCESS] Script 04 completed >> "%LOG_FILE%"
        echo End Time: %time% >> "%LOG_FILE%"

        REM Record stats
        if exist "%OUTPUT_DIR%\malaysia_fully_reimbursable_drugs.csv" (
            for /f %%a in ('find /c /v "" ^< "%OUTPUT_DIR%\malaysia_fully_reimbursable_drugs.csv"') do set LINE_COUNT=%%a
            echo Output file: malaysia_fully_reimbursable_drugs.csv (approx. !LINE_COUNT! lines) >> "%LOG_FILE%"
            echo Created malaysia_fully_reimbursable_drugs.csv
        )
        echo. >> "%LOG_FILE%"
    )
) else (
    echo Script not found: %SCRIPT_04%
    echo [SKIPPED] Script not found: %SCRIPT_04% >> "%LOG_FILE%"
    echo. >> "%LOG_FILE%"
)

REM Script 05: Generate PCID Mapped (FINAL)
echo.
echo ========================================
echo [5/5] Running Generate PCID Mapped (FINAL)
echo ========================================
set "SCRIPT_05=05_Generate_PCID_Mapped.py"
if exist "%SCRIPT_05%" (
    echo Running: %SCRIPT_05%
    echo [5/5] Running: %SCRIPT_05% >> "%LOG_FILE%"
    echo Start Time: %time% >> "%LOG_FILE%"

    python "%SCRIPT_05%"
    if errorlevel 1 (
        echo [FAILED] Script 05 failed! >> "%LOG_FILE%"
        echo ERROR: Script 05 failed!
        echo End Time: %time% >> "%LOG_FILE%"
        echo. >> "%LOG_FILE%"
        goto :script_failed
    ) else (
        echo [SUCCESS] Script 05 completed >> "%LOG_FILE%"
        echo End Time: %time% >> "%LOG_FILE%"

        REM Record stats
        if exist "%OUTPUT_DIR%\malaysia_pcid_mapped.csv" (
            for /f %%a in ('find /c /v "" ^< "%OUTPUT_DIR%\malaysia_pcid_mapped.csv"') do set MAPPED_COUNT=%%a
            echo Output file: malaysia_pcid_mapped.csv (approx. !MAPPED_COUNT! lines) >> "%LOG_FILE%"
            echo Created malaysia_pcid_mapped.csv (MAPPED records)
        )
        if exist "%OUTPUT_DIR%\malaysia_pcid_not_mapped.csv" (
            for /f %%a in ('find /c /v "" ^< "%OUTPUT_DIR%\malaysia_pcid_not_mapped.csv"') do set NOT_MAPPED_COUNT=%%a
            echo Output file: malaysia_pcid_not_mapped.csv (approx. !NOT_MAPPED_COUNT! lines) >> "%LOG_FILE%"
            echo Created malaysia_pcid_not_mapped.csv (NOT MAPPED records)
        )
        echo. >> "%LOG_FILE%"
    )
) else (
    echo Script not found: %SCRIPT_05%
    echo [SKIPPED] Script not found: %SCRIPT_05% >> "%LOG_FILE%"
    echo. >> "%LOG_FILE%"
)

REM Return to original directory
cd /d "%SCRIPT_DIR%"

REM ========================================
REM EXECUTION SUMMARY
REM ========================================
echo.
echo ========================================
echo Execution Summary
echo ========================================
echo.
echo ======================================== >> "%LOG_FILE%"
echo Execution Summary >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"

echo All scripts completed successfully!
echo Status: ALL SCRIPTS COMPLETED SUCCESSFULLY >> "%LOG_FILE%"
echo End Time: %date% %time% >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

echo Output Files:
echo. >> "%LOG_FILE%"
echo Output Files: >> "%LOG_FILE%"
if exist "%OUTPUT_DIR%\malaysia_drug_prices_view_all.csv" (
    echo - malaysia_drug_prices_view_all.csv (Script 01 output)
    echo - malaysia_drug_prices_view_all.csv >> "%LOG_FILE%"
)
if exist "%OUTPUT_DIR%\quest3_product_details.csv" (
    echo - quest3_product_details.csv (Script 02 output)
    echo - quest3_product_details.csv >> "%LOG_FILE%"
)
if exist "%OUTPUT_DIR%\consolidated_products.csv" (
    echo - consolidated_products.csv (Script 03 output)
    echo - consolidated_products.csv >> "%LOG_FILE%"
)
if exist "%OUTPUT_DIR%\malaysia_fully_reimbursable_drugs.csv" (
    echo - malaysia_fully_reimbursable_drugs.csv (Script 04 output)
    echo - malaysia_fully_reimbursable_drugs.csv >> "%LOG_FILE%"
)
if exist "%OUTPUT_DIR%\malaysia_pcid_mapped.csv" (
    echo - malaysia_pcid_mapped.csv (Script 05 - MAPPED records)
    echo - malaysia_pcid_mapped.csv (MAPPED records) >> "%LOG_FILE%"
)
if exist "%OUTPUT_DIR%\malaysia_pcid_not_mapped.csv" (
    echo - malaysia_pcid_not_mapped.csv (Script 05 - NOT MAPPED records)
    echo - malaysia_pcid_not_mapped.csv (NOT MAPPED records) >> "%LOG_FILE%"
)
echo.
echo ========================================
echo Log file saved to: Output\execution_log.txt
echo ========================================

echo. >> "%LOG_FILE%"
echo Log completed successfully >> "%LOG_FILE%"

pause
exit /b 0

:script_failed
cd /d "%SCRIPT_DIR%"
echo.
echo ========================================
echo Script execution failed!
echo ========================================
echo Please check the errors above and the log file: Output\execution_log.txt
echo.
echo Status: FAILED >> "%LOG_FILE%"
echo End Time: %date% %time% >> "%LOG_FILE%"
pause
exit /b 1

endlocal
