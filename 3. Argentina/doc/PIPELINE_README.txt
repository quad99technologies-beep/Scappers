============================================================================
Argentina Scraper Pipeline - Usage Guide
============================================================================

OVERVIEW
--------
This pipeline orchestrates 6 steps of the Argentina scraper with:
- Automatic state tracking and resume capability
- Backups after each step completion
- Human-readable logging
- Max row extraction limits
- Loop mode for continuous execution

SCRIPTS (in execution order)
----------------------------
Location: ./script/
1. 01_getCompanyList.py      - Extracts company list
2. 02_getProdList.py         - Extracts product list
3. 03_alfabeta_scraper_labs.py - Scrapes product details (supports --max-rows)
4. 04_TranslateUsingDictionary.py - Translates Spanish to English
5. 05_GenerateOutput.py     - Generates final report
6. 06_PCIDmissing.py         - Processes missing PCIDs

USAGE
-----
Basic usage (run all steps):
    run_pipeline.bat

With max row limit (limit step 3 extraction):
    run_pipeline.bat --max-rows 1000

Run in continuous loop:
    run_pipeline.bat --loop

Combine options:
    run_pipeline.bat --max-rows 500 --loop

RESUME CAPABILITY
-----------------
If the pipeline is halted (error, manual stop, etc.):
- The last completed step is saved in pipeline_state.txt
- Simply run run_pipeline.bat again
- It will automatically resume from the last completed step
- No data loss - backups are created after each step

BACKUPS
-------
- Backups are created after each step completion
- Location: ./backups/
- Structure:
  - backups/step1_TIMESTAMP/  - Backup after step 1
  - backups/step2_TIMESTAMP/  - Backup after step 2
  - ... and so on
  - backups/final_TIMESTAMP/ - Final backup after all steps complete

LOGGING
-------
- All execution logs are saved to: ./logs/
- Log file format: pipeline_YYYYMMDD_HHMMSS.log
- Logs include:
  - Step execution start/end times
  - Success/failure status
  - Error messages
  - Backup creation confirmations

STATE FILE
----------
- File: pipeline_state.txt
- Contains: Last completed step number (0-6)
- Automatically updated after each successful step
- Reset to 0 after full pipeline completion

MAX ROWS LIMIT
--------------
- Only applies to step 3 (main scraper)
- Limits the number of product rows processed
- Useful for testing or partial runs
- Set to 0 for unlimited (default)
- Example: --max-rows 1000 will process only first 1000 products

LOOP MODE
---------
- Runs the pipeline continuously
- Waits 60 seconds between runs
- Useful for scheduled/automated execution
- State is reset after each complete run
- Press Ctrl+C to stop

ERROR HANDLING
---------------
- If any step fails, the pipeline stops
- State is saved, allowing resume from last successful step
- Error details are logged to the log file
- Check logs/pipeline_*.log for details

NOTES
-----
- All business logic and scraping logic remain unchanged
- Only orchestration and state management added
- Scripts can still be run individually if needed
- Directory structure:
  * script/ - All Python scripts (2-digit prefix naming)
  * doc/ - Documentation files
  * Input/ - Input data files
  * Output/ - Output data files
  * logs/ - Execution logs (created automatically)
  * backups/ - Step backups (created automatically)

============================================================================

