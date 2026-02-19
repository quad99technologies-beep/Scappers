---
description: Debug a failing scraper or pipeline
---

# Debug Scraper Workflow

This workflow helps you diagnose and fix issues with a scraper.

## Steps

1.  **Identify the Failure**
    - Check the terminal output for immediate errors.
    - Check the logs in `logs/<CountryName>/<run_id>.log` or `logs/core.log`.

2.  **Verify Configuration**
    - Ensure `.env` has the correct credentials.
    - Check if `ConfigManager` is loading the correct scraper ID.

3.  **Check Database State**
    - Use `check_run_ledger.py` to see the status of recent runs.
    - Verify if the `run_id` exists in the `run_ledger` table.

    ```bash
    python check_run_ledger.py --scraper <ScraperName>
    ```

    - Check for data anomalies in the output tables (e.g., `extracted_data`, `products`).

4.  **Inspect Common Issues**
    - **Network**: Is the proxy/VPN working? Check `core.network.proxy_checker`.
    - **Browser**: Is the driver crashing? Check `chrome_debug.log` if enabled.
    - **Selectors**: Has the website layout changed? Open the URL in a browser and inspect elements.

5.  **Run in Debug Mode**
    - Modify the script to add more logging or breakpoints.
    - Run a specific step in isolation if possible.

    ```bash
    # Example: Run only step 2
    python scripts/<CountryName>/02_extract_data.py
    ```

6.  **Fix and Retry**
    - Apply the fix.
    - Run the pipeline again, optionally using `--resume` if supported by the script logic (most `run_pipeline_resume.py` scripts handle this automatically based on checkpoints).

## Key Files for Debugging
- `logs/`: Application logs.
- `core/monitoring/`: Health check scripts.
- `check_run_ledger.py`: Run status tracking.
