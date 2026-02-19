---
description: Run a scraper for a specific country
---

# Run Scraper Workflow

This workflow guides you through running a scraper for a specific country.

## Prerequisites
- The `.env` file must be configured with correct database credentials.
- The `scripts/<CountryName>` directory must exist.

## Steps

1.  **Identify the Scraper**
    Determine the `<CountryName>` you want to scrape (e.g., `Argentina`, `Russia`, `Malaysia`).

2.  **Navigate to Scraper Directory**
    The scraper scripts are located in `scripts/<CountryName>`.

3.  **Run the Pipeline**
    Execute the standard entry point `run_pipeline_resume.py`.

    ```bash
    # Run the scraper
    python scripts/<CountryName>/run_pipeline_resume.py
    ```

    **Optional Flags:**
    - `--fresh`: Start a fresh run, ignoring previous checkpoints.
    - `--step <N>`: Run a specific step (if supported by the custom implementation).

4.  **Monitor Execution**
    - Check the console output for progress.
    - Logs are also saved to `logs/<CountryName>/`.
    - Use the `core.monitoring` tools if you suspect resource issues.

## Example

To run the Argentina scraper:

```bash
python scripts/Argentina/run_pipeline_resume.py --fresh
```
