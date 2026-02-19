---
description: Create a new scraper for a new country/market
---

# Add New Scraper Workflow

This workflow systematically guides you through adding a new scraper to the platform.

## Steps

1.  **Create Directory**
    Create a new directory for the country in `scripts/`.
    ```bash
    mkdir scripts/<NewCountry>
    ```

2.  **Initialize Structure**
    Copy the structure from a reference implementation (e.g., `scripts/Argentina` or `scripts/Russia`).
    Required files:
    - `run_pipeline_resume.py` (The orchestrator)
    - `__init__.py`
    - `config/` (if specific configs are needed)

3.  **Configure Orchestrator**
    Edit `scripts/<NewCountry>/run_pipeline_resume.py` to update the `SCRAPER_NAME` and step definitions.

    ```python
    SCRAPER_NAME = "<NewCountry>"
    # ...
    # Define your steps
    steps = [
        "01_collect_urls.py",
        "02_extract_data.py"
    ]
    ```

4.  **Implement Steps**
    Create the python scripts for each step (e.g., `01_collect_urls.py`).
    **CRITICAL**: Use `core` modules!

    - **Imports**:
      ```python
      from core.utils.logger import get_logger
      from core.config.config_manager import ConfigManager
      from core.browser.driver_factory import create_chrome_driver
      ```

    - **Setup**:
      ```python
      logger = get_logger("<NewCountry>")
      ConfigManager.load_env("<NewCountry>")
      ```

5.  **Database Setup**
    - Define any necessary SQL schemas in `sql/schemas/postgres/<new_country>.sql`.
    - Apply the schema to the database.
    - Use `core.db.connection.CountryDB` within your scripts to interact with the DB.

6.  **Test**
    Run the scraper using the Run Scraper workflow.
    ```bash
    python scripts/<NewCountry>/run_pipeline_resume.py --fresh
    ```

7.  **Documentation**
    Add a `README.md` in `scripts/<NewCountry>/` explaining any specific nuances of this target site.
