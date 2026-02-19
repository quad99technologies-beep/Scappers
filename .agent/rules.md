# Scraper Platform Rules

This repository contains a robust, modular web scraping platform. All development must adhere to the following rules to ensure maintainability, reliability, and scalability.

## 1. Core Architecture
- **Do NOT reinvent the wheel.** Use the modules in `core/` for all infrastructure needs.
    - **Browser Automation:** Use `core.browser.driver_factory` to create drivers. NEVER instantiate `webdriver.Chrome()` directly.
    - **Configuration:** Use `core.config.config_manager.ConfigManager` for all settings. NEVER hardcode paths or credentials.
    - **Logging:** Use `core.utils.logger.get_logger`. NEVER use `print()` or raw `logging.getLogger`.
    - **Database:** Use `core.db.connection.CountryDB`.
    - **Monitoring:** Use `core.monitoring.resource_monitor` to check for resource usage.

## 2. Scraper Structure
- **Location:** All scrapers must reside in `scripts/<CountryName>/`.
- **Entry Point:** Every scraper MUST have a `run_pipeline_resume.py` file as the main orchestrator.
- **Step scripts:** Use numbered prefixes for execution steps (e.g., `01_login.py`, `02_search.py`).
- **Idempotency:** Scripts should be idempotent where possible. Use `core.pipeline.pipeline_checkpoint` to support resuming.

## 3. Configuration & Secrets
- **Environment Variables:** All secrets (DB creds, API keys) must be in `.env`.
- **Loading:** Load configuration via `ConfigManager.load_env("ScraperID")`.
- **Paths:** Use `ConfigManager.get_input_dir()` and `ConfigManager.get_output_dir()` for file I/O.

## 4. Error Handling & Reliability
- **Retries:** Use `core.reliability.SmartRetry` for network operations.
- **Failures:** Log all exceptions with `logger.error(..., exc_info=True)`.
- **Resource Leaks:** Always ensure drivers are quit, even on error. The `core` framework helps with this, but be mindful in custom scripts.

## 5. Coding Standards
- **Type Hinting:** Use Python type hints for all function signatures.
- **Docstrings:** specific complex logic must have docstrings.
- **Imports:** Use absolute imports for `core` modules (e.g., `from core.utils.logger import get_logger`).

## 6. Database
- **Schema:** Database schema changes should be reflected in `sql/` and applied via migration scripts.
- **Connections:** Always close database connections when done. Use context managers where possible.
