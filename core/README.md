# Scraper Platform Core

The `core/` package provides the foundational infrastructure for all scrapers in the platform. It is designed to be modular, robust, and site-agnostic. All site-specific logic belongs in `scripts/`, while shared infrastructure belongs here.

## Directory Structure

The `core` module is organized into logical domains:

*   **`core/browser/`**: Browser automation infrastructure.
    *   `driver_factory.py`: Standard way to create Chrome/Firefox drivers.
    *   `chrome_manager.py`: Process management (tracking, cleanup).
    *   `human_behavior.py`: Stealth and human emulation (jitter, typing delays).
*   **`core/config/`**: Configuration management.
    *   `config_manager.py`: Centralized loading of environment variables and paths.
*   **`core/data/`**: Data processing and validation.
    *   `data_quality_checks.py`: Schema validation and null checks.
    *   `deduplicator.py`: Logic for detecting duplicate records.
*   **`core/db/`**: Database interactions.
    *   `connection.py`: DB connection management (Postgres).
    *   `models/`: Shared SQLAlchemy models (e.g., `RunLedger`).
*   **`core/monitoring/`**: Observability and Health.
    *   `resource_monitor.py`: Memory and CPU tracking.
    *   `alerting_integration.py`: Integration with external alerting (e.g., Telegram).
    *   `audit_logger.py`: Tracking of critical system events.
*   **`core/network/`**: Networking tools.
    *   `proxy_checker.py`: Validating proxies and VPNs.
    *   `tor_manager.py`: Control of Tor circuits.
*   **`core/pipeline/`**: Scraper orchestration.
    *   `base_scraper.py`: Abstract base classes for scrapers.
    *   `pipeline_checkpoint.py`: Resumability and state saving.
    *   `step_hooks.py`: Hooks for execution steps.
*   **`core/utils/`**: General utilities.
    *   `logger.py`: Standardized logging configuration.
    *   `text_utils.py`: String normalization and cleaning.
    *   `file_utils.py`: File system helpers.

## Usage Reference for New Scrapers

When building a new scraper, you should rely on these core modules instead of reinventing the wheel.

### 1. Configuration
**Do not** explicitly load `.env` files. Use `ConfigManager`.

```python
from core.config.config_manager import ConfigManager

# Initialize and load
ConfigManager.ensure_dirs()
ConfigManager.load_env("MyScraperID")

# Access values
api_key = ConfigManager.get_env_value("MyScraperID", "API_KEY")
input_dir = ConfigManager.get_input_dir("MyScraperID")
```

### 2. Logging
**Do not** use raw `logging.getLogger`. Use the core factory.

```python
from core.utils.logger import get_logger

logger = get_logger("my_scraper")
logger.info("Starting scrape run...")
```

### 3. Browser Automation
**Do not** instantiate `webdriver.Chrome()` directly.

```python
from core.browser.driver_factory import create_chrome_driver

driver = create_chrome_driver(headless=True)
# ... use driver ...
driver.quit()
```

### 4. Database Access
Use the standardized connection class.

```python
from core.db.connection import CountryDB

db = CountryDB("MyCountry")
db.connect()
# ...
db.close()
```

### 5. Resource Monitoring
To prevent OOM crashes, use the resource monitor.

```python
from core.monitoring.resource_monitor import check_memory_limit

if check_memory_limit(limit_mb=2048):
    logger.warning("Memory limit reached!")
    # ... handle cleanup ...
```

## Migration Guide (Deprecated Imports)

The flat structure of `core/` has been cleaned up. If you encounter `ImportError` in older scripts, update them to the new locations:

| Old Import | New Import |
|Data to Verify|---|
| `core.logger` | `core.utils.logger` |
| `core.config_manager` | `core.config.config_manager` |
| `core.resource_monitor` | `core.monitoring.resource_monitor` |
| `core.alerting_integration` | `core.monitoring.alerting_integration` |
| `core.audit_logger` | `core.monitoring.audit_logger` |
| `core.data_quality_checks` | `core.data.data_quality_checks` |
| `core.pipeline_start_lock` | `core.pipeline.pipeline_start_lock` |

This ensures a clean, maintainable, and navigable codebase for the future.
