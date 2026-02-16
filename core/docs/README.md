# Core Module Documentation

The `core/` directory contains all reusable logic for the scraper system. Any logic that is not specific to a single site or business process MUST reside here.

## 1. Structure

*   `core/bootstrap/`: Environment setup and sys.path management.
*   `core/browser/`: Browser automation (Selenium, Playwright, Chrome/Firefox drivers).
*   `core/concurrency/`: Async workers, thread pools.
*   `core/control/`: Lifecycle management (shutdown hooks), rate limiting.
*   `core/http/`: HTTP clients (httpx wrapper, requests wrapper).
*   `core/io/`: Input/Output utilities (CSV, JSONL, File writers).
*   `core/network/`: Network tools (Proxy, VPN, Tor).
*   `core/parsing/`: Data extraction helpers (Price, Date).
*   `core/pipeline/`: Base classes for scrapers.
*   `core/utils/`: General utilities (String normalization).

## 2. Usage Guide

### Bootstrap
At the top of every scraper script:

```python
from core.bootstrap.environment import setup_scraper_environment
REPO_ROOT = setup_scraper_environment(__file__)
```

### HTTP Client
Avoid using `requests` or `httpx` directly. Use `core.http.client`:

```python
from core.http.client import HttpClient

async with HttpClient() as client:
    response = await client.get("https://example.com")
```

### Browser
Do not instantiate `webdriver.Chrome()` directly. Use `driver_factory`:

```python
from core.browser.driver_factory import create_chrome_driver
driver = create_chrome_driver(headless=True)
```

### Data Writing
Write standard output formats:

```python
from core.io.file_writer import DataWriter
with DataWriter(path, "output.jsonl") as writer:
    writer.write_jsonl(item)
```

## 3. Contribution Rules

1.  **Strict Separation**: If logic is reused >1 time, move to core.
2.  **No Site Logic**: Core modules must never contain site-specific selectors or URLs.
3.  **Atomic**: Modules should be small and focused (e.g., `price_parser.py` not `utils.py`).
