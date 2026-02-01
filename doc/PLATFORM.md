# Distributed Scraping Platform

A database-driven, distributed scraping platform with hybrid fetch capabilities.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL                                │
│  ┌───────────────┬───────────────┬───────────────┬────────────┐ │
│  │ pipeline_runs │ pipeline_cmds │   workers     │   urls     │ │
│  ├───────────────┼───────────────┼───────────────┼────────────┤ │
│  │   entities    │ entity_attrs  │  fetch_logs   │   files    │ │
│  └───────────────┴───────────────┴───────────────┴────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   ┌────▼────┐          ┌────▼────┐          ┌────▼────┐
   │ Worker 1│          │ Worker 2│          │ Worker N│
   │ (PC)    │          │(Mac Mini)│          │(Server) │
   └─────────┘          └─────────┘          └─────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │ Unified Fetcher │
                    │                 │
                    │ HTTP Stealth    │
                    │ Playwright      │
                    │ Selenium        │
                    │ TOR             │
                    │ API             │
                    └─────────────────┘
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r scripts/common/requirements.txt
playwright install chromium
```

### 2. Configure Database

Set environment variables:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=scrappers
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=your_password
```

### 3. Initialize Schema

```bash
psql -h localhost -U postgres -d scrappers -f sql/schemas/postgres/platform.sql
```

Or in Python:
```python
from scripts.common import ensure_platform_schema
ensure_platform_schema()
```

### 4. Start Workers

On each machine:
```bash
python scripts/common/worker.py
```

For specific countries:
```bash
python scripts/common/worker.py --countries "India,Malaysia"
```

### 5. Start Watchdog (Stale Job Recovery)

```bash
python scripts/common/watchdog.py --interval 120
```

### 6. Start GUI

```bash
streamlit run scripts/common/gui.py
```

## Components

### Database Layer (`scripts/common/db.py`)

Centralized database access with connection pooling.

```python
from scripts.common import (
    # Pipeline operations
    create_pipeline_run,
    claim_next_run,
    update_run_status,
    update_run_step,
    heartbeat,
    
    # URL operations
    register_url,
    get_url_id,
    update_url_status,
    
    # Entity operations
    insert_entity,
    insert_attributes,
    get_entity,
    
    # Logging
    log_fetch,
    log_error,
)
```

### Unified Fetcher (`scripts/common/fetcher.py`)

Single entry point for all HTTP/browser fetching.

```python
from scripts.common import fetch, fetch_html, FetchResult

# Automatic method selection based on country
result = fetch("https://example.com", country="Malaysia")

if result.success:
    html = result.content
    print(f"Method used: {result.method_used}")
else:
    print(f"Error: {result.error_type}")

# Convenience function
html = fetch_html("https://example.com", country="India")
```

#### Fetch Order by Country

| Country | Primary | Fallback 1 | Fallback 2 |
|---------|---------|------------|------------|
| Argentina | TOR | Selenium | Playwright |
| India | API | HTTP Stealth | Selenium |
| Russia | Selenium | Playwright | HTTP Stealth |
| Malaysia | HTTP Stealth | Playwright | Selenium |
| _default | HTTP Stealth | Playwright | Selenium |

### Response Validator (`scripts/common/validator.py`)

Validate fetched content.

```python
from scripts.common import validate_html, ValidationResult

result = validate_html(
    content=html,
    min_length=1000,
    required_selectors=["table.products", "#main"],
    check_cloudflare=True,
    check_captcha=True
)

if result.is_valid:
    # Process content
    pass
else:
    print(f"Invalid: {result.error_code}")
```

### Distributed Worker (`scripts/common/worker.py`)

Polls PostgreSQL for jobs and executes pipelines.

```python
from scripts.common import Worker, register_pipeline

# Register a pipeline runner
def my_pipeline(run_id, start_step, check_stop):
    for step in range(start_step, 5):
        if check_stop():
            return
        # Do work...

register_pipeline("Malaysia", my_pipeline)

# Run worker
worker = Worker(countries=["Malaysia"])
worker.run()
```

### Watchdog (`scripts/common/watchdog.py`)

Monitors for stale jobs and recovers them.

```python
from scripts.common.watchdog import run_watchdog

results = run_watchdog(
    heartbeat_timeout=600,  # 10 minutes
    worker_timeout=300,     # 5 minutes
    max_retries=3
)
```

### GUI (`scripts/common/gui.py`)

Streamlit-based control panel.

Features:
- Dashboard with run statistics
- Start/stop/resume pipelines
- Worker monitoring
- Error viewer
- Fetch statistics

## Database Schema

### Core Tables

#### `pipeline_runs`
Tracks each pipeline execution.

| Column | Type | Description |
|--------|------|-------------|
| run_id | UUID | Primary key |
| country | TEXT | Country name |
| status | TEXT | queued, running, stopped, completed, failed |
| current_step | TEXT | Current step name |
| current_step_num | INT | Current step number |
| worker_id | TEXT | Worker that claimed this job |
| last_heartbeat | TIMESTAMP | Last heartbeat from worker |

#### `entities`
Generic entity storage.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| entity_type | TEXT | product, tender, drug, etc. |
| country | TEXT | Country name |
| external_id | TEXT | External identifier |
| entity_hash | TEXT | Hash for deduplication |

#### `entity_attributes`
Key-value storage for entity fields.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| entity_id | BIGINT | FK to entities |
| field_name | TEXT | Attribute name |
| field_value | TEXT | Attribute value |
| field_type | TEXT | text, number, date, etc. |

#### `urls`
URL registry for deduplication and tracking.

#### `fetch_logs`
Detailed log of every fetch operation.

#### `files`
Tracks downloaded files (PDFs, images, etc.).

#### `errors`
Centralized error tracking.

## Migration Guide

### Migrating an Existing Scraper

**Before:**
```python
import requests
from bs4 import BeautifulSoup

response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
# ... extract data
# ... save to CSV
```

**After:**
```python
from scripts.common import (
    fetch,
    register_url,
    insert_entity,
    insert_attributes,
    log_error,
)

# Fetch with automatic fallback
result = fetch(url, country="Malaysia")

if not result.success:
    log_error("Malaysia", result.error_type, result.error_message)
    return

# Parse
soup = BeautifulSoup(result.content, 'html.parser')
data = extract_product(soup)

# Store in generic model
url_id = register_url(url, "Malaysia", entity_type="product")
entity_id = insert_entity("product", "Malaysia", source_url_id=url_id)
insert_attributes(entity_id, data)
```

### Making a Pipeline Worker-Compatible

Existing pipelines can run via subprocess without modification:

```python
from scripts.common import create_subprocess_runner, register_pipeline

runner = create_subprocess_runner("Malaysia")
register_pipeline("Malaysia", runner)
```

Or create a native runner:

```python
def malaysia_pipeline(run_id, start_step, check_stop):
    steps = [
        step_00_backup,
        step_01_collect,
        step_02_scrape,
        step_03_export,
    ]
    
    for i, step in enumerate(steps):
        if i < start_step:
            continue
        if check_stop():
            return
        update_run_step(run_id, i, step.__name__)
        step()

register_pipeline("Malaysia", malaysia_pipeline)
```

## Multi-Machine Deployment

1. **All machines point to same PostgreSQL**
2. **Each machine runs `worker.py`**
3. **Jobs distributed automatically**

```
Machine A (Windows PC)
├── worker.py (handles all countries)
└── watchdog.py

Machine B (Mac Mini)
├── worker.py --countries "India,Malaysia"

Machine C (Linux Server)
├── worker.py --countries "Argentina,Russia"
└── (runs TOR for Argentina)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| POSTGRES_HOST | localhost | Database host |
| POSTGRES_PORT | 5432 | Database port |
| POSTGRES_DB | scrappers | Database name |
| POSTGRES_USER | postgres | Database user |
| POSTGRES_PASSWORD | | Database password |
| POSTGRES_POOL_MIN | 2 | Min pool connections |
| POSTGRES_POOL_MAX | 10 | Max pool connections |

## Troubleshooting

### Connection Pool Exhausted

Increase pool size:
```bash
export POSTGRES_POOL_MAX=20
```

### Stale Jobs Not Recovering

Check watchdog is running:
```bash
python scripts/common/watchdog.py --verbose
```

### Fetch Always Falling Back to Browser

Check if curl_cffi is installed:
```bash
pip install curl-cffi
```

### GUI Not Updating

Click "Refresh Data" or check database connection.

## File Structure

```
scripts/
├── common/
│   ├── __init__.py          # Package exports
│   ├── db.py                 # Database layer
│   ├── fetcher.py            # Unified fetcher
│   ├── validator.py          # Response validation
│   ├── worker.py             # Distributed worker
│   ├── watchdog.py           # Stale job recovery
│   ├── gui.py                # Streamlit GUI
│   ├── requirements.txt      # Dependencies
│   └── example_integration.py # Usage examples
├── Malaysia/
│   └── ... (country scripts)
└── ... (other countries)

sql/
└── schemas/
    └── postgres/
        └── platform.sql      # Platform schema
```
