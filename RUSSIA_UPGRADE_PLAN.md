# Russia Scraper Upgrade Plan

## Overview
Upgrade Russia scraper from CSV-based to DB-based with resume support, incorporating best practices from Malaysia, India, and Argentina scrapers.

## Current State Analysis

### Existing Files
- `01_russia_farmcom_scraper.py` - VED data scraper (CSV output)
- `02_russia_farmcom_excluded_scraper.py` - Excluded list scraper (CSV output)
- `03_retry_failed_pages.py` - Retry failed pages (CSV-based)
- `04_process_and_translate.py` - Translation processing (CSV-based)
- `05_format_for_export.py` - Export formatting (CSV-based)
- `config_loader.py` - Configuration loading
- `state_machine.py` - Navigation state machine
- `smart_locator.py` - Smart element locators

### Issues to Fix
1. **CSV-based storage** - No resume capability at row level
2. **Hardcoded values** - Some values still in scripts instead of env
3. **No DB integration** - Missing from platform DB architecture
4. **Duplicate code** - Similar patterns in VED and Excluded scrapers

## New Architecture

### Database Schema (Created ✓)
- `ru_ved_products` - Step 1: VED pricing data
- `ru_excluded_products` - Step 2: Excluded drugs
- `ru_translated_products` - Step 3: Translated data
- `ru_export_ready` - Step 4: Final export data
- `ru_step_progress` - Sub-step resume tracking
- `ru_failed_pages` - Failed pages for retry

### Repository Pattern (Created ✓)
- `RussiaRepository` class with all DB operations
- Methods for insert, update, query, progress tracking
- Bulk insert support for performance

## Implementation Steps

### Phase 1: DB Infrastructure ✓
- [x] Create `scripts/Russia/db/schema.py`
- [x] Create `scripts/Russia/db/repositories.py`
- [x] Update `config/Russia.env.json` with all config options

### Phase 2: Upgrade Scraper Scripts

#### 01_russia_farmcom_scraper.py (VED Scraper)
**Changes needed:**
1. Add DB imports:
```python
from core.db.connection import CountryDB
from db.schema import apply_russia_schema
from db.repositories import RussiaRepository
from core.db.models import generate_run_id
```

2. Replace CSV operations with DB:
```python
# OLD: CSV-based
def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {}

def save_progress(progress: dict):
    PROGRESS_FILE.write_text(json.dumps(progress))

# NEW: DB-based
def get_resume_page(repo: RussiaRepository) -> int:
    """Get last completed page from DB for resume"""
    completed = repo.get_completed_keys(step_number=1)
    if not completed:
        return 1
    # Extract page numbers from progress keys like "ved_page:5"
    pages = [int(k.split(":")[1]) for k in completed if k.startswith("ved_page:")]
    return max(pages) + 1 if pages else 1
```

3. Replace row storage:
```python
# OLD: CSV append
with open(OUT_CSV, "a", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=...)
    writer.writerow(row_data)

# NEW: DB insert
repo.insert_ved_product({
    "item_id": item_id,
    "tn": tn,
    "inn": inn,
    ...
})
repo.mark_progress(1, "VED Scrape", f"ved_page:{page}", "completed")
```

4. Add deduplication check:
```python
# Check if item already exists
existing_ids = repo.get_existing_item_ids()
if item_id in existing_ids:
    continue  # Skip duplicate
```

#### 02_russia_farmcom_excluded_scraper.py (Excluded Scraper)
**Similar changes as Step 1**, but using:
- `repo.insert_excluded_product()`
- Progress key: `f"excluded_page:{page}"`

#### 04_process_and_translate.py (Translation)
**Changes needed:**
1. Read from DB instead of CSV:
```python
# OLD: CSV read
ved_products = list(csv.DictReader(open(VED_CSV)))

# NEW: DB read
ved_products = repo.get_ved_products()
```

2. Store translations in DB:
```python
repo.insert_translated_product({
    "item_id": product["item_id"],
    "tn_ru": product["tn"],
    "tn_en": translated_name,
    "translation_method": "dictionary" if dict_hit else "ai",
    ...
})
```

#### 05_format_for_export.py (Export)
**Changes needed:**
1. Read from `ru_translated_products` table
2. Insert into `ru_export_ready` table
3. Generate CSV from DB at the end

### Phase 3: Remove Duplicate/Outdated Code

#### Files to Remove:
- `03_retry_failed_pages.py` - Functionality merged into main scrapers via DB
- `01_russia_farmcom_scraper - Copy.py` - Duplicate file
- `test_ai_fallback.py` - Test file, not needed in production

#### Code to Clean Up:
1. Remove CSV-related code from all scrapers
2. Remove progress.json handling
3. Consolidate common Selenium setup into base class (optional)

### Phase 4: Update Config Loader

Ensure all values come from env:
```python
# In config_loader.py - already good, but verify:
BASE_URL = getenv("SCRIPT_01_BASE_URL")  # No default in script
REGION_VALUE = getenv("SCRIPT_01_REGION_VALUE")
# etc.
```

## Configuration Updates Needed

### New Config Values (Added to Russia.env.json)
```json
{
  "SCRIPT_01_NUM_WORKERS": 3,
  "SCRIPT_01_MAX_RETRIES_PER_PAGE": 3,
  "SCRIPT_01_NAV_RETRIES": 3,
  "SCRIPT_01_NAV_RETRY_SLEEP": 5.0,
  "SCRIPT_01_NAV_RESTART_DRIVER": true,
  "SCRIPT_01_CHROME_NO_SANDBOX": "--no-sandbox",
  "SCRIPT_01_CHROME_DISABLE_DEV_SHM": "--disable-dev-shm-usage",
  
  "SCRIPT_02_NUM_WORKERS": 3,
  "SCRIPT_02_MAX_RETRIES_PER_PAGE": 3,
  
  "SCRIPT_03_RETRY_MAX_ATTEMPTS": 3,
  "SCRIPT_03_RETRY_DELAY_SECONDS": 5,
  "SCRIPT_03_BATCH_SIZE": 100,
  "SCRIPT_03_USE_AI_FALLBACK": true,
  "SCRIPT_03_AI_MODEL": "gpt-4o-mini",
  
  "SCRIPT_04_EXPORT_BATCH_SIZE": 1000,
  "SCRIPT_04_OUTPUT_ENCODING": "utf-8-sig",
  
  "DB_RESUME_ENABLED": true,
  "DB_BATCH_INSERT_SIZE": 100,
  "DB_PROGRESS_LOG_INTERVAL": 50
}
```

## Features from Other Scrapers to Port

### From Malaysia:
1. ✓ DB schema with run_id isolation
2. ✓ Repository pattern for DB access
3. ✓ Step progress tracking
4. Page-level resume capability
5. Bulk insert for performance

### From India:
1. Work queue pattern (not needed - Russia is simpler)
2. Connection pooling (already in base)
3. Performance monitoring

### From Argentina:
1. State machine for navigation (already have)
2. Smart locator (already have)
3. Chrome PID tracking (already have)
4. Driver restart on failure
5. Periodic cleanup/GC

## Testing Checklist

- [ ] Fresh run completes successfully
- [ ] Resume from middle of Step 1 works
- [ ] Resume from middle of Step 2 works
- [ ] Failed pages are tracked and can be retried
- [ ] Deduplication works (same item_id not inserted twice)
- [ ] Export produces same CSV format as before
- [ ] GUI integration works (run_ledger updates)

## Migration Path

1. Deploy new code alongside old code
2. Test with small MAX_PAGES limit
3. Verify DB tables are populated correctly
4. Compare export CSV with old version
5. Once verified, remove old CSV-based scripts
