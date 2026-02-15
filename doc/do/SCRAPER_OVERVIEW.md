# North Macedonia Scraper - Complete Overview

## 📋 Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Sources](#data-sources)
4. [Pipeline Steps](#pipeline-steps)
5. [Key Components](#key-components)
6. [Database Schema](#database-schema)
7. [Configuration](#configuration)
8. [How to Run](#how-to-run)
9. [Technical Details](#technical-details)
10. [Troubleshooting](#troubleshooting)

---

## Overview

The **North Macedonia Scraper** is a sophisticated web scraping system designed to collect pharmaceutical drug data from North Macedonian government websites. It extracts drug registration information and maximum price data, then combines them into a standardized EVERSANA format.

### Purpose
- Collect drug register data from the Ministry of Health (MoH) website
- Extract maximum price information from zdravstvo.gov.mk
- Map products to PCID (Product Code ID) for standardization
- Export data in EVERSANA format for pharmaceutical pricing analysis

### Key Features
- ✅ **Resume-safe**: Checkpoints allow resuming from any step
- ✅ **Multi-threaded**: Parallel processing for faster scraping
- ✅ **Anti-bot protection**: Stealth mode, human-like delays, browser fingerprint masking
- ✅ **State machine**: Deterministic navigation with validation
- ✅ **Resource management**: Memory limits, automatic cleanup, orphan process handling
- ✅ **Translation**: Automatic Macedonian to English translation
- ✅ **Robust error handling**: Retries, fallbacks, and detailed logging

---

## Architecture

### High-Level Flow
```
┌─────────────────────────────────────────────────────────────┐
│                    Pipeline Runner                          │
│              (run_pipeline_resume.py)                       │
└────────────┬────────────────────────────────────────────────┘
             │
             ├─► Step 0: Backup & Clean (00_backup_and_clean.py)
             │   └─► Backup existing data, initialize DB schema
             │
             ├─► Step 1: Collect URLs (01_collect_urls.py)
             │   └─► Navigate drug register, extract detail URLs
             │
             ├─► Step 2: Scrape Details (02_scrape_details.py)
             │   └─► Visit each URL, extract drug registration data
             │
             ├─► Step 3: Scrape Zdravstvo (03_scrape_zdravstvo.py)
             │   └─► Extract maximum price data from zdravstvo.gov.mk
             │
             └─► Step 3a: Parallel Max Prices (03a_scrape_maxprices_parallel.py)
                 └─► Parallel extraction of price history
```

### Technology Stack
- **Language**: Python 3.x
- **Browser Automation**: Selenium WebDriver (Chrome)
- **Database**: PostgreSQL
- **Translation**: deep-translator library
- **Resource Monitoring**: psutil
- **Anti-Detection**: Stealth profile, user-agent rotation

---

## Data Sources

### 1. Drug Register (Ministry of Health)
- **URL**: North Macedonia Ministry of Health drug register
- **Data Extracted**:
  - Registration number
  - Product name (Macedonian & English)
  - Generic name (Macedonian & English)
  - Dosage form
  - Strength
  - Pack size
  - Manufacturer
  - Marketing Authorization Holder (MAH)
  - ATC code
  - Source URL

### 2. Maximum Prices (zdravstvo.gov.mk)
- **URL**: zdravstvo.gov.mk price database
- **Data Extracted**:
  - Product name
  - Generic name
  - Dosage form
  - Strength
  - Pack size
  - Manufacturer
  - Maximum price (MKD - Macedonian Denar)
  - Effective date
  - Price history

---

## Pipeline Steps

### Step 0: Backup & Clean (`00_backup_and_clean.py`)
**Purpose**: Prepare environment for fresh run

**Actions**:
1. Backup existing output files to `backups/NorthMacedonia/`
2. Initialize PostgreSQL schema (creates tables if not exist)
3. Clean temporary files
4. Preserve runs and backups directories

**Configuration**:
- `SCRIPT_00_KEEP_FILES`: Files to preserve
- `SCRIPT_00_KEEP_DIRS`: Directories to preserve (runs, backups)

---

### Step 1: Collect URLs (`01_collect_urls.py`)
**Purpose**: Navigate the drug register and collect all detail page URLs

**Process**:
1. Open drug register main page
2. Set rows per page to 200 (maximum)
3. Extract detail URLs from current page
4. Navigate to next page
5. Repeat until all pages processed
6. Save URLs to CSV: `north_macedonia_detail_urls.csv`

**Key Features**:
- **Telerik Grid Handling**: Specialized logic for Telerik RadGrid component
- **Pagination**: Automatic page navigation with retry logic
- **Checkpointing**: Resume from last processed page
- **Anti-bot**: Headless mode, image/CSS disabling, stealth profile

**Configuration**:
```json
{
  "SCRIPT_01_ROWS_PER_PAGE": "200",
  "SCRIPT_01_HEADLESS": true,
  "SCRIPT_01_DISABLE_IMAGES": true,
  "SCRIPT_01_DISABLE_CSS": true,
  "SCRIPT_01_CHECKPOINT_JSON": "mk_urls_checkpoint.json"
}
```

**Output**: `output/NorthMacedonia/north_macedonia_detail_urls.csv`

---

### Step 2: Scrape Details (`02_scrape_details.py`)
**Purpose**: Extract detailed drug registration data from each URL

**Process**:
1. Load URLs from Step 1 CSV
2. Launch 7 parallel worker threads (configurable)
3. Each worker:
   - Opens detail page URL
   - Waits for table to load
   - Extracts all registration fields
   - Translates Macedonian text to English
   - Saves to CSV
4. Checkpoint progress after each URL
5. Retry failed URLs up to 3 times

**Key Features**:
- **Multi-threading**: 7 concurrent workers for speed
- **Translation**: Automatic Cyrillic → English translation
- **Reimbursement Logic**: Applies standard reimbursement rules:
  - Reimbursable: Yes
  - Reimbursable Rate: 80.00%
  - Copayment Percent: 20.00%
  - VAT: 5%
  - Margin Rule: "650 PPP & PPI Listed"
- **Error Handling**: Dumps failed HTML for debugging
- **Session Management**: Detects invalid sessions, restarts drivers

**Configuration**:
```json
{
  "SCRIPT_02_DETAIL_WORKERS": 7,
  "SCRIPT_02_HEADLESS": true,
  "SCRIPT_02_SLEEP_BETWEEN_DETAILS": 0.15,
  "SCRIPT_02_PAGELOAD_TIMEOUT": 90,
  "SCRIPT_02_WAIT_SECONDS": 40,
  "SCRIPT_02_MAX_RETRIES": 3,
  "SCRIPT_02_DUMP_FAILED_HTML": true
}
```

**Output**: `output/NorthMacedonia/north_macedonia_drug_register.csv`

**Data Inserted**: `nm_drug_register` table

---

### Step 3: Scrape Zdravstvo (`03_scrape_zdravstvo.py`)
**Purpose**: Extract maximum price data from zdravstvo.gov.mk

**Process**:
1. Navigate to zdravstvo.gov.mk price database
2. Set rows per page to 200
3. For each row in the table:
   - Extract base product information (ATC, name, generic, manufacturer)
   - Click "Price History" link to open modal
   - Extract all price history records (price + date)
   - Close modal
   - Write records to CSV
4. Navigate to next page
5. Repeat until all pages processed

**Key Features**:
- **Modal Handling**: Opens/closes price history modals
- **Translation**: Macedonian → English for all fields
- **Deduplication**: Skips already-scraped records
- **Checkpointing**: Resume from specific page + row
- **Header Filtering**: Removes header rows from price history

**Configuration**:
```json
{
  "SCRIPT_03_ROWS_PER_PAGE": "200",
  "SCRIPT_03_HEADLESS": true,
  "SCRIPT_03_MAX_PAGES": 0,
  "SCRIPT_03_SLEEP_AFTER_ROW": 1.0,
  "SCRIPT_03_SLEEP_AFTER_MODAL_OPEN": 0.2,
  "SCRIPT_03_PAGE_LOAD_TIMEOUT": 90,
  "SCRIPT_03_WAIT_TIMEOUT": 30,
  "SCRIPT_03_CHECKPOINT_JSON": "mk_maxprices_checkpoint.json"
}
```

**Output**: `output/NorthMacedonia/maxprices_output.csv`

**Data Inserted**: `nm_max_prices` table

---

### Step 3a: Parallel Max Prices (`03a_scrape_maxprices_parallel.py`)
**Purpose**: Faster parallel extraction of maximum prices

**Process**: Similar to Step 3 but with multiple worker threads for parallel processing

**Configuration**: Same as Step 3 with additional worker settings

---

## Key Components

### 1. State Machine (`state_machine.py`)
**Purpose**: Deterministic navigation with state validation

**Features**:
- Explicit state definitions (PAGE_LOADED, GRID_READY, TABLE_READY, etc.)
- State validation with required elements
- Retry logic on validation failure
- State transition history tracking

**Example States**:
- `PAGE_LOADED`: Body element present
- `GRID_READY`: Telerik grid + pager visible
- `TABLE_READY`: Table with data rows
- `DETAIL_READY`: Detail page table loaded

**Usage**:
```python
state_machine = NavigationStateMachine(locator, logger)
if state_machine.transition_to(NavigationState.GRID_READY):
    # Grid is ready, proceed
    pass
```

---

### 2. Smart Locator (`smart_locator.py`)
**Purpose**: Intelligent element finding with multiple strategies

**Features**:
- Role-based finding (ARIA roles)
- Label-based finding
- Text-based finding
- CSS selector fallback
- Timeout and retry logic

**Usage**:
```python
locator = SmartLocator(driver, logger)
button = locator.find_element(role="button", text="Search")
```

---

### 3. Scraper Utils (`scraper_utils.py`)
**Purpose**: Shared utilities for resource management and progress tracking

**Features**:
- **Memory Monitoring**: 2GB hard limit, automatic cleanup
- **Chrome Process Management**: Track and kill orphaned processes
- **Progress Tracking**: Thread-safe progress counters
- **Graceful Shutdown**: Signal handlers, atexit cleanup
- **Resource Logging**: Memory and thread usage tracking

**Key Functions**:
- `register_driver(driver)`: Register driver for cleanup
- `check_memory_limit()`: Check if over 2GB limit
- `force_cleanup()`: Force garbage collection
- `kill_tracked_chrome_processes()`: Kill only tracked Chrome PIDs
- `log_progress_with_step()`: Log progress with step details

---

### 4. Config Loader (`config_loader.py`)
**Purpose**: Load configuration from `NorthMacedonia.env.json`

**Features**:
- Environment variable loading
- Type conversion (bool, int, float)
- Path resolution
- Default values

---

### 5. Progress UI (`progress_ui.py`)
**Purpose**: Real-time progress display in terminal

**Features**:
- Live progress bars
- Step status indicators
- ETA calculation
- Resource usage display

---

## Database Schema

### Tables (prefix: `nm_`)

#### 1. `nm_drug_register`
Stores drug registration data from MoH

**Columns**:
- `id`: Serial primary key
- `run_id`: Reference to run_ledger
- `registration_number`: Drug registration number
- `product_name`: Product name (Macedonian)
- `product_name_en`: Product name (English)
- `generic_name`: Generic name (Macedonian)
- `generic_name_en`: Generic name (English)
- `dosage_form`: Dosage form (tablet, capsule, etc.)
- `strength`: Drug strength
- `pack_size`: Package size
- `manufacturer`: Manufacturer name
- `marketing_authorisation_holder`: MAH
- `atc_code`: Anatomical Therapeutic Chemical code
- `source_url`: Source detail page URL
- `scraped_at`: Timestamp

**Indexes**:
- `idx_nm_drug_reg_run` on `run_id`
- `idx_nm_drug_reg_number` on `registration_number`

---

#### 2. `nm_max_prices`
Stores maximum price data from zdravstvo.gov.mk

**Columns**:
- `id`: Serial primary key
- `run_id`: Reference to run_ledger
- `product_name`: Product name (Macedonian)
- `product_name_en`: Product name (English)
- `generic_name`: Generic name (Macedonian)
- `generic_name_en`: Generic name (English)
- `dosage_form`: Dosage form
- `strength`: Drug strength
- `pack_size`: Package size
- `manufacturer`: Manufacturer
- `max_price`: Maximum price (REAL)
- `currency`: Currency (default: MKD)
- `effective_date`: Price effective date
- `source_url`: Source URL
- `scraped_at`: Timestamp

**Indexes**:
- `idx_nm_max_prices_run` on `run_id`
- `idx_nm_max_prices_product` on `product_name`

---

#### 3. `nm_final_output`
EVERSANA format output (merged data)

**Columns**:
- `id`: Serial primary key
- `run_id`: Reference to run_ledger
- `pcid`: Product Code ID
- `country`: Default 'NORTH MACEDONIA'
- `company`: Company name
- `local_product_name`: Local product name
- `generic_name`: Generic name (Macedonian)
- `generic_name_en`: Generic name (English)
- `description`: Product description
- `strength`: Drug strength
- `dosage_form`: Dosage form
- `pack_size`: Package size
- `max_price`: Maximum price
- `currency`: Currency (default: MKD)
- `effective_date`: Effective date
- `registration_number`: Registration number
- `atc_code`: ATC code
- `marketing_authorisation_holder`: MAH
- `source_type`: 'drug_register', 'max_prices', or 'merged'
- `source_url`: Source URL
- `created_at`: Timestamp

**Unique Constraint**: `(run_id, registration_number, product_name, pack_size)`

**Indexes**:
- `idx_nm_final_output_run` on `run_id`
- `idx_nm_final_output_pcid` on `pcid`
- `idx_nm_final_output_reg` on `registration_number`

---

#### 4. `nm_pcid_mappings`
PCID mapping data

**Columns**:
- `id`: Serial primary key
- `run_id`: Reference to run_ledger
- `pcid`: Product Code ID
- `local_pack_code`: Local pack code
- `presentation`: Product presentation
- `product_name`: Product name
- `generic_name`: Generic name
- `manufacturer`: Manufacturer
- `country`: Default 'NORTH MACEDONIA'
- `region`: Default 'EUROPE'
- `currency`: Default 'MKD'
- `max_price`: Maximum price
- `effective_date`: Effective date
- `source`: Default 'PRICENTRIC'
- `mapped_at`: Timestamp

**Unique Constraint**: `(run_id, pcid, local_pack_code)`

**Indexes**:
- `idx_nm_pcid_run` on `run_id`
- `idx_nm_pcid_code` on `pcid`
- `idx_nm_pcid_local` on `local_pack_code`

---

#### 5. `nm_step_progress`
Sub-step resume tracking

**Columns**:
- `id`: Serial primary key
- `run_id`: Reference to run_ledger
- `step_number`: Step number (0-3)
- `step_name`: Step name
- `progress_key`: Progress identifier
- `status`: 'pending', 'in_progress', 'completed', 'failed', 'skipped'
- `error_message`: Error message if failed
- `retry_count`: Number of retries
- `started_at`: Start timestamp
- `completed_at`: Completion timestamp

**Unique Constraint**: `(run_id, step_number, progress_key)`

**Indexes**:
- `idx_nm_progress_run_step` on `(run_id, step_number)`
- `idx_nm_progress_status` on `status`

---

#### 6. `nm_export_reports`
Export report metadata

**Columns**:
- `id`: Serial primary key
- `run_id`: Reference to run_ledger
- `report_type`: Report type
- `file_path`: Export file path
- `row_count`: Number of rows exported
- `export_format`: Format (default: 'db')
- `created_at`: Timestamp

**Indexes**:
- `idx_nm_export_reports_run` on `run_id`
- `idx_nm_export_reports_type` on `report_type`

---

#### 7. `nm_errors`
Error tracking

**Columns**:
- `id`: Serial primary key
- `run_id`: Reference to run_ledger
- `error_type`: Error type
- `error_message`: Error message
- `context`: JSONB context data
- `step_number`: Step number where error occurred
- `step_name`: Step name
- `created_at`: Timestamp

**Indexes**:
- `idx_nm_errors_run` on `run_id`
- `idx_nm_errors_step` on `step_number`
- `idx_nm_errors_type` on `error_type`

---

## Configuration

### File: `config/NorthMacedonia.env.json`

```json
{
  "scraper": {
    "id": "NorthMacedonia",
    "enabled": true
  },
  "config": {
    "BASE_DIR": "",
    "INPUT_DIR": "input",
    "OUTPUT_DIR": "output",
    "BACKUP_DIR": "backups",
    
    // Step 1: URL Collection
    "SCRIPT_01_URLS_CSV": "north_macedonia_detail_urls.csv",
    "SCRIPT_01_CHECKPOINT_JSON": "mk_urls_checkpoint.json",
    "SCRIPT_01_HEADLESS": true,
    "SCRIPT_01_ROWS_PER_PAGE": "200",
    "SCRIPT_01_DISABLE_IMAGES": true,
    "SCRIPT_01_DISABLE_CSS": true,
    
    // Step 2: Detail Scraping
    "SCRIPT_02_OUTPUT_CSV": "north_macedonia_drug_register.csv",
    "SCRIPT_02_DETAIL_WORKERS": 7,
    "SCRIPT_02_HEADLESS": true,
    "SCRIPT_02_SLEEP_BETWEEN_DETAILS": 0.15,
    "SCRIPT_02_DISABLE_IMAGES": true,
    "SCRIPT_02_DISABLE_CSS": true,
    "SCRIPT_02_PAGELOAD_TIMEOUT": 90,
    "SCRIPT_02_WAIT_SECONDS": 40,
    "SCRIPT_02_MAX_RETRIES": 3,
    "SCRIPT_02_DUMP_FAILED_HTML": true,
    
    // Step 3: Max Prices
    "SCRIPT_03_OUTPUT_CSV": "maxprices_output.csv",
    "SCRIPT_03_CHECKPOINT_JSON": "mk_maxprices_checkpoint.json",
    "SCRIPT_03_HEADLESS": true,
    "SCRIPT_03_ROWS_PER_PAGE": "200",
    "SCRIPT_03_DISABLE_IMAGES": true,
    "SCRIPT_03_DISABLE_CSS": true,
    "SCRIPT_03_MAX_PAGES": 0,
    "SCRIPT_03_SLEEP_AFTER_ROW": 1.0,
    "SCRIPT_03_SLEEP_AFTER_MODAL_OPEN": 0.2,
    "SCRIPT_03_PAGE_LOAD_TIMEOUT": 90,
    "SCRIPT_03_WAIT_TIMEOUT": 30,
    
    // Backup & Clean
    "SCRIPT_00_KEEP_FILES": [],
    "SCRIPT_00_KEEP_DIRS": ["runs", "backups"],
    
    "PIPELINE_LOG_FILE_PREFIX": "NorthMacedonia_run_"
  },
  "secrets": {}
}
```

---

## How to Run

### Prerequisites
1. **Python 3.x** installed
2. **PostgreSQL** database accessible
3. **Chrome browser** installed
4. **Dependencies** installed: `pip install -r requirements.txt`
5. **Environment** configured: `config/NorthMacedonia.env.json`

### Full Run (Fresh Start)
```bash
cd "scripts/North Macedonia"
python run_pipeline_resume.py --fresh
```

### Resume from Specific Step
```bash
# Resume from Step 1 (URL collection)
python run_pipeline_resume.py --step 1

# Resume from Step 2 (Detail scraping)
python run_pipeline_resume.py --step 2

# Resume from Step 3 (Max prices)
python run_pipeline_resume.py --step 3
```

### Health Check
```bash
cd "scripts/North Macedonia"
python health_check.py
```

Checks:
- Database connectivity
- URL reachability
- Disk space
- Chrome availability

---

## Technical Details

### Anti-Bot Measures
1. **Stealth Profile**: Removes webdriver property, masks automation
2. **User-Agent Rotation**: Randomized user agents
3. **Human-like Delays**: Random delays between actions
4. **Headless Mode**: Runs without visible browser
5. **Image/CSS Disabling**: Faster loading, less detection
6. **Session Management**: Detects and handles session invalidation

### Resource Management
1. **Memory Limit**: 2GB hard limit with automatic cleanup
2. **Chrome Process Tracking**: Tracks and kills orphaned processes
3. **Driver Registration**: Automatic cleanup on shutdown
4. **Graceful Shutdown**: Signal handlers (SIGINT, SIGTERM)
5. **Atexit Handlers**: Cleanup on normal exit

### Error Handling
1. **Retry Logic**: 3 retries per URL/page with exponential backoff
2. **Session Validation**: Detects invalid sessions, restarts drivers
3. **Failed HTML Dumps**: Saves HTML for debugging failed extractions
4. **Error Logging**: Comprehensive error tracking in `nm_errors` table
5. **Checkpoint Recovery**: Resume from last successful checkpoint

### Translation
- **Library**: deep-translator (GoogleTranslator)
- **Direction**: Macedonian (mk) → English (en)
- **Caching**: Translation results cached to avoid redundant API calls
- **Fallback**: If translation fails, original text is used
- **Fields Translated**: Product names, generic names, dosage forms

### Checkpointing
- **Step 1**: Saves current page number to `mk_urls_checkpoint.json`
- **Step 2**: Tracks processed URLs in CSV
- **Step 3**: Saves page + row index to `mk_maxprices_checkpoint.json`
- **Resume Logic**: Automatically detects and resumes from last checkpoint

---

## Troubleshooting

### Common Issues

#### 1. **zdravstvo.gov.mk Rate Limiting**
**Symptom**: Scraper gets blocked or slow responses

**Solution**:
- Increase delays in config:
  ```json
  "SCRIPT_03_SLEEP_AFTER_ROW": 2.0,
  "SCRIPT_03_SLEEP_AFTER_MODAL_OPEN": 0.5
  ```

#### 2. **Chrome Not Found**
**Symptom**: Error: "Chrome binary not found"

**Solution**:
- Ensure Chrome is installed
- Ensure chromedriver is on PATH
- Check `core/chrome_manager.py` for offline fallback

#### 3. **Stale Checkpoint**
**Symptom**: Scraper resumes from wrong point

**Solution**:
- Use `--fresh` flag to clear and restart:
  ```bash
  python run_pipeline_resume.py --fresh
  ```

#### 4. **Memory Limit Exceeded**
**Symptom**: "Memory usage exceeds limit 2048MB"

**Solution**:
- Reduce worker count:
  ```json
  "SCRIPT_02_DETAIL_WORKERS": 3
  ```
- Increase memory limit in `scraper_utils.py`:
  ```python
  MEMORY_LIMIT_MB = 4096  # 4GB
  ```

#### 5. **Database Connection Failed**
**Symptom**: "Could not connect to PostgreSQL"

**Solution**:
- Check PostgreSQL is running
- Verify connection settings in `.env`
- Test connection: `python verify_db.py`

#### 6. **Translation Errors**
**Symptom**: "Translation failed" warnings

**Solution**:
- Check internet connection (GoogleTranslator requires internet)
- Install deep-translator: `pip install deep-translator`
- Fallback: Original Macedonian text will be used

#### 7. **Orphaned Chrome Processes**
**Symptom**: Multiple Chrome processes remain after scraper stops

**Solution**:
- Run cleanup script:
  ```bash
  python cleanup_lock.py
  ```
- Or manually kill:
  ```bash
  taskkill /F /IM chrome.exe /T
  ```

---

## Performance Metrics

### Typical Run Times
- **Step 0 (Backup)**: ~10 seconds
- **Step 1 (URL Collection)**: ~5-10 minutes (depends on total pages)
- **Step 2 (Detail Scraping)**: ~30-60 minutes (depends on URL count, 7 workers)
- **Step 3 (Max Prices)**: ~20-40 minutes (depends on product count)

### Resource Usage
- **Memory**: ~500MB - 1.5GB (with 7 workers)
- **CPU**: Moderate (multi-threaded)
- **Network**: ~100-500 MB total
- **Disk**: ~50-200 MB (CSV outputs)

### Scalability
- **Workers**: Configurable (1-10 recommended)
- **Memory**: Scales with worker count
- **Speed**: Linear scaling with workers (up to ~10 workers)

---

## Future Enhancements

### Planned Features
1. **AI-Assisted Extraction**: Use LLM for complex field extraction
2. **Proxy Support**: Rotate proxies to avoid rate limiting
3. **Incremental Updates**: Only scrape new/changed records
4. **Data Validation**: Automated data quality checks
5. **Export Formats**: Excel, JSON, XML exports
6. **Monitoring Dashboard**: Real-time progress visualization
7. **Telegram Notifications**: Status updates via Telegram bot

---

## Support & Maintenance

### Logs
- **Location**: `logs/NorthMacedonia/`
- **Format**: Timestamped log files
- **Archive**: Old logs moved to `logs/NorthMacedonia/archive/`

### Backups
- **Location**: `backups/NorthMacedonia/`
- **Format**: Timestamped directories
- **Contents**: CSV files, checkpoints

### Exports
- **Location**: `exports/NorthMacedonia/`
- **Format**: CSV, database tables

### Monitoring
- **Run Metrics**: `cache/run_metrics/NorthMacedonia_*.json`
- **Run Ledger**: `run_ledger` table in PostgreSQL

---

## Contact & Documentation

### Related Files
- **README**: `doc/NorthMacedonia/README.md`
- **Schema**: `sql/schemas/postgres/north_macedonia.sql`
- **Config**: `config/NorthMacedonia.env.json`
- **Requirements**: `requirements/North Macedonia/`

### Key Scripts
- **Pipeline Runner**: `scripts/North Macedonia/run_pipeline_resume.py`
- **URL Collection**: `scripts/North Macedonia/01_collect_urls.py`
- **Detail Scraping**: `scripts/North Macedonia/02_scrape_details.py`
- **Max Prices**: `scripts/North Macedonia/03_scrape_zdravstvo.py`
- **Health Check**: `scripts/North Macedonia/health_check.py`

---

**Last Updated**: 2026-02-12  
**Version**: 1.0  
**Maintainer**: Quad99 Technologies
