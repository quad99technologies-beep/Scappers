# North Macedonia Scraper - Understanding Summary

## 🎯 Quick Overview

The **North Macedonia Scraper** collects pharmaceutical drug data from North Macedonian government websites:
1. **Drug Register** (Ministry of Health) - Registration details
2. **Zdravstvo.gov.mk** - Maximum price data

**Current Status**: Functional but uses CSV-based workflow. Needs modernization.

---

## 📊 Current Architecture

### Pipeline Flow
```
Step 0: Backup & Clean → Step 1: Collect URLs → Step 2: Scrape Details → Step 3: Max Prices
     (CSV backup)         (CSV output)           (CSV output)          (CSV output)
```

### Data Flow (Current - CSV-Based)
```
1. Collect URLs → north_macedonia_detail_urls.csv
2. Scrape Details → north_macedonia_drug_register.csv  
3. Max Prices → maxprices_output.csv
4. Manual merge/processing
```

### Database Tables (Already Defined)
- ✅ `nm_drug_register` - Drug registration data
- ✅ `nm_max_prices` - Maximum price data
- ✅ `nm_final_output` - EVERSANA format (merged)
- ✅ `nm_pcid_mappings` - PCID mapping
- ✅ `nm_step_progress` - Sub-step tracking
- ✅ `nm_export_reports` - Export metadata
- ✅ `nm_errors` - Error tracking

**Schema exists but scripts write to CSV instead of DB!**

---

## 🔧 Current Implementation Details

### Step 1: Collect URLs (`01_collect_urls.py`)
**What it does**:
- Navigates Telerik grid pagination
- Extracts detail page URLs
- Saves to CSV with checkpointing

**Key Features**:
- ✅ Multi-worker parallel processing (NUM_WORKERS)
- ✅ Checkpoint resume (JSON checkpoint file)
- ✅ Page-level tracking
- ✅ State machine validation
- ✅ Chrome instance management
- ❌ **Writes to CSV** instead of database

**Configuration**:
```json
"SCRIPT_01_ROWS_PER_PAGE": "200",
"SCRIPT_01_HEADLESS": true,
"SCRIPT_01_CHECKPOINT_JSON": "mk_urls_checkpoint.json",
"SCRIPT_01_URLS_CSV": "north_macedonia_detail_urls.csv"
```

---

### Step 2: Scrape Details (`02_scrape_details.py`)
**What it does**:
- Reads URLs from CSV (Step 1 output)
- Extracts drug registration details
- Translates Macedonian → English
- Saves to CSV

**Key Features**:
- ✅ Multi-threaded (7 workers default)
- ✅ Translation support (deep-translator)
- ✅ Retry logic (3 retries per URL)
- ✅ Session validation
- ✅ Chrome PID tracking
- ✅ Failed HTML dumps for debugging
- ❌ **Reads from CSV, writes to CSV** instead of database

**Configuration**:
```json
"SCRIPT_02_DETAIL_WORKERS": 7,
"SCRIPT_02_HEADLESS": true,
"SCRIPT_02_MAX_RETRIES": 3,
"SCRIPT_02_OUTPUT_CSV": "north_macedonia_drug_register.csv"
```

---

### Step 3: Max Prices (`03_scrape_zdravstvo.py`)
**What it does**:
- Scrapes zdravstvo.gov.mk price database
- Opens price history modals
- Extracts price + date records
- Saves to CSV

**Key Features**:
- ✅ Modal handling
- ✅ Translation
- ✅ Checkpointing (page + row)
- ✅ Deduplication
- ❌ **Writes to CSV** instead of database
- ❌ **Should be removed** per your requirements

**Configuration**:
```json
"SCRIPT_03_ROWS_PER_PAGE": "200",
"SCRIPT_03_CHECKPOINT_JSON": "mk_maxprices_checkpoint.json",
"SCRIPT_03_OUTPUT_CSV": "maxprices_output.csv"
```

---

## 🚨 Issues & Improvement Requirements

### Your Requirements (from feedback):
1. ❌ **Remove CSV dependencies** → Use database directly
2. ❌ **Add PCID mapping** → Not currently implemented
3. ❌ **Remove last max price step** → Step 3/3a should be removed
4. ✅ **Update env file** → Needs new DB-focused config
5. ⚠️ **Step management** → Needs improvement
6. ⚠️ **Chrome instance management** → Partially implemented, needs enhancement
7. ❌ **Crash/network failure management** → Basic retry, needs improvement
8. ⚠️ **Progress tracking** → Exists but not DB-based
9. ❌ **Better and fast data retrieve** → CSV reads are slow
10. ❌ **All DB no CSV** → Currently all CSV

---

## 🎯 Modernization Plan

### Phase 1: Database Integration
**Remove CSV, use PostgreSQL directly**

#### Step 1: Collect URLs (Modernized)
```python
# OLD: Write to CSV
append_urls(urls_path, rows, lock=csv_lock)

# NEW: Write to database
db.insert_urls(run_id, urls, page_num)
```

**Changes**:
- Remove `north_macedonia_detail_urls.csv`
- Store URLs in `nm_step_progress` or new `nm_urls` table
- Checkpoint in database, not JSON file
- Query unprocessed URLs from DB

#### Step 2: Scrape Details (Modernized)
```python
# OLD: Read from CSV, write to CSV
df_urls = pd.read_csv(urls_path)
append_rows_to_csv(output_path, [row], out_columns)

# NEW: Read from DB, write to DB
urls = db.get_pending_urls(run_id)
db.insert_drug_register(run_id, row)
```

**Changes**:
- Remove `north_macedonia_drug_register.csv`
- Insert directly to `nm_drug_register` table
- Query pending URLs from database
- Update progress in `nm_step_progress`

#### Step 3: Remove Max Prices
**Action**: Delete `03_scrape_zdravstvo.py` and `03a_scrape_maxprices_parallel.py`

**Rationale**: Per your requirement to remove last max price step

---

### Phase 2: PCID Mapping
**Add PCID mapping step**

```python
# New Step 3: PCID Mapping
def map_pcids(run_id):
    # Load PCID mapping file
    pcid_df = pd.read_excel("North Macedonia_PCID Mapping_20251215.xlsx")
    
    # Query drug register data
    drugs = db.get_drug_register(run_id)
    
    # Match and map
    for drug in drugs:
        pcid = match_pcid(drug, pcid_df)
        db.insert_pcid_mapping(run_id, drug, pcid)
    
    # Insert to final output
    db.insert_final_output(run_id, merged_data)
```

**Table**: `nm_pcid_mappings` (already exists in schema)

---

### Phase 3: Enhanced Management

#### Chrome Instance Management
**Current Issues**:
- Multiple workers create multiple Chrome instances
- Orphaned processes on crash
- Memory leaks

**Improvements**:
```python
class ChromePool:
    def __init__(self, max_instances=7):
        self.pool = []
        self.max_instances = max_instances
        self.lock = threading.Lock()
    
    def get_driver(self):
        with self.lock:
            if len(self.pool) < self.max_instances:
                driver = build_driver()
                self.pool.append(driver)
                return driver
            return self.pool[0]  # Reuse
    
    def cleanup(self):
        for driver in self.pool:
            shutdown_driver(driver)
```

#### Crash/Network Failure Management
**Current**: Basic retry (3 attempts)

**Improvements**:
```python
@retry(
    max_attempts=5,
    backoff=exponential_backoff,
    exceptions=(NetworkError, TimeoutError, SessionError)
)
def scrape_with_recovery(url):
    # Automatic retry with exponential backoff
    # Session recovery on failure
    # Network error detection
    pass
```

**Features**:
- Exponential backoff (1s, 2s, 4s, 8s, 16s)
- Session recovery (restart driver)
- Network error detection (check connectivity)
- Fallback to different Chrome profile

#### Progress Tracking (DB-Based)
**Current**: In-memory counters + console logs

**Improvements**:
```python
# Update progress in database
db.update_step_progress(
    run_id=run_id,
    step_number=2,
    progress_key=f"url_{url_id}",
    status="completed",
    items_processed=1
)

# Query progress
progress = db.get_step_progress(run_id, step_number=2)
print(f"Progress: {progress.completed}/{progress.total}")
```

**Table**: `nm_step_progress` (already exists)

---

### Phase 4: Fast Data Retrieval

#### Current Issues:
- CSV reads are slow for large files
- No indexing
- Full file scans

#### Improvements:
```python
# OLD: CSV scan
df = pd.read_csv("north_macedonia_detail_urls.csv")
pending = df[df["detailed_view_scraped"] == "no"]

# NEW: Indexed DB query
pending_urls = db.execute("""
    SELECT url_id, detail_url 
    FROM nm_urls 
    WHERE run_id = %s 
    AND status = 'pending'
    ORDER BY url_id
    LIMIT 1000
""", (run_id,))
```

**Benefits**:
- ✅ Indexed queries (milliseconds vs seconds)
- ✅ Pagination support
- ✅ Concurrent access
- ✅ ACID guarantees

---

## 📋 Updated Configuration (Proposed)

### New `NorthMacedonia.env.json`
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
    
    // Database settings
    "USE_DATABASE": true,
    "DB_BATCH_SIZE": 100,
    "DB_COMMIT_INTERVAL": 50,
    
    // Step 1: URL Collection
    "SCRIPT_01_HEADLESS": true,
    "SCRIPT_01_ROWS_PER_PAGE": "200",
    "SCRIPT_01_DISABLE_IMAGES": true,
    "SCRIPT_01_DISABLE_CSS": true,
    "SCRIPT_01_WORKERS": 5,
    
    // Step 2: Detail Scraping
    "SCRIPT_02_DETAIL_WORKERS": 7,
    "SCRIPT_02_HEADLESS": true,
    "SCRIPT_02_SLEEP_BETWEEN_DETAILS": 0.15,
    "SCRIPT_02_PAGELOAD_TIMEOUT": 90,
    "SCRIPT_02_WAIT_SECONDS": 40,
    "SCRIPT_02_MAX_RETRIES": 5,
    "SCRIPT_02_EXPONENTIAL_BACKOFF": true,
    
    // Step 3: PCID Mapping (NEW)
    "SCRIPT_03_PCID_MAPPING_FILE": "North Macedonia_PCID Mapping_20251215.xlsx",
    "SCRIPT_03_FUZZY_MATCH_THRESHOLD": 0.85,
    
    // Chrome Management
    "CHROME_MAX_INSTANCES": 7,
    "CHROME_RESTART_AFTER_REQUESTS": 100,
    "CHROME_MEMORY_LIMIT_MB": 500,
    
    // Network & Crash Management
    "NETWORK_TIMEOUT": 30,
    "NETWORK_RETRY_DELAY": 5,
    "SESSION_RECOVERY_ENABLED": true,
    "AUTO_RESTART_ON_CRASH": true,
    
    // Progress Tracking
    "PROGRESS_UPDATE_INTERVAL": 10,
    "PROGRESS_TELEGRAM_ENABLED": true,
    "PROGRESS_DB_COMMIT_INTERVAL": 50,
    
    // Removed CSV settings
    // "SCRIPT_01_URLS_CSV": "north_macedonia_detail_urls.csv",  // REMOVED
    // "SCRIPT_02_OUTPUT_CSV": "north_macedonia_drug_register.csv",  // REMOVED
    // "SCRIPT_03_OUTPUT_CSV": "maxprices_output.csv",  // REMOVED
    
    "PIPELINE_LOG_FILE_PREFIX": "NorthMacedonia_run_"
  },
  "secrets": {}
}
```

---

## 🔄 Modernized Pipeline Flow

### New Architecture
```
┌─────────────────────────────────────────────────────────┐
│                  Pipeline Runner                        │
│            (run_pipeline_resume.py)                     │
└────────────┬────────────────────────────────────────────┘
             │
             ├─► Step 0: Backup & Clean
             │   └─► Backup DB tables, initialize schema
             │
             ├─► Step 1: Collect URLs
             │   ├─► Navigate Telerik grid
             │   ├─► Extract URLs
             │   └─► INSERT INTO nm_urls (run_id, url, page_num)
             │
             ├─► Step 2: Scrape Details
             │   ├─► SELECT * FROM nm_urls WHERE status='pending'
             │   ├─► Extract drug data
             │   ├─► Translate to English
             │   └─► INSERT INTO nm_drug_register
             │
             └─► Step 3: PCID Mapping (NEW)
                 ├─► SELECT * FROM nm_drug_register
                 ├─► Load PCID mapping file
                 ├─► Match products to PCIDs
                 ├─► INSERT INTO nm_pcid_mappings
                 └─► INSERT INTO nm_final_output (EVERSANA format)
```

### Data Flow (Modernized - DB-Based)
```
1. Collect URLs → nm_urls table
2. Scrape Details → nm_drug_register table
3. PCID Mapping → nm_pcid_mappings + nm_final_output tables
4. Export → CSV/Excel from nm_final_output (optional)
```

---

## 📊 Key Components (Current State)

### 1. State Machine (`state_machine.py`)
- ✅ Deterministic navigation
- ✅ State validation
- ✅ Retry logic
- **Status**: Good, keep as-is

### 2. Smart Locator (`smart_locator.py`)
- ✅ Multi-strategy element finding
- ✅ ARIA role support
- **Status**: Good, keep as-is

### 3. Scraper Utils (`scraper_utils.py`)
- ✅ Memory monitoring (2GB limit)
- ✅ Chrome PID tracking
- ✅ Graceful shutdown
- ⚠️ **Needs**: Enhanced Chrome pool management

### 4. Config Loader (`config_loader.py`)
- ✅ JSON config loading
- ✅ Type conversion
- **Status**: Good, update config schema

### 5. Progress UI (`progress_ui.py`)
- ✅ Terminal progress bars
- ⚠️ **Needs**: DB-based progress tracking

---

## 🎯 Implementation Priority

### High Priority (Critical)
1. **Remove CSV dependencies** - Migrate to database
2. **Add PCID mapping step** - Core requirement
3. **Update env configuration** - New DB-focused settings
4. **Enhanced error handling** - Network/crash recovery

### Medium Priority (Important)
5. **Chrome pool management** - Prevent memory leaks
6. **DB-based progress tracking** - Better resume capability
7. **Step management improvements** - Cleaner pipeline flow

### Low Priority (Nice to have)
8. **Performance optimization** - Faster data retrieval
9. **Monitoring dashboard** - Real-time progress visualization
10. **Export enhancements** - Multiple format support

---

## 📝 Next Steps

### Immediate Actions:
1. **Create database helper module** (`db_helper.py`)
   - Connection pooling
   - CRUD operations for all tables
   - Transaction management

2. **Refactor Step 1** (`01_collect_urls.py`)
   - Remove CSV writes
   - Add database inserts
   - Update checkpoint to DB

3. **Refactor Step 2** (`02_scrape_details.py`)
   - Remove CSV reads/writes
   - Query URLs from database
   - Insert to `nm_drug_register`

4. **Create Step 3** (`03_map_pcids.py`)
   - Load PCID mapping file
   - Match products
   - Insert to `nm_pcid_mappings` and `nm_final_output`

5. **Delete old Step 3** (`03_scrape_zdravstvo.py`, `03a_scrape_maxprices_parallel.py`)

6. **Update configuration** (`NorthMacedonia.env.json`)
   - Remove CSV settings
   - Add DB settings
   - Add Chrome management settings

---

## 🔍 Current Strengths

✅ **Well-structured codebase**
✅ **Comprehensive error handling**
✅ **State machine for robust navigation**
✅ **Multi-threaded for performance**
✅ **Translation support**
✅ **Chrome PID tracking**
✅ **Telegram notifications**
✅ **Database schema already defined**

---

## 🚧 Current Weaknesses

❌ **CSV-based workflow** (slow, not scalable)
❌ **No PCID mapping**
❌ **Unnecessary max price step**
❌ **Limited crash recovery**
❌ **No Chrome instance pooling**
❌ **In-memory progress tracking**
❌ **Slow data retrieval from CSV**

---

## 📚 Key Files Reference

### Scripts
- `run_pipeline_resume.py` - Main pipeline orchestrator
- `01_collect_urls.py` - URL collection (needs DB migration)
- `02_scrape_details.py` - Detail scraping (needs DB migration)
- `03_scrape_zdravstvo.py` - Max prices (TO BE REMOVED)
- `state_machine.py` - Navigation state management
- `smart_locator.py` - Element finding
- `scraper_utils.py` - Utilities
- `config_loader.py` - Configuration

### Configuration
- `config/NorthMacedonia.env.json` - Main config
- `sql/schemas/postgres/north_macedonia.sql` - Database schema

### Documentation
- `doc/NorthMacedonia/README.md` - Basic runbook
- `doc/NorthMacedonia/SCRAPER_OVERVIEW.md` - Detailed overview
- `doc/NorthMacedonia/UNDERSTANDING_SUMMARY.md` - This file

---

**Last Updated**: 2026-02-12  
**Status**: Analysis Complete - Ready for Modernization  
**Maintainer**: Quad99 Technologies
