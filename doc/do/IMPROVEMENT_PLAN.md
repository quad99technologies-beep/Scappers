# Belarus Scraper - Improvement Plan

**Generated:** 2026-02-12  
**Status:** Recommendations for modernization without changing scraping/business logic

---

## Executive Summary

The Belarus scraper has a solid foundation with good database schema and repository pattern, but lacks several modern features present in recently updated scrapers (North Macedonia, Argentina, Netherlands). This document identifies **missing modules and features** that should be added to improve maintainability, monitoring, and user experience.

**Key Findings:**
- ✅ **Strong Foundation:** Good database schema, repositories, state machine, smart locator
- ✅ **Has test_scraper.py:** Basic testing infrastructure exists
- ❌ **Missing:** Data validation, statistics collection, progress UI, comprehensive health checks
- ⚠️ **Needs Enhancement:** Health checks are minimal, database layer organization

---

## 1. Missing Database Layer Modules

### 1.1 Data Validator Module ❌ MISSING
**Priority:** HIGH

**What's Missing:**
- No `db/validator.py` module for data quality checks
- No validation of scraped data before export
- No automated quality metrics

**Reference Implementation:** `North Macedonia/db/validator.py`

**Recommended Features:**
```python
class DataValidator:
    - validate_run_data(run_id) -> ValidationResult
    - check_required_fields()
    - check_data_completeness()
    - check_price_format()
    - check_atc_codes()
    - check_registration_numbers()
    - detect_duplicates()
    - validate_translation_coverage()
    - validate_pcid_mappings()
    - generate_validation_report()
```

**Benefits:**
- Early detection of data quality issues
- Automated validation reports
- Confidence in export data quality
- Reduced manual QA time
- Better PCID mapping validation

---

### 1.2 Statistics Collector Module ❌ MISSING
**Priority:** HIGH

**What's Missing:**
- No `db/statistics.py` module for metrics collection
- No automated statistics generation
- Limited visibility into scraper performance

**Reference Implementation:** `North Macedonia/db/statistics.py`

**Recommended Features:**
```python
class StatisticsCollector:
    - collect_run_statistics(run_id) -> dict
    - get_scraping_metrics()
    - get_translation_metrics()
    - get_pcid_mapping_metrics()
    - get_error_summary()
    - get_step_performance()
    - get_price_coverage_stats()
    - generate_statistics_report()
    - export_statistics_to_json()
```

**Metrics to Track:**
- Total RCETH records scraped
- PCID mapping success rate
- Translation coverage (dictionary vs AI)
- Error rates by step
- Processing time per step
- Data completeness percentages
- Price coverage (import vs retail)
- ATC code coverage

---

### 1.3 Enhanced Database __init__.py ⚠️ NEEDS IMPROVEMENT
**Priority:** MEDIUM

**Current State:**
```python
# Current: Basic exports
"""Belarus database module."""

from .schema import apply_belarus_schema, BELARUS_SCHEMA_DDL
from .repositories import BelarusRepository

__all__ = [
    "apply_belarus_schema",
    "BELARUS_SCHEMA_DDL",
    "BelarusRepository",
]
```

**Recommended Enhancement:**
```python
"""
Belarus database layer.

Provides PostgreSQL-backed storage for Belarus scraper.
All data operations go through the repository pattern.

Modules:
- repositories: BelarusRepository class with all DB operations
- schema: DDL for by_* tables
- validator: DataValidator for quality checks
- statistics: StatisticsCollector for metrics and reporting
"""

from .repositories import BelarusRepository
from .schema import apply_belarus_schema, BELARUS_SCHEMA_DDL
from .validator import DataValidator
from .statistics import StatisticsCollector

__all__ = [
    "BelarusRepository",
    "apply_belarus_schema",
    "BELARUS_SCHEMA_DDL",
    "DataValidator",
    "StatisticsCollector",
]
```

---

## 2. Missing User Interface Features

### 2.1 Progress UI Module ❌ MISSING
**Priority:** HIGH

**What's Missing:**
- No visual progress tracking during scraping
- Users must monitor console output only
- No real-time statistics display
- Tor Browser scraping is slow - progress UI would help

**Reference Implementation:** `North Macedonia/progress_ui.py`

**Recommended Features:**
```python
class ProgressUI:
    - show_step_progress(step_num, step_name, total_steps)
    - update_progress_bar(current, total)
    - display_live_statistics()
    - show_error_summary()
    - display_eta()
    - show_tor_connection_status()  # Belarus-specific
    - show_completion_summary()
```

**Benefits:**
- Better user experience during long Tor Browser scraping
- Real-time visibility into scraper progress
- Easier to identify when scraper is stuck
- Professional appearance
- Tor connection monitoring

---

## 3. Enhanced Testing Infrastructure

### 3.1 Database Layer Tests ❌ MISSING
**Priority:** MEDIUM

**Current State:**
- ✅ Has `test_scraper.py` for basic scraper testing
- ❌ No database layer testing

**What's Missing:**
- No `test_db_layer.py` for testing repositories
- No automated testing of database operations
- Manual verification required

**Reference Implementation:** `North Macedonia/test_db_layer.py`

**Recommended Test Coverage:**
```python
# test_db_layer.py
- test_schema_creation()
- test_repository_insert_operations()
- test_repository_query_operations()
- test_step_progress_tracking()
- test_error_logging()
- test_data_validation()
- test_statistics_collection()
- test_pcid_mapping_operations()
- test_translation_data_operations()
```

---

### 3.2 Schema Verification ❌ MISSING
**Priority:** LOW

**What's Missing:**
- No `check_schema.py` utility
- No automated schema validation

**Reference Implementation:** `North Macedonia/check_schema.py`

**Recommended Features:**
```python
# check_schema.py
- verify_all_tables_exist()
- verify_indexes_exist()
- verify_foreign_keys()
- check_column_types()
- validate_constraints()
- check_migration_status()
```

---

## 4. Enhanced Health Check System

### 4.1 Current Health Check ⚠️ MINIMAL - NEEDS MAJOR ENHANCEMENT
**Priority:** HIGH

**Current Features:** ✅
- Database connectivity check
- Disk space check
- Chrome availability check

**Missing Features:**
- ❌ No Tor Browser check (critical for Belarus!)
- ❌ No RCETH website reachability check
- ❌ No selector validation
- ❌ No input dictionary verification
- ❌ No validator module check
- ❌ No statistics module check
- ❌ No memory check
- ❌ No Tor proxy check

**Recommended Additions:**
```python
def check_tor_browser() -> Tuple[bool, str]:
    """Check Tor Browser availability (critical for Belarus)."""
    try:
        from selenium import webdriver
        from selenium.webdriver.firefox.options import Options
        
        options = Options()
        options.binary_location = "path/to/tor/browser"
        options.add_argument("--headless")
        
        driver = webdriver.Firefox(options=options)
        driver.quit()
        return True, "Tor Browser is available"
    except Exception as e:
        return False, f"Tor Browser not available: {e}"

def check_tor_proxy() -> Tuple[bool, str]:
    """Check if Tor proxy is running."""
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('127.0.0.1', 9150))  # Tor Browser proxy port
        sock.close()
        if result == 0:
            return True, "Tor proxy is running on port 9150"
        return False, "Tor proxy not running on port 9150"
    except Exception as e:
        return False, f"Tor proxy check failed: {e}"

def check_rceth_website() -> Tuple[bool, str]:
    """Check RCETH website reachability through Tor."""
    try:
        import requests
        proxies = {
            'http': 'socks5h://127.0.0.1:9150',
            'https': 'socks5h://127.0.0.1:9150'
        }
        response = requests.get(
            "http://www.rceth.by/",
            proxies=proxies,
            timeout=30
        )
        if response.status_code == 200:
            return True, f"RCETH website reachable (HTTP {response.status_code})"
        return False, f"RCETH website returned HTTP {response.status_code}"
    except Exception as e:
        return False, f"RCETH website unreachable: {e}"

def check_input_dictionary_table() -> Tuple[bool, str]:
    """Verify by_input_dictionary input table exists."""
    try:
        from core.db.connection import CountryDB
        with CountryDB("Belarus") as db:
            with db.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM by_input_dictionary")
                count = cur.fetchone()[0] or 0
        return True, f"by_input_dictionary accessible ({count} rows)"
    except Exception as e:
        return False, f"by_input_dictionary: {e}"

def check_validator_available() -> Tuple[bool, str]:
    """Check if DataValidator module is available."""
    try:
        from db.validator import DataValidator
        return True, "DataValidator module available"
    except ImportError:
        return False, "DataValidator module not found"

def check_statistics_available() -> Tuple[bool, str]:
    """Check if StatisticsCollector module is available."""
    try:
        from db.statistics import StatisticsCollector
        return True, "StatisticsCollector module available"
    except ImportError:
        return False, "StatisticsCollector module not found"

def check_memory() -> Tuple[bool, str]:
    """Check available system memory."""
    import psutil
    mem = psutil.virtual_memory()
    if mem.available < 1024**3:  # Less than 1GB
        return False, f"Low memory: {mem.available/(1024**3):.1f}GB available"
    return True, f"Memory OK: {mem.available/(1024**3):.1f}GB available"
```

**Enhanced Health Check Structure:**
```python
checks = [
    ("Config", "PostgreSQL (run_ledger)", check_database),
    ("Config", "Input table by_input_dictionary", check_input_dictionary_table),
    ("Config", "Disk Space", check_disk_space),
    ("Config", "System Memory", check_memory),
    ("Browser", "Chrome", check_chrome),
    ("Browser", "Tor Browser", check_tor_browser),
    ("Browser", "Tor Proxy", check_tor_proxy),
    ("Website", "RCETH website reachable", check_rceth_website),
    ("Modules", "DataValidator", check_validator_available),
    ("Modules", "StatisticsCollector", check_statistics_available),
]
```

---

## 5. Missing Utility Scripts

### 5.1 Migration Helper ❌ MISSING
**Priority:** LOW

**What's Missing:**
- No migration framework
- Schema changes require manual SQL

**Recommended Addition:**
```python
# migrate_schema.py
"""
Generic schema migration helper for Belarus scraper.
Handles adding new columns, indexes, and tables safely.
"""

def migrate_add_validator_tables():
    """Add validation result tables."""
    
def migrate_add_statistics_tables():
    """Add statistics tables."""
    
def migrate_add_missing_indexes():
    """Add any missing indexes for performance."""
    
def run_all_migrations():
    """Run all pending migrations."""
```

---

### 5.2 Statistics and Validation Script ❌ MISSING
**Priority:** MEDIUM

**What's Missing:**
- No standalone script to run validation and generate statistics
- Must be integrated into pipeline

**Reference Implementation:** `Argentina/08_stats_and_validation.py`

**Recommended Features:**
```python
# 05_stats_and_validation.py
"""
Generate statistics and validation reports for Belarus scraper.
Can be run standalone or as part of pipeline.
"""

def main():
    - Load run_id
    - Run data validation
    - Generate statistics
    - Create validation report
    - Create statistics report
    - Export to JSON/CSV
    - Display summary
    - Check PCID mapping quality
    - Validate translation coverage
```

---

## 6. Database Schema Enhancements

### 6.1 Add Validation Results Table ❌ MISSING
**Priority:** MEDIUM

**Recommended Schema:**
```sql
CREATE TABLE IF NOT EXISTS by_validation_results (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    validation_type TEXT NOT NULL,
    status TEXT CHECK(status IN ('pass', 'warning', 'fail')),
    message TEXT,
    details JSONB,
    checked_count INTEGER DEFAULT 0,
    failed_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_by_validation_run ON by_validation_results(run_id);
CREATE INDEX IF NOT EXISTS idx_by_validation_status ON by_validation_results(status);
```

---

### 6.2 Add Statistics Table ❌ MISSING
**Priority:** MEDIUM

**Recommended Schema:**
```sql
CREATE TABLE IF NOT EXISTS by_statistics (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    metric_name TEXT NOT NULL,
    metric_value NUMERIC,
    metric_unit TEXT,
    category TEXT,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, metric_name)
);
CREATE INDEX IF NOT EXISTS idx_by_statistics_run ON by_statistics(run_id);
CREATE INDEX IF NOT EXISTS idx_by_statistics_category ON by_statistics(category);
```

---

## 7. Pipeline Runner Enhancements

### 7.1 Add Statistics and Validation Step ⚠️ RECOMMENDED
**Priority:** MEDIUM

**Current Pipeline:**
```
Step 0: Backup and Clean
Step 1: Extract RCETH Data
Step 2: Map PCIDs
Step 3: Format for Export
Step 4: Process and Translate
```

**Recommended Addition:**
```
Step 5: Statistics and Validation
  - Run data validation checks
  - Generate statistics
  - Create validation report
  - Create statistics report
  - Flag data quality issues
  - Validate PCID mappings
  - Check translation coverage
```

**Implementation in `run_pipeline_resume.py`:**
```python
# Update TOTAL_STEPS
# Current: 5 steps (0-4)
# Recommended: 6 steps (0-5)

# Add new step
run_step(
    step_num=5,
    script_name="05_stats_and_validation.py",
    step_name="Statistics and Validation",
    output_files=["validation_report.json", "statistics_report.json"]
)
```

---

## 8. Documentation Enhancements

### 8.1 Add Module Documentation ⚠️ RECOMMENDED
**Priority:** LOW

**Missing Documentation:**
- No README.md in Belarus directory
- No module-level documentation
- No architecture diagram
- No Tor Browser setup guide

**Recommended Files:**
```
Belarus/
├── README.md                    # Overview, setup, usage
├── ARCHITECTURE.md              # System design, data flow
├── TROUBLESHOOTING.md           # Common issues and solutions
├── TOR_SETUP.md                 # Tor Browser configuration guide
└── db/
    └── README.md                # Database layer documentation
```

---

## 9. Belarus-Specific Considerations

### 9.1 Tor Browser Monitoring ⚠️ CRITICAL
**Priority:** HIGH

**Current State:**
- Belarus scraper uses Tor Browser for RCETH website
- No monitoring of Tor connection health
- No automatic Tor restart on failure

**Recommended Enhancements:**
```python
# tor_monitor.py
"""
Monitor Tor Browser connection health for Belarus scraper.
"""

class TorMonitor:
    - check_tor_connection()
    - verify_tor_circuit()
    - restart_tor_if_needed()
    - get_tor_ip_address()
    - log_tor_metrics()
```

**Integration Points:**
- Health check system
- Pipeline runner (pre-flight check)
- Scraper error handling

---

### 9.2 RCETH Website Change Detection ⚠️ RECOMMENDED
**Priority:** MEDIUM

**Recommended Feature:**
```python
# rceth_layout_validator.py
"""
Validate RCETH website layout hasn't changed.
"""

def validate_rceth_selectors():
    """Check critical selectors still exist."""
    
def validate_data_structure():
    """Verify data table structure."""
    
def generate_layout_report():
    """Create layout validation report."""
```

---

## 10. Implementation Priority

### Phase 1: Critical Infrastructure (Week 1)
1. **Enhanced health checks** (Tor Browser, Tor proxy, RCETH website)
2. **DataValidator module** (`db/validator.py`)
3. **StatisticsCollector module** (`db/statistics.py`)
4. **Update db/__init__.py** to export new modules
5. **Add validation and statistics tables** to schema

### Phase 2: User Experience (Week 2)
6. **ProgressUI module** (`progress_ui.py`) with Tor monitoring
7. **Statistics and validation script** (`05_stats_and_validation.py`)
8. **Tor monitor module** (`tor_monitor.py`)

### Phase 3: Testing and Quality (Week 3)
9. **Database layer tests** (`test_db_layer.py`)
10. **Schema verification** (`check_schema.py`)
11. **Migration helper** (`migrate_schema.py`)
12. **RCETH layout validator** (`rceth_layout_validator.py`)

### Phase 4: Documentation (Week 4)
13. **README.md** and documentation files
14. **TOR_SETUP.md** for Tor Browser configuration
15. **Code comments and docstrings**
16. **Architecture diagram**

---

## 11. Estimated Impact

### Before Implementation:
- ❌ No automated data validation
- ❌ No statistics generation
- ❌ No visual progress tracking
- ❌ Minimal health checks (missing Tor checks!)
- ❌ No Tor connection monitoring
- ❌ Limited automated testing

### After Implementation:
- ✅ Automated data quality validation
- ✅ Comprehensive statistics and metrics
- ✅ Real-time progress visualization
- ✅ Comprehensive health monitoring (including Tor)
- ✅ Tor connection health monitoring
- ✅ Automated testing coverage
- ✅ Better documentation
- ✅ Easier troubleshooting
- ✅ Professional user experience
- ✅ RCETH website change detection

---

## 12. Files to Create

### New Files (14 total):
```
Belarus/
├── db/
│   ├── validator.py              # NEW - Data validation
│   ├── statistics.py             # NEW - Statistics collection
│   └── __init__.py               # UPDATE - Export new modules
├── progress_ui.py                # NEW - Progress visualization
├── tor_monitor.py                # NEW - Tor connection monitoring
├── rceth_layout_validator.py    # NEW - Website change detection
├── test_db_layer.py              # NEW - Database tests
├── check_schema.py               # NEW - Schema verification
├── migrate_schema.py             # NEW - Migration helper
├── 05_stats_and_validation.py   # NEW - Stats/validation script
├── README.md                     # NEW - Documentation
├── ARCHITECTURE.md               # NEW - Architecture docs
├── TROUBLESHOOTING.md            # NEW - Troubleshooting guide
├── TOR_SETUP.md                  # NEW - Tor setup guide
└── db/README.md                  # NEW - DB layer docs
```

### Files to Update (3 total):
```
Belarus/
├── db/schema.py                  # UPDATE - Add new tables
├── health_check.py               # UPDATE - Major enhancement
└── run_pipeline_resume.py        # UPDATE - Add step 5
```

---

## 13. Success Metrics

After implementation, the Belarus scraper should have:

1. **Code Quality:**
   - ✅ 100% database operations tested
   - ✅ Automated validation on every run
   - ✅ Comprehensive error tracking

2. **User Experience:**
   - ✅ Real-time progress visualization
   - ✅ Automated statistics reports
   - ✅ Clear validation feedback
   - ✅ Tor connection status monitoring

3. **Maintainability:**
   - ✅ Complete documentation
   - ✅ Modular architecture
   - ✅ Easy to troubleshoot
   - ✅ Tor Browser setup guide

4. **Reliability:**
   - ✅ Comprehensive health checks (including Tor)
   - ✅ Early error detection
   - ✅ Data quality assurance
   - ✅ Website change detection
   - ✅ Tor connection monitoring

---

## Notes

- **No changes to scraping logic** - All improvements are infrastructure/tooling
- **No changes to business logic** - Data processing remains unchanged
- **Backward compatible** - Existing functionality preserved
- **Based on proven patterns** - All recommendations from working scrapers
- **Incremental implementation** - Can be done in phases
- **Belarus-specific features** - Tor monitoring and RCETH validation are unique to Belarus

---

**End of Belarus Improvement Plan**
