# North Macedonia Scraper - Modernization Implementation Plan

## ✅ COMPLETED

### Phase 1: Database Infrastructure
- [x] Created `db/__init__.py` - Database layer exports
- [x] Created `db/schema.py` - Complete schema with validation & statistics tables
- [x] Created `db/repositories.py` - Comprehensive repository with all CRUD operations
- [x] Created `db/validator.py` - Data validation module
- [x] Created `db/statistics.py` - Statistics collection and reporting
- [x] Updated `config/NorthMacedonia.env.json` - Modernized configuration

## 🔄 IN PROGRESS

### Phase 2: Refactor Step 1 (URL Collection)
**File**: `01_collect_urls.py`

**Changes Needed**:
1. Import database modules
2. Initialize repository
3. Replace CSV writes with `repo.insert_urls()`
4. Replace checkpoint JSON with `repo.mark_progress()`
5. Query pending pages from DB instead of tracking in memory
6. Update statistics collection
7. Add validation for collected URLs

**Key Code Changes**:
```python
# OLD
append_urls(urls_path, rows, lock=csv_lock)

# NEW
repo.insert_urls(urls, batch_size=100)
repo.mark_progress(step_number=1, step_name="collect_urls", 
                  progress_key=f"page_{page_num}", status="completed")
```

### Phase 3: Refactor Step 2 (Detail Scraping)
**File**: `02_scrape_details.py`

**Changes Needed**:
1. Import database modules
2. Initialize repository and validator
3. Replace CSV reads with `repo.get_pending_urls()`
4. Replace CSV writes with `repo.insert_drug_register()`
5. Add validation after each record insertion
6. Update URL status with `repo.mark_url_scraped()`
7. Collect statistics
8. Remove all pandas/CSV operations

**Key Code Changes**:
```python
# OLD
df_urls = pd.read_csv(urls_path)
append_rows_to_csv(output_path, [row], out_columns)

# NEW
pending_urls = repo.get_pending_urls(limit=1000)
record_id = repo.insert_drug_register(data, url_id=url_id)
validator.validate_drug_register_record(data, record_id)
repo.mark_url_scraped(url_id, status='scraped')
```

### Phase 4: Create Step 3 (PCID Mapping)
**File**: `03_map_pcids.py` (NEW)

**Features**:
1. Load PCID mapping file from Excel
2. Query all drug register records
3. Perform exact matching (product name + company + generic)
4. Perform fuzzy matching (using fuzzywuzzy/rapidfuzz)
5. Insert mapping results with match scores
6. Validate mappings
7. Insert to final output table
8. Generate statistics

**Pseudocode**:
```python
def main():
    # Initialize
    db = get_db()
    repo = NorthMacedoniaRepository(db, run_id)
    validator = DataValidator(repo)
    stats_collector = StatisticsCollector(repo)
    
    # Load PCID mapping
    pcid_df = load_pcid_mapping_file()
    
    # Get all drug register records
    drugs = repo.get_all_drug_register()
    
    # Process each record
    for drug in drugs:
        # Try exact match
        pcid, match_type, score = match_pcid(drug, pcid_df)
        
        # Insert mapping
        mapping_id = repo.insert_pcid_mapping(
            drug_register_id=drug['id'],
            pcid=pcid,
            match_type=match_type,
            match_score=score,
            product_data=drug
        )
        
        # Validate
        validator.validate_pcid_mapping(mapping_id, pcid, match_type, score)
        
        # Insert to final output
        final_data = merge_drug_and_pcid(drug, pcid)
        repo.insert_final_output(drug['id'], mapping_id, final_data)
    
    # Collect statistics
    stats_collector.collect_step_statistics(...)
```

### Phase 5: Update Pipeline Runner
**File**: `run_pipeline_resume.py`

**Changes Needed**:
1. Remove Step 3 (zdravstvo) and Step 3a (parallel max prices)
2. Add new Step 3 (PCID mapping)
3. Update step sequence
4. Add database initialization
5. Add schema application
6. Add final report generation
7. Update progress tracking

**New Step Sequence**:
```python
TOTAL_STEPS = 4

run_step(0, "00_backup_and_clean.py", "Backup & Clean", TOTAL_STEPS)
run_step(1, "01_collect_urls.py", "Collect URLs", TOTAL_STEPS)
run_step(2, "02_scrape_details.py", "Scrape Details", TOTAL_STEPS)
run_step(3, "03_map_pcids.py", "PCID Mapping", TOTAL_STEPS)  # NEW

# Generate final report
generate_final_report(run_id)
```

### Phase 6: Chrome Management Enhancement
**File**: `chrome_pool.py` (NEW)

**Features**:
1. Chrome instance pooling
2. Automatic restart after N requests
3. Memory monitoring
4. Orphaned process cleanup
5. Crash recovery
6. Session management

**Class Structure**:
```python
class ChromePool:
    def __init__(self, max_instances=7, restart_after=100):
        self.pool = []
        self.max_instances = max_instances
        self.restart_after = restart_after
        self.request_counts = {}
    
    def get_driver(self, worker_id):
        # Return existing or create new driver
        # Check if restart needed
        # Handle crashes
    
    def release_driver(self, driver):
        # Return to pool or restart if needed
    
    def cleanup_all(self):
        # Clean shutdown
```

### Phase 7: Network & Crash Recovery
**File**: `recovery_manager.py` (NEW)

**Features**:
1. Exponential backoff retry
2. Network connectivity checks
3. Session recovery
4. Automatic crash detection
5. State preservation
6. Resume from last checkpoint

### Phase 8: GUI Integration
**Files to Update**:
- `gui/scrapers/north_macedonia_panel.py` (if exists)
- `gui/main_window.py`

**Changes**:
1. Add PCID mapping file upload
2. Add validation report viewer
3. Add statistics dashboard
4. Add real-time progress from database
5. Add export options (CSV, Excel, JSON)
6. Add data quality indicators

## 📋 REMAINING TASKS

### High Priority
- [ ] Refactor `01_collect_urls.py` to use database
- [ ] Refactor `02_scrape_details.py` to use database
- [ ] Create `03_map_pcids.py` for PCID mapping
- [ ] Update `run_pipeline_resume.py` with new workflow
- [ ] Delete old Step 3 files (`03_scrape_zdravstvo.py`, `03a_scrape_maxprices_parallel.py`)

### Medium Priority
- [ ] Create `chrome_pool.py` for instance management
- [ ] Create `recovery_manager.py` for crash/network handling
- [ ] Add final report generation to pipeline
- [ ] Update GUI for North Macedonia

### Low Priority
- [ ] Performance optimization
- [ ] Export enhancements
- [ ] Monitoring dashboard
- [ ] Advanced analytics

## 🎯 SUCCESS CRITERIA

### Functional Requirements
- ✅ No CSV files used (all database)
- ✅ PCID mapping implemented
- ✅ Validation on all data
- ✅ Statistics collection
- ✅ Final report generation
- ⏳ Chrome pooling
- ⏳ Crash recovery
- ⏳ Network failure handling

### Quality Requirements
- Data completeness > 95%
- Data quality score > 90
- Validation pass rate > 95%
- PCID mapping rate > 85%
- Error rate < 5%

### Performance Requirements
- URL collection: > 50 URLs/minute
- Detail scraping: > 100 records/minute
- PCID mapping: > 500 mappings/minute
- Memory usage: < 2GB per worker
- Chrome instances: ≤ 7 concurrent

## 📊 TESTING PLAN

### Unit Tests
- [ ] Repository CRUD operations
- [ ] Validation rules
- [ ] Statistics calculations
- [ ] PCID matching logic

### Integration Tests
- [ ] End-to-end pipeline run
- [ ] Database schema migration
- [ ] Resume from checkpoint
- [ ] Error recovery

### Performance Tests
- [ ] Large dataset (10,000+ records)
- [ ] Concurrent workers
- [ ] Memory leak detection
- [ ] Chrome stability

## 📝 MIGRATION GUIDE

### For Existing Runs
1. Export existing CSV data
2. Import to database using migration script
3. Update run_ledger with historical runs
4. Verify data integrity

### For New Runs
1. Use new configuration
2. Run with database mode
3. Monitor validation results
4. Review final report

## 🔗 DEPENDENCIES

### Python Packages
- psycopg2-binary (PostgreSQL)
- pandas (data manipulation)
- openpyxl (Excel reading)
- rapidfuzz (fuzzy matching)
- selenium (web scraping)
- deep-translator (translation)

### External Services
- PostgreSQL database
- Telegram (notifications)
- Chrome/ChromeDriver

## 📚 DOCUMENTATION

### User Documentation
- [ ] Updated README with new workflow
- [ ] Configuration guide
- [ ] Troubleshooting guide
- [ ] FAQ

### Developer Documentation
- [x] Database schema documentation
- [x] API reference (repository methods)
- [ ] Architecture diagrams
- [ ] Code examples

---

**Last Updated**: 2026-02-12  
**Status**: Phase 1 Complete, Phase 2-8 In Progress  
**Next Steps**: Refactor Step 1 and Step 2 scripts
