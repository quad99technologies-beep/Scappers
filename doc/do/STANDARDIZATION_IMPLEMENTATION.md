# Standardization Implementation Guide

This document tracks the implementation of standard features across all regions.

## Shared Utilities Created

1. ✅ `core/step_progress_logger.py` - Standardized step progress logging
2. ✅ `core/error_tracker.py` - Standardized error tracking
3. ✅ `core/chrome_pid_tracker.py` - Already exists, browser PID cleanup
4. ✅ `shared_workflow_runner.py` - Already exists, stale pipeline recovery

## Features to Add to Each Region

### Standard Features (All Regions)

1. **Stale Pipeline Recovery**
   - Import `recover_stale_pipelines` from `shared_workflow_runner`
   - Call in `main()` before determining start step
   - Wrap in try/except

2. **Browser PID Cleanup**
   - Import `terminate_scraper_pids` from `core.chrome_pid_tracker`
   - Call before pipeline starts (pre-run cleanup)
   - Call after pipeline completes (post-run cleanup)
   - Wrap in try/except

3. **Step Progress DB Logging**
   - Import `log_step_progress`, `update_run_ledger_step_count` from `core.step_progress_logger`
   - Create helper functions `_log_step_progress()` and `_update_run_ledger_step_count()`
   - Call `_log_step_progress()` when step starts (status="in_progress")
   - Call `_log_step_progress()` when step completes (status="completed")
   - Call `_log_step_progress()` when step fails (status="failed", with error_message)
   - Call `_update_run_ledger_step_count()` after step completes

4. **Clear Step Data** (Optional - regions with DB repositories)
   - Add `--clear-step` and `--clear-downstream` arguments
   - Implement `clear_step_data()` in repository if not exists
   - Call before determining start step

5. **Step Pause** (Optional)
   - Add 10-second pause after step completion (except last step)
   - Only if region doesn't already have it

## Region-Specific Implementation Status

### ✅ Argentina
- [x] Stale pipeline recovery
- [x] Browser PID cleanup
- [x] Step progress DB logging (already had it)
- [x] Clear step data (needs repository method)
- [x] Step pause

### ✅ Malaysia
- [x] Stale pipeline recovery (already had it)
- [x] Browser PID cleanup
- [x] Step progress DB logging
- [x] Clear step data (already had it)
- [x] Step pause (already had it)

### ✅ Russia
- [x] Stale pipeline recovery
- [x] Browser PID cleanup
- [x] Step progress DB logging
- [x] Clear step data (needs repository method)
- [x] Step pause (already had it)

### ⏳ Belarus
- [ ] Stale pipeline recovery
- [ ] Browser PID cleanup
- [ ] Step progress DB logging
- [ ] Clear step data (needs repository method)
- [ ] Step pause

### ⏳ India
- [ ] Stale pipeline recovery
- [ ] Browser PID cleanup
- [ ] Step progress DB logging
- [ ] Clear step data (needs repository method)
- [ ] Step pause
- [ ] Output file verification
- [ ] Step duration tracking

### ⏳ North Macedonia
- [ ] Step progress DB logging
- [ ] Error tracking table
- [ ] Health check persistence

### ⏳ Canada Ontario
- [ ] Stale pipeline recovery
- [ ] Clear step data (needs repository method)
- [ ] Step progress DB logging
- [ ] Browser PID cleanup
- [ ] Error tracking table

### ⏳ Tender Chile
- [ ] Step progress DB logging
- [ ] Browser PID cleanup
- [ ] Error tracking table
- [ ] smart_locator.py
- [ ] state_machine.py

## Repository Methods Needed

### clear_step_data() Method Template

```python
_STEP_TABLE_MAP = {
    1: ("table1", "table2"),  # Step 1 tables
    2: ("table3",),           # Step 2 tables
    # ... etc
}

def clear_step_data(self, step: int, include_downstream: bool = False) -> Dict[str, int]:
    """
    Delete data for the given step (and optionally downstream steps) for this run_id.
    
    Args:
        step: Pipeline step number (1-based)
        include_downstream: If True, also clear tables for all later steps.
    
    Returns:
        Dict mapping full table name -> rows deleted.
    """
    if step not in self._STEP_TABLE_MAP:
        raise ValueError(f"Unsupported step {step}; valid steps: {sorted(self._STEP_TABLE_MAP)}")
    
    steps = [s for s in sorted(self._STEP_TABLE_MAP) if s == step or (include_downstream and s >= step)]
    deleted: Dict[str, int] = {}
    with self.db.cursor() as cur:
        for s in steps:
            for short_name in self._STEP_TABLE_MAP[s]:
                table = self._table(short_name)
                cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (self.run_id,))
                deleted[table] = cur.rowcount
    try:
        self.db.commit()
    except Exception:
        pass
    
    self._db_log(f"CLEAR | steps={steps} tables={','.join(deleted)} run_id={self.run_id}")
    return deleted
```

## Error Table Schema Template

```sql
CREATE TABLE IF NOT EXISTS {prefix}_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_{prefix}_errors_run ON {prefix}_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_{prefix}_errors_step ON {prefix}_errors(step_number);
```

## Next Steps

1. Complete Belarus updates
2. Complete India updates (most complex - needs many features)
3. Complete North Macedonia updates
4. Complete Canada Ontario updates
5. Complete Tender Chile updates
6. Add error tables to all regions that don't have them
7. Add clear_step_data methods to all repositories that don't have them
8. Test each region to ensure no business logic was changed
