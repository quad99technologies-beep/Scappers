# Core Folder Reorganization Plan

## Current State
- **71 Python files** in the root `core/` directory
- **5 existing subdirectories**: `db/`, `observability/`, `transform/`, `translation/`, `__pycache__/`
- Very difficult to navigate and find specific modules

## Proposed Structure

```
core/
├── __init__.py
├── README.md
│
├── browser/                    # Browser & automation (9 files)
│   ├── __init__.py
│   ├── browser_observer.py
│   ├── browser_session.py
│   ├── chrome_instance_tracker.py
│   ├── chrome_manager.py
│   ├── chrome_pid_tracker.py
│   ├── firefox_pid_tracker.py
│   ├── selector_healer.py
│   ├── stealth_profile.py
│   └── human_actions.py
│
├── config/                     # Configuration management (2 files)
│   ├── __init__.py
│   ├── config_manager.py
│   └── retry_config.py
│
├── data/                       # Data processing & validation (8 files)
│   ├── __init__.py
│   ├── data_diff.py
│   ├── data_quality_checks.py
│   ├── data_validator.py
│   ├── deduplicator.py
│   ├── schema_inference.py
│   ├── pcid_mapping.py
│   └── pcid_mapping_contract.py
│
├── db/                         # Database (existing, 7 files)
│   ├── __init__.py
│   ├── models.py
│   ├── postgres_connection.py
│   ├── postgres_pool.py
│   └── ...
│
├── monitoring/                 # Monitoring & observability (15 files)
│   ├── __init__.py
│   ├── alerting_contract.py
│   ├── alerting_integration.py
│   ├── anomaly_detection.py
│   ├── anomaly_detector.py
│   ├── audit_logger.py
│   ├── benchmarking.py
│   ├── cost_tracking.py
│   ├── dashboard.py
│   ├── diagnostics_exporter.py
│   ├── error_tracker.py
│   ├── health_monitor.py
│   ├── memory_leak_detector.py
│   ├── prometheus_exporter.py
│   ├── resource_monitor.py
│   └── trend_analysis.py
│
├── network/                    # Network & proxy management (6 files)
│   ├── __init__.py
│   ├── geo_router.py
│   ├── ip_rotation.py
│   ├── network_info.py
│   ├── proxy_pool.py
│   ├── tor_httpx.py
│   └── tor_manager.py
│
├── observability/             # (existing, 4 files)
│   └── ...
│
├── pipeline/                   # Pipeline management (12 files)
│   ├── __init__.py
│   ├── base_scraper.py
│   ├── frontier.py
│   ├── hybrid_auditor.py
│   ├── hybrid_scraper.py
│   ├── pipeline_checkpoint.py
│   ├── pipeline_start_lock.py
│   ├── preflight_checks.py
│   ├── run_rollback.py
│   ├── scraper_orchestrator.py
│   ├── standalone_checkpoint.py
│   ├── step_hooks.py
│   └── url_work_queue.py
│
├── progress/                   # Progress tracking & reporting (8 files)
│   ├── __init__.py
│   ├── export_delivery_tracking.py
│   ├── progress_tracker.py
│   ├── report_generator.py
│   ├── rich_progress.py
│   ├── run_comparison.py
│   ├── run_ledger.py
│   ├── run_metrics_integration.py
│   └── run_metrics_tracker.py
│
├── reliability/                # Retry & rate limiting (3 files)
│   ├── __init__.py
│   ├── rate_limiter.py
│   └──smart_retry.py
│
├── transform/                 # (existing, 1 file)
│   └── ...
│
├── translation/               # (existing, 2 files)
│   └── ...
│
├── utils/                     # Utilities & helpers (5 files)
│   ├── __init__.py
│   ├── cache_manager.py
│   ├── integration_example.py
│   ├── integration_helpers.py
│   ├── logger.py
│   ├── shared_utils.py
│   ├── step_progress_logger.py
│   ├── telegram_notifier.py
│   └── url_worker.py
│
└── __pycache__/
```

## Migration Steps

### Phase 1: Create New Directories
1. Create subdirectories with `__init__.py` files
2. Update imports in `__init__.py` to maintain backward compatibility

### Phase 2: Move Files
1. Move files to respective directories
2. Update `__init__.py` in each subdirectory
3. Add re-exports in `core/__init__.py` for backward compatibility

### Phase 3: Update Imports Across Project
1. Search for all imports from `core.`
2. Update to new paths (or rely on backward compatibility)
3. Test all scrapers

### Phase 4: Documentation
1. Update README.md in core/
2. Document new structure
3. Create migration guide

## File Categorization

### Browser & Automation (9)
- browser_observer.py
- browser_session.py
- chrome_instance_tracker.py
- chrome_manager.py
- chrome_pid_tracker.py
- firefox_pid_tracker.py
- selector_healer.py
- stealth_profile.py
- human_actions.py

### Config (2)
- config_manager.py
- retry_config.py

### Data Processing (8)
- data_diff.py
- data_quality_checks.py
- data_validator.py
- deduplicator.py
- schema_inference.py
- pcid_mapping.py
- pcid_mapping_contract.py

### Monitoring (15)
- alerting_contract.py
- alerting_integration.py
- anomaly_detection.py
- anomaly_detector.py
- audit_logger.py
- benchmarking.py
- cost_tracking.py
- dashboard.py
- diagnostics_exporter.py
- error_tracker.py
- health_monitor.py
- memory_leak_detector.py
- prometheus_exporter.py
- resource_monitor.py
- trend_analysis.py

### Network (6)
- geo_router.py
- ip_rotation.py
- network_info.py
- proxy_pool.py
- tor_httpx.py
- tor_manager.py

### Pipeline (12)
- base_scraper.py
- frontier.py
- hybrid_auditor.py
- hybrid_scraper.py
- pipeline_checkpoint.py
- pipeline_start_lock.py
- preflight_checks.py
- run_rollback.py
- scraper_orchestrator.py
- standalone_checkpoint.py
- step_hooks.py
- url_work_queue.py

### Progress & Reporting (8)
- export_delivery_tracking.py
- progress_tracker.py
- report_generator.py
- rich_progress.py
- run_comparison.py
- run_ledger.py
- run_metrics_integration.py
- run_metrics_tracker.py

### Reliability (3)
- rate_limiter.py
- smart_retry.py

### Utils (9)
- cache_manager.py
- integration_example.py
- integration_helpers.py
- logger.py
- shared_utils.py
- step_progress_logger.py
- telegram_notifier.py
- url_worker.py

## Backward Compatibility Strategy

Update `core/__init__.py` to re-export all moved modules:

```python
# Maintain backward compatibility
from core.browser.chrome_manager import ChromeManager
from core.config.config_manager import ConfigManager
from core.data.data_validator import DataValidator
# ... etc for all commonly used modules
```

This allows existing code to continue using:
```python
from core.config_manager import ConfigManager  # Old way
from core.config.config_manager import ConfigManager  # New way
```

Both will work during transition period.

## Benefits

1. ✅ **Better Organization**: Logical grouping by functionality
2. ✅ **Easier Navigation**: Find related modules quickly
3. ✅ **Clearer Dependencies**: See what each category depends on
4. ✅ **Scalability**: Easy to add new modules to appropriate category
5. ✅ **Maintainability**: Clear separation of concerns
6. ✅ **Documentation**: Each subdirectory can have its own README

## Risks & Mitigation

**Risk**: Breaking existing imports  
**Mitigation**: Use re-exports in `core/__init__.py` for backward compatibility

**Risk**: Large code change affecting multiple scrapers  
**Mitigation**: Do it in phases, test after each phase

**Risk**: Merge conflicts if others are working on code  
**Mitigation**: Coordinate timing, do during low-activity period

## Timeline

- **Phase 1** (30 min): Create directories and `__init__.py` files
- **Phase 2** (1 hour): Move files and set up re-exports
- **Phase 3** (1 hour): Test all scrapers
- **Phase 4** (30 min): Update documentation

**Total**: ~3 hours

## Status

- [x] Analysis complete
- [x] Plan documented
- [ ] Directories created
- [ ] Files moved
- [ ] Backward compatibility tested
- [ ] All scrapers tested
- [ ] Documentation updated
