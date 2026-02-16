# Gap Analysis: Core Features vs Usage

**Date:** 2026-02-16  
**Scope:** Core modules, broken workflows, pipeline errors, unused features

---

## 1. Core Features: Used vs Unused

### ✅ USED Core Modules

| Module | Used By | Notes |
|--------|---------|-------|
| `core.config.config_manager` | All cleanup_lock, config_loaders, scraper_gui, run_pipeline | Config loading |
| `core.db.connection` (CountryDB) | Argentina, Russia, Belarus, India, Netherlands, Chile | PostgreSQL via CountryDB |
| `core.db.postgres_connection` | Canada Ontario, India, Netherlands, scraper_gui | Direct Postgres |
| `core.db.models` | All scrapers | run_ledger, generate_run_id |
| `core.db.schema_registry` | Argentina, Russia | Schema apply |
| `core.db.csv_importer` | scraper_gui | CSV import/export |
| `core.pipeline.pipeline_checkpoint` | All run_pipeline_resume | Step resume |
| `core.pipeline.pipeline_start_lock` | shared_workflow_runner, cleanup_lock | Single-instance lock |
| `core.pipeline.base_scraper` | Netherlands, Chile, Brazil, Taiwan, Canada Ontario, India, North Macedonia, Quebec, Belarus | Base class |
| `core.pipeline.preflight_checks` | Russia, Belarus run_pipeline | Optional |
| `core.pipeline.step_hooks` | Russia, Belarus run_pipeline | Optional |
| `core.browser.chrome_manager` | Russia, Belarus, canada_ontario | Chrome driver |
| `core.browser.chrome_pid_tracker` | Russia, Belarus, North Macedonia, Taiwan, shared_workflow | PID tracking |
| `core.browser.chrome_instance_tracker` | Russia, canada_ontario | Instance tracking |
| `core.browser.stealth_profile` | Belarus, canada_ontario | Anti-detection |
| `core.browser.human_actions` | Russia, canada_ontario | Pause, type_delay |
| `core.browser.selector_healer` | scraper_gui | Optional |
| `core.network.tor_manager` | Belarus | Tor/Firefox |
| `core.network.ip_rotation` | Argentina 03b | IP rotation |
| `core.utils.shared_utils` | All 00_backup_and_clean | backup_output_folder |
| `core.utils.logger` | Canada Ontario, India | setup_standard_logger |
| `core.utils.telegram_notifier` | North Macedonia, alerting_integration | Notifications |
| `core.utils.integration_helpers` | scraper_gui, Malaysia base | get_geo_config, add_url_to_frontier |
| `core.utils.step_progress_logger` | Russia, Belarus run_pipeline | Optional |
| `core.data.data_validator` | Canada Ontario run_pipeline | QA validation |
| `core.data.pcid_mapping` | Argentina 06 | PCID mapping |
| `core.resource_monitor` | Russia, Belarus | periodic_resource_check |
| `core.monitoring.alerting_integration` | Russia, Belarus run_pipeline | Optional |
| `core.monitoring.audit_logger` | Russia, Belarus run_pipeline | Optional |
| `core.monitoring.benchmarking` | Russia, Belarus run_pipeline | Optional |
| `core.monitoring.prometheus_exporter` | Russia run_pipeline, scraper_gui | Optional |
| `core.standalone_checkpoint` | Argentina 00, Netherlands archive | run_with_checkpoint |

### ⚠️ PARTIALLY USED (Can Be Used More)

| Module | Current Usage | Gap / Opportunity |
|--------|---------------|-------------------|
| `core.network.proxy_pool` | BaseScraper, scraper_gui (proxy tab) | Argentina/Malaysia use custom ip_rotation instead |
| `core.network.geo_router` | integration_helpers, scraper_gui (geo tab) | Malaysia base imports but most scrapers don't use |
| `core.pipeline.frontier` | services/frontier_integration, Malaysia quest3 | Argentina/Chile init frontier but Redis often unavailable |
| `core.data.schema_inference` | integration_helpers, selector_healer | LLM schema inference - rarely invoked |
| `core.reliability.rate_limiter` | Tender Chile (AsyncRateLimiter - local) | core.rate_limiter not used |
| `core.reliability.smart_retry` | Self-doc only | No scraper uses retry_request/retry_browser_action |

### ❌ UNUSED Core Modules (Ghost / Orphan)

| Module | Purpose | Recommendation |
|--------|---------|----------------|
| `core.data.data_diff` | Compare runs, detect changes | Standalone CLI; no pipeline integration |
| `core.data.deduplicator` | Deduplicate dataframes | Standalone CLI; no pipeline integration |
| `core.data.data_quality_checks` | DataQualityChecker | Imported by Russia/Belarus but may not run |
| `core.monitoring.anomaly_detector` | Price anomaly detection | Standalone CLI |
| `core.monitoring.health_monitor` | Website health checks | Standalone CLI |
| `core.monitoring.dashboard` | get_dashboard_data | Not used by GUI (GUI has own dashboard) |
| `core.monitoring.cost_tracking` | Cost tracking | Not imported |
| `core.monitoring.trend_analysis` | Trend analysis | Not imported |
| `core.monitoring.diagnostics_exporter` | Diagnostics | Not imported |
| `core.monitoring.error_tracker` | Error tracking | Not imported |
| `core.progress.export_delivery_tracking` | Track export delivery | Example only |
| `core.progress.run_comparison` | services/run_replay | Used by run_replay only |
| `core.pipeline.run_rollback` | Rollback | Not imported |
| `core.pipeline.hybrid_auditor` | Hybrid audit | Not imported |
| `core.pipeline.hybrid_scraper` | Hybrid scraper | Not imported |
| `core.pipeline.scraper_orchestrator` | Orchestration | References url_worker; not in main flow |
| `core.utils.cache_manager` | Caching | core.utils.__init__ exports; rarely used |
| `core.utils.url_worker` | URL worker | Referenced by scraper_orchestrator; not in pipeline |
| `core.observability.*` | OpenTelemetry | Not integrated |
| `core.transform.*` | Transformations | Empty/minimal |

---

## 2. Broken Workflows & Pipeline Errors

### Frontier Queue (Redis-Dependent)

- **Argentina, Malaysia, Chile** call `initialize_frontier_for_scraper()` at pipeline start.
- **Issue:** Frontier requires Redis. If Redis is not running, returns `None`; pipeline continues but frontier features are no-op.
- **GUI:** "Frontier Queue" tab calls `services.frontier_integration.get_frontier_stats()` → fails silently if Redis down.
- **Recommendation:** Document Redis as optional; or add health check before showing frontier tab.

### Proxy Pool (Optional)

- **scraper_gui** "Proxy Pool" tab imports `core.proxy_pool.get_proxy_pool()`.
- **Issue:** `core.proxy_pool` may be at `core.network.proxy_pool`; stub in `core/` may point to wrong path.
- **Recommendation:** Verify stub; ensure GUI proxy tab works when proxy pool configured.

### Geo Router

- **scraper_gui** "Geo Router" tab calls `core.network.geo_router.get_geo_router()`.
- **Issue:** Geo router depends on proxy pool; if proxy pool not configured, may fail.
- **Recommendation:** Graceful fallback when geo router unavailable.

### services/ vs scripts/common (Deleted)

- **scripts/common/** was deleted (per git status).
- **services/** exists with: api_server, worker, scheduler, frontier_integration, etc.
- **Argentina run_pipeline** imports `services.frontier_integration` with try/except fallback.
- **Issue:** If services path not in sys.path, import fails; fallback defines no-op.
- **Recommendation:** Ensure repo root in sys.path when running pipelines.

### Archive North Macedonia Imports

- **archive/northmacedonia_unused_20260215/** uses old paths: `core.telegram_notifier`, `core.chrome_manager`, `core.stealth_profile`, `core.browser_observer`, `core.human_actions`, `core.tor_httpx`.
- **Issue:** These may resolve to stubs or wrong modules; archive code is unused.
- **Recommendation:** Keep in archive; do not run.

---

## 3. Scenarios: Core Features That Could Be Used But Aren't

| Scenario | Core Feature | Current State | How to Use |
|----------|--------------|---------------|------------|
| **Adaptive rate limiting** | `core.reliability.rate_limiter` | Tender Chile uses local AsyncRateLimiter | Replace with `@adaptive_rate_limit` decorator |
| **Smart retry** | `core.reliability.smart_retry` | Manual retry in scrapers | Use `@with_retry` or `retry_browser_action()` |
| **Run comparison** | `core.progress.run_comparison` | Only run_replay | Add "Compare Runs" to GUI |
| **Data diff** | `core.data.data_diff` | Standalone | Add post-export diff step |
| **Deduplication** | `core.data.deduplicator` | Standalone | Add dedup step before export |
| **Anomaly detection** | `core.monitoring.anomaly_detector` | Standalone | Add QA step for price anomalies |
| **Health monitoring** | `core.monitoring.health_monitor` | Standalone | Add pre-flight health check |
| **Unified proxy** | `core.network.proxy_pool` | Argentina uses ip_rotation | Migrate Argentina to proxy_pool |
| **Geo routing** | `core.network.geo_router` | Malaysia base only | Extend to other scrapers |

---

## 4. File Organization Recommendations

| Action | Details |
|--------|---------|
| **MD files** | Most already in `doc/`; `README.md` stays at root |
| **Test files** | Consolidate `testing/test/` and `testing/tests/` into `testing/tests/` |
| **Utility scripts** | Move `testing/check_*.py`, `fix_*.py` to `tools/` or `testing/tools/` |
| **Archive** | `archive/` already has northmacedonia_unused, argentina_unused, etc. |

---

## 5. Summary

- **Core integration:** Partial. DB, config, pipeline checkpoint, browser utilities are well used.
- **Unused:** Monitoring (cost, trend, error tracker, diagnostics), observability, hybrid scraper/auditor, run_rollback, export_delivery_tracking.
- **Broken/optional:** Frontier (Redis), Proxy Pool tab, Geo Router tab when dependencies missing.
- **Opportunities:** rate_limiter, smart_retry, data_diff, deduplicator, anomaly_detector, health_monitor for richer pipelines.
