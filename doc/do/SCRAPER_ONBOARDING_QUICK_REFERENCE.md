# Scraper Onboarding Quick Reference

**Quick checklist for developers onboarding new scrapers**

---

## 🚀 5-Minute Quick Start

### 1. Copy Template
```bash
cp -r scripts/Malaysia scripts/NewScraper
# Update all references from "Malaysia" to "NewScraper"
```

### 2. Database Schema
```sql
-- Create schema migration
-- Copy pattern from sql/schemas/postgres/my_schema.sql
-- Update table prefixes (my_ → new_)
```

### 3. Foundation Contracts
```python
# Add to run_pipeline_resume.py imports:
from core.preflight_checks import PreflightChecker
from core.step_hooks import StepHookRegistry, StepMetrics
from core.alerting_integration import setup_alerting_hooks
from core.data_quality_checks import DataQualityChecker
from core.audit_logger import audit_log
from core.benchmarking import record_step_benchmark
from core.chrome_instance_tracker import ChromeInstanceTracker
```

### 3b. Chrome Instance Tracking
```python
# Register browser instances:
tracker = ChromeInstanceTracker(scraper_name, run_id, db)
instance_id = tracker.register(step_number=1, thread_id=0, pid=12345)

# Mark terminated:
tracker.mark_terminated(instance_id, reason="cleanup")
```

### 3c. Stealth/Anti-Bot Features
```python
# For Playwright:
from core.stealth_profile import apply_playwright, get_stealth_init_script
context_kwargs = {}
apply_playwright(context_kwargs)  # Adds user agent, locale, viewport
context = browser.new_context(**context_kwargs)
context.add_init_script(get_stealth_init_script())  # Injects stealth script

# For Selenium:
from core.stealth_profile import apply_selenium
apply_selenium(options)  # Adds stealth flags and user agent
```

### 4. Integration Points
```python
# In main():
setup_alerting_hooks()  # Once at startup
checker = PreflightChecker(scraper_name, run_id)
if checker.has_critical_failures():
    sys.exit(1)

# In run_step():
metrics = StepMetrics(...)
StepHookRegistry.emit_step_start(metrics)
# ... execute step ...
log_step_progress(..., duration_seconds=..., rows_processed=...)
StepHookRegistry.emit_step_end(metrics)
```

---

## ✅ Critical Checks (Must Have)

- [ ] `run_pipeline_resume.py` exists
- [ ] Preflight checks integrated
- [ ] Step hooks integrated
- [ ] Enhanced metrics logged (`duration_seconds`, `rows_*`)
- [ ] Postgres-only (no SQLite, CSV only for exports)
- [ ] Browser cleanup (pre/post run)
- [ ] Stale pipeline recovery
- [ ] Step progress DB logging
- [ ] **Chrome instance tracking table** (`[prefix]_chrome_instances` or shared `chrome_instances`)
- [ ] **Stealth/anti-bot features** (`core.stealth_profile` - excludes human typing)

---

## 📋 Full Checklist

See [`SCRAPER_ONBOARDING_CHECKLIST.md`](SCRAPER_ONBOARDING_CHECKLIST.md) for complete checklist (39 categories, 160+ checks).

**New Standardized Requirements:**
- Chrome instance tracking table (Item 17)
- Stealth/anti-bot features (Item 26) - excludes human typing

---

**Reference:** `scripts/Malaysia/run_pipeline_resume.py`
