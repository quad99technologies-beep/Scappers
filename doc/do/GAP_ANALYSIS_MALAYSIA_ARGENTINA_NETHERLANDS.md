# Gap Analysis: Malaysia vs Argentina vs Netherlands Pipelines

**Date:** February 6, 2026  
**Scope:** Pipeline comparison, Postgres standards compliance, and step tracking verification

---

## Executive Summary

This document provides a detailed gap analysis comparing the **Malaysia**, **Argentina**, and **Netherlands** pipelines against platform standards, with emphasis on:
1. **Pipeline orchestration** (step-based runner, checkpoint/resume)
2. **Scraping approach** (Playwright/Selenium/requests)
3. **Postgres-only standards** (no SQLite, CSV only for exports)
4. **Mandatory step tracking** (lifespan, status, errors, metrics)
5. **Browser instance tracking** (Chrome lifecycle)

---

## Part A: Pipeline Comparison

### A1. Pipeline Orchestration

| Aspect | Malaysia | Argentina | Netherlands |
|--------|----------|-----------|-------------|
| **Orchestrator** | `run_pipeline_resume.py` | `run_pipeline_resume.py` | `run_pipeline_resume.py` |
| **Total Steps** | 6 steps (0-5) | 10 steps (0-9) | 5 steps (0,1,2,3,5) |
| **Checkpoint System** | ✅ File-based (`PipelineCheckpoint`) | ✅ File-based (`PipelineCheckpoint`) | ✅ File-based + DB-based resume |
| **Resume Support** | ✅ Yes (checkpoint → run_ledger → .current_run_id) | ✅ Yes (checkpoint → run_ledger → .current_run_id) | ✅ Yes (DB-first, fallback to checkpoint) |
| **Step Structure** | ✅ Modular (`steps/` directory) | ⚠️ Flat (scripts in root) | ⚠️ Flat (scripts in root) |
| **Run ID Management** | ✅ Environment + file | ✅ Environment + file | ✅ Environment + DB |
| **Recovery** | ✅ `recover_stale_pipelines()` | ✅ `recover_stale_pipelines()` | ⚠️ Partial (DB-based only) |

**Key Differences:**
- **Malaysia**: Clean modular structure with `steps/` subdirectory
- **Argentina**: More steps (10 vs 6), includes retry step (step 7)
- **Netherlands**: DB-first resume logic (prefers DB state over checkpoint file)

---

### A2. Scraping Approach

| Aspect | Malaysia | Argentina | Netherlands |
|--------|----------|-----------|-------------|
| **Primary Tool** | Playwright (Chromium) | Selenium (Chrome) | Playwright (Chromium) |
| **Session Handling** | ✅ Context manager (`browser_session()`) | ⚠️ Manual driver lifecycle | ✅ Context manager (implicit) |
| **Pagination Strategy** | ✅ State machine + CSV download | ⚠️ Manual pagination | ✅ State machine + URL collection |
| **Anti-Bot** | ✅ Stealth context, user-agent rotation | ⚠️ Basic stealth | ✅ Stealth context |
| **Multi-threading** | ⚠️ Single session per scraper | ✅ Worker threads (Selenium) | ✅ Worker threads (Playwright) |
| **Fallback Strategy** | ⚠️ None | ✅ Selenium → API fallback | ⚠️ None |

**Key Differences:**
- **Malaysia**: Playwright with clean session management, bulk CSV download strategy
- **Argentina**: Selenium with multi-threaded workers, API fallback for failed products
- **Netherlands**: Playwright with URL collection phase, then parallel extraction

---

### A3. Normalization/Mapping Logic

| Aspect | Malaysia | Argentina | Netherlands |
|--------|----------|-----------|-------------|
| **PCID Source** | ⚠️ Country-specific table (`my_pcid_reference`) | ✅ Shared table (`pcid_mapping`) | ⚠️ CSV file (direct read) |
| **Deduplication** | ✅ DB-based (`UNIQUE(run_id, registration_no)`) | ✅ DB-based (`UNIQUE(run_id, record_hash)`) | ✅ DB-based (`UNIQUE(run_id, product_url)`) |
| **Normalization** | ⚠️ Basic (lowercase, strip) | ✅ Advanced (`_norm()` function) | ⚠️ Basic |
| **Mapping Strategy** | ✅ Registration number → PCID | ✅ (company, product, generic, pack_desc) → PCID | ✅ Product URL → PCID |

**Gaps:**
- **Malaysia**: Uses country-specific PCID table instead of shared `pcid_mapping`
- **Netherlands**: Reads PCID mapping from CSV instead of DB table

---

### A4. QC/Validation Gates

| Aspect | Malaysia | Argentina | Netherlands |
|--------|----------|-----------|-------------|
| **Anomaly Checks** | ✅ `smart_locator.py` (CSV size, DOM checks) | ✅ `smart_locator.py` (CSV size, DOM checks) | ✅ `smart_locator.py` (CSV size, DOM checks) |
| **Data Validation** | ⚠️ Basic (row counts) | ✅ Step 8 (stats & validation) | ⚠️ Basic |
| **Error Tracking** | ✅ `my_bulk_search_counts` table | ✅ `ar_errors` table | ✅ `nl_errors` table |
| **Retry Logic** | ⚠️ Manual (checkpoint resume) | ✅ Step 7 (scrape_no_data retry) | ⚠️ Manual (checkpoint resume) |

**Gaps:**
- **Malaysia & Netherlands**: No dedicated validation step (Argentina has step 8)
- **Malaysia & Netherlands**: No automated retry step for failed products

---

### A5. Export Generation Logic

| Aspect | Malaysia | Argentina | Netherlands |
|--------|----------|-----------|-------------|
| **Export Format** | ✅ CSV (PCID-mapped, not-mapped, no-data) | ✅ CSV (mapping, missing, oos, no-data) | ✅ CSV (price/reimbursement mapped/not-mapped) |
| **Export Location** | ✅ `exports/` subdirectory | ✅ `exports/` subdirectory | ✅ Root output directory |
| **DB Persistence** | ✅ `my_export_reports` table | ✅ `ar_export_reports` table | ✅ `nl_export_reports` table |
| **Export Timing** | ✅ Step 5 (final step) | ✅ Step 6 (before retry step) | ✅ Step 5 (final step) |

**Compliance:** ✅ All three persist exports to DB via `*_export_reports` tables

---

### A6. Tracking/Logging Implementation

| Aspect | Malaysia | Argentina | Netherlands |
|--------|----------|-----------|-------------|
| **Run Tracking** | ✅ `run_ledger` table | ✅ `run_ledger` table | ✅ `run_ledger` table |
| **Step Tracking** | ✅ `my_step_progress` table | ✅ `ar_step_progress` table | ✅ `nl_step_progress` table |
| **Step Logger** | ✅ `log_step_progress()` | ✅ `log_step_progress()` | ⚠️ Partial (DB-based only) |
| **Run ID Source** | ✅ Checkpoint → run_ledger → file | ✅ Checkpoint → run_ledger → file | ✅ DB → checkpoint → env |
| **Error Logging** | ✅ Bulk search counts table | ✅ `ar_errors` table | ✅ `nl_errors` table |

**Gaps:**
- **Netherlands**: Step progress logging is DB-based only (no `log_step_progress()` calls)

---

## Part B: Postgres Standards Compliance

### B1. Data & Storage Standards

| Standard | Malaysia | Argentina | Netherlands |
|----------|----------|-----------|-------------|
| **Postgres as Source of Truth** | ✅ Yes (PostgresDB) | ✅ Yes (PostgresDB) | ✅ Yes (PostgresDB) |
| **SQLite Usage** | ❌ No SQLite | ❌ No SQLite | ❌ No SQLite |
| **CSV as Primary Input** | ⚠️ Step 2 reads `products.csv` | ❌ No (DB-only) | ⚠️ Step 1 reads `search_terms.csv` (fallback) |
| **CSV as Source of Truth** | ❌ No (exports only) | ❌ No (exports only) | ❌ No (exports only) |
| **CSV Export Persisted to DB** | ✅ `my_export_reports` | ✅ `ar_export_reports` | ✅ `nl_export_reports` |
| **Input Tables Never Deleted** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Output Cleanup (Scoped)** | ✅ By `run_id` | ✅ By `run_id` | ✅ By `run_id` |

**Gaps:**
- **Malaysia**: Step 2 reads `products.csv` from input directory (should be DB-only)
- **Netherlands**: Step 1 has CSV fallback for search terms (should be DB-only)

---

### B2. Pipeline I/O Contracts

| Standard | Malaysia | Argentina | Netherlands |
|----------|----------|-----------|-------------|
| **Explicit Postgres Read/Write** | ✅ Yes (Repository pattern) | ✅ Yes (Repository pattern) | ✅ Yes (Repository pattern) |
| **No Intermediate Files as Truth** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Resume/Checkpoint Support** | ✅ Idempotent steps | ✅ Idempotent steps | ✅ Idempotent steps |
| **Run-Scoped Writes** | ✅ All tables use `run_id` | ✅ All tables use `run_id` | ✅ All tables use `run_id` |

**Compliance:** ✅ All three pipelines meet I/O contract standards

---

### B3. Mandatory Step Tracking (Failure Analysis + Lifespan)

#### Step Tracking Schema Analysis

All three pipelines use `*_step_progress` tables with the following schema:
```sql
CREATE TABLE IF NOT EXISTS {prefix}_step_progress (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    step_number INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    progress_key TEXT NOT NULL,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    UNIQUE(run_id, step_number, progress_key)
);
```

#### Step Tracking Implementation Comparison

| Requirement | Malaysia | Argentina | Netherlands |
|-------------|----------|-----------|-------------|
| **Step start_time** | ✅ `started_at` (when status='in_progress') | ✅ `started_at` (when status='in_progress') | ✅ `started_at` (when status='in_progress') |
| **Step end_time** | ✅ `completed_at` (when status='completed'/'failed') | ✅ `completed_at` (when status='completed'/'failed') | ✅ `completed_at` (when status='completed'/'failed') |
| **Step duration** | ⚠️ Calculated from checkpoint metadata | ⚠️ Calculated from checkpoint metadata | ⚠️ Calculated from checkpoint metadata |
| **Step status** | ✅ pending/running/success/failed/skipped | ✅ pending/running/success/failed/skipped | ✅ pending/running/success/failed/skipped |
| **Attempt count** | ✅ `retry_count` | ✅ `retry_count` | ✅ `retry_count` |
| **Retry timestamps** | ❌ Not tracked | ❌ Not tracked | ❌ Not tracked |
| **Error summary** | ✅ `error_message` | ✅ `error_message` | ✅ `error_message` |
| **Full traceback/log reference** | ❌ Not stored | ❌ Not stored | ❌ Not stored |
| **Row metrics per step** | ⚠️ Partial (bulk search counts only) | ⚠️ Partial (via `ar_products` counts) | ⚠️ Partial (via `nl_products` counts) |
| **Resource metrics** | ⚠️ Browser PIDs tracked separately | ⚠️ Browser PIDs tracked separately | ⚠️ Browser PIDs tracked separately |
| **Run-level aggregation** | ⚠️ Partial (`run_ledger.step_count`) | ⚠️ Partial (`run_ledger.step_count`) | ⚠️ Partial (`run_ledger.step_count`) |

**Gaps Identified:**

1. **Duration tracking**: Duration is stored in checkpoint JSON, not in `*_step_progress` table
2. **Retry timestamps**: No table to track when retries occurred
3. **Full traceback**: Error messages are truncated, no link to full log files
4. **Row metrics**: No standardized `rows_read`, `rows_processed`, `rows_inserted`, `rows_updated`, `rows_rejected` columns
5. **Resource metrics**: Browser instance counts not stored per step
6. **Run-level aggregation**: No `slowest_step`, `failure_point`, `recovery_point` columns in `run_ledger`

#### Recommended Schema Enhancement

**⚠️ CRITICAL: Add these columns NOW to avoid schema migrations later**

```sql
-- Enhanced step_progress table (MANDATORY for all countries)
ALTER TABLE {prefix}_step_progress ADD COLUMN IF NOT EXISTS duration_seconds REAL;
ALTER TABLE {prefix}_step_progress ADD COLUMN IF NOT EXISTS rows_read INTEGER DEFAULT 0;
ALTER TABLE {prefix}_step_progress ADD COLUMN IF NOT EXISTS rows_processed INTEGER DEFAULT 0;
ALTER TABLE {prefix}_step_progress ADD COLUMN IF NOT EXISTS rows_inserted INTEGER DEFAULT 0;
ALTER TABLE {prefix}_step_progress ADD COLUMN IF NOT EXISTS rows_updated INTEGER DEFAULT 0;
ALTER TABLE {prefix}_step_progress ADD COLUMN IF NOT EXISTS rows_rejected INTEGER DEFAULT 0;
ALTER TABLE {prefix}_step_progress ADD COLUMN IF NOT EXISTS browser_instances_spawned INTEGER DEFAULT 0;
ALTER TABLE {prefix}_step_progress ADD COLUMN IF NOT EXISTS log_file_path TEXT;

-- Enhanced run_ledger table (MANDATORY for all countries)
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS total_runtime_seconds REAL;
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS slowest_step_number INTEGER;
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS slowest_step_name TEXT;
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS failure_step_number INTEGER;
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS failure_step_name TEXT;
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS recovery_step_number INTEGER;

-- Retry history table (MANDATORY for all countries)
CREATE TABLE IF NOT EXISTS step_retries (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    scraper_name TEXT NOT NULL,
    step_number INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    retry_number INTEGER NOT NULL, -- 1-based (first retry = 1)
    retry_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retry_reason TEXT, -- Why this retry was needed
    previous_status TEXT, -- Status before retry
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, step_number, retry_number)
);
CREATE INDEX IF NOT EXISTS idx_step_retries_run_step ON step_retries(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_step_retries_scraper ON step_retries(scraper_name, retry_at);
```

**Migration Script:** `sql/migrations/postgres/005_add_step_tracking_columns.sql`

---

### B4. Browser Instance Tracking (Chrome Lifecycle)

| Requirement | Malaysia | Argentina | Netherlands |
|-------------|----------|-----------|-------------|
| **Chrome PID Tracking** | ✅ `chrome_pid_tracker.py` | ✅ `chrome_pid_tracker.py` | ✅ `chrome_pid_tracker.py` |
| **Per-Run Tracking** | ✅ PID file per scraper | ✅ PID file per scraper | ✅ PID file per scraper |
| **Per-Step Tracking** | ❌ Not tracked | ❌ Not tracked | ❌ Not tracked |
| **Per-Worker Tracking** | ⚠️ Implicit (PID file) | ✅ Explicit (worker threads) | ⚠️ Implicit (PID file) |
| **Clean Shutdown** | ✅ `terminate_scraper_pids()` | ✅ `terminate_scraper_pids()` | ✅ `terminate_scraper_pids()` |
| **Orphan Process Cleanup** | ✅ Startup recovery | ✅ Startup recovery | ⚠️ Partial (DB-based only) |
| **Profile/Session Isolation** | ✅ Playwright context manager | ⚠️ Manual (Selenium) | ✅ Playwright context manager |
| **Deterministic Cleanup** | ✅ Context manager + atexit | ⚠️ Manual cleanup | ✅ Context manager + atexit |

**Gaps:**
- **All**: Browser instance counts not stored per step in `*_step_progress` table
- **Argentina**: Manual Selenium cleanup (no context manager)
- **Netherlands**: Orphan cleanup is DB-based only (no file-based PID tracking fallback)

---

## Part C: Foundation Contracts & Standards (Lock In Now)

### C0. Step Event Hooks Contract (Future-Proofing)

**Purpose:** Standardize step lifecycle hooks so dashboards/alerts/schedulers can attach without modifying step logic.

**Contract Definition:**

```python
# core/step_hooks.py
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime

@dataclass
class StepMetrics:
    """Standardized step metrics structure."""
    step_number: int
    step_name: str
    run_id: str
    scraper_name: str
    duration_seconds: float
    rows_read: int = 0
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_rejected: int = 0
    browser_instances_spawned: int = 0
    log_file_path: Optional[str] = None
    error_message: Optional[str] = None
    started_at: datetime
    completed_at: datetime

class StepHookRegistry:
    """Central registry for step lifecycle hooks."""
    
    _on_step_start: list[Callable[[StepMetrics], None]] = []
    _on_step_end: list[Callable[[StepMetrics], None]] = []
    _on_step_error: list[Callable[[StepMetrics, Exception], None]] = []
    
    @classmethod
    def register_start_hook(cls, callback: Callable[[StepMetrics], None]):
        """Register callback for step start events."""
        cls._on_step_start.append(callback)
    
    @classmethod
    def register_end_hook(cls, callback: Callable[[StepMetrics], None]):
        """Register callback for step end events."""
        cls._on_step_end.append(callback)
    
    @classmethod
    def register_error_hook(cls, callback: Callable[[StepMetrics, Exception], None]):
        """Register callback for step error events."""
        cls._on_step_error.append(callback)
    
    @classmethod
    def emit_step_start(cls, metrics: StepMetrics):
        """Emit step start event to all registered hooks."""
        for hook in cls._on_step_start:
            try:
                hook(metrics)
            except Exception as e:
                logger.error(f"Step start hook failed: {e}")
    
    @classmethod
    def emit_step_end(cls, metrics: StepMetrics):
        """Emit step end event to all registered hooks."""
        for hook in cls._on_step_end:
            try:
                hook(metrics)
            except Exception as e:
                logger.error(f"Step end hook failed: {e}")
    
    @classmethod
    def emit_step_error(cls, metrics: StepMetrics, error: Exception):
        """Emit step error event to all registered hooks."""
        for hook in cls._on_step_error:
            try:
                hook(metrics, error)
            except Exception as e:
                logger.error(f"Step error hook failed: {e}")
```

**Usage in `run_pipeline_resume.py`:**

```python
from core.step_hooks import StepHookRegistry, StepMetrics

def run_step(step_num: int, script_name: str, step_name: str, ...):
    start_time = time.time()
    metrics = StepMetrics(
        step_number=step_num,
        step_name=step_name,
        run_id=run_id,
        scraper_name=scraper_name,
        duration_seconds=0.0,
        started_at=datetime.now(),
        completed_at=datetime.now()
    )
    
    # Emit start event
    StepHookRegistry.emit_step_start(metrics)
    
    try:
        # Execute step...
        result = subprocess.run(...)
        
        # Calculate metrics
        metrics.duration_seconds = time.time() - start_time
        metrics.completed_at = datetime.now()
        # ... populate row metrics from step output ...
        
        # Emit end event
        StepHookRegistry.emit_step_end(metrics)
        
    except Exception as e:
        metrics.error_message = str(e)
        metrics.completed_at = datetime.now()
        StepHookRegistry.emit_step_error(metrics, e)
        raise
```

**Benefits:**
- Dashboard can register hooks without touching step code
- Alerting system can attach to error hooks
- Scheduler can track step completion
- Zero changes to existing step scripts

**Implementation Priority:** 🔴 P0 (Add contract now, populate later)

---

### C1. Preflight Health Checks Contract (Standardized Gate)

**Purpose:** Block bad runs before they start, preventing wasted time and resources.

**Contract Definition:**

```python
# core/preflight_checks.py
from typing import List, Dict, Any
from dataclasses import dataclass
from enum import Enum

class CheckSeverity(Enum):
    CRITICAL = "critical"  # Block run
    WARNING = "warning"   # Warn but allow
    INFO = "info"         # Informational only

@dataclass
class HealthCheckResult:
    """Result of a single health check."""
    name: str
    severity: CheckSeverity
    passed: bool
    message: str
    details: Dict[str, Any] = None

class PreflightChecker:
    """Standardized preflight health checks."""
    
    def __init__(self, scraper_name: str, run_id: str):
        self.scraper_name = scraper_name
        self.run_id = run_id
        self.results: List[HealthCheckResult] = []
    
    def check_database_connectivity(self) -> HealthCheckResult:
        """Check: Database is accessible."""
        # Implementation...
        return HealthCheckResult(
            name="database_connectivity",
            severity=CheckSeverity.CRITICAL,
            passed=True,
            message="Database connection successful"
        )
    
    def check_disk_space(self, min_gb: float = 10.0) -> HealthCheckResult:
        """Check: Sufficient disk space available."""
        # Implementation...
        return HealthCheckResult(...)
    
    def check_memory_available(self, min_gb: float = 4.0) -> HealthCheckResult:
        """Check: Sufficient memory available."""
        # Implementation...
        return HealthCheckResult(...)
    
    def check_browser_executable(self) -> HealthCheckResult:
        """Check: Browser executable is available."""
        # Implementation...
        return HealthCheckResult(...)
    
    def check_input_tables_populated(self) -> HealthCheckResult:
        """Check: Input tables have data."""
        # Implementation...
        return HealthCheckResult(...)
    
    def check_stale_run_detection(self, max_age_hours: int = 24) -> HealthCheckResult:
        """Check: Previous run is not stale (prevents accidental resume of old runs)."""
        # Implementation...
        return HealthCheckResult(...)
    
    def run_all_checks(self) -> List[HealthCheckResult]:
        """Run all preflight checks."""
        self.results = [
            self.check_database_connectivity(),
            self.check_disk_space(),
            self.check_memory_available(),
            self.check_browser_executable(),
            self.check_input_tables_populated(),
            self.check_stale_run_detection(),
        ]
        return self.results
    
    def has_critical_failures(self) -> bool:
        """Check if any critical checks failed."""
        return any(
            r.severity == CheckSeverity.CRITICAL and not r.passed
            for r in self.results
        )
    
    def get_failure_summary(self) -> str:
        """Get human-readable summary of failures."""
        failures = [r for r in self.results if not r.passed]
        if not failures:
            return "All checks passed"
        return "\n".join(f"- {r.name}: {r.message}" for r in failures)
```

**Usage in `run_pipeline_resume.py`:**

```python
from core.preflight_checks import PreflightChecker, CheckSeverity

def main():
    # ... existing code ...
    
    # MANDATORY: Run preflight checks before starting
    checker = PreflightChecker(scraper_name, run_id)
    check_results = checker.run_all_checks()
    
    # Log all results
    for result in check_results:
        if result.severity == CheckSeverity.CRITICAL:
            print(f"[PREFLIGHT] ❌ CRITICAL: {result.name} - {result.message}", flush=True)
        elif result.severity == CheckSeverity.WARNING:
            print(f"[PREFLIGHT] ⚠️  WARNING: {result.name} - {result.message}", flush=True)
        else:
            print(f"[PREFLIGHT] ℹ️  INFO: {result.name} - {result.message}", flush=True)
    
    # Block run if critical checks fail
    if checker.has_critical_failures():
        print(f"[PREFLIGHT] ❌ Pipeline blocked due to critical health check failures:", flush=True)
        print(checker.get_failure_summary(), flush=True)
        sys.exit(1)
    
    # Continue with pipeline execution...
```

**Required Checks (All Must Be Implemented):**

| Check | Severity | Block Run? | Description |
|-------|----------|------------|-------------|
| `database_connectivity` | CRITICAL | Yes | DB connection successful |
| `disk_space` | CRITICAL | Yes | >10GB free space |
| `memory_available` | CRITICAL | Yes | >4GB RAM available |
| `browser_executable` | CRITICAL | Yes | Chrome/Playwright executable found |
| `input_tables_populated` | CRITICAL | Yes | Input tables have rows |
| `stale_run_detection` | WARNING | No | Previous run <24h old |

**Implementation Priority:** 🔴 P0 (Required gate, prevents failures)

---

### C2. Alerting Contract (Trigger Rules)

**Purpose:** Define alert trigger rules now so alerting system can be bolted on later.

**Contract Definition:**

```python
# core/alerting_contract.py
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

class AlertSeverity(Enum):
    CRITICAL = "critical"  # Immediate notification
    HIGH = "high"          # Urgent notification
    MEDIUM = "medium"      # Standard notification
    LOW = "low"            # Informational only

class AlertChannel(Enum):
    TELEGRAM = "telegram"
    EMAIL = "email"
    WEBHOOK = "webhook"
    SLACK = "slack"

@dataclass
class AlertRule:
    """Definition of an alert trigger rule."""
    name: str
    description: str
    severity: AlertSeverity
    enabled: bool = True
    channels: List[AlertChannel] = None
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        """Check if this rule should trigger based on context."""
        raise NotImplementedError("Subclass must implement")

class AlertRuleRegistry:
    """Central registry for alert rules."""
    
    _rules: List[AlertRule] = []
    
    @classmethod
    def register_rule(cls, rule: AlertRule):
        """Register an alert rule."""
        cls._rules.append(rule)
    
    @classmethod
    def evaluate_rules(cls, context: Dict[str, Any]) -> List[AlertRule]:
        """Evaluate all rules and return triggered rules."""
        triggered = []
        for rule in cls._rules:
            if rule.enabled and rule.should_trigger(context):
                triggered.append(rule)
        return triggered

# Standard Alert Rules (MUST BE DEFINED NOW)

class StepFailedRule(AlertRule):
    """Alert: Step failed."""
    def __init__(self):
        super().__init__(
            name="step_failed",
            description="Pipeline step failed",
            severity=AlertSeverity.CRITICAL,
            channels=[AlertChannel.TELEGRAM, AlertChannel.EMAIL]
        )
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        return context.get("step_status") == "failed"

class StepDurationSpikeRule(AlertRule):
    """Alert: Step duration > 2x historical average."""
    def __init__(self, threshold_multiplier: float = 2.0):
        super().__init__(
            name="step_duration_spike",
            description=f"Step duration > {threshold_multiplier}x average",
            severity=AlertSeverity.HIGH,
            channels=[AlertChannel.TELEGRAM]
        )
        self.threshold_multiplier = threshold_multiplier
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        current_duration = context.get("duration_seconds", 0)
        avg_duration = context.get("avg_duration_seconds", 0)
        if avg_duration == 0:
            return False
        return current_duration > (avg_duration * self.threshold_multiplier)

class ZeroRowsRule(AlertRule):
    """Alert: Step processed zero rows."""
    def __init__(self):
        super().__init__(
            name="zero_rows",
            description="Step processed zero rows",
            severity=AlertSeverity.HIGH,
            channels=[AlertChannel.TELEGRAM]
        )
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        rows_processed = context.get("rows_processed", 0)
        return rows_processed == 0 and context.get("step_number", 0) > 0  # Skip step 0

class BrowserLeakRule(AlertRule):
    """Alert: Too many browser instances spawned."""
    def __init__(self, max_instances: int = 10):
        super().__init__(
            name="browser_leak",
            description=f"Browser instances > {max_instances}",
            severity=AlertSeverity.MEDIUM,
            channels=[AlertChannel.TELEGRAM]
        )
        self.max_instances = max_instances
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        browser_count = context.get("browser_instances_spawned", 0)
        return browser_count > self.max_instances

class DatabaseConnectionFailureRule(AlertRule):
    """Alert: Database connection failed."""
    def __init__(self):
        super().__init__(
            name="db_connection_failure",
            description="Database connection failed",
            severity=AlertSeverity.CRITICAL,
            channels=[AlertChannel.TELEGRAM, AlertChannel.EMAIL]
        )
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        return context.get("db_connection_failed", False)

# Register standard rules
AlertRuleRegistry.register_rule(StepFailedRule())
AlertRuleRegistry.register_rule(StepDurationSpikeRule())
AlertRuleRegistry.register_rule(ZeroRowsRule())
AlertRuleRegistry.register_rule(BrowserLeakRule())
AlertRuleRegistry.register_rule(DatabaseConnectionFailureRule())
```

**Usage in Step Hooks:**

```python
from core.step_hooks import StepHookRegistry, StepMetrics
from core.alerting_contract import AlertRuleRegistry

def alert_on_step_end(metrics: StepMetrics):
    """Alert hook: Evaluate alert rules on step completion."""
    context = {
        "step_status": "completed" if not metrics.error_message else "failed",
        "step_number": metrics.step_number,
        "step_name": metrics.step_name,
        "duration_seconds": metrics.duration_seconds,
        "rows_processed": metrics.rows_processed,
        "browser_instances_spawned": metrics.browser_instances_spawned,
        "scraper_name": metrics.scraper_name,
        "run_id": metrics.run_id,
        # ... fetch avg_duration from DB ...
    }
    
    triggered_rules = AlertRuleRegistry.evaluate_rules(context)
    for rule in triggered_rules:
        # Send alert via configured channels
        send_alert(rule, context)

# Register alert hook
StepHookRegistry.register_end_hook(alert_on_step_end)
```

**Required Alert Rules (Must Be Defined Now):**

| Rule Name | Severity | Trigger Condition |
|-----------|----------|-------------------|
| `step_failed` | CRITICAL | Step status = "failed" |
| `step_duration_spike` | HIGH | Duration > 2x historical average |
| `zero_rows` | HIGH | Rows processed = 0 (except step 0) |
| `browser_leak` | MEDIUM | Browser instances > 10 |
| `db_connection_failure` | CRITICAL | Database connection failed |

**Implementation Priority:** 🔴 P0 (Define contract now, implement later)

---

### C3. Single PCID Mapping Contract (Unified Interface)

**Purpose:** Lock in shared PCID mapping interface so Malaysia/Netherlands don't drift again.

**Contract Definition:**

```python
# core/pcid_mapping_contract.py
from typing import Optional, List, Dict
from abc import ABC, abstractmethod

class PCIDMappingInterface(ABC):
    """Standardized interface for PCID mapping (MANDATORY for all countries)."""
    
    @abstractmethod
    def get_all(self) -> List[Dict]:
        """Get all PCID mappings for this country."""
        pass
    
    @abstractmethod
    def lookup(self, company: str, product: str, 
               generic: str = "", pack_desc: str = "") -> Optional[str]:
        """Lookup PCID by product details."""
        pass
    
    @abstractmethod
    def get_oos(self) -> List[Dict]:
        """Get OOS (Out of Scope) products."""
        pass
    
    @abstractmethod
    def is_oos_product(self, company: str, product: str) -> bool:
        """Check if product is OOS."""
        pass

class SharedPCIDMapping(PCIDMappingInterface):
    """Standard implementation using shared pcid_mapping table."""
    
    def __init__(self, country: str, db=None):
        """
        Initialize PCID mapping for a country.
        
        Args:
            country: Country name (e.g., "Malaysia", "Argentina")
            db: Optional database connection (auto-connects if None)
        """
        self.country = country
        self.db = db or self._get_db()
        self._cache = None
    
    def _get_db(self):
        """Get database connection."""
        from core.db.postgres_connection import get_db
        return get_db(self.country)
    
    def get_all(self) -> List[Dict]:
        """Get all PCID mappings for this country from shared table."""
        with self.db.cursor() as cur:
            cur.execute("""
                SELECT pcid, company, local_product_name, generic_name,
                       local_pack_description, local_pack_code, presentation
                FROM pcid_mapping
                WHERE source_country = %s
                ORDER BY company, local_product_name
            """, (self.country,))
            return [dict(row) for row in cur.fetchall()]
    
    def lookup(self, company: str, product: str, 
               generic: str = "", pack_desc: str = "") -> Optional[str]:
        """Lookup PCID using normalized matching."""
        # Implementation using normalization logic...
        pass
    
    def get_oos(self) -> List[Dict]:
        """Get OOS products (PCID = 'OOS')."""
        with self.db.cursor() as cur:
            cur.execute("""
                SELECT pcid, company, local_product_name, generic_name,
                       local_pack_description, local_pack_code
                FROM pcid_mapping
                WHERE source_country = %s AND UPPER(pcid) = 'OOS'
            """, (self.country,))
            return [dict(row) for row in cur.fetchall()]
    
    def is_oos_product(self, company: str, product: str) -> bool:
        """Check if product is OOS."""
        pcid = self.lookup(company, product)
        return pcid is not None and pcid.upper() == "OOS"

# MANDATORY: All countries MUST use this interface
def get_pcid_mapping(country: str) -> PCIDMappingInterface:
    """
    Get PCID mapping instance for a country.
    
    This is the ONLY way to access PCID mappings.
    All countries must use this function.
    """
    return SharedPCIDMapping(country)
```

**Migration Contract:**

```python
# Migration path for Malaysia/Netherlands

# OLD (Malaysia):
from scripts.Malaysia.db.repositories import MalaysiaRepository
repo = MalaysiaRepository(db, run_id)
pcid_ref = repo.load_pcid_reference()  # ❌ Country-specific table

# NEW (All countries):
from core.pcid_mapping_contract import get_pcid_mapping
pcid = get_pcid_mapping("Malaysia")  # ✅ Shared table
all_mappings = pcid.get_all()
oos_products = pcid.get_oos()
```

**Database Contract:**

```sql
-- MANDATORY: All countries use shared pcid_mapping table
-- No country-specific PCID tables allowed

-- Malaysia migration:
-- 1. Migrate my_pcid_reference → pcid_mapping (set source_country='Malaysia')
-- 2. Update step_05_pcid_export.py to use get_pcid_mapping("Malaysia")
-- 3. Drop my_pcid_reference table

-- Netherlands migration:
-- 1. Load PCID CSV → pcid_mapping (set source_country='Netherlands')
-- 2. Update 05_Generate_PCID_Mapped.py to use get_pcid_mapping("Netherlands")
-- 3. Remove CSV fallback logic
```

**Implementation Priority:** 🔴 P0 (Lock contract now, migrate later)

---

### C4. Foundation Contracts Summary (Lock In Now)

**⚠️ CRITICAL: These contracts must be defined NOW to avoid major refactors later.**

| Contract | Status | Priority | Why Lock Now |
|----------|--------|----------|--------------|
| **Step Event Hooks** | ⚠️ Partially Covered | 🔴 P0 | Dashboard/alerts need hooks; adding later = touching every step |
| **Preflight Health Checks** | ⚠️ Mentioned, Not Standardized | 🔴 P0 | Prevents wasted runs; easy to add now, painful to retrofit |
| **Alerting Contract** | ❌ Not Covered | 🔴 P0 | Alert rules need context; define now so implementation is pluggable |
| **PCID Mapping Contract** | ⚠️ Flagged, Not Locked | 🔴 P0 | MY/NL will drift again without enforced interface |
| **Enhanced Step Metrics Schema** | ⚠️ Recommended, Not Created | 🔴 P0 | Schema migrations are painful; add columns now, populate later |
| **Retry Timestamps Table** | ⚠️ Mentioned, Not Created | 🟠 P1 | Analysis needs history; add table now, populate later |

**Action Items (This Week):**

1. ✅ **Create migration script** `sql/migrations/postgres/005_add_step_tracking_columns.sql`
   - Add all enhanced columns to `*_step_progress` tables
   - Add all enhanced columns to `run_ledger` table
   - Create `step_retries` table

2. ✅ **Create `core/step_hooks.py`** with `StepHookRegistry` class
   - Define `StepMetrics` dataclass
   - Implement hook registration/emission
   - Zero changes to step scripts (hooks are optional)

3. ✅ **Create `core/preflight_checks.py`** with `PreflightChecker` class
   - Implement all 6 required checks
   - Integrate into `run_pipeline_resume.py` as mandatory gate

4. ✅ **Create `core/alerting_contract.py`** with `AlertRuleRegistry`
   - Define all 5 standard alert rules
   - Register rules (implementation can come later)

5. ✅ **Create `core/pcid_mapping_contract.py`** with `PCIDMappingInterface`
   - Implement `SharedPCIDMapping` class
   - Update `core/pcid_mapping.py` to use contract
   - Document migration path for MY/NL

**Benefits of Locking Now:**
- ✅ **Zero rework**: Contracts are forward-compatible
- ✅ **Incremental implementation**: Can populate data later
- ✅ **Team alignment**: Everyone uses same interfaces
- ✅ **Future-proof**: Dashboard/alerts/scheduler can attach without touching pipelines

---

## Part C: Gap Classification & Recommendations

### C1. Must-Have Gaps (Production Reliability/Observability)

#### 1. **Step Duration Tracking in Database** 🔴 CRITICAL
- **Current**: Duration stored in checkpoint JSON only
- **Impact**: Cannot query step durations for analysis without parsing JSON
- **Fix**: Add `duration_seconds REAL` column to `*_step_progress` tables
- **Affected**: All three pipelines

#### 2. **Row Metrics Per Step** 🔴 CRITICAL
- **Current**: No standardized metrics (read/processed/inserted/updated/rejected)
- **Impact**: Cannot analyze data flow bottlenecks
- **Fix**: Add row metric columns to `*_step_progress` tables
- **Affected**: All three pipelines

#### 3. **Full Traceback/Log Reference** 🔴 CRITICAL
- **Current**: Error messages truncated, no link to log files
- **Impact**: Cannot debug failures without manual log file search
- **Fix**: Add `log_file_path TEXT` column to `*_step_progress` tables
- **Affected**: All three pipelines

#### 4. **Run-Level Aggregation** 🔴 CRITICAL
- **Current**: No `slowest_step`, `failure_point`, `recovery_point` in `run_ledger`
- **Impact**: Cannot quickly identify bottlenecks or failure patterns
- **Fix**: Add aggregation columns to `run_ledger` table
- **Affected**: All three pipelines

#### 5. **CSV Input Removal** 🔴 CRITICAL
- **Current**: Malaysia Step 2 reads `products.csv`, Netherlands Step 1 has CSV fallback
- **Impact**: Violates Postgres-only standard
- **Fix**: Migrate to DB-only input tables
- **Affected**: Malaysia, Netherlands

---

### C2. Good-to-Have Gaps (Maintainability/Scale)

#### 6. **Retry Timestamp Tracking** 🟡 MEDIUM
- **Current**: `retry_count` exists but no timestamps
- **Impact**: Cannot analyze retry patterns over time
- **Fix**: Create `step_retries` table with `retry_at TIMESTAMP`
- **Affected**: All three pipelines

#### 7. **Browser Instance Counts Per Step** 🟡 MEDIUM
- **Current**: Browser PIDs tracked but not per step
- **Impact**: Cannot correlate browser usage with step performance
- **Fix**: Add `browser_instances_spawned INTEGER` to `*_step_progress`
- **Affected**: All three pipelines

#### 8. **Modular Step Structure** 🟡 MEDIUM
- **Current**: Argentina and Netherlands use flat script structure
- **Impact**: Harder to maintain and test individual steps
- **Fix**: Migrate to `steps/` subdirectory (like Malaysia)
- **Affected**: Argentina, Netherlands

#### 9. **Unified PCID Mapping** 🟡 MEDIUM
- **Current**: Malaysia uses `my_pcid_reference`, Netherlands reads CSV
- **Impact**: Inconsistent PCID source across pipelines
- **Fix**: Migrate to shared `pcid_mapping` table (like Argentina)
- **Affected**: Malaysia, Netherlands

#### 10. **Dedicated Validation Step** 🟡 MEDIUM
- **Current**: Only Argentina has step 8 (stats & validation)
- **Impact**: No automated QC gates for Malaysia/Netherlands
- **Fix**: Add validation step to Malaysia/Netherlands pipelines
- **Affected**: Malaysia, Netherlands

---

## Part D: File-Level Refactor Plan

### D1. Standardized Template Architecture

```
scripts/{Country}/
├── run_pipeline_resume.py          # Main orchestrator (standardized)
├── steps/                          # Modular step structure
│   ├── step_00_backup_clean.py
│   ├── step_01_collect.py
│   ├── step_02_extract.py
│   ├── step_03_process.py
│   ├── step_04_validate.py         # Optional QC step
│   └── step_05_export.py
├── db/
│   ├── repositories.py             # Repository pattern
│   └── schema.py                   # Country-specific schema
├── scrapers/                       # Scraping logic
│   ├── base.py                     # Base scraper class
│   └── {source}_scraper.py
├── exports/
│   └── csv_exporter.py             # Export generation
├── config_loader.py                # Configuration management
└── scraper_utils.py                 # Shared utilities
```

### D2. Refactor Priority

#### Phase 1: Critical Fixes (Week 1-2)
1. **Add step tracking enhancements** (duration, row metrics, log paths)
   - Files: `sql/schemas/postgres/common.sql`, `core/step_progress_logger.py`
   - Migration: `sql/migrations/postgres/005_add_step_tracking_columns.sql`

2. **Remove CSV inputs** (Malaysia Step 2, Netherlands Step 1)
   - Files: `scripts/Malaysia/steps/step_02_product_details.py`, `scripts/Netherlands/01_get_medicijnkosten_data.py`
   - Migration: Create input tables, migrate CSV data

3. **Add run-level aggregation** (slowest_step, failure_point)
   - Files: `sql/schemas/postgres/common.sql`, `core/run_ledger.py`
   - Migration: `sql/migrations/postgres/006_add_run_aggregation_columns.sql`

#### Phase 2: Standardization (Week 3-4)
4. **Migrate to modular step structure** (Argentina, Netherlands)
   - Files: Create `steps/` directories, move scripts
   - Update: `run_pipeline_resume.py` to use `steps/` directory

5. **Unify PCID mapping** (Malaysia, Netherlands)
   - Files: `scripts/Malaysia/steps/step_05_pcid_export.py`, `scripts/Netherlands/05_Generate_PCID_Mapped.py`
   - Migration: Use `core.pcid_mapping.PCIDMapping` class

6. **Add validation steps** (Malaysia, Netherlands)
   - Files: Create `step_04_validate.py` (or `step_06_validate.py`)

#### Phase 3: Enhancements (Week 5-6)
7. **Add retry timestamp tracking**
   - Files: Create `sql/schemas/postgres/common.sql` → `step_retries` table

8. **Add browser instance counts per step**
   - Files: `core/chrome_pid_tracker.py`, `core/step_progress_logger.py`

9. **Standardize export location** (Netherlands → `exports/` subdirectory)
   - Files: `scripts/Netherlands/05_Generate_PCID_Mapped.py`

---

## Part E: Summary Tables

### E1. Compliance Matrix

| Standard | Malaysia | Argentina | Netherlands | Status |
|----------|----------|-----------|-------------|--------|
| **Postgres-only** | ⚠️ CSV input in Step 2 | ✅ Yes | ⚠️ CSV fallback in Step 1 | Partial |
| **CSV export persisted** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Compliant |
| **Step tracking (basic)** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Compliant |
| **Step tracking (enhanced)** | ❌ No | ❌ No | ❌ No | ❌ Missing |
| **Browser tracking** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Compliant |
| **Modular structure** | ✅ Yes | ❌ No | ❌ No | ⚠️ Partial |
| **Unified PCID** | ❌ No | ✅ Yes | ❌ No | ⚠️ Partial |

### E2. Unique Features Per Country

| Country | Unique Features |
|---------|----------------|
| **Malaysia** | • Modular `steps/` structure<br>• Bulk CSV download strategy<br>• Playwright with clean session management |
| **Argentina** | • Selenium multi-threaded workers<br>• API fallback for failed products<br>• Dedicated retry step (step 7)<br>• Dedicated validation step (step 8)<br>• Unified PCID mapping |
| **Netherlands** | • DB-first resume logic<br>• URL collection phase (Step 1)<br>• Optional reimbursement extraction (Step 2) |

---

## Part F: Recommendations

### F1. Immediate Actions (Must-Have)

1. **Enhance step tracking schema** (all pipelines)
   - Add `duration_seconds`, row metrics, `log_file_path` columns
   - Update `step_progress_logger.py` to populate new columns

2. **Remove CSV inputs** (Malaysia, Netherlands)
   - Migrate `products.csv` → `my_input_products` table
   - Migrate `search_terms.csv` → `nl_input_search_terms` table

3. **Add run-level aggregation** (all pipelines)
   - Add `slowest_step_number`, `failure_step_number` to `run_ledger`
   - Calculate during pipeline completion

### F2. Short-Term Actions (Good-to-Have)

4. **Migrate to modular structure** (Argentina, Netherlands)
   - Create `steps/` directories
   - Move scripts and update `run_pipeline_resume.py`

5. **Unify PCID mapping** (Malaysia, Netherlands)
   - Use `core.pcid_mapping.PCIDMapping` class
   - Remove country-specific tables/CSV reads

6. **Add validation steps** (Malaysia, Netherlands)
   - Create `step_04_validate.py` or `step_06_validate.py`
   - Mirror Argentina's step 8 logic

### F3. Long-Term Actions (Enhancements)

7. **Add retry timestamp tracking**
   - Create `step_retries` table
   - Track retry attempts with timestamps

8. **Add browser instance counts per step**
   - Update `step_progress_logger.py` to track browser spawns
   - Store in `browser_instances_spawned` column

9. **Standardize export locations**
   - Move Netherlands exports to `exports/` subdirectory
   - Update export report paths

---

## Part G: Additional Good-to-Have Features (Non-Breaking)

The following features enhance **observability**, **operations**, and **developer experience** without modifying scraping logic or business rules.

### G1. Observability & Monitoring

#### 1. **Real-Time Pipeline Dashboard** 🟢 HIGH VALUE
**Description:** Web-based dashboard showing live pipeline status, step progress, and metrics.

**Implementation:**
- Extend existing `scraper_gui.py` with real-time updates (WebSocket or polling)
- Display: current step, progress %, ETA, row counts, error rate
- Historical charts: step duration trends, success rate over time

**Benefits:**
- No code changes to pipelines (reads from DB only)
- Visual monitoring without SSH/log access
- Team visibility into pipeline health

**Tables Needed:**
- Use existing `run_ledger`, `*_step_progress` tables
- Optional: `pipeline_metrics` table for aggregated stats

---

#### 2. **Automated Alerting System** 🟢 HIGH VALUE
**Description:** Proactive notifications for failures, slowdowns, and anomalies.

**Implementation:**
- Extend `core/telegram_notifier.py` with alert rules
- Alert triggers:
  - Step failure (immediate)
  - Step duration > 2x historical average
  - Row count anomalies (sudden drop/increase)
  - Browser instance leaks (>10 instances)
  - Database connection failures

**Benefits:**
- Early detection of issues
- Reduces manual monitoring overhead
- No changes to scraping logic (post-step hooks only)

**Configuration:**
```python
# config/alerts.yaml
alerts:
  step_failure:
    enabled: true
    channels: ["telegram", "email"]
  performance_degradation:
    enabled: true
    threshold_multiplier: 2.0
    channels: ["telegram"]
```

---

#### 3. **Performance Benchmarking** 🟡 MEDIUM VALUE
**Description:** Track and compare step performance across runs.

**Implementation:**
- Create `pipeline_benchmarks` table:
  ```sql
  CREATE TABLE pipeline_benchmarks (
    id SERIAL PRIMARY KEY,
    scraper_name TEXT NOT NULL,
    step_number INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    duration_seconds REAL NOT NULL,
    rows_processed INTEGER,
    rows_per_second REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- Auto-populate from `*_step_progress` on completion
- Dashboard: show P50/P95/P99 durations, identify regressions

**Benefits:**
- Identify performance regressions early
- Set realistic SLAs per step
- No pipeline code changes (reads from step_progress)

---

#### 4. **Run Comparison Tool** 🟡 MEDIUM VALUE
**Description:** Side-by-side comparison of two pipeline runs.

**Implementation:**
- CLI tool: `python scripts/common/compare_runs.py Malaysia run_20260201_abc run_20260202_def`
- Compare: step durations, row counts, error rates, export file sizes
- Visual diff: highlight differences >10%

**Benefits:**
- Debug "why did this run take longer?"
- Validate improvements/changes
- No pipeline changes (analysis tool only)

---

### G2. Data Quality & Validation

#### 5. **Automated Data Quality Checks** 🟢 HIGH VALUE
**Description:** Pre-flight and post-run data quality validation.

**Implementation:**
- Create `data_quality_checks` table:
  ```sql
  CREATE TABLE data_quality_checks (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    scraper_name TEXT NOT NULL,
    check_type TEXT NOT NULL, -- 'preflight', 'postrun', 'export'
    check_name TEXT NOT NULL,
    status TEXT CHECK(status IN ('pass', 'fail', 'warning')),
    message TEXT,
    details_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- Checks:
  - **Pre-flight**: Input table row counts, PCID mapping coverage, DB connectivity
  - **Post-run**: Row count deltas, null rate, duplicate rate, export file integrity
  - **Export**: CSV row count matches DB, file size sanity checks

**Benefits:**
- Catch data issues before export delivery
- Automated QC gates (no manual inspection)
- No scraping changes (validation layer only)

**Integration:**
- Hook into `run_pipeline_resume.py` before step 0 and after final step
- Optional: Block export if critical checks fail

---

#### 6. **Export Validation & Verification** 🟡 MEDIUM VALUE
**Description:** Verify exports match database state and meet format requirements.

**Implementation:**
- Post-export validation:
  - CSV row count = DB row count (with tolerance for filtered rows)
  - Required columns present
  - Data types valid (no NaN in numeric fields)
  - Encoding correct (UTF-8-BOM for Excel compatibility)
  - File size reasonable (<100MB warning)

**Benefits:**
- Prevents corrupted exports reaching clients
- Automated format compliance
- No pipeline changes (post-export hook)

---

#### 7. **Anomaly Detection** 🟡 MEDIUM VALUE
**Description:** Detect unusual patterns in scraped data.

**Implementation:**
- Statistical checks:
  - Price outliers (3σ rule)
  - Sudden row count changes (>50% delta)
  - Missing data spikes (null rate >10%)
  - Duplicate rate anomalies

**Benefits:**
- Early detection of scraping issues
- Data quality monitoring
- No scraping logic changes (analysis layer)

---

### G3. Operational Improvements

#### 8. **Pipeline Scheduling** 🟢 HIGH VALUE
**Description:** Cron-like scheduling for automated pipeline runs.

**Implementation:**
- Create `pipeline_schedules` table:
  ```sql
  CREATE TABLE pipeline_schedules (
    id SERIAL PRIMARY KEY,
    scraper_name TEXT NOT NULL,
    schedule_cron TEXT NOT NULL, -- '0 2 * * *' = daily at 2 AM
    enabled BOOLEAN DEFAULT true,
    next_run_at TIMESTAMP,
    last_run_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- Scheduler service: `python scripts/common/scheduler.py` (runs as daemon)
- Integrate with existing `run_pipeline_resume.py`

**Benefits:**
- Automated daily/weekly runs
- No manual intervention needed
- No scraping changes (wrapper service)

---

#### 9. **Run Rollback Capability** 🟡 MEDIUM VALUE
**Description:** Revert to a previous run's state.

**Implementation:**
- Create `run_snapshots` table:
  ```sql
  CREATE TABLE run_snapshots (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    scraper_name TEXT NOT NULL,
    snapshot_type TEXT, -- 'before_step', 'after_step', 'final'
    step_number INTEGER,
    table_name TEXT NOT NULL,
    row_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- CLI: `python scripts/common/rollback.py Malaysia run_20260201_abc`
- Restore: Delete rows with `run_id > target_run_id`, restore from snapshot

**Benefits:**
- Quick recovery from bad runs
- Safe experimentation
- No pipeline changes (snapshot hooks only)

---

#### 10. **Automated Backup & Archive** 🟡 MEDIUM VALUE
**Description:** Automatic backup of critical data and exports.

**Implementation:**
- Backup strategy:
  - Daily: Full DB dump of last 7 days
  - Weekly: Archive exports older than 30 days
  - Monthly: Compress and move to cold storage
- CLI: `python scripts/common/backup.py --strategy daily`

**Benefits:**
- Disaster recovery
- Compliance (data retention)
- No pipeline changes (separate service)

---

#### 11. **Pipeline Health Checks** 🟢 HIGH VALUE
**Description:** Pre-flight checks before starting a pipeline run.

**Implementation:**
- Health checks:
  - Database connectivity
  - Disk space (>10GB free)
  - Memory available (>4GB)
  - Browser executable present
  - Input tables populated
  - Previous run not stale (<24h old)
- CLI: `python scripts/common/health_check.py Malaysia`
- Block run if critical checks fail

**Benefits:**
- Prevent failures from environment issues
- Clear error messages (not cryptic DB errors)
- No scraping changes (pre-flight wrapper)

---

### G4. Developer Experience

#### 12. **Pipeline Documentation Generator** 🟡 MEDIUM VALUE
**Description:** Auto-generate pipeline documentation from code.

**Implementation:**
- Parse `run_pipeline_resume.py` step definitions
- Extract docstrings from step scripts
- Generate Markdown: step descriptions, inputs/outputs, dependencies
- Output: `doc/{Country}/PIPELINE_AUTO.md`

**Benefits:**
- Always up-to-date docs
- Onboarding for new developers
- No pipeline changes (doc generator only)

---

#### 13. **Pipeline Testing Framework** 🟢 HIGH VALUE
**Description:** Smoke tests for pipeline steps.

**Implementation:**
- Test framework: `scripts/common/pipeline_tests.py`
- Tests:
  - Step 0: DB initialization succeeds
  - Step 1: Can connect to target website
  - Step 2: Can parse sample data
  - Step N: Export generation works
- Mock data: Use small test datasets
- CI integration: Run tests before merge

**Benefits:**
- Catch regressions early
- Safe refactoring
- No production pipeline changes (test layer)

---

#### 14. **Run Replay Tool** 🟡 MEDIUM VALUE
**Description:** Replay a previous run's steps with same inputs.

**Implementation:**
- CLI: `python scripts/common/replay_run.py Malaysia run_20260201_abc --step 2`
- Load: Input data from target run_id
- Execute: Same step script with same inputs
- Compare: Results vs original run

**Benefits:**
- Debug "why did step 2 fail?"
- Test fixes without full run
- No pipeline changes (replay wrapper)

---

### G5. Analytics & Reporting

#### 15. **Multi-Run Trend Analysis** 🟡 MEDIUM VALUE
**Description:** Analyze trends across multiple runs.

**Implementation:**
- Analytics queries:
  - Step duration trends (improving/degrading?)
  - Success rate over time
  - Data volume growth
  - Error pattern analysis
- Dashboard: Charts showing 30-day trends
- Alerts: Notify on negative trends

**Benefits:**
- Identify long-term issues
- Capacity planning
- No pipeline changes (analysis layer)

---

#### 16. **Cost Tracking** 🟡 MEDIUM VALUE
**Description:** Track resource usage and estimate costs.

**Implementation:**
- Track:
  - Browser instance hours
  - Database query count
  - Network bandwidth
  - Storage used
- Estimate: Cost per run, cost per country
- Report: Monthly cost summary

**Benefits:**
- Budget planning
- Optimization opportunities
- No pipeline changes (metrics collection)

---

#### 17. **Export Delivery Tracking** 🟡 MEDIUM VALUE
**Description:** Track export file delivery and client access.

**Implementation:**
- Create `export_deliveries` table:
  ```sql
  CREATE TABLE export_deliveries (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    scraper_name TEXT NOT NULL,
    export_file_path TEXT NOT NULL,
    delivery_method TEXT, -- 'email', 'sftp', 's3', 'manual'
    delivered_at TIMESTAMP,
    recipient TEXT,
    download_count INTEGER DEFAULT 0,
    last_downloaded_at TIMESTAMP
  );
  ```
- Track: When exports are delivered, who accessed them

**Benefits:**
- Audit trail
- Client communication
- No pipeline changes (delivery wrapper)

---

### G6. Integration & Automation

#### 18. **API Endpoints for Pipeline Control** 🟢 HIGH VALUE
**Description:** REST API for pipeline operations.

**Implementation:**
- Flask/FastAPI service: `scripts/common/pipeline_api.py`
- Endpoints:
  - `GET /api/v1/pipelines/{country}/status`
  - `POST /api/v1/pipelines/{country}/run`
  - `POST /api/v1/pipelines/{country}/stop`
  - `GET /api/v1/pipelines/{country}/runs/{run_id}/metrics`
- Authentication: API keys or OAuth

**Benefits:**
- Integration with external systems
- CI/CD integration
- No pipeline changes (API wrapper)

---

#### 19. **Webhook Notifications** 🟡 MEDIUM VALUE
**Description:** Send webhooks on pipeline events.

**Implementation:**
- Events: `pipeline.started`, `pipeline.completed`, `pipeline.failed`, `step.completed`
- Webhook config: Store URLs in `webhook_configs` table
- Retry: Exponential backoff on failure

**Benefits:**
- Integration with monitoring systems (Datadog, PagerDuty)
- Custom alerting
- No pipeline changes (event hooks)

---

#### 20. **Audit Logging** 🟢 HIGH VALUE
**Description:** Track who did what and when.

**Implementation:**
- Create `audit_log` table:
  ```sql
  CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    user TEXT, -- username or 'system'
    action TEXT NOT NULL, -- 'run_started', 'run_stopped', 'config_changed'
    scraper_name TEXT,
    run_id TEXT,
    details_json JSONB,
    ip_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- Log: All pipeline operations, config changes, manual interventions

**Benefits:**
- Compliance
- Debugging ("who stopped the pipeline?")
- Security audit trail
- No pipeline changes (logging hooks)

---

### G7. Feature Priority Matrix

| Feature | Value | Effort | Priority | Impact |
|---------|-------|--------|----------|--------|
| **Real-Time Dashboard** | High | Medium | 🔴 P0 | High visibility |
| **Automated Alerting** | High | Low | 🔴 P0 | Early issue detection |
| **Pipeline Health Checks** | High | Low | 🔴 P0 | Prevent failures |
| **Data Quality Checks** | High | Medium | 🟠 P1 | Catch data issues |
| **Pipeline Scheduling** | High | Medium | 🟠 P1 | Automation |
| **API Endpoints** | High | High | 🟠 P1 | Integration |
| **Audit Logging** | High | Low | 🟠 P1 | Compliance |
| **Performance Benchmarking** | Medium | Medium | 🟡 P2 | Optimization |
| **Run Comparison** | Medium | Low | 🟡 P2 | Debugging |
| **Export Validation** | Medium | Low | 🟡 P2 | Quality |
| **Pipeline Testing** | Medium | High | 🟡 P2 | Reliability |
| **Anomaly Detection** | Medium | Medium | 🟡 P2 | Quality |
| **Run Rollback** | Medium | Medium | 🟢 P3 | Recovery |
| **Multi-Run Trends** | Medium | Medium | 🟢 P3 | Analytics |
| **Documentation Generator** | Low | Low | 🟢 P3 | DX |
| **Cost Tracking** | Low | Medium | 🟢 P3 | Optimization |
| **Run Replay** | Low | Medium | 🟢 P3 | Debugging |
| **Webhook Notifications** | Low | Low | 🟢 P3 | Integration |
| **Backup & Archive** | Low | Medium | 🟢 P3 | Operations |
| **Export Delivery Tracking** | Low | Low | 🟢 P3 | Operations |

---

### G8. Implementation Strategy

**Phase 1: Quick Wins (Week 1-2)**
- Automated Alerting (extend Telegram notifier)
- Pipeline Health Checks (pre-flight wrapper)
- Audit Logging (logging hooks)

**Phase 2: High-Value Features (Week 3-4)**
- Real-Time Dashboard (extend GUI)
- Data Quality Checks (validation layer)
- Pipeline Scheduling (scheduler service)

**Phase 3: Integration & Polish (Week 5-6)**
- API Endpoints (REST API wrapper)
- Performance Benchmarking (metrics collection)
- Export Validation (post-export hooks)

**Phase 4: Nice-to-Have (Week 7+)**
- Run Comparison Tool
- Multi-Run Trends
- Documentation Generator
- Other P2/P3 features

---

## Conclusion

All three pipelines are **well-architected** and **mostly compliant** with Postgres standards. The main gaps are:

1. **Step tracking enhancements** (duration, row metrics, log paths) — missing in all three
2. **CSV input removal** — Malaysia Step 2, Netherlands Step 1
3. **Modular structure** — Argentina and Netherlands should adopt Malaysia's `steps/` pattern
4. **Unified PCID mapping** — Malaysia and Netherlands should use shared `pcid_mapping` table

The refactor plan prioritizes **critical observability gaps** first, then **standardization**, then **enhancements**.

---

---

## Part H: Implementation Checklist

### H1. Foundation Contracts (Week 1 - Lock In)

- [ ] **Schema Migration**: Create `005_add_step_tracking_columns.sql`
  - [ ] Add `duration_seconds` to all `*_step_progress` tables
  - [ ] Add row metrics columns (`rows_read`, `rows_processed`, etc.)
  - [ ] Add `log_file_path` to all `*_step_progress` tables
  - [ ] Add `browser_instances_spawned` to all `*_step_progress` tables
  - [ ] Add run-level aggregation columns to `run_ledger`
  - [ ] Create `step_retries` table

- [ ] **Step Event Hooks**: Create `core/step_hooks.py`
  - [ ] Define `StepMetrics` dataclass
  - [ ] Implement `StepHookRegistry` class
  - [ ] Add hook emission points in `run_pipeline_resume.py`

- [ ] **Preflight Health Checks**: Create `core/preflight_checks.py`
  - [ ] Implement `PreflightChecker` class
  - [ ] Implement all 6 required checks
  - [ ] Integrate into `run_pipeline_resume.py` as mandatory gate

- [ ] **Alerting Contract**: Create `core/alerting_contract.py`
  - [ ] Define `AlertRule` base class
  - [ ] Implement all 5 standard alert rules
  - [ ] Create `AlertRuleRegistry`

- [ ] **PCID Mapping Contract**: Create `core/pcid_mapping_contract.py`
  - [ ] Define `PCIDMappingInterface` ABC
  - [ ] Implement `SharedPCIDMapping` class
  - [ ] Update `core/pcid_mapping.py` to use contract
  - [ ] Document migration path for Malaysia/Netherlands

### H2. Critical Fixes (Week 2-3)

- [ ] **Populate Step Metrics**: Update `core/step_progress_logger.py`
  - [ ] Populate `duration_seconds` from checkpoint
  - [ ] Populate row metrics from step output
  - [ ] Populate `log_file_path` from step execution

- [ ] **Remove CSV Inputs**:
  - [ ] Malaysia: Migrate `products.csv` → `my_input_products` table
  - [ ] Netherlands: Migrate `search_terms.csv` → `nl_input_search_terms` table
  - [ ] Update step scripts to read from DB only

- [ ] **Run-Level Aggregation**: Update `core/run_ledger.py`
  - [ ] Calculate `slowest_step_number` on completion
  - [ ] Calculate `failure_step_number` on failure
  - [ ] Calculate `total_runtime_seconds`

### H3. Standardization (Week 4-5)

- [ ] **Modular Step Structure**:
  - [ ] Argentina: Create `steps/` directory, move scripts
  - [ ] Netherlands: Create `steps/` directory, move scripts
  - [ ] Update `run_pipeline_resume.py` to use `steps/` directory

- [ ] **Unified PCID Mapping**:
  - [ ] Malaysia: Migrate `my_pcid_reference` → `pcid_mapping`
  - [ ] Netherlands: Migrate CSV → `pcid_mapping`
  - [ ] Update step scripts to use `get_pcid_mapping()`

- [ ] **Validation Steps**:
  - [ ] Malaysia: Create `step_04_validate.py`
  - [ ] Netherlands: Create `step_04_validate.py`

### H4. Enhancements (Week 6+)

- [ ] **Retry Timestamp Tracking**: Populate `step_retries` table
- [ ] **Browser Instance Counts**: Track per step
- [ ] **Export Location Standardization**: Move Netherlands exports to `exports/`

---

**Document Version:** 2.0  
**Last Updated:** February 6, 2026  
**Changes:** Added Foundation Contracts section (C0-C4) and Implementation Checklist (Part H)
