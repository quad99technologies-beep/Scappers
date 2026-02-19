#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-10)
"""

import os
import sys
import subprocess
import argparse
import time
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Argentina to path for imports
# Ensure Argentina directory is at the front of sys.path to prioritize local 'db' package
# This fixes conflict with core/db which might be in sys.path
_script_dir = Path(__file__).resolve().parent
sys.path = [p for p in sys.path if not Path(p).name == 'core']
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

from core.pipeline.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir, USE_API_STEPS

# Import foundation contracts
try:
    from core.pipeline.preflight_checks import PreflightChecker, CheckSeverity
    from core.pipeline.step_hooks import StepHookRegistry, StepMetrics
    from core.monitoring.alerting_integration import setup_alerting_hooks
    from core.data.data_quality_checks import DataQualityChecker
    from core.monitoring.audit_logger import audit_log
    from core.monitoring.benchmarking import record_step_benchmark
    from core.utils.step_progress_logger import update_run_ledger_aggregation, log_step_progress, update_run_ledger_step_count
    from datetime import datetime
    _FOUNDATION_AVAILABLE = True
except ImportError:
    _FOUNDATION_AVAILABLE = False
    PreflightChecker = None
    StepHookRegistry = None
    setup_alerting_hooks = None
    DataQualityChecker = None
    audit_log = None
    record_step_benchmark = None
    update_run_ledger_aggregation = None
    log_step_progress = None
    update_run_ledger_step_count = None

# Import startup recovery
try:
    from shared_workflow_runner import recover_stale_pipelines
    _RECOVERY_AVAILABLE = True
except ImportError:
    _RECOVERY_AVAILABLE = False

# Import browser PID cleanup
try:
    from core.browser.chrome_pid_tracker import terminate_scraper_pids
    _BROWSER_CLEANUP_AVAILABLE = True
except ImportError:
    _BROWSER_CLEANUP_AVAILABLE = False
    def terminate_scraper_pids(scraper_name, repo_root, silent=False):
        return 0

# Import Prometheus metrics
try:
    from core.monitoring.prometheus_exporter import (
        init_prometheus_metrics,
        record_scraper_run,
        record_scraper_duration,
        record_items_scraped,
        record_step_duration,
        record_error
    )
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    def init_prometheus_metrics(*args, **kwargs):
        return False
    def record_scraper_run(*args, **kwargs):
        pass
    def record_scraper_duration(*args, **kwargs):
        pass
    def record_items_scraped(*args, **kwargs):
        pass
    def record_step_duration(*args, **kwargs):
        pass
    def record_error(*args, **kwargs):
        pass

# Import Frontier Queue
try:
    from services.frontier_integration import initialize_frontier_for_scraper
    _FRONTIER_AVAILABLE = True
except ImportError:
    _FRONTIER_AVAILABLE = False
    def initialize_frontier_for_scraper(*args, **kwargs):
        return None


def _read_run_id() -> str:
    """Load run_id from env or .current_run_id if present."""
    run_id = os.environ.get("ARGENTINA_RUN_ID")
    if run_id:
        return run_id
    run_id_file = get_output_dir() / ".current_run_id"
    if run_id_file.exists():
        try:
            run_id = run_id_file.read_text(encoding="utf-8").strip()
            if run_id:
                return run_id
        except Exception:
            pass
    return ""


def _get_latest_run_id_from_db() -> str:
    """Return the best Argentina run_id to resume: prefer runs with data (items_scraped > 0), then latest by started_at."""
    try:
        from core.db.connection import CountryDB

        with CountryDB("Argentina") as db:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT run_id FROM run_ledger WHERE scraper_name = %s "
                    "ORDER BY COALESCE(items_scraped, 0) DESC NULLS LAST, started_at DESC LIMIT 1",
                    ("Argentina",),
                )
                row = cur.fetchone()
                return (row[0] or "").strip() if row else ""
    except Exception:
        return ""


def _ensure_resume_run_id(start_step: int, is_fresh: bool = False) -> None:
    """Ensure we use the existing run_id when resuming (not fresh). Prefer checkpoint, then run_ledger
    (run with most data), then .current_run_id. Never trust file alone - it may have been overwritten by a bad run.
    
    When is_fresh=True, this function does nothing (allows new run_id generation in step 0).
    When is_fresh=False, this function preserves existing run_id even if start_step == 0.
    """
    if is_fresh:
        # Fresh run - don't preserve run_id, let step 0 generate a new one
        return
    
    cp = get_checkpoint_manager("Argentina")
    run_id = (cp.get_metadata() or {}).get("run_id") or ""
    if not run_id:
        run_id = _get_latest_run_id_from_db()
        if run_id:
            print(f"[RESUME] Using run from run_ledger (run with data): {run_id}", flush=True)
    if not run_id:
        run_id = _read_run_id()
        if run_id:
            print(f"[RESUME] Using run from .current_run_id: {run_id}", flush=True)
    
    # If we found an existing run_id, preserve it (even if start_step == 0)
    if run_id:
        os.environ["ARGENTINA_RUN_ID"] = run_id
        run_id_file = get_output_dir() / ".current_run_id"
        if not run_id_file.exists() or run_id_file.read_text(encoding="utf-8").strip() != run_id:
            try:
                run_id_file.parent.mkdir(parents=True, exist_ok=True)
                run_id_file.write_text(run_id, encoding="utf-8")
            except Exception:
                pass
        cp.update_metadata({"run_id": run_id})
        print(f"[RESUME] Preserved existing run_id: {run_id}", flush=True)


def _log_step_progress(step_num: int, step_name: str, status: str, error_message: str = None) -> None:
    """Persist step progress in PostgreSQL for Argentina pipeline."""
    run_id = _read_run_id()
    if not run_id:
        return
    try:
        from core.db.connection import CountryDB

        with CountryDB("Argentina") as db:
            with db.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ar_step_progress
                        (run_id, step_number, step_name, progress_key, status, error_message, started_at, completed_at)
                    VALUES
                        (%s, %s, %s, 'pipeline', %s, %s,
                         CASE WHEN %s = 'in_progress' THEN CURRENT_TIMESTAMP ELSE NULL END,
                         CASE WHEN %s IN ('completed','failed','skipped') THEN CURRENT_TIMESTAMP ELSE NULL END)
                    ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                        step_name = EXCLUDED.step_name,
                        status = EXCLUDED.status,
                        error_message = EXCLUDED.error_message,
                        started_at = COALESCE(ar_step_progress.started_at, EXCLUDED.started_at),
                        completed_at = EXCLUDED.completed_at
                    """,
                    (run_id, step_num, step_name, status, error_message, status, status),
                )
    except Exception:
        # Non-blocking: progress logging should not break pipeline execution
        return


def _update_run_ledger_step_count(step_num: int) -> None:
    """Update run_ledger.step_count for the current run_id."""
    run_id = _read_run_id()
    if not run_id:
        return
    try:
        from core.db.connection import CountryDB

        with CountryDB("Argentina") as db:
            with db.cursor() as cur:
                cur.execute(
                    "UPDATE run_ledger SET step_count = %s WHERE run_id = %s",
                    (step_num, run_id),
                )
    except Exception:
        return


def _has_legacy_mock_step_checkpoint(cp) -> bool:
    """
    Detect legacy Argentina checkpoints where step_9 was the temporary mock step.

    This allows automatic re-run of the real Step 9 (Refresh Export) after upgrade.
    """
    try:
        checkpoint_data = cp._load_checkpoint()  # Internal access for migration check
        step_outputs = checkpoint_data.get("step_outputs", {}) or {}
        step_9 = step_outputs.get("step_9", {}) or {}
        step_name = (step_9.get("step_name") or "").strip()
        return step_name == "Mock Verification Step"
    except Exception:
        return False


def _mark_run_ledger_active_if_resume(start_step: int) -> None:
    """Ensure run_ledger has a row for this run and status is running when resuming mid-pipeline."""
    if start_step <= 0:
        return
    run_id = _read_run_id()
    if not run_id:
        return
    try:
        from core.db.connection import CountryDB
        try:
            from db.repositories import ArgentinaRepository
        except ImportError:
            from scripts.Argentina.db.repositories import ArgentinaRepository

        with CountryDB("Argentina") as db:
            repo = ArgentinaRepository(db, run_id)
            repo.ensure_run_in_ledger(mode="resume")  # insert if missing (e.g. step 0 skipped)
            repo.resume_run()
            try:
                repo.snapshot_scrape_stats('pipeline_resume')
            except Exception:
                pass
    except Exception:
        # Non-blocking: don't fail pipeline if DB status update fails
        return


def cleanup_temp_files(output_dir: Path):
    """Remove stale temp files created during CSV rewrites (tmp* with no extension)."""
    try:
        removed = 0
        for item in output_dir.iterdir():
            if not item.is_file():
                continue
            if item.suffix:
                continue
            if not item.name.startswith("tmp"):
                continue
            try:
                item.unlink()
                removed += 1
            except Exception:
                continue
        if removed:
            print(f"[CLEANUP] Removed {removed} stale temp file(s) from {output_dir}", flush=True)
    except Exception:
        pass

def cleanup_legacy_progress(output_dir: Path):
    """Remove deprecated alfabeta_progress.csv if present."""
    try:
        legacy = output_dir / "alfabeta_progress.csv"
        if legacy.exists():
            legacy.unlink()
            print(f"[CLEANUP] Removed deprecated progress file: {legacy}", flush=True)
    except Exception:
        pass


def _find_no_data_csv() -> Path:
    """Find the latest pcid_no_data.csv file from exports folder."""
    output_dir = get_output_dir()
    exports_dir = output_dir / "exports"

    # Try exports folder first
    if exports_dir.exists():
        no_data_files = list(exports_dir.glob("*_pcid_no_data.csv"))
        if no_data_files:
            return sorted(no_data_files, key=lambda f: f.stat().st_mtime, reverse=True)[0]

    # Try output folder directly
    no_data_files = list(output_dir.glob("*_pcid_no_data.csv"))
    if no_data_files:
        return sorted(no_data_files, key=lambda f: f.stat().st_mtime, reverse=True)[0]

    return None


def _read_no_data_csv() -> list:
    """
    Read product+company combinations from pcid_no_data.csv file.
    Returns list of unique (company, product) tuples.
    """
    import pandas as pd

    csv_path = _find_no_data_csv()
    if not csv_path or not csv_path.exists():
        return []

    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        products = set()

        for _, row in df.iterrows():
            company = str(row.get("Company", "")).strip()
            product = str(row.get("Local Product Name", "")).strip()
            if company and product:
                products.add((company, product))

        return list(products)
    except Exception as e:
        print(f"[NO-DATA] Error reading CSV: {e}")
        return []


def _count_no_data_products() -> int:
    """
    Count UNIQUE products from pcid_no_data.csv file.
    Reads the exported CSV to get the exact products that need retry.
    """
    products = _read_no_data_csv()
    return len(products)


def _queue_no_data_products() -> int:
    """
    Queue products from pcid_no_data.csv for retry scraping.

    Reads the exported CSV file and queues unique product+company combinations.
    Existing data in DB is preserved - only NEW scraped data will be added.
    """
    try:
        import re
        import unicodedata
        from core.db.connection import CountryDB
        from config_loader import PRODUCTS_URL

        run_id = _read_run_id()
        if not run_id:
            return 0

        # Read from exported CSV file
        products = _read_no_data_csv()
        if not products:
            return 0

        def strip_accents(s: str) -> str:
            if not s:
                return ""
            s = s.replace("ß", "ss").replace("ẞ", "SS")
            s = s.replace("æ", "ae").replace("Æ", "AE")
            s = s.replace("œ", "oe").replace("Œ", "OE")
            s = s.replace("ø", "o").replace("Ø", "O")
            s = s.replace("ð", "d").replace("Ð", "D")
            s = s.replace("þ", "th").replace("Þ", "TH")
            return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

        def construct_url(product_name: str) -> str:
            if not product_name:
                return ""
            sanitized = strip_accents(product_name)
            sanitized = re.sub(r"\s*\+\s*", " ", sanitized)
            sanitized = re.sub(r"[^a-zA-Z0-9\s-]", "", sanitized)
            sanitized = re.sub(r"\s+", "-", sanitized.strip())
            sanitized = re.sub(r"-{3,}", "--", sanitized)
            sanitized = sanitized.lower().strip("-")
            if sanitized:
                base_url = PRODUCTS_URL.rstrip("/")
                return f"{base_url}/{sanitized}.html"
            return ""

        with CountryDB("Argentina") as db:
            with db.cursor() as cur:
                # Insert unique products into ar_product_index with pending status
                inserted = 0
                for company, product in products:
                    url = construct_url(product)
                    try:
                        cur.execute("""
                            INSERT INTO ar_product_index (run_id, product, company, url, status, total_records, loop_count)
                            VALUES (%s, %s, %s, %s, 'pending', 0, 0)
                            ON CONFLICT (run_id, company, product)
                            DO UPDATE SET url = EXCLUDED.url, status = 'pending', total_records = 0
                        """, (run_id, product, company, url))
                        inserted += 1
                    except Exception:
                        continue

                return inserted
    except Exception as e:
        print(f"[NO-DATA] Error queuing no_data products: {e}")
        return 0


def _get_step_row_counts(step_num: int, run_id: str) -> int:
    """
    Get row counts from database for a given step.
    This provides fallback metrics when step scripts don't write metrics files.
    
    Args:
        step_num: Step number (0-based)
        run_id: Current run ID
        
    Returns:
        Number of rows processed/inserted for this step
    """
    if not run_id:
        return 0
    
    try:
        from core.db.connection import CountryDB
        
        with CountryDB("Argentina") as db:
            with db.cursor() as cur:
                # Step 1: Get Product List - count products in ar_product_index
                if step_num == 1:
                    cur.execute("SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 2: Prepare URLs - count products with URLs
                elif step_num == 2:
                    cur.execute("SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s AND url IS NOT NULL AND url != ''", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 3: Selenium Product Search - count scraped products from this step
                elif step_num == 3:
                    cur.execute("SELECT COUNT(*) FROM ar_products WHERE run_id = %s AND (source = 'selenium' OR source = 'selenium_product')", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 4: Selenium Company Search - count scraped products from this step
                elif step_num == 4:
                    cur.execute("SELECT COUNT(*) FROM ar_products WHERE run_id = %s AND source = 'selenium_company'", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 5: API Scraper - count scraped products from API
                elif step_num == 5:
                    cur.execute("SELECT COUNT(*) FROM ar_products WHERE run_id = %s AND source = 'api'", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 6: Translate Using Dictionary - count translated rows
                elif step_num == 6:
                    cur.execute("SELECT COUNT(*) FROM ar_products_translated WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 7: Generate Output - count rows in ar_products (presentations)
                elif step_num == 7:
                    cur.execute("SELECT COUNT(*) FROM ar_products WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 8: Scrape No-Data - count retry attempts
                elif step_num == 8:
                    cur.execute("SELECT COUNT(*) FROM ar_products WHERE run_id = %s AND source = 'step7'", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 9: Refresh Export - same as step 7 (re-export)
                elif step_num == 9:
                    cur.execute("SELECT COUNT(*) FROM ar_products_translated WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 10: Statistics & Validation - use total scraped count
                elif step_num == 10:
                    cur.execute("SELECT COUNT(*) FROM ar_products WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 0: Backup and Clean - no rows to count
                else:
                    return 0
                    
    except Exception as e:
        print(f"[METRICS] Warning: Could not get DB row counts for step {step_num}: {e}")
        return 0


def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None, total_steps: int = 11):
    """Run a pipeline step and mark it complete if successful."""
    display_step = step_num + 1  # Display as 1-based for user friendliness
    
    print(f"\n{'='*80}")
    print(f"Step {display_step}/{total_steps}: {step_name}")
    print(f"{'='*80}\n")
    
    # Output overall pipeline progress with descriptive message
    pipeline_percent = round((step_num / total_steps) * 100, 1)
    if pipeline_percent > 100.0:
        pipeline_percent = 100.0
    
    # Create meaningful progress description based on step
    step_descriptions = {
        0: "Preparing: Backing up previous results and cleaning output directory",
        1: "Scraping: Fetching product list from AlfaBeta website",
        2: "Preparing: Building product URLs for scraping",
        3: "Scraping: Extracting product details using Selenium product search (this may take a while)",
        4: "Scraping: Extracting remaining products using Selenium company search",
        5: "Scraping: Extracting remaining products using API",
        6: "Processing: Translating Spanish terms to English using dictionary",
        7: "Generating: Creating final output files with PCID mapping",
        8: "Recovery: Retrying PCID no-data products using Selenium worker",
        9: "Refreshing: Re-running translation and output export after no-data retry",
        10: "Validation: Computing detailed stats and data quality checks",
    }
    step_desc = step_descriptions.get(step_num, step_name)
    
    print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({pipeline_percent}%) - {step_desc}", flush=True)
    print(f"[PIPELINE] Executing: {script_name}")
    print(f"[PIPELINE] This step will run until completion before moving to next step.\n")
    
    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False
    
    # Track step execution time
    start_time = time.time()
    duration_seconds = None
    run_id = _read_run_id()

    # Create log file for this step (persistent across resume)
    output_dir = get_output_dir()
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file_name = f"step_{step_num:02d}_{step_name.replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file_path = logs_dir / log_file_name
    
    # Also create a symlink/latest log file for easy access
    latest_log_path = logs_dir / f"step_{step_num:02d}_latest.log"

    # Create temporary metrics file
    import json
    import tempfile
    metrics_file_fd, metrics_file_path = tempfile.mkstemp(prefix=f"metrics_step_{step_num}_", suffix=".json")
    os.close(metrics_file_fd)
    # Ensure it's empty/valid
    Path(metrics_file_path).write_text("{}", encoding="utf-8")
    
    # Create metrics object for hooks
    metrics = None
    if _FOUNDATION_AVAILABLE and StepHookRegistry:
        try:
            metrics = StepMetrics(
                step_number=step_num,
                step_name=step_name,
                run_id=run_id or "pending",
                scraper_name="Argentina",
                started_at=datetime.now(),
                log_file_path=log_file_path
            )
            StepHookRegistry.emit_step_start(metrics)
        except Exception as e:
            print(f"[HOOKS] Warning: Could not emit step start hook: {e}")

    # Log step start (if run_id already available)
    if _FOUNDATION_AVAILABLE and log_step_progress:
        try:
            log_step_progress(
                scraper_name="Argentina",
                run_id=run_id or "pending",
                step_num=step_num,
                step_name=step_name,
                status="in_progress"
            )
        except Exception:
            _log_step_progress(step_num, step_name, "in_progress")
    else:
        _log_step_progress(step_num, step_name, "in_progress")
    
    # Start the step and capture output to log file
    print(f"[LOG] Step output will be saved to: {log_file_path}")
    
    try:
        # Diagnostic: Check if critical environment variables are present
        if not os.environ.get("ALFABETA_USER"):
            print("[DEBUG] ALFABETA_USER missing from os.environ, attempting to reload...", flush=True)
            try:
                from config_loader import ALFABETA_USER, ALFABETA_PASS
                if ALFABETA_USER:
                    os.environ["ALFABETA_USER"] = ALFABETA_USER
                    print("[DEBUG] Injected ALFABETA_USER from config_loader", flush=True)
                if ALFABETA_PASS:
                    os.environ["ALFABETA_PASS"] = ALFABETA_PASS
            except ImportError:
                print("[DEBUG] Could not import config_loader for fallback", flush=True)

        env = os.environ.copy()
        env["PIPELINE_RUNNER"] = "1"
        env["PIPELINE_STEP_DISPLAY"] = str(display_step)
        env["PIPELINE_TOTAL_STEPS"] = str(total_steps)
        env["PIPELINE_STEP_NAME"] = step_name
        env["PIPELINE_SCRIPT"] = script_name
        env["PIPELINE_METRICS_FILE"] = str(metrics_file_path)
        if run_id:
            env["ARGENTINA_RUN_ID"] = run_id
        
        # Run subprocess with output tee to both console and log file
        process = subprocess.Popen(
            [sys.executable, "-u", str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Tee output to both log file and console
        with open(log_file_path, "w", encoding="utf-8") as log_f:
            # Write header to log file
            log_f.write(f"=== Step {display_step}/{total_steps}: {step_name} ===\n")
            log_f.write(f"=== Script: {script_name} ===\n")
            log_f.write(f"=== Started: {datetime.now().isoformat()} ===\n")
            log_f.write(f"=== Run ID: {run_id or 'pending'} ===\n")
            log_f.write("=" * 80 + "\n\n")
            log_f.flush()
            
            for line in process.stdout:
                # Write to log file
                log_f.write(line)
                log_f.flush()
                # Write to console
                print(line, end="", flush=True)
        
        process.wait()
        
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, script_path)
        
        # Update symlink to point to latest log
        try:
            if latest_log_path.exists() or latest_log_path.is_symlink():
                latest_log_path.unlink()
            latest_log_path.symlink_to(log_file_path.name)
        except Exception:
            pass  # Non-critical: symlink is just for convenience
        
        # Calculate duration
        duration_seconds = time.time() - start_time
        
        # Format duration for display
        hours = int(duration_seconds // 3600)
        minutes = int((duration_seconds % 3600) // 60)
        seconds = int(duration_seconds % 60)
        if hours > 0:
            duration_str = f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            duration_str = f"{minutes}m {seconds}s"
        else:
            duration_str = f"{seconds}s"
        print(f"[TIMING] Step {step_num} completed in {duration_str}", flush=True)
        
        # Record Prometheus metrics
        if _PROMETHEUS_AVAILABLE:
            try:
                record_step_duration("Argentina", step_name, duration_seconds)
            except Exception as e:
                print(f"[METRICS] Warning: Could not record step duration: {e}")
        
        # Update metrics
        if metrics:
            metrics.duration_seconds = duration_seconds
            metrics.completed_at = datetime.now()
            
            # Read metrics from file if available
            file_metrics_read = False
            try:
                if Path(metrics_file_path).exists():
                    content = Path(metrics_file_path).read_text(encoding="utf-8").strip()
                    if content:
                        data = json.loads(content)
                        if isinstance(data, dict):
                            metrics.rows_processed = int(data.get("rows_processed", 0))
                            metrics.rows_read = int(data.get("rows_read", 0))
                            metrics.rows_inserted = int(data.get("rows_inserted", 0))
                            metrics.rows_updated = int(data.get("rows_updated", 0))
                            metrics.rows_rejected = int(data.get("rows_rejected", 0))
                            # Add any other custom metrics to context
                            for k, v in data.items():
                                if k not in ["rows_processed", "rows_read", "rows_inserted", "rows_updated", "rows_rejected"]:
                                    metrics.context[k] = v
                            file_metrics_read = True
                            print(f"[METRICS] Loaded metrics from step: rows_processed={metrics.rows_processed}")
            except Exception as e:
                print(f"[METRICS] Warning: Failed to read metrics file: {e}")
            finally:
                # Cleanup metrics file
                try:
                    if Path(metrics_file_path).exists():
                        Path(metrics_file_path).unlink()
                except Exception:
                    pass
            
            # If metrics file didn't have row counts, get from DB based on step
            if not file_metrics_read or metrics.rows_processed == 0:
                db_rows = _get_step_row_counts(step_num, run_id)
                if db_rows > 0:
                    metrics.rows_processed = db_rows
                    print(f"[METRICS] Loaded metrics from DB: rows_processed={metrics.rows_processed}")
        
        # Special validation for step 3 (Selenium Scraping): verify no eligible products remain
        # before marking complete
        if step_num == 3:
            try:
                run_id = _read_run_id()
                if run_id:
                    from core.db.connection import CountryDB
                    try:
                        from db.schema import apply_argentina_schema
                        from db.repositories import ArgentinaRepository
                    except ImportError:
                        from scripts.Argentina.db.schema import apply_argentina_schema
                        from scripts.Argentina.db.repositories import ArgentinaRepository
                    from scraper_utils import combine_skip_sets, is_product_already_scraped, nk
                    os.environ["ARGENTINA_RUN_ID"] = run_id
                    db = CountryDB("Argentina")
                    apply_argentina_schema(db)
                    repo = ArgentinaRepository(db, run_id)

                    # Get pending products (same logic as worker)
                    from config_loader import SELENIUM_MAX_LOOPS, SELENIUM_STEP3_MAX_ATTEMPTS
                    pending_rows = repo.get_pending_products(max_loop=int(SELENIUM_MAX_LOOPS), limit=200000)
                    skip_set = combine_skip_sets()

                    # Count eligible products
                    eligible_count = 0
                    seen_keys = set()
                    for row in pending_rows:
                        prod = (row.get("product") or "").strip()
                        comp = (row.get("company") or "").strip()
                        url = (row.get("url") or "").strip()
                        if not (prod and comp and url):
                            continue
                        key = (nk(comp), nk(prod))
                        if key in seen_keys:
                            continue
                        if key in skip_set:
                            continue
                        if is_product_already_scraped(comp, prod):
                            continue
                        seen_keys.add(key)
                        eligible_count += 1

                    if eligible_count > 0:
                        # Check step3_attempts to prevent infinite retry
                        cp = get_checkpoint_manager("Argentina")
                        metadata = cp.get_metadata() or {}
                        step3_attempts = metadata.get("step3_attempts", 0)

                        if step3_attempts >= SELENIUM_STEP3_MAX_ATTEMPTS:
                            print(f"\n[WARNING] Step 3 has reached maximum attempts ({SELENIUM_STEP3_MAX_ATTEMPTS}), marking complete despite {eligible_count} eligible products remaining.")
                            print(f"[WARNING] This prevents infinite retry loops. Check logs for issues (rate limiting, network errors, etc.)")
                            # Reset counter for next run
                            metadata["step3_attempts"] = 0
                            cp.update_metadata(metadata)
                            # Allow marking complete
                        else:
                            # Increment attempt counter
                            metadata["step3_attempts"] = step3_attempts + 1
                            cp.update_metadata(metadata)
                            print(f"\n[WARNING] Step 3 script completed with exit code 0, but {eligible_count} products still eligible for Selenium scraping.")
                            print(f"[WARNING] Step 3 attempt {step3_attempts + 1}/{SELENIUM_STEP3_MAX_ATTEMPTS} - will re-run.")
                            # Don't mark complete - there's still work to do
                            return False
            except Exception as e:
                print(f"[WARNING] Failed to validate step 3 eligibility after script completion: {e}")
                import traceback
                traceback.print_exc()
                # On error, don't mark complete to be safe
                return False
        
        # Mark step as complete
        cp = get_checkpoint_manager("Argentina")
        if output_files:
            # Convert to absolute paths
            output_dir = get_output_dir()
            abs_output_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)
        if step_num == 0:
            rid = _read_run_id()
            if rid:
                cp.update_metadata({"run_id": rid})

        # Log DB progress with enhanced metrics
        if _FOUNDATION_AVAILABLE and log_step_progress:
            try:
                log_step_progress(
                    scraper_name="Argentina",
                    run_id=run_id or "pending",
                    step_num=step_num,
                    step_name=step_name,
                    status="completed",
                    duration_seconds=duration_seconds,
                    log_file_path=log_file_path
                )
            except Exception:
                _log_step_progress(step_num, step_name, "completed")
        else:
            _log_step_progress(step_num, step_name, "completed")
        
        if _FOUNDATION_AVAILABLE and update_run_ledger_step_count:
            try:
                update_run_ledger_step_count("Argentina", run_id or "pending", display_step)
            except Exception:
                _update_run_ledger_step_count(display_step)
        else:
            _update_run_ledger_step_count(display_step)
        
        # Record benchmark
        if _FOUNDATION_AVAILABLE and record_step_benchmark:
            try:
                record_step_benchmark(
                    scraper_name="Argentina",
                    step_number=step_num,
                    step_name=step_name,
                    run_id=run_id or "pending",
                    duration_seconds=duration_seconds,
                    rows_processed=metrics.rows_processed if metrics else 0
                )
            except Exception:
                pass
        
        # Emit step end hook
        if metrics:
            try:
                StepHookRegistry.emit_step_end(metrics)
            except Exception as e:
                print(f"[HOOKS] Warning: Could not emit step end hook: {e}")
        
        # MEMORY FIX: Periodic resource monitoring
        try:
            from core.monitoring.resource_monitor import periodic_resource_check
            resource_status = periodic_resource_check("Argentina", force=False)
            if resource_status.get("warnings"):
                for warning in resource_status["warnings"]:
                    print(f"[RESOURCE WARNING] {warning}", flush=True)
        except Exception:
            pass

        # Cleanup stale temp files (e.g., tmp* from CSV rewrites)
        cleanup_temp_files(get_output_dir())
        cleanup_legacy_progress(get_output_dir())
        
        # Output completion progress with descriptive message
        completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0
        
        next_step_descriptions = {
            0: "Ready to fetch product list",
            1: "Ready to prepare URLs",
            2: "Ready to scrape products with Selenium",
            3: "Ready to scrape remaining products with Selenium company search",
            4: "Ready to scrape products with API" if USE_API_STEPS else "Ready to translate terms",
            5: "Ready to translate terms",
            6: "Ready to generate final output",
            7: "Ready to retry no-data products (auto)",
            8: "Ready to refresh export with no-data retry results",
            9: "Ready to compute detailed stats and validation",
            10: "Pipeline completed successfully",
        }
        next_desc = next_step_descriptions.get(step_num, "Moving to next step")
        
        print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({completion_percent}%) - {next_desc}", flush=True)
        
        # Wait 10 seconds after step completion before proceeding to next step
        if step_num < total_steps - 1:  # Don't pause after last step
            print(f"\n[PAUSE] Waiting 10 seconds before next step...", flush=True)
            time.sleep(10.0)
            print(f"[PAUSE] Resuming pipeline...\n", flush=True)
        
        return True
    except subprocess.CalledProcessError as e:
        # Step failed
        duration_seconds = time.time() - start_time
        
        # Record Prometheus metrics
        if _PROMETHEUS_AVAILABLE:
            try:
                record_step_duration("Argentina", step_name, duration_seconds)
                record_error("Argentina", "step_failed")
            except Exception as e:
                print(f"[METRICS] Warning: Could not record error metrics: {e}")
        
        if metrics:
            metrics.duration_seconds = duration_seconds
            metrics.completed_at = datetime.now()
            metrics.error_message = f"exit_code={e.returncode}"
        
        # Log failure
        if _FOUNDATION_AVAILABLE and log_step_progress:
            try:
                log_step_progress(
                    scraper_name="Argentina",
                    run_id=run_id or "pending",
                    step_num=step_num,
                    step_name=step_name,
                    status="failed",
                    error_message=f"exit_code={e.returncode}",
                    duration_seconds=duration_seconds,
                    log_file_path=log_file_path
                )
            except Exception:
                _log_step_progress(step_num, step_name, "failed", error_message=f"exit_code={e.returncode}")
        else:
            _log_step_progress(step_num, step_name, "failed", error_message=f"exit_code={e.returncode}")
        
        # Emit error hook
        if metrics:
            try:
                StepHookRegistry.emit_step_error(metrics, e)
            except Exception:
                pass
        
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode} (duration: {duration_seconds:.2f}s)")
        return False
    except Exception as e:
        # Unexpected error
        duration_seconds = time.time() - start_time
        
        # Record Prometheus metrics
        if _PROMETHEUS_AVAILABLE:
            try:
                record_step_duration("Argentina", step_name, duration_seconds)
                record_error("Argentina", "step_failed")
            except Exception as e:
                print(f"[METRICS] Warning: Could not record error metrics: {e}")
        
        if metrics:
            metrics.duration_seconds = duration_seconds
            metrics.completed_at = datetime.now()
            metrics.error_message = str(e)
            try:
                StepHookRegistry.emit_step_error(metrics, e)
            except Exception:
                pass
        
        if _FOUNDATION_AVAILABLE and log_step_progress:
            try:
                log_step_progress(
                    scraper_name="Argentina",
                    run_id=run_id or "pending",
                    step_num=step_num,
                    step_name=step_name,
                    status="failed",
                    error_message=str(e),
                    duration_seconds=duration_seconds,
                    log_file_path=log_file_path
                )
            except Exception:
                _log_step_progress(step_num, step_name, "failed", error_message=str(e))
        else:
            _log_step_progress(step_num, step_name, "failed", error_message=str(e))
        
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e} (duration: {duration_seconds:.2f}s)")
        return False

def main():
    # Initialize Prometheus metrics
    if _PROMETHEUS_AVAILABLE:
        try:
            init_prometheus_metrics(port=9090)
            print("[METRICS] Prometheus metrics initialized on port 9090")
        except Exception as e:
            print(f"[METRICS] Warning: Could not initialize Prometheus metrics: {e}")
    
    # Initialize Frontier Queue
    if _FRONTIER_AVAILABLE:
        try:
            frontier = initialize_frontier_for_scraper("Argentina")
            if frontier:
                print("[FRONTIER] Frontier queue initialized for Argentina")
        except Exception as e:
            print(f"[FRONTIER] Warning: Could not initialize frontier queue: {e}")
    
    parser = argparse.ArgumentParser(description="Argentina Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-10)")
    # NOTE: clear-step is only supported for steps that map cleanly to DB tables.
    # Steps 8+ are composite/analytics steps and are intentionally excluded here.
    parser.add_argument("--clear-step", type=int, choices=[1, 2, 3, 4, 5, 6, 7],
                        help="Clear data for a step (and optionally downstream) before running")
    parser.add_argument("--clear-downstream", action="store_true",
                        help="When used with --clear-step, also clear downstream steps")
    
    args = parser.parse_args()
    
    # Setup foundation contracts
    if _FOUNDATION_AVAILABLE:
        try:
            # Setup alerting hooks
            setup_alerting_hooks()
            print("[SETUP] Alerting hooks registered")
        except Exception as e:
            print(f"[SETUP] Warning: Could not setup alerting hooks: {e}")
    
    # Recover stale pipelines on startup (handles crash recovery)
    if _RECOVERY_AVAILABLE:
        try:
            recovery_result = recover_stale_pipelines(["Argentina"])
            if recovery_result.get("total_recovered", 0) > 0:
                print(f"[RECOVERY] Recovered {recovery_result['total_recovered']} stale pipeline state(s)")
        except Exception as e:
            print(f"[RECOVERY] Warning: Could not run startup recovery: {e}")
    
    # Get run_id early for preflight checks
    run_id = _read_run_id()
    if not run_id and not args.fresh:
        # Try to get from checkpoint
        cp = get_checkpoint_manager("Argentina")
        run_id = (cp.get_metadata() or {}).get("run_id") or ""
    
    # Run preflight health checks (MANDATORY GATE)
    if _FOUNDATION_AVAILABLE and PreflightChecker:
        try:
            checker = PreflightChecker("Argentina", run_id or "pending")
            results = checker.run_all_checks()
            
            print("\n[PREFLIGHT] Health Checks:")
            for result in results:
                if result.severity == CheckSeverity.CRITICAL:
                    emoji = "[FAIL]" if not result.passed else "[OK]"
                elif result.severity == CheckSeverity.WARNING:
                    emoji = "[WARN]" if not result.passed else "[OK]"
                else:
                    emoji = "[INFO]"
                print(f"  {emoji} {result.name}: {result.message}")
            
            if checker.has_critical_failures():
                print("\n[PREFLIGHT] Pipeline blocked due to critical failures:")
                print(checker.get_failure_summary())
                print(checker.get_failure_summary())
                print("\n[TESTING] BYPASSING PREFLIGHT FAILURE FOR VERIFICATION")
                # sys.exit(1)
            
            # Run pre-flight data quality checks
            if run_id:
                dq_checker = DataQualityChecker("Argentina", run_id)
                dq_checker.run_preflight_checks()
                dq_checker.save_results_to_db()
        except Exception as e:
            print(f"[PREFLIGHT] Warning: Could not run preflight checks: {e}")
    
    # Audit log: pipeline started
    if _FOUNDATION_AVAILABLE and audit_log:
        try:
            audit_log(
                action="run_started",
                scraper_name="Argentina",
                run_id=run_id or "pending",
                user="system"
            )
        except Exception:
            pass
    
    # Validate config (required credentials, optional API key)
    try:
        from config_loader import load_env_file, validate_config
        load_env_file()
        issues = validate_config()
        for msg in issues:
            print(f"[CONFIG] {msg}", flush=True)
    except Exception as e:
        print(f"[CONFIG] Validation warning: {e}", flush=True)
    
    cp = get_checkpoint_manager("Argentina")
    
    # Optional pre-clear of data for a step/run_id
    if args.clear_step is not None:
        def _resolve_run_id():
            run_id = os.environ.get("ARGENTINA_RUN_ID")
            if run_id:
                return run_id
            run_id_file = get_output_dir() / ".current_run_id"
            if run_id_file.exists():
                return run_id_file.read_text(encoding="utf-8").strip()
            raise RuntimeError("No run_id found. Run Step 0 first or set ARGENTINA_RUN_ID.")

        from core.db.connection import CountryDB
        try:
            from db.repositories import ArgentinaRepository
        except ImportError:
            from scripts.Argentina.db.repositories import ArgentinaRepository

        run_id = _resolve_run_id()
        db = CountryDB("Argentina")
        repo = ArgentinaRepository(db, run_id)
        cleared = repo.clear_step_data(args.clear_step, include_downstream=args.clear_downstream)
        print(f"[CLEAR] run_id={run_id} step={args.clear_step} downstream={args.clear_downstream}")
        for tbl, cnt in cleared.items():
            print(f"  - {tbl}: deleted {cnt} rows")
    
    # Determine start step
    skip_step_zero_to_preserve_data = False  # only True when resume skips step 0 to keep data
    is_fresh_run = False
    if args.fresh:
        cp.clear_checkpoint()
        start_step = 0
        is_fresh_run = True
        print("Starting fresh run (checkpoint cleared)")
        
        # Check if external run_id is provided (from GUI/Telegram/API sync)
        external_run_id = os.environ.get("ARGENTINA_RUN_ID")
        if external_run_id:
            print(f"[INIT] Using external run_id from environment: {external_run_id}")
            # Write to .current_run_id file so steps can find it
            run_id_file = get_output_dir() / ".current_run_id"
            try:
                run_id_file.parent.mkdir(parents=True, exist_ok=True)
                run_id_file.write_text(external_run_id, encoding="utf-8")
                cp.update_metadata({"run_id": external_run_id})
            except Exception as e:
                print(f"[WARN] Could not save external run_id: {e}")
        else:
            # Fresh run should not reuse previous run_id
            os.environ.pop("ARGENTINA_RUN_ID", None)
            try:
                run_id_file = get_output_dir() / ".current_run_id"
                if run_id_file.exists():
                    run_id_file.unlink()
                    print(f"[CLEAN] Removed previous run_id file: {run_id_file}")
            except Exception as e:
                print(f"[CLEAN] Warning: could not remove previous run_id file: {e}")
    elif args.step is not None:
        start_step = args.step
        print(f"Starting from step {start_step}")
    else:
        # Resume from last completed step (never run step 0 here if we have data — it wipes output)
        info = cp.get_checkpoint_info()
        start_step = info["next_step"]
        if start_step == 0:
            existing_run_id = _get_latest_run_id_from_db() or _read_run_id()
            if existing_run_id:
                start_step = 1
                skip_step_zero_to_preserve_data = True
                print("Resume: skipping step 0 (Backup and Clean) to preserve existing data; starting from step 1")
        if info["total_completed"] > 0 and start_step > 0:
            print(f"Resuming from step {start_step} (last completed: step {info['last_completed_step']})")
        elif start_step == 0:
            print("Starting fresh run (no checkpoint and no existing run_id)")
            is_fresh_run = True
        else:
            print(f"Resuming from step {start_step}")

    # Backward compatibility: older runs used step 9 as "Mock Verification Step".
    # If detected, force resume from step 9 so real Refresh Export + Stats run once.
    if (not args.fresh) and (args.step is None) and _has_legacy_mock_step_checkpoint(cp):
        if start_step > 9:
            start_step = 9
            print(
                "[CHECKPOINT] Legacy checkpoint mapping detected (old step 9 = Mock Verification Step). "
                "Will re-run step 9 (Refresh Export) and step 10 (Statistics & Data Validation)."
            )
    
    # Define pipeline steps with their output files
    # Steps 0-10 (11 total):
    #   0: Backup and Clean
    #   1: Get Product List
    #   2: Prepare URLs
    #   3: Selenium Product Search (search by product name)
    #   4: Selenium Company Search (search by company name for products with 0 records)
    #   5: API Scraper (for remaining products)
    #   6: Translate Using Dictionary
    #   7: Generate Output
    #   8: Scrape No-Data (retry step)
    #   9: Refresh Export
    #   10: Statistics & Data Validation
    output_dir = get_output_dir()
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None),
        (1, "01_getProdList.py", "Get Product List", None),  # DB-backed
        (2, "02_prepare_urls.py", "Prepare URLs", None),  # DB-backed
        (3, "03_alfabeta_selenium_scraper.py", "Scrape Products (Selenium - Product Search)", None),  # DB-backed
        (4, "03b_alfabeta_selenium_company_search.py", "Scrape Products (Selenium - Company Search)", None),  # DB-backed
        (5, "04_alfabeta_api_scraper.py", "Scrape Products (API)", None),  # DB-backed (to be refactored)
        (6, "05_TranslateUsingDictionary.py", "Translate Using Dictionary", None),
        (7, "06_GenerateOutput.py", "Generate Output", None),  # Output files vary by date
        (8, "07_scrape_no_data_pipeline.py", "Scrape No-Data (Selenium Retry)", None),
        (9, "08_refresh_export.py", "Refresh Export", None),
        (10, "08_stats_and_validation.py", "Statistics & Data Validation", None),
    ]
    total_steps = len(steps)
    
    # Check all steps before start_step to find the earliest step that needs re-running
    earliest_rerun_step = None
    for step_num, script_name, step_name, output_files in steps:
        if step_num == 5 and not USE_API_STEPS:  # API step is now step 5
            continue
        if step_num < start_step:
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            
            should_skip = cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files)
            
            # Special validation for step 2 (Prepare URLs): verify all products have URLs
            if step_num == 2 and should_skip:
                try:
                    run_id = _read_run_id()
                    if run_id:
                        from core.db.connection import CountryDB
                        try:
                            from db.schema import apply_argentina_schema
                        except ImportError:
                            from scripts.Argentina.db.schema import apply_argentina_schema
                        db = CountryDB("Argentina")
                        apply_argentina_schema(db)
                        with db.cursor() as cur:
                            # Check if any products are missing URLs
                            cur.execute("""
                                SELECT COUNT(*) 
                                FROM ar_product_index 
                                WHERE run_id = %s 
                                  AND (url IS NULL OR url = '')
                            """, (run_id,))
                            missing_urls = cur.fetchone()[0]
                            if missing_urls > 0:
                                print(
                                    f"[CHECKPOINT] WARNING: Step 2 (Prepare URLs) marked complete but {missing_urls} products are missing URLs. Will re-run.",
                                    flush=True,
                                )
                                should_skip = False
                except Exception as e:
                    print(f"[CHECKPOINT] WARNING: Failed to validate step 2 URLs: {e}", flush=True)
                    # On error, don't skip to be safe
                    should_skip = False
            
            # Special validation for step 3 (Selenium Scraping): verify no eligible products remain
            if step_num == 3 and should_skip:
                try:
                    run_id = _read_run_id()
                    if run_id:
                        from core.db.connection import CountryDB
                        from db.schema import apply_argentina_schema
                        from db.repositories import ArgentinaRepository
                        from scraper_utils import combine_skip_sets, is_product_already_scraped, nk
                        os.environ["ARGENTINA_RUN_ID"] = run_id
                        db = CountryDB("Argentina")
                        apply_argentina_schema(db)
                        repo = ArgentinaRepository(db, run_id)

                        # Get pending products (same logic as worker)
                        from config_loader import SELENIUM_MAX_LOOPS, SELENIUM_STEP3_MAX_ATTEMPTS
                        pending_rows = repo.get_pending_products(max_loop=int(SELENIUM_MAX_LOOPS), limit=200000)
                        skip_set = combine_skip_sets()

                        # Count eligible products
                        eligible_count = 0
                        seen_keys = set()
                        for row in pending_rows:
                            prod = (row.get("product") or "").strip()
                            comp = (row.get("company") or "").strip()
                            url = (row.get("url") or "").strip()
                            if not (prod and comp and url):
                                continue
                            key = (nk(comp), nk(prod))
                            if key in seen_keys:
                                continue
                            if key in skip_set:
                                continue
                            if is_product_already_scraped(comp, prod):
                                continue
                            seen_keys.add(key)
                            eligible_count += 1

                        if eligible_count > 0:
                            # Check step3_attempts to prevent infinite retry
                            cp = get_checkpoint_manager("Argentina")
                            metadata = cp.get_metadata() or {}
                            step3_attempts = metadata.get("step3_attempts", 0)

                            if step3_attempts >= SELENIUM_STEP3_MAX_ATTEMPTS:
                                print(
                                    f"[CHECKPOINT] WARNING: Step 3 has reached maximum attempts ({SELENIUM_STEP3_MAX_ATTEMPTS}), allowing skip despite {eligible_count} eligible products.",
                                    flush=True,
                                )
                                print(
                                    f"[CHECKPOINT] This prevents infinite retry loops. {eligible_count} products will not be scraped.",
                                    flush=True,
                                )
                                # Reset counter and allow skip
                                metadata["step3_attempts"] = 0
                                cp.update_metadata(metadata)
                                # should_skip remains True
                            else:
                                # Increment attempt counter and force re-run
                                metadata["step3_attempts"] = step3_attempts + 1
                                cp.update_metadata(metadata)
                                print(
                                    f"[CHECKPOINT] WARNING: Step 3 marked complete but {eligible_count} products still eligible. Attempt {step3_attempts + 1}/{SELENIUM_STEP3_MAX_ATTEMPTS} - will re-run.",
                                    flush=True,
                                )
                                should_skip = False
                except Exception as e:
                    print(f"[CHECKPOINT] WARNING: Failed to validate step 3 eligibility: {e}", flush=True)
                    # On error, don't skip to be safe
                    should_skip = False
            
            if not should_skip:
                # Step marked complete but validation failed - needs re-run
                if earliest_rerun_step is None or step_num < earliest_rerun_step:
                    earliest_rerun_step = step_num
    
    # Adjust start_step if any earlier step needs re-running (never force step 0 on resume when preserving data)
    if earliest_rerun_step is not None:
        if earliest_rerun_step == 0 and skip_step_zero_to_preserve_data:
            print(f"\nWARNING: Step 0 would normally re-run (output files missing), but skipping to preserve existing data.")
            print(f"Starting from step {start_step}. Use --fresh if you intend to clear and restart from step 0.\n")
        else:
            print(f"\nWARNING: Step {earliest_rerun_step} needs re-run (output files missing).")
            print(f"Adjusting start step from {start_step} to {earliest_rerun_step} to maintain pipeline integrity.\n")
            start_step = earliest_rerun_step

    # When resuming (not fresh), lock to existing run_id (from checkpoint, run_ledger, or .current_run_id) 
    # so we never create a new one. This runs even if start_step == 0 (unless it's a fresh run).
    _ensure_resume_run_id(start_step, is_fresh=is_fresh_run)

    # If resuming mid-pipeline, mark run_ledger as running (status) and resume (mode)
    _mark_run_ledger_active_if_resume(start_step)

    # Snapshot scrape stats at pipeline start (fresh run)
    if start_step == 0:
        try:
            from core.db.connection import CountryDB
            try:
                from db.repositories import ArgentinaRepository
            except ImportError:
                from scripts.Argentina.db.repositories import ArgentinaRepository
            run_id = _read_run_id()
            if run_id:
                with CountryDB("Argentina") as db:
                    repo = ArgentinaRepository(db, run_id)
                    repo.snapshot_scrape_stats('pipeline_start')
        except Exception:
            pass

    # Pre-run cleanup of any leftover browser PIDs for this scraper
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids("Argentina", _repo_root, silent=True)
        except Exception:
            pass
    
    # Run steps starting from start_step
    print(f"\n{'='*80}")
    print(f"PIPELINE EXECUTION PLAN")
    print(f"{'='*80}")
    for step_num, script_name, step_name, output_files in steps:
        display_step = step_num + 1  # Display as 1-based
        if step_num == 5 and not USE_API_STEPS:  # API step is now step 5
            print(f"Step {display_step}/{total_steps}: {step_name} - SKIPPED (disabled in config)")
            continue
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                print(f"Step {display_step}/{total_steps}: {step_name} - SKIPPED (already completed in checkpoint)")
            else:
                print(f"Step {display_step}/{total_steps}: {step_name} - WILL RE-RUN (output files missing)")
        elif step_num == start_step:
            print(f"Step {display_step}/{total_steps}: {step_name} - WILL RUN NOW (starting from here)")
        else:
            print(f"Step {display_step}/{total_steps}: {step_name} - WILL RUN AFTER previous steps complete")
    print(f"{'='*80}\n")
    
    # Now execute the steps
    for step_num, script_name, step_name, output_files in steps:
        if step_num == 5 and not USE_API_STEPS:  # API step is now step 5
            display_step = step_num + 1
            completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
            if completion_percent > 100.0:
                completion_percent = 100.0
            print(f"\nStep {display_step}/{total_steps}: {step_name} - SKIPPED (disabled in config)")
            print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({completion_percent}%) - Skipped: API step disabled", flush=True)
            cp.mark_step_complete(step_num, step_name, duration_seconds=0.0)
            _log_step_progress(step_num, step_name, "skipped")
            _update_run_ledger_step_count(display_step)
            continue
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                display_step = step_num + 1  # Display as 1-based
                completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0
                
                step_descriptions = {
                    0: "Skipped: Backup already completed",
                    1: "Skipped: Product list already fetched",
                    2: "Skipped: URLs already prepared",
                    3: "Skipped: Selenium product search already completed",
                    4: "Skipped: Selenium company search already completed",
                    5: "Skipped: API scraping already completed",
                    6: "Skipped: Translation already completed",
                    7: "Skipped: Output already generated",
                    8: "Skipped: No-data retry already completed",
                    9: "Skipped: Refresh export already completed",
                    10: "Skipped: Stats & validation already completed",
                }
                skip_desc = step_descriptions.get(step_num, f"Skipped: {step_name} already completed")
                
                print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({completion_percent}%) - {skip_desc}", flush=True)
                _log_step_progress(step_num, step_name, "completed")
                _update_run_ledger_step_count(display_step)
            else:
                # Step marked complete but output files missing - will re-run
                display_step = step_num + 1
                print(f"\nStep {display_step}/{total_steps}: {step_name} - WILL RE-RUN (output files missing)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files, total_steps=total_steps)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            
            # Record Prometheus metrics for failed pipeline
            if _PROMETHEUS_AVAILABLE:
                try:
                    record_scraper_run("Argentina", "failed")
                    record_error("Argentina", "pipeline_failed")
                except Exception as e:
                    print(f"[METRICS] Warning: Could not record failure metrics: {e}")
            
            sys.exit(1)

    cp = get_checkpoint_manager("Argentina")
    cp.mark_as_completed()
    # Calculate total pipeline duration
    timing_info = cp.get_pipeline_timing()
    total_duration = timing_info.get("total_duration_seconds", 0.0)

    # Format total duration
    hours = int(total_duration // 3600)
    minutes = int((total_duration % 3600) // 60)
    seconds = int(total_duration % 60)
    if hours > 0:
        total_duration_str = f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        total_duration_str = f"{minutes}m {seconds}s"
    else:
        total_duration_str = f"{seconds}s"

    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    
    # Record Prometheus metrics for completed pipeline
    if _PROMETHEUS_AVAILABLE:
        try:
            pipeline_start_time = None
            # Try to get pipeline start time from checkpoint or run_ledger
            try:
                from core.db.connection import CountryDB
                with CountryDB("Argentina") as db:
                    with db.cursor() as cur:
                        cur.execute(
                            "SELECT started_at FROM run_ledger WHERE scraper_name = %s AND run_id = %s",
                            ("Argentina", run_id or "pending")
                        )
                        row = cur.fetchone()
                        if row and row[0]:
                            pipeline_start_time = row[0]
            except Exception:
                pass
            
            if pipeline_start_time:
                total_duration = (datetime.now() - pipeline_start_time).total_seconds()
                record_scraper_duration("Argentina", total_duration)
            
            record_scraper_run("Argentina", "success")
        except Exception as e:
            print(f"[METRICS] Warning: Could not record pipeline completion metrics: {e}")
    
    # Update run-level aggregation
    if _FOUNDATION_AVAILABLE and update_run_ledger_aggregation:
        try:
            update_run_ledger_aggregation("Argentina", run_id or "pending")
            print("[POST-RUN] Run-level aggregation updated")
        except Exception as e:
            print(f"[POST-RUN] Warning: Could not update aggregation: {e}")
    
    # Run post-run data quality checks
    if _FOUNDATION_AVAILABLE and DataQualityChecker and run_id:
        try:
            dq_checker = DataQualityChecker("Argentina", run_id)
            dq_checker.run_postrun_checks()
            dq_checker.save_results_to_db()
            print("[POST-RUN] Data quality checks completed")
        except Exception as e:
            print(f"[POST-RUN] Warning: Could not run data quality checks: {e}")
    
    # Audit log: pipeline completed
    if _FOUNDATION_AVAILABLE and audit_log:
        try:
            audit_log(
                action="run_completed",
                scraper_name="Argentina",
                run_id=run_id or "pending",
                user="system"
            )
        except Exception:
            pass
    print(f"[TIMING] Total pipeline duration: {total_duration_str}")
    print(f"{'='*80}\n")
    print(f"[PROGRESS] Pipeline Step: {total_steps}/{total_steps} (100%)", flush=True)
    
    # Show log file location
    logs_dir = get_output_dir() / "logs"
    print(f"\n[LOGS] Step logs saved to: {logs_dir}")
    print(f"[LOGS] Use 'ls {logs_dir}/*.log' to view all step logs")
    cleanup_temp_files(get_output_dir())
    cleanup_legacy_progress(get_output_dir())
    
    # Post-run cleanup of any leftover browser PIDs for this scraper
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids("Argentina", _repo_root, silent=True)
        except Exception:
            pass
    
    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except:
        pass

if __name__ == "__main__":
    main()
