#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Canada Ontario Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-3)
"""

import sys
import subprocess
import argparse
import os
import time
import csv
from pathlib import Path
from typing import List, Dict, Optional

# Add repo root and script dir to path (script dir first for config_loader)
_repo_root = Path(__file__).resolve().parents[2]
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Clear conflicting config_loader when run in same process as other scrapers (e.g. GUI)
if "config_loader" in sys.modules:
    del sys.modules["config_loader"]

from core.pipeline.pipeline_checkpoint import get_checkpoint_manager
from core.utils.logger import setup_standard_logger
from core.progress.progress_tracker import StandardProgress
from core.data.data_validator import validate_output
from config_loader import get_output_dir, getenv_bool, getenv_int, get_run_id, get_run_dir
from core.config.config_manager import ConfigManager

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

# Import step progress logger
try:
    from core.utils.step_progress_logger import log_step_progress, update_run_ledger_step_count
    _STEP_PROGRESS_AVAILABLE = True
except ImportError:
    _STEP_PROGRESS_AVAILABLE = False
    def log_step_progress(*args, **kwargs):
        return False
    def update_run_ledger_step_count(*args, **kwargs):
        return False


def _log_step_progress(step_num: int, step_name: str, status: str, error_message: str = None) -> None:
    """Persist step progress in PostgreSQL for Canada Ontario pipeline."""
    run_id = get_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    log_step_progress("CanadaOntario", run_id, step_num, step_name, status, error_message)


def _update_run_ledger_step_count(step_num: int) -> None:
    """Update run_ledger.step_count for the current run_id."""
    run_id = get_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    update_run_ledger_step_count("CanadaOntario", run_id, step_num)

RUN_ID = get_run_id()
RUN_DIR = get_run_dir(RUN_ID)
logger = setup_standard_logger(
    "canada_ontario_pipeline",
    scraper_name="CanadaOntario",
    log_file=RUN_DIR / "logs" / "pipeline.log",
)


def _is_process_running(pid: int) -> bool:
    try:
        if sys.platform == "win32":
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}"],
                capture_output=True,
                text=True,
                timeout=2,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
            return "No tasks" not in result.stdout and str(pid) in result.stdout
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _get_process_name(pid: int) -> str:
    if sys.platform != "win32":
        return ""
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            timeout=2,
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
        )
        output = result.stdout.strip().strip('"')
        if not output or "No tasks" in output:
            return ""
        return output.split('","')[0].strip('"')
    except Exception:
        return ""


def _acquire_lock(scraper_id: str) -> Optional[Path]:
    lock_file = ConfigManager.get_sessions_dir() / f"{scraper_id}.lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_force = getenv_bool("LOCK_FORCE", False)
    stale_seconds = getenv_int("LOCK_STALE_SECONDS", 21600)
    if lock_file.exists():
        try:
            with open(lock_file, "r", encoding="utf-8") as fh:
                lines = [line.strip() for line in fh.readlines() if line.strip()]
            if lines:
                pid = int(lines[0])
                lock_age = None
                if len(lines) > 1:
                    try:
                        lock_age = time.time() - float(lines[1])
                    except Exception:
                        lock_age = None
                if pid == os.getpid():
                    logger.info("Lock already held by current process at %s.", lock_file)
                    return lock_file
                if _is_process_running(pid):
                    process_name = _get_process_name(pid)
                    if lock_force:
                        logger.warning("Forcing lock override for %s (pid=%s) at %s.", scraper_id, pid, lock_file)
                    elif process_name and not process_name.lower().startswith("python"):
                        logger.warning("Stale lock detected (pid=%s, name=%s). Clearing lock.", pid, process_name)
                    elif lock_age is not None and lock_age > stale_seconds:
                        logger.warning("Stale lock older than %ss detected. Clearing lock.", stale_seconds)
                    else:
                        if not lock_file.exists():
                            logger.warning("Lock vanished during check for %s. Continuing.", scraper_id)
                        else:
                            logger.error("Lock exists for %s (pid=%s) at %s.", scraper_id, pid, lock_file)
                            return None
            lock_file.unlink()
        except Exception as exc:
            try:
                lock_file.unlink()
            except Exception:
                logger.error("Failed to clear stale lock at %s: %s", lock_file, exc)
                return None
    try:
        with open(lock_file, "w", encoding="utf-8") as fh:
            fh.write(f"{os.getpid()}\n{time.time()}\n")
        return lock_file
    except Exception as exc:
        logger.error("Failed to create lock at %s: %s", lock_file, exc)
        return None


def _release_lock(lock_file: Optional[Path]) -> None:
    if not lock_file:
        return
    for attempt in range(3):
        try:
            if lock_file.exists():
                lock_file.unlink()
            return
        except Exception:
            time.sleep(0.2 * (attempt + 1))


def _validate_output_files(output_files: Optional[List[str]]) -> List[str]:
    if not output_files:
        return []
    missing = []
    for f in output_files:
        path = Path(f)
        if not path.exists():
            missing.append(f"{f} (missing)")
            continue
        try:
            if path.stat().st_size <= 0:
                missing.append(f"{f} (empty)")
        except Exception:
            missing.append(f"{f} (unreadable)")
    return missing


def _validate_step1_db_only() -> List[str]:
    """When DB_ONLY, validate step 1 by checking co_products has rows instead of CSV files."""
    try:
        from core.db.postgres_connection import PostgresDB
        db = PostgresDB("CanadaOntario")
        db.connect()
        try:
            with db.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM co_products WHERE run_id = %s", (RUN_ID,))
                count = cur.fetchone()[0] or 0
            if count > 0:
                return []
            return ["co_products has 0 rows for this run"]
        finally:
            db.close()
    except Exception as e:
        return [f"DB validation failed: {e}"]


def _validate_final_output(path: Path) -> List[str]:
    required = {
        "PCID",
        "Country",
        "Company",
        "Generic Name",
        "Public With VAT Price",
        "LOCAL_PACK_CODE",
    }
    if not path.exists():
        return ["missing file"]
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, [])
    except Exception as exc:
        return [f"read error: {exc}"]
    missing = sorted(required.difference(header))
    return [f"missing columns: {', '.join(missing)}"] if missing else []


def run_step(step_meta: Dict, progress: StandardProgress):
    """Run a pipeline step and mark it complete if successful."""
    step_num = step_meta["id"]
    step_name = step_meta["name"]
    script_name = step_meta["script"]

    progress.update(step_num, message=f"starting {step_name}", force=True)
    logger.info("Starting step %s: %s", step_num, step_name)
    if step_meta.get("inputs"):
        logger.info("Step inputs: %s", step_meta["inputs"])
    if step_meta.get("outputs"):
        logger.info("Step outputs: %s", step_meta["outputs"])
    logger.info("Executing: %s", script_name)

    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        logger.error("Script not found: %s", script_path)
        return False
    
    try:
        started_at = time.perf_counter()
        env = os.environ.copy()
        env["PIPELINE_RUNNER"] = "1"
        env["RUN_ID"] = RUN_ID
        subprocess.run([sys.executable, "-u", str(script_path)], check=True, env=env)
        
        # Mark step as complete
        cp = get_checkpoint_manager("CanadaOntario")
        output_files = step_meta.get("outputs") or []
        # Step 1: when DB_ONLY, validate DB instead of CSV files
        db_only = getenv_bool("DB_ONLY", True)
        if step_num == 1 and db_only:
            missing = _validate_step1_db_only()
        else:
            missing = _validate_output_files(output_files)
        if missing:
            logger.error("Step %s outputs invalid: %s", step_name, "; ".join(missing))
            return False
        duration = time.perf_counter() - started_at
        cp.mark_step_complete(step_num, step_name, output_files, duration_seconds=duration)
        logger.info("Step %s completed in %.2fs", step_name, duration)
        progress.update(step_num + 1, message=f"completed {step_name}", force=True)
        
        # Log step progress to database
        _log_step_progress(step_num, step_name, "completed")
        _update_run_ledger_step_count(step_num + 1)
        
        # MEMORY FIX: Periodic resource monitoring
        try:
            from core.monitoring.resource_monitor import periodic_resource_check
            resource_status = periodic_resource_check("CanadaOntario", force=False)
            if resource_status.get("warnings"):
                for warning in resource_status["warnings"]:
                    logger.warning(f"[RESOURCE WARNING] {warning}")
        except Exception:
            pass
        
        return True
    except subprocess.CalledProcessError as e:
        error_msg = f"exit_code={e.returncode}"
        logger.error("Step %s failed with exit code %s", step_name, e.returncode)
        _log_step_progress(step_num, step_name, "failed", error_message=error_msg)
        return False
    except Exception as e:
        error_msg = str(e)
        logger.error("Step %s failed: %s", step_name, e)
        _log_step_progress(step_num, step_name, "failed", error_message=error_msg)
        return False

def main():
    parser = argparse.ArgumentParser(description="Canada Ontario Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-3)")
    parser.add_argument("--ignore-lock", action="store_true", help="Ignore existing lock (stale lock override)")
    parser.add_argument("--clear-step", type=int, choices=[1, 2, 3],
                        help="Clear data for a step (and optionally downstream) before running")
    parser.add_argument("--clear-downstream", action="store_true",
                        help="When used with --clear-step, also clear downstream steps")
    
    args = parser.parse_args()
    if args.ignore_lock:
        os.environ["LOCK_FORCE"] = "true"
    
    # Recover stale pipelines on startup (handles crash recovery)
    if _RECOVERY_AVAILABLE:
        try:
            recovery_result = recover_stale_pipelines(["CanadaOntario"])
            if recovery_result.get("total_recovered", 0) > 0:
                logger.info(f"Recovered {recovery_result['total_recovered']} stale pipeline state(s)")
        except Exception as e:
            logger.warning(f"Could not run startup recovery: {e}")
    
    cp = get_checkpoint_manager("CanadaOntario")
    run_dir = RUN_DIR
    
    # Add repo root for browser cleanup
    _repo_root = Path(__file__).resolve().parents[2]

    # Pre-flight health check (optional but recommended)
    health_required = getenv_bool("HEALTH_CHECK_REQUIRED", True)
    health_script = Path(__file__).parent / "health_check.py"
    if health_script.exists():
        logger.info("Running health check")
        result = subprocess.run([sys.executable, "-u", str(health_script)], capture_output=False)
        if result.returncode != 0:
            logger.error("Health check failed")
            if health_required:
                logger.error("Aborting pipeline. Set HEALTH_CHECK_REQUIRED=false to override.")
                return
            logger.warning("Continuing despite failure (HEALTH_CHECK_REQUIRED=false)")
    
    # Determine start step
    if args.fresh:
        cp.clear_checkpoint()
        start_step = 0
        logger.info("Starting fresh run (checkpoint cleared)")
    elif args.step is not None:
        start_step = args.step
        logger.info("Starting from step %s", start_step)
    else:
        # Resume from last completed step
        info = cp.get_checkpoint_info()
        start_step = info["next_step"]
        if info["total_completed"] > 0:
            logger.info("Resuming from step %s (last completed: step %s)", start_step, info["last_completed_step"])
        else:
            logger.info("Starting fresh run (no checkpoint found)")

    # Optional pre-clear of data for a step/run_id
    if args.clear_step is not None:
        run_id = get_run_id()
        if not run_id:
            logger.error("No run_id found. Run Step 0 first or set RUN_ID.")
            sys.exit(1)
        
        from core.db.connection import CountryDB
        from db.repositories import CanadaOntarioRepository

        db = CountryDB("CanadaOntario")
        repo = CanadaOntarioRepository(db, run_id)
        cleared = repo.clear_step_data(args.clear_step, include_downstream=args.clear_downstream)
        logger.info(f"[CLEAR] run_id={run_id} step={args.clear_step} downstream={args.clear_downstream}")
        for tbl, cnt in cleared.items():
            logger.info(f"  - {tbl}: deleted {cnt} rows")
    
    # Pre-run cleanup of any leftover browser PIDs for this scraper
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids("CanadaOntario", _repo_root, silent=True)
        except Exception:
            pass
    
    lock_file = _acquire_lock("CanadaOntario")
    if not lock_file:
        sys.exit(1)
    pipeline_start = time.perf_counter()
    
    # Define pipeline steps with their output files
    script_dir = Path(__file__).parent
    output_dir = get_output_dir()
    from config_loader import (
        get_central_output_dir,
        FINAL_REPORT_NAME_PREFIX,
        FINAL_REPORT_DATE_FORMAT,
        EAP_PRICES_CSV_NAME
    )
    from datetime import datetime
    central_output_dir = get_central_output_dir()  # exports/CanadaOntario/
    date_str = datetime.now().strftime(FINAL_REPORT_DATE_FORMAT)
    final_report_name = f"{FINAL_REPORT_NAME_PREFIX}{date_str}.csv"
    steps = [
        {
            "id": 0,
            "name": "Backup and Clean",
            "script": "00_backup_and_clean.py",
            "inputs": [],
            "outputs": [],
        },
        {
            "id": 1,
            "name": "Extract Product Details",
            "script": "01_extract_product_details.py",
            "inputs": [],
            "outputs": [
                str(output_dir / "products.csv"),
                str(output_dir / "manufacturer_master.csv"),
                str(output_dir / "completed_letters.json"),
            ],
        },
        {
            "id": 2,
            "name": "Extract EAP Prices",
            "script": "02_ontario_eap_prices.py",
            "inputs": [],
            "outputs": [
                str(output_dir / EAP_PRICES_CSV_NAME),
            ],
        },
        {
            "id": 3,
            "name": "Generate Final Output",
            "script": "03_GenerateOutput.py",
            "inputs": [
                str(output_dir / "products.csv"),
                str(output_dir / EAP_PRICES_CSV_NAME),
            ],
            "outputs": [
                str(central_output_dir / final_report_name),
            ],
        },
    ]
    try:
        cp.update_metadata({"steps": steps}, replace=False)
    except Exception:
        pass
    
    # Check all steps before start_step to find the earliest step that needs re-running
    earliest_rerun_step = None
    db_only = getenv_bool("DB_ONLY", True)
    for step in steps:
        step_num = step["id"]
        if step_num < start_step:
            step_complete = cp.is_step_complete(step_num)
            if not step_complete:
                earliest_rerun_step = step_num if earliest_rerun_step is None else min(earliest_rerun_step, step_num)
            else:
                if step_num == 1 and db_only:
                    missing = _validate_step1_db_only()
                else:
                    missing = _validate_output_files(step.get("outputs"))
                if missing:
                    logger.warning("Step %s marked complete but outputs missing: %s", step_num, "; ".join(missing))
                    earliest_rerun_step = step_num if earliest_rerun_step is None else min(earliest_rerun_step, step_num)
    
    if earliest_rerun_step is not None and earliest_rerun_step < start_step:
        logger.warning("Step %s needs to be re-run. Adjusting start step.", earliest_rerun_step)
        start_step = earliest_rerun_step
    
    # Run steps from start_step
    progress_state = run_dir / "logs" / "pipeline_progress.json"
    progress = StandardProgress("canada_ontario_pipeline", total=len(steps), unit="steps", logger=logger, state_path=progress_state)
    progress.update(start_step, message="resume", force=True)

    qa_enabled = getenv_bool("QA_CHECKS_ENABLED", True)

    try:
        for step in steps:
            if step["id"] < start_step:
                logger.info("Skipping step %s: %s (already completed)", step["id"], step["name"])
                continue

            success = run_step(step, progress)
            if not success:
                logger.error("Pipeline stopped at step %s", step["id"])
                sys.exit(1)

            if qa_enabled and step["id"] == 1 and not getenv_bool("DB_ONLY", True):
                try:
                    qa_result = validate_output(str(output_dir / "products.csv"), "CanadaOntario")
                    if not qa_result.get("valid", True):
                        logger.error("QA validation failed for products.csv: %s", qa_result.get("errors"))
                        sys.exit(1)
                    logger.info("QA validation passed for products.csv")
                except Exception as exc:
                    logger.error("QA validation failed: %s", exc)
                    sys.exit(1)
            if qa_enabled and step["id"] == 3:
                issues = _validate_final_output(Path(step["outputs"][0]))
                if issues:
                    logger.error("Final output validation failed: %s", "; ".join(issues))
                    sys.exit(1)
                logger.info("Final output validation passed")
    finally:
        _release_lock(lock_file)
    
    # Post-run cleanup of any leftover browser PIDs for this scraper
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids("CanadaOntario", _repo_root, silent=True)
        except Exception:
            pass
    
    total_duration = time.perf_counter() - pipeline_start
    progress.update(len(steps), message="pipeline completed", force=True)
    logger.info("Pipeline completed successfully in %.2fs", total_duration)
    logger.info("[DB] All data migrated to PostgreSQL database successfully")
    try:
        timing = cp.get_pipeline_timing()
        timing["total_duration_seconds"] = total_duration
        cp.update_metadata({"pipeline_timing": timing})
    except Exception:
        pass

if __name__ == "__main__":
    main()
