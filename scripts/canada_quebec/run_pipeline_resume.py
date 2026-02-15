#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CanadaQuebec Pipeline Runner with Resume/Checkpoint Support

Aligned with Malaysia/Argentina/NorthMacedonia patterns:
  - Step progress DB logging
  - Prometheus metrics
  - Recovery of stale pipelines
  - Log tee to file + console
  - Step hooks & alerting
  - Benchmarking & run ledger aggregation

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-6)
"""

import os
import sys
import subprocess
import argparse
import time
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Path wiring
# ---------------------------------------------------------------------------
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# ---------------------------------------------------------------------------
# Core imports
# ---------------------------------------------------------------------------
from core.pipeline.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_csv_output_dir, get_split_pdf_dir, DB_ENABLED
from db_handler import DBHandler

SCRAPER_NAME = "CanadaQuebec"
MAX_STEPS = 7  # steps 0-6

# ---------------------------------------------------------------------------
# Foundation contracts (same imports as Malaysia/Argentina/NorthMacedonia)
# ---------------------------------------------------------------------------
try:
    from core.pipeline.preflight_checks import PreflightChecker, CheckSeverity
    from core.step_hooks import StepHookRegistry, StepMetrics
    from core.alerting_integration import setup_alerting_hooks
    from core.data.data_quality_checks import DataQualityChecker
    from core.audit_logger import audit_log
    from core.monitoring.benchmarking import record_step_benchmark
    from core.utils.step_progress_logger import update_run_ledger_aggregation
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

# Import startup recovery
try:
    from shared_workflow_runner import recover_stale_pipelines
    _RECOVERY_AVAILABLE = True
except ImportError:
    _RECOVERY_AVAILABLE = False

# Import browser PID cleanup (kept for consistency even though CQ doesn't use browsers)
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

# Import Prometheus metrics
try:
    from core.monitoring.prometheus_exporter import (
        init_prometheus_metrics,
        record_scraper_run,
        record_scraper_duration,
        record_items_scraped,
        record_step_duration,
        record_error,
    )
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    def init_prometheus_metrics(*args, **kwargs): return False
    def record_scraper_run(*args, **kwargs): pass
    def record_scraper_duration(*args, **kwargs): pass
    def record_items_scraped(*args, **kwargs): pass
    def record_step_duration(*args, **kwargs): pass
    def record_error(*args, **kwargs): pass


# ---------------------------------------------------------------------------
# Run-ID management (aligned with Malaysia/NorthMacedonia)
# ---------------------------------------------------------------------------

def _get_output_dir() -> Path:
    """Return the main output directory for CanadaQuebec."""
    # Reuse the config_loader output (CSV output dir parent)
    csv_dir = get_csv_output_dir()
    return csv_dir.parent  # output/CanadaQuebec


def _read_run_id() -> str:
    """Load run_id from env or .current_run_id if present."""
    run_id = os.environ.get("PIPELINE_RUN_ID")
    if run_id:
        return run_id
    run_id_file = _get_output_dir() / ".current_run_id"
    if run_id_file.exists():
        try:
            run_id = run_id_file.read_text(encoding="utf-8").strip()
            if run_id:
                return run_id
        except Exception:
            pass
    return ""


def _get_latest_run_id_from_db() -> str:
    """Return the best CanadaQuebec run_id to resume."""
    try:
        from core.db.connection import CountryDB
        with CountryDB(SCRAPER_NAME) as db:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT run_id FROM run_ledger WHERE scraper_name = %s "
                    "ORDER BY COALESCE(items_scraped, 0) DESC NULLS LAST, started_at DESC LIMIT 1",
                    (SCRAPER_NAME,),
                )
                row = cur.fetchone()
                return (row[0] or "").strip() if row else ""
    except Exception:
        return ""


def _ensure_resume_run_id(start_step: int) -> None:
    """When resuming (start_step > 0), ensure we reuse the existing run_id."""
    if start_step <= 0:
        return
    cp = get_checkpoint_manager(SCRAPER_NAME)
    run_id = (cp.get_metadata() or {}).get("run_id") or ""
    if not run_id:
        run_id = _get_latest_run_id_from_db()
        if run_id:
            print(f"[RESUME] Using run from run_ledger (run with data): {run_id}", flush=True)
    if not run_id:
        run_id = _read_run_id()
        if run_id:
            print(f"[RESUME] Using run from .current_run_id: {run_id}", flush=True)
    if run_id:
        os.environ["PIPELINE_RUN_ID"] = run_id
        run_id_file = _get_output_dir() / ".current_run_id"
        if not run_id_file.exists() or run_id_file.read_text(encoding="utf-8").strip() != run_id:
            try:
                run_id_file.parent.mkdir(parents=True, exist_ok=True)
                run_id_file.write_text(run_id, encoding="utf-8")
            except Exception:
                pass
        cp.update_metadata({"run_id": run_id})


def _log_step_progress(step_num: int, step_name: str, status: str, error_message: str = None) -> None:
    """Persist step progress in PostgreSQL."""
    run_id = _read_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    log_step_progress(SCRAPER_NAME, run_id, step_num, step_name, status, error_message)


def _update_run_ledger_step_count(step_num: int) -> None:
    """Update run_ledger.step_count for the current run_id."""
    run_id = _read_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    update_run_ledger_step_count(SCRAPER_NAME, run_id, step_num)


# ---------------------------------------------------------------------------
# Step runner (with log tee + DB tracking — aligned with NorthMacedonia)
# ---------------------------------------------------------------------------

def run_step(step_num: int, script_name: str, step_name: str,
             output_files: list = None, allow_failure: bool = False):
    """Run a pipeline step, tee output to file + console, log to DB."""
    display_step = step_num + 1

    print(f"\n{'='*80}")
    print(f"Step {display_step}/{MAX_STEPS}: {step_name}")
    print(f"{'='*80}\n")

    pipeline_percent = round((step_num / MAX_STEPS) * 100, 1)

    step_descriptions = {
        0: "Preparing: Backing up previous results and cleaning output directory",
        1: "Processing: Splitting PDF into separate annexe files",
        2: "Validating: Checking PDF structure (optional step)",
        3: "Extracting: Processing Annexe IV.1 with AI (this may take a while)",
        4: "Extracting: Processing Annexe IV.2 with AI (this may take a while)",
        5: "Extracting: Processing Annexe V pages (this may take a while)",
        6: "Generating: Merging all annexes into final output",
    }
    step_desc = step_descriptions.get(step_num, step_name)

    print(f"[PROGRESS] Pipeline Step: {display_step}/{MAX_STEPS} ({pipeline_percent}%) - {step_desc}", flush=True)

    # Update checkpoint metadata
    cp = get_checkpoint_manager(SCRAPER_NAME)
    run_id = _read_run_id()
    cp.update_metadata({"current_step": step_num, "current_step_name": step_name, "status": "running"})

    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False

    start_time = time.time()

    # Create log file for this step
    output_dir = _get_output_dir()
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file_name = (
        f"step_{step_num:02d}_{step_name.replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_')}"
        f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    log_file_path = logs_dir / log_file_name

    print(f"[LOG] Step output will be saved to: {log_file_path}")

    # Log step start to DB
    _log_step_progress(step_num, step_name, "in_progress")

    # Step hooks
    metrics = None
    if _FOUNDATION_AVAILABLE and StepHookRegistry:
        try:
            metrics = StepMetrics(
                step_number=step_num,
                step_name=step_name,
                run_id=run_id or "pending",
                scraper_name=SCRAPER_NAME,
                started_at=datetime.now(),
                log_file_path=str(log_file_path),
            )
            StepHookRegistry.emit_step_start(metrics)
        except Exception as e:
            print(f"[HOOKS] Warning: Could not emit step start hook: {e}")

    try:
        env = os.environ.copy()
        env["PIPELINE_RUNNER"] = "1"
        if run_id:
            env["PIPELINE_RUN_ID"] = run_id

        # Run subprocess with output tee to both console and log file
        process = subprocess.Popen(
            [sys.executable, "-u", str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        with open(log_file_path, "w", encoding="utf-8") as log_f:
            log_f.write(f"=== Step {display_step}/{MAX_STEPS}: {step_name} ===\n")
            log_f.write(f"=== Script: {script_name} ===\n")
            log_f.write(f"=== Started: {datetime.now().isoformat()} ===\n")
            log_f.write(f"=== Run ID: {run_id or 'pending'} ===\n")
            log_f.write("=" * 80 + "\n\n")
            log_f.flush()

            for line in process.stdout:
                log_f.write(line)
                log_f.flush()
                print(line, end="", flush=True)

        process.wait()

        if process.returncode != 0:
            if not allow_failure:
                raise subprocess.CalledProcessError(process.returncode, script_path)
            else:
                print(f"\nWARNING: Step {step_num} ({step_name}) failed but continuing (allow_failure=True)")

        duration_seconds = time.time() - start_time

        # Format duration
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

        # Mark step as complete in checkpoint
        if output_files:
            abs_output_files = []
            csv_dir = get_csv_output_dir()
            pdf_dir = get_split_pdf_dir()
            for f in output_files:
                if Path(f).is_absolute():
                    abs_output_files.append(f)
                elif f.endswith(".csv"):
                    abs_output_files.append(str(csv_dir / f))
                elif f.endswith(".pdf"):
                    abs_output_files.append(str(pdf_dir / f))
                else:
                    abs_output_files.append(f)
            cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)

        # Log step completion to DB
        _log_step_progress(step_num, step_name, "completed")
        _update_run_ledger_step_count(display_step)

        # Prometheus
        if _PROMETHEUS_AVAILABLE:
            record_step_duration(SCRAPER_NAME, step_name, duration_seconds)

        # Benchmarking
        if _FOUNDATION_AVAILABLE and record_step_benchmark:
            try:
                record_step_benchmark(SCRAPER_NAME, run_id or "unknown", step_num, step_name, duration_seconds)
            except Exception:
                pass

        # Step hooks - complete
        if metrics and _FOUNDATION_AVAILABLE and StepHookRegistry:
            try:
                metrics.completed_at = datetime.now()
                metrics.duration_seconds = duration_seconds
                StepHookRegistry.emit_step_complete(metrics)
            except Exception:
                pass

        # Output completion progress
        completion_percent = round(((step_num + 1) / MAX_STEPS) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0

        next_step_descriptions = {
            0: "Ready to split PDF",
            1: "Ready to validate PDF structure",
            2: "Ready to extract Annexe IV.1",
            3: "Ready to extract Annexe IV.2",
            4: "Ready to extract Annexe V",
            5: "Ready to merge annexes",
            6: "Pipeline completed successfully",
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")

        print(f"[PROGRESS] Pipeline Step: {display_step}/{MAX_STEPS} ({completion_percent}%) - {next_desc}", flush=True)

        return True

    except subprocess.CalledProcessError as e:
        duration_seconds = time.time() - start_time
        error_msg = f"exit_code={e.returncode}"
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode} (duration: {duration_seconds:.2f}s)")
        _log_step_progress(step_num, step_name, "failed", error_message=error_msg)
        if _PROMETHEUS_AVAILABLE:
            record_error(SCRAPER_NAME, f"step_{step_num}_failed")
        return False

    except Exception as e:
        duration_seconds = time.time() - start_time
        error_msg = str(e)
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e} (duration: {duration_seconds:.2f}s)")
        _log_step_progress(step_num, step_name, "failed", error_message=error_msg)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CanadaQuebec Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-6)")
    args = parser.parse_args()

    # Recover stale pipelines on startup
    if _RECOVERY_AVAILABLE:
        try:
            recovery_result = recover_stale_pipelines([SCRAPER_NAME])
            if recovery_result.get("total_recovered", 0) > 0:
                print(f"[RECOVERY] Recovered {recovery_result['total_recovered']} stale pipeline state(s)")
        except Exception as e:
            print(f"[RECOVERY] Warning: Could not run startup recovery: {e}")

    # Initialize Prometheus metrics
    if _PROMETHEUS_AVAILABLE:
        init_prometheus_metrics(port=9090)

    cp = get_checkpoint_manager(SCRAPER_NAME)

    # Run ID management
    if args.fresh:
        # Check if external run_id is provided (from GUI/Telegram/API sync)
        external_run_id = os.environ.get("PIPELINE_RUN_ID") or os.environ.get("CANADAQUEBEC_RUN_ID")
        if external_run_id:
            PIPELINE_RUN_ID = external_run_id
            print(f"[INIT] Using external run_id from environment: {PIPELINE_RUN_ID}")
        else:
            PIPELINE_RUN_ID = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    else:
        PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID") or os.getenv("CANADAQUEBEC_RUN_ID") or f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.environ["PIPELINE_RUN_ID"] = PIPELINE_RUN_ID

    # DB handler
    db = DBHandler()
    if DB_ENABLED:
        db.start_run(PIPELINE_RUN_ID)
        print(f"[DB] Pipeline Run ID: {PIPELINE_RUN_ID}")

    # Write run_id to .current_run_id
    run_id_file = _get_output_dir() / ".current_run_id"
    try:
        run_id_file.parent.mkdir(parents=True, exist_ok=True)
        run_id_file.write_text(PIPELINE_RUN_ID, encoding="utf-8")
    except Exception:
        pass

    # Update checkpoint with run_id
    cp.update_metadata({"run_id": PIPELINE_RUN_ID, "status": "running"})

    # Determine start step
    if args.fresh:
        cp.clear_checkpoint()
        start_step = 0
        print("Starting fresh run (checkpoint cleared)")
    elif args.step is not None:
        start_step = args.step
        print(f"Starting from step {start_step}")
    else:
        info = cp.get_checkpoint_info()
        start_step = info["next_step"]
        if info["total_completed"] > 0:
            print(f"Resuming from step {start_step} (last completed: step {info['last_completed_step']})")
        else:
            print("Starting fresh run (no checkpoint found)")

    # Ensure resume uses existing run_id
    _ensure_resume_run_id(start_step)

    # Record run start in Prometheus
    if _PROMETHEUS_AVAILABLE:
        record_scraper_run(SCRAPER_NAME, "started")

    # Define pipeline steps
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None),
        (1, "01_split_pdf_into_annexes.py", "Split PDF into Annexes",
         ["annexe_iv1.pdf", "annexe_iv2.pdf", "annexe_v.pdf"]),
        (2, "02_validate_pdf_structure.py", "Validate PDF Structure", None, True),
        (3, "03_extract_annexe_iv1.py", "Extract Annexe IV.1", None),
        (4, "04_extract_annexe_iv2.py", "Extract Annexe IV.2", None),
        (5, "05_extract_annexe_v.py", "Extract Annexe V", None),
        (6, "06_merge_all_annexes.py", "Merge All Annexes", None),
    ]

    # Check if earlier steps need re-running (output files missing)
    earliest_rerun_step = None
    for step_info in steps:
        if len(step_info) == 4:
            step_num, script_name, step_name, output_files = step_info
        else:
            step_num, script_name, step_name, output_files, _ = step_info

        if step_num < start_step:
            expected_files = None
            if output_files:
                csv_dir = get_csv_output_dir()
                pdf_dir = get_split_pdf_dir()
                expected_files = []
                for f in output_files:
                    if Path(f).is_absolute():
                        expected_files.append(f)
                    elif f.endswith(".csv"):
                        expected_files.append(str(csv_dir / f))
                    elif f.endswith(".pdf"):
                        expected_files.append(str(pdf_dir / f))
                    else:
                        expected_files.append(str(csv_dir / f))

            if not cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                if earliest_rerun_step is None or step_num < earliest_rerun_step:
                    earliest_rerun_step = step_num

    if earliest_rerun_step is not None:
        print(f"\nWARNING: Step {earliest_rerun_step} needs re-run (output files missing).")
        print(f"Adjusting start step from {start_step} to {earliest_rerun_step} to maintain pipeline integrity.\n")
        start_step = earliest_rerun_step

    # Run steps
    pipeline_start_time = time.time()
    for step_info in steps:
        if len(step_info) == 4:
            step_num, script_name, step_name, output_files = step_info
            allow_failure = False
        else:
            step_num, script_name, step_name, output_files, allow_failure = step_info

        if step_num < start_step:
            expected_files = None
            if output_files:
                csv_dir = get_csv_output_dir()
                pdf_dir = get_split_pdf_dir()
                expected_files = []
                for f in output_files:
                    if Path(f).is_absolute():
                        expected_files.append(f)
                    elif f.endswith(".csv"):
                        expected_files.append(str(csv_dir / f))
                    elif f.endswith(".pdf"):
                        expected_files.append(str(pdf_dir / f))
                    else:
                        expected_files.append(str(csv_dir / f))

            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                display_step = step_num + 1
                completion_percent = min(round(((step_num + 1) / MAX_STEPS) * 100, 1), 100.0)
                skip_descriptions = {
                    0: "Skipped: Backup already completed",
                    1: "Skipped: PDF already split into annexes",
                    2: "Skipped: PDF structure already validated",
                    3: "Skipped: Annexe IV.1 already extracted",
                    4: "Skipped: Annexe IV.2 already extracted",
                    5: "Skipped: Annexe V already extracted",
                    6: "Skipped: Annexes already merged",
                }
                skip_desc = skip_descriptions.get(step_num, f"Skipped: {step_name} already completed")
                print(f"\nStep {display_step}/{MAX_STEPS}: {step_name} - SKIPPED (already completed in checkpoint)")
                print(f"[PROGRESS] Pipeline Step: {display_step}/{MAX_STEPS} ({completion_percent}%) - {skip_desc}", flush=True)
            else:
                display_step = step_num + 1
                print(f"\nStep {display_step}/{MAX_STEPS}: {step_name} - WILL RE-RUN (output files missing)")
            continue

        success = run_step(step_num, script_name, step_name, output_files, allow_failure)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            if _PROMETHEUS_AVAILABLE:
                record_scraper_run(SCRAPER_NAME, "failed")
            if DB_ENABLED:
                db.finish_run(PIPELINE_RUN_ID, status="FAILED")
            sys.exit(1)

    # Pipeline completed
    pipeline_duration = time.time() - pipeline_start_time
    cp = get_checkpoint_manager(SCRAPER_NAME)
    timing_info = cp.get_pipeline_timing()
    total_duration = timing_info.get("total_duration_seconds", pipeline_duration)

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
    print(f"[TIMING] Total pipeline duration: {total_duration_str}")
    print(f"{'='*80}\n")
    print(f"[PROGRESS] Pipeline Step: {MAX_STEPS}/{MAX_STEPS} (100%)", flush=True)

    # Prometheus
    if _PROMETHEUS_AVAILABLE:
        record_scraper_run(SCRAPER_NAME, "completed")
        record_scraper_duration(SCRAPER_NAME, total_duration)

    # Run ledger aggregation
    if _FOUNDATION_AVAILABLE and update_run_ledger_aggregation:
        try:
            update_run_ledger_aggregation(SCRAPER_NAME, _read_run_id())
        except Exception:
            pass

    if DB_ENABLED:
        db.finish_run(PIPELINE_RUN_ID, status="COMPLETED")

    # Update checkpoint metadata
    cp.update_metadata({"status": "completed"})

    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except Exception:
        pass


if __name__ == "__main__":
    main()
