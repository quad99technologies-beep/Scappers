#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Netherlands Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-1)
"""

import os
import sys
import subprocess
import argparse
import time
from pathlib import Path
from datetime import datetime

# Force UTF-8 output on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Netherlands to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir

# Import browser PID cleanup
try:
    from core.browser.chrome_pid_tracker import terminate_scraper_pids
    _BROWSER_CLEANUP_AVAILABLE = True
except ImportError:
    _BROWSER_CLEANUP_AVAILABLE = False
    def terminate_scraper_pids(scraper_name, repo_root, silent=False):
        return 0

SCRIPT_ID = "Netherlands"

# Pipeline steps: (step_num, script, display_name)
PIPELINE_STEPS = [
    (0, "00_backup_and_clean.py", "Backup and Clean"),
    (1, "scraper.py", "Hybrid Scraper (URL Collection + Product Scraping + Consolidation)"),
]
TOTAL_STEPS = len(PIPELINE_STEPS)
VALID_STEPS = [step_num for step_num, _, _ in PIPELINE_STEPS]

STEP_DESCRIPTIONS = {
    0: "Preparing: Backing up previous results and cleaning output directory",
    1: "Scraping: Collecting URLs, extracting product details, and consolidating data",
}


# =========================================================
# RUN ID HELPERS
# =========================================================

def _read_run_id() -> str:
    """Load run_id from env or .current_run_id file."""
    run_id = os.environ.get("NL_RUN_ID", "").strip()
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
    """
    Return the best Netherlands run_id to resume.
    Pattern from Argentina: prefer runs with data (items_scraped > 0), then latest by started_at.
    """
    try:
        from core.db.postgres_connection import get_db
        from db.repositories import NetherlandsRepository

        db = get_db("Netherlands")
        run_id = NetherlandsRepository.get_latest_incomplete_run(db)
        return run_id or ""
    except Exception:
        return ""


def _save_run_id(run_id: str) -> None:
    """Persist run_id to env var and .current_run_id file."""
    os.environ["NL_RUN_ID"] = run_id
    run_id_file = get_output_dir() / ".current_run_id"
    try:
        run_id_file.parent.mkdir(parents=True, exist_ok=True)
        run_id_file.write_text(run_id, encoding="utf-8")
    except Exception:
        pass


def _ensure_resume_run_id(is_fresh: bool = False) -> str:
    """
    Ensure we use an existing run_id when resuming (not fresh).
    Priority: 1) env var, 2) .current_run_id file, 3) run_ledger (run with most data)

    When is_fresh=True, clears existing run_id and returns empty string.
    """
    if is_fresh:
        os.environ.pop("NL_RUN_ID", None)
        run_id_file = get_output_dir() / ".current_run_id"
        try:
            if run_id_file.exists():
                run_id_file.unlink()
        except Exception:
            pass
        return ""

    # Try sources in order
    run_id = os.environ.get("NL_RUN_ID", "").strip()
    source = "environment"

    if not run_id:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            try:
                run_id = run_id_file.read_text(encoding="utf-8").strip()
                source = ".current_run_id file"
            except Exception:
                pass

    if not run_id:
        run_id = _get_latest_run_id_from_db()
        source = "run_ledger (run with data)"

    if run_id:
        _save_run_id(run_id)
        cp = get_checkpoint_manager(SCRIPT_ID)
        cp.update_metadata({"run_id": run_id})
        print(f"[RESUME] Preserved run_id from {source}: {run_id}", flush=True)
        return run_id

    return ""


def _mark_run_ledger_active(run_id: str) -> None:
    """Ensure run_ledger has a row for this run and status is running when resuming."""
    if not run_id:
        return
    try:
        from core.db.postgres_connection import get_db
        from db.repositories import NetherlandsRepository

        db = get_db("Netherlands")
        # Stop any other resume runs so only one remains resumable
        try:
            NetherlandsRepository.stop_other_resume_runs(db, run_id)
            db.commit()
        except Exception:
            pass
        repo = NetherlandsRepository(db, run_id)
        repo.ensure_run_in_ledger(mode="resume")
        repo.resume_run()
    except Exception:
        pass


def _get_checkpoint_completed_steps(cp) -> set:
    """Return completed steps from checkpoint file."""
    try:
        info = cp.get_checkpoint_info()
        completed = info.get("completed_steps", [])
        if isinstance(completed, list):
            return set(completed)
    except Exception:
        pass
    return set()


# =========================================================
# DB STEP COMPLETION CHECKS
# =========================================================

def _get_db_step_status(run_id: str) -> dict:
    """
    Query database for step completion status.
    Returns dict with counts for each step.
    """
    if not run_id:
        return {}

    try:
        from core.db.postgres_connection import get_db
        db = get_db("Netherlands")

        with db.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM nl_collected_urls WHERE run_id = %s) as urls,
                    (SELECT COUNT(*) FROM nl_collected_urls WHERE run_id = %s AND packs_scraped = 'success') as url_success,
                    (SELECT COUNT(*) FROM nl_collected_urls WHERE run_id = %s AND packs_scraped = 'failed') as url_failed,
                    (SELECT COUNT(*) FROM nl_collected_urls WHERE run_id = %s AND packs_scraped = 'pending') as url_pending,
                    (SELECT COUNT(*) FROM nl_packs WHERE run_id = %s) as packs
            """, (run_id, run_id, run_id, run_id, run_id))
            row = cur.fetchone()
            if row:
                return {
                    'urls_collected': row[0] or 0,
                    'urls_success': row[1] or 0,
                    'urls_failed': row[2] or 0,
                    'urls_pending': row[3] or 0,
                    'packs_count': row[4] or 0,
                }
    except Exception as e:
        print(f"[DB CHECK] Warning: Could not check DB status: {e}", flush=True)

    return {
        'urls_collected': 0,
        'urls_success': 0,
        'urls_failed': 0,
        'urls_pending': 0,
        'packs_count': 0,
    }


def _is_step_complete(step_num: int, db_status: dict) -> bool:
    """
    Check if a step is fully complete based on database state.

    Step 0: Never "complete" (always safe to re-run, but skipped on resume)
    Step 1: Complete when 95%+ of URLs are processed (success/failed),
            or (legacy fallback) 95%+ packs are present.
    """
    if step_num == 0:
        return False  # Backup/clean — always re-runnable, but we skip on resume

    if step_num == 1:
        urls = db_status.get('urls_collected', 0)
        success = db_status.get('urls_success', 0)
        failed = db_status.get('urls_failed', 0)
        pending = db_status.get('urls_pending', 0)
        packs = db_status.get('packs_count', 0)
        if urls == 0:
            return False

        # Primary signal: URL status coverage
        processed_ratio = (success + failed) / urls if urls > 0 else 0
        if pending == 0 and (success + failed) > 0:
            return True
        if processed_ratio >= 0.95:
            return True

        # Legacy fallback for older runs where URL status wasn't updated reliably
        packs_ratio = packs / urls if urls > 0 else 0
        return packs_ratio >= 0.95

    return False


# =========================================================
# DB PROGRESS LOGGING
# =========================================================

def _log_step_progress(step_num: int, step_name: str, status: str, error_message: str = None) -> None:
    """Persist step progress in PostgreSQL."""
    run_id = _read_run_id()
    if not run_id:
        return
    try:
        from core.db.postgres_connection import get_db
        from db.repositories import NetherlandsRepository

        db = get_db("Netherlands")
        repo = NetherlandsRepository(db, run_id)
        repo.ensure_run_in_ledger(mode="pipeline")
        repo.mark_progress(step_num, step_name, "pipeline", status, error_message)
        db.commit()
    except Exception:
        pass


# =========================================================
# FORMAT HELPERS
# =========================================================

def _format_duration(seconds: float) -> str:
    """Format duration to human-readable string."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"


# =========================================================
# STEP RUNNER
# =========================================================

def run_step(step_num: int, script_name: str, step_name: str) -> bool:
    """Run a pipeline step. Returns True on success."""
    display_step = step_num + 1

    print(f"\n{'='*80}", flush=True)
    print(f"Step {display_step}/{TOTAL_STEPS}: {step_name}", flush=True)
    print(f"{'='*80}\n", flush=True)

    # Progress with description
    pipeline_percent = round((step_num / TOTAL_STEPS) * 100, 1)
    step_desc = STEP_DESCRIPTIONS.get(step_num, step_name)
    print(f"[PROGRESS] Pipeline Step: {display_step}/{TOTAL_STEPS} ({pipeline_percent}%) - {step_desc}", flush=True)
    print(f"[PIPELINE] Executing: {script_name}", flush=True)

    # Verify script exists
    script_path = _script_dir / script_name
    if not script_path.exists():
        print(f"[ERROR] Script not found: {script_path}", flush=True)
        _log_step_progress(step_num, step_name, "failed", error_message="Script not found")
        return False

    # Log start
    _log_step_progress(step_num, step_name, "in_progress")
    start_time = time.time()

    try:
        env = os.environ.copy()
        env["PIPELINE_RUNNER"] = "1"
        env["PIPELINE_STEP_DISPLAY"] = str(display_step)
        env["PIPELINE_TOTAL_STEPS"] = str(TOTAL_STEPS)
        env["PIPELINE_STEP_NAME"] = step_name

        # Ensure NL_RUN_ID is in env
        current_run_id = _read_run_id()
        if current_run_id:
            env["NL_RUN_ID"] = current_run_id

        result = subprocess.run(
            [sys.executable, "-u", str(script_path)],
            check=True,
            capture_output=False,
            env=env
        )

        duration = time.time() - start_time

        # After step 0, pick up the run_id that may have been generated
        if step_num == 0:
            new_run_id = _read_run_id()
            if new_run_id:
                cp = get_checkpoint_manager(SCRIPT_ID)
                cp.update_metadata({"run_id": new_run_id})

        # After step 1, pick up run_id that the scraper may have generated
        if step_num == 1:
            new_run_id = _read_run_id()
            if new_run_id:
                cp = get_checkpoint_manager(SCRIPT_ID)
                cp.update_metadata({"run_id": new_run_id})

        # Mark checkpoint complete
        cp = get_checkpoint_manager(SCRIPT_ID)
        cp.mark_step_complete(step_num, step_name, output_files=[], duration_seconds=duration)

        # Log completion
        _log_step_progress(step_num, step_name, "completed")

        # Print timing
        print(f"\n[SUCCESS] Step {step_num} ({step_name}) completed in {_format_duration(duration)}", flush=True)

        # Progress after completion
        completion_percent = round(((step_num + 1) / TOTAL_STEPS) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0
        print(f"[PROGRESS] Pipeline Step: {display_step}/{TOTAL_STEPS} ({completion_percent}%) - Step complete", flush=True)

        # Pause between steps
        if step_num < TOTAL_STEPS - 1:
            print(f"\n[PAUSE] Waiting 5 seconds before next step...", flush=True)
            time.sleep(5.0)

        return True

    except subprocess.CalledProcessError as e:
        duration = time.time() - start_time
        print(f"\n[ERROR] Step {step_num} ({step_name}) failed with exit code {e.returncode} ({_format_duration(duration)})", flush=True)
        _log_step_progress(step_num, step_name, "failed", error_message=f"Exit code {e.returncode}")
        return False
    except Exception as e:
        duration = time.time() - start_time
        print(f"\n[ERROR] Step {step_num} ({step_name}) failed: {e} ({_format_duration(duration)})", flush=True)
        _log_step_progress(step_num, step_name, "failed", error_message=str(e))
        return False


# =========================================================
# MAIN
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="Netherlands Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint, new run_id)")
    parser.add_argument("--step", type=int, choices=VALID_STEPS, help="Start from specific step (0-1)")
    args = parser.parse_args()
    
    # Pre-run cleanup of any leftover browser PIDs for this scraper
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids(SCRIPT_ID, _repo_root, silent=True)
        except Exception:
            pass

    pipeline_start = time.time()
    cp = get_checkpoint_manager(SCRIPT_ID)
    checkpoint_completed_steps = _get_checkpoint_completed_steps(cp)
    is_fresh = args.fresh
    skip_step_zero = False

    # ---- Determine start_step and run_id ----

    if is_fresh:
        # Fresh: clear everything, start from step 0
        cp.clear_checkpoint()
        checkpoint_completed_steps = set()
        _ensure_resume_run_id(is_fresh=True)
        start_step = 0
        print("[PIPELINE] Starting fresh run (clearing checkpoint)", flush=True)

    elif args.step is not None:
        # Specific step requested
        start_step = args.step
        run_id = _ensure_resume_run_id(is_fresh=False)
        if not run_id:
            print("[ERROR] No run_id found. Use --fresh to start a new run.", flush=True)
            sys.exit(1)
        print(f"[PIPELINE] Starting from step {start_step} as requested", flush=True)

    else:
        # Default: resume from where we left off
        run_id = _ensure_resume_run_id(is_fresh=False)

        if run_id:
            # We have an existing run — check DB to find where to resume
            db_status = _get_db_step_status(run_id)
            urls = db_status.get('urls_collected', 0)
            success = db_status.get('urls_success', 0)
            failed = db_status.get('urls_failed', 0)
            pending = db_status.get('urls_pending', 0)
            packs = db_status.get('packs_count', 0)

            print(f"[RESUME] Found run: {run_id}", flush=True)
            print(
                f"[RESUME] DB Status: URLs={urls}, Success={success}, Failed={failed}, Pending={pending}, Packs={packs}",
                flush=True,
            )

            if checkpoint_completed_steps:
                completed_text = ", ".join(str(s) for s in sorted(checkpoint_completed_steps))
                print(f"[RESUME] Checkpoint completed steps: {completed_text}", flush=True)
            else:
                print("[RESUME] Checkpoint completed steps: none", flush=True)

            # Primary resume source: checkpoint step completion markers
            if 1 in checkpoint_completed_steps:
                print("[RESUME] All steps already complete per checkpoint! Use --fresh to start over.", flush=True)
                sys.exit(0)

            # Secondary resume source: DB completion heuristic
            if _is_step_complete(1, db_status):
                if 1 not in checkpoint_completed_steps:
                    try:
                        cp.mark_step_complete(1, PIPELINE_STEPS[1][2], output_files=[], duration_seconds=None)
                    except Exception:
                        pass
                print("[RESUME] All steps already complete! Use --fresh to start over.", flush=True)
                sys.exit(0)
            elif success > 0 or failed > 0 or pending > 0 or packs > 0:
                start_step = 1
                skip_step_zero = True
                print(
                    f"[RESUME] Step 1 partial (success={success}, failed={failed}, pending={pending}, urls={urls}). Resuming step 1.",
                    flush=True,
                )
            elif urls > 0:
                # Step 1 has partial data — resume it (scraper handles internal resume)
                start_step = 1
                skip_step_zero = True
                print(f"[RESUME] Step 1 partial ({urls} URLs present). Resuming step 1.", flush=True)
            else:
                # No data at all — but we still have a run_id, skip step 0 to preserve it
                start_step = 1
                skip_step_zero = True
                print(f"[RESUME] Run exists but no data yet. Resuming from step 1.", flush=True)
        else:
            # No existing run found — truly fresh start
            start_step = 0
            is_fresh = True
            print("[PIPELINE] No existing run found. Starting fresh.", flush=True)

    # Checkpoint-based skip: if step 0 was completed previously, skip it on resume.
    if not is_fresh and args.step is None and 0 in checkpoint_completed_steps:
        skip_step_zero = True

    # ---- Generate run_id if fresh ----

    if is_fresh and not _read_run_id():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_run_id = f"nl_{timestamp}"
        _save_run_id(new_run_id)
        cp.update_metadata({"run_id": new_run_id})
        print(f"[PIPELINE] Generated new run_id: {new_run_id}", flush=True)

    # ---- Mark run as active in ledger if resuming ----

    if not is_fresh:
        _mark_run_ledger_active(_read_run_id())

    # ---- Print execution plan ----

    print(f"\n{'='*80}", flush=True)
    print("PIPELINE EXECUTION PLAN", flush=True)
    print(f"{'='*80}", flush=True)

    run_id = _read_run_id()
    if run_id:
        print(f"Run ID: {run_id}", flush=True)

    for step_num, script_name, step_name in PIPELINE_STEPS:
        display_step = step_num + 1
        if step_num == 0 and skip_step_zero:
            print(f"  Step {display_step}/{TOTAL_STEPS}: {step_name} - SKIP (resume, preserving data)", flush=True)
        elif step_num < start_step:
            print(f"  Step {display_step}/{TOTAL_STEPS}: {step_name} - SKIP (already completed)", flush=True)
        elif step_num == start_step:
            print(f"  Step {display_step}/{TOTAL_STEPS}: {step_name} - RUN NOW", flush=True)
        else:
            print(f"  Step {display_step}/{TOTAL_STEPS}: {step_name} - RUN (after previous steps)", flush=True)
    print(f"{'='*80}\n", flush=True)

    # ---- Execute steps ----

    for step_num, script_name, step_name in PIPELINE_STEPS:
        # Skip step 0 on resume
        if step_num == 0 and skip_step_zero:
            _log_step_progress(step_num, step_name, "skipped")
            continue

        # Skip already completed steps
        if step_num < start_step:
            display_step = step_num + 1
            completion_pct = round(((step_num + 1) / TOTAL_STEPS) * 100, 1)
            print(f"[PROGRESS] Pipeline Step: {display_step}/{TOTAL_STEPS} ({completion_pct}%) - Skipped: {step_name} already completed", flush=True)
            _log_step_progress(step_num, step_name, "skipped")
            continue

        # Run the step
        success = run_step(step_num, script_name, step_name)
        if not success:
            print(f"\n[PIPELINE] Pipeline failed at step {step_num} ({step_name})", flush=True)
            sys.exit(1)

    # ---- Pipeline complete ----

    total_duration = time.time() - pipeline_start
    run_id = _read_run_id()

    print(f"\n{'='*80}", flush=True)
    print("NETHERLANDS PIPELINE COMPLETED SUCCESSFULLY", flush=True)
    
    # Post-run cleanup
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids(SCRIPT_ID, _repo_root, silent=True)
        except Exception:
            pass

    if run_id:
        print(f"Run ID: {run_id}", flush=True)
    print(f"[TIMING] Total pipeline duration: {_format_duration(total_duration)}", flush=True)
    print(f"{'='*80}", flush=True)
    print(f"[PROGRESS] Pipeline Step: {TOTAL_STEPS}/{TOTAL_STEPS} (100%)", flush=True)


if __name__ == "__main__":
    main()
