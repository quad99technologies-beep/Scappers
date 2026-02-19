#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Integration Example

Shows how to integrate foundation contracts into run_pipeline_resume.py
without modifying scraping or business logic.

This is a reference implementation - copy patterns into your pipeline runners.
"""

import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime

# Import foundation contracts
from core.pipeline.preflight_checks import PreflightChecker, CheckSeverity
from core.pipeline.step_hooks import StepHookRegistry, StepMetrics
from core.monitoring.alerting_integration import setup_alerting_hooks
from core.data.data_quality_checks import DataQualityChecker
from core.utils.step_progress_logger import (
    log_step_progress,
    update_run_ledger_aggregation,
    update_run_ledger_step_count
)


def example_main(scraper_name: str, run_id: str):
    """
    Example main() function showing integration pattern.
    
    Copy this pattern into your run_pipeline_resume.py files.
    """
    
    # =========================================================================
    # STEP 1: Setup (once at startup)
    # =========================================================================
    
    # Setup alerting hooks (enables automatic alerting)
    setup_alerting_hooks()
    print("[SETUP] Alerting hooks registered")
    
    # =========================================================================
    # STEP 2: Preflight Health Checks (MANDATORY GATE)
    # =========================================================================
    
    checker = PreflightChecker(scraper_name, run_id)
    results = checker.run_all_checks()
    
    # Log all results
    print("\n[PREFLIGHT] Health Checks:")
    for result in results:
        if result.severity == CheckSeverity.CRITICAL:
            emoji = "❌" if not result.passed else "✅"
        elif result.severity == CheckSeverity.WARNING:
            emoji = "⚠️" if not result.passed else "✅"
        else:
            emoji = "ℹ️"
        print(f"  {emoji} {result.name}: {result.message}")
    
    # Block run if critical checks fail
    if checker.has_critical_failures():
        print("\n[PREFLIGHT] ❌ Pipeline blocked due to critical failures:")
        print(checker.get_failure_summary())
        sys.exit(1)
    
    # Run pre-flight data quality checks
    dq_checker = DataQualityChecker(scraper_name, run_id)
    dq_checker.run_preflight_checks()
    dq_checker.save_results_to_db()
    print("[PREFLIGHT] Data quality checks completed")
    
    # =========================================================================
    # STEP 3: Run Pipeline Steps (with hooks and enhanced metrics)
    # =========================================================================
    
    steps = [
        (0, "step_00_backup_clean.py", "Backup and Clean"),
        (1, "step_01_collect.py", "Collect Data"),
        # ... more steps ...
    ]
    
    for step_num, script_name, step_name in steps:
        run_step_with_hooks(
            scraper_name=scraper_name,
            run_id=run_id,
            step_num=step_num,
            script_name=script_name,
            step_name=step_name
        )
    
    # =========================================================================
    # STEP 4: Post-Run Processing
    # =========================================================================
    
    # Update run-level aggregation
    update_run_ledger_aggregation(scraper_name, run_id)
    print("[POST-RUN] Run-level aggregation updated")
    
    # Run post-run data quality checks
    dq_checker = DataQualityChecker(scraper_name, run_id)
    dq_checker.run_postrun_checks()
    dq_checker.save_results_to_db()
    print("[POST-RUN] Data quality checks completed")
    
    # Validate exports
    export_files = [
        Path("output") / scraper_name / "exports" / "report.csv",
        # ... more exports ...
    ]
    for export_file in export_files:
        if export_file.exists():
            result = dq_checker.validate_export(export_file)
            if not result.passed and result.severity == CheckSeverity.CRITICAL:
                print(f"[EXPORT] ❌ Export validation failed: {export_file}")
                # Optionally block or warn


def run_step_with_hooks(
    scraper_name: str,
    run_id: str,
    step_num: int,
    script_name: str,
    step_name: str
):
    """
    Example run_step() function showing hook integration.
    
    Copy this pattern into your run_step() functions.
    """
    
    print(f"\n[STEP {step_num}] {step_name}")
    
    start_time = time.time()
    log_file_path = None  # Set this from step output if available
    
    # Create metrics object
    metrics = StepMetrics(
        step_number=step_num,
        step_name=step_name,
        run_id=run_id,
        scraper_name=scraper_name,
        started_at=datetime.now(),
        log_file_path=log_file_path
    )
    
    # Emit step start hook (triggers any registered hooks)
    StepHookRegistry.emit_step_start(metrics)
    
    try:
        # Execute step script
        script_path = Path(__file__).parent.parent / "scripts" / scraper_name / script_name
        result = subprocess.run(
            [sys.executable, "-u", str(script_path)],
            check=True,
            capture_output=False,
            env=os.environ.copy()
        )
        
        # Calculate duration
        duration = time.time() - start_time
        
        # Update metrics (populate from step output if available)
        metrics.duration_seconds = duration
        metrics.completed_at = datetime.now()
        # metrics.rows_processed = ...  # Get from step output
        # metrics.rows_inserted = ...    # Get from step output
        # metrics.browser_instances_spawned = ...  # Track browser spawns
        
        # Log to database with enhanced metrics
        log_step_progress(
            scraper_name=scraper_name,
            run_id=run_id,
            step_num=step_num,
            step_name=step_name,
            status="completed",
            duration_seconds=duration,
            rows_read=metrics.rows_read,
            rows_processed=metrics.rows_processed,
            rows_inserted=metrics.rows_inserted,
            rows_updated=metrics.rows_updated,
            rows_rejected=metrics.rows_rejected,
            browser_instances_spawned=metrics.browser_instances_spawned,
            log_file_path=log_file_path
        )
        
        # Update run ledger step count
        update_run_ledger_step_count(scraper_name, run_id, step_num + 1)
        
        # Emit step end hook (triggers alerts, dashboard updates, etc.)
        StepHookRegistry.emit_step_end(metrics)
        
        print(f"[STEP {step_num}] ✅ Completed in {duration:.1f}s")
        
    except subprocess.CalledProcessError as e:
        # Step failed
        duration = time.time() - start_time
        metrics.duration_seconds = duration
        metrics.completed_at = datetime.now()
        metrics.error_message = str(e)
        
        # Log failure
        log_step_progress(
            scraper_name=scraper_name,
            run_id=run_id,
            step_num=step_num,
            step_name=step_name,
            status="failed",
            error_message=str(e),
            duration_seconds=duration,
            log_file_path=log_file_path
        )
        
        # Emit error hook (triggers alerts)
        StepHookRegistry.emit_step_error(metrics, e)
        
        print(f"[STEP {step_num}] ❌ Failed: {e}")
        raise
    
    except Exception as e:
        # Unexpected error
        metrics.error_message = str(e)
        metrics.completed_at = datetime.now()
        StepHookRegistry.emit_step_error(metrics, e)
        raise


# Example usage
if __name__ == "__main__":
    import os
    scraper_name = os.getenv("SCRAPER_NAME", "Malaysia")
    run_id = os.getenv("RUN_ID", "test_run_001")
    example_main(scraper_name, run_id)
