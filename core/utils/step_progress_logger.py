#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standardized Step Progress Logger

Provides a unified interface for logging step progress to database across all regions.
Each region can use this to log step-level progress without duplicating code.
"""

import logging
from typing import Optional
from pathlib import Path

logger = logging.getLogger(__name__)


def log_step_progress(
    scraper_name: str,
    run_id: str,
    step_num: int,
    step_name: str,
    status: str,
    error_message: Optional[str] = None,
    progress_key: str = "pipeline",
    duration_seconds: Optional[float] = None,
    rows_read: int = 0,
    rows_processed: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_rejected: int = 0,
    browser_instances_spawned: int = 0,
    log_file_path: Optional[str] = None,
) -> bool:
    """
    Log step progress to database for any scraper.
    
    Args:
        scraper_name: Name of the scraper (e.g., "Argentina", "Malaysia")
        run_id: Current run ID
        step_num: Step number (0-based)
        step_name: Human-readable step name
        status: Status ("in_progress", "completed", "failed", "skipped")
        error_message: Optional error message if status is "failed"
        progress_key: Progress key (default: "pipeline")
        duration_seconds: Optional step duration in seconds
        rows_read: Number of rows read
        rows_processed: Number of rows processed
        rows_inserted: Number of rows inserted
        rows_updated: Number of rows updated
        rows_rejected: Number of rows rejected
        browser_instances_spawned: Number of browser instances spawned
        log_file_path: Path to log file for this step
    
    Returns:
        True if logged successfully, False otherwise
    """
    if not run_id:
        return False
    
    try:
        from core.db.connection import CountryDB
        
        # Map scraper names to table prefixes
        table_prefix_map = {
            "Argentina": "ar",
            "Malaysia": "my",
            "Russia": "ru",
            "Belarus": "by",
            "NorthMacedonia": "nm",
            "CanadaOntario": "co",
            "Tender_Chile": "tc",
            "India": "in",  # India uses different schema but we'll handle it
        }
        
        prefix = table_prefix_map.get(scraper_name, scraper_name.lower()[:2])
        table_name = f"{prefix}_step_progress"
        
        with CountryDB(scraper_name) as db:
            with db.cursor() as cur:
                # Check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (table_name,))
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    logger.debug(f"Table {table_name} does not exist for {scraper_name}, skipping step progress logging")
                    return False
                
                # Check if enhanced columns exist
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                    AND column_name IN ('duration_seconds', 'rows_read', 'log_file_path')
                """, (table_name,))
                enhanced_columns = {row[0] for row in cur.fetchall()}
                has_enhanced = len(enhanced_columns) >= 3
                
                if has_enhanced:
                    # Use enhanced columns
                    cur.execute(f"""
                        INSERT INTO {table_name}
                            (run_id, step_number, step_name, progress_key, status, error_message,
                             duration_seconds, rows_read, rows_processed, rows_inserted, rows_updated, rows_rejected,
                             browser_instances_spawned, log_file_path,
                             started_at, completed_at)
                        VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                             CASE WHEN %s = 'in_progress' THEN CURRENT_TIMESTAMP ELSE NULL END,
                             CASE WHEN %s IN ('completed','failed','skipped') THEN CURRENT_TIMESTAMP ELSE NULL END)
                        ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                            step_name = EXCLUDED.step_name,
                            status = EXCLUDED.status,
                            error_message = EXCLUDED.error_message,
                            duration_seconds = EXCLUDED.duration_seconds,
                            rows_read = EXCLUDED.rows_read,
                            rows_processed = EXCLUDED.rows_processed,
                            rows_inserted = EXCLUDED.rows_inserted,
                            rows_updated = EXCLUDED.rows_updated,
                            rows_rejected = EXCLUDED.rows_rejected,
                            browser_instances_spawned = EXCLUDED.browser_instances_spawned,
                            log_file_path = EXCLUDED.log_file_path,
                            started_at = COALESCE({table_name}.started_at, EXCLUDED.started_at),
                            completed_at = EXCLUDED.completed_at
                    """, (
                        run_id, step_num, step_name, progress_key, status, error_message,
                        duration_seconds, rows_read, rows_processed, rows_inserted, rows_updated, rows_rejected,
                        browser_instances_spawned, log_file_path,
                        status, status
                    ))
                else:
                    # Fallback to basic columns (backward compatibility)
                    cur.execute(f"""
                        INSERT INTO {table_name}
                            (run_id, step_number, step_name, progress_key, status, error_message, started_at, completed_at)
                        VALUES
                            (%s, %s, %s, %s, %s, %s,
                             CASE WHEN %s = 'in_progress' THEN CURRENT_TIMESTAMP ELSE NULL END,
                             CASE WHEN %s IN ('completed','failed','skipped') THEN CURRENT_TIMESTAMP ELSE NULL END)
                        ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                            step_name = EXCLUDED.step_name,
                            status = EXCLUDED.status,
                            error_message = EXCLUDED.error_message,
                            started_at = COALESCE({table_name}.started_at, EXCLUDED.started_at),
                            completed_at = EXCLUDED.completed_at
                    """, (run_id, step_num, step_name, progress_key, status, error_message, status, status))

                # Mirror step transitions into checkpoint timeline so API/GUI can
                # reconstruct exact per-step state changes even across restarts.
                try:
                    from core.pipeline.pipeline_checkpoint import get_checkpoint_manager

                    cp = get_checkpoint_manager(scraper_name)
                    cp.record_event(
                        event_type="step_status",
                        run_id=run_id,
                        status=status,
                        step_number=step_num,
                        step_name=step_name,
                        source="step_progress",
                        message=error_message if status == "failed" else None,
                        details={
                            "progress_key": progress_key,
                            "duration_seconds": duration_seconds,
                            "rows_read": rows_read,
                            "rows_processed": rows_processed,
                            "rows_inserted": rows_inserted,
                            "rows_updated": rows_updated,
                            "rows_rejected": rows_rejected,
                            "browser_instances_spawned": browser_instances_spawned,
                            "log_file_path": log_file_path,
                        },
                    )
                except Exception:
                    # Non-blocking: timeline enrichment should never affect pipeline.
                    pass

                return True
    except Exception as e:
        # Non-blocking: progress logging should not break pipeline execution
        logger.debug(f"Could not log step progress for {scraper_name}: {e}")
        return False


def update_run_ledger_step_count(scraper_name: str, run_id: str, step_count: int) -> bool:
    """
    Update run_ledger.step_count for the current run_id.
    
    Args:
        scraper_name: Name of the scraper
        run_id: Current run ID
        step_count: Step count (1-based, total steps completed)
    
    Returns:
        True if updated successfully, False otherwise
    """
    if not run_id:
        return False
    
    try:
        from core.db.connection import CountryDB
        
        with CountryDB(scraper_name) as db:
            with db.cursor() as cur:
                cur.execute(
                    "UPDATE run_ledger SET step_count = %s WHERE run_id = %s AND scraper_name = %s",
                    (step_count, run_id, scraper_name),
                )
                return True
    except Exception as e:
        logger.debug(f"Could not update run_ledger step_count for {scraper_name}: {e}")
        return False


def update_run_ledger_aggregation(scraper_name: str, run_id: str) -> bool:
    """
    Update run_ledger with aggregation data (slowest_step, failure_step, total_runtime).
    
    Args:
        scraper_name: Name of the scraper
        run_id: Current run ID
    
    Returns:
        True if updated successfully, False otherwise
    """
    if not run_id:
        return False
    
    try:
        from core.db.connection import CountryDB
        
        # Map scraper names to table prefixes
        table_prefix_map = {
            "Argentina": "ar",
            "Malaysia": "my",
            "Russia": "ru",
            "Belarus": "by",
            "NorthMacedonia": "nm",
            "CanadaOntario": "co",
            "Tender_Chile": "tc",
            "India": "in",
            "Netherlands": "nl",
        }
        
        prefix = table_prefix_map.get(scraper_name, scraper_name.lower()[:2])
        table_name = f"{prefix}_step_progress"
        
        with CountryDB(scraper_name) as db:
            with db.cursor() as cur:
                # Check if enhanced columns exist
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'run_ledger'
                    AND column_name IN ('total_runtime_seconds', 'slowest_step_number')
                """)
                enhanced_columns = {row[0] for row in cur.fetchall()}
                has_enhanced = len(enhanced_columns) >= 2
                
                if not has_enhanced:
                    logger.debug("Enhanced run_ledger columns not found, skipping aggregation")
                    return False
                
                # Calculate total runtime
                cur.execute("""
                    SELECT 
                        EXTRACT(EPOCH FROM (COALESCE(ended_at, CURRENT_TIMESTAMP) - started_at)) as runtime_seconds
                    FROM run_ledger
                    WHERE run_id = %s AND scraper_name = %s
                """, (run_id, scraper_name))
                runtime_row = cur.fetchone()
                total_runtime = runtime_row[0] if runtime_row else None
                
                # Find slowest step
                cur.execute(f"""
                    SELECT step_number, step_name, duration_seconds
                    FROM {table_name}
                    WHERE run_id = %s
                    AND duration_seconds IS NOT NULL
                    ORDER BY duration_seconds DESC
                    LIMIT 1
                """, (run_id,))
                slowest_row = cur.fetchone()
                slowest_step_num = slowest_row[0] if slowest_row else None
                slowest_step_name = slowest_row[1] if slowest_row else None
                
                # Find failure step
                cur.execute(f"""
                    SELECT step_number, step_name
                    FROM {table_name}
                    WHERE run_id = %s
                    AND status = 'failed'
                    ORDER BY step_number ASC
                    LIMIT 1
                """, (run_id,))
                failure_row = cur.fetchone()
                failure_step_num = failure_row[0] if failure_row else None
                failure_step_name = failure_row[1] if failure_row else None
                
                # Update run_ledger
                cur.execute("""
                    UPDATE run_ledger 
                    SET 
                        total_runtime_seconds = COALESCE(%s, total_runtime_seconds),
                        slowest_step_number = COALESCE(%s, slowest_step_number),
                        slowest_step_name = COALESCE(%s, slowest_step_name),
                        failure_step_number = COALESCE(%s, failure_step_number),
                        failure_step_name = COALESCE(%s, failure_step_name)
                    WHERE run_id = %s AND scraper_name = %s
                """, (
                    total_runtime,
                    slowest_step_num,
                    slowest_step_name,
                    failure_step_num,
                    failure_step_name,
                    run_id,
                    scraper_name
                ))
                
                return True
    except Exception as e:
        logger.debug(f"Could not update run_ledger aggregation for {scraper_name}: {e}")
        return False
