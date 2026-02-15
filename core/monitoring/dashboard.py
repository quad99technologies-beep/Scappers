#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Real-Time Dashboard Module

Provides dashboard data for GUI integration.
Can be used by scraper_gui.py to display real-time pipeline status.

Usage:
    from core.monitoring.dashboard import get_dashboard_data
    
    data = get_dashboard_data("Malaysia")
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def get_pipeline_status(scraper_name: str) -> Dict[str, Any]:
    """Get current pipeline status."""
    try:
        from core.db.postgres_connection import get_db
        
        db = get_db(scraper_name)
        with db.cursor() as cur:
            # Get latest run
            cur.execute("""
                SELECT run_id, status, started_at, ended_at, step_count,
                       total_runtime_seconds, slowest_step_number, slowest_step_name,
                       failure_step_number, failure_step_name
                FROM run_ledger
                WHERE scraper_name = %s
                ORDER BY started_at DESC
                LIMIT 1
            """, (scraper_name,))
            
            row = cur.fetchone()
            if row:
                return {
                    "scraper_name": scraper_name,
                    "run_id": row[0],
                    "status": row[1],
                    "started_at": row[2].isoformat() if row[2] else None,
                    "ended_at": row[3].isoformat() if row[3] else None,
                    "step_count": row[4],
                    "total_runtime_seconds": float(row[5]) if row[5] else None,
                    "slowest_step_number": row[6],
                    "slowest_step_name": row[7],
                    "failure_step_number": row[8],
                    "failure_step_name": row[9],
                    "is_running": row[1] == "running"
                }
            return {
                "scraper_name": scraper_name,
                "status": "idle",
                "run_id": None,
                "is_running": False
            }
    except Exception as e:
        logger.error(f"Could not get pipeline status: {e}")
        return {"scraper_name": scraper_name, "status": "error", "error": str(e)}


def get_current_step_progress(scraper_name: str, run_id: str) -> Dict[str, Any]:
    """Get current step progress."""
    try:
        from core.db.postgres_connection import get_db, COUNTRY_PREFIX_MAP
        
        db = get_db(scraper_name)
        prefix = COUNTRY_PREFIX_MAP.get(scraper_name, scraper_name.lower()[:2])
        table_name = f"{prefix}_step_progress"
        
        with db.cursor() as cur:
            # Get current step
            cur.execute(f"""
                SELECT step_number, step_name, status, duration_seconds,
                       rows_processed, rows_inserted, started_at, completed_at
                FROM {table_name}
                WHERE run_id = %s
                AND status IN ('in_progress', 'running')
                ORDER BY step_number DESC
                LIMIT 1
            """, (run_id,))
            
            row = cur.fetchone()
            if row:
                return {
                    "step_number": row[0],
                    "step_name": row[1],
                    "status": row[2],
                    "duration_seconds": float(row[3]) if row[3] else 0,
                    "rows_processed": row[4] or 0,
                    "rows_inserted": row[5] or 0,
                    "started_at": row[6].isoformat() if row[6] else None,
                    "completed_at": row[7].isoformat() if row[7] else None
                }
            return {}
    except Exception as e:
        logger.error(f"Could not get step progress: {e}")
        return {}


def get_dashboard_data(scraper_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Get dashboard data for one or all scrapers.
    
    Args:
        scraper_name: Optional scraper name (if None, returns all)
    
    Returns:
        Dictionary with dashboard data
    """
    scrapers = [scraper_name] if scraper_name else ["Malaysia", "Argentina", "Netherlands"]
    
    dashboard = {
        "timestamp": datetime.now().isoformat(),
        "scrapers": []
    }
    
    for scraper in scrapers:
        status = get_pipeline_status(scraper)
        
        if status.get("is_running") and status.get("run_id"):
            step_progress = get_current_step_progress(scraper, status["run_id"])
            status["current_step"] = step_progress
        
        dashboard["scrapers"].append(status)
    
    return dashboard


def get_recent_runs(scraper_name: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent runs for a scraper."""
    try:
        from core.db.postgres_connection import get_db
        
        db = get_db(scraper_name)
        with db.cursor() as cur:
            cur.execute("""
                SELECT run_id, status, started_at, ended_at, step_count,
                       total_runtime_seconds, failure_step_name
                FROM run_ledger
                WHERE scraper_name = %s
                ORDER BY started_at DESC
                LIMIT %s
            """, (scraper_name, limit))
            
            runs = []
            for row in cur.fetchall():
                runs.append({
                    "run_id": row[0],
                    "status": row[1],
                    "started_at": row[2].isoformat() if row[2] else None,
                    "ended_at": row[3].isoformat() if row[3] else None,
                    "step_count": row[4],
                    "total_runtime_seconds": float(row[5]) if row[5] else None,
                    "failure_step_name": row[6]
                })
            return runs
    except Exception as e:
        logger.error(f"Could not get recent runs: {e}")
        return []


def get_step_metrics_summary(scraper_name: str, run_id: str) -> Dict[str, Any]:
    """Get step metrics summary for a run."""
    try:
        from core.db.postgres_connection import get_db, COUNTRY_PREFIX_MAP
        
        db = get_db(scraper_name)
        prefix = COUNTRY_PREFIX_MAP.get(scraper_name, scraper_name.lower()[:2])
        table_name = f"{prefix}_step_progress"
        
        with db.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    step_number,
                    step_name,
                    status,
                    duration_seconds,
                    rows_read,
                    rows_processed,
                    rows_inserted,
                    rows_updated,
                    rows_rejected,
                    browser_instances_spawned
                FROM {table_name}
                WHERE run_id = %s
                ORDER BY step_number
            """, (run_id,))
            
            steps = []
            for row in cur.fetchall():
                steps.append({
                    "step_number": row[0],
                    "step_name": row[1],
                    "status": row[2],
                    "duration_seconds": float(row[3]) if row[3] else 0,
                    "rows_read": row[4] or 0,
                    "rows_processed": row[5] or 0,
                    "rows_inserted": row[6] or 0,
                    "rows_updated": row[7] or 0,
                    "rows_rejected": row[8] or 0,
                    "browser_instances_spawned": row[9] or 0
                })
            
            return {
                "run_id": run_id,
                "scraper_name": scraper_name,
                "steps": steps,
                "total_steps": len(steps),
                "completed_steps": sum(1 for s in steps if s["status"] == "completed"),
                "failed_steps": sum(1 for s in steps if s["status"] == "failed")
            }
    except Exception as e:
        logger.error(f"Could not get step metrics: {e}")
        return {}
