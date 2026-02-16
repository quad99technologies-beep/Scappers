#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline Scheduler

Cron-like scheduling for automated pipeline runs.

Usage:
    python services/scheduler.py
    
    Or run as daemon:
    python services/scheduler.py --daemon
"""

import os
import sys
import time
import argparse
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import re

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from core.db.postgres_connection import get_db

logger = logging.getLogger(__name__)


def parse_cron(cron_expr: str) -> Dict[str, Any]:
    """
    Parse cron expression (simplified - supports common patterns).
    
    Format: "minute hour day month day_of_week"
    Examples:
        "0 2 * * *" - Daily at 2 AM
        "0 */6 * * *" - Every 6 hours
        "0 0 * * 0" - Weekly on Sunday at midnight
    
    Returns:
        Dictionary with parsed schedule
    """
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {cron_expr}")
    
    minute, hour, day, month, dow = parts
    
    return {
        "minute": minute,
        "hour": hour,
        "day": day,
        "month": month,
        "day_of_week": dow
    }


def cron_matches(cron_schedule: Dict[str, Any], dt: datetime) -> bool:
    """
    Check if datetime matches cron schedule.
    
    Args:
        cron_schedule: Parsed cron schedule
        dt: Datetime to check
    
    Returns:
        True if matches
    """
    def matches_field(field: str, value: int) -> bool:
        if field == "*":
            return True
        if "/" in field:
            # */N pattern
            _, interval = field.split("/")
            return value % int(interval) == 0
        if "-" in field:
            # Range pattern
            start, end = field.split("-")
            return int(start) <= value <= int(end)
        if "," in field:
            # List pattern
            return value in [int(x) for x in field.split(",")]
        return int(field) == value
    
    return (
        matches_field(cron_schedule["minute"], dt.minute) and
        matches_field(cron_schedule["hour"], dt.hour) and
        matches_field(cron_schedule["day"], dt.day) and
        matches_field(cron_schedule["month"], dt.month) and
        matches_field(cron_schedule["day_of_week"], dt.weekday())
    )


def get_schedules() -> List[Dict[str, Any]]:
    """Get all enabled schedules from database."""
    try:
        db = get_db("system")
        with db.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'pipeline_schedules'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                # Create table
                cur.execute("""
                    CREATE TABLE pipeline_schedules (
                        id SERIAL PRIMARY KEY,
                        scraper_name TEXT NOT NULL,
                        schedule_cron TEXT NOT NULL,
                        enabled BOOLEAN DEFAULT true,
                        next_run_at TIMESTAMP,
                        last_run_id TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE INDEX idx_schedules_enabled ON pipeline_schedules(enabled);
                    CREATE INDEX idx_schedules_next_run ON pipeline_schedules(next_run_at);
                """)
                db.commit()
                return []
            
            cur.execute("""
                SELECT id, scraper_name, schedule_cron, enabled, next_run_at, last_run_id
                FROM pipeline_schedules
                WHERE enabled = true
            """)
            
            schedules = []
            for row in cur.fetchall():
                schedules.append({
                    "id": row[0],
                    "scraper_name": row[1],
                    "schedule_cron": row[2],
                    "enabled": row[3],
                    "next_run_at": row[4],
                    "last_run_id": row[5]
                })
            return schedules
    except Exception as e:
        logger.error(f"Could not get schedules: {e}")
        return []


def update_schedule_next_run(schedule_id: int, next_run_at: datetime, last_run_id: Optional[str] = None):
    """Update schedule's next_run_at timestamp."""
    try:
        db = get_db("system")
        with db.cursor() as cur:
            if last_run_id:
                cur.execute("""
                    UPDATE pipeline_schedules
                    SET next_run_at = %s, last_run_id = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (next_run_at, last_run_id, schedule_id))
            else:
                cur.execute("""
                    UPDATE pipeline_schedules
                    SET next_run_at = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (next_run_at, schedule_id))
            db.commit()
    except Exception as e:
        logger.error(f"Could not update schedule: {e}")


def calculate_next_run(cron_expr: str, from_dt: Optional[datetime] = None) -> datetime:
    """
    Calculate next run time from cron expression.
    
    Args:
        cron_expr: Cron expression
        from_dt: Starting datetime (defaults to now)
    
    Returns:
        Next run datetime
    """
    from_dt = from_dt or datetime.now()
    schedule = parse_cron(cron_expr)
    
    # Simple implementation: check next 24 hours
    for hours in range(24):
        check_dt = from_dt + timedelta(hours=hours)
        if cron_matches(schedule, check_dt):
            return check_dt
    
    # If no match in 24 hours, try next day
    return from_dt + timedelta(days=1)


def run_pipeline(scraper_name: str) -> Optional[str]:
    """
    Run a pipeline.
    
    Args:
        scraper_name: Name of the scraper
    
    Returns:
        Run ID if successful, None otherwise
    """
    try:
        script_path = REPO_ROOT / "scripts" / scraper_name / "run_pipeline_resume.py"
        if not script_path.exists():
            logger.error(f"Pipeline script not found: {script_path}")
            return None
        
        # Run pipeline in background
        process = subprocess.Popen(
            [sys.executable, str(script_path)],
            cwd=str(script_path.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Extract run_id from output (if available)
        # This is a simplified version - actual implementation would parse output
        return f"scheduled_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    except Exception as e:
        logger.error(f"Could not run pipeline {scraper_name}: {e}")
        return None


def scheduler_loop(interval_seconds: int = 60):
    """Main scheduler loop."""
    logger.info("Scheduler started")
    
    while True:
        try:
            now = datetime.now()
            schedules = get_schedules()
            
            for schedule in schedules:
                scraper_name = schedule["scraper_name"]
                cron_expr = schedule["schedule_cron"]
                schedule_id = schedule["id"]
                next_run_at = schedule.get("next_run_at")
                
                # Initialize next_run_at if not set
                if not next_run_at:
                    next_run_at = calculate_next_run(cron_expr, now)
                    update_schedule_next_run(schedule_id, next_run_at)
                    continue
                
                # Check if it's time to run
                if now >= next_run_at:
                    logger.info(f"Running scheduled pipeline: {scraper_name}")
                    run_id = run_pipeline(scraper_name)
                    
                    # Calculate next run
                    new_next_run = calculate_next_run(cron_expr, now)
                    update_schedule_next_run(schedule_id, new_next_run, run_id)
                    logger.info(f"Scheduled next run for {scraper_name}: {new_next_run}")
            
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Scheduler stopped")
            break
        except Exception as e:
            logger.error(f"Scheduler error: {e}", exc_info=True)
            time.sleep(interval_seconds)


def main():
    parser = argparse.ArgumentParser(description="Pipeline Scheduler")
    parser.add_argument("--daemon", action="store_true", help="Run as daemon")
    parser.add_argument("--interval", type=int, default=60, help="Check interval in seconds")
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    if args.daemon:
        # Run as daemon (simplified - use systemd/supervisor in production)
        import daemon
        with daemon.DaemonContext():
            scheduler_loop(args.interval)
    else:
        scheduler_loop(args.interval)


if __name__ == "__main__":
    main()
