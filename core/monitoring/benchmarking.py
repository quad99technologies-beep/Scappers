#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Performance Benchmarking

Track and compare step performance across runs.

Usage:
    from core.monitoring.benchmarking import record_step_benchmark
    
    record_step_benchmark(
        scraper_name="Malaysia",
        step_number=2,
        step_name="Product Details",
        run_id="run_20260206_abc",
        duration_seconds=125.5,
        rows_processed=1000
    )
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def record_step_benchmark(
    scraper_name: str,
    step_number: int,
    step_name: str,
    run_id: str,
    duration_seconds: float,
    rows_processed: int = 0,
    rows_per_second: Optional[float] = None
) -> bool:
    """
    Record a step benchmark to database.
    
    Args:
        scraper_name: Name of the scraper
        step_number: Step number
        step_name: Step name
        run_id: Run ID
        duration_seconds: Step duration in seconds
        rows_processed: Number of rows processed
        rows_per_second: Optional rows per second (calculated if not provided)
    
    Returns:
        True if recorded successfully, False otherwise
    """
    try:
        from core.db.postgres_connection import get_db
        
        if rows_per_second is None and rows_processed > 0:
            rows_per_second = rows_processed / duration_seconds if duration_seconds > 0 else 0
        
        db = get_db(scraper_name)
        with db.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'pipeline_benchmarks'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                # Create table
                cur.execute("""
                    CREATE TABLE pipeline_benchmarks (
                        id SERIAL PRIMARY KEY,
                        scraper_name TEXT NOT NULL,
                        step_number INTEGER NOT NULL,
                        step_name TEXT NOT NULL,
                        run_id TEXT NOT NULL,
                        duration_seconds REAL NOT NULL,
                        rows_processed INTEGER DEFAULT 0,
                        rows_per_second REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE INDEX idx_benchmarks_scraper_step ON pipeline_benchmarks(scraper_name, step_number);
                    CREATE INDEX idx_benchmarks_run ON pipeline_benchmarks(run_id);
                    CREATE INDEX idx_benchmarks_created ON pipeline_benchmarks(created_at DESC);
                """)
                db.commit()
            
            # Insert benchmark
            cur.execute("""
                INSERT INTO pipeline_benchmarks
                    (scraper_name, step_number, step_name, run_id, duration_seconds, rows_processed, rows_per_second)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                scraper_name,
                step_number,
                step_name,
                run_id,
                duration_seconds,
                rows_processed,
                rows_per_second
            ))
            db.commit()
            return True
    except Exception as e:
        logger.debug(f"Could not record benchmark: {e}")
        return False


def get_step_statistics(
    scraper_name: str,
    step_number: int,
    days: int = 30
) -> Dict[str, Any]:
    """
    Get step performance statistics.
    
    Args:
        scraper_name: Name of the scraper
        step_number: Step number
        days: Number of days to look back
    
    Returns:
        Dictionary with statistics (p50, p95, p99, avg, min, max)
    """
    try:
        from core.db.postgres_connection import get_db
        
        db = get_db(scraper_name)
        with db.cursor() as cur:
            cur.execute("""
                SELECT 
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_seconds) as p50,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_seconds) as p95,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_seconds) as p99,
                    AVG(duration_seconds) as avg_duration,
                    MIN(duration_seconds) as min_duration,
                    MAX(duration_seconds) as max_duration,
                    COUNT(*) as run_count
                FROM pipeline_benchmarks
                WHERE scraper_name = %s
                AND step_number = %s
                AND created_at > CURRENT_TIMESTAMP - INTERVAL '%s days'
            """, (scraper_name, step_number, days))
            
            row = cur.fetchone()
            if row and row[6] > 0:  # run_count > 0
                return {
                    "p50": float(row[0]) if row[0] else 0,
                    "p95": float(row[1]) if row[1] else 0,
                    "p99": float(row[2]) if row[2] else 0,
                    "avg": float(row[3]) if row[3] else 0,
                    "min": float(row[4]) if row[4] else 0,
                    "max": float(row[5]) if row[5] else 0,
                    "run_count": row[6]
                }
            return {}
    except Exception as e:
        logger.debug(f"Could not get step statistics: {e}")
        return {}


def detect_performance_regression(
    scraper_name: str,
    step_number: int,
    current_duration: float,
    threshold_multiplier: float = 2.0
) -> Dict[str, Any]:
    """
    Detect if current step duration indicates a performance regression.
    
    Args:
        scraper_name: Name of the scraper
        step_number: Step number
        current_duration: Current step duration
        threshold_multiplier: Multiplier for average to trigger regression
    
    Returns:
        Dictionary with regression info if detected, empty dict otherwise
    """
    stats = get_step_statistics(scraper_name, step_number)
    if not stats or stats.get("avg", 0) == 0:
        return {}
    
    avg_duration = stats["avg"]
    if current_duration > (avg_duration * threshold_multiplier):
        return {
            "is_regression": True,
            "current_duration": current_duration,
            "avg_duration": avg_duration,
            "threshold": avg_duration * threshold_multiplier,
            "multiplier": current_duration / avg_duration
        }
    
    return {"is_regression": False}
