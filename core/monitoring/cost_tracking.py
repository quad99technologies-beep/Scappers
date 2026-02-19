#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cost Tracking

Track resource usage and estimate costs.

Usage:
    from core.monitoring.cost_tracking import track_run_cost
    
    track_run_cost(
        scraper_name="Malaysia",
        run_id="run_20260206_abc",
        browser_hours=2.5,
        db_queries=1500,
        network_mb=500
    )
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


def track_run_cost(
    scraper_name: str,
    run_id: str,
    browser_hours: float = 0.0,
    db_queries: int = 0,
    network_mb: float = 0.0,
    storage_mb: float = 0.0
) -> bool:
    """
    Track resource usage and cost for a run.
    
    Args:
        scraper_name: Name of the scraper
        run_id: Run ID
        browser_hours: Browser instance hours
        db_queries: Database query count
        network_mb: Network bandwidth in MB
        storage_mb: Storage used in MB
    
    Returns:
        True if tracked successfully
    """
    try:
        from core.db.postgres_connection import get_db
        
        # Cost estimates (adjust based on your infrastructure)
        COST_PER_BROWSER_HOUR = 0.10  # $0.10 per browser hour
        COST_PER_DB_QUERY = 0.0001    # $0.0001 per query
        COST_PER_GB_NETWORK = 0.10    # $0.10 per GB
        COST_PER_GB_STORAGE = 0.05    # $0.05 per GB per month
        
        estimated_cost = (
            (browser_hours * COST_PER_BROWSER_HOUR) +
            (db_queries * COST_PER_DB_QUERY) +
            (network_mb / 1024 * COST_PER_GB_NETWORK) +
            (storage_mb / 1024 * COST_PER_GB_STORAGE)
        )
        
        db = get_db(scraper_name)
        with db.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'cost_tracking'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                # Create table
                cur.execute("""
                    CREATE TABLE cost_tracking (
                        id SERIAL PRIMARY KEY,
                        run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                        scraper_name TEXT NOT NULL,
                        browser_hours REAL DEFAULT 0,
                        db_queries INTEGER DEFAULT 0,
                        network_mb REAL DEFAULT 0,
                        storage_mb REAL DEFAULT 0,
                        estimated_cost_usd REAL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE INDEX idx_cost_tracking_run ON cost_tracking(run_id);
                    CREATE INDEX idx_cost_tracking_scraper ON cost_tracking(scraper_name);
                """)
                db.commit()
            
            # Insert cost record
            cur.execute("""
                INSERT INTO cost_tracking
                    (run_id, scraper_name, browser_hours, db_queries, network_mb, storage_mb, estimated_cost_usd)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                run_id,
                scraper_name,
                browser_hours,
                db_queries,
                network_mb,
                storage_mb,
                estimated_cost
            ))
            db.commit()
            return True
    except Exception as e:
        logger.debug(f"Could not track cost: {e}")
        return False


def get_monthly_cost_summary(scraper_name: Optional[str] = None, month: Optional[int] = None) -> Dict[str, Any]:
    """Get monthly cost summary."""
    try:
        from core.db.postgres_connection import get_db
        
        db = get_db("system")
        with db.cursor() as cur:
            query = """
                SELECT 
                    scraper_name,
                    COUNT(*) as run_count,
                    SUM(browser_hours) as total_browser_hours,
                    SUM(db_queries) as total_db_queries,
                    SUM(network_mb) as total_network_mb,
                    SUM(storage_mb) as total_storage_mb,
                    SUM(estimated_cost_usd) as total_cost_usd
                FROM cost_tracking
                WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)
            """
            params = []
            
            if scraper_name:
                query += " AND scraper_name = %s"
                params.append(scraper_name)
            
            query += " GROUP BY scraper_name"
            
            cur.execute(query, params)
            
            summary = {
                "month": datetime.now().strftime("%Y-%m"),
                "scrapers": []
            }
            
            for row in cur.fetchall():
                summary["scrapers"].append({
                    "scraper_name": row[0],
                    "run_count": row[1],
                    "total_browser_hours": float(row[2]) if row[2] else 0,
                    "total_db_queries": row[3] or 0,
                    "total_network_mb": float(row[4]) if row[4] else 0,
                    "total_storage_mb": float(row[5]) if row[5] else 0,
                    "total_cost_usd": float(row[6]) if row[6] else 0
                })
            
            summary["total_cost_usd"] = sum(s["total_cost_usd"] for s in summary["scrapers"])
            return summary
    except Exception as e:
        logger.error(f"Could not get cost summary: {e}")
        return {}
