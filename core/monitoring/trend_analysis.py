#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Run Trend Analysis

Analyze trends across multiple runs.

Usage:
    from core.monitoring.trend_analysis import analyze_trends
    
    trends = analyze_trends("Malaysia", days=30)
"""

import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def analyze_trends(scraper_name: str, days: int = 30) -> Dict[str, Any]:
    """
    Analyze trends across multiple runs.
    
    Args:
        scraper_name: Name of the scraper
        days: Number of days to analyze
    
    Returns:
        Dictionary with trend analysis
    """
    try:
        from core.db.postgres_connection import get_db
        
        db = get_db(scraper_name)
        
        trends = {
            "scraper_name": scraper_name,
            "analysis_period_days": days,
            "runs_analyzed": 0,
            "step_duration_trends": {},
            "success_rate": 0,
            "data_volume_trend": {},
            "error_patterns": []
        }
        
        with db.cursor() as cur:
            # Get run statistics
            cur.execute("""
                SELECT 
                    COUNT(*) as total_runs,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed_runs,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_runs,
                    AVG(total_runtime_seconds) as avg_runtime,
                    AVG(step_count) as avg_steps
                FROM run_ledger
                WHERE scraper_name = %s
                AND started_at > CURRENT_TIMESTAMP - INTERVAL '%s days'
            """, (scraper_name, days))
            
            row = cur.fetchone()
            if row and row[0] > 0:
                trends["runs_analyzed"] = row[0]
                trends["success_rate"] = (row[1] / row[0] * 100) if row[0] > 0 else 0
                trends["avg_runtime_seconds"] = float(row[3]) if row[3] else 0
                trends["avg_steps_completed"] = float(row[4]) if row[4] else 0
            
            # Get step duration trends
            table_prefix_map = {
                "Argentina": "ar",
                "Malaysia": "my",
                "Netherlands": "nl",
            }
            prefix = table_prefix_map.get(scraper_name, scraper_name.lower()[:2])
            table_name = f"{prefix}_step_progress"
            
            cur.execute(f"""
                SELECT 
                    step_number,
                    AVG(duration_seconds) as avg_duration,
                    MIN(duration_seconds) as min_duration,
                    MAX(duration_seconds) as max_duration,
                    COUNT(*) as run_count
                FROM {table_name}
                WHERE scraper_name = %s
                AND duration_seconds IS NOT NULL
                AND run_id IN (
                    SELECT run_id FROM run_ledger
                    WHERE scraper_name = %s
                    AND started_at > CURRENT_TIMESTAMP - INTERVAL '%s days'
                )
                GROUP BY step_number
                ORDER BY step_number
            """, (scraper_name, scraper_name, days))
            
            for row in cur.fetchall():
                trends["step_duration_trends"][row[0]] = {
                    "avg_duration": float(row[1]) if row[1] else 0,
                    "min_duration": float(row[2]) if row[2] else 0,
                    "max_duration": float(row[3]) if row[3] else 0,
                    "run_count": row[4]
                }
            
            # Get data volume trend
            cur.execute(f"""
                SELECT 
                    DATE(started_at) as run_date,
                    SUM(items_scraped) as total_items
                FROM run_ledger
                WHERE scraper_name = %s
                AND started_at > CURRENT_TIMESTAMP - INTERVAL '%s days'
                GROUP BY DATE(started_at)
                ORDER BY run_date
            """, (scraper_name, days))
            
            volume_trend = []
            for row in cur.fetchall():
                volume_trend.append({
                    "date": row[0].isoformat() if row[0] else None,
                    "items_scraped": row[1] or 0
                })
            trends["data_volume_trend"] = volume_trend
            
            # Get error patterns
            cur.execute("""
                SELECT 
                    failure_step_name,
                    COUNT(*) as failure_count
                FROM run_ledger
                WHERE scraper_name = %s
                AND failure_step_name IS NOT NULL
                AND started_at > CURRENT_TIMESTAMP - INTERVAL '%s days'
                GROUP BY failure_step_name
                ORDER BY failure_count DESC
            """, (scraper_name, days))
            
            for row in cur.fetchall():
                trends["error_patterns"].append({
                    "step_name": row[0],
                    "failure_count": row[1]
                })
        
        return trends
    except Exception as e:
        logger.error(f"Could not analyze trends: {e}")
        return {}
