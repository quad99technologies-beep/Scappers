#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Anomaly Detection

Detect unusual patterns in scraped data.

Usage:
    from core.monitoring.anomaly_detection import detect_anomalies
    
    anomalies = detect_anomalies("Malaysia", run_id)
"""

import logging
import statistics
from typing import List, Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def detect_price_outliers(scraper_name: str, run_id: str, threshold_sigma: float = 3.0) -> List[Dict[str, Any]]:
    """Detect price outliers using 3-sigma rule."""
    try:
        from core.db.postgres_connection import get_db, COUNTRY_PREFIX_MAP
        
        db = get_db(scraper_name)
        prefix = COUNTRY_PREFIX_MAP.get(scraper_name, "")
        product_table = f"{prefix}_products"
        
        anomalies = []
        with db.cursor() as cur:
            # Get price column name (varies by country)
            price_columns = ["price", "unit_price", "retail_price", "price_ars"]
            
            for price_col in price_columns:
                try:
                    cur.execute(f"""
                        SELECT {price_col}
                        FROM {product_table}
                        WHERE run_id = %s
                        AND {price_col} IS NOT NULL
                        AND {price_col} > 0
                    """, (run_id,))
                    
                    prices = [row[0] for row in cur.fetchall()]
                    if len(prices) < 10:
                        continue
                    
                    mean = statistics.mean(prices)
                    stdev = statistics.stdev(prices) if len(prices) > 1 else 0
                    
                    if stdev == 0:
                        continue
                    
                    threshold = threshold_sigma * stdev
                    outliers = [p for p in prices if abs(p - mean) > threshold]
                    
                    if outliers:
                        anomalies.append({
                            "type": "price_outlier",
                            "column": price_col,
                            "mean": mean,
                            "stdev": stdev,
                            "outlier_count": len(outliers),
                            "outliers": sorted(outliers, reverse=True)[:10]  # Top 10
                        })
                except Exception:
                    # Column doesn't exist, skip
                    continue
        
        return anomalies
    except Exception as e:
        logger.error(f"Could not detect price outliers: {e}")
        return []


def detect_row_count_anomalies(scraper_name: str, run_id: str, threshold_pct: float = 50.0) -> List[Dict[str, Any]]:
    """Detect sudden row count changes."""
    try:
        from core.db.postgres_connection import get_db, COUNTRY_PREFIX_MAP
        
        db = get_db(scraper_name)
        prefix = COUNTRY_PREFIX_MAP.get(scraper_name, "")
        table_name = f"{prefix}_step_progress"
        
        anomalies = []
        with db.cursor() as cur:
            # Get historical averages
            cur.execute(f"""
                SELECT step_number, AVG(rows_processed) as avg_rows
                FROM {table_name}
                WHERE scraper_name = %s
                AND rows_processed > 0
                AND run_id IN (
                    SELECT run_id FROM run_ledger
                    WHERE scraper_name = %s
                    AND started_at > CURRENT_TIMESTAMP - INTERVAL '30 days'
                    AND run_id != %s
                )
                GROUP BY step_number
            """, (scraper_name, scraper_name, run_id))
            
            historical_avg = {row[0]: row[1] for row in cur.fetchall()}
            
            # Get current run step counts
            cur.execute(f"""
                SELECT step_number, rows_processed
                FROM {table_name}
                WHERE run_id = %s
            """, (run_id,))
            
            current_counts = {row[0]: row[1] for row in cur.fetchall()}
            
            for step_num, current_count in current_counts.items():
                if current_count == 0:
                    continue
                
                avg_count = historical_avg.get(step_num, 0)
                if avg_count == 0:
                    continue
                
                delta_pct = ((current_count - avg_count) / avg_count) * 100
                
                if abs(delta_pct) > threshold_pct:
                    anomalies.append({
                        "type": "row_count_anomaly",
                        "step_number": step_num,
                        "current_count": current_count,
                        "avg_count": avg_count,
                        "delta_pct": delta_pct
                    })
        
        return anomalies
    except Exception as e:
        logger.error(f"Could not detect row count anomalies: {e}")
        return []


def detect_null_rate_spikes(scraper_name: str, run_id: str, threshold_pct: float = 10.0) -> List[Dict[str, Any]]:
    """Detect spikes in null rates."""
    try:
        from core.db.postgres_connection import get_db, COUNTRY_PREFIX_MAP
        
        db = get_db(scraper_name)
        prefix = COUNTRY_PREFIX_MAP.get(scraper_name, "")
        product_table = f"{prefix}_products"
        
        anomalies = []
        with db.cursor() as cur:
            # Check key columns
            key_columns = ["product_name", "company", "price"]
            
            for col in key_columns:
                try:
                    cur.execute(f"""
                        SELECT 
                            COUNT(*) as total,
                            COUNT(*) FILTER (WHERE {col} IS NULL) as null_count
                        FROM {product_table}
                        WHERE run_id = %s
                    """, (run_id,))
                    
                    row = cur.fetchone()
                    if row and row[0] > 0:
                        total = row[0]
                        null_count = row[1]
                        null_rate = (null_count / total) * 100
                        
                        if null_rate > threshold_pct:
                            anomalies.append({
                                "type": "null_rate_spike",
                                "column": col,
                                "null_rate": null_rate,
                                "null_count": null_count,
                                "total": total
                            })
                except Exception:
                    # Column doesn't exist, skip
                    continue
        
        return anomalies
    except Exception as e:
        logger.error(f"Could not detect null rate spikes: {e}")
        return []


def detect_anomalies(scraper_name: str, run_id: str) -> List[Dict[str, Any]]:
    """
    Detect all anomalies for a run.
    
    Args:
        scraper_name: Name of the scraper
        run_id: Run ID
    
    Returns:
        List of anomaly dictionaries
    """
    anomalies = []
    
    anomalies.extend(detect_price_outliers(scraper_name, run_id))
    anomalies.extend(detect_row_count_anomalies(scraper_name, run_id))
    anomalies.extend(detect_null_rate_spikes(scraper_name, run_id))
    
    return anomalies
