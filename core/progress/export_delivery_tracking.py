#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Export Delivery Tracking

Track export file delivery and client access.

Usage:
    from core.progress.export_delivery_tracking import track_export_delivery
    
    track_export_delivery(
        run_id="run_20260206_abc",
        scraper_name="Malaysia",
        export_file_path=Path("exports/report.csv"),
        delivery_method="email",
        recipient="client@example.com"
    )
"""

import logging
from typing import Optional
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


def track_export_delivery(
    run_id: str,
    scraper_name: str,
    export_file_path: Path,
    delivery_method: str = "manual",
    recipient: Optional[str] = None
) -> bool:
    """
    Track export file delivery.
    
    Args:
        run_id: Run ID
        scraper_name: Name of the scraper
        export_file_path: Path to export file
        delivery_method: Delivery method (email, sftp, s3, manual)
        recipient: Recipient email/identifier
    
    Returns:
        True if tracked successfully
    """
    try:
        from core.db.postgres_connection import get_db
        
        db = get_db(scraper_name)
        with db.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'export_deliveries'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                # Create table
                cur.execute("""
                    CREATE TABLE export_deliveries (
                        id SERIAL PRIMARY KEY,
                        run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                        scraper_name TEXT NOT NULL,
                        export_file_path TEXT NOT NULL,
                        delivery_method TEXT,
                        delivered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        recipient TEXT,
                        download_count INTEGER DEFAULT 0,
                        last_downloaded_at TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE INDEX idx_export_deliveries_run ON export_deliveries(run_id);
                    CREATE INDEX idx_export_deliveries_scraper ON export_deliveries(scraper_name);
                """)
                db.commit()
            
            # Insert or update delivery record
            cur.execute("""
                INSERT INTO export_deliveries
                    (run_id, scraper_name, export_file_path, delivery_method, recipient)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (run_id, export_file_path) DO UPDATE SET
                    delivery_method = EXCLUDED.delivery_method,
                    recipient = EXCLUDED.recipient,
                    delivered_at = CURRENT_TIMESTAMP
            """, (
                run_id,
                scraper_name,
                str(export_file_path),
                delivery_method,
                recipient
            ))
            db.commit()
            return True
    except Exception as e:
        logger.debug(f"Could not track export delivery: {e}")
        return False


def record_export_download(
    run_id: str,
    export_file_path: Path,
    scraper_name: Optional[str] = None
) -> bool:
    """
    Record export file download.
    
    Args:
        run_id: Run ID
        export_file_path: Path to export file
        scraper_name: Optional scraper name
    
    Returns:
        True if recorded successfully
    """
    try:
        from core.db.postgres_connection import get_db
        
        scraper_name = scraper_name or "system"
        db = get_db(scraper_name)
        with db.cursor() as cur:
            cur.execute("""
                UPDATE export_deliveries
                SET 
                    download_count = download_count + 1,
                    last_downloaded_at = CURRENT_TIMESTAMP
                WHERE run_id = %s AND export_file_path = %s
            """, (run_id, str(export_file_path)))
            db.commit()
            return True
    except Exception as e:
        logger.debug(f"Could not record export download: {e}")
        return False
