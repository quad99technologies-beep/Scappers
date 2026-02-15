#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit Logging

Tracks who did what and when for compliance and debugging.

Usage:
    from core.audit_logger import audit_log
    
    audit_log(
        action="run_started",
        scraper_name="Malaysia",
        run_id="run_20260206_abc",
        user="system"
    )
"""

import logging
import json
import socket
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def get_user() -> str:
    """Get current user (system or actual user)."""
    import os
    return os.getenv("USER", os.getenv("USERNAME", "system"))


def get_ip_address() -> str:
    """Get local IP address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "unknown"


def audit_log(
    action: str,
    scraper_name: Optional[str] = None,
    run_id: Optional[str] = None,
    user: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    ip_address: Optional[str] = None
) -> bool:
    """
    Log an audit event to database.
    
    Args:
        action: Action performed (e.g., "run_started", "run_stopped", "config_changed")
        scraper_name: Optional scraper name
        run_id: Optional run ID
        user: Optional user (defaults to current user or "system")
        details: Optional additional details
        ip_address: Optional IP address (defaults to local IP)
    
    Returns:
        True if logged successfully, False otherwise
    """
    try:
        from core.db.postgres_connection import get_db
        
        user = user or get_user()
        ip_address = ip_address or get_ip_address()
        
        db = get_db(scraper_name or "system")
        with db.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'audit_log'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                # Create table
                cur.execute("""
                    CREATE TABLE audit_log (
                        id SERIAL PRIMARY KEY,
                        user TEXT NOT NULL,
                        action TEXT NOT NULL,
                        scraper_name TEXT,
                        run_id TEXT,
                        details_json JSONB DEFAULT '{}'::jsonb,
                        ip_address TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE INDEX idx_audit_log_user ON audit_log(user);
                    CREATE INDEX idx_audit_log_action ON audit_log(action);
                    CREATE INDEX idx_audit_log_scraper ON audit_log(scraper_name);
                    CREATE INDEX idx_audit_log_run ON audit_log(run_id);
                    CREATE INDEX idx_audit_log_created ON audit_log(created_at DESC);
                """)
                db.commit()
            
            # Insert audit log entry
            cur.execute("""
                INSERT INTO audit_log
                    (user, action, scraper_name, run_id, details_json, ip_address)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s)
            """, (
                user,
                action,
                scraper_name,
                run_id,
                json.dumps(details or {}),
                ip_address
            ))
            db.commit()
            return True
    except Exception as e:
        logger.debug(f"Could not write audit log: {e}")
        return False
