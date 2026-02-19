#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Webhook Notifications

Send webhooks on pipeline events.

Usage:
    from services.webhook_notifications import send_webhook
    
    send_webhook(
        event="pipeline.completed",
        scraper_name="Malaysia",
        run_id="run_20260206_abc",
        data={"status": "completed"}
    )
"""

import logging
import json
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


def get_webhook_configs(scraper_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get webhook configurations from database."""
    try:
        from core.db.postgres_connection import get_db
        
        db = get_db("system")
        with db.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'webhook_configs'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                # Create table
                cur.execute("""
                    CREATE TABLE webhook_configs (
                        id SERIAL PRIMARY KEY,
                        scraper_name TEXT,
                        event_type TEXT NOT NULL,
                        webhook_url TEXT NOT NULL,
                        enabled BOOLEAN DEFAULT true,
                        retry_count INTEGER DEFAULT 3,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE INDEX idx_webhook_configs_scraper ON webhook_configs(scraper_name);
                    CREATE INDEX idx_webhook_configs_event ON webhook_configs(event_type);
                """)
                db.commit()
                return []
            
            query = "SELECT id, scraper_name, event_type, webhook_url, enabled, retry_count FROM webhook_configs WHERE enabled = true"
            params = []
            
            if scraper_name:
                query += " AND (scraper_name = %s OR scraper_name IS NULL)"
                params.append(scraper_name)
            
            cur.execute(query, params)
            
            configs = []
            for row in cur.fetchall():
                configs.append({
                    "id": row[0],
                    "scraper_name": row[1],
                    "event_type": row[2],
                    "webhook_url": row[3],
                    "enabled": row[4],
                    "retry_count": row[5]
                })
            return configs
    except Exception as e:
        logger.error(f"Could not get webhook configs: {e}")
        return []


def send_webhook(
    event: str,
    scraper_name: str,
    run_id: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
    retry_count: int = 3
) -> bool:
    """
    Send webhook notification.
    
    Args:
        event: Event type (e.g., "pipeline.started", "pipeline.completed")
        scraper_name: Name of the scraper
        run_id: Optional run ID
        data: Optional additional data
        retry_count: Number of retry attempts
    
    Returns:
        True if sent successfully
    """
    configs = get_webhook_configs(scraper_name)
    
    # Filter configs by event type
    matching_configs = [
        c for c in configs
        if c["event_type"] == event or c["event_type"] == "*"
    ]
    
    if not matching_configs:
        return False
    
    payload = {
        "event": event,
        "scraper_name": scraper_name,
        "run_id": run_id,
        "timestamp": datetime.now().isoformat(),
        "data": data or {}
    }
    
    success = False
    for config in matching_configs:
        url = config["webhook_url"]
        max_retries = config.get("retry_count", retry_count)
        
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    url,
                    json=payload,
                    timeout=10,
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()
                success = True
                logger.info(f"Webhook sent successfully: {url}")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Webhook failed (attempt {attempt + 1}/{max_retries}): {e}")
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Webhook failed after {max_retries} attempts: {e}")
    
    return success
