#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standardized Error Tracker

Provides a unified interface for tracking errors in database across all regions.
Each region can use this to log errors without duplicating code.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


def log_error(
    scraper_name: str,
    run_id: str,
    error_type: str,
    error_message: str,
    context: Optional[Dict[str, Any]] = None,
    step_num: Optional[int] = None,
    step_name: Optional[str] = None
) -> bool:
    """
    Log an error to the database for any scraper.
    
    Args:
        scraper_name: Name of the scraper (e.g., "Argentina", "Malaysia")
        run_id: Current run ID
        error_type: Type of error (e.g., "scraping", "validation", "database")
        error_message: Error message
        context: Optional context dictionary with additional error details
        step_num: Optional step number where error occurred
        step_name: Optional step name where error occurred
    
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
            "India": "in",
        }
        
        prefix = table_prefix_map.get(scraper_name, scraper_name.lower()[:2])
        table_name = f"{prefix}_errors"
        
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
                    logger.debug(f"Table {table_name} does not exist for {scraper_name}, skipping error logging")
                    return False
                
                # Insert error
                import json
                context_json = json.dumps(context) if context else None
                
                cur.execute(f"""
                    INSERT INTO {table_name}
                        (run_id, error_type, error_message, context, step_number, step_name, created_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (run_id, error_type, error_message, context_json, step_num, step_name))
                
                return True
    except Exception as e:
        # Non-blocking: error logging should not break pipeline execution
        logger.debug(f"Could not log error for {scraper_name}: {e}")
        return False
