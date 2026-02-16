#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Alerting Integration

Integrates alerting contract with step hooks and Telegram notifier.
Automatically evaluates alert rules and sends notifications.

Usage:
    from core.alerting_integration import setup_alerting_hooks
    
    # Register alerting hooks (call once at startup)
    setup_alerting_hooks()
    
    # Alerts will now be sent automatically on step events
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

# Import hooks and alerting contract
try:
    from core.pipeline.step_hooks import StepHookRegistry, StepMetrics
    from core.monitoring.alerting_contract import AlertRuleRegistry, AlertChannel
    HOOKS_AVAILABLE = True
except ImportError as e:
    HOOKS_AVAILABLE = False
    logger.warning(f"Step hooks or alerting contract not available: {e}")

# Import Telegram notifier
try:
    from core.utils.telegram_notifier import TelegramNotifier
    TELEGRAM_AVAILABLE = True
except ImportError as e:
    TELEGRAM_AVAILABLE = False
    logger.warning(f"Telegram notifier not available: {e}")


def _get_avg_duration_from_db(scraper_name: str, step_number: int) -> float:
    """Get average duration for a step from database."""
    try:
        from core.db.connection import CountryDB
        
        table_prefix_map = {
            "Argentina": "ar",
            "Malaysia": "my",
            "Russia": "ru",
            "Belarus": "by",
            "Netherlands": "nl",
        }
        prefix = table_prefix_map.get(scraper_name, scraper_name.lower()[:2])
        table_name = f"{prefix}_step_progress"
        
        with CountryDB(scraper_name) as db:
            with db.cursor() as cur:
                cur.execute(f"""
                    SELECT AVG(duration_seconds)
                    FROM {table_name}
                    WHERE step_number = %s
                    AND duration_seconds IS NOT NULL
                    AND status = 'completed'
                    AND run_id IN (
                        SELECT run_id FROM run_ledger
                        WHERE scraper_name = %s
                        AND started_at > CURRENT_TIMESTAMP - INTERVAL '30 days'
                    )
                """, (step_number, scraper_name))
                row = cur.fetchone()
                return row[0] if row and row[0] else 0.0
    except Exception:
        return 0.0


def _alert_on_step_end(metrics: StepMetrics):
    """Alert hook: Evaluate alert rules on step completion."""
    if not HOOKS_AVAILABLE:
        return
    
    try:
        # Build context for alert rules
        avg_duration = _get_avg_duration_from_db(metrics.scraper_name, metrics.step_number)
        
        context = {
            "step_status": "completed" if not metrics.error_message else "failed",
            "step_number": metrics.step_number,
            "step_name": metrics.step_name,
            "duration_seconds": metrics.duration_seconds,
            "avg_duration_seconds": avg_duration,
            "rows_processed": metrics.rows_processed,
            "browser_instances_spawned": metrics.browser_instances_spawned,
            "scraper_name": metrics.scraper_name,
            "run_id": metrics.run_id,
            "error_message": metrics.error_message,
        }
        
        # Evaluate alert rules
        triggered_rules = AlertRuleRegistry.evaluate_rules(context)
        
        if not triggered_rules:
            return
        
        # Send alerts via configured channels
        if TELEGRAM_AVAILABLE:
            notifier = TelegramNotifier(metrics.scraper_name)
            
            for rule in triggered_rules:
                if AlertChannel.TELEGRAM in (rule.channels or []):
                    message = rule.get_message(context)
                    notifier.send_status(
                        step=f"Alert: {rule.name}",
                        description=message,
                        force=True  # Force send for alerts
                    )
                    logger.info(f"Sent alert via Telegram: {rule.name}")
        
        # Log alerts
        for rule in triggered_rules:
            logger.warning(f"Alert triggered: {rule.name} - {rule.get_message(context)}")
            
    except Exception as e:
        logger.error(f"Alert hook failed: {e}", exc_info=True)


def _alert_on_step_error(metrics: StepMetrics, error: Exception):
    """Alert hook: Evaluate alert rules on step error."""
    if not HOOKS_AVAILABLE:
        return
    
    try:
        context = {
            "step_status": "failed",
            "step_number": metrics.step_number,
            "step_name": metrics.step_name,
            "scraper_name": metrics.scraper_name,
            "run_id": metrics.run_id,
            "error_message": str(error),
        }
        
        triggered_rules = AlertRuleRegistry.evaluate_rules(context)
        
        if triggered_rules and TELEGRAM_AVAILABLE:
            notifier = TelegramNotifier(metrics.scraper_name)
            for rule in triggered_rules:
                if AlertChannel.TELEGRAM in (rule.channels or []):
                    message = rule.get_message(context)
                    # Build error message with title prefix
                    error_msg = f"Pipeline Error: {metrics.scraper_name}\n\n{message}"
                    notifier.send_error(
                        error_msg=error_msg,
                        force=True
                    )
                    
    except Exception as e:
        logger.error(f"Error alert hook failed: {e}", exc_info=True)


def setup_alerting_hooks():
    """
    Register alerting hooks with step hook registry.
    
    Call this once at application startup to enable automatic alerting.
    """
    if not HOOKS_AVAILABLE:
        logger.warning("Cannot setup alerting hooks: dependencies not available")
        return
    
    StepHookRegistry.register_end_hook(_alert_on_step_end)
    StepHookRegistry.register_error_hook(_alert_on_step_error)
    logger.info("Alerting hooks registered")
