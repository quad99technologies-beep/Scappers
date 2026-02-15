#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Alerting Contract

Define alert trigger rules now so alerting system can be bolted on later
without touching step logic.

Usage:
    from core.monitoring.alerting_contract import AlertRuleRegistry, StepFailedRule
    
    # Rules are auto-registered
    # In step hooks:
    context = {"step_status": "failed", ...}
    triggered = AlertRuleRegistry.evaluate_rules(context)
    for rule in triggered:
        send_alert(rule, context)
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    CRITICAL = "critical"  # Immediate notification
    HIGH = "high"          # Urgent notification
    MEDIUM = "medium"      # Standard notification
    LOW = "low"            # Informational only


class AlertChannel(Enum):
    """Alert delivery channels."""
    TELEGRAM = "telegram"
    EMAIL = "email"
    WEBHOOK = "webhook"
    SLACK = "slack"


@dataclass
class AlertRule:
    """Definition of an alert trigger rule."""
    name: str
    description: str
    severity: AlertSeverity
    enabled: bool = True
    channels: Optional[List[AlertChannel]] = None
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        """
        Check if this rule should trigger based on context.
        
        Args:
            context: Dictionary with step/pipeline context
            
        Returns:
            True if rule should trigger
        """
        raise NotImplementedError("Subclass must implement")
    
    def get_message(self, context: Dict[str, Any]) -> str:
        """
        Generate alert message from context.
        
        Args:
            context: Dictionary with step/pipeline context
            
        Returns:
            Alert message string
        """
        return f"{self.description}: {context.get('step_name', 'Unknown step')}"


class AlertRuleRegistry:
    """Central registry for alert rules."""
    
    _rules: List[AlertRule] = []
    
    @classmethod
    def register_rule(cls, rule: AlertRule):
        """Register an alert rule."""
        if rule not in cls._rules:
            cls._rules.append(rule)
            logger.debug(f"Registered alert rule: {rule.name}")
    
    @classmethod
    def unregister_rule(cls, rule: AlertRule):
        """Unregister an alert rule."""
        if rule in cls._rules:
            cls._rules.remove(rule)
    
    @classmethod
    def evaluate_rules(cls, context: Dict[str, Any]) -> List[AlertRule]:
        """
        Evaluate all rules and return triggered rules.
        
        Args:
            context: Dictionary with step/pipeline context
            
        Returns:
            List of triggered AlertRule instances
        """
        triggered = []
        for rule in cls._rules:
            if rule.enabled and rule.should_trigger(context):
                triggered.append(rule)
        return triggered
    
    @classmethod
    def clear_all_rules(cls):
        """Clear all registered rules (useful for testing)."""
        cls._rules.clear()


# =============================================================================
# Standard Alert Rules (MUST BE DEFINED NOW)
# =============================================================================

class StepFailedRule(AlertRule):
    """Alert: Step failed."""
    
    def __init__(self):
        super().__init__(
            name="step_failed",
            description="Pipeline step failed",
            severity=AlertSeverity.CRITICAL,
            channels=[AlertChannel.TELEGRAM, AlertChannel.EMAIL]
        )
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        return context.get("step_status") == "failed"
    
    def get_message(self, context: Dict[str, Any]) -> str:
        step_name = context.get("step_name", "Unknown step")
        error = context.get("error_message", "Unknown error")
        return f"❌ Step Failed: {step_name}\nError: {error}"


class StepDurationSpikeRule(AlertRule):
    """Alert: Step duration > threshold_multiplier x historical average."""
    
    def __init__(self, threshold_multiplier: float = 2.0):
        super().__init__(
            name="step_duration_spike",
            description=f"Step duration > {threshold_multiplier}x average",
            severity=AlertSeverity.HIGH,
            channels=[AlertChannel.TELEGRAM]
        )
        self.threshold_multiplier = threshold_multiplier
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        current_duration = context.get("duration_seconds", 0)
        avg_duration = context.get("avg_duration_seconds", 0)
        if avg_duration == 0:
            return False
        return current_duration > (avg_duration * self.threshold_multiplier)
    
    def get_message(self, context: Dict[str, Any]) -> str:
        step_name = context.get("step_name", "Unknown step")
        current = context.get("duration_seconds", 0)
        avg = context.get("avg_duration_seconds", 0)
        return f"⚠️ Step Duration Spike: {step_name}\nCurrent: {current:.1f}s (avg: {avg:.1f}s)"


class ZeroRowsRule(AlertRule):
    """Alert: Step processed zero rows."""
    
    def __init__(self):
        super().__init__(
            name="zero_rows",
            description="Step processed zero rows",
            severity=AlertSeverity.HIGH,
            channels=[AlertChannel.TELEGRAM]
        )
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        rows_processed = context.get("rows_processed", 0)
        step_number = context.get("step_number", 0)
        # Skip step 0 (backup/clean) as it may legitimately process 0 rows
        return rows_processed == 0 and step_number > 0
    
    def get_message(self, context: Dict[str, Any]) -> str:
        step_name = context.get("step_name", "Unknown step")
        return f"⚠️ Zero Rows Processed: {step_name}\nNo data was processed in this step"


class BrowserLeakRule(AlertRule):
    """Alert: Too many browser instances spawned."""
    
    def __init__(self, max_instances: int = 10):
        super().__init__(
            name="browser_leak",
            description=f"Browser instances > {max_instances}",
            severity=AlertSeverity.MEDIUM,
            channels=[AlertChannel.TELEGRAM]
        )
        self.max_instances = max_instances
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        browser_count = context.get("browser_instances_spawned", 0)
        return browser_count > self.max_instances
    
    def get_message(self, context: Dict[str, Any]) -> str:
        step_name = context.get("step_name", "Unknown step")
        browser_count = context.get("browser_instances_spawned", 0)
        return f"⚠️ Browser Instance Leak: {step_name}\n{browser_count} instances spawned (max: {self.max_instances})"


class DatabaseConnectionFailureRule(AlertRule):
    """Alert: Database connection failed."""
    
    def __init__(self):
        super().__init__(
            name="db_connection_failure",
            description="Database connection failed",
            severity=AlertSeverity.CRITICAL,
            channels=[AlertChannel.TELEGRAM, AlertChannel.EMAIL]
        )
    
    def should_trigger(self, context: Dict[str, Any]) -> bool:
        return context.get("db_connection_failed", False)
    
    def get_message(self, context: Dict[str, Any]) -> str:
        scraper_name = context.get("scraper_name", "Unknown scraper")
        error = context.get("db_error", "Unknown error")
        return f"❌ Database Connection Failed: {scraper_name}\nError: {error}"


# Auto-register standard rules
AlertRuleRegistry.register_rule(StepFailedRule())
AlertRuleRegistry.register_rule(StepDurationSpikeRule())
AlertRuleRegistry.register_rule(ZeroRowsRule())
AlertRuleRegistry.register_rule(BrowserLeakRule())
AlertRuleRegistry.register_rule(DatabaseConnectionFailureRule())
