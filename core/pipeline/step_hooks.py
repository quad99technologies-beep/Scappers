#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step Event Hooks Contract

Standardizes step lifecycle hooks so dashboards/alerts/schedulers can attach
without modifying step logic.

Usage:
    from core.step_hooks import StepHookRegistry, StepMetrics
    
    # Register a hook
    def my_dashboard_hook(metrics: StepMetrics):
        print(f"Step {metrics.step_number} completed in {metrics.duration_seconds}s")
    
    StepHookRegistry.register_end_hook(my_dashboard_hook)
    
    # In run_pipeline_resume.py:
    metrics = StepMetrics(...)
    StepHookRegistry.emit_step_start(metrics)
    # ... execute step ...
    StepHookRegistry.emit_step_end(metrics)
"""

import logging
from typing import Dict, Any, Optional, Callable, List
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class StepMetrics:
    """Standardized step metrics structure."""
    step_number: int
    step_name: str
    run_id: str
    scraper_name: str
    duration_seconds: float = 0.0
    rows_read: int = 0
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_rejected: int = 0
    browser_instances_spawned: int = 0
    log_file_path: Optional[str] = None
    error_message: Optional[str] = None
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime = field(default_factory=datetime.now)
    context: Dict[str, Any] = field(default_factory=dict)  # Additional context
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "step_number": self.step_number,
            "step_name": self.step_name,
            "run_id": self.run_id,
            "scraper_name": self.scraper_name,
            "duration_seconds": self.duration_seconds,
            "rows_read": self.rows_read,
            "rows_processed": self.rows_processed,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_rejected": self.rows_rejected,
            "browser_instances_spawned": self.browser_instances_spawned,
            "log_file_path": self.log_file_path,
            "error_message": self.error_message,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "context": self.context,
        }


class StepHookRegistry:
    """Central registry for step lifecycle hooks."""
    
    _on_step_start: List[Callable[[StepMetrics], None]] = []
    _on_step_end: List[Callable[[StepMetrics], None]] = []
    _on_step_error: List[Callable[[StepMetrics, Exception], None]] = []
    
    @classmethod
    def register_start_hook(cls, callback: Callable[[StepMetrics], None]):
        """
        Register callback for step start events.
        
        Args:
            callback: Function that takes StepMetrics as argument
        """
        if callback not in cls._on_step_start:
            cls._on_step_start.append(callback)
            logger.debug(f"Registered step start hook: {callback.__name__}")
    
    @classmethod
    def register_end_hook(cls, callback: Callable[[StepMetrics], None]):
        """
        Register callback for step end events.
        
        Args:
            callback: Function that takes StepMetrics as argument
        """
        if callback not in cls._on_step_end:
            cls._on_step_end.append(callback)
            logger.debug(f"Registered step end hook: {callback.__name__}")
    
    @classmethod
    def register_error_hook(cls, callback: Callable[[StepMetrics, Exception], None]):
        """
        Register callback for step error events.
        
        Args:
            callback: Function that takes StepMetrics and Exception as arguments
        """
        if callback not in cls._on_step_error:
            cls._on_step_error.append(callback)
            logger.debug(f"Registered step error hook: {callback.__name__}")
    
    @classmethod
    def unregister_start_hook(cls, callback: Callable[[StepMetrics], None]):
        """Unregister a step start hook."""
        if callback in cls._on_step_start:
            cls._on_step_start.remove(callback)
    
    @classmethod
    def unregister_end_hook(cls, callback: Callable[[StepMetrics], None]):
        """Unregister a step end hook."""
        if callback in cls._on_step_end:
            cls._on_step_end.remove(callback)
    
    @classmethod
    def unregister_error_hook(cls, callback: Callable[[StepMetrics, Exception], None]):
        """Unregister a step error hook."""
        if callback in cls._on_step_error:
            cls._on_step_error.remove(callback)
    
    @classmethod
    def emit_step_start(cls, metrics: StepMetrics):
        """
        Emit step start event to all registered hooks.
        
        Args:
            metrics: StepMetrics instance with step information
        """
        for hook in cls._on_step_start:
            try:
                hook(metrics)
            except Exception as e:
                logger.error(f"Step start hook {hook.__name__} failed: {e}", exc_info=True)
    
    @classmethod
    def emit_step_end(cls, metrics: StepMetrics):
        """
        Emit step end event to all registered hooks.
        
        Args:
            metrics: StepMetrics instance with step information
        """
        for hook in cls._on_step_end:
            try:
                hook(metrics)
            except Exception as e:
                logger.error(f"Step end hook {hook.__name__} failed: {e}", exc_info=True)
    
    @classmethod
    def emit_step_error(cls, metrics: StepMetrics, error: Exception):
        """
        Emit step error event to all registered hooks.
        
        Args:
            metrics: StepMetrics instance with step information
            error: Exception that occurred
        """
        metrics.error_message = str(error)
        for hook in cls._on_step_error:
            try:
                hook(metrics, error)
            except Exception as e:
                logger.error(f"Step error hook {hook.__name__} failed: {e}", exc_info=True)
    
    @classmethod
    def clear_all_hooks(cls):
        """Clear all registered hooks (useful for testing)."""
        cls._on_step_start.clear()
        cls._on_step_end.clear()
        cls._on_step_error.clear()
