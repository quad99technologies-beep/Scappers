#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run Metrics Integration

Integrates RunMetricsTracker with the WorkflowRunner to automatically track
network consumption and active execution time for all scraper runs.

This module provides:
1. Automatic metrics tracking during workflow execution
2. Pause/Resume support when pipeline is stopped/resumed
3. Integration with existing run ledger
"""

from __future__ import annotations

import logging
from typing import Optional, Dict, Any, Callable
from pathlib import Path

# Import the metrics tracker
try:
    from core.progress.run_metrics_tracker import (
        RunMetricsTracker,
        RunMetrics,
        get_metrics_tracker,
        format_metrics_summary,
    )
    _METRICS_AVAILABLE = True
except ImportError:
    _METRICS_AVAILABLE = False
    RunMetricsTracker = None
    RunMetrics = None

# Import run ledger for integration
try:
    from core.progress.run_ledger import FileRunLedger, RunStatus
    _RUN_LEDGER_AVAILABLE = True
except ImportError:
    _RUN_LEDGER_AVAILABLE = False
    FileRunLedger = None
    RunStatus = None

log = logging.getLogger(__name__)


class WorkflowMetricsIntegration:
    """
    Integration layer between WorkflowRunner and RunMetricsTracker.
    
    This class provides methods to hook into the workflow lifecycle:
    - on_run_start: Called when a run starts
    - on_run_pause: Called when a run is paused (e.g., user stops pipeline)
    - on_run_resume: Called when a run is resumed
    - on_run_complete: Called when a run completes
    
    Usage in WorkflowRunner:
        def __init__(self, ...):
            ...
            self.metrics_integration = WorkflowMetricsIntegration()
        
        def run(self, scraper, ...):
            # Start tracking
            self.metrics_integration.on_run_start(self.run_id, self.scraper_name)
            
            try:
                # ... run steps ...
                
                # On successful completion
                self.metrics_integration.on_run_complete(
                    self.run_id, 
                    status="completed"
                )
            except Exception as e:
                # On failure
                self.metrics_integration.on_run_complete(
                    self.run_id,
                    status="failed"
                )
                raise
    """
    
    def __init__(self):
        self._tracker: Optional[RunMetricsTracker] = None
        if _METRICS_AVAILABLE:
            self._tracker = get_metrics_tracker()
    
    def is_available(self) -> bool:
        """Check if metrics tracking is available."""
        return _METRICS_AVAILABLE and self._tracker is not None
    
    def on_run_start(self, run_id: str, scraper_name: str) -> Optional[RunMetrics]:
        """
        Called when a workflow run starts.
        
        Args:
            run_id: Unique run identifier
            scraper_name: Name of the scraper
            
        Returns:
            RunMetrics if tracking started, None if unavailable
        """
        if not self.is_available():
            log.debug("Metrics tracking not available, skipping start")
            return None
        
        try:
            metrics = self._tracker.start_run(run_id, scraper_name)
            log.info(f"Started metrics tracking for run {run_id}")
            return metrics
        except Exception as e:
            log.warning(f"Failed to start metrics tracking: {e}")
            return None
    
    def on_run_pause(self, run_id: str) -> Optional[RunMetrics]:
        """
        Called when a workflow run is paused (e.g., user stops pipeline).
        
        Args:
            run_id: Unique run identifier
            
        Returns:
            RunMetrics if tracking paused, None if unavailable
        """
        if not self.is_available():
            return None
        
        try:
            metrics = self._tracker.pause_run(run_id)
            if metrics:
                log.info(f"Paused metrics tracking for run {run_id}. "
                        f"Duration: {metrics.active_duration_seconds:.2f}s, "
                        f"Network: {metrics.network_total_gb:.4f} GB")
            return metrics
        except Exception as e:
            log.warning(f"Failed to pause metrics tracking: {e}")
            return None
    
    def on_run_resume(self, run_id: str) -> Optional[RunMetrics]:
        """
        Called when a workflow run is resumed.
        
        Args:
            run_id: Unique run identifier
            
        Returns:
            RunMetrics if tracking resumed, None if unavailable
        """
        if not self.is_available():
            return None
        
        try:
            metrics = self._tracker.resume_run(run_id)
            if metrics:
                log.info(f"Resumed metrics tracking for run {run_id}")
            return metrics
        except Exception as e:
            log.warning(f"Failed to resume metrics tracking: {e}")
            return None
    
    def on_run_complete(
        self,
        run_id: str,
        status: str = "completed",
    ) -> Optional[RunMetrics]:
        """
        Called when a workflow run completes (success, failure, or cancellation).
        
        Args:
            run_id: Unique run identifier
            status: Final status (completed, failed, cancelled)
            
        Returns:
            Final RunMetrics if tracking completed, None if unavailable
        """
        if not self.is_available():
            return None
        
        try:
            metrics = self._tracker.complete_run(run_id, status)
            if metrics:
                log.info(f"Completed metrics tracking for run {run_id}. "
                        f"Total Duration: {metrics.active_duration_seconds:.2f}s, "
                        f"Total Network: {metrics.network_total_gb:.4f} GB")
                
                # Also update the run ledger with metrics
                self._update_run_ledger(run_id, metrics)
            return metrics
        except Exception as e:
            log.warning(f"Failed to complete metrics tracking: {e}")
            return None
    
    def get_current_metrics(self, run_id: str) -> Optional[RunMetrics]:
        """Get current metrics for a run (including active session)."""
        if not self.is_available():
            return None
        
        try:
            return self._tracker.get_current_metrics(run_id)
        except Exception as e:
            log.warning(f"Failed to get current metrics: {e}")
            return None
    
    def _update_run_ledger(self, run_id: str, metrics: RunMetrics) -> None:
        """Update run ledger with metrics information."""
        if not _RUN_LEDGER_AVAILABLE:
            return
        
        try:
            ledger = FileRunLedger()
            run_metadata = ledger.get_run(run_id)
            
            if run_metadata:
                # Update metrics in the run ledger
                ledger.record_run_end(
                    run_id=run_id,
                    status=RunStatus(metrics.status) if metrics.status in [s.value for s in RunStatus] else RunStatus.COMPLETED,
                    metrics={
                        "active_duration_seconds": metrics.active_duration_seconds,
                        "network_sent_bytes": metrics.network_sent_bytes,
                        "network_received_bytes": metrics.network_received_bytes,
                        "network_total_gb": metrics.network_total_gb,
                        "network_sent_mb": metrics.network_sent_mb,
                        "network_received_mb": metrics.network_received_mb,
                    }
                )
                log.debug(f"Updated run ledger with metrics for {run_id}")
        except Exception as e:
            log.warning(f"Failed to update run ledger with metrics: {e}")


class MetricsContextManager:
    """
    Context manager for automatic metrics tracking.
    
    Usage:
        with MetricsContextManager(run_id, scraper_name) as metrics:
            # Run your scraper here
            ...
        # Metrics are automatically saved when exiting the context
    """
    
    def __init__(self, run_id: str, scraper_name: str):
        self.run_id = run_id
        self.scraper_name = scraper_name
        self.integration = WorkflowMetricsIntegration()
        self.metrics: Optional[RunMetrics] = None
        self._completed = False
    
    def __enter__(self) -> "MetricsContextManager":
        self.metrics = self.integration.on_run_start(self.run_id, self.scraper_name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._completed:
            status = "failed" if exc_type else "completed"
            self.metrics = self.integration.on_run_complete(self.run_id, status)
            self._completed = True
        return False  # Don't suppress exceptions
    
    def pause(self) -> Optional[RunMetrics]:
        """Pause tracking."""
        return self.integration.on_run_pause(self.run_id)
    
    def resume(self) -> Optional[RunMetrics]:
        """Resume tracking."""
        return self.integration.on_run_resume(self.run_id)
    
    def complete(self, status: str = "completed") -> Optional[RunMetrics]:
        """Manually complete tracking."""
        self._completed = True
        return self.integration.on_run_complete(self.run_id, status)


def integrate_with_workflow_runner(runner_class: Any) -> Any:
    """
    Decorator/mixin to add metrics tracking to WorkflowRunner.
    
    This is a convenience function that can be used to patch WorkflowRunner
    with metrics tracking capabilities.
    
    Usage:
        from shared_workflow_runner import WorkflowRunner
        from core.progress.run_metrics_integration import integrate_with_workflow_runner
        
        MetricsTrackingRunner = integrate_with_workflow_runner(WorkflowRunner)
        runner = MetricsTrackingRunner("Malaysia", scraper_root, repo_root)
    """
    
    original_init = runner_class.__init__
    original_run = runner_class.run
    
    def new_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.metrics_integration = WorkflowMetricsIntegration()
    
    def new_run(self, scraper, progress_callback: Optional[Callable[[str], None]] = None):
        # Start metrics tracking
        if hasattr(self, 'run_id') and self.run_id:
            self.metrics_integration.on_run_start(self.run_id, self.scraper_name)
        
        try:
            result = original_run(self, scraper, progress_callback)
            
            # Complete metrics tracking
            if hasattr(self, 'run_id') and self.run_id:
                status = "completed" if result.get("status") == "ok" else "failed"
                self.metrics_integration.on_run_complete(self.run_id, status)
            
            return result
        except Exception as e:
            # Complete metrics tracking on exception
            if hasattr(self, 'run_id') and self.run_id:
                self.metrics_integration.on_run_complete(self.run_id, "failed")
            raise
    
    runner_class.__init__ = new_init
    runner_class.run = new_run
    
    # Add stop_pipeline integration
    original_stop_pipeline = runner_class.stop_pipeline
    
    @staticmethod
    def new_stop_pipeline(scraper_name: str, repo_root: Path = None):
        # Get the current run_id if available
        run_id = None
        try:
            if _RUN_LEDGER_AVAILABLE:
                ledger = FileRunLedger()
                runs = ledger.list_runs(limit=1, scraper_name=scraper_name)
                if runs and runs[0].status.value == "running":
                    run_id = runs[0].run_id
        except Exception:
            pass
        
        # Pause metrics tracking before stopping
        if run_id and _METRICS_AVAILABLE:
            try:
                integration = WorkflowMetricsIntegration()
                integration.on_run_pause(run_id)
            except Exception:
                pass
        
        return original_stop_pipeline(scraper_name, repo_root)
    
    runner_class.stop_pipeline = new_stop_pipeline
    
    return runner_class


# Convenience functions

def get_run_metrics_summary(run_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a summary of metrics for a run.
    
    Args:
        run_id: Run identifier
        
    Returns:
        Dictionary with metrics summary or None if not found
    """
    if not _METRICS_AVAILABLE:
        return None
    
    try:
        tracker = get_metrics_tracker()
        metrics = tracker.get_metrics(run_id)
        
        if metrics:
            return {
                "run_id": metrics.run_id,
                "scraper_name": metrics.scraper_name,
                "status": metrics.status,
                "active_duration_seconds": metrics.active_duration_seconds,
                "active_duration_minutes": round(metrics.active_duration_seconds / 60, 2),
                "network_total_gb": round(metrics.network_total_gb, 4),
                "network_sent_mb": round(metrics.network_sent_mb, 2),
                "network_received_mb": round(metrics.network_received_mb, 2),
                "started_at": metrics.started_at,
                "ended_at": metrics.ended_at,
            }
    except Exception as e:
        log.warning(f"Failed to get run metrics summary: {e}")
    
    return None


def list_run_metrics(
    scraper_name: Optional[str] = None,
    limit: int = 100,
) -> list:
    """
    List metrics for runs.
    
    Args:
        scraper_name: Optional scraper name filter
        limit: Maximum number of results
        
    Returns:
        List of metrics dictionaries
    """
    if not _METRICS_AVAILABLE:
        return []
    
    try:
        tracker = get_metrics_tracker()
        metrics_list = tracker.list_metrics(scraper_name=scraper_name, limit=limit)
        
        return [
            {
                "run_id": m.run_id,
                "scraper_name": m.scraper_name,
                "status": m.status,
                "active_duration_seconds": m.active_duration_seconds,
                "active_duration_minutes": round(m.active_duration_seconds / 60, 2),
                "network_total_gb": round(m.network_total_gb, 4),
                "network_sent_mb": round(m.network_sent_mb, 2),
                "network_received_mb": round(m.network_received_mb, 2),
                "started_at": m.started_at,
                "ended_at": m.ended_at,
            }
            for m in metrics_list
        ]
    except Exception as e:
        log.warning(f"Failed to list run metrics: {e}")
        return []


def print_run_metrics(run_id: str) -> None:
    """Print metrics for a run in a readable format."""
    if not _METRICS_AVAILABLE:
        print("Metrics tracking not available")
        return
    
    try:
        tracker = get_metrics_tracker()
        metrics = tracker.get_metrics(run_id)
        
        if metrics:
            print(format_metrics_summary(metrics))
        else:
            print(f"No metrics found for run {run_id}")
    except Exception as e:
        print(f"Failed to get metrics: {e}")


if __name__ == "__main__":
    # Test the integration
    import sys
    import time
    
    print("Run Metrics Integration Test")
    print("=" * 60)
    
    integration = WorkflowMetricsIntegration()
    
    if not integration.is_available():
        print("Metrics tracking not available!")
        sys.exit(1)
    
    test_run_id = f"integration_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"\n1. Testing start_run")
    metrics = integration.on_run_start(test_run_id, "TestScraper")
    print(f"   Started: {metrics is not None}")
    
    print(f"\n2. Simulating work (2 seconds)...")
    time.sleep(2)
    
    print(f"\n3. Testing pause")
    metrics = integration.on_run_pause(test_run_id)
    print(f"   Duration: {metrics.active_duration_seconds:.2f}s")
    
    print(f"\n4. Simulating idle (1 second)...")
    time.sleep(1)
    
    print(f"\n5. Testing resume")
    metrics = integration.on_run_resume(test_run_id)
    print(f"   Resumed: {metrics is not None}")
    
    print(f"\n6. Simulating more work (1 second)...")
    time.sleep(1)
    
    print(f"\n7. Testing complete")
    final_metrics = integration.on_run_complete(test_run_id, "completed")
    print(f"   Final Duration: {final_metrics.active_duration_seconds:.2f}s")
    print(f"   Final Network: {final_metrics.network_total_gb:.6f} GB")
    
    print("\n" + "=" * 60)
    print("Test completed successfully!")
    
    # Print final summary
    print("\nFinal Metrics:")
    print_run_metrics(test_run_id)
