#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run Metrics Tracker

Tracks network consumption and active execution time for each scraper run.
Only counts time and network usage when the script is actively running
(excludes paused/idle periods).

Stores one row per run_id with:
- run_id: Unique identifier for the run
- scraper_name: Name of the scraper
- active_duration_seconds: Actual running time (excluding pauses)
- network_sent_bytes: Total bytes sent during active execution
- network_received_bytes: Total bytes received during active execution
- network_total_gb: Total network usage in GB (sent + received)
- started_at: When the run first started
- ended_at: When the run completed
- status: final status of the run
"""

from __future__ import annotations

import json
import logging
import time
import psutil
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
from threading import Lock

try:
    from core.config.config_manager import ConfigManager
except Exception:
    ConfigManager = None

log = logging.getLogger(__name__)


class RunSessionStatus(str, Enum):
    """Status of a run session (active tracking period)."""
    ACTIVE = "active"      # Currently tracking
    PAUSED = "paused"      # Temporarily paused
    STOPPED = "stopped"    # Stopped, can be resumed
    COMPLETED = "completed"  # Finished successfully


@dataclass
class RunMetrics:
    """Metrics for a single run."""
    run_id: str
    scraper_name: str
    started_at: str
    ended_at: Optional[str] = None
    active_duration_seconds: float = 0.0
    network_sent_bytes: int = 0
    network_received_bytes: int = 0
    status: str = "running"
    last_updated: str = ""
    
    @property
    def network_total_gb(self) -> float:
        """Total network usage in GB."""
        total_bytes = self.network_sent_bytes + self.network_received_bytes
        return total_bytes / (1024 ** 3)  # Convert to GB
    
    @property
    def network_sent_mb(self) -> float:
        """Sent data in MB."""
        return self.network_sent_bytes / (1024 ** 2)
    
    @property
    def network_received_mb(self) -> float:
        """Received data in MB."""
        return self.network_received_bytes / (1024 ** 2)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "run_id": self.run_id,
            "scraper_name": self.scraper_name,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "active_duration_seconds": self.active_duration_seconds,
            "network_sent_bytes": self.network_sent_bytes,
            "network_received_bytes": self.network_received_bytes,
            "network_total_gb": round(self.network_total_gb, 6),
            "network_sent_mb": round(self.network_sent_mb, 2),
            "network_received_mb": round(self.network_received_mb, 2),
            "status": self.status,
            "last_updated": self.last_updated or self._now_iso(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunMetrics":
        """Create from dictionary."""
        return cls(
            run_id=data["run_id"],
            scraper_name=data["scraper_name"],
            started_at=data["started_at"],
            ended_at=data.get("ended_at"),
            active_duration_seconds=data.get("active_duration_seconds", 0.0),
            network_sent_bytes=data.get("network_sent_bytes", 0),
            network_received_bytes=data.get("network_received_bytes", 0),
            status=data.get("status", "running"),
            last_updated=data.get("last_updated", ""),
        )
    
    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()


class RunMetricsTracker:
    """
    Tracks network consumption and active execution time for scraper runs.
    
    Features:
    - Tracks only active execution time (excludes paused periods)
    - Tracks network I/O during active periods only
    - Supports pause/resume functionality
    - Stores one row per run_id
    - Thread-safe for concurrent access
    
    Usage:
        tracker = RunMetricsTracker()
        
        # Start tracking a new run
        tracker.start_run("run_001", "Malaysia")
        
        # ... scraper executes ...
        
        # Pause tracking (e.g., when user pauses)
        tracker.pause_run("run_001")
        
        # Resume tracking
        tracker.resume_run("run_001")
        
        # ... scraper continues ...
        
        # Complete the run
        metrics = tracker.complete_run("run_001")
        print(f"Duration: {metrics.active_duration_seconds}s")
        print(f"Network: {metrics.network_total_gb:.4f} GB")
    """
    
    def __init__(self, metrics_dir: Optional[Path] = None) -> None:
        self.metrics_dir = self._resolve_metrics_dir(metrics_dir)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        
        # In-memory tracking for active runs
        self._active_runs: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()
        
        # Get initial network stats for the process
        self._process = psutil.Process()
        self._initial_io = self._get_net_io()
    
    def _resolve_metrics_dir(self, metrics_dir: Optional[Path]) -> Path:
        """Resolve the metrics directory."""
        if metrics_dir:
            return Path(metrics_dir)
        if ConfigManager:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_cache_dir() / "run_metrics"
        return Path.cwd() / "cache" / "run_metrics"
    
    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()
    
    def _get_metrics_path(self, run_id: str) -> Path:
        """Get the file path for a run's metrics."""
        return self.metrics_dir / f"{run_id}.json"
    
    def _get_net_io(self) -> psutil._psplatform.NetIOCounters:
        """Get current network I/O counters for the process."""
        try:
            # Try to get process-specific IO (may not be available on all platforms)
            io = self._process.io_counters()
            # io_counters returns different fields on different platforms
            # On Windows: read_bytes, write_bytes
            # On Linux: read_bytes, write_bytes, read_chars, write_chars
            return io
        except (AttributeError, psutil.AccessDenied):
            # Fallback to system-wide network stats
            return psutil.net_io_counters()
    
    def _get_net_bytes(self) -> tuple:
        """Get current sent and received bytes."""
        io = self._get_net_io()
        # Handle both process IO and system net_io_counters
        if hasattr(io, 'write_bytes') and hasattr(io, 'read_bytes'):
            # Process IO counters
            return io.write_bytes, io.read_bytes
        elif hasattr(io, 'bytes_sent') and hasattr(io, 'bytes_recv'):
            # System net_io_counters
            return io.bytes_sent, io.bytes_recv
        return 0, 0
    
    def start_run(self, run_id: str, scraper_name: str) -> RunMetrics:
        """
        Start tracking metrics for a new run.
        
        Args:
            run_id: Unique identifier for the run
            scraper_name: Name of the scraper
            
        Returns:
            RunMetrics object with initial values
        """
        with self._lock:
            started_at = self._now_iso()
            
            # Initialize metrics
            metrics = RunMetrics(
                run_id=run_id,
                scraper_name=scraper_name,
                started_at=started_at,
                last_updated=started_at,
            )
            
            # Get initial network stats
            sent, received = self._get_net_bytes()
            
            # Store in-memory tracking info
            self._active_runs[run_id] = {
                "metrics": metrics,
                "session_start_time": time.monotonic(),
                "last_net_sent": sent,
                "last_net_received": received,
                "status": RunSessionStatus.ACTIVE,
            }
            
            # Save to disk
            self._save_metrics(metrics)
            
            log.info(f"Started metrics tracking for run {run_id}")
            return metrics
    
    def pause_run(self, run_id: str) -> Optional[RunMetrics]:
        """
        Pause tracking for a run (e.g., when user pauses the scraper).
        Accumulates time and network usage up to this point.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Updated RunMetrics or None if run not found
        """
        with self._lock:
            if run_id not in self._active_runs:
                log.warning(f"Cannot pause: run {run_id} not found")
                return None
            
            run_info = self._active_runs[run_id]
            if run_info["status"] != RunSessionStatus.ACTIVE:
                log.warning(f"Cannot pause: run {run_id} is not active")
                return run_info["metrics"]
            
            metrics = run_info["metrics"]
            
            # Calculate elapsed time for this session
            session_start = run_info["session_start_time"]
            elapsed = time.monotonic() - session_start
            metrics.active_duration_seconds += elapsed
            
            # Calculate network usage for this session
            current_sent, current_received = self._get_net_bytes()
            session_sent = max(0, current_sent - run_info["last_net_sent"])
            session_received = max(0, current_received - run_info["last_net_received"])
            metrics.network_sent_bytes += session_sent
            metrics.network_received_bytes += session_received
            
            # Update status
            run_info["status"] = RunSessionStatus.PAUSED
            metrics.last_updated = self._now_iso()
            
            # Save to disk
            self._save_metrics(metrics)
            
            log.info(f"Paused metrics tracking for run {run_id}. "
                    f"Session: {elapsed:.2f}s, "
                    f"Network: {(session_sent + session_received) / (1024**2):.2f} MB")
            
            return metrics
    
    def resume_run(self, run_id: str) -> Optional[RunMetrics]:
        """
        Resume tracking for a paused run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Updated RunMetrics or None if run not found
        """
        with self._lock:
            if run_id not in self._active_runs:
                log.warning(f"Cannot resume: run {run_id} not found")
                return None
            
            run_info = self._active_runs[run_id]
            if run_info["status"] != RunSessionStatus.PAUSED:
                log.warning(f"Cannot resume: run {run_id} is not paused")
                return run_info["metrics"]
            
            # Get fresh network stats for new session
            sent, received = self._get_net_bytes()
            
            # Update tracking info
            run_info["session_start_time"] = time.monotonic()
            run_info["last_net_sent"] = sent
            run_info["last_net_received"] = received
            run_info["status"] = RunSessionStatus.ACTIVE
            
            metrics = run_info["metrics"]
            metrics.last_updated = self._now_iso()
            
            log.info(f"Resumed metrics tracking for run {run_id}")
            return metrics
    
    def stop_run(self, run_id: str) -> Optional[RunMetrics]:
        """
        Stop tracking for a run (e.g., when user stops but may resume later).
        Similar to pause but marks as stopped.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Updated RunMetrics or None if run not found
        """
        with self._lock:
            if run_id not in self._active_runs:
                log.warning(f"Cannot stop: run {run_id} not found")
                return None
            
            run_info = self._active_runs[run_id]
            if run_info["status"] != RunSessionStatus.ACTIVE:
                log.warning(f"Cannot stop: run {run_id} is not active")
                return run_info["metrics"]
            
            metrics = run_info["metrics"]
            
            # Calculate elapsed time for this session
            session_start = run_info["session_start_time"]
            elapsed = time.monotonic() - session_start
            metrics.active_duration_seconds += elapsed
            
            # Calculate network usage for this session
            current_sent, current_received = self._get_net_bytes()
            session_sent = max(0, current_sent - run_info["last_net_sent"])
            session_received = max(0, current_received - run_info["last_net_received"])
            metrics.network_sent_bytes += session_sent
            metrics.network_received_bytes += session_received
            
            # Update status
            run_info["status"] = RunSessionStatus.STOPPED
            metrics.status = "stopped"
            metrics.last_updated = self._now_iso()
            
            # Save to disk
            self._save_metrics(metrics)
            
            log.info(f"Stopped metrics tracking for run {run_id}")
            return metrics
    
    def complete_run(self, run_id: str, final_status: str = "completed") -> Optional[RunMetrics]:
        """
        Complete tracking for a run and finalize metrics.
        
        Args:
            run_id: Run identifier
            final_status: Final status (completed, failed, cancelled)
            
        Returns:
            Final RunMetrics or None if run not found
        """
        with self._lock:
            if run_id not in self._active_runs:
                # Try to load from disk
                metrics = self._load_metrics(run_id)
                if metrics:
                    metrics.status = final_status
                    metrics.ended_at = self._now_iso()
                    self._save_metrics(metrics)
                return metrics
            
            run_info = self._active_runs[run_id]
            metrics = run_info["metrics"]
            
            # If still active, accumulate current session
            if run_info["status"] == RunSessionStatus.ACTIVE:
                session_start = run_info["session_start_time"]
                elapsed = time.monotonic() - session_start
                metrics.active_duration_seconds += elapsed
                
                current_sent, current_received = self._get_net_bytes()
                session_sent = max(0, current_sent - run_info["last_net_sent"])
                session_received = max(0, current_received - run_info["last_net_received"])
                metrics.network_sent_bytes += session_sent
                metrics.network_received_bytes += session_received
            
            # Finalize
            metrics.status = final_status
            metrics.ended_at = self._now_iso()
            metrics.last_updated = self._now_iso()
            
            # Save to disk
            self._save_metrics(metrics)
            
            # Remove from active runs
            del self._active_runs[run_id]
            
            log.info(f"Completed metrics tracking for run {run_id}. "
                    f"Total duration: {metrics.active_duration_seconds:.2f}s, "
                    f"Total network: {metrics.network_total_gb:.4f} GB")
            
            return metrics
    
    def get_current_metrics(self, run_id: str) -> Optional[RunMetrics]:
        """
        Get current metrics for a run (including active session if running).
        
        Args:
            run_id: Run identifier
            
        Returns:
            Current RunMetrics or None if not found
        """
        with self._lock:
            if run_id in self._active_runs:
                run_info = self._active_runs[run_id]
                metrics = run_info["metrics"]
                
                # If active, include current session
                if run_info["status"] == RunSessionStatus.ACTIVE:
                    # Create a copy with current session included
                    current_metrics = RunMetrics.from_dict(metrics.to_dict())
                    
                    session_start = run_info["session_start_time"]
                    elapsed = time.monotonic() - session_start
                    current_metrics.active_duration_seconds += elapsed
                    
                    current_sent, current_received = self._get_net_bytes()
                    session_sent = max(0, current_sent - run_info["last_net_sent"])
                    session_received = max(0, current_received - run_info["last_net_received"])
                    current_metrics.network_sent_bytes += session_sent
                    current_metrics.network_received_bytes += session_received
                    
                    return current_metrics
                
                return metrics
            
            # Try to load from disk
            return self._load_metrics(run_id)
    
    def _save_metrics(self, metrics: RunMetrics) -> None:
        """Save metrics to disk."""
        try:
            metrics_path = self._get_metrics_path(metrics.run_id)
            temp_path = metrics_path.with_suffix(".tmp")
            
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(metrics.to_dict(), f, indent=2)
            
            temp_path.replace(metrics_path)
        except Exception as exc:
            log.warning(f"Failed to save metrics for {metrics.run_id}: {exc}")
    
    def _load_metrics(self, run_id: str) -> Optional[RunMetrics]:
        """Load metrics from disk."""
        try:
            metrics_path = self._get_metrics_path(run_id)
            if not metrics_path.exists():
                return None
            
            with open(metrics_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            return RunMetrics.from_dict(data)
        except Exception as exc:
            log.warning(f"Failed to load metrics for {run_id}: {exc}")
            return None
    
    def get_metrics(self, run_id: str) -> Optional[RunMetrics]:
        """Get metrics for a run (from disk)."""
        return self._load_metrics(run_id)
    
    def list_metrics(
        self,
        scraper_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[RunMetrics]:
        """
        List all tracked metrics, optionally filtered by scraper.
        
        Args:
            scraper_name: Optional scraper name filter
            limit: Maximum number of results
            
        Returns:
            List of RunMetrics objects
        """
        results = []
        
        try:
            for metrics_file in sorted(self.metrics_dir.glob("*.json"), reverse=True):
                try:
                    with open(metrics_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    
                    if scraper_name and data.get("scraper_name") != scraper_name:
                        continue
                    
                    results.append(RunMetrics.from_dict(data))
                    
                    if len(results) >= limit:
                        break
                except Exception:
                    continue
        except Exception as exc:
            log.warning(f"Failed to list metrics: {exc}")
        
        return results
    
    def get_summary(self, scraper_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get summary statistics for runs.
        
        Args:
            scraper_name: Optional scraper name filter
            
        Returns:
            Dictionary with summary statistics
        """
        metrics_list = self.list_metrics(scraper_name=scraper_name, limit=10000)
        
        if not metrics_list:
            return {
                "total_runs": 0,
                "total_duration_seconds": 0.0,
                "total_duration_hours": 0.0,
                "total_network_gb": 0.0,
                "avg_duration_seconds": 0.0,
                "avg_network_gb": 0.0,
            }
        
        total_duration = sum(m.active_duration_seconds for m in metrics_list)
        total_network_gb = sum(m.network_total_gb for m in metrics_list)
        
        return {
            "total_runs": len(metrics_list),
            "total_duration_seconds": round(total_duration, 2),
            "total_duration_hours": round(total_duration / 3600, 2),
            "total_network_gb": round(total_network_gb, 4),
            "avg_duration_seconds": round(total_duration / len(metrics_list), 2),
            "avg_network_gb": round(total_network_gb / len(metrics_list), 4),
        }
    
    def delete_metrics(self, run_id: str) -> bool:
        """
        Delete metrics for a run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            True if deleted, False otherwise
        """
        try:
            # Remove from active runs if present
            with self._lock:
                if run_id in self._active_runs:
                    del self._active_runs[run_id]
            
            # Delete file
            metrics_path = self._get_metrics_path(run_id)
            if metrics_path.exists():
                metrics_path.unlink()
                return True
            return False
        except Exception as exc:
            log.warning(f"Failed to delete metrics for {run_id}: {exc}")
            return False


# Singleton instance for global access
_tracker_instance: Optional[RunMetricsTracker] = None
_tracker_lock = Lock()


def get_metrics_tracker() -> RunMetricsTracker:
    """Get the global metrics tracker instance."""
    global _tracker_instance
    with _tracker_lock:
        if _tracker_instance is None:
            _tracker_instance = RunMetricsTracker()
        return _tracker_instance


def reset_metrics_tracker() -> None:
    """Reset the global metrics tracker instance (mainly for testing)."""
    global _tracker_instance
    with _tracker_lock:
        _tracker_instance = None


# Convenience functions for common operations

def start_run_tracking(run_id: str, scraper_name: str) -> RunMetrics:
    """Start tracking metrics for a run."""
    return get_metrics_tracker().start_run(run_id, scraper_name)


def pause_run_tracking(run_id: str) -> Optional[RunMetrics]:
    """Pause tracking for a run."""
    return get_metrics_tracker().pause_run(run_id)


def resume_run_tracking(run_id: str) -> Optional[RunMetrics]:
    """Resume tracking for a run."""
    return get_metrics_tracker().resume_run(run_id)


def stop_run_tracking(run_id: str) -> Optional[RunMetrics]:
    """Stop tracking for a run."""
    return get_metrics_tracker().stop_run(run_id)


def complete_run_tracking(run_id: str, final_status: str = "completed") -> Optional[RunMetrics]:
    """Complete tracking for a run."""
    return get_metrics_tracker().complete_run(run_id, final_status)


def get_run_metrics(run_id: str) -> Optional[RunMetrics]:
    """Get metrics for a run."""
    return get_metrics_tracker().get_current_metrics(run_id)


def format_metrics_summary(metrics: RunMetrics) -> str:
    """Format metrics as a readable summary."""
    duration_mins = metrics.active_duration_seconds / 60
    # Calculate cost (approx $5 per GB)
    COST_PER_GB = 5.0
    estimated_cost = metrics.network_total_gb * COST_PER_GB
    return (
        f"Run: {metrics.run_id}\n"
        f"Scraper: {metrics.scraper_name}\n"
        f"Status: {metrics.status}\n"
        f"Active Duration: {metrics.active_duration_seconds:.1f}s ({duration_mins:.2f} min)\n"
        f"Network Usage: {metrics.network_total_gb:.4f} GB\n"
        f"  - Sent: {metrics.network_sent_mb:.2f} MB\n"
        f"  - Received: {metrics.network_received_mb:.2f} MB\n"
        f"Estimated Cost: ${estimated_cost:.2f} (at ${COST_PER_GB}/GB)\n"
        f"Started: {metrics.started_at}\n"
        f"Ended: {metrics.ended_at or 'N/A'}"
    )


if __name__ == "__main__":
    # Test the module
    import sys
    
    print("Run Metrics Tracker Test")
    print("=" * 60)
    
    tracker = RunMetricsTracker()
    
    # Test run
    test_run_id = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"\n1. Starting run: {test_run_id}")
    metrics = tracker.start_run(test_run_id, "TestScraper")
    print(f"   Started at: {metrics.started_at}")
    
    # Simulate some work
    print("\n2. Simulating work (3 seconds)...")
    time.sleep(3)
    
    # Pause
    print("\n3. Pausing run...")
    metrics = tracker.pause_run(test_run_id)
    print(f"   Duration so far: {metrics.active_duration_seconds:.2f}s")
    print(f"   Network so far: {metrics.network_total_gb:.6f} GB")
    
    # Simulate idle time (should not count)
    print("\n4. Simulating idle time (2 seconds)...")
    time.sleep(2)
    
    # Resume
    print("\n5. Resuming run...")
    tracker.resume_run(test_run_id)
    
    # More work
    print("\n6. Simulating more work (2 seconds)...")
    time.sleep(2)
    
    # Complete
    print("\n7. Completing run...")
    final_metrics = tracker.complete_run(test_run_id, "completed")
    
    print("\n" + "=" * 60)
    print("FINAL METRICS:")
    print("=" * 60)
    print(format_metrics_summary(final_metrics))
    
    # List all metrics
    print("\n" + "=" * 60)
    print("ALL METRICS:")
    print("=" * 60)
    all_metrics = tracker.list_metrics()
    for m in all_metrics[:5]:
        print(f"\n{m.run_id}: {m.active_duration_seconds:.1f}s, {m.network_total_gb:.4f} GB")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print("=" * 60)
    summary = tracker.get_summary()
    for key, value in summary.items():
        print(f"  {key}: {value}")
