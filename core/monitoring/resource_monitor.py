#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Resource Monitor and Leak Prevention

Monitors and prevents resource leaks:
1. Database connection pool monitoring
2. Browser instance tracking
3. File handle monitoring
4. Memory leak detection
5. Resource lock detection
"""

import gc
import os
import sys
import time
import threading
import logging
from typing import Dict, Set, List, Optional, Any
from collections import deque

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)

# Global monitoring
_monitoring_enabled = True
_check_interval = 300.0  # Check every 5 minutes
_last_check_time = 0.0
_memory_history = deque(maxlen=50)
_monitor_lock = threading.Lock()


def check_memory_leak() -> Dict[str, Any]:
    """
    Check for memory leaks by analyzing memory trend.
    
    Returns:
        Dict with leak detection results
    """
    if not PSUTIL_AVAILABLE:
        return {"leak_detected": False, "reason": "psutil not available"}
    
    try:
        process = psutil.Process(os.getpid())
        current_mb = process.memory_info().rss / 1024 / 1024
        
        with _monitor_lock:
            _memory_history.append((time.time(), current_mb))
            
            if len(_memory_history) < 10:
                return {
                    "leak_detected": False,
                    "current_mb": current_mb,
                    "reason": "insufficient data"
                }
            
            # Analyze trend
            recent = list(_memory_history)[-10:]
            times = [r[0] for r in recent]
            mems = [r[1] for r in recent]
            
            time_diff = times[-1] - times[0]
            mem_diff = mems[-1] - mems[0]
            
            if time_diff > 0:
                growth_rate = (mem_diff / time_diff) * 60.0  # MB per minute
            else:
                growth_rate = 0.0
            
            # Detect leak: consistent growth > 10MB/min over 10+ minutes
            leak_detected = False
            if len(recent) >= 10 and growth_rate > 10.0:
                # Check if consistently increasing
                increasing = sum(1 for i in range(1, len(recent)) if mems[i] > mems[i-1])
                if increasing >= len(recent) * 0.8:  # 80% of readings increasing
                    leak_detected = True
            
            return {
                "leak_detected": leak_detected,
                "current_mb": current_mb,
                "growth_rate_mb_per_min": growth_rate,
                "trend": "increasing" if growth_rate > 5.0 else ("decreasing" if growth_rate < -5.0 else "stable")
            }
    except Exception as e:
        logger.debug(f"Memory check failed: {e}")
        return {"leak_detected": False, "reason": str(e)}


def check_database_connections() -> Dict[str, Any]:
    """
    Check database connection pool status.
    
    Returns:
        Dict with connection pool info
    """
    try:
        from core.db.postgres_connection import _connection_pool, _pool_lock
        
        with _pool_lock:
            if _connection_pool is None:
                return {"status": "no_pool", "active": 0, "idle": 0}
            
            # Try to get pool stats (if available)
            try:
                # psycopg2.pool doesn't expose stats directly, so we estimate
                return {
                    "status": "active",
                    "minconn": getattr(_connection_pool, "_minconn", 0),
                    "maxconn": getattr(_connection_pool, "_maxconn", 0),
                }
            except Exception:
                return {"status": "active", "details": "stats unavailable"}
    except Exception as e:
        logger.debug(f"DB connection check failed: {e}")
        return {"status": "error", "error": str(e)}


def check_browser_processes(scraper_name: str) -> Dict[str, Any]:
    """
    Check for orphaned browser processes.
    
    Args:
        scraper_name: Name of scraper to check
    
    Returns:
        Dict with browser process info
    """
    if not PSUTIL_AVAILABLE:
        return {"browsers": 0, "error": "psutil not available"}
    
    try:
        browser_count = 0
        chromedriver_count = 0
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                name = (proc.info.get('name') or '').lower()
                cmdline = ' '.join(proc.info.get('cmdline') or [])
                
                if 'chrome' in name and 'chromedriver' not in name:
                    # Check if it's an automation instance
                    if any(flag in cmdline for flag in ['--remote-debugging-port', '--test-type', '--user-data-dir']):
                        browser_count += 1
                elif 'chromedriver' in name or 'geckodriver' in name:
                    chromedriver_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        return {
            "browsers": browser_count,
            "drivers": chromedriver_count,
            "total": browser_count + chromedriver_count
        }
    except Exception as e:
        logger.debug(f"Browser check failed: {e}")
        return {"browsers": 0, "error": str(e)}


def check_file_handles() -> Dict[str, Any]:
    """
    Check number of open file handles.
    
    Returns:
        Dict with file handle info
    """
    if not PSUTIL_AVAILABLE:
        return {"handles": 0, "error": "psutil not available"}
    
    try:
        process = psutil.Process(os.getpid())
        handles = process.num_fds() if hasattr(process, 'num_fds') else len(process.open_files())
        
        return {
            "handles": handles,
            "warning": handles > 1000  # Warn if > 1000 handles
        }
    except Exception as e:
        logger.debug(f"File handle check failed: {e}")
        return {"handles": 0, "error": str(e)}


def periodic_resource_check(scraper_name: str, force: bool = False) -> Dict[str, Any]:
    """
    Perform periodic resource check and cleanup.
    
    Args:
        scraper_name: Name of scraper
        force: If True, force check even if interval not reached
    
    Returns:
        Dict with resource status and warnings
    """
    global _last_check_time
    
    current_time = time.time()
    if not force and (current_time - _last_check_time) < _check_interval:
        return {"skipped": True, "reason": "interval_not_reached"}
    
    _last_check_time = current_time
    
    warnings = []
    
    # Check memory
    memory_check = check_memory_leak()
    if memory_check.get("leak_detected"):
        warnings.append(
            f"Memory leak detected: {memory_check.get('current_mb', 0):.1f}MB, "
            f"growing at {memory_check.get('growth_rate_mb_per_min', 0):.1f}MB/min"
        )
    
    # Check database connections
    db_check = check_database_connections()
    
    # Check browser processes
    browser_check = check_browser_processes(scraper_name)
    if browser_check.get("total", 0) > 10:
        warnings.append(
            f"High number of browser processes: {browser_check.get('total', 0)} "
            f"(browsers: {browser_check.get('browsers', 0)}, drivers: {browser_check.get('drivers', 0)})"
        )
    
    # Check file handles
    handle_check = check_file_handles()
    if handle_check.get("warning"):
        warnings.append(
            f"High number of file handles: {handle_check.get('handles', 0)}"
        )
    
    # Force garbage collection if memory is high
    gc_count = 0
    if memory_check.get("current_mb", 0) > 2000:  # > 2GB
        gc_count = gc.collect()
        logger.info(f"[RESOURCE] Forced GC collected {gc_count} objects")
    
    return {
        "memory": memory_check,
        "database": db_check,
        "browsers": browser_check,
        "file_handles": handle_check,
        "warnings": warnings,
        "gc_collected": gc_count,
        "timestamp": current_time
    }


def log_resource_status(scraper_name: str, prefix: str = "") -> None:
    """Log current resource status."""
    status = periodic_resource_check(scraper_name, force=True)
    
    mem = status.get("memory", {})
    mem_str = f"{mem.get('current_mb', 0):.1f}MB"
    if mem.get("growth_rate_mb_per_min", 0) > 5.0:
        mem_str += f" (+{mem.get('growth_rate_mb_per_min', 0):.1f}MB/min)"
    
    browsers = status.get("browsers", {})
    browser_str = f"{browsers.get('total', 0)} processes"
    
    handles = status.get("file_handles", {})
    handles_str = f"{handles.get('handles', 0)} handles"
    
    logger.info(
        f"{prefix}[RESOURCES] Memory: {mem_str} | "
        f"Browsers: {browser_str} | File handles: {handles_str}"
    )
    
    if status.get("warnings"):
        for warning in status["warnings"]:
            logger.warning(f"{prefix}[RESOURCE WARNING] {warning}")
