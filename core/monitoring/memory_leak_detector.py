#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Memory Leak Detector and Resource Monitor

Detects and prevents memory leaks by:
1. Monitoring memory usage over time
2. Detecting unbounded data structure growth
3. Tracking resource usage (connections, file handles, browser instances)
4. Providing cleanup utilities
"""

import gc
import os
import sys
import time
import threading
import logging
from typing import Dict, Set, List, Optional, Any
from collections import deque
from pathlib import Path

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)

# Global memory tracking
_memory_history = deque(maxlen=100)  # Track last 100 memory readings
_memory_lock = threading.Lock()
_last_gc_time = 0.0
_gc_interval = 300.0  # Force GC every 5 minutes

# Resource tracking
_tracked_sets: Dict[str, Set] = {}
_tracked_lists: Dict[str, List] = {}
_tracked_resources: Dict[str, Any] = {}
_resource_lock = threading.Lock()


def get_memory_usage_mb() -> float:
    """Get current process memory usage in MB."""
    if not PSUTIL_AVAILABLE:
        return 0.0
    try:
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    except Exception:
        return 0.0


def get_memory_trend() -> Dict[str, Any]:
    """
    Analyze memory usage trend to detect leaks.
    
    Returns:
        Dict with:
        - current_mb: Current memory usage
        - trend: 'increasing', 'stable', 'decreasing'
        - growth_rate_mb_per_min: Estimated growth rate
        - leak_detected: True if leak pattern detected
    """
    with _memory_lock:
        if len(_memory_history) < 10:
            return {
                "current_mb": get_memory_usage_mb(),
                "trend": "unknown",
                "growth_rate_mb_per_min": 0.0,
                "leak_detected": False
            }
        
        current_mb = get_memory_usage_mb()
        _memory_history.append((time.time(), current_mb))
        
        # Analyze trend over last 10 readings
        recent = list(_memory_history)[-10:]
        if len(recent) < 2:
            return {
                "current_mb": current_mb,
                "trend": "unknown",
                "growth_rate_mb_per_min": 0.0,
                "leak_detected": False
            }
        
        times = [r[0] for r in recent]
        mems = [r[1] for r in recent]
        
        # Calculate growth rate
        time_diff = times[-1] - times[0]
        mem_diff = mems[-1] - mems[0]
        
        if time_diff > 0:
            growth_rate = (mem_diff / time_diff) * 60.0  # MB per minute
        else:
            growth_rate = 0.0
        
        # Detect leak: consistent growth over time
        leak_detected = False
        if len(recent) >= 5:
            # Check if memory consistently increases
            increasing_count = sum(1 for i in range(1, len(recent)) if mems[i] > mems[i-1])
            if increasing_count >= len(recent) * 0.7 and growth_rate > 5.0:  # 70% increasing, >5MB/min
                leak_detected = True
        
        trend = "increasing" if growth_rate > 1.0 else ("decreasing" if growth_rate < -1.0 else "stable")
        
        return {
            "current_mb": current_mb,
            "trend": trend,
            "growth_rate_mb_per_min": growth_rate,
            "leak_detected": leak_detected
        }


def track_set(name: str, set_obj: Set, max_size: int = 100000) -> None:
    """
    Track a set for unbounded growth detection.
    
    Args:
        name: Name identifier for the set
        set_obj: The set to track
        max_size: Maximum expected size before warning
    """
    with _resource_lock:
        _tracked_sets[name] = {
            "set": set_obj,
            "max_size": max_size,
            "last_size": len(set_obj),
            "last_check": time.time()
        }


def track_list(name: str, list_obj: List, max_size: int = 100000) -> None:
    """
    Track a list for unbounded growth detection.
    
    Args:
        name: Name identifier for the list
        list_obj: The list to track
        max_size: Maximum expected size before warning
    """
    with _resource_lock:
        _tracked_lists[name] = {
            "list": list_obj,
            "max_size": max_size,
            "last_size": len(list_obj),
            "last_check": time.time()
        }


def check_tracked_resources() -> List[str]:
    """
    Check all tracked resources for unbounded growth.
    
    Returns:
        List of warning messages for resources exceeding limits
    """
    warnings = []
    current_time = time.time()
    
    with _resource_lock:
        # Check sets
        for name, info in _tracked_sets.items():
            set_obj = info["set"]
            current_size = len(set_obj)
            max_size = info["max_size"]
            
            if current_size > max_size:
                warnings.append(
                    f"Set '{name}' has {current_size} items (limit: {max_size}). "
                    f"Consider periodic clearing or using DB-backed deduplication."
                )
            
            # Check growth rate
            if info["last_check"] > 0:
                time_diff = current_time - info["last_check"]
                size_diff = current_size - info["last_size"]
                if time_diff > 60 and size_diff > 1000:  # >1000 items per minute
                    warnings.append(
                        f"Set '{name}' growing rapidly: +{size_diff} items in {time_diff:.1f}s"
                    )
            
            info["last_size"] = current_size
            info["last_check"] = current_time
        
        # Check lists
        for name, info in _tracked_lists.items():
            list_obj = info["list"]
            current_size = len(list_obj)
            max_size = info["max_size"]
            
            if current_size > max_size:
                warnings.append(
                    f"List '{name}' has {current_size} items (limit: {max_size}). "
                    f"Consider flushing to DB or clearing periodically."
                )
            
            # Check growth rate
            if info["last_check"] > 0:
                time_diff = current_time - info["last_check"]
                size_diff = current_size - info["last_size"]
                if time_diff > 60 and size_diff > 1000:  # >1000 items per minute
                    warnings.append(
                        f"List '{name}' growing rapidly: +{size_diff} items in {time_diff:.1f}s"
                    )
            
            info["last_size"] = current_size
            info["last_check"] = current_time
    
    return warnings


def periodic_cleanup(force_gc: bool = False) -> Dict[str, Any]:
    """
    Perform periodic cleanup and return resource usage report.
    
    Args:
        force_gc: If True, force garbage collection
    
    Returns:
        Dict with cleanup results and resource usage
    """
    global _last_gc_time
    
    current_time = time.time()
    should_gc = force_gc or (current_time - _last_gc_time) > _gc_interval
    
    # Update memory history
    mem_mb = get_memory_usage_mb()
    with _memory_lock:
        _memory_history.append((current_time, mem_mb))
    
    # Check tracked resources
    warnings = check_tracked_resources()
    
    # Force garbage collection if needed
    gc_count = 0
    if should_gc:
        gc_count = gc.collect()
        _last_gc_time = current_time
    
    # Get memory trend
    trend = get_memory_trend()
    
    return {
        "memory_mb": mem_mb,
        "memory_trend": trend,
        "gc_collected": gc_count,
        "warnings": warnings,
        "tracked_sets": len(_tracked_sets),
        "tracked_lists": len(_tracked_lists)
    }


def clear_tracked_set(name: str, keep_recent: int = 0) -> int:
    """
    Clear a tracked set, optionally keeping recent items.
    
    Args:
        name: Name of the set to clear
        keep_recent: Number of recent items to keep (0 = clear all)
    
    Returns:
        Number of items cleared
    """
    with _resource_lock:
        if name not in _tracked_sets:
            return 0
        
        set_obj = _tracked_sets[name]["set"]
        original_size = len(set_obj)
        
        if keep_recent > 0:
            # Keep only recent items (convert to list, keep last N)
            items_list = list(set_obj)
            set_obj.clear()
            set_obj.update(items_list[-keep_recent:])
        else:
            set_obj.clear()
        
        cleared = original_size - len(set_obj)
        _tracked_sets[name]["last_size"] = len(set_obj)
        _tracked_sets[name]["last_check"] = time.time()
        
        return cleared


def clear_tracked_list(name: str, keep_recent: int = 0) -> int:
    """
    Clear a tracked list, optionally keeping recent items.
    
    Args:
        name: Name of the list to clear
        keep_recent: Number of recent items to keep (0 = clear all)
    
    Returns:
        Number of items cleared
    """
    with _resource_lock:
        if name not in _tracked_lists:
            return 0
        
        list_obj = _tracked_lists[name]["list"]
        original_size = len(list_obj)
        
        if keep_recent > 0:
            list_obj[:] = list_obj[-keep_recent:]
        else:
            list_obj.clear()
        
        cleared = original_size - len(list_obj)
        _tracked_lists[name]["last_size"] = len(list_obj)
        _tracked_lists[name]["last_check"] = time.time()
        
        return cleared


def get_resource_report() -> Dict[str, Any]:
    """
    Get comprehensive resource usage report.
    
    Returns:
        Dict with memory, tracked resources, and warnings
    """
    cleanup_result = periodic_cleanup()
    trend = get_memory_trend()
    
    # Count tracked resources
    set_sizes = {}
    list_sizes = {}
    with _resource_lock:
        for name, info in _tracked_sets.items():
            set_sizes[name] = len(info["set"])
        for name, info in _tracked_lists.items():
            list_sizes[name] = len(info["list"])
    
    return {
        "memory": {
            "current_mb": trend["current_mb"],
            "trend": trend["trend"],
            "growth_rate_mb_per_min": trend["growth_rate_mb_per_min"],
            "leak_detected": trend["leak_detected"]
        },
        "tracked_sets": set_sizes,
        "tracked_lists": list_sizes,
        "warnings": cleanup_result["warnings"],
        "gc_collected": cleanup_result["gc_collected"]
    }


def log_resource_usage(prefix: str = "") -> None:
    """Log current resource usage."""
    report = get_resource_report()
    mem = report["memory"]
    
    log_msg = f"{prefix}[RESOURCES] Memory: {mem['current_mb']:.1f}MB"
    if mem["trend"] != "stable":
        log_msg += f" ({mem['trend']}, {mem['growth_rate_mb_per_min']:.1f}MB/min)"
    
    if report["tracked_sets"]:
        set_info = ", ".join(f"{k}={v}" for k, v in report["tracked_sets"].items())
        log_msg += f" | Sets: {set_info}"
    
    if report["tracked_lists"]:
        list_info = ", ".join(f"{k}={v}" for k, v in report["tracked_lists"].items())
        log_msg += f" | Lists: {list_info}"
    
    if report["warnings"]:
        log_msg += f" | WARNINGS: {len(report['warnings'])}"
    
    logger.info(log_msg)
    
    # Print warnings
    for warning in report["warnings"]:
        logger.warning(f"{prefix}[RESOURCE WARNING] {warning}")
