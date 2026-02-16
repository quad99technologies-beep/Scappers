"""
Shared utilities for Russia scraper scripts (Facade for Core).
Delegates to Core modules where possible.
"""

import os
import gc
import sys
import time
import signal
import atexit
import threading
import logging
import psutil
from datetime import datetime
from typing import Set, Optional

# Core Imports
from core.browser.chrome_manager import (
    register_chrome_driver as core_register,
    unregister_chrome_driver as core_unregister,
    cleanup_all_chrome_instances as core_cleanup_all,
    kill_orphaned_chrome_processes as core_kill_orphaned
)
from core.browser.human_behavior import jitter_sleep

# =============================================================================
# LOGGING SETUP
# =============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("russia_scraper")

# =============================================================================
# RESOURCE MONITORING
# =============================================================================

# Memory section kept local as it matches specific scraper logic
MEMORY_LIMIT_MB = 2048
MEMORY_CHECK_INTERVAL = 10

def get_resource_usage():
    try:
        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().rss / 1024 / 1024
        return mem_mb, threading.active_count()
    except Exception:
        return 0, threading.active_count()

def log_resource_usage(prefix="[RESOURCE]"):
    mem_mb, threads = get_resource_usage()
    log.info(f"{prefix} Memory: {mem_mb:.1f}MB, Threads: {threads}")

def get_memory_usage_mb() -> float:
    try:
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    except Exception:
        return 0.0

def check_memory_limit() -> bool:
    mem_mb = get_memory_usage_mb()
    if mem_mb > MEMORY_LIMIT_MB:
        log.warning(f"[MEMORY_LIMIT] Memory usage {mem_mb:.0f}MB exceeds limit {MEMORY_LIMIT_MB}MB")
        return True
    return False

def force_cleanup():
    gc.collect()
    for _ in range(3):
        gc.collect()

# =============================================================================
# CHROME CLEANUP (FACADE)
# =============================================================================

def register_driver(driver):
    core_register(driver)

def unregister_driver(driver):
    core_unregister(driver)

def kill_tracked_chrome_processes():
    # Core manager tracks its own PIDs now.
    # If the scraper uses ChromeInstanceTracker, that handles DB.
    # Using core_kill_orphaned as a safe fallback?
    # Or just returning 0 as core handles it via atexit.
    return 0 

def kill_all_chrome_processes():
    """Nuclear option."""
    # We can implement this using psutil locally or if core has it.
    # Core has cleanup_all_chrome_instances but that only kills registered.
    # Russia script wants system-wide kill.
    killed = 0
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if 'chrome' in (proc.info['name'] or '').lower():
                psutil.Process(proc.info['pid']).kill()
                killed += 1
        except Exception:
            pass
    return killed

def cleanup_orphaned_chrome_processes():
    return core_kill_orphaned()

def close_all_drivers():
    core_cleanup_all()

def track_chrome_pids(pids: Set[int]):
    # Deprecated/No-op: Core handles this via ChromeInstanceTracker if used.
    pass

# =============================================================================
# SIGNALS
# =============================================================================
_shutdown_requested = threading.Event()

def signal_handler(signum, frame):
    log.warning(f"[SHUTDOWN] Signal {signum} received...")
    _shutdown_requested.set()
    close_all_drivers()
    sys.exit(0)

try:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
except Exception:
    pass

atexit.register(close_all_drivers)

# =============================================================================
# PROGRESS TRACKING (kept local)
# =============================================================================
_progress_lock = threading.Lock()
_products_completed = 0
_total_products = 0

def log_progress_with_step(step: str, completed: int = None, total: int = None):
    global _products_completed, _total_products
    with _progress_lock:
        if completed is None: completed = _products_completed
        if total is None: total = _total_products
        
    if total > 0:
        percent = round((completed / total) * 100, 2)
        log.info(f"[PROGRESS] {step}: {completed}/{total} ({percent}%)")
    else:
        log.info(f"[PROGRESS] {step}")

def update_progress(completed: int, total: int = None):
    global _products_completed, _total_products
    with _progress_lock:
        _products_completed = completed
        if total is not None: _total_products = total

def set_total_products(total: int):
    global _total_products
    with _progress_lock: _total_products = total

# =============================================================================
# TIMING / HUMAN BEHAVIOR
# =============================================================================

def format_duration(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0: return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0: return f"{minutes}m {secs}s"
    return f"{secs}s"

def ts() -> str:
    return datetime.now().isoformat(timespec="seconds")

def interruptible_sleep(seconds: float, check_interval: float = 0.5) -> bool:
    elapsed = 0.0
    while elapsed < seconds:
        if _shutdown_requested.is_set():
            return True
        sleep_time = min(check_interval, seconds - elapsed)
        time.sleep(sleep_time)
        elapsed += sleep_time
    return _shutdown_requested.is_set()

def human_delay(min_s: float = 0.3, max_s: float = 1.0):
    jitter_sleep(min_s, max_s)

def human_typing_delay() -> int:
    import random
    return random.randint(50, 150)
