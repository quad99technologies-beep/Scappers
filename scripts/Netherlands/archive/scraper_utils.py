"""
Shared utilities for Netherlands scraper scripts.
Contains common functions for memory management, resource cleanup, and progress tracking.
"""

import os
import gc
import sys
import time
import psutil
import signal
import atexit
import threading
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Set, Tuple

# =============================================================================
# LOGGING SETUP
# =============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("netherlands_scraper")

# =============================================================================
# PERFORMANCE FIX: Resource Monitoring and Cleanup
# =============================================================================

_shutdown_requested = threading.Event()
_active_drivers = []
_drivers_lock = threading.Lock()
_tracked_chrome_pids = set()
_tracked_pids_lock = threading.Lock()

# Memory limits
MEMORY_LIMIT_MB = 2048  # 2GB hard limit
MEMORY_CHECK_INTERVAL = 10  # Check every 10 operations


def get_resource_usage():
    """Get current resource usage for monitoring"""
    try:
        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().rss / 1024 / 1024
        threads = threading.active_count()
        return mem_mb, threads
    except Exception:
        return 0, threading.active_count()


def log_resource_usage(prefix="[RESOURCE]"):
    """Log current memory and thread usage"""
    mem_mb, threads = get_resource_usage()
    log.info(f"{prefix} Memory: {mem_mb:.1f}MB, Threads: {threads}")


def get_memory_usage_mb() -> float:
    """Get current process memory usage in MB"""
    try:
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    except Exception:
        return 0.0


def check_memory_limit() -> bool:
    """Check if memory usage exceeds limit. Returns True if over limit."""
    mem_mb = get_memory_usage_mb()
    if mem_mb > MEMORY_LIMIT_MB:
        log.warning(f"[MEMORY_LIMIT] Memory usage {mem_mb:.0f}MB exceeds limit {MEMORY_LIMIT_MB}MB")
        return True
    return False


def force_cleanup():
    """Force garbage collection and cleanup of temporary resources"""
    gc.collect()
    for _ in range(3):
        gc.collect()


def register_driver(driver):
    """Register a driver for cleanup on shutdown"""
    with _drivers_lock:
        _active_drivers.append(driver)


def unregister_driver(driver):
    """Unregister a driver"""
    with _drivers_lock:
        if driver in _active_drivers:
            _active_drivers.remove(driver)


def track_chrome_pids(pids: Set[int]):
    """Track Chrome PIDs for cleanup"""
    with _tracked_pids_lock:
        _tracked_chrome_pids.update(pids)


def kill_tracked_chrome_processes():
    """Kill only tracked Chrome processes from this scraper instance"""
    killed_count = 0
    with _tracked_pids_lock:
        if not _tracked_chrome_pids:
            return 0
        tracked_pids = _tracked_chrome_pids.copy()
    for pid in tracked_pids:
        try:
            proc = psutil.Process(pid)
            proc_name = (proc.name() or '').lower()
            if 'chrome' in proc_name or 'chromium' in proc_name:
                proc.kill()
                killed_count += 1
                log.info(f"[CLEANUP] Killed tracked Chrome process: PID {pid}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    with _tracked_pids_lock:
        _tracked_chrome_pids.clear()
    return killed_count


def kill_all_chrome_processes():
    """Kill ALL Chrome/Chromium processes system-wide (nuclear option)"""
    killed_count = 0
    log.warning("[NUCLEAR_CLEANUP] Killing ALL Chrome/Chromium processes system-wide")
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            proc_name = (proc.info['name'] or '').lower()
            if 'chrome' in proc_name or 'chromium' in proc_name:
                try:
                    p = psutil.Process(proc.info['pid'])
                    p.kill()
                    killed_count += 1
                    log.info(f"[NUCLEAR_CLEANUP] Killed {proc_name} PID {proc.info['pid']}")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    if killed_count > 0:
        log.warning(f"[NUCLEAR_CLEANUP] Killed {killed_count} Chrome/Chromium processes")
        time.sleep(2)
    return killed_count


def close_all_drivers():
    """Close all registered drivers and cleanup Chrome processes"""
    with _drivers_lock:
        driver_count = len(_active_drivers)
        if driver_count == 0:
            kill_tracked_chrome_processes()
            return
        log.info(f"[SHUTDOWN] Closing {driver_count} driver session(s)...")
        all_pids = set()
        for driver in _active_drivers[:]:
            try:
                if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                    pid = driver.service.process.pid
                    if pid:
                        all_pids.add(pid)
                        try:
                            parent = psutil.Process(pid)
                            for child in parent.children(recursive=True):
                                all_pids.add(child.pid)
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
            except Exception:
                pass
        for driver in _active_drivers[:]:
            try:
                driver.quit()
            except Exception:
                pass
        _active_drivers.clear()
        if all_pids:
            for pid in all_pids:
                try:
                    proc = psutil.Process(pid)
                    proc.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        kill_tracked_chrome_processes()


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    log.warning(f"[SHUTDOWN] Signal {signum} received, initiating graceful shutdown...")
    _shutdown_requested.set()
    close_all_drivers()
    sys.exit(0)


try:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
except (AttributeError, ValueError):
    pass

atexit.register(close_all_drivers)


# =============================================================================
# PROGRESS TRACKING
# =============================================================================

_progress_lock = threading.Lock()
_drugs_completed = 0
_total_drugs = 0
_current_step = ""


def log_progress_with_step(step: str, completed: int = None, total: int = None):
    """Log progress with step details"""
    global _drugs_completed, _total_drugs, _current_step
    with _progress_lock:
        if completed is None:
            completed = _drugs_completed
        if total is None:
            total = _total_drugs
        _current_step = step
    if total > 0:
        remaining = max(0, total - completed)
        percent = round((completed / total) * 100, 2)
        if percent > 100.0:
            percent = 100.0
        if percent < 0.0:
            percent = 0.0
        if remaining > 0:
            step_display = f"{step} - {remaining} left"
        else:
            step_display = step
        progress_msg = f"[PROGRESS] {step_display}: {completed}/{total} ({percent}%)"
        print(progress_msg, flush=True)
        log.info(progress_msg)
    else:
        progress_msg = f"[PROGRESS] {step}"
        print(progress_msg, flush=True)
        log.info(progress_msg)


def update_progress(completed: int, total: int = None):
    """Update progress counters"""
    global _drugs_completed, _total_drugs
    with _progress_lock:
        _drugs_completed = completed
        if total is not None:
            _total_drugs = total


def set_total_drugs(total: int):
    """Set total drugs count"""
    global _total_drugs
    with _progress_lock:
        _total_drugs = total


# =============================================================================
# TIMING UTILITIES
# =============================================================================

def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable string"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"


def ts() -> str:
    """Get current timestamp as ISO string"""
    return datetime.now().isoformat(timespec="seconds")


# =============================================================================
# INTERRUPTIBLE SLEEP
# =============================================================================

def interruptible_sleep(seconds: float, check_interval: float = 0.5) -> bool:
    """Sleep with periodic shutdown checks. Returns True if shutdown was requested."""
    elapsed = 0.0
    while elapsed < seconds:
        if _shutdown_requested.is_set():
            return True
        sleep_time = min(check_interval, seconds - elapsed)
        time.sleep(sleep_time)
        elapsed += sleep_time
    return _shutdown_requested.is_set()


# =============================================================================
# HUMAN-LIKE DELAYS
# =============================================================================

def human_delay(min_s: float = 0.3, max_s: float = 1.0):
    """Random delay to mimic human behavior"""
    import random
    time.sleep(random.uniform(min_s, max_s))


def human_typing_delay() -> int:
    """Random per-character typing delay in ms"""
    import random
    return random.randint(50, 150)
