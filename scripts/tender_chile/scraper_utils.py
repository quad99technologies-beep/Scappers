"""
Shared utilities for Tender Chile scraper scripts (Facade for Core).
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
from pathlib import Path
from typing import Set, Optional

# Add repo root to path for core imports (MUST be before any core imports)
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

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
log = logging.getLogger("chile_scraper")

# =============================================================================
# RESOURCE MONITORING
# =============================================================================

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
    return 0 

def kill_all_chrome_processes():
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
_tenders_completed = 0
_total_tenders = 0

def log_progress_with_step(step: str, completed: int = None, total: int = None):
    global _tenders_completed, _total_tenders
    with _progress_lock:
        if completed is None: completed = _tenders_completed
        if total is None: total = _total_tenders
        
    if total > 0:
        percent = round((completed / total) * 100, 2)
        log.info(f"[PROGRESS] {step}: {completed}/{total} ({percent}%)")
    else:
        log.info(f"[PROGRESS] {step}")

def update_progress(completed: int, total: int = None):
    global _tenders_completed, _total_tenders
    with _progress_lock:
        _tenders_completed = completed
        if total is not None: _total_tenders = total

def set_total_tenders(total: int):
    global _total_tenders
    with _progress_lock: _total_tenders = total

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
