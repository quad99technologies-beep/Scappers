#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Selenium Scraper (DB-only)
Processes products from ar_product_index (pending rows with URL).
Rotates accounts every 50 searches or when captcha is detected.
"""

import csv
import re
import json
import time
import random
import statistics
import logging
import argparse
import tempfile
import shutil
import threading
import signal
import sys
import atexit
import gc
from pathlib import Path
from datetime import datetime
from queue import Queue, Empty
from typing import Optional, Tuple, List, Dict, Any

try:
    import psutil  # optional
except Exception:
    psutil = None

# =============================================================================
# PERFORMANCE FIX: Resource Monitoring and Cleanup
# =============================================================================

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

def force_cleanup():
    """Force garbage collection and cleanup of temporary resources"""
    gc.collect()
    # Clear any unreferenced temp profiles
    with _temp_profile_lock:
        dead_profiles = []
        for profile_dir in list(_temp_profile_dirs):
            path = Path(profile_dir)
            if not path.exists():
                dead_profiles.append(profile_dir)
        for profile_dir in dead_profiles:
            _temp_profile_dirs.discard(profile_dir)


# =============================================================================
# HARD MEMORY LIMIT - Force restart when memory > 2GB
# =============================================================================

MEMORY_LIMIT_MB = 2048  # 2GB hard limit
MEMORY_CHECK_INTERVAL = 10  # Check every 10 products

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


# =============================================================================
# STRICTER CLEANUP - Kill ALL Firefox/Geckodriver processes
# =============================================================================

def kill_all_firefox_processes():
    """Kill ALL Firefox and geckodriver processes system-wide (nuclear option)"""
    killed_count = 0
    if not psutil:
        return killed_count
    
    log.warning("[NUCLEAR_CLEANUP] Killing ALL Firefox/geckodriver processes system-wide")
    
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            proc_name = (proc.info['name'] or '').lower()
            if 'firefox' in proc_name or 'geckodriver' in proc_name:
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
        log.warning(f"[NUCLEAR_CLEANUP] Killed {killed_count} Firefox/geckodriver processes")
        # Wait for processes to die
        time.sleep(2)
    
    return killed_count

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None
    REQUESTS_AVAILABLE = False

try:
    import urllib3
    from urllib3.exceptions import ProtocolError
    URLLIB3_AVAILABLE = True
    # Suppress urllib3 retry warnings for expected connection errors (e.g., when Firefox is already closed)
    import logging
    urllib3_logger = logging.getLogger('urllib3.connectionpool')
    urllib3_logger.setLevel(logging.ERROR)  # Only show errors, not retry warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    urllib3 = None
    ProtocolError = Exception
    URLLIB3_AVAILABLE = False

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.alert import Alert
from selenium.common.exceptions import TimeoutException, NoSuchElementException, InvalidSessionIdException, WebDriverException, StaleElementReferenceException
import os
import socket
from webdriver_manager.firefox import GeckoDriverManager

from core.db.connection import CountryDB
from db.repositories import ArgentinaRepository
from db.schema import apply_argentina_schema
from core.db.models import generate_run_id

# ====== FINGERPRINT / SESSION ISOLATION ======
UA_POOL = [
    # Keep within common desktop Firefox versions; avoid Tor Browser UA if not using Tor Browser UI.
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

LANG_POOL = [
    "es-AR,es,en-US,en",
    "es-AR,es,en;q=0.8",
    "es,en-US,en;q=0.8",
]

TZ_POOL = [
    "America/Argentina/Buenos_Aires",
    "America/Sao_Paulo",
    "America/Santiago",
    "America/Mexico_City",
    "America/New_York",
]

VIEWPORT_POOL = [
    (1365, 768),
    (1536, 864),
    (1440, 900),
    (1600, 900),
    (1920, 1080),
]

def pick_fingerprint() -> dict:
    return {
        "ua": random.choice(UA_POOL),
        "lang": random.choice(LANG_POOL),
        "tz": random.choice(TZ_POOL),
        "viewport": random.choice(VIEWPORT_POOL),
    }

# ====== CONFIG ======
from config_loader import (
    get_input_dir, get_output_dir, get_accounts,
    ALFABETA_USER, ALFABETA_PASS, HEADLESS, HUB_URL, PRODUCTS_URL,
    SELENIUM_ROTATION_LIMIT, SELENIUM_THREADS, SELENIUM_SINGLE_ATTEMPT,
    SELENIUM_MAX_RUNS,
    SELENIUM_PRODUCTS_PER_RESTART,
    DUPLICATE_RATE_LIMIT_SECONDS,
    SKIP_REPEAT_SELENIUM_TO_API,
    USE_API_STEPS,
    REQUEST_PAUSE_BASE, REQUEST_PAUSE_JITTER_MIN, REQUEST_PAUSE_JITTER_MAX,
    WAIT_ALERT, WAIT_SEARCH_FORM, WAIT_SEARCH_RESULTS, WAIT_PAGE_LOAD,
    PAGE_LOAD_TIMEOUT, MAX_RETRIES_TIMEOUT, CPU_THROTTLE_HIGH, PAUSE_CPU_THROTTLE,
    QUEUE_GET_TIMEOUT,
    PRODUCTLIST_FILE, PREPARED_URLS_FILE,
    OUTPUT_PRODUCTS_CSV, OUTPUT_PROGRESS_CSV, OUTPUT_ERRORS_CSV,
    SLOW_PAGE_RESTART_ENABLED, SLOW_PAGE_MEDIAN_WINDOW, SLOW_PAGE_MIN_SAMPLES,
    SLOW_PAGE_MEDIAN_THRESHOLD_SECONDS,
    TOR_CONTROL_HOST, TOR_CONTROL_PORT, TOR_CONTROL_PASSWORD, TOR_CONTROL_COOKIE_FILE,
    TOR_NEWNYM_ENABLED, TOR_NEWNYM_INTERVAL_SECONDS, TOR_SOCKS_PORT,
    TOR_NEWNYM_COOLDOWN_SECONDS,
    SURFSHARK_RECONNECT_CMD, SURFSHARK_ROTATE_INTERVAL_SECONDS, SURFSHARK_IP_CHANGE_TIMEOUT_SECONDS,
    MAX_BROWSER_RUNTIME_SECONDS,
    REQUIRE_TOR_PROXY,
    SELENIUM_ROUND_ROBIN_RETRY, SELENIUM_MAX_ATTEMPTS_PER_PRODUCT
)

from core.ip_rotation import (
    get_public_ip_direct,
    get_public_ip_via_socks,
    run_command,
    tor_signal_newnym,
    wait_tor_ready,
    wait_for_ip_change_direct,
)

from scraper_utils import (
    ensure_headers, combine_skip_sets,
    append_rows, append_progress, append_error,
    nk, ts, strip_accents, OUT_FIELDS, update_prepared_urls_source,
    sync_files_from_output, sync_files_before_selenium
)
from core.pipeline_checkpoint import get_checkpoint_manager

try:
    from core.firefox_pid_tracker import save_firefox_pids, cleanup_pid_file as cleanup_firefox_pid_file
except Exception:
    save_firefox_pids = None
    cleanup_firefox_pid_file = None

# ====== OUTPUT SCHEMA PATCH ======
# Ensure PAMI_OS is written to CSV even if scraper_utils.OUT_FIELDS is older.
try:
    if isinstance(OUT_FIELDS, list) and "PAMI_OS" not in OUT_FIELDS:
        # place PAMI_OS right after PAMI_AF if possible
        if "PAMI_AF" in OUT_FIELDS:
            OUT_FIELDS.insert(OUT_FIELDS.index("PAMI_AF") + 1, "PAMI_OS")
        else:
            OUT_FIELDS.append("PAMI_OS")
except Exception:
    pass

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("selenium_scraper")

_GECKODRIVER_INSTALL_LOCK = threading.Lock()
_GECKODRIVER_PATH: str | None = None


def _wipe_wdm_metadata() -> None:
    """Clear webdriver-manager metadata file if it gets corrupted by concurrent writes."""
    try:
        root = Path.home() / ".wdm"
        for name in ("drivers.json", "drivers.json.lock"):
            p = root / name
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
    except Exception:
        pass


def _get_geckodriver_path() -> str:
    """Resolve geckodriver path once per process to avoid webdriver-manager races."""
    global _GECKODRIVER_PATH
    if _GECKODRIVER_PATH:
        return _GECKODRIVER_PATH

    with _GECKODRIVER_INSTALL_LOCK:
        if _GECKODRIVER_PATH:
            return _GECKODRIVER_PATH

        for attempt in range(1, 4):
            try:
                _GECKODRIVER_PATH = GeckoDriverManager().install()
                return _GECKODRIVER_PATH
            except Exception as e:
                msg = str(e) or ""
                if "JSONDecodeError" in type(e).__name__ or "Expecting value" in msg:
                    log.warning("[WDM] Corrupted metadata detected; clearing drivers.json and retrying...")
                    _wipe_wdm_metadata()
                    time.sleep(0.25)
                    continue
                if attempt < 3:
                    log.warning(f"[WDM] GeckoDriverManager install failed (attempt {attempt}/3): {e}")
                    time.sleep(0.5)
                    continue
                raise

    return _GECKODRIVER_PATH  # pragma: no cover

def _pipeline_context_suffix() -> str:
    """Build a short context suffix so UI/pipeline logs show consistent numbering."""
    step = os.environ.get("PIPELINE_STEP_DISPLAY", "").strip()
    total = os.environ.get("PIPELINE_TOTAL_STEPS", "").strip()
    rnd = os.environ.get("SELENIUM_ROUND", "").strip()
    rnd_total = os.environ.get("SELENIUM_TOTAL_ROUNDS", "").strip()

    parts = []
    if step and total:
        parts.append(f"pipeline step {step}/{total}")
    if rnd and rnd_total:
        parts.append(f"round {rnd}/{rnd_total}")
    elif rnd:
        parts.append(f"round {rnd}")
    if not parts:
        return ""
    return " (" + ", ".join(parts) + ")"

def clear_browser_storage(driver):
    """Best-effort cleanup of cookies and WebStorage before closing a session."""
    if driver is None:
        return
    try:
        driver.delete_all_cookies()
    except Exception:
        pass
    try:
        driver.execute_script(
            "try { localStorage.clear(); } catch (e) {}; "
            "try { sessionStorage.clear(); } catch (e) {}"
        )
    except Exception:
        pass

def create_temp_profile():
    profile_dir = Path(tempfile.mkdtemp(prefix="argentina_ff_"))
    with _temp_profile_lock:
        _temp_profile_dirs.add(str(profile_dir))
    return webdriver.FirefoxProfile(str(profile_dir)), profile_dir

def cleanup_temp_profile(profile_dir):
    """Clean up temporary Firefox profile directory with retries"""
    if not profile_dir:
        return
    profile_path = Path(profile_dir)
    
    # Remove from tracking set first
    with _temp_profile_lock:
        _temp_profile_dirs.discard(str(profile_path))
    
    # Try to delete with retries (Windows file locking can cause issues)
    if profile_path.exists():
        for attempt in range(3):
            try:
                shutil.rmtree(profile_path, ignore_errors=False)
                if not profile_path.exists():
                    log.debug(f"[PROFILE] Cleaned up temp profile: {profile_path}")
                    break
            except Exception as e:
                if attempt < 2:
                    wait_time = 0.5 * (attempt + 1)
                    log.debug(f"[PROFILE] Cleanup attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    # Final attempt - use ignore_errors=True as fallback
                    try:
                        shutil.rmtree(profile_path, ignore_errors=True)
                    except Exception:
                        pass
                    if profile_path.exists():
                        log.warning(f"[PROFILE] Could not remove temp profile dir {profile_path} after 3 attempts")

# ====== SHUTDOWN HANDLING ======
_shutdown_requested = threading.Event()
_active_drivers = []
_drivers_lock = threading.Lock()
_skip_lock = threading.Lock()  # Lock for updating skip_set during runtime
_attempted_lock = threading.Lock()  # Lock for tracking attempted items during runtime
_attempted_keys = set()  # Track items already attempted to avoid repeat Selenium searches

# ====== TRACKED PIDs ======
_tracked_firefox_pids = set()  # Track all Firefox/Tor PIDs created by this scraper instance
_tracked_pids_lock = threading.Lock()  # Lock for tracked PIDs

# ====== TOR NEWNYM TRACKING ======
_tor_newnym_counter = 0
_tor_newnym_lock = threading.Lock()
_tor_newnym_thread = None

# ====== TEMP PROFILE TRACKING ======
_temp_profile_dirs = set()
_temp_profile_lock = threading.Lock()

# ====== ROUND-ROBIN RETRY TRACKING ======
_product_attempt_counts = {}  # (company, product) -> attempt_count
_attempt_counts_lock = threading.Lock()

def get_product_attempt_count(company: str, product: str) -> int:
    """Get the number of attempts made for a product."""
    key = (nk(company), nk(product))
    with _attempt_counts_lock:
        return _product_attempt_counts.get(key, 0)

def increment_product_attempt_count(company: str, product: str) -> int:
    """Increment and return the attempt count for a product."""
    key = (nk(company), nk(product))
    with _attempt_counts_lock:
        _product_attempt_counts[key] = _product_attempt_counts.get(key, 0) + 1
        return _product_attempt_counts[key]

def should_requeue_for_round_robin(company: str, product: str) -> bool:
    """Check if product should be requeued for round-robin retry.
    
    Returns True if product should be requeued (attempts < max),
    False if max attempts reached (should move to API).
    """
    if not SELENIUM_ROUND_ROBIN_RETRY:
        return True  # Not in round-robin mode, always requeue
    
    attempts = increment_product_attempt_count(company, product)
    if attempts >= SELENIUM_MAX_ATTEMPTS_PER_PRODUCT:
        log.warning(f"[ROUND_ROBIN] Max attempts ({SELENIUM_MAX_ATTEMPTS_PER_PRODUCT}) reached for {company} | {product}")
        return False
    log.info(f"[ROUND_ROBIN] Requeueing {company} | {product} for attempt {attempts + 1}/{SELENIUM_MAX_ATTEMPTS_PER_PRODUCT}")
    return True

# ====== ROTATION COORDINATION ======
class RotationCoordinator:
    """
    Coordinates global identity rotations (Surfshark reconnect + Tor NEWNYM) across all Selenium workers.
    Workers must fully stop/kill their browsers before rotation starts, and only relaunch after rotation completes.
    """

    def __init__(self, num_workers: int):
        self.num_workers = max(1, int(num_workers))
        self._cv = threading.Condition()
        self._rotation_in_progress = False
        self._rotation_seq = 0
        self._ready_workers = set()
        self._reason = ""
        self._shutdown = False

    def shutdown(self) -> None:
        with self._cv:
            self._shutdown = True
            self._cv.notify_all()

    def start_rotation(self, reason: str) -> int:
        with self._cv:
            if self._shutdown:
                return self._rotation_seq
            if self._rotation_in_progress:
                return self._rotation_seq
            self._rotation_in_progress = True
            self._rotation_seq += 1
            self._ready_workers.clear()
            self._reason = reason
            self._cv.notify_all()
            return self._rotation_seq

    def mark_worker_ready(self, worker_id: int) -> None:
        with self._cv:
            self._ready_workers.add(worker_id)
            self._cv.notify_all()

    def wait_all_ready(self, timeout_seconds: int = 120) -> bool:
        deadline = time.time() + timeout_seconds
        with self._cv:
            while not self._shutdown and len(self._ready_workers) < self.num_workers and time.time() < deadline:
                self._cv.wait(timeout=1.0)
            return len(self._ready_workers) >= self.num_workers

    def finish_rotation(self) -> None:
        with self._cv:
            self._rotation_in_progress = False
            self._reason = ""
            self._cv.notify_all()

    def wait_if_rotating_and_get_seq(self, last_seen_seq: int) -> Tuple[bool, int, str]:
        """
        Returns (should_rotate_now, seq, reason). If should_rotate_now is True, worker must stop browser.
        """
        with self._cv:
            if self._shutdown:
                return False, self._rotation_seq, ""
            if self._rotation_in_progress and last_seen_seq != self._rotation_seq:
                return True, self._rotation_seq, self._reason
            return False, self._rotation_seq, ""

    def wait_rotation_done(self, seq: int) -> None:
        with self._cv:
            while not self._shutdown and self._rotation_in_progress and self._rotation_seq == seq:
                self._cv.wait(timeout=1.0)


def _rotation_loop(rotation: RotationCoordinator):
    """
    Periodically rotate Surfshark IP and/or Tor identity.
    The loop blocks workers via RotationCoordinator so no browser is reused across IP changes.
    """
    # Stagger first rotations slightly so initial startup isn't interrupted.
    next_surf = time.monotonic() + max(30, int(SURFSHARK_ROTATE_INTERVAL_SECONDS))
    next_tor = time.monotonic() + max(30, int(TOR_NEWNYM_INTERVAL_SECONDS))

    while not _shutdown_requested.is_set():
        now = time.monotonic()
        surf_due = bool(SURFSHARK_RECONNECT_CMD) and now >= next_surf
        tor_due = bool(TOR_NEWNYM_ENABLED) and now >= next_tor

        if not surf_due and not tor_due:
            time.sleep(1.0)
            continue

        reasons = []
        if surf_due:
            reasons.append("surfshark")
        if tor_due:
            reasons.append("tor_newnym")
        reason = "+".join(reasons) if reasons else "rotation"

        seq = rotation.start_rotation(reason)
        log.warning(f"[ROTATION] Starting rotation seq {seq}: {reason}")

        # Wait until all workers fully closed their browsers.
        all_ready = rotation.wait_all_ready(timeout_seconds=180)
        if not all_ready:
            log.warning(f"[ROTATION] Not all workers reached safe point before rotation seq {seq} (continuing anyway)")

        # IP snapshots (direct + Tor exit) for logging/verification.
        direct_before = get_public_ip_direct()
        tor_before = None
        try:
            socks_port = int(TOR_SOCKS_PORT or TOR_PROXY_PORT or 0)
            if socks_port > 0:
                tor_before = get_public_ip_via_socks("127.0.0.1", socks_port)
        except Exception:
            tor_before = None
        log.info(f"[ROTATION] IP before: direct={direct_before or 'unknown'} tor={tor_before or 'unknown'}")

        # Surfshark rotation (VPN IP)
        if surf_due:
            ok, out = run_command(SURFSHARK_RECONNECT_CMD, timeout_seconds=120)
            log.info(f"[ROTATION] Surfshark reconnect cmd ok={ok} output_tail={out!r}")
            new_direct = None
            if direct_before:
                new_direct = wait_for_ip_change_direct(
                    direct_before,
                    timeout_seconds=int(SURFSHARK_IP_CHANGE_TIMEOUT_SECONDS),
                    poll_seconds=2.0,
                )
            else:
                new_direct = get_public_ip_direct()
            if new_direct and (not direct_before or new_direct != direct_before):
                log.info(f"[ROTATION] Surfshark IP changed: {direct_before} -> {new_direct}")
            else:
                log.warning(f"[ROTATION] Surfshark IP did not change within timeout (before={direct_before}, after={new_direct})")
            next_surf = time.monotonic() + max(30, int(SURFSHARK_ROTATE_INTERVAL_SECONDS))

        # Tor NEWNYM rotation (exit circuit)
        if tor_due:
            # Ensure Tor is ready before signaling NEWNYM.
            if TOR_CONTROL_PORT:
                ready = wait_tor_ready(
                    TOR_CONTROL_HOST or "127.0.0.1",
                    int(TOR_CONTROL_PORT),
                    cookie_file=TOR_CONTROL_COOKIE_FILE or "",
                    password=TOR_CONTROL_PASSWORD or "",
                    timeout_seconds=120,
                )
                if not ready:
                    log.warning("[ROTATION] Tor control port not ready/bootstrapped; NEWNYM may fail")

            res = tor_signal_newnym(
                TOR_CONTROL_HOST or "127.0.0.1",
                int(TOR_CONTROL_PORT or 0),
                cookie_file=TOR_CONTROL_COOKIE_FILE or "",
                password=TOR_CONTROL_PASSWORD or "",
                cooldown_seconds=int(TOR_NEWNYM_COOLDOWN_SECONDS),
            )
            log.info(
                f"[ROTATION] Tor NEWNYM ok={res.ok} circuit_id {res.old_circuit_id}->{res.new_circuit_id}"
            )

            # Verify Tor exit IP changed (best-effort).
            tor_after = None
            socks_port = int(TOR_SOCKS_PORT or TOR_PROXY_PORT or 0)
            if socks_port > 0:
                deadline = time.time() + 90
                while time.time() < deadline:
                    tor_after = get_public_ip_via_socks("127.0.0.1", socks_port)
                    if tor_after and tor_before and tor_after != tor_before:
                        break
                    if tor_after and not tor_before:
                        break
                    time.sleep(2.0)
            if tor_after and (not tor_before or tor_after != tor_before):
                log.info(f"[ROTATION] Tor exit IP changed: {tor_before} -> {tor_after}")
            else:
                log.warning(f"[ROTATION] Tor exit IP did not change within timeout (before={tor_before}, after={tor_after})")

            next_tor = time.monotonic() + max(30, int(TOR_NEWNYM_INTERVAL_SECONDS))

        direct_after = get_public_ip_direct()
        tor_after_final = None
        try:
            socks_port = int(TOR_SOCKS_PORT or TOR_PROXY_PORT or 0)
            if socks_port > 0:
                tor_after_final = get_public_ip_via_socks("127.0.0.1", socks_port)
        except Exception:
            tor_after_final = None
        log.info(f"[ROTATION] IP after: direct={direct_after or 'unknown'} tor={tor_after_final or 'unknown'}")

        rotation.finish_rotation()
        log.warning(f"[ROTATION] Finished rotation seq {seq}: {reason}")


# ====== PROGRESS TRACKING ======
_progress_lock = threading.Lock()
_products_completed = 0
_total_products = 0
_current_step = ""  # Track current step for progress display

def signal_handler(signum, frame):
    """Handle shutdown signals (Ctrl+C, SIGTERM, etc.)"""
    log.warning(f"[SHUTDOWN] Shutdown signal received ({signum}), requesting graceful shutdown...")
    # Set shutdown event first - workers will check this and exit gracefully
    _shutdown_requested.set()
    # Don't kill Firefox immediately - let workers exit gracefully first
    # Firefox cleanup will happen in close_all_drivers() after workers exit

def interruptible_sleep(seconds: float, check_interval: float = 0.5):
    """Sleep with periodic shutdown checks. Returns True if shutdown was requested."""
    elapsed = 0.0
    while elapsed < seconds:
        if _shutdown_requested.is_set():
            return True
        sleep_time = min(check_interval, seconds - elapsed)
        time.sleep(sleep_time)
        elapsed += sleep_time
    return _shutdown_requested.is_set()

def log_progress_with_step(step: str, completed: int = None, total: int = None):
    """Log progress with step details. If completed/total not provided, uses global values."""
    global _products_completed, _total_products, _current_step
    
    with _progress_lock:
        if completed is None:
            completed = _products_completed
        if total is None:
            total = _total_products
        _current_step = step
    
    if total > 0:
        # Calculate remaining products for display
        remaining = max(0, total - completed)
        # Calculate percentage correctly - use 2 decimal places for better accuracy on small percentages
        percent = round((completed / total) * 100, 2)
        # Ensure percent doesn't exceed 100%
        if percent > 100.0:
            percent = 100.0
        # Ensure percent is at least 0%
        if percent < 0.0:
            percent = 0.0
        
        # Format must match GUI parser regex: [PROGRESS] {step}: X/Y (Z%)
        # The regex expects: \[PROGRESS\]\s+(.+?)\s*:\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)
        # So format should be: [PROGRESS] {step}: {completed}/{total} ({percent}%)
        # Include remaining count in step description for visibility
        if remaining > 0:
            step_display = f"{step} - {remaining} left"
        else:
            step_display = step
        progress_msg = f"[PROGRESS] {step_display}: {completed}/{total} ({percent}%)"
        # Use both print (for real-time progress bar) and log (for log files)
        print(progress_msg, flush=True)
        log.info(progress_msg)
    else:
        # If total not set yet, just show step
        progress_msg = f"[PROGRESS] {step}"
        print(progress_msg, flush=True)
        log.info(progress_msg)

def register_driver(driver):
    """Register a driver for cleanup on shutdown"""
    with _drivers_lock:
        _active_drivers.append(driver)

def unregister_driver(driver):
    """Unregister a driver"""
    with _drivers_lock:
        if driver in _active_drivers:
            _active_drivers.remove(driver)

def close_all_drivers():
    """Close all registered Firefox/Tor drivers and kill only tracked Firefox processes (Alfabeta scraper only)"""
    with _drivers_lock:
        driver_count = len(_active_drivers)
        if driver_count == 0:
            log.info("[SHUTDOWN] No Firefox/Tor sessions to close")
            # Only kill tracked PIDs (from this scraper instance)
            kill_tracked_firefox_processes()
            return
        
        log.info(f"[SHUTDOWN] Closing {driver_count} Firefox/Tor session(s)...")
        
        # First, get all Firefox/geckodriver PIDs from drivers before closing
        all_pids = set()
        for driver in _active_drivers[:]:  # Copy list to avoid modification during iteration
            try:
                # Get geckodriver PID and Firefox child processes
                if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                    geckodriver_pid = driver.service.process.pid
                    if geckodriver_pid:
                        all_pids.add(geckodriver_pid)
                        if psutil:
                            try:
                                parent = psutil.Process(geckodriver_pid)
                                for child in parent.children(recursive=True):
                                    all_pids.add(child.pid)
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                pass
                # Track these PIDs
                if all_pids:
                    with _tracked_pids_lock:
                        _tracked_firefox_pids.update(all_pids)
                    if save_firefox_pids:
                        save_firefox_pids("Argentina", REPO_ROOT, all_pids)
            except Exception:
                pass
        
        # Close all drivers
        for driver in _active_drivers[:]:  # Copy list to avoid modification during iteration
            try:
                clear_browser_storage(driver)
                driver.quit()
            except Exception as e:
                # Only log if it's not a "session not found" type error (expected after quit)
                error_msg = str(e).lower()
                if "session" not in error_msg and "connection" not in error_msg and "target window" not in error_msg:
                    log.warning(f"[SHUTDOWN] Error closing driver: {e}")
            finally:
                cleanup_temp_profile(getattr(driver, "_profile_dir", None))
            # Don't call driver.close() after quit() - it causes noisy connection errors
        _active_drivers.clear()
        
        # Kill Firefox/geckodriver processes associated with these drivers (only tracked PIDs)
        if all_pids and psutil:
            log.info(f"[SHUTDOWN] Killing Firefox/geckodriver processes (Alfabeta only): {sorted(all_pids)}")
            for pid in all_pids:
                try:
                    proc = psutil.Process(pid)
                    proc.kill()
                    log.info(f"[SHUTDOWN] Killed Firefox/geckodriver process (Alfabeta): PID {pid}")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        
        # Only kill tracked orphaned Firefox/geckodriver processes (from this scraper instance only)
        kill_tracked_firefox_processes()
        
        log.info("[SHUTDOWN] All Alfabeta Firefox/Tor sessions closed")

def kill_tracked_firefox_processes():
    """Kill only tracked Firefox/geckodriver processes from this Alfabeta scraper instance"""
    killed_count = 0
    
    with _tracked_pids_lock:
        if not _tracked_firefox_pids:
            return  # No tracked PIDs to kill
        
        tracked_pids = _tracked_firefox_pids.copy()
        log.info(f"[SHUTDOWN] Killing tracked Firefox/geckodriver processes (Alfabeta only): {sorted(tracked_pids)}")
    
    # Only kill processes we tracked (from this scraper instance)
    if psutil:
        for pid in tracked_pids:
            try:
                proc = psutil.Process(pid)
                # Verify it's still a Firefox/geckodriver process before killing
                proc_name = (proc.name() or '').lower()
                if 'firefox' in proc_name or 'geckodriver' in proc_name:
                    proc.kill()
                    killed_count += 1
                    log.info(f"[SHUTDOWN] Killed tracked Firefox/geckodriver process (Alfabeta): PID {pid}")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass  # Process already dead or inaccessible
            except Exception as e:
                log.debug(f"[SHUTDOWN] Error killing tracked PID {pid}: {e}")
    
    with _tracked_pids_lock:
        _tracked_firefox_pids.clear()
    if cleanup_firefox_pid_file:
        cleanup_firefox_pid_file("Argentina", REPO_ROOT)
    
    if killed_count > 0:
        log.info(f"[SHUTDOWN] Killed {killed_count} tracked Firefox/geckodriver process(es) (Alfabeta only)")


def cleanup_orphaned_firefox_processes():
    """PERFORMANCE FIX: Kill any orphaned Firefox/geckodriver processes not in active drivers list"""
    if not psutil:
        return 0
    
    killed_count = 0
    current_pids = set()
    
    # Get PIDs from active drivers
    with _drivers_lock:
        for driver in _active_drivers:
            try:
                if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                    if driver.service.process:
                        current_pids.add(driver.service.process.pid)
            except Exception:
                pass
    
    # Find and kill orphaned processes
    try:
        for proc in psutil.process_iter(['pid', 'name', 'ppid']):
            try:
                proc_name = (proc.info['name'] or '').lower()
                pid = proc.info['pid']
                
                # Skip if it's in our active drivers
                if pid in current_pids:
                    continue
                
                # Check if it's a Firefox or geckodriver process started by us
                if 'firefox' in proc_name or 'geckodriver' in proc_name:
                    # Check if parent is our Python process or if it's an orphan
                    try:
                        parent = proc.parent()
                        if parent and parent.pid == os.getpid():
                            proc.kill()
                            killed_count += 1
                            log.info(f"[ORPHAN_CLEANUP] Killed orphaned {proc_name}: PID {pid}")
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    except Exception as e:
        log.debug(f"[ORPHAN_CLEANUP] Error during cleanup: {e}")
    
    return killed_count

# Register signal handlers
try:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
except (AttributeError, ValueError):
    # Windows may not support all signals
    pass

# Register atexit handler to ensure cleanup on any exit
atexit.register(close_all_drivers)

# ====== PATHS ======
REPO_ROOT = Path(__file__).resolve().parents[2]
INPUT_DIR = get_input_dir()
OUTPUT_DIR = get_output_dir()
PREPARED_URLS_FILE_PATH = OUTPUT_DIR / PREPARED_URLS_FILE
OUT_CSV = OUTPUT_DIR / OUTPUT_PRODUCTS_CSV
PROGRESS = OUTPUT_DIR / OUTPUT_PROGRESS_CSV
ERRORS = OUTPUT_DIR / OUTPUT_ERRORS_CSV
DEBUG_ERR = OUTPUT_DIR / "debug" / "error"
DEBUG_NF = OUTPUT_DIR / "debug" / "not_found"

# DB setup
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
_RUN_ID_FILE = OUTPUT_DIR / ".current_run_id"

def _get_run_id() -> str:
    rid = os.environ.get("ARGENTINA_RUN_ID")
    if rid:
        return rid
    if _RUN_ID_FILE.exists():
        try:
            txt = _RUN_ID_FILE.read_text(encoding="utf-8").strip()
            if txt:
                return txt
        except Exception:
            pass
    rid = generate_run_id()
    os.environ["ARGENTINA_RUN_ID"] = rid
    _RUN_ID_FILE.write_text(rid, encoding="utf-8")
    return rid

_DB = CountryDB("Argentina")
apply_argentina_schema(_DB)
_RUN_ID = _get_run_id()
_REPO = ArgentinaRepository(_DB, _RUN_ID)

# Create debug directories
for d in [DEBUG_ERR, DEBUG_NF]:
    d.mkdir(parents=True, exist_ok=True)

# Request pause jitter tuple
REQUEST_PAUSE_JITTER = (REQUEST_PAUSE_JITTER_MIN, REQUEST_PAUSE_JITTER_MAX)

# Load accounts at startup
ACCOUNTS = get_accounts()
if not ACCOUNTS:
    raise RuntimeError("No accounts found! Please configure ALFABETA_USER and ALFABETA_PASS in environment")

# ====== DRIVER HEALTH CHECKS ======

FATAL_DRIVER_SUBSTRINGS = (
    "tab crashed",
    "invalid session id",
    "disconnected",
    "cannot determine loading status",
    "firefox not reachable",
    "session deleted",
    "target window already closed",
    "connection refused",
    "connection reset",
)

class LoadTimeMonitor:
    def __init__(self, window_size: int, min_samples: int, threshold_seconds: float):
        self.window_size = max(1, int(window_size))
        self.min_samples = max(1, int(min_samples))
        if self.min_samples > self.window_size:
            self.min_samples = self.window_size
        self.threshold_seconds = float(threshold_seconds)
        self.samples = []

    def record(self, seconds: float) -> Optional[float]:
        if seconds is None:
            return None
        try:
            value = float(seconds)
        except (TypeError, ValueError):
            return None
        if value <= 0:
            return None
        self.samples.append(value)
        if len(self.samples) > self.window_size:
            self.samples.pop(0)
        if len(self.samples) < self.min_samples:
            return None
        return statistics.median(self.samples)

    def reset(self) -> None:
        self.samples.clear()

def is_driver_alive(driver) -> bool:
    """Check if driver is still alive by attempting a cheap operation."""
    if driver is None:
        return False
    try:
        _ = driver.current_url  # cheap ping
        return True
    except (InvalidSessionIdException, WebDriverException, AttributeError, Exception):
        return False

def is_fatal_driver_error(e: Exception) -> bool:
    """Check if exception indicates driver is fatally dead."""
    msg = (str(e) or "").lower()
    error_type = type(e).__name__.lower()
    
    # Check error message
    if any(s in msg for s in FATAL_DRIVER_SUBSTRINGS):
        return True
    
    # Check error type
    if "protocol" in error_type or "connection" in error_type:
        return True
    
    # Check for specific exception types
    if isinstance(e, (InvalidSessionIdException, ConnectionResetError, socket.error)):
        return True
    
    if URLLIB3_AVAILABLE and isinstance(e, ProtocolError):
        return True
    
    return False

def restart_driver(thread_id: int, driver, headless: bool):
    """Restart a dead driver and navigate back to products page."""
    try:
        if driver:
            try:
                # Get Firefox/geckodriver PIDs before closing
                try:
                    pids = set()
                    if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                        geckodriver_pid = driver.service.process.pid
                        if geckodriver_pid:
                            pids.add(geckodriver_pid)
                            if psutil:
                                try:
                                    parent = psutil.Process(geckodriver_pid)
                                    for child in parent.children(recursive=True):
                                        pids.add(child.pid)
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass
                    if pids:
                        log.info(f"[DRIVER_RESTART] Thread {thread_id}: Killing Firefox/geckodriver PIDs before restart (Alfabeta only): {sorted(pids)}")
                        # Kill Firefox/geckodriver processes associated with this driver (only tracked PIDs)
                        if psutil:
                            for pid in pids:
                                try:
                                    # Only kill if it's in our tracked set (Alfabeta scraper only)
                                    with _tracked_pids_lock:
                                        if pid in _tracked_firefox_pids:
                                            proc = psutil.Process(pid)
                                            proc.kill()
                                            log.info(f"[DRIVER_RESTART] Killed Firefox/geckodriver process (Alfabeta): PID {pid}")
                                            _tracked_firefox_pids.discard(pid)  # Remove from tracked set after killing
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass
                except Exception as e:
                    log.debug(f"[DRIVER_RESTART] Could not get/kill Firefox/geckodriver PIDs: {e}")
                
                unregister_driver(driver)
                clear_browser_storage(driver)
                driver.quit()
                # Firefox will close asynchronously
            except Exception as e:
                log.warning(f"[DRIVER_RESTART] Error closing old driver: {e}")
            finally:
                cleanup_temp_profile(getattr(driver, "_profile_dir", None))
    except Exception:
        pass
    
    log.warning(f"[DRIVER_RESTART] Thread {thread_id}: restarting Firefox/Tor driver...")
    
    # Check shutdown before creating new driver
    if _shutdown_requested.is_set():
        log.warning(f"[DRIVER_RESTART] Thread {thread_id}: shutdown requested, not restarting driver")
        return None
    
    try:
        new_driver = setup_driver(headless=headless)
        # Navigate back to products page
        navigate_to_products_page(new_driver)
        log.info(f"[DRIVER_RESTART] Thread {thread_id}: driver restarted and products page loaded")
        return new_driver
    except Exception as e:
        log.error(f"[DRIVER_RESTART] Thread {thread_id}: failed to restart driver: {e}")
        return None

# ====== UTILITY FUNCTIONS ======

def normalize_ws(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def ar_money_to_float(s: str) -> Optional[float]:
    if not s:
        return None
    t = re.sub(r"[^\d\.,]", "", s.strip())
    if not t:
        return None
    # AR: dot thousands, comma decimals
    t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None

def parse_date(s: str) -> Optional[str]:
    """Accepts '(24/07/25)' or '24/07/25' or '24-07-2025' -> '2025-07-24'"""
    s = (s or "").strip()
    m = re.search(r"\((\d{2})/(\d{2})/(\d{2})\)", s) or re.search(r"\b(\d{2})/(\d{2})/(\d{2})\b", s)
    if m:
        d, mn, y = map(int, m.groups())
        y += 2000
        try:
            return datetime(y, mn, d).date().isoformat()
        except:
            return None
    m = re.search(r"\b(\d{2})-(\d{2})-(\d{4})\b", s)
    if m:
        d, mn, y = map(int, m.groups())
        try:
            return datetime(y, mn, d).date().isoformat()
        except:
            return None
    return None

def rate_limit_pause():
    """Rate limiting pause between requests to avoid overwhelming the server."""
    time.sleep(REQUEST_PAUSE_BASE + random.uniform(*REQUEST_PAUSE_JITTER))


def mark_api_pending(company: str, product: str):
    """Mark product as pending API fallback with zero records.

    If API steps are disabled, keep the product in selenium flow so wrapper rounds (2/3) can retry it.
    """
    try:
        if USE_API_STEPS:
            # DB-only: mark for API fallback
            try:
                _REPO.mark_attempt_by_name(
                    company,
                    product,
                    loop_count=int(SELENIUM_MAX_RUNS),
                    total_records=0,
                    status="failed",
                    source="selenium",
                    error_message="pending_api",
                )
            except Exception:
                pass
            update_prepared_urls_source(
                company,
                product,
                new_source="api",
                scraped_by_selenium="yes",
                scraped_by_api="no",
                selenium_records="0",
                api_records="0",
            )
        else:
            # Keep "Source=selenium" so 3-round wrapper can pick it up again.
            update_prepared_urls_source(
                company,
                product,
                new_source="selenium",
                scraped_by_selenium="no",
                scraped_by_api="no",
                selenium_records="0",
                api_records="0",
            )
    except Exception:
        pass


def check_connection_with_retry(driver, url: str, max_retries: int = 3) -> bool:
    """Check if connection is working. If not, wait 2 min and retry.
    Returns True if connection works, False if all retries fail.
    """
    for attempt in range(max_retries):
        try:
            log.info(f"[CONNECTION_CHECK] Attempt {attempt + 1}/{max_retries}: Testing connection to {url}")
            driver.set_page_load_timeout(30)  # 30 second timeout for connection test
            driver.get(url)
            
            # Check if page loaded successfully
            if driver.current_url and driver.current_url != "about:blank":
                log.info(f"[CONNECTION_CHECK] [OK] Connection successful (URL: {driver.current_url})")
                return True
            else:
                log.warning(f"[CONNECTION_CHECK] Page loaded but URL is invalid: {driver.current_url}")
                
        except Exception as e:
            log.warning(f"[CONNECTION_CHECK] Connection failed (attempt {attempt + 1}/{max_retries}): {e}")
            
        # If not last attempt, wait 2 minutes before retry
        if attempt < max_retries - 1:
            wait_time = 120  # 2 minutes
            log.warning(f"[CONNECTION_CHECK] Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
    
    log.error(f"[CONNECTION_CHECK] [FAIL] Connection failed after {max_retries} attempts")
    return False

def save_debug(driver, folder: Path, tag: str):
    try:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        png = folder / f"{tag}_{stamp}.png"
        html = folder / f"{tag}_{stamp}.html"
        driver.save_screenshot(str(png))
        html.write_text(driver.page_source, encoding="utf-8")
    except Exception as e:
        log.warning(f"Could not save debug for {tag}: {e}")

# ====== DRIVER / LOGIN ======

# Global variable to store the Tor SOCKS port (0/None means direct connection).
# Prefer configured TOR_SOCKS_PORT (standalone Tor: 9050) when available.
TOR_PROXY_PORT = int(TOR_SOCKS_PORT) if TOR_SOCKS_PORT else 0

def check_tor_running(host="127.0.0.1", timeout=2):
    """
    Check if Tor SOCKS5 proxy is running and accepting connections.
    Checks both port 9050 (Tor service) and 9150 (Tor Browser).
    
    Returns:
        Tuple of (is_running: bool, port: int) - port is 9050 or 9150 if running, None otherwise
    """
    # Prefer configured TOR_SOCKS_PORT. Otherwise, infer likely SOCKS port from control port.
    if TOR_SOCKS_PORT and TOR_SOCKS_PORT > 0:
        ports_to_check = [TOR_SOCKS_PORT]
    elif TOR_CONTROL_PORT == 9051:
        ports_to_check = [9050, 9150]
    elif TOR_CONTROL_PORT == 9151:
        ports_to_check = [9150, 9050]
    else:
        ports_to_check = [9150, 9050]
    
    for port in ports_to_check:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                port_name = "Tor Browser" if port == 9150 else "Tor service"
                log.info(f"[TOR_CHECK] {port_name} proxy is running on {host}:{port}")
                return True, port
        except Exception as e:
            log.debug(f"[TOR_CHECK] Error checking port {port}: {e}")
            continue
    
    log.warning(f"[TOR_CHECK] Tor proxy is not running on {host}:9050 or {host}:9150")
    return False, None

def get_tor_newnym_counter() -> int:
    with _tor_newnym_lock:
        return _tor_newnym_counter

def _increment_tor_newnym_counter() -> int:
    with _tor_newnym_lock:
        global _tor_newnym_counter
        _tor_newnym_counter += 1
        return _tor_newnym_counter

def _resolve_tor_control_port() -> int:
    if TOR_CONTROL_PORT and TOR_CONTROL_PORT > 0:
        return TOR_CONTROL_PORT
    return 9151 if TOR_PROXY_PORT == 9150 else 9051

def _read_control_response(sock, timeout_seconds: float = 5.0) -> str:
    sock.settimeout(timeout_seconds)
    data = b""
    end_time = time.monotonic() + timeout_seconds
    while time.monotonic() < end_time:
        try:
            chunk = sock.recv(1024)
        except socket.timeout:
            break
        if not chunk:
            break
        data += chunk
        lines = data.split(b"\r\n")
        for line in lines:
            if not line:
                continue
            if line.startswith(b"250") or line.startswith(b"5"):
                return line.decode("utf-8", "ignore")
    if not data:
        return ""
    return data.decode("utf-8", "ignore").strip().splitlines()[-1]

def _send_control_command(sock, command: str) -> str:
    sock.sendall((command + "\r\n").encode("utf-8"))
    return _read_control_response(sock)

def _build_auth_command() -> str:
    if TOR_CONTROL_COOKIE_FILE:
        try:
            cookie = Path(TOR_CONTROL_COOKIE_FILE).read_bytes()
            return f"AUTHENTICATE {cookie.hex()}"
        except Exception as e:
            log.warning(f"[TOR_NEWNYM] Could not read control cookie: {e}")
    if TOR_CONTROL_PASSWORD:
        safe_pw = TOR_CONTROL_PASSWORD.replace("\\", "\\\\").replace('"', '\\"')
        return f'AUTHENTICATE "{safe_pw}"'
    return "AUTHENTICATE"

def request_newnym() -> bool:
    host = TOR_CONTROL_HOST or "127.0.0.1"
    port = _resolve_tor_control_port()
    if not port:
        log.warning("[TOR_NEWNYM] Control port not configured; cannot request NEWNYM")
        return False
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            auth_cmd = _build_auth_command()
            auth_resp = _send_control_command(sock, auth_cmd)
            if not auth_resp.startswith("250"):
                log.warning(f"[TOR_NEWNYM] AUTH failed: {auth_resp}")
                return False
            newnym_resp = _send_control_command(sock, "SIGNAL NEWNYM")
            if not newnym_resp.startswith("250"):
                log.warning(f"[TOR_NEWNYM] NEWNYM failed: {newnym_resp}")
                return False
    except Exception as e:
        log.warning(f"[TOR_NEWNYM] Control connection failed: {e}")
        return False
    seq = _increment_tor_newnym_counter()
    log.info(f"[TOR_NEWNYM] NEWNYM signaled (seq {seq})")
    return True

def _tor_newnym_loop():
    interval = max(10, int(TOR_NEWNYM_INTERVAL_SECONDS))
    next_at = time.monotonic() + interval
    while not _shutdown_requested.is_set():
        now = time.monotonic()
        if now >= next_at:
            request_newnym()
            next_at = now + interval
            continue
        time.sleep(min(1.0, next_at - now))

def start_tor_newnym_thread():
    global _tor_newnym_thread
    if not TOR_NEWNYM_ENABLED or TOR_NEWNYM_INTERVAL_SECONDS <= 0:
        return
    if _tor_newnym_thread and _tor_newnym_thread.is_alive():
        return
    control_port = _resolve_tor_control_port()
    interval = max(10, int(TOR_NEWNYM_INTERVAL_SECONDS))
    log.info(f"[TOR_NEWNYM] Auto NEWNYM enabled: every {interval}s via {TOR_CONTROL_HOST}:{control_port}")
    _tor_newnym_thread = threading.Thread(
        target=_tor_newnym_loop,
        name="tor-newnym",
        daemon=True,
    )
    _tor_newnym_thread.start()

def find_firefox_binary():
    """
    Find Firefox binary in common locations on Windows.
    Checks for:
    1. Regular Firefox installation
    2. Tor Browser (which includes Firefox)
    3. Environment variable FIREFOX_BINARY
    """
    import os
    from pathlib import Path
    
    # Check environment variable first
    firefox_bin = os.getenv("FIREFOX_BINARY", "")
    if firefox_bin and Path(firefox_bin).exists():
        log.info(f"[FIREFOX] Using Firefox binary from FIREFOX_BINARY env: {firefox_bin}")
        return str(Path(firefox_bin).resolve())
    
    # Common Firefox installation paths on Windows
    userprofile = os.environ.get("USERPROFILE", "")
    possible_paths = [
        # Regular Firefox
        Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "Mozilla Firefox" / "firefox.exe",
        Path(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")) / "Mozilla Firefox" / "firefox.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Mozilla Firefox" / "firefox.exe",
        # Tor Browser (includes Firefox) - Standard locations
        Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "Tor Browser" / "Browser" / "firefox.exe",
        Path(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")) / "Tor Browser" / "Browser" / "firefox.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Tor Browser" / "Browser" / "firefox.exe",
        # Common user installation locations
        Path(userprofile) / "AppData" / "Local" / "Mozilla Firefox" / "firefox.exe",
        Path(userprofile) / "AppData" / "Local" / "Tor Browser" / "Browser" / "firefox.exe",
        # Desktop location (common for portable installations)
        Path(userprofile) / "Desktop" / "Tor Browser" / "Browser" / "firefox.exe",
        Path(userprofile) / "OneDrive" / "Desktop" / "Tor Browser" / "Browser" / "firefox.exe",
        # Downloads folder (common for portable installations)
        Path(userprofile) / "Downloads" / "Tor Browser" / "Browser" / "firefox.exe",
        Path(userprofile) / "OneDrive" / "Downloads" / "Tor Browser" / "Browser" / "firefox.exe",
    ]
    
    for path in possible_paths:
        if path.exists():
            log.info(f"[FIREFOX] Found Firefox binary: {path}")
            return str(path.resolve())
    
    # Last resort: try to find firefox.exe in PATH
    import shutil
    firefox_path = shutil.which("firefox")
    if firefox_path:
        log.info(f"[FIREFOX] Found Firefox in PATH: {firefox_path}")
        return firefox_path
    
    return None

def check_requirements():
    """
    Check all requirements before starting the scraper.
    Returns True if all requirements are met, False otherwise.
    Prints detailed error messages for missing requirements.
    """
    print("\n" + "=" * 80)
    print("[REQUIREMENTS] Checking prerequisites...")
    print("=" * 80)
    
    all_ok = True
    
    # Check 1: Firefox/Tor Browser installation
    print("\n[REQUIREMENTS] 1. Checking Firefox/Tor Browser installation...")
    firefox_binary = find_firefox_binary()
    if firefox_binary:
        print(f"  [OK] Firefox/Tor Browser found: {firefox_binary}")
        log.info(f"[REQUIREMENTS] Firefox/Tor Browser found: {firefox_binary}")
    else:
        print("  [FAIL] Firefox/Tor Browser not found")
        print("  [INFO] Please install Firefox or Tor Browser")
        print("  [INFO] Firefox: https://www.mozilla.org/firefox/")
        print("  [INFO] Tor Browser: https://www.torproject.org/download/")
        print("  [INFO] Or set FIREFOX_BINARY environment variable")
        log.error("[REQUIREMENTS] Firefox/Tor Browser not found")
        all_ok = False
    
    # Check 2: Tor service running (only required if REQUIRE_TOR_PROXY=true)
    print("\n[REQUIREMENTS] 2. Checking Tor proxy service...")
    tor_running, tor_port = check_tor_running()
    if tor_running:
        port_name = "Tor Browser" if tor_port == 9150 else "Tor service"
        print(f"  [OK] {port_name} proxy is running on localhost:{tor_port}")
        log.info(f"[REQUIREMENTS] {port_name} proxy is running on port {tor_port}")
        # Store the detected port for later use
        global TOR_PROXY_PORT
        TOR_PROXY_PORT = tor_port
    else:
        if REQUIRE_TOR_PROXY:
            print("  [FAIL] Tor proxy is not running on localhost:9050 or localhost:9150")
            print("  [INFO] Please start Tor before running the scraper:")
            print("  [INFO]   Option 1: Start Tor Browser (uses port 9150)")
            print("  [INFO]   Option 2: Start Tor service separately (uses port 9050)")
            print("  [INFO]   The scraper will automatically detect which port Tor is using")
            log.error("[REQUIREMENTS] Tor proxy is not running")
            all_ok = False
        else:
            print("  [WARN] Tor proxy is not running; continuing with direct connection")
            print("  [INFO] Set REQUIRE_TOR_PROXY=true to enforce Tor usage")
            log.warning("[REQUIREMENTS] Tor proxy is not running; proceeding without Tor")
            TOR_PROXY_PORT = 0
    
    # Check 3: Required Python packages (basic check)
    print("\n[REQUIREMENTS] 3. Checking Python dependencies...")
    missing_packages = []
    try:
        import selenium
        print("  [OK] selenium package installed")
    except ImportError:
        print("  [FAIL] selenium package not found")
        missing_packages.append("selenium")
        all_ok = False
    
    try:
        from webdriver_manager.firefox import GeckoDriverManager
        print("  [OK] webdriver-manager package installed")
    except ImportError:
        print("  [FAIL] webdriver-manager package not found")
        missing_packages.append("webdriver-manager")
        all_ok = False
    
    if missing_packages:
        print(f"  [INFO] Install missing packages: pip install {' '.join(missing_packages)}")
        log.error(f"[REQUIREMENTS] Missing Python packages: {', '.join(missing_packages)}")
    
    # Summary
    print("\n" + "=" * 80)
    if all_ok:
        print("[REQUIREMENTS] All requirements met! Starting scraper...")
        log.info("[REQUIREMENTS] All requirements met")
    else:
        print("[REQUIREMENTS] Some requirements are missing. Please fix the issues above.")
        log.error("[REQUIREMENTS] Requirements check failed")
    print("=" * 80 + "\n")
    
    return all_ok

def setup_driver(headless=False):
    
    opts = webdriver.FirefoxOptions()
    if headless:
        opts.add_argument("--headless")
    
    # Create a temporary profile for isolation
    profile, profile_dir = create_temp_profile()
    fp = pick_fingerprint()
    profile.set_preference("browser.cache.disk.enable", False)
    profile.set_preference("browser.cache.memory.enable", False)
    profile.set_preference("browser.cache.offline.enable", False)
    profile.set_preference("network.http.use-cache", False)
    profile.set_preference("dom.serviceWorkers.enabled", False)
    profile.set_preference("dom.indexedDB.enabled", False)
    profile.set_preference("dom.storage.enabled", False)
    profile.set_preference("places.history.enabled", False)
    profile.set_preference("browser.formfill.enable", False)
    profile.set_preference("signon.rememberSignons", False)
    # Avoid persistence even within a long run (we also delete the profile directory on shutdown).
    profile.set_preference("network.cookie.lifetimePolicy", 2)  # expire at end of session
    profile.set_preference("privacy.sanitize.sanitizeOnShutdown", True)
    profile.set_preference("privacy.clearOnShutdown.cookies", True)
    profile.set_preference("privacy.clearOnShutdown.cache", True)
    profile.set_preference("privacy.clearOnShutdown.offlineApps", True)
    profile.set_preference("privacy.clearOnShutdown.history", True)
    
    # Block images and CSS for performance
    profile.set_preference("permissions.default.image", 2)  # Block images
    profile.set_preference("permissions.default.stylesheet", 2)  # Block CSS
    profile.set_preference("browser.display.use_document_fonts", 0)
    profile.set_preference("gfx.downloadable_fonts.enabled", False)
    profile.set_preference("webgl.disabled", True)
    profile.set_preference("media.autoplay.default", 1)
    profile.set_preference("media.autoplay.blocking_policy", 2)
    profile.set_preference("media.peerconnection.enabled", False)
    profile.set_preference("media.peerconnection.ice.default_address_only", True)
    profile.set_preference("media.peerconnection.ice.no_host", True)
    profile.set_preference("media.peerconnection.ice.proxy_only", True)
    # Reduce obvious webdriver signals (best-effort; some may be overridden by geckodriver).
    profile.set_preference("dom.webdriver.enabled", False)
    
    # Disable speculative connections and prefetch
    profile.set_preference("network.prefetch-next", False)
    profile.set_preference("network.dns.disablePrefetch", True)
    profile.set_preference("network.dns.disablePrefetchFromHTTPS", True)
    profile.set_preference("network.predictor.enabled", False)
    profile.set_preference("network.predictor.enable-prefetch", False)
    profile.set_preference("network.http.speculative-parallel-limit", 0)
    profile.set_preference("browser.urlbar.speculativeConnect.enabled", False)
    
    # Fingerprint variation (per browser instance)
    profile.set_preference("general.useragent.override", fp["ua"])
    profile.set_preference("intl.accept_languages", fp["lang"])
    profile.set_preference("intl.timezone.override", fp["tz"])
    
    # Disable notifications and popups
    profile.set_preference("dom.webnotifications.enabled", False)
    profile.set_preference("dom.push.enabled", False)
    
    # Configure Tor SOCKS5 proxy (only if Tor is enabled)
    socks_port = int(TOR_SOCKS_PORT or TOR_PROXY_PORT or 0)
    if socks_port > 0:
        profile.set_preference("network.proxy.type", 1)  # Manual proxy configuration
        profile.set_preference("network.proxy.socks", "127.0.0.1")
        profile.set_preference("network.proxy.socks_port", socks_port)
        profile.set_preference("network.proxy.socks_version", 5)
        profile.set_preference("network.proxy.socks_remote_dns", True)  # Route DNS through Tor
        port_name = "Tor Browser" if socks_port == 9150 else "Tor service"
        log.info(f"[TOR_CONFIG] Using Tor proxy on port {socks_port} ({port_name})")
    else:
        profile.set_preference("network.proxy.type", 0)  # Direct connection
        log.info("[TOR_CONFIG] Tor proxy disabled; using direct connection")
    
    # Update preferences
    opts.profile = profile
    
    # Set page load strategy to "eager" to avoid hanging on slow-loading resources
    opts.set_capability("pageLoadStrategy", "eager")
    
    # Find and set Firefox binary path
    firefox_binary = find_firefox_binary()
    if firefox_binary:
        # In Selenium 4, use binary_location instead of FirefoxBinary
        opts.binary_location = firefox_binary
        log.info(f"[FIREFOX] Using Firefox binary: {firefox_binary}")
    else:
        log.error("[FIREFOX] Firefox binary not found in common locations")
        log.error("[FIREFOX] Please install Firefox or set FIREFOX_BINARY environment variable")
        log.error("[FIREFOX] Example: set FIREFOX_BINARY=C:\\Program Files\\Mozilla Firefox\\firefox.exe")
        raise RuntimeError(
            "Firefox binary not found. Please:\n"
            "1. Install Firefox from https://www.mozilla.org/firefox/\n"
            "2. Or install Tor Browser (includes Firefox)\n"
            "3. Or set FIREFOX_BINARY environment variable to Firefox executable path"
        )
    
    # Check if shutdown was requested before creating new driver
    if _shutdown_requested.is_set():
        cleanup_temp_profile(profile_dir)
        raise RuntimeError("Shutdown requested, cannot create new Firefox/Tor session")
    
    try:
        drv = webdriver.Firefox(service=Service(_get_geckodriver_path()), options=opts)
    except Exception:
        cleanup_temp_profile(profile_dir)
        raise
    drv.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    drv._profile_dir = str(profile_dir)
    drv._fingerprint = fp
    try:
        w, h = fp["viewport"]
        # Small jitter prevents a single fixed size across restarts.
        w = int(w + random.randint(-8, 8))
        h = int(h + random.randint(-8, 8))
        drv.set_window_size(w, h)
    except Exception:
        pass
    # Best-effort cleanup on startup (fresh profile already, but this keeps behavior consistent).
    try:
        clear_browser_storage(drv)
    except Exception:
        pass
    
    # Register driver for cleanup on shutdown
    register_driver(drv)
    
    # Track Firefox/geckodriver process IDs for this pipeline run
    try:
        pids = set()
        # Get geckodriver process ID
        if hasattr(drv, 'service') and hasattr(drv.service, 'process'):
            geckodriver_pid = drv.service.process.pid
            if geckodriver_pid:
                pids.add(geckodriver_pid)
                log.debug(f"[PID_TRACKER] Found geckodriver PID: {geckodriver_pid}")
                
                # Get all descendant processes (Firefox instances)
                if psutil:
                    try:
                        parent = psutil.Process(geckodriver_pid)
                        for child in parent.children(recursive=True):
                            pids.add(child.pid)
                            log.debug(f"[PID_TRACKER] Found Firefox/Tor process PID: {child.pid}")
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
        
        if pids:
            # Track these PIDs so we only kill our own Firefox/geckodriver processes
            with _tracked_pids_lock:
                _tracked_firefox_pids.update(pids)
            log.debug(f"[PID_TRACKER] Tracked Firefox/geckodriver PIDs: {sorted(pids)}")
            if save_firefox_pids:
                save_firefox_pids("Argentina", REPO_ROOT, pids)
    except Exception as e:
        log.debug(f"[PID_TRACKER] Could not track Firefox/geckodriver PIDs: {e}")
    
    return drv

def is_login_page(driver) -> bool:
    """Check if current page is a login page"""
    try:
        return bool(driver.find_elements(By.ID, "usuario")) and bool(driver.find_elements(By.ID, "clave"))
    except Exception:
        return False

def wait_for_login_page_to_clear(driver, max_wait_seconds=120, check_interval=5):
    """
    Wait for login page to clear. Checks every check_interval seconds.
    Returns True if login page cleared, False if still showing after max_wait_seconds.
    """
    log.info(f"[LOGIN_WAIT] Login page detected. Waiting up to {max_wait_seconds}s for it to clear (checking every {check_interval}s)...")
    
    start_time = time.time()
    checks = 0
    
    while time.time() - start_time < max_wait_seconds:
        # Check for shutdown
        if _shutdown_requested.is_set():
            log.warning("[LOGIN_WAIT] Shutdown requested during login wait")
            return False
        
        checks += 1
        elapsed = int(time.time() - start_time)
        
        try:
            # Refresh the page to check if login is still there
            driver.refresh()
            time.sleep(2)  # Wait for page to load after refresh
            
            if not is_login_page(driver):
                log.info(f"[LOGIN_WAIT] Login page cleared after {elapsed}s ({checks} checks)")
                return True
            
            log.info(f"[LOGIN_WAIT] Still showing login page after {elapsed}s... (check {checks})")
            
        except Exception as e:
            log.warning(f"[LOGIN_WAIT] Error checking login page: {e}")
        
        # Wait before next check
        time.sleep(check_interval)
    
    log.warning(f"[LOGIN_WAIT] Login page still present after {max_wait_seconds}s. Will restart browser.")
    return False

def wait_for_user_resume():
    """Wait for user to press Enter key after changing VPN location"""
    log.warning("[CAPTCHA_PAUSE] Session closed.")
    log.info("[CAPTCHA_PAUSE] Please change your VPN location and press ENTER to resume...")
    try:
        input()  # Wait for Enter key press
        log.info("[CAPTCHA_PAUSE] Resuming with new session...")
    except (EOFError, KeyboardInterrupt):
        log.warning("[CAPTCHA_PAUSE] Input interrupted, exiting...")
        _shutdown_requested.set()
        raise

# ====== SEARCH / RESULTS ======

def navigate_to_products_page(driver):
    """Navigate to products page once. Called only when driver is created."""
    log.info(f"[NAVIGATE] Navigating to products page: {PRODUCTS_URL}")
    
    # Navigation watchdog: if driver.get() hangs, we'll detect it
    navigation_start = time.time()
    navigation_timeout = PAGE_LOAD_TIMEOUT + 10  # Give extra time beyond page load timeout
    
    # Retry navigation with connection check if it fails
    max_nav_retries = 3
    nav_retry_count = 0
    navigation_success = False
    
    while nav_retry_count < max_nav_retries and not navigation_success:
        try:
            log.info(f"[NAVIGATE] Calling driver.get() with URL: {PRODUCTS_URL} (attempt {nav_retry_count + 1}/{max_nav_retries})")
            log.info(f"[NAVIGATE] Navigation timeout: {navigation_timeout}s")
            
            # Use threading to detect if navigation hangs
            navigation_complete = threading.Event()
            navigation_error = [None]
            
            def do_navigation():
                try:
                    driver.get(PRODUCTS_URL)
                    navigation_complete.set()
                except Exception as e:
                    navigation_error[0] = e
                    navigation_complete.set()
            
            nav_thread = threading.Thread(target=do_navigation, daemon=True)
            nav_thread.start()
            
            # Wait for navigation with timeout
            if navigation_complete.wait(timeout=navigation_timeout):
                # PERFORMANCE FIX: Clean up thread object to prevent accumulation
                nav_thread.join(timeout=1.0)
                if navigation_error[0]:
                    raise navigation_error[0]
                elapsed = time.time() - navigation_start
                log.info(f"[NAVIGATE] driver.get() completed in {elapsed:.2f}s. Current URL: {driver.current_url}")
                navigation_success = True
            else:
                # Navigation hung - log and retry
                elapsed = time.time() - navigation_start
                log.warning(f"[NAVIGATE] Navigation hung after {elapsed:.2f}s (timeout: {navigation_timeout}s)")
                nav_retry_count += 1
                if nav_retry_count < max_nav_retries:
                    log.warning(f"[NAVIGATE] Waiting 120 seconds before retry {nav_retry_count + 1}/{max_nav_retries}...")
                    time.sleep(120)  # Wait 2 minutes before retry
                    # Check connection before retrying
                    if not check_connection_with_retry(driver, PRODUCTS_URL, max_retries=1):
                        log.warning(f"[NAVIGATE] Connection check failed, will retry navigation anyway...")
                else:
                    raise TimeoutException(f"Navigation to {PRODUCTS_URL} hung after {max_nav_retries} attempts")
        except TimeoutException:
            if nav_retry_count >= max_nav_retries - 1:
                raise  # Re-raise on last attempt
            nav_retry_count += 1
            log.warning(f"[NAVIGATE] Navigation failed, waiting 120 seconds before retry {nav_retry_count + 1}/{max_nav_retries}...")
            time.sleep(120)  # Wait 2 minutes before retry
            # Check connection before retrying
            if not check_connection_with_retry(driver, PRODUCTS_URL, max_retries=1):
                log.warning(f"[NAVIGATE] Connection check failed, will retry navigation anyway...")
        except Exception as e:
            # Other errors - retry if not last attempt
            nav_retry_count += 1
            if nav_retry_count < max_nav_retries:
                log.warning(f"[NAVIGATE] Navigation error: {e}, waiting 120 seconds before retry {nav_retry_count + 1}/{max_nav_retries}...")
                time.sleep(120)  # Wait 2 minutes before retry
                # Check connection before retrying
                if not check_connection_with_retry(driver, PRODUCTS_URL, max_retries=1):
                    log.warning(f"[NAVIGATE] Connection check failed, will retry navigation anyway...")
            else:
                raise  # Re-raise on last attempt
    
    # Only proceed if navigation was successful
    if not navigation_success:
        raise TimeoutException(f"Failed to navigate to {PRODUCTS_URL} after {max_nav_retries} attempts")
    
    try:
        log.info(f"[NAVIGATE] Page title: {driver.title}")
        
        # Check if driver is still valid before accessing current_url
        try:
            log.info(f"[NAVIGATE] After 2s wait. Current URL: {driver.current_url}")
        except (InvalidSessionIdException, WebDriverException) as session_error:
            log.warning(f"[NAVIGATE] Driver session closed (likely during cleanup): {session_error}")
            return  # Driver was closed, skip remaining navigation checks
        
        # Wait for document ready state to ensure JavaScript has executed
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            log.info("[NAVIGATE] Document ready state: complete")
        except Exception as e:
            log.warning(f"[NAVIGATE] Document ready state check failed: {e}, continuing anyway")
    except (InvalidSessionIdException, WebDriverException) as session_error:
        # Driver session was closed (likely during worker cleanup when queue is empty)
        log.debug(f"[NAVIGATE] Driver session closed during navigation: {session_error}")
        return  # Exit gracefully, don't log as error
    except Exception as e:
        log.error(f"[NAVIGATE] Error after navigation: {e}")
        import traceback
        log.error(f"[NAVIGATE] Traceback: {traceback.format_exc()}")
        return  # Exit if there was an error
    
    # Check for login page after navigation - wait for it to clear
    try:
        if is_login_page(driver):
            log.warning("[NAVIGATE] Login page detected after navigating to products URL")
            # Wait for login page to clear (checks every 5s, waits up to 2 minutes)
            login_cleared = wait_for_login_page_to_clear(driver, max_wait_seconds=INSTANCE_RESTART_WAIT_SECONDS)
    except (InvalidSessionIdException, WebDriverException) as session_error:
        # Driver was closed during cleanup (likely when queue is empty)
        log.debug(f"[NAVIGATE] Driver session closed during login check: {session_error}")
        return  # Exit gracefully
        if not login_cleared:
            log.error("[NAVIGATE] Login page still present after waiting")
            raise RuntimeError("Login page detected after navigating to products URL and did not clear")
    
    # Wait for search form to be present
    log.info(f"[NAVIGATE] Waiting for search form (timeout: {WAIT_SEARCH_FORM}s)...")
    try:
        form = WebDriverWait(driver, WAIT_SEARCH_FORM).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
        )
        log.info("[NAVIGATE] Search form found - products page ready")
    except TimeoutException:
        # Check again for login page in case it appeared during wait
        if is_login_page(driver):
            log.warning("[NAVIGATE] Login page detected while waiting for search form")
            # Wait for login page to clear
            login_cleared = wait_for_login_page_to_clear(driver, max_wait_seconds=INSTANCE_RESTART_WAIT_SECONDS)
            if not login_cleared:
                log.error("[NAVIGATE] Login page still present after waiting")
                raise RuntimeError("Login page detected while waiting for search form and did not clear")
            # Retry waiting for form after login cleared
            form = WebDriverWait(driver, WAIT_SEARCH_FORM).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
            )
        log.error(f"[NAVIGATE] Form not found after {WAIT_SEARCH_FORM}s. Current URL: {driver.current_url}")
        log.error(f"[NAVIGATE] Page title: {driver.title}")
        log.error(f"[NAVIGATE] Page source snippet: {driver.page_source[:500]}")
        raise

def is_products_search_url(url: str) -> bool:
    """
    True if `url` is the Alfabeta products search/listing page (base URL), not a product detail page.

    Examples:
      - https://www.alfabeta.net/precio         -> True
      - https://www.alfabeta.net/precio?x=y     -> True
      - https://www.alfabeta.net/precio/abc.html -> False
    """
    base = (PRODUCTS_URL or "").lower().rstrip("/")
    u = (url or "").lower()
    if not base or not u.startswith(base):
        return False
    rest = u[len(base):]
    return rest == "" or rest.startswith("?") or rest.startswith("#")

def search_product_on_page(driver, product_term: str):
    """Search for product using the existing search form (no navigation - assumes already on products page)"""
    log.info(f"[SEARCH] Searching for product: {product_term}")
    
    # Check if we're still on the products search page (not a product detail page), if not navigate back
    current_url = driver.current_url
    if not is_products_search_url(current_url):
        log.warning(f"[SEARCH] Not on products search page (current: {current_url}), navigating back to {PRODUCTS_URL}...")
        navigate_to_products_page(driver)
    
    # Wait for search form to be present (might be refreshing after previous search)
    log.info(f"[SEARCH] Waiting for search form (timeout: {WAIT_SEARCH_FORM}s)...")
    try:
        form = WebDriverWait(driver, WAIT_SEARCH_FORM).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
        )
        log.info("[SEARCH] Search form found")
    except TimeoutException:
        # Form not found, try navigating again
        log.warning("[SEARCH] Search form not found, navigating to products page...")
        navigate_to_products_page(driver)
        form = WebDriverWait(driver, WAIT_SEARCH_FORM).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
        )
    
    log.info(f"[SEARCH] Entering search term: {product_term}")
    box = form.find_element(By.NAME, "patron")
    box.clear()
    box.send_keys(product_term)
    box.send_keys(Keys.ENTER)
    # Rate limiting: pause after form submission to avoid overwhelming server
    rate_limit_pause()
    log.info(f"[SEARCH] Search submitted, waiting for results (timeout: {WAIT_SEARCH_RESULTS}s)...")
    
    try:
        # Wait for search results to appear
        WebDriverWait(driver, WAIT_SEARCH_RESULTS).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "a.rprod, form[name^='pr']")
        )
        log.info("[SEARCH] Search results loaded successfully")
        
        # Verify results are still present and page is stable
        results = driver.find_elements(By.CSS_SELECTOR, "a.rprod, form[name^='pr']")
        if results:
            log.info(f"[SEARCH] Confirmed {len(results)} result elements are present")
        else:
            log.warning("[SEARCH] Results disappeared after wait, may need more time")
    except TimeoutException:
        log.error(f"[SEARCH] Search results not found after {WAIT_SEARCH_RESULTS}s. Current URL: {driver.current_url}")
        log.error(f"[SEARCH] Page title: {driver.title}")
        raise

def enumerate_pairs(driver) -> List[Dict[str, Any]]:
    out = []
    for a in driver.find_elements(By.CSS_SELECTOR, "a.rprod"):
        # Check if element is still present and accessible before fetching values
        try:
            # Verify element is still attached to DOM
            _ = a.is_displayed()
            prod_txt = normalize_ws(a.text) or ""
            href = a.get_attribute("href") or ""
        except Exception:
            # Element may be stale or not accessible, skip it
            continue
        
        m = re.search(r"document\.(pr\d+)\.submit", href)
        pr_form = m.group(1) if m else None
        comp_txt = ""
        
        # Check for company label before fetching
        rlab_elements = a.find_elements(By.XPATH, "following-sibling::a[contains(@class,'rlab')][1]")
        if rlab_elements:
            try:
                rlab = rlab_elements[0]
                _ = rlab.is_displayed()  # Check presence before fetching
                comp_txt = normalize_ws(rlab.text) or ""
            except Exception:
                pass
        out.append({"prod": prod_txt, "comp": comp_txt, "pr_form": pr_form})
    return out

def open_exact_pair(driver, product: str, company: str) -> bool:
    """Open exact product-company pair from search results"""
    # Fail fast if driver is already dead
    if not is_driver_alive(driver):
        raise WebDriverException("Driver is not alive before submit (likely crashed)")
    
    rows = enumerate_pairs(driver)
    matches = [r for r in rows if nk(r["prod"]) == nk(product) and nk(r["comp"]) == nk(company)]
    if not matches:
        return False
    pr = matches[0]["pr_form"]
    if not pr:
        return False
    
    # Check driver alive again before execute_script
    if not is_driver_alive(driver):
        raise WebDriverException("Driver is not alive before execute_script (likely crashed)")
    
    prev_url = driver.current_url
    base = (PRODUCTS_URL or "").lower().rstrip("/")
    driver.execute_script(f"if (document.{pr}) document.{pr}.submit();")
    
    # Rate limiting: pause after opening product page
    rate_limit_pause()
    
    # Wait for product page to load with threading-based timeout to prevent hanging
    page_load_start = time.time()
    page_load_complete = threading.Event()
    page_load_error = [None]
    
    def wait_for_page_load():
        """Wait for product page to load in a separate thread"""
        try:
            WebDriverWait(driver, WAIT_PAGE_LOAD).until(
                lambda d: (
                    (d.current_url != prev_url)
                    and ((not base) or d.current_url.lower().startswith(base + "/"))
                    and (
                        d.find_elements(By.CSS_SELECTOR, "tr.lproducto span.tproducto")
                        or d.find_elements(By.CSS_SELECTOR, "td.dproducto > table.presentacion")
                    )
                )
            )
            page_load_complete.set()
        except Exception as e:
            page_load_error[0] = e
            page_load_complete.set()
    
    # Start waiting in a separate thread
    wait_thread = threading.Thread(target=wait_for_page_load, daemon=True)
    wait_thread.start()
    
    # Wait for completion with timeout (add 5 seconds buffer to WAIT_PAGE_LOAD)
    timeout_with_buffer = WAIT_PAGE_LOAD + 5
    if page_load_complete.wait(timeout=timeout_with_buffer):
        # PERFORMANCE FIX: Clean up thread object
        wait_thread.join(timeout=1.0)
        if page_load_error[0]:
            # Re-raise the error from the wait thread
            raise page_load_error[0]
    else:
        # Page load hung - check if thread is still alive (indicates WebDriverWait hung)
        if wait_thread.is_alive():
            log.error(f"[OPEN_PAIR] WebDriverWait thread is still alive after timeout - driver may be hung")
        elapsed = time.time() - page_load_start
        log.error(f"[OPEN_PAIR] Page load hung after {elapsed:.2f}s (timeout: {WAIT_PAGE_LOAD}s)")
        raise TimeoutException(f"Page load hung after {elapsed:.2f}s")
    
    # Wait for document ready state with threading-based timeout
    ready_state_complete = threading.Event()
    ready_state_error = [None]
    
    def wait_for_ready_state():
        """Wait for document ready state in a separate thread"""
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            log.info("[OPEN_PAIR] Document ready state: complete")
            ready_state_complete.set()
        except Exception as e:
            ready_state_error[0] = e
            ready_state_complete.set()
    
    # Start waiting in a separate thread
    ready_thread = threading.Thread(target=wait_for_ready_state, daemon=True)
    ready_thread.start()
    
    # Wait for completion with timeout (15 seconds buffer)
    if ready_state_complete.wait(timeout=15):
        # PERFORMANCE FIX: Clean up thread object
        ready_thread.join(timeout=1.0)
        if ready_state_error[0]:
            # Log warning but don't fail - this is not critical
            log.warning(f"[OPEN_PAIR] Document ready state check failed: {ready_state_error[0]}, continuing anyway")
    else:
        # Ready state check hung - log warning but continue
        log.warning("[OPEN_PAIR] Document ready state check hung, continuing anyway")
    
    # Verify key elements are present
    try:
        pres_elements = driver.find_elements(By.CSS_SELECTOR, "td.dproducto > table.presentacion")
        if pres_elements:
            log.info(f"[OPEN_PAIR] Confirmed {len(pres_elements)} presentation table(s) are present")
        else:
            log.warning("[OPEN_PAIR] No presentation tables found, content may not be fully loaded")
    except Exception as e:
        log.warning(f"[OPEN_PAIR] Could not verify presentation tables: {e}")

    # Guard against false-success where navigation never happened and we'd re-extract the prior product page
    try:
        loaded_name = normalize_ws((driver.find_element(By.CSS_SELECTOR, "tr.lproducto span.tproducto").text or ""))
        if loaded_name and nk(loaded_name) != nk(product):
            log.warning(
                f"[OPEN_PAIR] Product title mismatch after open (expected: {product} | got: {loaded_name}). URL: {driver.current_url}"
            )
            raise TimeoutException("Opened page does not match requested product")
    except TimeoutException:
        raise
    except Exception:
        # If we can't read the title, don't fail hard; extraction will handle missing elements.
        pass
    
    return True

# ====== PRODUCT PAGE PARSING ======

def get_text_safe(root, css, retry_count=2):
    """Safely get text from element, checking presence before fetching values with retry logic"""
    for attempt in range(retry_count + 1):
        try:
            # Check if element exists before fetching
            elements = root.find_elements(By.CSS_SELECTOR, css)
            if not elements:
                return None
            
            el = elements[0]
            # Verify element is still attached and accessible
            try:
                _ = el.is_displayed()
            except (StaleElementReferenceException, InvalidSessionIdException, WebDriverException):
                if attempt < retry_count:
                    # Re-find elements if stale
                    elements = root.find_elements(By.CSS_SELECTOR, css)
                    if not elements:
                        return None
                    el = elements[0]
                else:
                    return None
            
            # Now fetch values
            txt = el.get_attribute("innerText")
            if not txt:
                txt = el.get_attribute("innerHTML")
            return normalize_ws(txt)
        except (StaleElementReferenceException, InvalidSessionIdException, WebDriverException) as e:
            if attempt < retry_count:
                # Retry immediately (I/O operations provide natural delays)
                continue
            return None
        except Exception:
            return None
    return None

def collect_coverage(pres_el) -> Dict[str, Any]:
    """Robust coverage parser: normalizes payer keys and reads innerHTML to catch AF/OS in <b> tags.
    Checks element presence before fetching values with better stale element handling."""
    cov: Dict[str, Any] = {}
    
    # Check if coverage table exists before accessing
    try:
        cob_elements = pres_el.find_elements(By.CSS_SELECTOR, "table.coberturas")
        if not cob_elements:
            return cov
        
        cob = cob_elements[0]
        _ = cob.is_displayed()  # Verify element is accessible
    except (StaleElementReferenceException, InvalidSessionIdException, WebDriverException):
        return cov
    except Exception:
        return cov

    current_payer = None
    try:
        tr_elements = cob.find_elements(By.CSS_SELECTOR, "tr")
    except (StaleElementReferenceException, InvalidSessionIdException, WebDriverException):
        return cov
    except Exception:
        return cov
    
    for tr in tr_elements:
        try:
            # Payer name (fallback to innerHTML) - check presence before fetching
            payer_elements = tr.find_elements(By.CSS_SELECTOR, "td.obrasn")
            if payer_elements:
                try:
                    payer_el = payer_elements[0]
                    _ = payer_el.is_displayed()  # Check presence before fetching
                    payer_text = normalize_ws(payer_el.get_attribute("innerText")) or normalize_ws(payer_el.get_attribute("innerHTML"))
                    if payer_text:
                        current_payer = strip_accents(payer_text).upper()
                        cov.setdefault(current_payer, {})
                except (StaleElementReferenceException, InvalidSessionIdException, WebDriverException):
                    continue  # Skip this row if element is stale
                except Exception:
                    pass

            # Detail/description - check presence before fetching
            detail_elements = tr.find_elements(By.CSS_SELECTOR, "td.obrasd")
            if detail_elements:
                try:
                    detail_el = detail_elements[0]
                    _ = detail_el.is_displayed()  # Check presence before fetching
                    detail = normalize_ws(detail_el.get_attribute("innerText"))
                    if current_payer and detail:
                        cov[current_payer]["detail"] = detail
                except (StaleElementReferenceException, InvalidSessionIdException, WebDriverException):
                    continue  # Skip this row if element is stale
                except Exception:
                    pass

            # Amounts: check both left/right amount cells, use innerText first
            for sel in ("td.importesi", "td.importesd"):
                amount_elements = tr.find_elements(By.CSS_SELECTOR, sel)
                if amount_elements:
                    try:
                        amount_el = amount_elements[0]
                        _ = amount_el.is_displayed()  # Check presence before fetching
                        txt = amount_el.get_attribute("innerText")
                        if not txt:
                            txt = amount_el.get_attribute("innerHTML")
                            txt = re.sub(r'<[^>]*>', '', txt)
                        for tag, amt in re.findall(r"(AF|OS)[^<]*?[\$]?([\d\.,]+)", txt or "", flags=re.I):
                            val = ar_money_to_float(amt)
                            if val is not None and current_payer:
                                cov[current_payer][tag.upper()] = val
                    except (StaleElementReferenceException, InvalidSessionIdException, WebDriverException):
                        continue  # Skip this cell if element is stale
                    except Exception:
                        pass
        except (StaleElementReferenceException, InvalidSessionIdException, WebDriverException):
            continue  # Skip this row if it becomes stale
        except Exception:
            continue  # Skip this row on other errors
    return cov

def extract_rows(driver, in_company, in_product, max_retries=2):
    """
    Extract product data from the loaded page with retry logic and better error handling.
    
    Args:
        driver: Selenium WebDriver instance
        in_company: Input company name
        in_product: Input product name
        max_retries: Maximum number of retry attempts if extraction fails
    
    Returns:
        List of extracted row dictionaries
    """
    # Check shutdown before starting extraction
    if _shutdown_requested.is_set():
        return []
    
    for attempt in range(max_retries + 1):
        try:
            # Ensure page is fully loaded before parsing
            try:
                # Wait for document ready state
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
            except (InvalidSessionIdException, WebDriverException) as e:
                log.error(f"[EXTRACT] Driver error during page load check: {e}")
                raise  # Re-raise driver errors immediately
            except Exception as e:
                if attempt < max_retries:
                    log.warning(f"[EXTRACT] Document ready state check failed (attempt {attempt + 1}/{max_retries + 1}): {e}, retrying...")
                    continue
                else:
                    log.warning(f"[EXTRACT] Document ready state check failed after {max_retries + 1} attempts: {e}, continuing anyway")
                    # Continue to extraction even if ready state check failed
                    pass
            
            # Check shutdown again after page load
            if _shutdown_requested.is_set():
                return []
            
            # Validate that we're on a product page before extracting
            try:
                # Check for key product page elements
                product_elements = driver.find_elements(By.CSS_SELECTOR, "tr.lproducto span.tproducto, td.dproducto > table.presentacion")
                if not product_elements and attempt < max_retries:
                    log.warning(f"[EXTRACT] Product page elements not found (attempt {attempt + 1}/{max_retries + 1}), waiting and retrying...")
                    if interruptible_sleep(2):
                        return []
                    continue
            except (InvalidSessionIdException, WebDriverException) as e:
                log.error(f"[EXTRACT] Driver error during page validation: {e}")
                raise
            except Exception as e:
                if attempt < max_retries:
                    log.warning(f"[EXTRACT] Page validation failed (attempt {attempt + 1}/{max_retries + 1}): {e}, retrying...")
                    if interruptible_sleep(1):
                        return []
                    continue
            
            # Header/meta from the product page - get_text_safe now checks presence before fetching
            try:
                active = get_text_safe(driver, "tr.sproducto td.textoe i")
                therap = get_text_safe(driver, "tr.sproducto td.textor i")
                comp = get_text_safe(driver, "tr.lproducto td.textor .defecto") or \
                       get_text_safe(driver, "td.textoe b")
                pname = get_text_safe(driver, "tr.lproducto span.tproducto")
            except (InvalidSessionIdException, WebDriverException) as e:
                log.error(f"[EXTRACT] Driver error while extracting header data: {e}")
                raise
            except Exception as e:
                log.warning(f"[EXTRACT] Error extracting header data: {e}")
                active = None
                therap = None
                comp = None
                pname = None

            rows: List[Dict[str, Any]] = []
            # Check if presentation elements exist before iterating
            try:
                pres = driver.find_elements(By.CSS_SELECTOR, "td.dproducto > table.presentacion")
                log.info(f"[EXTRACT] Found {len(pres)} presentation table(s) for {in_company} | {in_product}")
            except (InvalidSessionIdException, WebDriverException) as e:
                log.error(f"[EXTRACT] Driver error while finding presentation tables: {e}")
                raise
            except Exception as e:
                log.warning(f"[EXTRACT] Error finding presentation tables: {e}")
                pres = []
            
            # Extract data from each presentation table
            for idx, p in enumerate(pres):
                # Check shutdown during extraction loop
                if _shutdown_requested.is_set():
                    return []
                
                try:
                    # Verify element is still accessible before processing
                    _ = p.is_displayed()
                except (StaleElementReferenceException, InvalidSessionIdException, WebDriverException) as e:
                    log.warning(f"[EXTRACT] Element {idx} is stale or driver error: {e}, skipping...")
                    continue
                except Exception as e:
                    log.warning(f"[EXTRACT] Element {idx} may be stale: {e}, skipping...")
                    continue
                
                try:
                    desc = get_text_safe(p, "td.tddesc")
                    price = get_text_safe(p, "td.tdprecio")
                    datev = get_text_safe(p, "td.tdfecha")
                    import_status = get_text_safe(p, "td.import")
                    cov = collect_coverage(p)

                    rows.append({
                        "input_company": in_company,
                        "input_product_name": in_product,
                        "company": comp,
                        "product_name": pname,
                        "active_ingredient": active,
                        "therapeutic_class": therap,
                        "description": desc,
                        "price_ars": ar_money_to_float(price or ""),
                        "date": parse_date(datev or ""),
                        "scraped_at": ts(),
                        "SIFAR_detail": (cov.get("SIFAR") or {}).get("detail"),
                        "PAMI_AF": (cov.get("PAMI") or {}).get("AF"),
                        "PAMI_OS": (cov.get("PAMI") or {}).get("OS"),
                        "IOMA_detail": (cov.get("IOMA") or {}).get("detail"),
                        "IOMA_AF": (cov.get("IOMA") or {}).get("AF"),
                        "IOMA_OS": (cov.get("IOMA") or {}).get("OS"),
                        "import_status": import_status,
                        "coverage_json": json.dumps(cov, ensure_ascii=False)
                    })
                except (StaleElementReferenceException, InvalidSessionIdException, WebDriverException) as e:
                    log.warning(f"[EXTRACT] Driver error while extracting row {idx}: {e}, skipping this row...")
                    continue
                except Exception as e:
                    log.warning(f"[EXTRACT] Error extracting row {idx}: {e}, skipping this row...")
                    continue

            # If we got rows, return them (success)
            if rows:
                log.info(f"[EXTRACT] Successfully extracted {len(rows)} row(s) for {in_company} | {in_product}")
                return rows
            
            # If no rows but we have product name, create fallback row
            if pname or comp:
                log.info(f"[EXTRACT] No presentation rows found but product info exists, creating fallback row for {in_company} | {in_product}")
                rows.append({
                    "input_company": in_company,
                    "input_product_name": in_product,
                    "company": comp,
                    "product_name": pname,
                    "active_ingredient": active,
                    "therapeutic_class": therap,
                    "description": None,
                    "price_ars": None,
                    "date": None,
                    "scraped_at": ts(),
                    "SIFAR_detail": None, "PAMI_AF": None, "PAMI_OS": None, "IOMA_detail": None, "IOMA_AF": None, "IOMA_OS": None,
                    "import_status": None,
                    "coverage_json": "{}"
                })
                return rows
            
            # If we got here and no rows, retry if we have attempts left
            if attempt < max_retries:
                log.warning(f"[EXTRACT] No data extracted (attempt {attempt + 1}/{max_retries + 1}) for {in_company} | {in_product}, retrying...")
                if interruptible_sleep(2):
                    return []
                continue
            
            # Final attempt failed - return empty list
            log.warning(f"[EXTRACT] Failed to extract data after {max_retries + 1} attempts for {in_company} | {in_product}")
            return []
            
        except (InvalidSessionIdException, WebDriverException) as e:
            # Driver errors should not be retried - they indicate session is dead
            log.error(f"[EXTRACT] Driver error during extraction for {in_company} | {in_product}: {e}")
            raise
        except Exception as e:
            if attempt < max_retries:
                log.warning(f"[EXTRACT] Extraction error (attempt {attempt + 1}/{max_retries + 1}) for {in_company} | {in_product}: {e}, retrying...")
                if interruptible_sleep(2):
                    return []
                continue
            else:
                log.error(f"[EXTRACT] Extraction failed after {max_retries + 1} attempts for {in_company} | {in_product}: {e}")
                return []
    
    # Should never reach here, but return empty list as fallback
    return []

# ====== BROWSER RESTART LOGIC ======

# Constants for browser management
PRODUCTS_PER_RESTART = max(1, SELENIUM_PRODUCTS_PER_RESTART)
SLOW_PAGE_WINDOW_SIZE = max(1, SLOW_PAGE_MEDIAN_WINDOW)
SLOW_PAGE_MIN_SAMPLES_COUNT = max(1, min(SLOW_PAGE_MIN_SAMPLES, SLOW_PAGE_WINDOW_SIZE))
SLOW_PAGE_THRESHOLD_SECONDS = max(0.0, SLOW_PAGE_MEDIAN_THRESHOLD_SECONDS)
INSTANCE_RESTART_WAIT_SECONDS = 84  # Reduced by 30%
LOGIN_CAPTCHA_WAIT_SECONDS = 126  # Reduced by 30%

# ====== CAPTCHA DETECTION ======

def is_captcha_page(driver) -> bool:
    """Check if current page is a captcha page.
    Skips check if driver is on about:blank to avoid hanging.
    """
    try:
        # Skip captcha check on about:blank pages (can hang on page_source access)
        current_url = driver.current_url.lower()
        if current_url.startswith("about:") or current_url == "data:":
            return False
        
        page_source_lower = driver.page_source.lower()
        url_lower = current_url
        
        captcha_indicators = [
            "captcha",
            "recaptcha",
            "cloudflare",
            "challenge",
            "verify you are human",
            "access denied",
            "checking your browser"
        ]
        
        for indicator in captcha_indicators:
            if indicator in page_source_lower or indicator in url_lower:
                return True
        
        return False
    except Exception:
        return False

# ====== RATE LIMITING ======

_duplicate_rate_limit_per_thread = {}  # thread_id -> last_process_time

def duplicate_rate_limit_wait(thread_id: int):
    """Wait if needed to respect rate limit for duplicates: 1 product per 10 seconds per thread (Selenium)"""
    global _duplicate_rate_limit_per_thread
    now = time.time()
    last_time = _duplicate_rate_limit_per_thread.get(thread_id, 0)
    time_since_last = now - last_time
    
    if time_since_last < DUPLICATE_RATE_LIMIT_SECONDS:
        wait_time = DUPLICATE_RATE_LIMIT_SECONDS - time_since_last
        log.info(f"[DUPLICATE_RATE_LIMIT] Thread {thread_id}: waiting {wait_time:.2f}s (1 product per {DUPLICATE_RATE_LIMIT_SECONDS}s)")
        time.sleep(wait_time)
    
    _duplicate_rate_limit_per_thread[thread_id] = time.time()

# ====== MAIN ======

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-rows", type=int, default=0, help="Maximum number of rows to process (0 = unlimited)")
    # Headless mode is always enforced for Argentina scraper
    # Removed --no-headless option to ensure all browser instances are hidden
    args = ap.parse_args()
    
    # Force headless mode always (hide all browser instances)
    args.headless = True
    
    # Log browser mode
    log.info(f"[BROWSER] Running in HEADLESS mode (all browser instances hidden)")
    
    # Check all requirements before starting
    if not check_requirements():
        log.error("[STARTUP] Requirements check failed. Please fix the issues above and try again.")
        print("\n[ERROR] Requirements check failed. Please fix the issues above and try again.")
        return 1
    
    print("\n" + "=" * 80)
    print("[STARTUP] Starting Selenium scraper...")
    print("[STARTUP] Configuration:")
    print(f"[STARTUP]   - Browser instances: {SELENIUM_THREADS}")
    socks_port = int(TOR_SOCKS_PORT or TOR_PROXY_PORT or 0)
    if socks_port > 0:
        port_name = "Tor Browser" if socks_port == 9150 else "Tor service"
        print(f"[STARTUP]   - Browser: Firefox with Tor (SOCKS5 proxy on localhost:{socks_port} - {port_name})")
    else:
        print("[STARTUP]   - Browser: Firefox (direct connection)")
    print(f"[STARTUP]   - Restart browser every: {PRODUCTS_PER_RESTART} products")
    print(f"[STARTUP]   - Slow restart: {SLOW_PAGE_RESTART_ENABLED} (median >= {SLOW_PAGE_THRESHOLD_SECONDS}s, window {SLOW_PAGE_WINDOW_SIZE}, min {SLOW_PAGE_MIN_SAMPLES_COUNT})")
    if TOR_NEWNYM_ENABLED and TOR_NEWNYM_INTERVAL_SECONDS > 0:
        print(f"[STARTUP]   - NEWNYM interval: {TOR_NEWNYM_INTERVAL_SECONDS} seconds")
    print(f"[STARTUP]   - Wait on captcha/login: {INSTANCE_RESTART_WAIT_SECONDS} seconds")
    print(f"[STARTUP]   - Per-thread rate limit: {DUPLICATE_RATE_LIMIT_SECONDS} seconds")
    print(f"[STARTUP]   - Repeat items -> API: {SKIP_REPEAT_SELENIUM_TO_API}")
    print(f"[STARTUP]   - Images/CSS: BLOCKED (for performance)")
    print("=" * 80 + "\n")
    log.info("[STARTUP] Starting Selenium scraper...")
    if socks_port > 0:
        port_name = "Tor Browser" if socks_port == 9150 else "Tor service"
        log.info(f"[STARTUP] Browser: Firefox with Tor (SOCKS5 proxy on localhost:{socks_port} - {port_name})")
    else:
        log.info("[STARTUP] Browser: Firefox (direct connection)")
    log.info(f"[STARTUP] Browser instances: {SELENIUM_THREADS}, Restart every: {PRODUCTS_PER_RESTART} products")
    log.info(f"[STARTUP] Slow restart: {SLOW_PAGE_RESTART_ENABLED} (median >= {SLOW_PAGE_THRESHOLD_SECONDS}s, window {SLOW_PAGE_WINDOW_SIZE}, min {SLOW_PAGE_MIN_SAMPLES_COUNT})")
    if TOR_NEWNYM_ENABLED and TOR_NEWNYM_INTERVAL_SECONDS > 0:
        log.info(f"[STARTUP] NEWNYM interval: {TOR_NEWNYM_INTERVAL_SECONDS} seconds")
    log.info(f"[STARTUP] Repeat items -> API: {SKIP_REPEAT_SELENIUM_TO_API}")

    # Global rotation (Surfshark + Tor NEWNYM) is coordinated across workers.

    ensure_headers()
    skip_set = combine_skip_sets()
    
    # Pre-sync: Align files with output BEFORE Selenium starts
    # This ensures Source="selenium" for all products, counts match output, progress file aligned
    log.info("[PRE-SYNC] Aligning files with output before Selenium step starts...")
    try:
        sync_files_before_selenium()
    except Exception as e:
        log.warning(f"[PRE-SYNC] Error during pre-sync: {e}")
        import traceback
        log.error(traceback.format_exc())
        # Continue anyway - pre-sync failure shouldn't stop processing
    
    # Load pending products from DB (no CSV)
    log.info("[INPUT] Loading pending products from DB (ar_product_index)...")
    try:
        pending_rows = _REPO.get_pending_products(max_loop=int(SELENIUM_MAX_RUNS), limit=200000)
    except Exception as e:
        log.error(f"[INPUT] Failed to load pending rows from DB: {e}")
        return

    eligible_count = 0
    seen_keys = set()
    for row in pending_rows:
        prod = (row.get("product") or "").strip()
        comp = (row.get("company") or "").strip()
        url = (row.get("url") or "").strip()
        if not (prod and comp and url):
            continue
        key = (nk(comp), nk(prod))
        if key in seen_keys:
            continue
        with _skip_lock:
            if key in skip_set:
                continue
        # double-check DB for already scraped
        from scraper_utils import is_product_already_scraped
        if is_product_already_scraped(comp, prod):
            continue
        seen_keys.add(key)
        eligible_count += 1
    
    log.info(f"[ELIGIBLE] Found {eligible_count} eligible rows for Selenium scraping")
    
    # Dynamic instance count: min(4, eligible_count)
    if eligible_count == 0:
        log.info(f"[ELIGIBLE] No eligible rows found. Selenium completed immediately{_pipeline_context_suffix()}.")
        # Still sync files to ensure consistency (clean up progress file and update Productlist_with_urls.csv)
        log.info("[SELENIUM] Syncing files to ensure consistency after Selenium step completion...")
        try:
            sync_files_from_output()
        except Exception as e:
            log.warning(f"[SELENIUM] Error syncing files: {e}")
        log.info("=" * 80)
        log.info(f"[SELENIUM] Selenium completed (no products to scrape){_pipeline_context_suffix()}.")
        log.info("=" * 80)
        print(f"[SELENIUM] Selenium completed successfully (no products to scrape){_pipeline_context_suffix()}", flush=True)
        return 0
    
    num_threads = min(max(1, SELENIUM_THREADS), eligible_count)
    log.info(f"[SELENIUM] Using {num_threads} browser instance(s) (min({max(1, SELENIUM_THREADS)}, {eligible_count}))")
    
    # Set total products for progress tracking
    global _total_products
    _total_products = eligible_count
    progress_msg = f"[PROGRESS] Products to scrape: {_total_products}"
    print(progress_msg, flush=True)
    log.info(progress_msg)
    
    # Main processing: read CSV row by row and process with multiple threads
    # Use a queue to coordinate reading from CSV and processing
    selenium_queue = Queue()
    threads = []
    
    # Start global rotation coordinator (Surfshark + Tor NEWNYM).
    rotation = RotationCoordinator(num_threads)
    rotation_thread = threading.Thread(target=_rotation_loop, args=(rotation,), name="RotationCoordinator", daemon=True)
    rotation_thread.start()

    # Start worker threads
    for thread_idx in range(num_threads):
        thread = threading.Thread(
            target=selenium_worker,
            args=(selenium_queue, args, skip_set, rotation),
            name=f"SeleniumWorker-{thread_idx + 1}",
            daemon=False
        )
        threads.append(thread)
        thread.start()
        log.info(f"[SELENIUM] Started thread {thread_idx + 1}/{num_threads}")
    
    # Queue items from DB
    log.info("[DB_READER] Queueing pending products from DB...")

    selenium_count = 0
    seen_keys = set()
    for row in pending_rows:
        prod = (row.get("product") or "").strip()
        comp = (row.get("company") or "").strip()
        url = (row.get("url") or "").strip()
        if not (prod and comp and url):
            continue
        key = (nk(comp), nk(prod))
        if key in seen_keys:
            continue
        with _skip_lock:
            if key in skip_set:
                continue
        from scraper_utils import is_product_already_scraped
        if is_product_already_scraped(comp, prod):
            continue
        seen_keys.add(key)
        selenium_queue.put((prod, comp))
        selenium_count += 1

    log.info(f"[DB_READER] Queued {selenium_count} selenium products from DB")

    for _ in range(num_threads):
        selenium_queue.put(None)  # None signals end of processing
    log.info("[SELENIUM] All products queued. Waiting for queue to be processed...")
    
    # Wait for queue to be processed (with periodic checks and timeout)
    # This ensures all items in queue are processed, including any requeued items
    # Note: Queue may contain None sentinels (one per thread), so we expect queue size to decrease
    max_wait_time = 3600  # Maximum 1 hour total wait
    check_interval = 5  # Check every 5 seconds
    elapsed = 0
    last_queue_size = selenium_queue.qsize()
    stable_count = 0  # Count how many times queue size stayed the same
    stable_threshold = 6  # Consider queue stable if size unchanged for 6 checks (30 seconds)
    
    while elapsed < max_wait_time and not _shutdown_requested.is_set():
        current_queue_size = selenium_queue.qsize()
        
        # If queue size hasn't changed, increment stable count
        if current_queue_size == last_queue_size:
            stable_count += 1
        else:
            stable_count = 0  # Reset if size changed
            last_queue_size = current_queue_size
        
        # If queue is empty or has only None sentinels (size <= num_threads), we're done
        # Also break if queue size has been stable for a while (likely just None sentinels or processing done)
        if current_queue_size <= num_threads:
            log.info(f"[SELENIUM] Queue size ({current_queue_size}) indicates only sentinels remaining, waiting for threads...")
            break
        
        if stable_count >= stable_threshold and current_queue_size <= num_threads * 2:
            log.info(f"[SELENIUM] Queue size stable at {current_queue_size} for {stable_threshold * check_interval}s, likely done processing")
            break
        
        time.sleep(check_interval)
        elapsed += check_interval
        
        if elapsed % 30 == 0:  # Log every 30 seconds
            log.info(f"[SELENIUM] Queue status: {current_queue_size} items remaining, elapsed: {elapsed}s")
    
    if not _shutdown_requested.is_set():
        final_queue_size = selenium_queue.qsize()
        if final_queue_size > num_threads:
            log.warning(f"[SELENIUM] Queue still has {final_queue_size} items after {elapsed}s, waiting for threads to finish...")
        else:
            log.info(f"[SELENIUM] Queue appears empty (size: {final_queue_size}), waiting for threads to complete...")
    
    log.info("[SELENIUM] Waiting for all worker threads to complete...")
    
    # Wait for all threads to complete (with timeout to check shutdown)
    try:
        for i, thread in enumerate(threads):
            # Use timeout to periodically check for shutdown
            while thread.is_alive():
                thread.join(timeout=1.0)  # Check every second
                if _shutdown_requested.is_set():
                    log.warning(f"[SELENIUM] Shutdown requested, waiting for thread {i + 1}/{num_threads} to exit gracefully...")
                    # Give thread more time to exit gracefully (up to 10 seconds)
                    thread.join(timeout=10.0)
                    break
            if thread.is_alive():
                log.warning(f"[SELENIUM] Thread {i + 1}/{num_threads} still alive after shutdown request and timeout")
            else:
                log.info(f"[SELENIUM] Thread {i + 1}/{num_threads} completed")
    except KeyboardInterrupt:
            log.warning("[SELENIUM] Interrupted, shutting down...")
            _shutdown_requested.set()
            # Wait for threads to exit gracefully before closing drivers
            for i, thread in enumerate(threads):
                if thread.is_alive():
                    log.warning(f"[SELENIUM] Waiting for thread {i + 1}/{num_threads} to exit gracefully...")
                    thread.join(timeout=10.0)
            # Now close drivers after workers have exited
            close_all_drivers()
            raise
    
    # Final check: ensure all threads are closed before exiting
    if _shutdown_requested.is_set():
        log.warning("[SELENIUM] Shutdown requested, ensuring all threads are closed...")
        # Get any remaining threads (in case we're in the middle of a loop)
        all_threads = [t for t in threading.enumerate() if t.name.startswith("SeleniumWorker-")]
        for thread in all_threads:
            if thread.is_alive():
                log.warning(f"[SELENIUM] Waiting for thread {thread.name} to exit...")
                thread.join(timeout=2.0)
                if thread.is_alive():
                    log.warning(f"[SELENIUM] Thread {thread.name} did not exit in time")
    
    # Ensure all drivers are closed (after workers have exited)
    log.info("[SELENIUM] Closing all Firefox/Tor drivers...")
    close_all_drivers()
    
    # Final progress update
    with _progress_lock:
        completed = _products_completed
        total = _total_products
    if total > 0:
        percent = round((completed / total) * 100, 2) if total > 0 else 0.0
        if percent > 100.0:
            percent = 100.0
        log.info(f"[SELENIUM] Final progress: {completed}/{total} ({percent}%)")
        print(f"[PROGRESS] Selenium scraping: {completed}/{total} ({percent}%)", flush=True)
    
    # Final status logging
    if _shutdown_requested.is_set():
        log.warning("[SELENIUM] Processing stopped due to shutdown request")
        # Still sync files even on shutdown to preserve progress
        log.info("[SELENIUM] Syncing files to ensure consistency...")
        try:
            sync_files_from_output()
        except Exception as e:
            log.warning(f"[SELENIUM] Error syncing files: {e}")
        return 1  # Exit with non-zero code on shutdown
    else:
        remaining = selenium_queue.qsize()
        if remaining > 0:
            log.warning(f"[SELENIUM] Queue has {remaining} remaining items (likely sentinels)")
        
        log.info("=" * 80)
        log.info(f"[SELENIUM] All products processed. Selenium completed{_pipeline_context_suffix()}.")
        log.info("=" * 80)
        print(f"[SELENIUM] Selenium completed{_pipeline_context_suffix()}", flush=True)
        
        # Sync files when Selenium step completed (regardless of queue state)
        # This ensures alfabeta_progress.csv and Productlist_with_urls.csv are aligned with alfabeta_products_by_product.csv
        log.info("[SELENIUM] Syncing files to ensure consistency after Selenium step completion...")
        try:
            sync_files_from_output()
            log.info("[SELENIUM] File synchronization completed successfully")
        except Exception as e:
            log.error(f"[SELENIUM] Error syncing files: {e}")
            import traceback
            log.error(traceback.format_exc())
        
        if remaining > 0:
            log.warning(f"[SELENIUM] Selenium completed with warnings{_pipeline_context_suffix()} ({remaining} items remaining in queue, likely sentinels)")
        return 0

# ====== SELENIUM WORKER ======

def selenium_worker(selenium_queue: Queue, args, skip_set: set, rotation: RotationCoordinator):
    """Selenium worker: processes products from queue with browser restart logic"""
    global _products_completed, _total_products
    
    thread_id = threading.get_ident()
    log.info(f"[SELENIUM_WORKER] Thread {thread_id} started")
    
    driver = None
    products_processed = 0  # Counter for products processed by this worker
    load_monitor = LoadTimeMonitor(SLOW_PAGE_WINDOW_SIZE, SLOW_PAGE_MIN_SAMPLES_COUNT, SLOW_PAGE_THRESHOLD_SECONDS)
    restart_due_to_slow = False
    slow_restart_reason = ""
    rotation_seq_seen = 0
    driver_started_at = None
    session_id = 0
    session_started_at = None
    session_products = 0

    def note_product_complete(start_ts: Optional[float]):
        nonlocal products_processed, session_products, restart_due_to_slow, slow_restart_reason
        products_processed += 1
        session_products += 1
        if not SLOW_PAGE_RESTART_ENABLED or start_ts is None:
            return
        elapsed = time.monotonic() - start_ts
        median_value = load_monitor.record(elapsed)
        if median_value is None or SLOW_PAGE_THRESHOLD_SECONDS <= 0:
            return
        if median_value >= SLOW_PAGE_THRESHOLD_SECONDS and not restart_due_to_slow:
            restart_due_to_slow = True
            slow_restart_reason = (
                f"median {median_value:.2f}s over {len(load_monitor.samples)} items "
                f"(threshold {SLOW_PAGE_THRESHOLD_SECONDS:.2f}s)"
            )
    
    def _log_session_end(reason: str):
        nonlocal session_id, session_started_at, session_products
        if session_id <= 0:
            return
        dur = None
        try:
            if session_started_at is not None:
                dur = time.monotonic() - session_started_at
        except Exception:
            dur = None
        dur_s = f"{dur:.1f}s" if isinstance(dur, (int, float)) else "unknown"
        log.warning(
            f"[SESSION] end id={session_id} reason={reason or 'unknown'} "
            f"processed={session_products} duration={dur_s}"
        )

    def _log_session_start(fp: Optional[dict]):
        nonlocal session_id, session_started_at, session_products
        session_id += 1
        session_started_at = time.monotonic()
        session_products = 0
        direct_ip = get_public_ip_direct()
        tor_ip = None
        try:
            socks_port = int(TOR_SOCKS_PORT or TOR_PROXY_PORT or 0)
            if socks_port > 0:
                tor_ip = get_public_ip_via_socks("127.0.0.1", socks_port)
        except Exception:
            tor_ip = None
        if fp:
            log.info(
                f"[SESSION] start id={session_id} direct_ip={direct_ip or 'unknown'} tor_ip={tor_ip or 'unknown'} "
                f"ua={fp.get('ua','')} lang={fp.get('lang','')} tz={fp.get('tz','')} viewport={fp.get('viewport','')}"
            )
        else:
            log.info(
                f"[SESSION] start id={session_id} direct_ip={direct_ip or 'unknown'} tor_ip={tor_ip or 'unknown'}"
            )

    def create_new_driver(reason: str = ""):
        """Helper to create and initialize a new driver"""
        nonlocal driver, products_processed, restart_due_to_slow, slow_restart_reason, driver_started_at
        if driver:
            _log_session_end(reason or "restart")
            pids_to_kill = set()
            if psutil:
                try:
                    if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                        geckodriver_pid = driver.service.process.pid
                        if geckodriver_pid:
                            pids_to_kill.add(geckodriver_pid)
                            try:
                                parent = psutil.Process(geckodriver_pid)
                                for child in parent.children(recursive=True):
                                    pids_to_kill.add(child.pid)
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                pass
                except Exception:
                    pass
            try:
                log.info(f"[SELENIUM_WORKER] Closing old driver...")
                unregister_driver(driver)
                clear_browser_storage(driver)
                driver.quit()
            except Exception as e:
                log.warning(f"[SELENIUM_WORKER] Error closing old driver: {e}")
            finally:
                cleanup_temp_profile(getattr(driver, "_profile_dir", None))
            if pids_to_kill and psutil:
                with _tracked_pids_lock:
                    for pid in list(pids_to_kill):
                        if pid in _tracked_firefox_pids:
                            try:
                                proc = psutil.Process(pid)
                                proc.kill()
                                log.info(f"[SELENIUM_WORKER] Killed Firefox/geckodriver process (Alfabeta): PID {pid}")
                                _tracked_firefox_pids.discard(pid)
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                pass
            driver = None
        
        log.info(f"[SELENIUM_WORKER] Creating new Firefox/Tor driver (headless={args.headless})...")
        driver = setup_driver(headless=args.headless)
        driver_started_at = time.monotonic()
        _log_session_start(getattr(driver, "_fingerprint", None))
        
        # Track Firefox/geckodriver PIDs for this new driver instance (already done in setup_driver)
        
        # Navigate to products page once when driver is created
        log.info(f"[SELENIUM_WORKER] Navigating to products page (one-time setup)...")
        navigate_to_products_page(driver)
        log.info(f"[SELENIUM_WORKER] Products page loaded - search form ready for use")

    def close_driver_only(reason: str):
        """Close and kill the current driver without creating a new one (used before IP rotation)."""
        nonlocal driver, driver_started_at
        if not driver:
            return
        _log_session_end(reason or "close")
        pids_to_kill = set()
        if psutil:
            try:
                if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                    geckodriver_pid = driver.service.process.pid
                    if geckodriver_pid:
                        pids_to_kill.add(geckodriver_pid)
                        try:
                            parent = psutil.Process(geckodriver_pid)
                            for child in parent.children(recursive=True):
                                pids_to_kill.add(child.pid)
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
            except Exception:
                pass
        try:
            log.warning(f"[SELENIUM_WORKER] Closing driver ({reason})...")
            unregister_driver(driver)
            clear_browser_storage(driver)
            driver.quit()
        except Exception as e:
            log.warning(f"[SELENIUM_WORKER] Error closing driver ({reason}): {e}")
        finally:
            cleanup_temp_profile(getattr(driver, "_profile_dir", None))
        if pids_to_kill and psutil:
            with _tracked_pids_lock:
                for pid in list(pids_to_kill):
                    if pid in _tracked_firefox_pids:
                        try:
                            proc = psutil.Process(pid)
                            proc.kill()
                            log.info(f"[SELENIUM_WORKER] Killed Firefox/geckodriver process (Alfabeta): PID {pid}")
                            _tracked_firefox_pids.discard(pid)
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
        driver = None
        driver_started_at = None
        products_processed = 0
        restart_due_to_slow = False
        slow_restart_reason = ""
        load_monitor.reset()
    
    try:
        # Check if shutdown was requested before initializing
        if _shutdown_requested.is_set():
            log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting before initialization")
            return
        
        # Initialize first driver
        create_new_driver()
        
        while True:
            # Check for shutdown before processing next item
            if _shutdown_requested.is_set():
                log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                break

            # Global IP rotation barrier: stop browser before Surfshark/Tor changes, relaunch after confirmation.
            should_rotate, seq, reason = rotation.wait_if_rotating_and_get_seq(rotation_seq_seen)
            if should_rotate:
                close_driver_only(f"rotation seq {seq}: {reason}")
                rotation.mark_worker_ready(thread_id)
                rotation_seq_seen = seq
                rotation.wait_rotation_done(seq)
                continue

            # Enforce max runtime per browser instance (<8 minutes) to reduce long-lived fingerprint linking.
            if driver_started_at and (time.monotonic() - driver_started_at) >= max(60, int(MAX_BROWSER_RUNTIME_SECONDS)):
                log.warning(f"[SELENIUM_WORKER] Max browser runtime exceeded, restarting session...")
                create_new_driver("max_runtime")
            
            if driver is None:
                log.warning(f"[SELENIUM_WORKER] Driver missing, creating a new session...")
                try:
                    create_new_driver("missing_driver")
                except Exception as e:
                    log.error(f"[SELENIUM_WORKER] Failed to create driver: {e}")
                    if interruptible_sleep(5):
                        break
                    continue
            
            if restart_due_to_slow:
                log.warning(f"[SELENIUM_WORKER] Slowdown detected, restarting browser ({slow_restart_reason})")
                create_new_driver(f"slow_page:{slow_restart_reason}")

            # Restart browser every N products
            if products_processed > 0 and products_processed % PRODUCTS_PER_RESTART == 0:
                log.info(f"[SELENIUM_WORKER] Restarting browser after {products_processed} products...")
                create_new_driver("products_per_restart")
            
            # PERFORMANCE FIX: Periodic resource cleanup every 25 products
            if products_processed > 0 and products_processed % 25 == 0:
                force_cleanup()
                # Log resource usage every 50 products
                if products_processed % 50 == 0:
                    log_resource_usage(f"[SELENIUM_WORKER] Thread {thread_id} - Product {products_processed}")
                # Clean up orphaned Firefox processes every 100 products
                if products_processed % 100 == 0:
                    orphaned = cleanup_orphaned_firefox_processes()
                    if orphaned > 0:
                        log.warning(f"[SELENIUM_WORKER] Cleaned up {orphaned} orphaned Firefox/geckodriver processes")
            
            # HARD MEMORY LIMIT: Check memory every N products
            if products_processed > 0 and products_processed % MEMORY_CHECK_INTERVAL == 0:
                if check_memory_limit():
                    log.error(f"[SELENIUM_WORKER] Thread {thread_id}: MEMORY LIMIT EXCEEDED - forcing nuclear cleanup and restart")
                    # Nuclear option: kill ALL Firefox processes
                    kill_all_firefox_processes()
                    # Force garbage collection
                    gc.collect()
                    # Restart driver
                    create_new_driver("memory_limit_exceeded")
                    # Reset counter
                    products_processed = 0
            
            # Check shutdown before getting from queue
            if _shutdown_requested.is_set():
                log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                break
            
            try:
                item = selenium_queue.get(timeout=QUEUE_GET_TIMEOUT)
                # Check if this is the sentinel value (None) signaling end of processing
                if item is None:
                    log.info(f"[SELENIUM_WORKER] Received stop signal (None), thread {thread_id} exiting")
                    selenium_queue.task_done()
                    break
                # Format: (product, company)
                in_product, in_company = item
            except Empty:
                # Check shutdown when queue is empty
                if _shutdown_requested.is_set():
                    log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                break
            
            product_start = time.monotonic()
            search_attempted = False
            
            # Check if driver is alive before processing
            if driver and not is_driver_alive(driver):
                log.warning(f"[SELENIUM_WORKER] Driver is dead before processing {in_company} | {in_product}, restarting...")
                driver = restart_driver(thread_id, driver, args.headless)
                if driver is None:
                    # Failed to restart, requeue and continue (but check if already completed first)
                    if not _shutdown_requested.is_set():
                        if SELENIUM_SINGLE_ATTEMPT or (SELENIUM_ROUND_ROBIN_RETRY and not should_requeue_for_round_robin(in_company, in_product)):
                            log.warning(f"[SELENIUM_WORKER] Driver restart failed, moving to API (single-attempt): {in_company} | {in_product}")
                            mark_api_pending(in_company, in_product)
                            try:
                                append_progress(in_company, in_product, 0)
                                with _skip_lock:
                                    skip_set.add((nk(in_company), nk(in_product)))
                            except Exception:
                                pass
                            with _progress_lock:
                                _products_completed += 1
                                completed = _products_completed
                                total = _total_products
                            note_product_complete(product_start)
                            log_progress_with_step(f"Completed (driver->API): {in_product}", completed, total)
                            selenium_queue.task_done()
                            continue
                        key = (nk(in_company), nk(in_product))
                        with _skip_lock:
                            if key not in skip_set:
                                # Check if product was already marked as completed in CSV
                                from scraper_utils import is_product_already_scraped
                                if is_product_already_scraped(in_company, in_product):
                                    log.info(f"[SELENIUM_WORKER] Skipping requeue for {in_company} | {in_product} after driver restart failure (already marked as scraped in CSV)")
                                    skip_set.add(key)
                                    selenium_queue.task_done()  # Mark current attempt as done
                                else:
                                    # Check round-robin retry logic
                                    if SELENIUM_ROUND_ROBIN_RETRY and not should_requeue_for_round_robin(in_company, in_product):
                                        # Max attempts reached, move to API
                                        mark_api_pending(in_company, in_product)
                                        skip_set.add(key)
                                        selenium_queue.task_done()
                                        with _progress_lock:
                                            _products_completed += 1
                                        note_product_complete(product_start)
                                    else:
                                        selenium_queue.task_done()  # Mark current attempt as done first
                                        selenium_queue.put(item)  # Then requeue (will have its own task_done() when processed)
                                        log.info(f"[SELENIUM_WORKER] Requeued {in_company} | {in_product} after driver restart failure")
                            else:
                                log.debug(f"[SELENIUM_WORKER] Skipping requeue for {in_company} | {in_product} after driver restart failure (already in skip_set)")
                                selenium_queue.task_done()  # Mark current attempt as done
                    continue
                # Driver restarted successfully, continue processing
            
            task_done_called = False  # Flag to track if task_done() was called explicitly (e.g., for requeue)
            try:
                # Defensive skip check with lock (runtime update protection)
                with _skip_lock:
                    key = (nk(in_company), nk(in_product))
                    if key in skip_set:
                        log.info(f"[SKIP-RUNTIME] {in_company} | {in_product}")
                        # task_done() will be called in finally block
                        continue

                if SKIP_REPEAT_SELENIUM_TO_API:
                    with _attempted_lock:
                        if key in _attempted_keys:
                            log.warning(f"[DUPLICATE] {in_company} | {in_product} already attempted - moving to API")
                            try:
                                update_prepared_urls_source(
                                    in_company,
                                    in_product,
                                    new_source="api",
                                    scraped_by_selenium="yes",
                                    scraped_by_api="no",
                                    selenium_records="0",
                                    api_records="0",
                                )
                            except Exception:
                                pass
                            with _skip_lock:
                                skip_set.add(key)
                            with _progress_lock:
                                _products_completed += 1
                                completed = _products_completed
                                total = _total_products
                            note_product_complete(product_start)
                            log_progress_with_step(f"Moved to API (duplicate): {in_product}", completed, total)
                            continue
                        _attempted_keys.add(key)
                
                log.info(f"[SELENIUM_WORKER] [SEARCH_START] {in_company} | {in_product}")
                search_attempted = True
                
                # Log progress: Starting search (don't increment counter yet - wait for completion)
                with _progress_lock:
                    completed = _products_completed
                    total = _total_products
                log_progress_with_step(f"Searching: {in_company} | {in_product}", completed, total)
                
                # Check for shutdown before rate limiting
                if _shutdown_requested.is_set():
                    log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                    # task_done() will be called in finally block
                    break
                
                # Apply rate limit
                duplicate_rate_limit_wait(thread_id)
                
                # Check for shutdown after rate limiting
                if _shutdown_requested.is_set():
                    log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                    # task_done() will be called in finally block
                    break
                
                # Check for captcha before processing (skip if on about:blank to avoid hanging)
                # Wrap driver access in try-except to handle shutdown gracefully
                try:
                    if _shutdown_requested.is_set():
                        log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                        # task_done() will be called in finally block
                        break
                    if driver and driver.current_url and not driver.current_url.startswith("about:") and is_captcha_page(driver):
                        log.warning(f"[SELENIUM_WORKER] [CAPTCHA_DETECTED] Captcha detected for {in_company} | {in_product}")
                        log.warning(f"[SELENIUM_WORKER] Closing browser instance, waiting {INSTANCE_RESTART_WAIT_SECONDS}s, then reopening...")
                        # Close browser, wait, reopen
                        try:
                            # Get Firefox/geckodriver PIDs before closing
                            try:
                                pids = set()
                                if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                                    geckodriver_pid = driver.service.process.pid
                                    if geckodriver_pid:
                                        pids.add(geckodriver_pid)
                                        if psutil:
                                            try:
                                                parent = psutil.Process(geckodriver_pid)
                                                for child in parent.children(recursive=True):
                                                    pids.add(child.pid)
                                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                                pass
                                if pids:
                                    log.info(f"[SELENIUM_WORKER] Thread {thread_id}: Killing Firefox/geckodriver PIDs before restart (Alfabeta only): {sorted(pids)}")
                                    # Kill Firefox/geckodriver processes associated with this driver (only tracked PIDs)
                                    if psutil:
                                        for pid in pids:
                                            try:
                                                # Only kill if it's in our tracked set (Alfabeta scraper only)
                                                with _tracked_pids_lock:
                                                    if pid in _tracked_firefox_pids:
                                                        proc = psutil.Process(pid)
                                                        proc.kill()
                                                        log.info(f"[SELENIUM_WORKER] Killed Firefox/geckodriver process (Alfabeta): PID {pid}")
                                                        _tracked_firefox_pids.discard(pid)  # Remove from tracked set after killing
                                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                                pass
                            except Exception as e:
                                log.debug(f"[SELENIUM_WORKER] Could not get/kill Firefox/geckodriver PIDs: {e}")
                            
                            unregister_driver(driver)
                            clear_browser_storage(driver)
                            driver.quit()
                        except Exception as e:
                            log.warning(f"[SELENIUM_WORKER] Error closing driver: {e}")
                        finally:
                            cleanup_temp_profile(getattr(driver, "_profile_dir", None))
                        driver = None
                        
                        # Wait 2 minutes
                        log.info(f"[SELENIUM_WORKER] Waiting {INSTANCE_RESTART_WAIT_SECONDS} seconds before reopening browser...")
                        with _progress_lock:
                            completed = _products_completed
                            total = _total_products
                        log_progress_with_step(f"Waiting before browser restart", completed, total)
                        if interruptible_sleep(INSTANCE_RESTART_WAIT_SECONDS):
                            break  # Shutdown requested
                        
                        # Create new driver
                        create_new_driver()
                        
                        # Check shutdown before requeuing
                        if _shutdown_requested.is_set():
                            log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting (not requeuing)")
                            # task_done() will be called in finally block
                            break
                        
                        # Don't requeue if shutdown requested
                        if _shutdown_requested.is_set():
                            log.warning(f"[SELENIUM_WORKER] Shutdown requested, not requeueing {in_company} | {in_product}")
                            # Write progress/error before exiting
                            try:
                                append_error(in_company, in_product, "Captcha detected during shutdown")
                                append_progress(in_company, in_product, 0)
                                with _skip_lock:
                                    skip_set.add((nk(in_company), nk(in_product)))
                            except Exception:
                                pass
                            # Increment progress counter on completion (shutdown)
                            with _progress_lock:
                                _products_completed += 1
                                completed = _products_completed
                                total = _total_products
                            note_product_complete(product_start)
                            log_progress_with_step(f"Completed (shutdown): {in_product}", completed, total)
                            # task_done() will be called in finally block
                            break
                        
                        # In single-attempt or round-robin mode with max attempts reached: move to API
                        if SELENIUM_SINGLE_ATTEMPT or (SELENIUM_ROUND_ROBIN_RETRY and not should_requeue_for_round_robin(in_company, in_product)):
                            log.warning(f"[SELENIUM_WORKER] Captcha detected, moving to API (single-attempt/max-round-robin): {in_company} | {in_product}")
                            mark_api_pending(in_company, in_product)
                            try:
                                append_progress(in_company, in_product, 0)
                                with _skip_lock:
                                    skip_set.add((nk(in_company), nk(in_product)))
                            except Exception:
                                pass
                            with _progress_lock:
                                _products_completed += 1
                                completed = _products_completed
                                total = _total_products
                            note_product_complete(product_start)
                            log_progress_with_step(f"Completed (captcha->API): {in_product}", completed, total)
                            continue  # Skip requeue

                        # Requeue product to retry (only if not shutdown and not already completed)
                        key = (nk(in_company), nk(in_product))
                        with _skip_lock:
                            if key not in skip_set:
                                # Check if product was already marked as completed in CSV (prevent infinite requeue)
                                from scraper_utils import is_product_already_scraped
                                if is_product_already_scraped(in_company, in_product):
                                    log.info(f"[SELENIUM_WORKER] Skipping requeue for {in_company} | {in_product} (already marked as scraped in CSV)")
                                    skip_set.add(key)
                                    # task_done() will be called in finally block
                                else:
                                    # Check round-robin retry logic
                                    if SELENIUM_ROUND_ROBIN_RETRY and not should_requeue_for_round_robin(in_company, in_product):
                                        # Max attempts reached, move to API
                                        mark_api_pending(in_company, in_product)
                                        skip_set.add(key)
                                        with _progress_lock:
                                            _products_completed += 1
                                        note_product_complete(product_start)
                                        task_done_called = True
                                        selenium_queue.task_done()
                                    else:
                                        # For requeue: call task_done() explicitly and set flag to prevent double call
                                        selenium_queue.task_done()
                                        task_done_called = True
                                        selenium_queue.put(item)  # Then requeue (will have its own task_done() when processed)
                                        log.info(f"[SELENIUM_WORKER] Requeued {in_company} | {in_product} for retry")
                            else:
                                log.debug(f"[SELENIUM_WORKER] Skipping requeue for {in_company} | {in_product} (already in skip_set)")
                                # task_done() will be called in finally block
                        continue  # Continue to next item
                except (InvalidSessionIdException, WebDriverException) as driver_error:
                    # Clean up the dead driver
                    if driver:
                        try:
                            # Get Firefox/geckodriver PIDs before closing
                            try:
                                pids = set()
                                if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                                    geckodriver_pid = driver.service.process.pid
                                    if geckodriver_pid:
                                        pids.add(geckodriver_pid)
                                        if psutil:
                                            try:
                                                parent = psutil.Process(geckodriver_pid)
                                                for child in parent.children(recursive=True):
                                                    pids.add(child.pid)
                                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                                pass
                                if pids:
                                    log.info(f"[SELENIUM_WORKER] Thread {thread_id}: Killing Firefox/geckodriver PIDs from dead driver (Alfabeta only): {sorted(pids)}")
                                    # Kill Firefox/geckodriver processes associated with this driver (only tracked PIDs)
                                    if psutil:
                                        for pid in pids:
                                            try:
                                                # Only kill if it's in our tracked set (Alfabeta scraper only)
                                                with _tracked_pids_lock:
                                                    if pid in _tracked_firefox_pids:
                                                        proc = psutil.Process(pid)
                                                        proc.kill()
                                                        log.info(f"[SELENIUM_WORKER] Killed Firefox/geckodriver process (Alfabeta): PID {pid}")
                                                        _tracked_firefox_pids.discard(pid)  # Remove from tracked set after killing
                                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                                pass
                            except Exception as e:
                                log.debug(f"[SELENIUM_WORKER] Could not get/kill Firefox/geckodriver PIDs: {e}")
                            
                            unregister_driver(driver)
                            try:
                                clear_browser_storage(driver)
                                driver.quit()
                            except Exception:
                                pass
                            finally:
                                cleanup_temp_profile(getattr(driver, "_profile_dir", None))
                            # Firefox will close asynchronously
                        except Exception as e:
                            log.warning(f"[SELENIUM_WORKER] Error cleaning up dead driver: {e}")
                    
                    # Driver was killed (likely due to shutdown)
                    if _shutdown_requested.is_set():
                        log.info(f"[SELENIUM_WORKER] Driver session ended (shutdown requested), thread {thread_id} exiting")
                        # Write progress/error before exiting to prevent reprocessing
                        try:
                            append_error(in_company, in_product, f"Driver error during shutdown: {driver_error}")
                            append_progress(in_company, in_product, 0)
                            with _skip_lock:
                                skip_set.add((nk(in_company), nk(in_product)))
                        except Exception:
                            pass
                        # Increment progress counter on completion (shutdown)
                        with _progress_lock:
                            _products_completed += 1
                            completed = _products_completed
                            total = _total_products
                        note_product_complete(product_start)
                        log_progress_with_step(f"Completed (shutdown): {in_product}", completed, total)
                        # task_done() will be called in finally block
                        break
                    # If not shutdown, treat as normal error
                    log.warning(f"[SELENIUM_WORKER] Driver session invalid: {driver_error}")
                    driver = None
                    # Write error and progress to prevent infinite retries
                    try:
                        append_error(in_company, in_product, f"Driver error: {driver_error}")
                        append_progress(in_company, in_product, 0)
                        with _skip_lock:
                            skip_set.add((nk(in_company), nk(in_product)))
                    except Exception:
                        pass
                    # Increment progress counter on completion (driver error)
                    with _progress_lock:
                        _products_completed += 1
                        completed = _products_completed
                        total = _total_products
                    note_product_complete(product_start)
                    log_progress_with_step(f"Completed (driver error): {in_product}", completed, total)
                    # Don't requeue on driver errors - they indicate session is dead
                    log.warning(f"[SELENIUM_WORKER] Not requeueing {in_company} | {in_product} (driver session dead)")
                    # task_done() will be called in finally block
                    continue
                
                # Retry logic for TimeoutException
                # Check round-robin mode: if enabled, check max attempts before proceeding
                if SELENIUM_ROUND_ROBIN_RETRY:
                    current_attempts = get_product_attempt_count(in_company, in_product)
                    if current_attempts >= SELENIUM_MAX_ATTEMPTS_PER_PRODUCT:
                        log.warning(f"[ROUND_ROBIN] Max attempts ({SELENIUM_MAX_ATTEMPTS_PER_PRODUCT}) reached for {in_company} | {in_product}, moving to API")
                        try:
                            update_prepared_urls_source(
                                in_company, in_product,
                                new_source="api",
                                scraped_by_selenium="yes",
                                scraped_by_api="no",
                                selenium_records="0",
                                api_records="0",
                            )
                        except Exception as e:
                            log.warning(f"[ROUND_ROBIN] Failed to update source for {in_company} | {in_product}: {e}")
                        with _skip_lock:
                            skip_set.add((nk(in_company), nk(in_product)))
                        with _progress_lock:
                            _products_completed += 1
                            completed = _products_completed
                            total = _total_products
                        note_product_complete(product_start)
                        log_progress_with_step(f"Completed (max round-robin attempts): {in_product}", completed, total)
                        continue
                    log.info(f"[ROUND_ROBIN] Attempt {current_attempts + 1}/{SELENIUM_MAX_ATTEMPTS_PER_PRODUCT} for {in_company} | {in_product}")
                
                max_retries = 0 if (SELENIUM_SINGLE_ATTEMPT or SELENIUM_ROUND_ROBIN_RETRY) else MAX_RETRIES_TIMEOUT
                retry_count = 0
                success = False
                
                while retry_count <= max_retries and not success:
                    # Check for shutdown at start of each retry
                    if _shutdown_requested.is_set():
                        log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                        # task_done() will be called in finally block
                        break
                    
                    try:
                        if retry_count > 0:
                            log.info(f"[SELENIUM_WORKER] [RETRY {retry_count}/{max_retries}] {in_company} | {in_product}")
                            # Check shutdown during sleep (interruptible)
                            if interruptible_sleep(10):
                                log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                                # task_done() will be called in finally block
                                break
                        
                        # Check shutdown before search
                        if _shutdown_requested.is_set():
                            log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                            # task_done() will be called in finally block
                            break
                        
                        # Simple retry loop: search -> check for login -> if login, wait 2 min and retry -> until data found or no data
                        data_found = False
                        product_not_found = False
                        driver_restarted = False  # Flag to track if driver was restarted (requires requeue)
                        retry_loop_count = 0
                        # In round-robin mode: single attempt per loop, requeue to end for next loop
                        max_retry_loops = 1 if (SELENIUM_SINGLE_ATTEMPT or SELENIUM_ROUND_ROBIN_RETRY) else 5
                        
                        while not data_found and not product_not_found and retry_loop_count < max_retry_loops:
                            retry_loop_count += 1
                            
                            # Safety check: if we've exceeded max retries, force exit immediately
                            if retry_loop_count > max_retry_loops:
                                log.error(f"[SELENIUM_WORKER] [SAFETY] Retry loop count exceeded max ({max_retry_loops}), forcing exit to prevent infinite loop")
                                product_not_found = True
                                break
                            
                            if retry_loop_count > 1:
                                log.warning(f"[SELENIUM_WORKER] [RETRY_LOOP] Retry loop iteration {retry_loop_count}/{max_retry_loops} for {in_company} | {in_product}")
                            
                            # Check for shutdown
                            if _shutdown_requested.is_set():
                                log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                                # task_done() will be called in finally block
                                break
                            
                            # Log before attempting search
                            log.info(f"[SELENIUM_WORKER] [SEARCH] Starting search for {in_company} | {in_product}")
                            try:
                                log.info(f"[SELENIUM_WORKER] [SEARCH] Driver current URL: {driver.current_url}")
                            except (InvalidSessionIdException, WebDriverException):
                                if _shutdown_requested.is_set():
                                    log.warning(f"[SELENIUM_WORKER] Driver session invalid (shutdown requested), thread {thread_id} exiting")
                                    # task_done() will be called in finally block
                                    break
                                # Driver crashed, break retry loop
                                break
                            
                            # Update progress: Searching
                            with _progress_lock:
                                completed = _products_completed
                                total = _total_products
                            log_progress_with_step(f"Searching: {in_product}", completed, total)
                            
                            # Search for product
                            try:
                                search_product_on_page(driver, in_product)
                                log.info(f"[SELENIUM_WORKER] [SEARCH] Successfully searched for {in_company} | {in_product}")
                            except (InvalidSessionIdException, WebDriverException) as driver_error:
                                if _shutdown_requested.is_set():
                                    log.warning(f"[SELENIUM_WORKER] Driver session invalid (shutdown requested), thread {thread_id} exiting")
                                    # Write progress/error before exiting
                                    try:
                                        append_error(in_company, in_product, f"Driver error during shutdown: {driver_error}")
                                        append_progress(in_company, in_product, 0)
                                        with _skip_lock:
                                            skip_set.add((nk(in_company), nk(in_product)))
                                    except Exception:
                                        pass
                                    selenium_queue.task_done()
                                    break
                                
                                # Check if fatal driver error - restart driver and exit retry loop
                                if is_fatal_driver_error(driver_error) or not is_driver_alive(driver):
                                    log.error(f"[FATAL_DRIVER] Thread {thread_id}: Driver error during search (fatal): {driver_error}")
                                    driver = restart_driver(thread_id, driver, args.headless)
                                    if driver is None:
                                        # Failed to restart, mark as error
                                        try:
                                            append_error(in_company, in_product, f"Driver restart failed after search error: {driver_error}")
                                            append_progress(in_company, in_product, 0)
                                            with _skip_lock:
                                                skip_set.add((nk(in_company), nk(in_product)))
                                        except Exception:
                                            pass
                                        product_not_found = True
                                        break
                                    # Driver restarted, mark for requeue and exit retry loop
                                    driver_restarted = True
                                    product_not_found = True
                                    break
                                
                                log.warning(f"[SELENIUM_WORKER] [SEARCH] Driver error during search: {driver_error}")
                                # Mark as failed and don't retry
                                try:
                                    append_error(in_company, in_product, f"Driver error during search: {driver_error}")
                                    append_progress(in_company, in_product, 0)
                                    with _skip_lock:
                                        skip_set.add((nk(in_company), nk(in_product)))
                                except Exception:
                                    pass
                                product_not_found = True  # Mark as done to exit retry loop
                                break  # Break retry loop on driver error
                            except (RuntimeError, TimeoutException) as e:
                                log.error(f"[SELENIUM_WORKER] [SEARCH] Error during search: {e}")
                                # Mark as failed to prevent infinite loop
                                product_not_found = True
                                break  # Break retry loop on search error
                            except Exception as search_error:
                                # Catch any other unexpected exceptions to prevent infinite loop
                                log.error(f"[SELENIUM_WORKER] [SEARCH] Unexpected error during search: {search_error}")
                                log.error(f"[SELENIUM_WORKER] [SEARCH] Error type: {type(search_error).__name__}")
                                import traceback
                                log.error(f"[SELENIUM_WORKER] [SEARCH] Traceback: {traceback.format_exc()}")
                                # Mark as failed to prevent infinite loop
                                product_not_found = True
                                break  # Break retry loop on unexpected error
                            
                            # Check if login page appeared after search
                            try:
                                if is_login_page(driver):
                                    # In single-attempt or round-robin mode: exit early to allow requeue/next loop
                                    if SELENIUM_SINGLE_ATTEMPT or SELENIUM_ROUND_ROBIN_RETRY:
                                        mode_str = "single-attempt" if SELENIUM_SINGLE_ATTEMPT else "round-robin"
                                        log.warning(f"[SELENIUM_WORKER] [LOGIN_CAPTCHA] Login captcha detected after search, exiting for {mode_str}: {in_company} | {in_product}")
                                        if SELENIUM_SINGLE_ATTEMPT:
                                            mark_api_pending(in_company, in_product)
                                            try:
                                                append_progress(in_company, in_product, 0)
                                                with _skip_lock:
                                                    skip_set.add((nk(in_company), nk(in_product)))
                                            except Exception:
                                                pass
                                        product_not_found = True
                                        break
                                    log.warning(f"[SELENIUM_WORKER] [LOGIN_CAPTCHA] Login captcha detected after search for {in_company} | {in_product}")
                                    log.info(f"[SELENIUM_WORKER] [LOGIN_CAPTCHA] Waiting {LOGIN_CAPTCHA_WAIT_SECONDS} seconds, then retrying search...")
                                    with _progress_lock:
                                        completed = _products_completed
                                        total = _total_products
                                    log_progress_with_step(f"Waiting for login captcha to clear: {in_product}", completed, total)
                                    if interruptible_sleep(LOGIN_CAPTCHA_WAIT_SECONDS):
                                        break  # Shutdown requested
                                    # Navigate back to products page and retry
                                    navigate_to_products_page(driver)
                                    continue  # Retry search
                            except (InvalidSessionIdException, WebDriverException) as driver_error:
                                if _shutdown_requested.is_set():
                                    log.warning(f"[SELENIUM_WORKER] Driver session invalid (shutdown requested), thread {thread_id} exiting")
                                    selenium_queue.task_done()
                                    break
                                # Driver crashed, break retry loop
                                log.warning(f"[SELENIUM_WORKER] Driver error during login check: {driver_error}")
                                break
                            
                            # Check for captcha after search
                            try:
                                if driver.current_url and not driver.current_url.startswith("about:") and is_captcha_page(driver):
                                    # In single-attempt or round-robin mode: exit early to allow requeue/next loop
                                    if SELENIUM_SINGLE_ATTEMPT or SELENIUM_ROUND_ROBIN_RETRY:
                                        mode_str = "single-attempt" if SELENIUM_SINGLE_ATTEMPT else "round-robin"
                                        log.warning(f"[SELENIUM_WORKER] [CAPTCHA] Captcha detected after search, exiting for {mode_str}: {in_company} | {in_product}")
                                        if SELENIUM_SINGLE_ATTEMPT:
                                            mark_api_pending(in_company, in_product)
                                            try:
                                                append_progress(in_company, in_product, 0)
                                                with _skip_lock:
                                                    skip_set.add((nk(in_company), nk(in_product)))
                                            except Exception:
                                                pass
                                        product_not_found = True
                                        break
                                    log.warning(f"[SELENIUM_WORKER] [CAPTCHA] Captcha detected after search for {in_company} | {in_product}")
                                    log.info(f"[SELENIUM_WORKER] [CAPTCHA] Waiting {INSTANCE_RESTART_WAIT_SECONDS} seconds, then retrying search...")
                                    with _progress_lock:
                                        completed = _products_completed
                                        total = _total_products
                                    log_progress_with_step(f"Waiting for captcha to clear: {in_product}", completed, total)
                                    if interruptible_sleep(INSTANCE_RESTART_WAIT_SECONDS):
                                        break  # Shutdown requested
                                    # Navigate back to products page and retry
                                    navigate_to_products_page(driver)
                                    continue  # Retry search
                            except (InvalidSessionIdException, WebDriverException) as driver_error:
                                if _shutdown_requested.is_set():
                                    log.warning(f"[SELENIUM_WORKER] Driver session invalid (shutdown requested), thread {thread_id} exiting")
                                    selenium_queue.task_done()
                                    break
                                log.warning(f"[SELENIUM_WORKER] Driver error during captcha check: {driver_error}")
                                break
                            
                            # Update progress: Opening product page
                            with _progress_lock:
                                completed = _products_completed
                                total = _total_products
                            log_progress_with_step(f"Opening product page: {in_product}", completed, total)
                            
                            # Try to open product page
                            try:
                                if not open_exact_pair(driver, in_product, in_company):
                                    # Product not found - no data
                                    save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                                    log.warning(f"[SELENIUM_WORKER] [NOT_FOUND] Product not found for {in_company} | {in_product}, marking for API fallback")
                                    # If Selenium extracts NO values:
                                    # Source = api, Scraped_By_Selenium = yes, Scraped_By_API = no (pending API)
                                    update_prepared_urls_source(
                                        in_company,
                                        in_product,
                                        new_source="api",
                                        scraped_by_selenium="yes",
                                        scraped_by_api="no",
                                        selenium_records="0",
                                        api_records="0",
                                    )
                                    with _skip_lock:
                                        skip_set.add((nk(in_company), nk(in_product)))
                                    product_not_found = True
                                    break  # Exit retry loop - no data
                            except TimeoutException:
                                # Let TimeoutException propagate to outer retry handler
                                # so it can retry up to MAX_RETRIES_TIMEOUT times
                                log.warning(f"[SELENIUM_WORKER] [TIMEOUT] Timeout opening product page for {in_company} | {in_product}")
                                raise  # Re-raise to let outer retry logic handle it
                            except (InvalidSessionIdException, WebDriverException) as driver_error:
                                if _shutdown_requested.is_set():
                                    log.warning(f"[SELENIUM_WORKER] Driver session invalid (shutdown requested), thread {thread_id} exiting")
                                    # Write progress/error before exiting
                                    try:
                                        append_error(in_company, in_product, f"Driver error during shutdown: {driver_error}")
                                        append_progress(in_company, in_product, 0)
                                        with _skip_lock:
                                            skip_set.add((nk(in_company), nk(in_product)))
                                    except Exception:
                                        pass
                                    selenium_queue.task_done()
                                    break
                                
                                # Check if fatal driver error - restart driver and exit retry loop
                                if is_fatal_driver_error(driver_error) or not is_driver_alive(driver):
                                    log.error(f"[FATAL_DRIVER] Thread {thread_id}: Driver error opening product page (fatal): {driver_error}")
                                    driver = restart_driver(thread_id, driver, args.headless)
                                    if driver is None:
                                        # Failed to restart, mark as error
                                        try:
                                            append_error(in_company, in_product, f"Driver restart failed after open error: {driver_error}")
                                            append_progress(in_company, in_product, 0)
                                            with _skip_lock:
                                                skip_set.add((nk(in_company), nk(in_product)))
                                        except Exception:
                                            pass
                                        product_not_found = True
                                        break
                                    # Driver restarted, mark for requeue and exit retry loop
                                    driver_restarted = True
                                    product_not_found = True
                                    break
                                
                                log.warning(f"[SELENIUM_WORKER] Driver error opening product page: {driver_error}")
                                # Mark as failed and don't retry
                                try:
                                    append_error(in_company, in_product, f"Driver error opening product page: {driver_error}")
                                    append_progress(in_company, in_product, 0)
                                    with _skip_lock:
                                        skip_set.add((nk(in_company), nk(in_product)))
                                except Exception:
                                    pass
                                product_not_found = True  # Mark as done to exit retry loop
                                break  # Break on driver error
                            
                            # Check shutdown before checking login page
                            if _shutdown_requested.is_set():
                                log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                                selenium_queue.task_done()
                                break
                            
                            # Check if login page appeared after opening product page
                            try:
                                if is_login_page(driver):
                                    # In single-attempt or round-robin mode: exit early to allow requeue/next loop
                                    if SELENIUM_SINGLE_ATTEMPT or SELENIUM_ROUND_ROBIN_RETRY:
                                        mode_str = "single-attempt" if SELENIUM_SINGLE_ATTEMPT else "round-robin"
                                        log.warning(f"[SELENIUM_WORKER] [LOGIN_CAPTCHA] Login captcha on product page, exiting for {mode_str}: {in_company} | {in_product}")
                                        if SELENIUM_SINGLE_ATTEMPT:
                                            mark_api_pending(in_company, in_product)
                                            try:
                                                append_progress(in_company, in_product, 0)
                                                with _skip_lock:
                                                    skip_set.add((nk(in_company), nk(in_product)))
                                            except Exception:
                                                pass
                                        product_not_found = True
                                        break
                                    log.warning(f"[SELENIUM_WORKER] [LOGIN_CAPTCHA] Login captcha detected on product page for {in_company} | {in_product}")
                                    log.info(f"[SELENIUM_WORKER] [LOGIN_CAPTCHA] Waiting {LOGIN_CAPTCHA_WAIT_SECONDS} seconds, then retrying search...")
                                    with _progress_lock:
                                        completed = _products_completed
                                        total = _total_products
                                    log_progress_with_step(f"Waiting for login captcha to clear: {in_product}", completed, total)
                                    if interruptible_sleep(LOGIN_CAPTCHA_WAIT_SECONDS):
                                        break  # Shutdown requested
                                    # Navigate back to products page and retry
                                    navigate_to_products_page(driver)
                                    continue  # Retry search
                            except (InvalidSessionIdException, WebDriverException) as driver_error:
                                if _shutdown_requested.is_set():
                                    log.warning(f"[SELENIUM_WORKER] Driver session invalid (shutdown requested), thread {thread_id} exiting")
                                    # Write progress/error before exiting
                                    try:
                                        append_error(in_company, in_product, f"Driver error during shutdown: {driver_error}")
                                        append_progress(in_company, in_product, 0)
                                        with _skip_lock:
                                            skip_set.add((nk(in_company), nk(in_product)))
                                    except Exception:
                                        pass
                                    selenium_queue.task_done()
                                    break
                                log.warning(f"[SELENIUM_WORKER] Driver error during login check: {driver_error}")
                                # Mark as failed and don't retry
                                try:
                                    append_error(in_company, in_product, f"Driver error during login check: {driver_error}")
                                    append_progress(in_company, in_product, 0)
                                    with _skip_lock:
                                        skip_set.add((nk(in_company), nk(in_product)))
                                except Exception:
                                    pass
                                product_not_found = True  # Mark as done to exit retry loop
                                break
                            
                            # Check for captcha on product page
                            try:
                                if driver.current_url and not driver.current_url.startswith("about:") and is_captcha_page(driver):
                                    # In single-attempt or round-robin mode: exit early to allow requeue/next loop
                                    if SELENIUM_SINGLE_ATTEMPT or SELENIUM_ROUND_ROBIN_RETRY:
                                        mode_str = "single-attempt" if SELENIUM_SINGLE_ATTEMPT else "round-robin"
                                        log.warning(f"[SELENIUM_WORKER] [CAPTCHA] Captcha on product page, exiting for {mode_str}: {in_company} | {in_product}")
                                        if SELENIUM_SINGLE_ATTEMPT:
                                            mark_api_pending(in_company, in_product)
                                            try:
                                                append_progress(in_company, in_product, 0)
                                                with _skip_lock:
                                                    skip_set.add((nk(in_company), nk(in_product)))
                                            except Exception:
                                                pass
                                        product_not_found = True
                                        break
                                    log.warning(f"[SELENIUM_WORKER] [CAPTCHA] Captcha detected on product page for {in_company} | {in_product}")
                                    log.info(f"[SELENIUM_WORKER] [CAPTCHA] Waiting {INSTANCE_RESTART_WAIT_SECONDS} seconds, then retrying search...")
                                    with _progress_lock:
                                        completed = _products_completed
                                        total = _total_products
                                    log_progress_with_step(f"Waiting for captcha to clear: {in_product}", completed, total)
                                    if interruptible_sleep(INSTANCE_RESTART_WAIT_SECONDS):
                                        break  # Shutdown requested
                                    # Navigate back to products page and retry
                                    navigate_to_products_page(driver)
                                    continue  # Retry search
                            except (InvalidSessionIdException, WebDriverException) as driver_error:
                                if _shutdown_requested.is_set():
                                    log.warning(f"[SELENIUM_WORKER] Driver session invalid (shutdown requested), thread {thread_id} exiting")
                                    selenium_queue.task_done()
                                    break
                                log.warning(f"[SELENIUM_WORKER] Driver error during captcha check: {driver_error}")
                                break
                            
                            # If we got here, product page is open and no login/captcha - extract data NOW
                            # Check shutdown before extracting rows
                            if _shutdown_requested.is_set():
                                log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                                selenium_queue.task_done()
                                break
                            
                            # Update progress: Extracting data
                            with _progress_lock:
                                completed = _products_completed
                                total = _total_products
                            log_progress_with_step(f"Extracting data: {in_product}", completed, total)
                            
                            # Extract data from product page with error handling
                            try:
                                rows = extract_rows(driver, in_company, in_product)
                            except (InvalidSessionIdException, WebDriverException) as driver_error:
                                # Driver error during extraction - session is dead
                                log.error(f"[SELENIUM_WORKER] [EXTRACT] Driver error during extraction for {in_company} | {in_product}: {driver_error}")
                                if _shutdown_requested.is_set():
                                    log.warning(f"[SELENIUM_WORKER] Driver session invalid (shutdown requested), thread {thread_id} exiting")
                                    # Write progress/error before exiting
                                    try:
                                        append_error(in_company, in_product, f"Driver error during extraction (shutdown): {driver_error}")
                                        append_progress(in_company, in_product, 0)
                                        with _skip_lock:
                                            skip_set.add((nk(in_company), nk(in_product)))
                                    except Exception:
                                        pass
                                    selenium_queue.task_done()
                                    break
                                
                                # Check if fatal driver error - restart driver and exit retry loop
                                if is_fatal_driver_error(driver_error) or not is_driver_alive(driver):
                                    log.error(f"[FATAL_DRIVER] Thread {thread_id}: Driver error during extraction (fatal): {driver_error}")
                                    driver = restart_driver(thread_id, driver, args.headless)
                                    if driver is None:
                                        # Failed to restart, mark as error
                                        try:
                                            append_error(in_company, in_product, f"Driver restart failed after extraction error: {driver_error}")
                                            append_progress(in_company, in_product, 0)
                                            with _skip_lock:
                                                skip_set.add((nk(in_company), nk(in_product)))
                                        except Exception:
                                            pass
                                        product_not_found = True
                                        break
                                    # Driver restarted, mark for requeue and exit retry loop
                                    driver_restarted = True
                                    product_not_found = True
                                    break
                                
                                # Mark as failed and don't retry
                                try:
                                    append_error(in_company, in_product, f"Driver error during extraction: {driver_error}")
                                    append_progress(in_company, in_product, 0)
                                    with _skip_lock:
                                        skip_set.add((nk(in_company), nk(in_product)))
                                except Exception:
                                    pass
                                product_not_found = True  # Mark as done to exit retry loop
                                break
                            except Exception as extract_error:
                                # Other extraction errors - log and continue
                                log.error(f"[SELENIUM_WORKER] [EXTRACT] Extraction error for {in_company} | {in_product}: {extract_error}")
                                rows = []  # Set empty rows to continue processing
                            
                            # Check shutdown after extraction
                            if _shutdown_requested.is_set():
                                log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                                selenium_queue.task_done()
                                break
                            
                            # Check if rows have meaningful data (not blank)
                            rows_with_values = []
                            for row in rows:
                                # Check if row has actual values: price_ars, description, coverage, etc.
                                has_price = row.get("price_ars") is not None
                                has_description = row.get("description") and row.get("description").strip()
                                has_coverage = row.get("coverage_json") and row.get("coverage_json") != "{}"
                                has_import_status = row.get("import_status") and row.get("import_status").strip()
                                has_product_name = row.get("product_name") and row.get("product_name").strip()
                                
                                # Only include rows with at least one meaningful value
                                if has_price or has_description or has_coverage or has_import_status or has_product_name:
                                    rows_with_values.append(row)
                            
                            if rows_with_values:
                                # Update progress: Saving results
                                with _progress_lock:
                                    completed = _products_completed
                                    total = _total_products
                                log_progress_with_step(f"Saving results: {in_product}", completed, total)
                                
                                # Data found - save results and exit retry loop
                                saved = append_rows(rows_with_values)
                                if saved:
                                    append_progress(in_company, in_product, len(rows_with_values))
                                    # Update skip_set to prevent reprocessing in same run
                                    with _skip_lock:
                                        skip_set.add((nk(in_company), nk(in_product)))
                                    log.info(f"[SELENIUM_WORKER] [SUCCESS] {in_company} | {in_product} -> {len(rows_with_values)} rows with values")
                                else:
                                    append_error(in_company, in_product, "DB insert failed (ar_products); leaving total_records=0 for retry")
                                    append_progress(in_company, in_product, 0)
                                    log.warning(f"[SELENIUM_WORKER] [DB_FAIL] {in_company} | {in_product} -> insert failed, will retry in later rounds")
                                
                                # Update result: If Selenium extracts valid values:
                                # Source = selenium, Scraped_By_Selenium = yes, Scraped_By_API = no
                                update_prepared_urls_source(
                                    in_company,
                                    in_product,
                                    new_source="selenium",
                                    scraped_by_selenium="yes",
                                    scraped_by_api="no",
                                    selenium_records=str(len(rows_with_values)),
                                    api_records="0",
                                )
                                
                                note_product_complete(product_start)
                                
                                # Increment progress counter on actual completion (success)
                                with _progress_lock:
                                    _products_completed += 1
                                    completed = _products_completed
                                    total = _total_products
                                
                                # Log progress immediately after increment (for real-time progress bar updates)
                                log_progress_with_step(f"Completed: {in_product}", completed, total)
                                
                                # Rate limiting: pause after completing each product to avoid overwhelming server
                                rate_limit_pause()
                                
                                data_found = True
                                
                                # Navigate back to products page for next search
                                current_url = driver.current_url
                                if not is_products_search_url(current_url):
                                    log.info(f"[SELENIUM_WORKER] Navigating back to products page for next search...")
                                    try:
                                        driver.back()
                                        if interruptible_sleep(1):
                                            break  # Shutdown requested
                                        WebDriverWait(driver, 5).until(
                                            EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
                                        )
                                        log.info(f"[SELENIUM_WORKER] Successfully navigated back to products page")
                                    except Exception:
                                        navigate_to_products_page(driver)
                                
                                # Exit retry loop - data found!
                                break
                            else:
                                # No data found - mark for API fallback and exit retry loop
                                log.warning(f"[SELENIUM_WORKER] [BLANK_RESULT] Selenium returned blank result for {in_company} | {in_product}, marking for API fallback")
                                # If Selenium extracts NO values:
                                # Source = api, Scraped_By_Selenium = yes, Scraped_By_API = no (pending API)
                                update_prepared_urls_source(
                                    in_company,
                                    in_product,
                                    new_source="api",
                                    scraped_by_selenium="yes",
                                    scraped_by_api="no",
                                    selenium_records="0",
                                    api_records="0",
                                )
                                save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                                product_not_found = True
                                
                                # Write progress with 0 records to prevent reprocessing
                                try:
                                    append_progress(in_company, in_product, 0)
                                    with _skip_lock:
                                        skip_set.add((nk(in_company), nk(in_product)))
                                except Exception:
                                    pass
                                
                                # Increment progress counter on completion (no data found)
                                with _progress_lock:
                                    _products_completed += 1
                                    completed = _products_completed
                                    total = _total_products
                                note_product_complete(product_start)
                                log_progress_with_step(f"Completed (no data): {in_product}", completed, total)
                                
                                # Rate limiting: pause after completing each product to avoid overwhelming server
                                rate_limit_pause()
                                
                                # Mark as processed and move to next product
                                # task_done() will be called in finally block
                                continue  # Move to next product in queue
                        
                        # Check if retry loop exceeded max iterations
                        if retry_loop_count >= max_retry_loops and not data_found and not product_not_found:
                            log.warning(f"[SELENIUM_WORKER] [MAX_RETRIES] Maximum retry loops ({max_retry_loops}) reached for {in_company} | {in_product}, marking for API fallback")
                            # If Selenium extracts NO values (timeout after retries):
                            # Source = api, Scraped_By_Selenium = yes, Scraped_By_API = no (pending API)
                            update_prepared_urls_source(
                                in_company,
                                in_product,
                                new_source="api",
                                scraped_by_selenium="yes",
                                scraped_by_api="no",
                                selenium_records="0",
                                api_records="0",
                            )
                            save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                            product_not_found = True
                            # Write progress with 0 records to prevent reprocessing
                            try:
                                append_progress(in_company, in_product, 0)
                                with _skip_lock:
                                    skip_set.add((nk(in_company), nk(in_product)))
                            except Exception:
                                pass
                            
                            # Increment progress counter on completion (max retries reached)
                            with _progress_lock:
                                _products_completed += 1
                                completed = _products_completed
                                total = _total_products
                            note_product_complete(product_start)
                            log_progress_with_step(f"Completed (max retries): {in_product}", completed, total)
                            
                            # Mark as processed and move to next product
                            # task_done() will be called in finally block
                            continue  # Move to next product in queue
                        
                        # After retry loop: if we got data or determined no data, mark success
                        # BUT: if driver was restarted, we need to requeue the item
                        if driver_restarted:
                            if SELENIUM_SINGLE_ATTEMPT:
                                log.warning(f"[FATAL_DRIVER] Thread {thread_id}: Driver restarted, moving to API (single-attempt): {in_company} | {in_product}")
                                mark_api_pending(in_company, in_product)
                                try:
                                    append_progress(in_company, in_product, 0)
                                    with _skip_lock:
                                        skip_set.add((nk(in_company), nk(in_product)))
                                except Exception:
                                    pass
                                with _progress_lock:
                                    _products_completed += 1
                                    completed = _products_completed
                                    total = _total_products
                                note_product_complete(product_start)
                                log_progress_with_step(f"Completed (driver->API): {in_product}", completed, total)
                                continue  # Skip requeue
                            # Driver was restarted in inner loop, requeue item for retry with new driver
                            log.info(f"[FATAL_DRIVER] Thread {thread_id}: Driver restarted in retry loop, requeueing {in_company} | {in_product}")
                            # Check round-robin retry logic
                            if SELENIUM_ROUND_ROBIN_RETRY and not should_requeue_for_round_robin(in_company, in_product):
                                # Max attempts reached, move to API
                                mark_api_pending(in_company, in_product)
                                with _skip_lock:
                                    skip_set.add((nk(in_company), nk(in_product)))
                                with _progress_lock:
                                    _products_completed += 1
                                note_product_complete(product_start)
                                task_done_called = True
                                selenium_queue.task_done()
                                continue
                            # For requeue: call task_done() explicitly and set flag to prevent double call
                            selenium_queue.task_done()
                            task_done_called = True
                            if _shutdown_requested.is_set():
                                log.warning(f"[FATAL_DRIVER] Thread {thread_id}: Shutdown requested; not requeueing {in_company} | {in_product}")
                                break
                            selenium_queue.put(item)  # Put item back for retry (will have its own task_done() when processed)
                            continue  # Continue to next item
                        
                        if data_found or product_not_found:
                            # In single-attempt mode: mark API pending when product not found
                            # In round-robin mode with max attempts: also mark API pending
                            if product_not_found and not data_found:
                                if SELENIUM_SINGLE_ATTEMPT:
                                    mark_api_pending(in_company, in_product)
                                    with _skip_lock:
                                        skip_set.add((nk(in_company), nk(in_product)))
                                elif SELENIUM_ROUND_ROBIN_RETRY:
                                    # Check if max attempts reached
                                    if not should_requeue_for_round_robin(in_company, in_product):
                                        mark_api_pending(in_company, in_product)
                                        with _skip_lock:
                                            skip_set.add((nk(in_company), nk(in_product)))
                            success = True
                            # If product_not_found but counter not yet incremented, increment now
                            # (blank result and max retries paths already increment, but error paths that break early might not)
                            if product_not_found and not data_found:
                                # Check current counter before incrementing - if it matches expected, don't double-increment
                                # Blank result and max retries paths increment above, error paths need increment here
                                # This is a safety net to ensure counter is incremented for all completion paths
                                with _progress_lock:
                                    # Only increment if we're sure we haven't already (check if this is from error path)
                                    # The blank result and max retries cases already incremented, so only increment for error paths
                                    # We'll increment here to be safe - if already incremented in blank/max paths, that's okay
                                    # The duplicate increment prevention would require tracking, which is more complex
                                    # For now, we rely on the fact that blank/max paths use continue, so they won't reach here
                                    _products_completed += 1
                                    completed = _products_completed
                                    total = _total_products
                                note_product_complete(product_start)
                                log_progress_with_step(f"Completed (error/no data): {in_product}", completed, total)
                        # If retry loop ended without success (error/timeout), success remains False
                        
                    except TimeoutException as te:
                        retry_count += 1
                        if retry_count > max_retries:
                            log.error(f"[SELENIUM_WORKER] [TIMEOUT] {in_company} | {in_product} - All {max_retries} retries exhausted, moving to API queue")
                            try:
                                # If API steps are disabled, keep the row eligible for Selenium retries
                                # in later rounds / next runs (Source=selenium, Scraped_By_Selenium=no).
                                if USE_API_STEPS:
                                    mark_api_pending(in_company, in_product)
                                else:
                                    update_prepared_urls_source(
                                        in_company,
                                        in_product,
                                        new_source="selenium",
                                        scraped_by_selenium="no",
                                        scraped_by_api="no",
                                        selenium_records="0",
                                        api_records="0",
                                    )
                                append_progress(in_company, in_product, 0)
                                if USE_API_STEPS:
                                    with _skip_lock:
                                        skip_set.add((nk(in_company), nk(in_product)))
                            except Exception:
                                pass
                            
                            # Increment progress counter
                            with _progress_lock:
                                _products_completed += 1
                                completed = _products_completed
                                total = _total_products
                            note_product_complete(product_start)
                            log_progress_with_step(f"Completed (timeout->API): {in_product}", completed, total)
                            
                            # Mark as processed and move to next product
                            # Note: task_done() will be called in finally block, so don't call it here
                            success = True  # Mark as "handled" to exit retry loop
                            break  # Exit the retry loop - will continue to next item
                        log.warning(f"[SELENIUM_WORKER] [TIMEOUT] {in_company} | {in_product} - Retry {retry_count}/{max_retries}")
                    except Exception as e:
                        raise
                
                # After retry loop: if timeout was handled (success=True), continue to next item
                # This prevents falling through to exception handlers that might requeue
                if success and retry_count > max_retries:
                    # Timeout was handled, product marked as failed, continue to next item
                    continue
                        
            except (InvalidSessionIdException, WebDriverException) as driver_error:
                # Check if this is a fatal driver error that requires restart
                if is_fatal_driver_error(driver_error) or not is_driver_alive(driver):
                    log.error(f"[FATAL_DRIVER] Thread {thread_id}: {type(driver_error).__name__}: {driver_error}")
                    
                    if _shutdown_requested.is_set():
                        log.info(f"[SELENIUM_WORKER] Driver session ended (shutdown requested) for {in_company} | {in_product}")
                        # Write progress/error before exiting to prevent reprocessing
                        if search_attempted:
                            try:
                                append_error(in_company, in_product, f"Driver error during shutdown: {driver_error}")
                                append_progress(in_company, in_product, 0)
                                with _skip_lock:
                                    skip_set.add((nk(in_company), nk(in_product)))
                            except Exception:
                                pass
                            selenium_queue.task_done()
                        break
                    
                    if SELENIUM_SINGLE_ATTEMPT:
                        log.warning(f"[FATAL_DRIVER] Thread {thread_id}: Moving to API (single-attempt): {in_company} | {in_product}")
                        mark_api_pending(in_company, in_product)
                        try:
                            append_error(in_company, in_product, f"Driver error: {driver_error}")
                            append_progress(in_company, in_product, 0)
                            with _skip_lock:
                                skip_set.add((nk(in_company), nk(in_product)))
                        except Exception:
                            pass
                        with _progress_lock:
                            _products_completed += 1
                            completed = _products_completed
                            total = _total_products
                        note_product_complete(product_start)
                        log_progress_with_step(f"Completed (driver->API): {in_product}", completed, total)
                        selenium_queue.task_done()
                        task_done_called = True
                        continue

                    # Restart driver and requeue the item
                    log.warning(f"[FATAL_DRIVER] Thread {thread_id}: Restarting driver and requeueing {in_company} | {in_product}")
                    driver = restart_driver(thread_id, driver, args.headless)
                    if driver is None:
                        # Failed to restart, mark as error and continue
                        try:
                            append_error(in_company, in_product, f"Driver restart failed: {driver_error}")
                            append_progress(in_company, in_product, 0)
                            with _skip_lock:
                                skip_set.add((nk(in_company), nk(in_product)))
                        except Exception:
                            pass
                        selenium_queue.task_done()
                        continue
                    
                    # Requeue the item to retry with new driver
                    # Check round-robin retry logic
                    if SELENIUM_ROUND_ROBIN_RETRY and not should_requeue_for_round_robin(in_company, in_product):
                        # Max attempts reached, move to API
                        mark_api_pending(in_company, in_product)
                        with _skip_lock:
                            skip_set.add((nk(in_company), nk(in_product)))
                        with _progress_lock:
                            _products_completed += 1
                        note_product_complete(product_start)
                        task_done_called = True
                        selenium_queue.task_done()
                        continue
                    # For requeue: call task_done() explicitly and set flag to prevent double call
                    selenium_queue.task_done()
                    task_done_called = True
                    selenium_queue.put(item)  # Then requeue (will have its own task_done() when processed)
                    log.info(f"[FATAL_DRIVER] Thread {thread_id}: Requeued {in_company} | {in_product} for retry with new driver")
                    continue
                
                # If not fatal, treat as normal error
                if _shutdown_requested.is_set():
                    log.info(f"[SELENIUM_WORKER] Driver session ended (shutdown requested) for {in_company} | {in_product}")
                    # Write progress/error before exiting to prevent reprocessing
                    if search_attempted:
                        try:
                            append_error(in_company, in_product, f"Driver error during shutdown: {driver_error}")
                            append_progress(in_company, in_product, 0)
                            with _skip_lock:
                                skip_set.add((nk(in_company), nk(in_product)))
                        except Exception:
                            pass
                        selenium_queue.task_done()
                    break
                
                msg = f"{type(driver_error).__name__}: {driver_error}"
                try:
                    # In single-attempt mode: mark API pending
                    # In round-robin mode with max attempts: also mark API pending
                    if SELENIUM_SINGLE_ATTEMPT:
                        mark_api_pending(in_company, in_product)
                    elif SELENIUM_ROUND_ROBIN_RETRY and not should_requeue_for_round_robin(in_company, in_product):
                        mark_api_pending(in_company, in_product)
                    append_error(in_company, in_product, msg)
                    append_progress(in_company, in_product, 0)
                    with _skip_lock:
                        skip_set.add((nk(in_company), nk(in_product)))
                    save_debug(driver, DEBUG_ERR, f"{in_company}_{in_product}")
                except Exception:
                    pass  # Driver is dead, can't save debug
                log.error(f"[SELENIUM_WORKER] [ERROR] {in_company} | {in_product}: {msg}")
                
                # Log progress for failed product (counter already incremented at start if search_attempted)
                if search_attempted:
                    with _progress_lock:
                        completed = _products_completed
                        total = _total_products
                    
                    # Log progress for failed product
                    log_progress_with_step(f"Failed: {in_product}", completed, total)
            except (ConnectionResetError, socket.error) as conn_error:
                # Network/connection errors - check if driver is dead
                if URLLIB3_AVAILABLE and isinstance(conn_error, ProtocolError):
                    if is_fatal_driver_error(conn_error) or (driver and not is_driver_alive(driver)):
                        log.error(f"[FATAL_DRIVER] Thread {thread_id}: Connection error (likely driver dead): {conn_error}")
                        if _shutdown_requested.is_set():
                            if search_attempted:
                                try:
                                    append_error(in_company, in_product, f"Connection error during shutdown: {conn_error}")
                                    append_progress(in_company, in_product, 0)
                                    with _skip_lock:
                                        skip_set.add((nk(in_company), nk(in_product)))
                                except Exception:
                                    pass
                                selenium_queue.task_done()
                            break
                        if SELENIUM_SINGLE_ATTEMPT:
                            log.warning(f"[FATAL_DRIVER] Thread {thread_id}: Moving to API after connection error (single-attempt): {in_company} | {in_product}")
                            mark_api_pending(in_company, in_product)
                            try:
                                append_error(in_company, in_product, f"Connection error: {conn_error}")
                                append_progress(in_company, in_product, 0)
                                with _skip_lock:
                                    skip_set.add((nk(in_company), nk(in_product)))
                            except Exception:
                                pass
                            with _progress_lock:
                                _products_completed += 1
                                completed = _products_completed
                                total = _total_products
                            note_product_complete(product_start)
                            log_progress_with_step(f"Completed (conn->API): {in_product}", completed, total)
                            selenium_queue.task_done()
                            task_done_called = True
                            continue
                        # Restart driver and requeue
                        driver = restart_driver(thread_id, driver, args.headless)
                        if driver is None:
                            try:
                                append_error(in_company, in_product, f"Driver restart failed after connection error: {conn_error}")
                                append_progress(in_company, in_product, 0)
                                with _skip_lock:
                                    skip_set.add((nk(in_company), nk(in_product)))
                            except Exception:
                                pass
                            selenium_queue.task_done()
                            continue
                        # Check round-robin retry logic
                        if SELENIUM_ROUND_ROBIN_RETRY and not should_requeue_for_round_robin(in_company, in_product):
                            # Max attempts reached, move to API
                            mark_api_pending(in_company, in_product)
                            with _skip_lock:
                                skip_set.add((nk(in_company), nk(in_product)))
                            with _progress_lock:
                                _products_completed += 1
                            note_product_complete(product_start)
                            task_done_called = True
                            selenium_queue.task_done()
                            continue
                        # For requeue: call task_done() explicitly and set flag to prevent double call
                        selenium_queue.task_done()
                        task_done_called = True
                        selenium_queue.put(item)  # Then requeue (will have its own task_done() when processed)
                        log.info(f"[FATAL_DRIVER] Thread {thread_id}: Requeued {in_company} | {in_product} after connection error")
                        continue
                
                # Not a fatal driver error, treat as normal error
                msg = f"{type(conn_error).__name__}: {conn_error}"
                try:
                    # In single-attempt mode: mark API pending
                    # In round-robin mode with max attempts: also mark API pending
                    if SELENIUM_SINGLE_ATTEMPT:
                        mark_api_pending(in_company, in_product)
                    elif SELENIUM_ROUND_ROBIN_RETRY and not should_requeue_for_round_robin(in_company, in_product):
                        mark_api_pending(in_company, in_product)
                    append_error(in_company, in_product, msg)
                    append_progress(in_company, in_product, 0)
                    with _skip_lock:
                        skip_set.add((nk(in_company), nk(in_product)))
                except Exception:
                    pass
                log.error(f"[SELENIUM_WORKER] [ERROR] {in_company} | {in_product}: {msg}")
                if search_attempted:
                    with _progress_lock:
                        completed = _products_completed
                        total = _total_products
                    log_progress_with_step(f"Failed: {in_product}", completed, total)
            except Exception as e:
                # Check if this might be a fatal driver error
                if is_fatal_driver_error(e) or (driver and not is_driver_alive(driver)):
                    log.error(f"[FATAL_DRIVER] Thread {thread_id}: {type(e).__name__}: {e}")
                    if _shutdown_requested.is_set():
                        log.info(f"[SELENIUM_WORKER] Shutdown requested during error handling for {in_company} | {in_product}")
                        # Write progress/error before exiting to prevent reprocessing
                        if search_attempted:
                            try:
                                msg = f"{type(e).__name__}: {e}"
                                append_error(in_company, in_product, msg)
                                append_progress(in_company, in_product, 0)
                                with _skip_lock:
                                    skip_set.add((nk(in_company), nk(in_product)))
                            except Exception:
                                pass
                            selenium_queue.task_done()
                        break
                    if SELENIUM_SINGLE_ATTEMPT:
                        log.warning(f"[FATAL_DRIVER] Thread {thread_id}: Moving to API after fatal error (single-attempt): {in_company} | {in_product}")
                        mark_api_pending(in_company, in_product)
                        try:
                            msg = f"{type(e).__name__}: {e}"
                            append_error(in_company, in_product, msg)
                            append_progress(in_company, in_product, 0)
                            with _skip_lock:
                                skip_set.add((nk(in_company), nk(in_product)))
                        except Exception:
                            pass
                        with _progress_lock:
                            _products_completed += 1
                            completed = _products_completed
                            total = _total_products
                        note_product_complete(product_start)
                        log_progress_with_step(f"Completed (fatal->API): {in_product}", completed, total)
                        selenium_queue.task_done()
                        task_done_called = True
                        continue
                    # Restart driver and requeue
                    # Check round-robin retry logic first
                    if SELENIUM_ROUND_ROBIN_RETRY and not should_requeue_for_round_robin(in_company, in_product):
                        # Max attempts reached, move to API
                        mark_api_pending(in_company, in_product)
                        with _skip_lock:
                            skip_set.add((nk(in_company), nk(in_product)))
                        with _progress_lock:
                            _products_completed += 1
                        note_product_complete(product_start)
                        task_done_called = True
                        selenium_queue.task_done()
                        continue
                    driver = restart_driver(thread_id, driver, args.headless)
                    if driver is None:
                        try:
                            append_error(in_company, in_product, f"Driver restart failed: {e}")
                            append_progress(in_company, in_product, 0)
                            with _skip_lock:
                                skip_set.add((nk(in_company), nk(in_product)))
                        except Exception:
                            pass
                            selenium_queue.task_done()
                            continue
                    # For requeue: call task_done() explicitly and set flag to prevent double call
                    selenium_queue.task_done()
                    task_done_called = True
                    selenium_queue.put(item)  # Then requeue (will have its own task_done() when processed)
                    log.info(f"[FATAL_DRIVER] Thread {thread_id}: Requeued {in_company} | {in_product} after fatal error")
                    continue
                
                # Check if shutdown was requested during error handling
                if _shutdown_requested.is_set():
                    log.info(f"[SELENIUM_WORKER] Shutdown requested during error handling for {in_company} | {in_product}")
                    # Write progress/error before exiting to prevent reprocessing
                    if search_attempted:
                        try:
                            msg = f"{type(e).__name__}: {e}"
                            append_error(in_company, in_product, msg)
                            append_progress(in_company, in_product, 0)
                            with _skip_lock:
                                skip_set.add((nk(in_company), nk(in_product)))
                        except Exception:
                            pass
                        selenium_queue.task_done()
                    break
                
                msg = f"{type(e).__name__}: {e}"
                try:
                    # In single-attempt mode: mark API pending
                    # In round-robin mode with max attempts: also mark API pending
                    if SELENIUM_SINGLE_ATTEMPT:
                        mark_api_pending(in_company, in_product)
                    elif SELENIUM_ROUND_ROBIN_RETRY and not should_requeue_for_round_robin(in_company, in_product):
                        mark_api_pending(in_company, in_product)
                    append_error(in_company, in_product, msg)
                    append_progress(in_company, in_product, 0)
                    with _skip_lock:
                        skip_set.add((nk(in_company), nk(in_product)))
                    save_debug(driver, DEBUG_ERR, f"{in_company}_{in_product}")
                except Exception:
                    pass  # Driver might be dead
                log.error(f"[SELENIUM_WORKER] [ERROR] {in_company} | {in_product}: {msg}")
                import traceback
                log.error(f"[SELENIUM_WORKER] [ERROR] Traceback: {traceback.format_exc()}")
                
                # Log progress for failed product (counter already incremented at start if search_attempted)
                if search_attempted:
                    with _progress_lock:
                        completed = _products_completed
                        total = _total_products
                    
                    # Log progress for failed product
                    log_progress_with_step(f"Failed: {in_product}", completed, total)
            finally:
                # Only call task_done() if it wasn't already called explicitly (e.g., for requeue cases)
                if not task_done_called:
                    selenium_queue.task_done()
    
    finally:
        # Clean up driver on shutdown or normal exit
        if driver:
            try:
                log.info(f"[SELENIUM_WORKER] Thread {thread_id} cleaning up driver...")
                
                # Unregister driver first to prevent close_all_drivers() from trying to close it again
                unregister_driver(driver)
                
                # Get Firefox/geckodriver PIDs before closing (if driver is still alive)
                pids_to_kill = set()
                try:
                    if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                        geckodriver_pid = driver.service.process.pid
                        if geckodriver_pid:
                            pids_to_kill.add(geckodriver_pid)
                            if psutil:
                                try:
                                    parent = psutil.Process(geckodriver_pid)
                                    for child in parent.children(recursive=True):
                                        pids_to_kill.add(child.pid)
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass
                except Exception:
                    pass  # Driver might already be dead
                
                # Try to quit gracefully first (before killing processes)
                # Use a timeout to prevent hanging on quit() if Firefox is already dead
                try:
                    clear_browser_storage(driver)
                    # Check if driver is still alive before attempting quit
                    if is_driver_alive(driver):
                        # Set a short timeout for quit() to prevent hanging
                        old_timeout = socket.getdefaulttimeout()
                        socket.setdefaulttimeout(3)  # 3 second timeout for quit()
                        try:
                            driver.quit()
                        finally:
                            socket.setdefaulttimeout(old_timeout)  # Reset timeout
                    else:
                        log.debug(f"[SELENIUM_WORKER] Driver already dead, skipping quit()")
                except Exception as e:
                    # Driver quit failed, which is expected if Firefox was already killed
                    # Only log if it's not a connection-related error
                    error_msg = str(e).lower()
                    if "connection" not in error_msg and "session" not in error_msg:
                        log.debug(f"[SELENIUM_WORKER] Driver quit() failed (expected): {e}")
                finally:
                    cleanup_temp_profile(getattr(driver, "_profile_dir", None))
                
                # Kill Firefox/geckodriver processes if we found any (or if quit failed)
                if pids_to_kill and psutil:
                    log.info(f"[SELENIUM_WORKER] Thread {thread_id}: Killing Firefox/geckodriver PIDs (Alfabeta only): {sorted(pids_to_kill)}")
                    # Only kill tracked PIDs (Alfabeta scraper only)
                    with _tracked_pids_lock:
                        for pid in list(pids_to_kill):  # Iterate over copy
                            if pid in _tracked_firefox_pids:
                                try:
                                    proc = psutil.Process(pid)
                                    proc.kill()
                                    log.info(f"[SELENIUM_WORKER] Killed Firefox/geckodriver process (Alfabeta): PID {pid}")
                                    _tracked_firefox_pids.discard(pid)  # Remove from tracked set after killing
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass
            except Exception as e:
                # Don't log warnings for expected errors (driver already dead, etc.)
                error_msg = str(e).lower()
                if "session" not in error_msg and "connection" not in error_msg and "target window" not in error_msg:
                    log.warning(f"[SELENIUM_WORKER] Error closing driver: {e}")
        
        if _shutdown_requested.is_set():
            log.warning(f"[SELENIUM_WORKER] Thread {thread_id} completed (shutdown requested)")
        else:
            log.info(f"[SELENIUM_WORKER] Thread {thread_id} completed")

def mark_pipeline_step_if_standalone():
    """Mark checkpoint for Step 3 when running outside the pipeline runner."""
    if os.environ.get("PIPELINE_RUNNER") == "1":
        return
    try:
        cp = get_checkpoint_manager("Argentina")
        cp.mark_step_complete(
            3,
            "Scrape Products (Selenium)",
            output_files=None
        )
    except Exception as exc:
        log.warning(f"[CHECKPOINT] Failed to mark pipeline step: {exc}")


if __name__ == "__main__":
    import sys
    exit_code = None
    try:
        exit_code = main()
        if exit_code is None:
            exit_code = 0
        if exit_code == 0:
            mark_pipeline_step_if_standalone()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        log.warning("[MAIN] Keyboard interrupt received, cleaning up...")
        _shutdown_requested.set()
        close_all_drivers()
        sys.exit(1)
    except SystemExit:
        # Ensure cleanup on system exit
        _shutdown_requested.set()
        close_all_drivers()
        raise
    except Exception as e:
        log.error(f"[MAIN] Fatal error: {e}", exc_info=True)
        _shutdown_requested.set()
        close_all_drivers()
        sys.exit(1)


