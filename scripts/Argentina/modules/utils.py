"""
Argentina Scraper Utilities
Helper functions for data parsing, validation, and resource monitoring
"""

import re
import time
import random
import os
import gc
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional
import logging

try:
    import psutil
except ImportError:
    psutil = None

from selenium.common.exceptions import InvalidSessionIdException, WebDriverException
from urllib3.exceptions import ProtocolError

from .config import REQUEST_PAUSE_BASE, REQUEST_PAUSE_JITTER, MEMORY_LIMIT_MB

log = logging.getLogger("selenium_scraper")

# Temp profile tracking
_temp_profile_dirs = set()
_temp_profile_lock = threading.Lock()


# ==================== TEXT NORMALIZATION ====================

def normalize_ws(s: Optional[str]) -> Optional[str]:
    """Normalize whitespace in string"""
    if s is None:
        return None
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()


def ar_money_to_float(s: str) -> Optional[float]:
    """Convert Argentine money format to float (dot thousands, comma decimals)"""
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
    """Parse date from various formats to ISO format
    
    Accepts '(24/07/25)' or '24/07/25' or '24-07-2025' -> '2025-07-24'
    """
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


# ==================== TIMING & DELAYS ====================

def rate_limit_pause():
    """Rate limiting pause between requests"""
    time.sleep(REQUEST_PAUSE_BASE + random.uniform(*REQUEST_PAUSE_JITTER))


# ==================== RESOURCE MONITORING ====================

def get_resource_usage():
    """Get current resource usage for monitoring"""
    try:
        if psutil:
            process = psutil.Process(os.getpid())
            mem_mb = process.memory_info().rss / 1024 / 1024
            threads = threading.active_count()
            return mem_mb, threads
    except Exception:
        pass
    return 0, threading.active_count()


def log_resource_usage(prefix="[RESOURCE]"):
    """Log current memory and thread usage"""
    mem_mb, threads = get_resource_usage()
    log.info(f"{prefix} Memory: {mem_mb:.1f}MB, Threads: {threads}")


def get_memory_usage_mb() -> float:
    """Get current process memory usage in MB"""
    try:
        if psutil:
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / 1024 / 1024
    except Exception:
        pass
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
    # Clear any unreferenced temp profiles
    with _temp_profile_lock:
        dead_profiles = []
        for profile_dir in list(_temp_profile_dirs):
            path = Path(profile_dir)
            if not path.exists():
                dead_profiles.append(profile_dir)
        for profile_dir in dead_profiles:
            _temp_profile_dirs.discard(profile_dir)


# ==================== DRIVER HEALTH CHECKS ====================

def is_driver_alive(driver) -> bool:
    """Check if driver is still alive by attempting a cheap operation"""
    if driver is None:
        return False
    try:
        _ = driver.current_url  # cheap ping
        return True
    except (InvalidSessionIdException, WebDriverException, AttributeError, Exception):
        return False


def is_fatal_driver_error(e: Exception) -> bool:
    """Check if exception indicates driver is fatally dead"""
    from .config import FATAL_DRIVER_SUBSTRINGS
    
    msg = (str(e) or "").lower()
    error_type = type(e).__name__.lower()
    
    # Check error message
    if any(s in msg for s in FATAL_DRIVER_SUBSTRINGS):
        return True
    
    # Check error type
    if "protocol" in error_type or "connection" in error_type:
        return True
    
    # Check for specific exception types
    if isinstance(e, (InvalidSessionIdException, ConnectionResetError)):
        return True
    
    try:
        if isinstance(e, ProtocolError):
            return True
    except:
        pass
    
    return False


# ==================== DEBUG UTILITIES ====================

def save_debug(driver, folder: Path, tag: str):
    """Save screenshot and HTML for debugging"""
    try:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        png = folder / f"{tag}_{stamp}.png"
        html = folder / f"{tag}_{stamp}.html"
        driver.save_screenshot(str(png))
        html.write_text(driver.page_source, encoding="utf-8")
    except Exception as e:
        log.warning(f"Could not save debug for {tag}: {e}")


def check_connection_with_retry(driver, url: str, max_retries: int = 3) -> bool:
    """Check if connection is working. If not, wait 2 min and retry.
    
    Returns True if connection works, False if all retries fail.
    """
    for attempt in range(max_retries):
        try:
            log.info(f"[CONNECTION_CHECK] Attempt {attempt + 1}/{max_retries}: Testing connection to {url}")
            driver.set_page_load_timeout(30)  # 30 second timeout
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
