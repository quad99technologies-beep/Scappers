#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
India NPPA Pharma Sahi Daam Scraper - Get Medicine Details

Searches for formulations, downloads Excel exports, and extracts detailed
medicine information including substitutes/available brands.

Features:
- Loads formulations from input/India/formulations.csv
- Chrome instance management with PID tracking
- Humanization and anti-bot measures
- Rich progress bar support
- Resume support: skips fully completed formulations
- Handles partial scrapes without duplicating data
- Generates final summary report
"""

import os
import re
import sys
import time
import csv
import json
import random
import atexit
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Set

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# Force unbuffered output for real-time progress
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)
os.environ.setdefault('PYTHONUNBUFFERED', '1')

# Add repo root to path for core imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/India to path for local imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Import platform components
from config_loader import (
    get_output_dir, get_input_dir, get_download_dir, get_repo_root,
    getenv, getenv_bool, getenv_int, getenv_float, load_env_file, SCRAPER_ID
)

# Load environment configuration early
load_env_file()

# Import Chrome PID tracker for proper cleanup
try:
    from core.chrome_pid_tracker import (
        get_chrome_pids_from_driver, save_chrome_pids,
        terminate_chrome_pids, cleanup_pid_file
    )
    _PID_TRACKER_AVAILABLE = True
except ImportError:
    _PID_TRACKER_AVAILABLE = False
    def get_chrome_pids_from_driver(driver): return set()
    def save_chrome_pids(name, root, pids): pass
    def terminate_chrome_pids(name, root, silent=False): return 0
    def cleanup_pid_file(name, root): pass

# Import Chrome manager for additional cleanup
try:
    from core.chrome_manager import register_chrome_driver, cleanup_all_chrome_instances
    _CHROME_MANAGER_AVAILABLE = True
except ImportError:
    _CHROME_MANAGER_AVAILABLE = False
    def register_chrome_driver(driver): pass
    def cleanup_all_chrome_instances(silent=False): pass

# Import human actions for anti-bot
try:
    from core.human_actions import pause, type_delay
    _HUMAN_ACTIONS_AVAILABLE = True
except ImportError:
    _HUMAN_ACTIONS_AVAILABLE = False
    def pause(min_s=0.2, max_s=0.6):
        if getenv_bool("HUMAN_ACTIONS_ENABLED", False):
            time.sleep(random.uniform(min_s, max_s))
    def type_delay():
        if getenv_bool("HUMAN_ACTIONS_ENABLED", False):
            return random.uniform(0.05, 0.15)
        return 0

# Import stealth profile for anti-detection
try:
    from core.stealth_profile import apply_selenium
    _STEALTH_PROFILE_AVAILABLE = True
except ImportError:
    _STEALTH_PROFILE_AVAILABLE = False
    def apply_selenium(options): pass

# Import rich progress for beautiful output
try:
    from core.rich_progress import (
        create_progress, console, print_status as _rich_print_status,
        ScraperProgress, print_summary, format_duration
    )
    _RICH_PROGRESS_AVAILABLE = True

    # Wrap print_status to handle Windows encoding issues
    def print_status(msg, status="info"):
        try:
            _rich_print_status(msg, status)
        except UnicodeEncodeError:
            # Fallback for Windows console encoding issues with emoji
            icons = {"info": "[INFO]", "success": "[OK]", "warning": "[WARN]", "error": "[ERROR]"}
            print(f"{icons.get(status, '[INFO]')} {msg}", flush=True)

except ImportError:
    _RICH_PROGRESS_AVAILABLE = False
    def print_status(msg, status="info"): print(f"[{status.upper()}] {msg}", flush=True)
    def format_duration(s): return f"{s:.1f}s"

URL = "https://nppaipdms.gov.in/NPPA/PharmaSahiDaam/searchMedicine"

# -----------------------------
# Checkpoint/Resume System for Formulations
# -----------------------------
class FormulationCheckpoint:
    """Manages checkpoint/resume for formulation-level processing."""
    
    def __init__(self, output_dir: Path):
        self.checkpoint_dir = output_dir / ".checkpoints"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / "formulation_progress.json"
        self._data = None

    @staticmethod
    def _key(formulation: str) -> str:
        return (formulation or "").strip().upper()
    
    def _load(self) -> Dict:
        """Load checkpoint data."""
        if self._data is not None:
            return self._data
        
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    self._data = json.load(f)
            except Exception as e:
                print(f"[WARN] Failed to load formulation checkpoint: {e}")
                self._data = self._default_data()
        else:
            self._data = self._default_data()

        # Ensure required keys exist (supports older checkpoint schemas)
        if not isinstance(self._data, dict):
            self._data = self._default_data()
            return self._data

        self._data.setdefault("schema_version", 2)
        self._data.setdefault("completed_formulations", [])
        self._data.setdefault("zero_record_formulations", [])
        self._data.setdefault("failed_formulations", {})
        self._data.setdefault("in_progress", None)
        self._data.setdefault("last_updated", None)
        self._data.setdefault("stats", {})

        stats = self._data["stats"]
        had_total_success = "total_success" in stats
        had_total_zero_records = "total_zero_records" in stats

        stats.setdefault("total_processed", 0)  # terminal (success + zero-record)
        stats.setdefault("total_success", 0)
        stats.setdefault("total_zero_records", 0)
        stats.setdefault("total_medicines", 0)
        stats.setdefault("total_substitutes", 0)
        stats.setdefault("errors", 0)  # failures only

        # Best-effort migration from older schema (which only tracked "total_processed" as success count).
        if not had_total_success and stats.get("total_success", 0) == 0:
            stats["total_success"] = int(stats.get("total_processed", 0) or 0)
        if not had_total_zero_records:
            stats["total_zero_records"] = int(stats.get("total_zero_records", 0) or 0)

        return self._data
    
    def _default_data(self) -> Dict:
        return {
            "schema_version": 2,
            "completed_formulations": [],
            "zero_record_formulations": [],
            "failed_formulations": {},
            "in_progress": None,
            "last_updated": None,
            "stats": {
                "total_processed": 0,  # terminal (success + zero-record)
                "total_success": 0,
                "total_zero_records": 0,
                "total_medicines": 0,
                "total_substitutes": 0,
                "errors": 0  # failures only
            }
        }
    
    def _save(self):
        """Save checkpoint data atomically."""
        data = self._load()
        data["last_updated"] = datetime.now().isoformat()
        
        try:
            temp_file = self.checkpoint_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            temp_file.replace(self.checkpoint_file)
        except Exception as e:
            print(f"[ERROR] Failed to save formulation checkpoint: {e}")
    
    def is_completed(self, formulation: str) -> bool:
        """
        Check if a formulation is in a terminal state (success OR confirmed zero-record).

        Note: Historically this meant "success". We treat "zero-record" as terminal too so
        resume runs don't keep re-trying formulations that legitimately have no results.
        """
        data = self._load()
        k = self._key(formulation)
        if not k:
            return False
        if k in [self._key(f) for f in data.get("completed_formulations", [])]:
            return True
        if k in [self._key(f) for f in data.get("zero_record_formulations", [])]:
            return True
        return False
    
    def mark_completed(self, formulation: str, medicines_count: int = 0, substitutes_count: int = 0):
        self.mark_success(formulation, medicines_count, substitutes_count)

    def mark_success(self, formulation: str, medicines_count: int = 0, substitutes_count: int = 0):
        """Mark a formulation as successfully completed (results found or already captured)."""
        data = self._load()
        formulation_upper = self._key(formulation)
        if not formulation_upper:
            return

        # Clear failed/zero-record state if present
        data["failed_formulations"].pop(formulation_upper, None)
        data["zero_record_formulations"] = [
            f for f in data.get("zero_record_formulations", []) if self._key(f) != formulation_upper
        ]

        first_time = formulation_upper not in [self._key(f) for f in data["completed_formulations"]]
        if first_time:
            data["completed_formulations"].append(formulation)
            data["stats"]["total_processed"] += 1
            data["stats"]["total_success"] += 1

        # Add record counts (best-effort) – caller should pass only "new" counts.
        data["stats"]["total_medicines"] += medicines_count
        data["stats"]["total_substitutes"] += substitutes_count

        # Clear in_progress if it matches
        if data["in_progress"] and self._key(data["in_progress"]) == formulation_upper:
            data["in_progress"] = None

        self._save()
        print(f"[CHECKPOINT] Marked formulation '{formulation}' as completed")

    def mark_zero_record(self, formulation: str, reason: Optional[str] = None):
        """Mark a formulation as terminal with no records found (do not retry on resume)."""
        data = self._load()
        formulation_upper = self._key(formulation)
        if not formulation_upper:
            return

        # Clear failed/success state if present
        data["failed_formulations"].pop(formulation_upper, None)
        data["completed_formulations"] = [
            f for f in data.get("completed_formulations", []) if self._key(f) != formulation_upper
        ]

        first_time = formulation_upper not in [self._key(f) for f in data["zero_record_formulations"]]
        if first_time:
            data["zero_record_formulations"].append(formulation)
            data["stats"]["total_processed"] += 1
            data["stats"]["total_zero_records"] += 1

        # Clear in_progress if it matches
        if data["in_progress"] and self._key(data["in_progress"]) == formulation_upper:
            data["in_progress"] = None

        self._save()
        msg = f"[CHECKPOINT] Marked formulation '{formulation}' as zero-record"
        if reason:
            msg += f" ({reason})"
        print(msg)
    
    def mark_in_progress(self, formulation: str):
        """Mark a formulation as currently being processed."""
        data = self._load()
        data["in_progress"] = formulation
        self._save()
    
    def mark_error(self, formulation: str):
        self.mark_failed(formulation, error=None)

    def mark_failed(self, formulation: str, error: Optional[str] = None):
        """Record a failure for a formulation (will be retried on resume)."""
        data = self._load()
        data["stats"]["errors"] += 1

        k = self._key(formulation)
        if k:
            entry = data["failed_formulations"].get(k) or {
                "formulation": formulation,
                "attempts": 0,
                "last_error": None,
                "last_failed_at": None,
            }
            entry["attempts"] = int(entry.get("attempts", 0) or 0) + 1
            entry["last_error"] = error
            entry["last_failed_at"] = datetime.now().isoformat()
            data["failed_formulations"][k] = entry

        # Clear in_progress if it matches
        if data["in_progress"] and self._key(data["in_progress"]) == k:
            data["in_progress"] = None

        self._save()
    
    def get_completed_count(self) -> int:
        """Get count of completed formulations."""
        return len(self._load()["completed_formulations"])
    
    def get_stats(self) -> Dict:
        """Get processing statistics."""
        data = self._load()
        stats = data["stats"]
        stats["failed_formulations"] = len(data.get("failed_formulations", {}))
        stats["zero_record_formulations"] = len(data.get("zero_record_formulations", []))
        stats["success_formulations"] = len(data.get("completed_formulations", []))
        return stats
    
    def clear(self):
        """Clear all checkpoint data for fresh start."""
        self._data = self._default_data()
        self._save()
        print("[CHECKPOINT] Cleared formulation checkpoint data")


# -----------------------------
# Load formulations from input folder
# -----------------------------


def should_emit_progress() -> bool:
    return not getenv_bool("SUPPRESS_WORKER_PROGRESS", False)


def load_formulations_from_input() -> List[str]:
    """Load formulations from input/India/formulations.csv or an override path."""
    override_env = os.getenv("FORMULATIONS_FILE")
    override = (override_env or getenv("FORMULATIONS_FILE", "") or "").strip()
    if override:
        input_file = Path(override)
        if not input_file.is_absolute():
            input_file = get_input_dir() / input_file
    else:
        input_file = get_input_dir() / "formulations.csv"
    print(f"[INFO] Loading formulations from: {input_file}")

    if input_file.exists():
        try:
            df = pd.read_csv(input_file)
            # Look for column named 'formulation', 'Formulation', 'name', 'Name', or first column
            formulation_col = None
            for col in ['formulation', 'Formulation', 'name', 'Name', 'generic_name', 'Generic_Name']:
                if col in df.columns:
                    formulation_col = col
                    break

            # Use first column if no known column found
            if formulation_col is None:
                formulation_col = df.columns[0]

            formulations = df[formulation_col].dropna().astype(str).str.strip().tolist()
            # Filter out empty strings
            formulations = [f for f in formulations if f]

            # De-duplicate by normalized key to avoid repeated work in a single run
            seen = set()
            deduped = []
            dup_count = 0
            for f in formulations:
                k = f.strip().upper()
                if not k:
                    continue
                if k in seen:
                    dup_count += 1
                    continue
                seen.add(k)
                deduped.append(f)
            formulations = deduped

            if dup_count:
                print(f"[INFO] Removed {dup_count} duplicate formulation(s)")
            print(f"[OK] Loaded {len(formulations)} formulations from input file")

            # Show sample
            if formulations:
                sample = formulations[:5]
                safe_sample = [s.encode('ascii', 'replace').decode('ascii') for s in sample]
                print(f"[INFO] Sample formulations: {safe_sample}")

            return formulations
        except Exception as e:
            print(f"[ERROR] Failed to load formulations from {input_file}: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"[ERROR] Formulations file not found: {input_file}")

    return []


# Default formulations (used if no input file)
DEFAULT_FORMULATIONS = [
    "ABACAVIR",
]


# -----------------------------
# Small utilities
# -----------------------------
def slugify(s: str, max_len: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"[\\/:*?\"<>|]+", "_", s)  # windows illegal
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > max_len:
        s = s[:max_len].rstrip()
    return s


def click_js(driver, el):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    driver.execute_script("arguments[0].click();", el)


def latest_file(folder: Path) -> Optional[Path]:
    files = [p for p in folder.glob("*") if p.is_file()]
    return max(files, key=lambda p: p.stat().st_mtime) if files else None


def wait_for_download_complete(folder: Path, timeout_sec: int = 180) -> Path:
    end = time.time() + timeout_sec
    while time.time() < end:
        if list(folder.glob("*.crdownload")):
            time.sleep(1)
            continue

        f = latest_file(folder)
        if f:
            time.sleep(1)  # flush grace
            if not list(folder.glob("*.crdownload")):
                return f
        time.sleep(1)
    raise TimeoutError("Download did not complete in time.")


def build_driver(download_dir: Path, headless: bool = None) -> webdriver.Chrome:
    """Build Chrome WebDriver with proper configuration, PID tracking, and anti-bot measures."""
    download_dir.mkdir(parents=True, exist_ok=True)

    # Use config headless setting if not explicitly passed
    if headless is None:
        headless = getenv_bool("HEADLESS", False)

    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Apply stealth profile for anti-detection
    if _STEALTH_PROFILE_AVAILABLE:
        apply_selenium(options)
    else:
        # Manual stealth settings if module not available
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--lang=en-US,en")

    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=options)

    # Remove webdriver flag via CDP
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
    except Exception:
        pass

    # Track Chrome PIDs for cleanup
    if _PID_TRACKER_AVAILABLE:
        pids = get_chrome_pids_from_driver(driver)
        if pids:
            save_chrome_pids(SCRAPER_ID, _repo_root, pids)
            print(f"[PID] Tracking {len(pids)} Chrome process(es)")

    # Register with Chrome manager for additional cleanup
    if _CHROME_MANAGER_AVAILABLE:
        register_chrome_driver(driver)

    return driver


def cleanup_chrome(driver=None, silent: bool = False):
    """Cleanup Chrome instances - both driver and any tracked PIDs."""
    if driver:
        try:
            driver.quit()
        except Exception:
            pass

    # Terminate tracked PIDs
    if _PID_TRACKER_AVAILABLE:
        terminated = terminate_chrome_pids(SCRAPER_ID, _repo_root, silent=silent)
        if terminated > 0 and not silent:
            print(f"[PID] Terminated {terminated} Chrome process(es)")

    # Additional cleanup via Chrome manager
    if _CHROME_MANAGER_AVAILABLE:
        cleanup_all_chrome_instances(silent=silent)


# -----------------------------
# Page-specific robust waits
# -----------------------------
def wait_results_loaded(driver, wait: WebDriverWait):
    """
    Results table is injected into #pharmaDiv. Wait until table with medicine data appears.
    """
    def _ready(d):
        # Check for any table with Medicine Name links
        tables = d.find_elements(By.CSS_SELECTOR, "table")
        for table in tables:
            links = table.find_elements(By.CSS_SELECTOR, "a[data-toggle='modal']")
            if links:
                return True
        # Also check for DataTables
        if d.find_elements(By.CSS_SELECTOR, "#myDatatable"):
            return True
        if d.find_elements(By.CSS_SELECTOR, ".dataTables_wrapper"):
            return True
        return False

    wait.until(_ready)
    time.sleep(1.5)


def dismiss_alert_if_present(driver):
    """Dismiss any alert dialog that may be present."""
    try:
        from selenium.webdriver.common.alert import Alert
        alert = Alert(driver)
        alert_text = alert.text
        alert.accept()
        print(f"[WARN] Dismissed alert: {alert_text}")
        return True
    except:
        return False


def pick_autocomplete_exact_match(driver, wait: WebDriverWait, search_term: str, timeout: int = 10, max_scroll_attempts: int = 20) -> bool:
    """
    Wait for autocomplete dropdown and click on the item that exactly matches the search term.
    If no exact match found, click the first item.

    Handles large dropdowns by scrolling through items to find matches not initially visible.

    Args:
        driver: Selenium WebDriver
        wait: WebDriverWait instance
        search_term: The term to search for
        timeout: Max seconds to wait for dropdown to appear
        max_scroll_attempts: Max scroll attempts to find item in large dropdown

    Returns:
        True if autocomplete selection was successful, False otherwise
    """
    # Try multiple possible autocomplete selectors
    autocomplete_selectors = [
        ".autocomplete-suggestions div",  # NPPA uses this
        "ul.ui-autocomplete li.ui-menu-item",
        "ul.ui-autocomplete li",
        ".ui-autocomplete .ui-menu-item",
        ".ui-autocomplete-results li",
        "ul.ui-menu li.ui-menu-item",
        ".ui-menu-item",
        "[role='option']",
        ".dropdown-menu li",
        ".typeahead li",
    ]

    # Container selectors for scrolling
    container_selectors = [
        ".autocomplete-suggestions",
        "ul.ui-autocomplete",
        ".ui-autocomplete",
        ".ui-autocomplete-results",
        ".dropdown-menu",
        ".typeahead",
    ]

    items = None
    active_selector = None
    end_time = time.time() + timeout

    safe_search = search_term.encode('ascii', 'replace').decode('ascii')
    print(f"[DEBUG] Looking for autocomplete dropdown for '{safe_search}'...")

    while time.time() < end_time:
        # Check for alert first
        dismiss_alert_if_present(driver)

        for selector in autocomplete_selectors:
            try:
                found = driver.find_elements(By.CSS_SELECTOR, selector)
                if found:
                    visible = [el for el in found if el.is_displayed()]
                    if visible:
                        items = visible
                        active_selector = selector
                        print(f"[DEBUG] Found {len(items)} items with selector: {selector}")
                        break
            except Exception as e:
                pass
        if items:
            break
        time.sleep(0.3)

    if not items:
        print(f"[WARN] No autocomplete dropdown found for '{safe_search}'.")
        return False

    # Look for exact match (case-insensitive)
    search_upper = search_term.strip().upper()

    def find_match_in_items(item_list, print_debug=True):
        """Helper to find match in a list of items."""
        for idx, item in enumerate(item_list):
            try:
                item_text = item.text.strip().upper()
                # Only print first few items to avoid log spam
                if print_debug and idx < 5:
                    safe_text = item.text.strip().encode('ascii', 'replace').decode('ascii')
                    print(f"[DEBUG] Checking item: '{safe_text}'")
                if item_text == search_upper:
                    safe_text = item.text.strip().encode('ascii', 'replace').decode('ascii')
                    print(f"[OK] Found exact match: {safe_text}")
                    return item
                # Also check if item starts with search term (for partial matches)
                if item_text.startswith(search_upper):
                    safe_text = item.text.strip().encode('ascii', 'replace').decode('ascii')
                    print(f"[OK] Found partial match: {safe_text}")
                    return item
            except Exception as e:
                continue
        return None

    # First pass: check currently visible items
    match = find_match_in_items(items, print_debug=True)
    if match:
        click_js(driver, match)
        time.sleep(0.5)
        return True

    # If many items, try scrolling through the dropdown to find the match
    if len(items) >= 5:
        print(f"[DEBUG] Dropdown has {len(items)}+ items, scrolling to find match...")

        # Find the dropdown container for scrolling
        container = None
        for cont_sel in container_selectors:
            try:
                containers = driver.find_elements(By.CSS_SELECTOR, cont_sel)
                for c in containers:
                    if c.is_displayed():
                        container = c
                        break
                if container:
                    break
            except:
                pass

        # Track seen items to detect when we've scrolled through all
        seen_texts = set()
        for item in items:
            try:
                seen_texts.add(item.text.strip().upper())
            except:
                pass

        scroll_attempts = 0
        last_count = 0

        while scroll_attempts < max_scroll_attempts:
            scroll_attempts += 1

            # Scroll the container or use keyboard navigation
            try:
                if container:
                    # Scroll the container down
                    driver.execute_script("arguments[0].scrollTop += 200;", container)
                else:
                    # Use keyboard to navigate down through items
                    from selenium.webdriver.common.keys import Keys
                    active_element = driver.switch_to.active_element
                    for _ in range(5):  # Press down arrow 5 times
                        active_element.send_keys(Keys.ARROW_DOWN)
                        time.sleep(0.05)
            except Exception as e:
                pass

            time.sleep(0.2)  # Wait for scroll/render

            # Re-fetch items after scroll
            try:
                new_items = driver.find_elements(By.CSS_SELECTOR, active_selector)
                new_visible = [el for el in new_items if el.is_displayed()]

                if new_visible:
                    # Check for new items we haven't seen
                    new_count = 0
                    for item in new_visible:
                        try:
                            txt = item.text.strip().upper()
                            if txt and txt not in seen_texts:
                                seen_texts.add(txt)
                                new_count += 1
                        except:
                            pass

                    # Check for match in newly visible items
                    match = find_match_in_items(new_visible, print_debug=False)
                    if match:
                        click_js(driver, match)
                        time.sleep(0.5)
                        return True

                    # If no new items found for 3 consecutive scrolls, we've reached the end
                    if new_count == 0:
                        last_count += 1
                        if last_count >= 3:
                            print(f"[DEBUG] Reached end of dropdown after {scroll_attempts} scroll attempts")
                            break
                    else:
                        last_count = 0

            except Exception as e:
                pass

        print(f"[DEBUG] Searched through {len(seen_texts)} unique items in dropdown")

    # If no exact/partial match found after scrolling, re-fetch and click the first item
    try:
        # Re-fetch items in case scroll changed things
        items = driver.find_elements(By.CSS_SELECTOR, active_selector)
        items = [el for el in items if el.is_displayed()]
    except:
        pass

    if items:
        try:
            safe_text = items[0].text.strip().encode('ascii', 'replace').decode('ascii')
            print(f"[INFO] No exact match for '{safe_search}', clicking first item: {safe_text}")
        except:
            print(f"[INFO] No exact match for '{safe_search}', clicking first item")
        click_js(driver, items[0])
        time.sleep(0.5)
        return True

    return False


def set_datatable_show_max(driver):
    """
    Best-effort: increase 'Show N entries' to max to reduce paging.
    """
    sels = driver.find_elements(By.CSS_SELECTOR, 'select[name$="_length"]')
    if not sels:
        return
    sel = Select(sels[0])
    values = [o.get_attribute("value") for o in sel.options]
    # prefer "All" (-1) else max numeric
    if "-1" in values:
        sel.select_by_value("-1")
    else:
        nums = []
        for v in values:
            try:
                nums.append(int(v))
            except:
                pass
        if nums:
            sel.select_by_value(str(max(nums)))
    time.sleep(1.0)


def click_excel_export(driver, wait: WebDriverWait):
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.dt-buttons")))
    # Excel button: button.buttons-excel (title Excel)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.buttons-excel[title="Excel"]')))
    btn = driver.find_element(By.CSS_SELECTOR, 'button.buttons-excel[title="Excel"]')
    click_js(driver, btn)


# -----------------------------
# CSV helpers
# -----------------------------
def excel_to_csv(excel_path: Path, csv_path: Path):
    df = pd.read_excel(excel_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def write_csv_rows(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str], append: bool = True):
    """
    Write rows to CSV file.
    
    Args:
        path: Output file path
        rows: List of row dictionaries
        fieldnames: Column names
        append: If True, append to existing file; if False, overwrite
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    
    if append:
        new_file = not path.exists()
        mode = "a"
    else:
        new_file = True
        mode = "w"
    
    with path.open(mode, newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def get_existing_records(csv_path: Path, key_field: str = "MedicineName") -> Set[str]:
    """
    Get set of existing records from a CSV file to avoid duplicates.
    
    Args:
        csv_path: Path to CSV file
        key_field: Field to use as unique key
        
    Returns:
        Set of existing key values
    """
    existing = set()
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            if key_field in df.columns:
                existing = set(df[key_field].dropna().astype(str).str.strip())
        except Exception as e:
            print(f"[WARN] Could not read existing records from {csv_path}: {e}")
    return existing


# -----------------------------
# Extract search results table (as CSV)
# -----------------------------
def extract_search_table_rows(driver) -> List[Dict[str, Any]]:
    """
    Extract visible rows from the results table.
    Columns: S.No., Medicine Name, Status, Ceiling Price (₹), Unit, M.R.P (₹)/Unit
    """
    rows_out = []
    
    # Find the table containing medicine links
    tables = driver.find_elements(By.CSS_SELECTOR, "table")
    target_table = None
    for table in tables:
        try:
            if table.find_elements(By.CSS_SELECTOR, "a[data-toggle='modal']"):
                target_table = table
                break
            # Use XPath for onclick contains
            if table.find_elements(By.XPATH, ".//a[contains(@onclick, 'getOtherBrandPrice')]"):
                target_table = table
                break
        except:
            continue
    
    if not target_table:
        print("[WARN] Could not find results table")
        return rows_out

    rows = target_table.find_elements(By.CSS_SELECTOR, "tbody tr")
    for r in rows:
        tds = r.find_elements(By.CSS_SELECTOR, "td")
        if len(tds) < 6:
            continue
        med_link = tds[1].find_elements(By.CSS_SELECTOR, "a")
        med_name = med_link[0].text.strip() if med_link else tds[1].text.strip()

        rows_out.append({
            "SNo": tds[0].text.strip(),
            "MedicineName": med_name,
            "Status": tds[2].text.strip(),
            "CeilingPrice": tds[3].text.strip(),
            "Unit": tds[4].text.strip(),
            "MRP_Unit": tds[5].text.strip(),
        })
    return rows_out


# -----------------------------
# Modal extraction (Market Price - Available Brands)
# -----------------------------
def wait_modal_open(wait: WebDriverWait):
    # Example modal id in HTML: #exampleModal (Market Price)
    wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#exampleModal")))
    wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#popupDiv")))
    time.sleep(0.5)


def close_modal(driver, wait: WebDriverWait):
    # Close 'X'
    btns = driver.find_elements(By.CSS_SELECTOR, "#exampleModal button.close")
    if btns:
        click_js(driver, btns[0])
    else:
        driver.switch_to.active_element.send_keys(Keys.ESC)
    time.sleep(0.5)
    # wait till popup hidden
    try:
        wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "#popupDiv")))
    except:
        pass


def parse_modal_header(driver) -> Dict[str, str]:
    """
    Extract header info from the modal's #testTable.
    Structure:
    Row 1: Formulation/Brand, Company Name, M.R.P.
    Row 2: M.R.P./Unit, M.R.P. as on Date
    """
    out = {
        "Formulation_Brand": "",
        "CompanyName_Header": "",
        "MRP_Header": "",
        "MRP_Unit_Header": "",
        "MRP_AsOnDate": "",
    }
    
    try:
        # Try to find the header table (#testTable)
        header_table = driver.find_element(By.CSS_SELECTOR, "#popupDiv #testTable")
        rows = header_table.find_elements(By.CSS_SELECTOR, "tr")
        
        for row in rows:
            tds = row.find_elements(By.CSS_SELECTOR, "td")
            # Parse each pair of label:value cells
            i = 0
            while i < len(tds) - 1:
                label = tds[i].text.strip().lower()
                value = tds[i + 1].text.strip()
                
                if "formulation" in label or "brand" in label:
                    out["Formulation_Brand"] = value
                elif "company" in label:
                    out["CompanyName_Header"] = value
                elif "m.r.p./unit" in label or "mrp/unit" in label:
                    out["MRP_Unit_Header"] = value
                elif "as on date" in label:
                    out["MRP_AsOnDate"] = value
                elif "m.r.p" in label:
                    out["MRP_Header"] = value
                
                i += 2
    except Exception as e:
        # Fallback: use regex on popup text
        try:
            popup = driver.find_element(By.CSS_SELECTOR, "#popupDiv")
            popup_text = popup.text
            
            def grab(label: str):
                m = re.search(rf"{re.escape(label)}\s*:?\s*(.+?)(?:\n|$)", popup_text, flags=re.IGNORECASE)
                return m.group(1).strip() if m else ""
            
            out["Formulation_Brand"] = grab("Formulation / Brand")
            out["CompanyName_Header"] = grab("Company Name")
            out["MRP_Unit_Header"] = grab("M.R.P./Unit")
            out["MRP_AsOnDate"] = grab("M.R.P. as on Date")
            out["MRP_Header"] = grab("M.R.P.")
        except:
            pass
    
    return out


def extract_modal_substitutes(driver) -> List[Dict[str, str]]:
    """
    Extract substitutes from the modal's #nonSchTable.
    Columns: S.No., Brand Name, Pack Size, M.R.P (₹), M.R.P./Unit (₹), Company Name
    """
    subs = []
    
    try:
        # Target the specific substitutes table
        sub_table = driver.find_element(By.CSS_SELECTOR, "#popupDiv #nonSchTable")
        rows = sub_table.find_elements(By.CSS_SELECTOR, "tbody tr")
        
        for r in rows:
            tds = r.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) >= 6:
                subs.append({
                    "Sub_SNo": tds[0].text.strip(),
                    "Sub_BrandName": tds[1].text.strip(),
                    "Sub_PackSize": tds[2].text.strip(),
                    "Sub_MRP": tds[3].text.strip(),
                    "Sub_MRP_Unit": tds[4].text.strip(),
                    "Sub_CompanyName": tds[5].text.strip(),
                })
    except Exception as e:
        # Fallback: find any table with 6 columns in popup (excluding header table)
        try:
            popup = driver.find_element(By.CSS_SELECTOR, "#popupDiv")
            tables = popup.find_elements(By.CSS_SELECTOR, "table")
            
            for table in tables:
                table_id = table.get_attribute("id") or ""
                # Skip the header table
                if table_id == "testTable":
                    continue
                    
                rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                for r in rows:
                    tds = r.find_elements(By.CSS_SELECTOR, "td")
                    if len(tds) >= 6:
                        subs.append({
                            "Sub_SNo": tds[0].text.strip(),
                            "Sub_BrandName": tds[1].text.strip(),
                            "Sub_PackSize": tds[2].text.strip(),
                            "Sub_MRP": tds[3].text.strip(),
                            "Sub_MRP_Unit": tds[4].text.strip(),
                            "Sub_CompanyName": tds[5].text.strip(),
                        })
        except:
            pass
    
    return subs


def get_medicine_table(driver):
    """Find the table containing medicine links."""
    tables = driver.find_elements(By.CSS_SELECTOR, "table")
    for table in tables:
        try:
            # Check for modal links
            if table.find_elements(By.CSS_SELECTOR, "a[data-toggle='modal']"):
                return table
            # Check for onclick handlers (use XPath for contains)
            if table.find_elements(By.XPATH, ".//a[contains(@onclick, 'getOtherBrandPrice')]"):
                return table
        except:
            continue
    return None


def scrape_details_for_all_medicines(
    driver, 
    wait: WebDriverWait, 
    formulation: str, 
    max_medicines: int = 0,
    existing_medicines: Set[str] = None
) -> List[Dict[str, Any]]:
    """
    Click each medicine link on the current results view (handles paging best-effort),
    extract modal header + substitutes, and flatten into CSV rows:
    one row per substitute (or one row with blanks if no substitute rows).
    
    Args:
        driver: Selenium WebDriver
        wait: WebDriverWait instance
        formulation: Formulation name being processed
        max_medicines: Maximum number of medicines to process (0 = no limit)
        existing_medicines: Set of already processed medicine names to skip (for resume)
    
    Returns:
        List of detail row dictionaries
    """
    if existing_medicines is None:
        existing_medicines = set()
    
    all_rows = []
    total_processed = 0
    total_skipped = 0
    errors_count = 0
    page_num = 1
    processed_medicines = set()  # Track processed medicine names to avoid duplicates
    
    set_datatable_show_max(driver)
    time.sleep(1)  # Wait for table to reload after changing page size

    # handle pagination if exists
    while True:
        table = get_medicine_table(driver)
        if not table:
            print("[WARN] Could not find medicine table")
            break
            
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        num_rows = len(rows)
        print(f"[INFO] Page {page_num}: Found {num_rows} medicine rows")
        
        # Debug: print first row structure
        if num_rows > 0 and page_num == 1:
            first_row = rows[0]
            first_tds = first_row.find_elements(By.CSS_SELECTOR, "td")
            print(f"[DEBUG] First row has {len(first_tds)} cells")
            if len(first_tds) > 1:
                # Check for links in first few cells
                for cell_idx, td in enumerate(first_tds[:3]):
                    links = td.find_elements(By.CSS_SELECTOR, "a")
                    link_info = f"{len(links)} links" if links else "no links"
                    try:
                        cell_text = td.text[:30].encode('ascii', 'replace').decode('ascii')
                    except:
                        cell_text = "..."
                    print(f"[DEBUG] Cell {cell_idx}: '{cell_text}' ({link_info})")

        for idx in range(num_rows):
            try:
                # Check if we've hit the max limit
                if max_medicines > 0 and total_processed >= max_medicines:
                    print(f"[INFO] Reached max medicines limit ({max_medicines}), stopping")
                    return all_rows
                
                # re-fetch table and rows each time (DOM may change after modal close)
                table = get_medicine_table(driver)
                if not table:
                    break
                rows2 = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                if idx >= len(rows2):
                    break

                row = rows2[idx]
                tds = row.find_elements(By.CSS_SELECTOR, "td")
                if len(tds) < 6:
                    if idx == 0:
                        print(f"[DEBUG] Row {idx+1} has only {len(tds)} cells, skipping")
                    continue

                sno = tds[0].text.strip()
                
                # Find the medicine link - try multiple selectors
                med_links = tds[1].find_elements(By.CSS_SELECTOR, "a")
                if not med_links:
                    # Try finding link in the entire row
                    med_links = row.find_elements(By.CSS_SELECTOR, "a[data-toggle='modal']")
                if not med_links:
                    med_links = row.find_elements(By.XPATH, ".//a[contains(@onclick, 'getOtherBrandPrice')]")
                if not med_links:
                    if idx == 0:
                        # Debug: print what's in td[1]
                        td1_html = tds[1].get_attribute('innerHTML')[:200] if len(tds) > 1 else "N/A"
                        print(f"[DEBUG] No medicine link in row {idx+1}. TD[1] content: {td1_html}")
                    continue
                med_link = med_links[0]
                med_name = med_link.text.strip()
                status = tds[2].text.strip()
                ceiling_price = tds[3].text.strip()
                unit = tds[4].text.strip()
                mrp_unit = tds[5].text.strip()

                # Check for duplicate (already processed this medicine in this session)
                med_key = f"{sno}_{med_name}"
                if med_key in processed_medicines:
                    print(f"[WARN] Skipping duplicate: {med_name} (row {idx+1})")
                    continue
                
                # Check if already in existing records (resume support)
                if med_name in existing_medicines:
                    total_skipped += 1
                    if total_skipped <= 5:  # Only print first few skips
                        try:
                            safe_name = med_name.encode('ascii', 'replace').decode('ascii')
                            print(f"[SKIP] Already processed: {safe_name}")
                        except:
                            print(f"[SKIP] Already processed: Medicine {idx+1}")
                    elif total_skipped == 6:
                        print(f"[SKIP] ... and more (suppressing further skip messages)")
                    continue
                
                processed_medicines.add(med_key)
                
                # Safe print for Windows console
                try:
                    safe_med_name = med_name.encode('ascii', 'replace').decode('ascii')
                except:
                    safe_med_name = f"Medicine {idx+1}"
                print(f"[INFO] Processing medicine {idx+1}/{num_rows}: {safe_med_name}")

                # Scroll the medicine link into view before clicking
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'instant'});", med_link)
                time.sleep(0.3)  # Wait for scroll to complete
                
                # open modal by clicking the medicine link
                click_js(driver, med_link)
                
                try:
                    wait_modal_open(wait)

                    # Extract header info from modal
                    header = parse_modal_header(driver)
                    subs = extract_modal_substitutes(driver)

                    base = {
                        "FormulationInput": formulation,
                        "SNo": sno,
                        "MedicineName": med_name,
                        "Status": status,
                        "CeilingPrice": ceiling_price,
                        "Unit": unit,
                        "MRP_Unit": mrp_unit,
                        "CapturedAt": datetime.now().isoformat(timespec="seconds"),
                        **header,
                    }

                    if subs:
                        for s in subs:
                            all_rows.append({**base, **s})
                    else:
                        all_rows.append(base)

                    close_modal(driver, wait)
                    total_processed += 1
                except Exception as modal_e:
                    errors_count += 1
                    error_msg = str(modal_e).encode('ascii', 'replace').decode('ascii')
                    print(f"[ERROR] Failed to extract modal data: {error_msg[:150]}")
                    # Try to close modal if open
                    try:
                        close_modal(driver, wait)
                    except:
                        pass
                    # Continue with next medicine instead of stopping
                    continue
                    
            except Exception as row_e:
                errors_count += 1
                error_msg = str(row_e).encode('ascii', 'replace').decode('ascii')
                print(f"[ERROR] Failed to process row {idx+1}: {error_msg[:150]}")
                continue
        
        # Print progress summary for this page
        print(f"[INFO] Page {page_num} complete: {total_processed} processed, {total_skipped} skipped, {errors_count} errors")

        # go next page if pagination exists
        # Use valid CSS selectors only (no jQuery :contains)
        next_btns = driver.find_elements(By.CSS_SELECTOR, "a.paginate_button.next:not(.disabled)")
        if not next_btns:
            # Try alternative pagination selectors with XPath
            next_btns = driver.find_elements(By.XPATH, "//a[contains(text(),'Next') and not(contains(@class,'disabled'))]")
        
        if not next_btns:
            break
        next_btn = next_btns[0]
        cls = next_btn.get_attribute("class") or ""
        if "disabled" in cls:
            break
        click_js(driver, next_btn)
        page_num += 1
        time.sleep(1.0)

    print(f"[OK] Finished processing: {total_processed} medicines, {len(all_rows)} detail rows, {total_skipped} skipped, {errors_count} errors")
    return all_rows


# -----------------------------
# Main run per formulation
# -----------------------------
def run_for_formulation(
    driver, 
    wait: WebDriverWait, 
    formulation: str, 
    download_dir: Path, 
    out_dir: Path,
    checkpoint: FormulationCheckpoint
) -> Dict[str, Any]:
    """
    Process a single formulation and return statistics.
    
    Returns:
        Dict with stats: {"medicines": int, "substitutes": int, "status": str}
    """
    formulation_slug = slugify(formulation)
    stats = {"medicines": 0, "substitutes": 0, "status": "zero_records"}

    # Get delay settings from config
    detail_delay = getenv_float("DETAIL_DELAY", 1.5)

    # Mark as in progress
    checkpoint.mark_in_progress(formulation)

    driver.get(URL)

    # Human-like pause after page load
    pause(0.5, 1.0)

    # Dismiss any initial alerts
    dismiss_alert_if_present(driver)

    # Step 1-2: Click search field and enter formulation name
    inp = wait.until(EC.element_to_be_clickable((By.ID, "searchFormulation")))
    click_js(driver, inp)  # Click to focus
    pause(0.3, 0.6)
    inp.clear()
    pause(0.2, 0.4)

    # Type the formulation with human-like delays
    for char in formulation:
        inp.send_keys(char)
        delay = type_delay()
        if delay > 0:
            time.sleep(delay)

    pause(1.0, 1.5)  # Wait for autocomplete dropdown to appear

    # Trigger input event via JavaScript to ensure autocomplete fires
    driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", inp)
    driver.execute_script("arguments[0].dispatchEvent(new Event('keyup', { bubbles: true }));", inp)
    pause(0.8, 1.2)

    # Step 3: Select exact match from autocomplete dropdown
    autocomplete_success = pick_autocomplete_exact_match(driver, wait, formulation, timeout=15)
    
    if not autocomplete_success:
        # Autocomplete failed - formulation may not exist in database
        safe_name = formulation.encode('ascii', 'replace').decode('ascii')
        print(f"[WARN] Autocomplete failed for '{safe_name}' - skipping this formulation")
        # Dismiss any alert that may have appeared
        dismiss_alert_if_present(driver)
        stats["reason"] = "autocomplete_failed"
        return stats

    # Step 4: Click GO button
    go = wait.until(EC.element_to_be_clickable((By.ID, "gobtn")))
    click_js(driver, go)
    
    # Check for alert after clicking GO
    time.sleep(0.5)
    if dismiss_alert_if_present(driver):
        safe_name = formulation.encode('ascii', 'replace').decode('ascii')
        print(f"[WARN] Alert appeared after GO for '{safe_name}' - skipping")
        stats["reason"] = "alert_after_go"
        return stats

    # Wait results injected
    wait_results_loaded(driver, wait)

    # Check if results table loaded - look for medicine links
    medicine_links = driver.find_elements(By.CSS_SELECTOR, "a[data-toggle='modal'][data-target='#exampleModal']")
    if not medicine_links:
        # Fallback: check for any table with links using XPath
        medicine_links = driver.find_elements(By.XPATH, "//table//a[contains(@onclick, 'getOtherBrandPrice')]")
    
    if not medicine_links:
        print(f"[WARN] No medicine links found for {formulation}.")
        stats["reason"] = "no_medicine_links"
        return stats
    
    print(f"[OK] Found {len(medicine_links)} medicine links")

    # Save search table rows to CSV
    search_rows = extract_search_table_rows(driver)
    search_csv = out_dir / "search_results" / f"{formulation_slug}.csv"
    write_csv_rows(
        search_csv,
        search_rows,
        fieldnames=["SNo", "MedicineName", "Status", "CeilingPrice", "Unit", "MRP_Unit"],
        append=False  # Overwrite for search results
    )
    print(f"[OK] Search rows CSV: {search_csv} ({len(search_rows)} rows)")

    # Step 4: Click Excel button to download
    click_excel_export(driver, wait)
    downloaded = wait_for_download_complete(download_dir, timeout_sec=180)

    excel_target = out_dir / "excel_raw" / f"{formulation_slug}{downloaded.suffix}"
    excel_target.parent.mkdir(parents=True, exist_ok=True)
    if excel_target.exists():
        excel_target.unlink()
    downloaded.rename(excel_target)
    print(f"[OK] Excel saved: {excel_target}")

    exported_csv = out_dir / "excel_as_csv" / f"{formulation_slug}.csv"
    excel_to_csv(excel_target, exported_csv)
    print(f"[OK] Excel->CSV: {exported_csv}")

    # Step 5: Click each medicine link to get individual details
    # Check for existing records to support resume
    details_csv = out_dir / "details" / f"{formulation_slug}.csv"
    existing_medicines = get_existing_records(details_csv, "MedicineName")
    
    if existing_medicines:
        print(f"[RESUME] Found {len(existing_medicines)} already processed medicines for {formulation}")
    
    max_meds = getenv_int("MAX_MEDICINES_PER_FORMULATION", 5000)
    detail_rows = scrape_details_for_all_medicines(
        driver, wait, formulation, 
        max_medicines=max_meds,
        existing_medicines=existing_medicines
    )

    # Columns for details CSV (flattened; one row per substitute)
    detail_fieldnames = [
        "FormulationInput",
        "SNo",
        "MedicineName",
        "Status",
        "CeilingPrice",
        "Unit",
        "MRP_Unit",
        "CapturedAt",
        "Formulation_Brand",
        "CompanyName_Header",
        "MRP_Unit_Header",
        "MRP_AsOnDate",
        "MRP_Header",
        "Sub_SNo",
        "Sub_BrandName",
        "Sub_PackSize",
        "Sub_MRP",
        "Sub_MRP_Unit",
        "Sub_CompanyName",
    ]
    
    # Only write new rows (append mode for resume support)
    if detail_rows:
        write_csv_rows(details_csv, detail_rows, fieldnames=detail_fieldnames, append=True)
        print(f"[OK] Details CSV: {details_csv} ({len(detail_rows)} new rows)")
    else:
        print(f"[INFO] No new detail rows to write for {formulation}")
    
    # Calculate stats
    unique_medicines = set(r.get("MedicineName", "") for r in detail_rows)
    stats["medicines"] = len(unique_medicines)
    stats["substitutes"] = len(detail_rows)
    stats["status"] = "success"
    
    return stats


def generate_final_report(out_dir: Path, checkpoint: FormulationCheckpoint, total_formulations: int, start_time: datetime):
    """Generate a final summary report."""
    report_file = out_dir / "scraping_report.json"
    
    stats = checkpoint.get_stats()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Count files in output directories
    details_dir = out_dir / "details"
    details_files = list(details_dir.glob("*.csv")) if details_dir.exists() else []
    
    # Count total rows in details
    total_detail_rows = 0
    for csv_file in details_files:
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            total_detail_rows += len(df)
        except:
            pass
    
    report = {
        "scraper": "India NPPA Pharma Sahi Daam",
        "generated_at": end_time.isoformat(),
        "duration_seconds": round(duration, 2),
        "duration_formatted": f"{int(duration // 3600)}h {int((duration % 3600) // 60)}m {int(duration % 60)}s",
        "summary": {
            "total_formulations_input": total_formulations,
            "formulations_terminal": stats.get("total_processed", 0),
            "formulations_success": stats.get("total_success", 0),
            "formulations_zero_records": stats.get("total_zero_records", 0),
            "formulations_failed": stats.get("failed_formulations", 0),
            "formulations_remaining": total_formulations - stats.get("total_processed", 0),
            "total_medicines_processed": stats["total_medicines"],
            "total_substitute_rows": stats["total_substitutes"],
            "errors": stats["errors"]
        },
        "output_files": {
            "details_csvs": len(details_files),
            "total_detail_rows": total_detail_rows
        },
        "output_directory": str(out_dir)
    }
    
    # Write report
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print("\n" + "=" * 70)
    print("FINAL SCRAPING REPORT")
    print("=" * 70)
    print(f"Duration: {report['duration_formatted']}")
    print(f"Formulations terminal: {stats.get('total_processed', 0)}/{total_formulations}")
    print(f"  Success: {stats.get('total_success', 0)}")
    print(f"  Zero-record: {stats.get('total_zero_records', 0)}")
    print(f"  Failed: {stats.get('failed_formulations', 0)}")
    print(f"Total medicines: {stats['total_medicines']}")
    print(f"Total detail rows: {total_detail_rows}")
    print(f"Errors: {stats['errors']}")
    print(f"Report saved: {report_file}")
    print("=" * 70)
    
    return report


def main():
    print("=" * 70)
    print("India NPPA Scraper - Get Medicine Details")
    print("=" * 70)

    start_time = datetime.now()
    start_time_ts = time.time()

    # Get paths from platform config
    download_dir = get_download_dir()
    out_dir = get_output_dir()
    input_dir = get_input_dir()
    repo_root = get_repo_root()

    print(f"[CONFIG] Input dir: {input_dir}")
    print(f"[CONFIG] Output dir: {out_dir}")
    print(f"[CONFIG] Download dir: {download_dir}")
    worker_id = (getenv("WORKER_ID", "") or "").strip()
    if worker_id:
        print(f"[CONFIG] Worker ID: {worker_id}")

    # Show feature status
    print(f"[CONFIG] PID Tracker: {'enabled' if _PID_TRACKER_AVAILABLE else 'disabled'}")
    print(f"[CONFIG] Human Actions: {'enabled' if getenv_bool('HUMAN_ACTIONS_ENABLED', False) else 'disabled'}")
    print(f"[CONFIG] Stealth Profile: {'enabled' if getenv_bool('STEALTH_PROFILE_ENABLED', False) else 'disabled'}")
    print(f"[CONFIG] Rich Progress: {'enabled' if _RICH_PROGRESS_AVAILABLE else 'disabled'}")

    # Register cleanup on exit
    def _cleanup_on_exit():
        cleanup_chrome(silent=True)
        if _PID_TRACKER_AVAILABLE:
            cleanup_pid_file(SCRAPER_ID, repo_root)

    atexit.register(_cleanup_on_exit)

    # Initialize checkpoint manager
    checkpoint = FormulationCheckpoint(out_dir)

    # Check for --fresh flag to clear checkpoint
    if "--fresh" in sys.argv:
        checkpoint.clear()
        print("[CONFIG] Starting fresh (checkpoint cleared)")

    # Load formulations from input folder
    formulations = load_formulations_from_input()
    if not formulations:
        formulations = DEFAULT_FORMULATIONS
        print(f"[CONFIG] Using default formulations: {formulations}")
    else:
        print(f"[CONFIG] Loaded {len(formulations)} formulations")

    # Apply max limit if configured
    max_formulations = getenv_int("MAX_FORMULATIONS", 0)
    if max_formulations > 0 and len(formulations) > max_formulations:
        formulations = formulations[:max_formulations]
        print(f"[CONFIG] Limited to {max_formulations} formulations")

    total_formulations = len(formulations)

    # Filter out already completed formulations
    pending_formulations = [f for f in formulations if not checkpoint.is_completed(f)]
    skipped_count = len(formulations) - len(pending_formulations)

    if skipped_count > 0:
        print(f"[RESUME] Skipping {skipped_count} already completed formulations")
        print(f"[RESUME] {len(pending_formulations)} formulations remaining")

    if not pending_formulations:
        print("[INFO] All formulations already completed!")
        generate_final_report(out_dir, checkpoint, total_formulations, start_time)
        return

    wait_seconds = getenv_int("WAIT_SECONDS", 60)
    search_delay = getenv_float("SEARCH_DELAY", 2.0)

    driver = None
    total_medicines = 0
    total_substitutes = 0
    errors_count = 0

    try:
        driver = build_driver(download_dir)
        wait = WebDriverWait(driver, wait_seconds)

        # Use rich progress if available (with fallback for Windows encoding issues)
        progress = None
        task = None
        if _RICH_PROGRESS_AVAILABLE:
            try:
                progress = create_progress()
                progress.start()
                task = progress.add_task(
                    "Processing formulations",
                    total=len(pending_formulations)
                )
            except (UnicodeEncodeError, Exception) as e:
                print(f"[WARN] Rich progress unavailable: {e}")
                progress = None
                task = None

        for idx, f in enumerate(pending_formulations):
            f = (f or "").strip()
            if not f:
                continue

            # Calculate overall progress including skipped
            completed_so_far = skipped_count + idx
            progress_pct = round(((completed_so_far + 1) / total_formulations) * 100, 1)

            # Print progress (parseable format for orchestration)
            if should_emit_progress():
                print(
                    f"\n[PROGRESS] Formulation: {completed_so_far + 1}/{total_formulations} ({progress_pct}%)",
                    flush=True,
                )

            # Safe print formulation name
            safe_name = f.encode('ascii', 'replace').decode('ascii')
            print(f"[INFO] Processing: {safe_name}")

            try:
                stats = run_for_formulation(driver, wait, f, download_dir, out_dir, checkpoint)

                if stats.get("status") == "success":
                    checkpoint.mark_success(f, stats.get("medicines", 0), stats.get("substitutes", 0))
                    total_medicines += stats["medicines"]
                    total_substitutes += stats["substitutes"]
                    print_status(f"Completed: {safe_name} ({stats['medicines']} medicines)", "success")
                else:
                    checkpoint.mark_zero_record(f, reason=stats.get("reason"))
                    print_status(f"No records for: {safe_name}", "warning")

            except Exception as e:
                error_msg = str(e).encode('ascii', 'replace').decode('ascii')
                print_status(f"Failed: {safe_name} - {error_msg[:100]}", "error")
                checkpoint.mark_failed(f, error=error_msg[:500])
                errors_count += 1
                continue
            finally:
                # Update progress bar
                if progress and task is not None:
                    try:
                        progress.update(task, advance=1)
                    except UnicodeEncodeError:
                        pass

                # Human-like delay between formulations
                if idx < len(pending_formulations) - 1:
                    pause(search_delay * 0.8, search_delay * 1.2)

        if progress:
            try:
                progress.stop()
            except UnicodeEncodeError:
                pass

        print("\n" + "=" * 70)
        print("Medicine details extraction complete!")
        print("=" * 70)

    except KeyboardInterrupt:
        print("\n[WARN] Interrupted by user - saving progress...")
    except Exception as e:
        print(f"[FATAL] Script failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup Chrome
        cleanup_chrome(driver, silent=False)

    # Generate final report
    report = generate_final_report(out_dir, checkpoint, total_formulations, start_time)

    # Print summary
    duration = time.time() - start_time_ts
    if _RICH_PROGRESS_AVAILABLE:
        try:
            print_summary(
                SCRAPER_ID,
                records_processed=total_medicines,
                duration_seconds=duration,
                errors=errors_count,
                warnings=0
            )
        except UnicodeEncodeError:
            # Fallback for Windows console encoding issues
            pass

    # Always print text summary as backup
    print(f"\n[SUMMARY] Duration: {format_duration(duration)}")
    print(f"[SUMMARY] Medicines processed: {total_medicines}")
    print(f"[SUMMARY] Substitutes extracted: {total_substitutes}")
    print(f"[SUMMARY] Errors: {errors_count}")


if __name__ == "__main__":
    main()
