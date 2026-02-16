#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chrome Process ID Tracker

Tracks Chrome browser and ChromeDriver process IDs for each pipeline run,
allowing targeted termination of only the Chrome instances belonging to a specific pipeline.
"""

import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Set, Optional

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)

_PID_LOCK_TIMEOUT_S = 5.0
_PID_LOCK_STALE_S = 30.0


def _acquire_pid_lock(lock_path: Path, timeout_s: float = _PID_LOCK_TIMEOUT_S) -> Optional[int]:
    """Acquire a simple inter-process lock using an exclusive lock file."""
    start = time.time()
    while True:
        try:
            return os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError:
            try:
                if lock_path.exists() and (time.time() - lock_path.stat().st_mtime) > _PID_LOCK_STALE_S:
                    lock_path.unlink()
                    continue
            except Exception:
                pass
            if (time.time() - start) >= timeout_s:
                return None
            time.sleep(0.05)


def _release_pid_lock(lock_path: Path, lock_fd: Optional[int]) -> None:
    if lock_fd is None:
        return
    try:
        os.close(lock_fd)
    except Exception:
        pass
    try:
        lock_path.unlink()
    except Exception:
        pass


def _atomic_write_json(path: Path, payload: dict) -> None:
    """Write JSON atomically to avoid partial/garbled files under concurrency."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=path.name, suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except Exception:
            pass


def get_chrome_pids_from_playwright_browser(browser) -> Set[int]:
    """
    Extract Chrome/Chromium process IDs from a Playwright browser instance.
    
    Args:
        browser: Playwright Browser instance
        
    Returns:
        Set of process IDs (browser process and child processes)
    """
    pids = set()
    
    if not PSUTIL_AVAILABLE:
        return pids
    
    try:
        # Playwright browser doesn't expose PID directly, so we need to find it
        # by looking for Chrome/Chromium processes with Playwright-specific flags
        for proc in psutil.process_iter(['pid', 'ppid', 'name', 'cmdline']):
            try:
                proc_name = (proc.info.get('name') or '').lower()
                cmdline = ' '.join(proc.info.get('cmdline') or [])
                
                # Playwright uses chromium with specific flags
                if 'chrome' in proc_name or 'chromium' in proc_name:
                    # Look for Playwright-specific flags
                    playwright_flags = [
                        '--remote-debugging-pipe',
                        '--disable-blink-features=AutomationControlled',
                        '--test-type',
                        '--user-data-dir'
                    ]
                    
                    # Check if it has Playwright flags (usually has multiple)
                    flag_count = sum(1 for flag in playwright_flags if flag in cmdline)
                    if flag_count >= 2:  # At least 2 Playwright flags
                        pids.add(proc.info['pid'])
                        logger.debug(f"Found Playwright Chrome PID: {proc.info['pid']}")
                        
                        # Also get all child processes
                        try:
                            parent = psutil.Process(proc.info['pid'])
                            for child in parent.children(recursive=True):
                                pids.add(child.pid)
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    except Exception as e:
        logger.warning(f"Error finding Playwright Chrome PIDs: {e}")
    
    return pids


def get_chrome_pids_from_driver(driver) -> Set[int]:
    """
    Extract Chrome and ChromeDriver process IDs from a WebDriver instance.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        Set of process IDs (ChromeDriver PID and Chrome browser PIDs)
    """
    pids = set()
    
    try:
        # Get ChromeDriver process ID
        if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
            chromedriver_pid = driver.service.process.pid
            if chromedriver_pid:
                pids.add(chromedriver_pid)
                logger.debug(f"Found ChromeDriver PID: {chromedriver_pid}")
    except Exception as e:
        logger.warning(f"Could not get ChromeDriver PID: {e}")
    
    # Find Chrome browser processes spawned by this ChromeDriver
    if PSUTIL_AVAILABLE:
        try:
            # Get ChromeDriver PID first
            chromedriver_pid = None
            try:
                if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                    chromedriver_pid = driver.service.process.pid
            except Exception:
                pass
            
            # Get all descendant processes of ChromeDriver (recursive)
            def get_descendant_pids(pid):
                """Get all descendant PIDs of a process"""
                descendants = set()
                try:
                    parent = psutil.Process(pid)
                    for child in parent.children(recursive=True):
                        descendants.add(child.pid)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                return descendants
            
            if chromedriver_pid:
                # Get all descendants of ChromeDriver
                descendant_pids = get_descendant_pids(chromedriver_pid)
                pids.update(descendant_pids)
                logger.debug(f"Found {len(descendant_pids)} descendant process(es) of ChromeDriver {chromedriver_pid}")
            
            # Also find Chrome processes by automation flags (more aggressive)
            for proc in psutil.process_iter(['pid', 'ppid', 'name', 'cmdline']):
                try:
                    proc_name = (proc.info.get('name') or '').lower()
                    cmdline = ' '.join(proc.info.get('cmdline') or [])
                    
                    # Check if it's a Chrome browser process (not ChromeDriver)
                    if 'chrome' in proc_name and 'chromedriver' not in proc_name:
                        # Check if it's a child of our ChromeDriver
                        if chromedriver_pid and proc.info.get('ppid') == chromedriver_pid:
                            pids.add(proc.info['pid'])
                            logger.debug(f"Found Chrome browser PID (child of ChromeDriver): {proc.info['pid']}")
                        # Also check for automation flags that indicate Selenium-controlled Chrome
                        elif any(flag in cmdline for flag in ['--remote-debugging-port', '--test-type', '--user-data-dir', '--incognito']):
                            # Check for Selenium-specific flags
                            selenium_flags = ['--remote-debugging-port', '--disable-blink-features=AutomationControlled']
                            if any(flag in cmdline for flag in selenium_flags):
                                pids.add(proc.info['pid'])
                                logger.debug(f"Found Chrome browser PID (automation flags): {proc.info['pid']}")
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
        except Exception as e:
            logger.warning(f"Error finding Chrome browser PIDs: {e}")
    
    return pids


def get_pid_file_path(scraper_name: str, repo_root: Path) -> Path:
    """Get the path to the Chrome PID tracking file for a scraper"""
    return repo_root / f".{scraper_name}_chrome_pids.json"


def save_chrome_pids(scraper_name: str, repo_root: Path, pids: Set[int]):
    """
    Save Chrome process IDs to a file for later retrieval.
    
    Args:
        scraper_name: Name of the scraper
        repo_root: Repository root path
        pids: Set of process IDs to save
    """
    pid_file = get_pid_file_path(scraper_name, repo_root)
    
    lock_path = pid_file.with_suffix(pid_file.suffix + ".lock")
    lock_fd = _acquire_pid_lock(lock_path)
    try:
        # Read existing PIDs if file exists
        existing_pids = set()
        if pid_file.exists():
            try:
                with open(pid_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    existing_pids = set(data.get('pids', []))
            except Exception as e:
                logger.warning(f"Could not read existing PID file: {e}")

        # Merge with new PIDs
        all_pids = existing_pids | pids

        # Save to file (atomic to avoid corruption)
        _atomic_write_json(pid_file, {
            'scraper_name': scraper_name,
            'pids': list(all_pids),
            'updated_at': str(Path(__file__).stat().st_mtime) if Path(__file__).exists() else None
        })

        logger.debug(f"Saved {len(all_pids)} Chrome PIDs to {pid_file}")
    except Exception as e:
        logger.warning(f"Could not save Chrome PIDs: {e}")
    finally:
        _release_pid_lock(lock_path, lock_fd)


def _get_run_id_from_file(scraper_name: str, repo_root: Path) -> Optional[str]:
    """Get run_id from output/{scraper}/.current_run_id for DB-based termination."""
    output_dir = repo_root / "output" / scraper_name
    run_id_file = output_dir / ".current_run_id"
    if not run_id_file.exists():
        return None
    try:
        return run_id_file.read_text(encoding="utf-8").strip() or None
    except Exception:
        return None


def get_active_pids_from_db(
    scraper_name: str,
    run_id: Optional[str],
    repo_root: Path,
    browser_types: Optional[List[str]] = None
) -> Set[int]:
    """
    Get active browser PIDs from chrome_instances table for termination.
    Uses all_pids column when present, else pid.
    """
    if not run_id:
        return set()
    pids = set()
    try:
        from core.db.postgres_connection import PostgresDB
        db = PostgresDB(scraper_name)
        db.connect()
        try:
            with db.cursor() as cur:
                types_filter = ""
                params = [run_id, scraper_name]
                if browser_types:
                    placeholders = ", ".join(["%s"] * len(browser_types))
                    types_filter = f" AND browser_type IN ({placeholders})"
                    params.extend(browser_types)
                cur.execute(f"""
                    SELECT pid, all_pids FROM chrome_instances
                    WHERE run_id = %s AND scraper_name = %s AND terminated_at IS NULL
                    {types_filter}
                """, params)
                for row in cur.fetchall():
                    driver_pid, all_pids_val = row[0], row[1]
                    if all_pids_val and len(all_pids_val) > 0:
                        try:
                            pids.update(int(p) for p in all_pids_val)
                        except (TypeError, ValueError):
                            pids.add(driver_pid)
                    else:
                        pids.add(driver_pid)
        finally:
            if hasattr(db, "close"):
                db.close()
    except Exception as e:
        logger.debug(f"Could not get PIDs from DB for {scraper_name}: {e}")
    return pids


def load_chrome_pids(scraper_name: str, repo_root: Path) -> Set[int]:
    """
    Load Chrome process IDs from file.
    
    Args:
        scraper_name: Name of the scraper
        repo_root: Repository root path
        
    Returns:
        Set of process IDs
    """
    pid_file = get_pid_file_path(scraper_name, repo_root)
    
    if not pid_file.exists():
        return set()
    
    try:
        with open(pid_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            # Try to fix common JSON corruption issues
            # Remove trailing extra closing braces
            while content.endswith('}}') and content.count('{') < content.count('}'):
                content = content[:-1]
            
            data = json.loads(content)
            # Verify the file belongs to the correct scraper
            file_scraper_name = data.get('scraper_name', '')
            if file_scraper_name and file_scraper_name != scraper_name:
                logger.warning(f"PID file {pid_file} belongs to {file_scraper_name}, not {scraper_name}. Returning empty set.")
                return set()
            pids = set(data.get('pids', []))
            logger.debug(f"Loaded {len(pids)} Chrome PIDs for {scraper_name} from {pid_file}")
            return pids
    except json.JSONDecodeError as e:
        logger.warning(f"Could not parse Chrome PIDs JSON from {pid_file}: {e}. File may be corrupted. Attempting to fix...")
        # Try to fix corrupted file by rewriting it with empty data
        lock_path = pid_file.with_suffix(pid_file.suffix + ".lock")
        lock_fd = _acquire_pid_lock(lock_path)
        try:
            _atomic_write_json(pid_file, {
                'scraper_name': scraper_name,
                'pids': [],
                'updated_at': None
            })
            logger.info(f"Fixed corrupted PID file {pid_file}")
        except Exception as fix_error:
            logger.warning(f"Could not fix corrupted PID file: {fix_error}")
        finally:
            _release_pid_lock(lock_path, lock_fd)
        return set()
    except Exception as e:
        logger.warning(f"Could not load Chrome PIDs from {pid_file}: {e}")
        return set()


def terminate_chrome_pids(scraper_name: str, repo_root: Path, silent: bool = False) -> int:
    """
    Terminate Chrome processes tracked for a specific scraper.
    Uses DB (chrome_instances) as source; run_id from output/{scraper}/.current_run_id.
    
    Args:
        scraper_name: Name of the scraper
        repo_root: Repository root path
        silent: If True, suppress log messages
        
    Returns:
        Number of processes terminated
    """
    run_id = _get_run_id_from_file(scraper_name, repo_root)
    pids = get_active_pids_from_db(
        scraper_name, run_id, repo_root,
        browser_types=["chrome", "chromium"]
    )
    
    if not pids:
        if not silent:
            logger.debug(f"No Chrome PIDs found for {scraper_name}")
        return 0
    
    terminated_count = 0
    
    if not silent:
        logger.info(f"Terminating {len(pids)} Chrome process(es) for {scraper_name}: {sorted(pids)}")
    
    valid_pids = []
    for pid in pids:
        # Check if process still exists
        if PSUTIL_AVAILABLE:
            try:
                proc = psutil.Process(pid)
                if proc.is_running():
                    valid_pids.append(pid)
                else:
                    if not silent:
                        logger.debug(f"PID {pid} is not running, skipping")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                if not silent:
                    logger.debug(f"PID {pid} does not exist or cannot access, skipping")
        else:
            # If psutil not available, assume PID is valid
            valid_pids.append(pid)
    
    if not valid_pids:
        if not silent:
            logger.debug(f"No valid Chrome PIDs to terminate for {scraper_name}")
        return 0
    
    # Terminate each valid PID individually (without /T flag first to avoid killing unrelated processes)
    # Only use /T if we're sure the PID is a parent process with Chrome children
    for pid in valid_pids:
        try:
            if sys.platform == "win32":
                # First, try to kill just the process (without /T) to be more selective
                # This prevents killing child processes that might belong to other scrapers
                result = subprocess.run(
                    ['taskkill', '/F', '/PID', str(pid)],
                    capture_output=True,
                    timeout=5,
                    text=True
                )
                
                # If that didn't work, check if process still exists and has Chrome children
                # Only then use /T flag
                if result.returncode != 0 and PSUTIL_AVAILABLE:
                    try:
                        proc = psutil.Process(pid)
                        if proc.is_running():
                            # Check if it has children - if so, we might need /T
                            children = proc.children(recursive=False)
                            if children:
                                # Only kill children that are Chrome-related
                                chrome_children = [c for c in children if 'chrome' in c.name().lower() or 'chromedriver' in c.name().lower()]
                                if chrome_children:
                                    # Use /T only for this specific PID tree
                                    result = subprocess.run(
                                        ['taskkill', '/F', '/T', '/PID', str(pid)],
                                        capture_output=True,
                                        timeout=10,
                                        text=True
                                    )
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass  # Process already dead
                
                if result.returncode == 0 or "not found" not in result.stdout.lower():
                    terminated_count += 1
                    if not silent:
                        logger.debug(f"Terminated Chrome process: PID {pid}")
            else:
                # Unix-like: use kill with -9 to force kill (process group)
                result = subprocess.run(
                    ['kill', '-9', '-{}'.format(pid)],  # Negative PID kills process group
                    capture_output=True,
                    timeout=10
                )
                if result.returncode == 0:
                    terminated_count += 1
                    if not silent:
                        logger.debug(f"Terminated Chrome process group: PID {pid}")
        except subprocess.TimeoutExpired:
            if not silent:
                logger.warning(f"Timeout terminating PID {pid}")
        except Exception as e:
            if not silent:
                logger.warning(f"Error terminating PID {pid}: {e}")
    
    # Mark instances as terminated in DB
    if run_id and terminated_count > 0:
        try:
            from core.browser.chrome_instance_tracker import ChromeInstanceTracker
            from core.db.postgres_connection import PostgresDB
            db = PostgresDB(scraper_name)
            db.connect()
            try:
                tracker = ChromeInstanceTracker(scraper_name, run_id, db)
                tracker.terminate_all(reason="pipeline_cleanup")
            finally:
                if hasattr(db, "close"):
                    db.close()
        except Exception as e:
            if not silent:
                logger.debug(f"Could not mark Chrome instances terminated in DB: {e}")
    
    if not silent:
        logger.info(f"Terminated {terminated_count}/{len(valid_pids)} Chrome process(es) for {scraper_name}")
    
    return terminated_count


def terminate_chrome_by_flags(silent: bool = False) -> int:
    """
    Fallback method: Find and terminate Chrome processes with automation flags.
    This is used when PID tracking fails or is incomplete.
    
    Args:
        silent: If True, suppress log messages
        
    Returns:
        Number of processes terminated
    """
    terminated_count = 0
    
    if not PSUTIL_AVAILABLE:
        return 0
    
    try:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                proc_name = (proc.info.get('name') or '').lower()
                cmdline = ' '.join(proc.info.get('cmdline') or [])
                
                # Check if it's a Chrome browser process with automation flags
                if 'chrome' in proc_name and 'chromedriver' not in proc_name:
                    # Look for Selenium automation flags
                    automation_flags = [
                        '--remote-debugging-port',
                        '--disable-blink-features=AutomationControlled',
                        '--test-type',
                        '--user-data-dir',
                        '--incognito'
                    ]
                    
                    # Check if it has multiple automation flags (more likely to be Selenium-controlled)
                    flag_count = sum(1 for flag in automation_flags if flag in cmdline)
                    if flag_count >= 2:  # At least 2 automation flags
                        try:
                            proc.kill()
                            terminated_count += 1
                            if not silent:
                                logger.debug(f"Terminated Chrome process (automation flags): PID {proc.info['pid']}")
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    except Exception as e:
        if not silent:
            logger.warning(f"Error terminating Chrome by flags: {e}")
    
    return terminated_count


def cleanup_pid_file(scraper_name: str, repo_root: Path):
    """Remove the PID file if it exists"""
    pid_file = get_pid_file_path(scraper_name, repo_root)
    lock_path = pid_file.with_suffix(pid_file.suffix + ".lock")
    lock_fd = _acquire_pid_lock(lock_path)
    try:
        if pid_file.exists():
            pid_file.unlink()
            logger.debug(f"Removed PID file: {pid_file}")
    except Exception as e:
        logger.warning(f"Could not remove PID file: {e}")
    finally:
        _release_pid_lock(lock_path, lock_fd)


def terminate_scraper_pids(scraper_name: str, repo_root: Path, silent: bool = False) -> int:
    """Terminate tracked browser PIDs (Chrome/Firefox) for a specific scraper."""
    terminated = 0
    try:
        terminated += terminate_chrome_pids(scraper_name, repo_root, silent=silent)
    except Exception as e:
        if not silent:
            logger.warning(f"Could not terminate Chrome PIDs for {scraper_name}: {e}")

    try:
        from core.browser.firefox_pid_tracker import terminate_firefox_pids
        terminated += terminate_firefox_pids(scraper_name, repo_root, silent=silent)
    except Exception as e:
        if not silent:
            logger.warning(f"Could not terminate Firefox PIDs for {scraper_name}: {e}")

    return terminated
