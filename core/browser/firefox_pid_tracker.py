#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Firefox/GeckoDriver Process ID Tracker

Tracks Firefox/GeckoDriver process IDs per scraper run so pipeline stop
can terminate only processes belonging to that scraper.
"""

import json
import logging
import os
import tempfile
import time
import subprocess
import sys
from pathlib import Path
from typing import Optional, Set

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


def get_firefox_pids_from_driver(driver) -> Set[int]:
    """
    Extract Firefox and GeckoDriver process IDs from a WebDriver instance.

    Args:
        driver: Selenium WebDriver instance (Firefox/GeckoDriver)

    Returns:
        Set of process IDs (GeckoDriver PID and Firefox browser PIDs)
    """
    pids = set()

    try:
        if hasattr(driver, "service") and hasattr(driver.service, "process"):
            geckodriver_pid = driver.service.process.pid
            if geckodriver_pid:
                pids.add(geckodriver_pid)
                logger.debug(f"Found GeckoDriver PID: {geckodriver_pid}")
    except Exception as e:
        logger.warning(f"Could not get GeckoDriver PID: {e}")

    if PSUTIL_AVAILABLE:
        try:
            geckodriver_pid = None
            try:
                if hasattr(driver, "service") and hasattr(driver.service, "process"):
                    geckodriver_pid = driver.service.process.pid
            except Exception:
                pass

            def get_descendant_pids(pid):
                descendants = set()
                try:
                    parent = psutil.Process(pid)
                    for child in parent.children(recursive=True):
                        descendants.add(child.pid)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                return descendants

            if geckodriver_pid:
                descendant_pids = get_descendant_pids(geckodriver_pid)
                pids.update(descendant_pids)
                logger.debug(f"Found {len(descendant_pids)} descendant process(es) of GeckoDriver {geckodriver_pid}")
        except Exception as e:
            logger.warning(f"Error finding Firefox browser PIDs: {e}")

    return pids


def get_pid_file_path(scraper_name: str, repo_root: Path) -> Path:
    """Get the path to the Firefox PID tracking file for a scraper."""
    return repo_root / f".{scraper_name}_firefox_pids.json"


def save_firefox_pids(scraper_name: str, repo_root: Path, pids: Set[int]) -> None:
    """Save Firefox/GeckoDriver process IDs to a file for later retrieval."""
    pid_file = get_pid_file_path(scraper_name, repo_root)
    lock_path = pid_file.with_suffix(pid_file.suffix + ".lock")
    lock_fd = _acquire_pid_lock(lock_path)
    try:
        existing_pids = set()
        if pid_file.exists():
            try:
                with open(pid_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    existing_pids = set(data.get('pids', []))
            except Exception as e:
                logger.warning(f"Could not read existing Firefox PID file: {e}")

        all_pids = existing_pids | pids
        _atomic_write_json(pid_file, {
            'scraper_name': scraper_name,
            'pids': list(all_pids),
            'updated_at': str(Path(__file__).stat().st_mtime) if Path(__file__).exists() else None
        })
        logger.debug(f"Saved {len(all_pids)} Firefox PIDs to {pid_file}")
    except Exception as e:
        logger.warning(f"Could not save Firefox PIDs: {e}")
    finally:
        _release_pid_lock(lock_path, lock_fd)


def load_firefox_pids(scraper_name: str, repo_root: Path) -> Set[int]:
    """Load Firefox/GeckoDriver process IDs from file."""
    pid_file = get_pid_file_path(scraper_name, repo_root)
    if not pid_file.exists():
        return set()

    try:
        with open(pid_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            while content.endswith('}}') and content.count('{') < content.count('}'):
                content = content[:-1]
            data = json.loads(content)
            file_scraper_name = data.get('scraper_name', '')
            if file_scraper_name and file_scraper_name != scraper_name:
                logger.warning(f"Firefox PID file {pid_file} belongs to {file_scraper_name}, not {scraper_name}. Returning empty set.")
                return set()
            return set(data.get('pids', []))
    except json.JSONDecodeError as e:
        logger.warning(f"Could not parse Firefox PIDs JSON from {pid_file}: {e}. File may be corrupted. Attempting to fix...")
        lock_path = pid_file.with_suffix(pid_file.suffix + ".lock")
        lock_fd = _acquire_pid_lock(lock_path)
        try:
            _atomic_write_json(pid_file, {
                'scraper_name': scraper_name,
                'pids': [],
                'updated_at': None
            })
            logger.info(f"Fixed corrupted Firefox PID file {pid_file}")
        except Exception as fix_error:
            logger.warning(f"Could not fix corrupted Firefox PID file: {fix_error}")
        finally:
            _release_pid_lock(lock_path, lock_fd)
        return set()
    except Exception as e:
        logger.warning(f"Could not load Firefox PIDs from {pid_file}: {e}")
        return set()


def terminate_firefox_pids(scraper_name: str, repo_root: Path, silent: bool = False) -> int:
    """Terminate Firefox/GeckoDriver processes tracked for a specific scraper. Uses DB as primary source."""
    from core.browser.chrome_pid_tracker import _get_run_id_from_file, get_active_pids_from_db
    run_id = _get_run_id_from_file(scraper_name, repo_root)
    pids = get_active_pids_from_db(
        scraper_name, run_id, repo_root,
        browser_types=["firefox"]
    )
    if not pids:
        if not silent:
            logger.debug(f"No Firefox PIDs found for {scraper_name}")
        return 0

    valid_pids = []
    for pid in pids:
        if PSUTIL_AVAILABLE:
            try:
                proc = psutil.Process(pid)
                if not proc.is_running():
                    continue
                proc_name = (proc.name() or '').lower()
                if 'firefox' in proc_name or 'geckodriver' in proc_name:
                    valid_pids.append(pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        else:
            valid_pids.append(pid)

    if not valid_pids:
        if not silent:
            logger.debug(f"No valid Firefox PIDs to terminate for {scraper_name}")
        return 0

    terminated_count = 0
    if not silent:
        logger.info(f"Terminating {len(valid_pids)} Firefox process(es) for {scraper_name}: {sorted(valid_pids)}")

    for pid in valid_pids:
        try:
            if sys.platform == "win32":
                result = subprocess.run(
                    ['taskkill', '/F', '/PID', str(pid)],
                    capture_output=True,
                    timeout=5,
                    text=True
                )
                if result.returncode == 0 or "not found" not in result.stdout.lower():
                    terminated_count += 1
            else:
                result = subprocess.run(
                    ['kill', '-9', '-{}'.format(pid)],
                    capture_output=True,
                    timeout=10
                )
                if result.returncode == 0:
                    terminated_count += 1
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
                logger.debug(f"Could not mark Firefox instances terminated in DB: {e}")

    if not silent:
        logger.info(f"Terminated {terminated_count}/{len(valid_pids)} Firefox process(es) for {scraper_name}")

    return terminated_count


def cleanup_pid_file(scraper_name: str, repo_root: Path) -> None:
    """Remove the Firefox PID file if it exists."""
    pid_file = get_pid_file_path(scraper_name, repo_root)
    lock_path = pid_file.with_suffix(pid_file.suffix + ".lock")
    lock_fd = _acquire_pid_lock(lock_path)
    try:
        if pid_file.exists():
            pid_file.unlink()
            logger.debug(f"Removed Firefox PID file: {pid_file}")
    except Exception as e:
        logger.warning(f"Could not remove Firefox PID file: {e}")
    finally:
        _release_pid_lock(lock_path, lock_fd)
