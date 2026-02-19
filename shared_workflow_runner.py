#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shared Workflow Runner

Provides a unified workflow orchestration for all scrapers with:
- Mandatory backup-first step
- Deterministic run folders
- Consistent logging and error handling
- Single-instance locking
"""

import os
import sys
import json
import shutil
import logging
import subprocess
import atexit
import time
try:
    import fcntl  # For Unix-like systems
except ImportError:
    fcntl = None  # Windows doesn't have fcntl
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Callable, Optional, Any
from abc import ABC, abstractmethod

# Import platform config system
try:
    from core.config.config_manager import ConfigManager
    _PLATFORM_CONFIG_AVAILABLE = True
except ImportError:
    # Fallback if platform_config not available (backward compatibility)
    _PLATFORM_CONFIG_AVAILABLE = False
    PathManager = None
    ConfigResolver = None
    get_path_manager = None
    get_config_resolver = None

# Import Chrome manager for cleanup
try:
    from core.browser.chrome_manager import cleanup_all_chrome_instances
    _CHROME_MANAGER_AVAILABLE = True
except ImportError:
    _CHROME_MANAGER_AVAILABLE = False
    def cleanup_all_chrome_instances(silent=False):
        pass  # No-op if Chrome manager not available

# Optional run ledger (metadata tracking)
try:
    from core.progress.run_ledger import FileRunLedger, RunStatus
    _RUN_LEDGER_AVAILABLE = True
except ImportError:
    FileRunLedger = None
    class RunStatus:
        RUNNING = "running"
        COMPLETED = "completed"
        FAILED = "failed"
        CANCELLED = "cancelled"
        RESUME = "resume"
        STOPPED = "stopped"
    _RUN_LEDGER_AVAILABLE = False

# Optional pipeline checkpoint (for marking resumable state)
try:
    from core.pipeline.pipeline_checkpoint import get_checkpoint_manager, recover_all_stale_checkpoints
    _CHECKPOINT_AVAILABLE = True
except ImportError:
    _CHECKPOINT_AVAILABLE = False
    def get_checkpoint_manager(scraper_name):
        return None
    def recover_all_stale_checkpoints(scraper_names=None):
        return {}

# Optional run metrics tracking (network consumption and active time)
try:
    from core.progress.run_metrics_integration import WorkflowMetricsIntegration
    _METRICS_AVAILABLE = True
except ImportError:
    _METRICS_AVAILABLE = False
    WorkflowMetricsIntegration = None


class ScraperInterface(ABC):
    """Interface that all scrapers must implement"""
    
    @abstractmethod
    def validate_inputs(self) -> Dict[str, Any]:
        """Validate input files and configuration. Returns status dict."""
        pass
    
    @abstractmethod
    def run_steps(self, run_dir: Path) -> Dict[str, Any]:
        """
        Run all scraper steps. Returns status dict with 'status' and 'outputs'.
        All outputs should be written to run_dir/exports/
        """
        pass
    
    @abstractmethod
    def write_outputs(self, run_dir: Path) -> Dict[str, Any]:
        """
        Write final outputs. Returns status dict.
        Outputs should be in run_dir/exports/
        """
        pass


class WorkflowRunner:
    """Unified workflow runner for all scrapers"""
    
    def __init__(self, scraper_name: str, scraper_root: Path, repo_root: Path):
        """
        Initialize workflow runner.

        Args:
            scraper_name: Name of the scraper (e.g., "canada_quebec", "Malaysia", "Argentina")
            scraper_root: Root directory of the scraper (legacy, kept for backward compatibility)
            repo_root: Root directory of the repository (legacy, kept for backward compatibility)
        """
        self.scraper_name = scraper_name
        self.scraper_root = Path(scraper_root).resolve()
        self.repo_root = Path(repo_root).resolve()

        # Use PathManager if available, otherwise fall back to repo-relative paths
        if _PLATFORM_CONFIG_AVAILABLE:
            # Migrated: get_path_manager() -> ConfigManager
            self.backup_dir = ConfigManager.get_backups_dir(scraper_name)  # Scraper-specific backups
            self.runs_dir = ConfigManager.get_runs_dir()
            from core.pipeline.pipeline_start_lock import get_lock_paths
            self.lock_file, _ = get_lock_paths(scraper_name, self.repo_root)
        else:
            # Fallback: use repo-relative paths (backward compatibility)
            self.backup_dir = self.repo_root / "output" / "backups"
            self.runs_dir = self.repo_root / "output" / "runs"
            self.lock_file = self.repo_root / f".{scraper_name}_run.lock"

        # Ensure directories exist
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        # Ensure lock file directory exists
        if self.lock_file:
            self.lock_file.parent.mkdir(parents=True, exist_ok=True)

        self.lock_handle = None
        self._lock_released = False  # Track if lock has been released

        # Register atexit handler to ensure lock is released on process exit
        atexit.register(self._cleanup_lock_on_exit)

        # Current run info
        self.run_id = None
        self.run_dir = None
        self.run_ledger = FileRunLedger() if _RUN_LEDGER_AVAILABLE else None
        
        # Metrics tracking integration
        self.metrics_integration = WorkflowMetricsIntegration() if _METRICS_AVAILABLE else None
    
    def _cleanup_lock_on_exit(self):
        """Cleanup lock file on process exit (atexit handler)"""
        try:
            self.release_lock()
        except:
            pass  # Ignore errors during exit cleanup
        
    def acquire_lock(self) -> bool:
        """Acquire single-instance lock. Returns True if successful."""
        # Reset lock released flag when acquiring new lock
        self._lock_released = False

        try:
            if sys.platform == "win32":
                # Windows - use file creation as lock
                try:
                    # Try to create the file exclusively
                    self.lock_handle = open(self.lock_file, 'x')  # 'x' mode fails if file exists
                    self.lock_handle.write(f"{os.getpid()}\n{datetime.now().isoformat()}\n")
                    self.lock_handle.flush()
                    return True
                except FileExistsError:
                    # Check if lock is stale (older than 1 hour) or process is dead
                    try:
                        if self.lock_file.exists():
                            # First check: Is the lock file older than 1 hour?
                            mtime = self.lock_file.stat().st_mtime
                            age_seconds = datetime.now().timestamp() - mtime
                            if age_seconds > 3600:  # 1 hour
                                # Stale lock, remove it
                                self.lock_file.unlink()
                                # Try again
                                self.lock_handle = open(self.lock_file, 'x')
                                self.lock_handle.write(f"{os.getpid()}\n{datetime.now().isoformat()}\n")
                                self.lock_handle.flush()
                                return True
                            
                            # Second check: Is the process that created the lock still running?
                            try:
                                with open(self.lock_file, 'r') as f:
                                    lock_content = f.read().strip().split('\n')
                                    if lock_content and lock_content[0].isdigit():
                                        lock_pid = int(lock_content[0])
                                        # Check if process is still running
                                        try:
                                            # On Windows, check if process exists
                                            if sys.platform == "win32":
                                                result = subprocess.run(
                                                    ['tasklist', '/FI', f'PID eq {lock_pid}'],
                                                    capture_output=True,
                                                    text=True,
                                                    timeout=2
                                                )
                                                # If PID not found in tasklist, process is dead
                                                if str(lock_pid) not in result.stdout:
                                                    # Process is dead, remove stale lock
                                                    self.lock_file.unlink()
                                                    # Try again
                                                    self.lock_handle = open(self.lock_file, 'x')
                                                    self.lock_handle.write(f"{os.getpid()}\n{datetime.now().isoformat()}\n")
                                                    self.lock_handle.flush()
                                                    return True
                                            else:
                                                # Unix-like: send signal 0 to check if process exists
                                                os.kill(lock_pid, 0)
                                        except (ProcessLookupError, OSError, subprocess.TimeoutExpired, ValueError):
                                            # Process doesn't exist or error checking, remove stale lock
                                            self.lock_file.unlink()
                                            # Try again
                                            self.lock_handle = open(self.lock_file, 'x')
                                            self.lock_handle.write(f"{os.getpid()}\n{datetime.now().isoformat()}\n")
                                            self.lock_handle.flush()
                                            return True
                            except (ValueError, IndexError, IOError):
                                # Lock file format is invalid, remove it
                                self.lock_file.unlink()
                                # Try again
                                self.lock_handle = open(self.lock_file, 'x')
                                self.lock_handle.write(f"{os.getpid()}\n{datetime.now().isoformat()}\n")
                                self.lock_handle.flush()
                                return True
                    except Exception:
                        pass
                    return False
            else:
                # Unix-like
                self.lock_handle = open(self.lock_file, 'w')
                try:
                    fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.lock_handle.write(f"{os.getpid()}\n{datetime.now().isoformat()}\n")
                    self.lock_handle.flush()
                    return True
                except IOError:
                    self.lock_handle.close()
                    self.lock_handle = None
                    return False
        except Exception as e:
            if self.lock_handle:
                try:
                    self.lock_handle.close()
                except:
                    pass
                self.lock_handle = None
            return False
    
    def release_lock(self, silent=False):
        """
        Release single-instance lock and delete lock file.

        Args:
            silent: If True, suppress warning messages (used after successful completion)
        """
        import time

        # Prevent multiple releases
        if self._lock_released:
            return

        # Mark as released immediately to prevent race conditions
        self._lock_released = True

        # Close handle first
        if self.lock_handle:
            try:
                if sys.platform != "win32" and fcntl:
                    # Unix-like: unlock
                    fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_UN)
                self.lock_handle.close()
                # On Windows, give the OS more time to release the file handle
                if sys.platform == "win32":
                    time.sleep(0.5)  # Increased delay for Windows
            except:
                pass
            finally:
                self.lock_handle = None

        # Always delete the lock file with robust retry mechanism
        if not self.lock_file.exists():
            return  # Lock file already gone, nothing to do
        
        # Try multiple strategies to delete the lock file
        max_retries = 8
        retry_delays = [0.2, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0, 3.0]  # Exponential backoff
        
        for attempt in range(max_retries):
            try:
                # Try direct deletion
                self.lock_file.unlink()
                return  # Success!
            except PermissionError:
                # File is locked, wait and retry
                if attempt < max_retries - 1:
                    time.sleep(retry_delays[attempt])
                    continue
            except FileNotFoundError:
                # File already deleted (race condition)
                return
            except Exception as e:
                # Other error, try next strategy
                if attempt < max_retries - 1:
                    time.sleep(retry_delays[attempt])
                    continue
        
        # If direct deletion failed, try renaming as fallback
        try:
            backup_name = self.lock_file.with_suffix('.lock.old')
            # Clean up old backup if exists
            if backup_name.exists():
                try:
                    backup_name.unlink()
                except:
                    # If old backup can't be deleted, use timestamp
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_name = self.lock_file.with_name(f"{self.lock_file.stem}_{timestamp}.lock.old")
            
            # Try to rename (this often works when delete doesn't)
            self.lock_file.rename(backup_name)
            
            # Try to delete renamed file in background (best effort, don't wait)
            def delete_background():
                time.sleep(2.0)  # Wait a bit longer
                try:
                    if backup_name.exists():
                        backup_name.unlink()
                except:
                    pass  # Ignore errors in background cleanup
            
            import threading
            cleanup_thread = threading.Thread(target=delete_background, daemon=True)
            cleanup_thread.start()
            
            # Only log if not silent (rename succeeded, so this is fine)
            if not silent:
                logger = logging.getLogger(__name__)
                logger.info(f"Lock file renamed to {backup_name.name} (will be cleaned up automatically)")
            return
            
        except Exception as rename_error:
            # Rename also failed - this is unusual but not critical
            # The lock file will be cleaned up on next run (stale lock detection)
            if not silent:
                logger = logging.getLogger(__name__)
                logger.warning(f"Could not delete or rename lock file {self.lock_file}")
                logger.warning("  This is not critical - the lock will be automatically cleaned up on next run.")
            # Don't raise exception - workflow completed successfully

    @staticmethod
    def stop_pipeline(scraper_name: str, repo_root: Path = None) -> Dict[str, Any]:
        """
        Stop a running pipeline by killing its process.

        Args:
            scraper_name: Name of the scraper to stop (e.g., "Malaysia", "canada_quebec")
            repo_root: Repository root path (optional, auto-detected if not provided)

        Returns:
            Dict with status and message
        """
        import signal

        if repo_root is None:
            # Auto-detect repo root (assuming this file is in repo root)
            repo_root = Path(__file__).resolve().parent

        # Get lock file path - check both new and old locations
        lock_file = None
        if _PLATFORM_CONFIG_AVAILABLE:
            # Migrated: get_path_manager() -> ConfigManager
            from core.pipeline.pipeline_start_lock import get_lock_paths
            lock_file, _ = get_lock_paths(scraper_name, repo_root)
            # Also check old location as fallback
            old_lock_file = repo_root / f".{scraper_name}_run.lock"
            if not lock_file.exists() and old_lock_file.exists():
                lock_file = old_lock_file
        else:
            lock_file = repo_root / f".{scraper_name}_run.lock"

        # Check if lock file exists
        if not lock_file or not lock_file.exists():
            return {
                "status": "error",
                "message": f"No running pipeline found for {scraper_name} (lock file not found)"
            }

        # Read PID from lock file
        try:
            with open(lock_file, 'r') as f:
                lock_content = f.read().strip().split('\n')
                if not lock_content or not lock_content[0].isdigit():
                    return {
                        "status": "error",
                        "message": f"Invalid lock file format (no PID found)"
                    }

                pid = int(lock_content[0])
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to read lock file: {str(e)}"
            }

        # Check if process is still running
        try:
            if sys.platform == "win32":
                # Windows: use tasklist to check if process exists
                result = subprocess.run(
                    ['tasklist', '/FI', f'PID eq {pid}'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if str(pid) not in result.stdout:
                    # Process is dead, clean up stale lock
                    try:
                        lock_file.unlink()
                    except:
                        pass
                    return {
                        "status": "error",
                        "message": f"Process {pid} is not running (cleaning up stale lock)"
                    }

                # IMPORTANT: Clean up Chrome instances FIRST (scraper-specific) BEFORE killing main process
                # This prevents killing Chrome instances that belong to other scrapers
                try:
                    from core.browser.chrome_pid_tracker import terminate_scraper_pids
                    terminated_count = terminate_scraper_pids(scraper_name, repo_root, silent=True)
                    if terminated_count > 0:
                        # Wait a moment for Chrome processes to fully terminate before killing main process
                        # This prevents taskkill /T from killing Chrome instances that are still shutting down
                        import time
                        time.sleep(1.0)  # Give Chrome processes time to fully terminate
                except Exception:
                    # Don't use general cleanup - it would kill all scrapers' Chrome instances
                    pass

                # Kill the process tree (including child processes) AFTER cleaning up Chrome instances
                # Use /PID instead of /T to avoid killing unrelated child processes
                # Note: We've already cleaned up Chrome instances above, so /T should be safe,
                # but we use /PID first to be more selective
                try:
                    # First try to kill just the main process (without /T flag)
                    # This is safer and won't kill child processes that might belong to other scrapers
                    kill_result = subprocess.run(
                        ['taskkill', '/F', '/PID', str(pid)],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    
                    # If that didn't work or there are still child processes, use /T flag
                    # But only if the main process is still running
                    import time
                    time.sleep(0.5)  # Brief wait to see if process terminated
                    try:
                        # Check if process still exists
                        check_result = subprocess.run(
                            ['tasklist', '/FI', f'PID eq {pid}'],
                            capture_output=True,
                            text=True,
                            timeout=3
                        )
                        if str(pid) in check_result.stdout:
                            # Process still exists, use /T to kill tree
                            kill_result = subprocess.run(
                                ['taskkill', '/F', '/T', '/PID', str(pid)],
                                capture_output=True,
                                text=True,
                                timeout=10
                            )
                    except Exception:
                        # If check fails, assume process is dead
                        pass

                    # Clean up lock file
                    try:
                        lock_file.unlink()
                    except:
                        pass

                    # Mark checkpoint as resumable so pipeline can be resumed
                    try:
                        if _CHECKPOINT_AVAILABLE:
                            cp = get_checkpoint_manager(scraper_name)
                            if cp:
                                cp.mark_as_resumable()
                    except Exception as e:
                        logging.getLogger(__name__).warning(f"Failed to mark checkpoint as resumable: {e}")

                    # Mark run ledger status as RESUME (file-based)
                    run_id_to_pause = None
                    try:
                        if _RUN_LEDGER_AVAILABLE:
                            ledger = FileRunLedger()
                            # Find the latest running run for this scraper
                            running_runs = ledger.list_runs(limit=10, status=RunStatus.RUNNING, scraper_name=scraper_name)
                            if running_runs:
                                latest_run = running_runs[0]
                                run_id_to_pause = latest_run.run_id
                                ledger.update_run_status(latest_run.run_id, RunStatus.RESUME)
                    except Exception as e:
                        logging.getLogger(__name__).warning(f"Failed to update run ledger status: {e}")
                    
                    # Pause metrics tracking for the run
                    if run_id_to_pause and _METRICS_AVAILABLE:
                        try:
                            from core.progress.run_metrics_integration import WorkflowMetricsIntegration
                            metrics_integration = WorkflowMetricsIntegration()
                            metrics = metrics_integration.on_run_pause(run_id_to_pause)
                            if metrics:
                                logging.getLogger(__name__).info(
                                    f"Paused metrics for {run_id_to_pause}: "
                                    f"{metrics.active_duration_seconds:.2f}s, "
                                    f"{metrics.network_total_gb:.4f} GB"
                                )
                        except Exception as e:
                            logging.getLogger(__name__).warning(f"Failed to pause metrics tracking: {e}")

                    # Mark DATABASE run_ledger status as RESUME (SQLite table)
                    _db = None
                    try:
                        from core.db.models import run_ledger_mark_resumable
                        from core.db.connection import CountryDB

                        # Migrated: get_path_manager() -> ConfigManager
                        output_dir = ConfigManager.get_output_dir(scraper_name)
                        db_file = output_dir / f"{scraper_name.lower()}.db"
                        if db_file.exists():
                            _db = CountryDB(scraper_name, db_path=db_file)
                            conn = _db.connect()
                            # Find the latest running run and mark it as resume
                            cursor = conn.execute(
                                "SELECT run_id FROM run_ledger WHERE status = 'running' AND scraper_name = ? ORDER BY started_at DESC LIMIT 1",
                                (scraper_name,)
                            )
                            row = cursor.fetchone()
                            if row:
                                sql, params = run_ledger_mark_resumable(row[0])
                                conn.execute(sql, params)
                                conn.commit()
                                logging.getLogger(__name__).info(f"Marked DB run {row[0]} as 'resume'")
                    except Exception as e:
                        logging.getLogger(__name__).warning(f"Failed to update DB run ledger status: {e}")
                    finally:
                        if _db is not None:
                            try:
                                _db.close()
                            except Exception:
                                pass

                    return {
                        "status": "ok",
                        "message": f"Stopped {scraper_name} pipeline (PID {pid}). Pipeline can be resumed.",
                        "pid": pid,
                        "resumable": True
                    }
                except subprocess.TimeoutExpired:
                    return {
                        "status": "error",
                        "message": f"Timeout while trying to stop process {pid}"
                    }
                except Exception as e:
                    return {
                        "status": "error",
                        "message": f"Failed to kill process {pid}: {str(e)}"
                    }
            else:
                # Unix-like: send SIGTERM, then SIGKILL if needed
                try:
                    os.kill(pid, 0)  # Check if process exists
                except (ProcessLookupError, OSError):
                    # Process is dead, clean up stale lock
                    try:
                        lock_file.unlink()
                    except:
                        pass
                    return {
                        "status": "error",
                        "message": f"Process {pid} is not running (cleaning up stale lock)"
                    }

                try:
                    # Try graceful shutdown first (SIGTERM)
                    os.kill(pid, signal.SIGTERM)

                    # Wait a bit for graceful shutdown
                    import time
                    time.sleep(2)

                    # Check if still running
                    try:
                        os.kill(pid, 0)
                        # Still running, force kill
                        os.kill(pid, signal.SIGKILL)
                    except (ProcessLookupError, OSError):
                        # Process terminated gracefully
                        pass

                    # Clean up Chrome instances before cleaning up lock file (scraper-specific only)
                    try:
                        from core.browser.chrome_pid_tracker import terminate_scraper_pids
                        terminate_scraper_pids(scraper_name, repo_root, silent=True)
                    except Exception:
                        # Don't use general cleanup - it would kill all scrapers' Chrome instances
                        pass

                    # Clean up lock file
                    try:
                        lock_file.unlink()
                    except:
                        pass

                    # Mark checkpoint as resumable so pipeline can be resumed
                    try:
                        if _CHECKPOINT_AVAILABLE:
                            cp = get_checkpoint_manager(scraper_name)
                            if cp:
                                cp.mark_as_resumable()
                    except Exception as e:
                        logging.getLogger(__name__).warning(f"Failed to mark checkpoint as resumable: {e}")

                    # Mark run ledger status as RESUME (file-based)
                    run_id_to_pause_unix = None
                    try:
                        if _RUN_LEDGER_AVAILABLE:
                            ledger = FileRunLedger()
                            # Find the latest running run for this scraper
                            running_runs = ledger.list_runs(limit=10, status=RunStatus.RUNNING, scraper_name=scraper_name)
                            if running_runs:
                                latest_run = running_runs[0]
                                run_id_to_pause_unix = latest_run.run_id
                                ledger.update_run_status(latest_run.run_id, RunStatus.RESUME)
                    except Exception as e:
                        logging.getLogger(__name__).warning(f"Failed to update run ledger status: {e}")
                    
                    # Pause metrics tracking for the run
                    if run_id_to_pause_unix and _METRICS_AVAILABLE:
                        try:
                            from core.progress.run_metrics_integration import WorkflowMetricsIntegration
                            metrics_integration = WorkflowMetricsIntegration()
                            metrics = metrics_integration.on_run_pause(run_id_to_pause_unix)
                            if metrics:
                                logging.getLogger(__name__).info(
                                    f"Paused metrics for {run_id_to_pause_unix}: "
                                    f"{metrics.active_duration_seconds:.2f}s, "
                                    f"{metrics.network_total_gb:.4f} GB"
                                )
                        except Exception as e:
                            logging.getLogger(__name__).warning(f"Failed to pause metrics tracking: {e}")

                    # Mark DATABASE run_ledger status as RESUME (SQLite table)
                    _db = None
                    try:
                        from core.db.models import run_ledger_mark_resumable
                        from core.db.connection import CountryDB

                        # Migrated: get_path_manager() -> ConfigManager
                        output_dir = ConfigManager.get_output_dir(scraper_name)
                        db_file = output_dir / f"{scraper_name.lower()}.db"
                        if db_file.exists():
                            _db = CountryDB(scraper_name, db_path=db_file)
                            conn = _db.connect()
                            # Find the latest running run and mark it as resume
                            cursor = conn.execute(
                                "SELECT run_id FROM run_ledger WHERE status = 'running' AND scraper_name = ? ORDER BY started_at DESC LIMIT 1",
                                (scraper_name,)
                            )
                            row = cursor.fetchone()
                            if row:
                                sql, params = run_ledger_mark_resumable(row[0])
                                conn.execute(sql, params)
                                conn.commit()
                                logging.getLogger(__name__).info(f"Marked DB run {row[0]} as 'resume'")
                    except Exception as e:
                        logging.getLogger(__name__).warning(f"Failed to update DB run ledger status: {e}")
                    finally:
                        if _db is not None:
                            try:
                                _db.close()
                            except Exception:
                                pass

                    return {
                        "status": "ok",
                        "message": f"Stopped {scraper_name} pipeline (PID {pid}). Pipeline can be resumed.",
                        "pid": pid,
                        "resumable": True
                    }
                except Exception as e:
                    return {
                        "status": "error",
                        "message": f"Failed to kill process {pid}: {str(e)}"
                    }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error stopping pipeline: {str(e)}"
            }

    def create_backup(self) -> Dict[str, Any]:
        """
        Create backup of inputs, config, and previous outputs.
        This MUST be called before any scraper step executes.
        """
        try:
            # Generate run ID
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.run_id = f"{self.scraper_name}_{timestamp}"
            backup_dir = self.backup_dir / self.run_id
            
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            backup_manifest = {
                "run_id": self.run_id,
                "scraper_name": self.scraper_name,
                "timestamp": timestamp,
                "backup_time": datetime.now().isoformat(),
                "backed_up": {
                    "config": [],
                    "inputs": [],
                    "outputs": [],
                    "final_outputs": []
                }
            }
            
            # CRITICAL: Do NOT backup config files (.env, platform.env, etc.)
            # Config files must NEVER be written to backups, output/, runs/, or sessions/
            # They live ONLY in Documents/ScraperPlatform/config/ (single source of truth)
            # Only backup metadata about config location for reference
            config_metadata = {
                "note": "Config files are NOT backed up. They must remain in Documents/ScraperPlatform/config/",
                "config_location": "Documents/ScraperPlatform/config/",
                "platform_env": "Documents/ScraperPlatform/config/platform.env",
                "scraper_env": f"Documents/ScraperPlatform/config/{self.scraper_name}.env"
            }
            
            backup_manifest["backed_up"]["config"] = config_metadata
            
            # Backup input files (from platform root if available, else scraper root)
            if _PLATFORM_CONFIG_AVAILABLE:
                # Migrated: get_path_manager() -> ConfigManager
                input_dir = ConfigManager.get_input_dir(self.scraper_name)
            else:
                # Fallback: use scraper root
                input_dir = self.scraper_root / "input"
                if not input_dir.exists():
                    input_dir = self.scraper_root / "Input"
            
            if input_dir.exists():
                input_backup_dir = backup_dir / "inputs"
                for item in input_dir.rglob("*"):
                    if item.is_file():
                        rel_path = item.relative_to(input_dir)
                        backup_path = input_backup_dir / rel_path
                        backup_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(item, backup_path)
                        backup_manifest["backed_up"]["inputs"].append(str(rel_path))
            
            # Backup previous outputs (if any)
            old_output_dir = self.scraper_root / "output"
            if old_output_dir.exists():
                output_backup_dir = backup_dir / "previous_outputs"
                for item in old_output_dir.rglob("*"):
                    if item.is_file():
                        rel_path = item.relative_to(old_output_dir)
                        backup_path = output_backup_dir / rel_path
                        backup_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(item, backup_path)
                        backup_manifest["backed_up"]["outputs"].append(str(rel_path))
            
            # Backup final output files (from exports directory) for this scraper
            if _PLATFORM_CONFIG_AVAILABLE:
                # Migrated: get_path_manager() -> ConfigManager
                # Use exports directory for final reports (scraper-specific)
                from core.config.config_manager import ConfigManager
                central_output_dir = ConfigManager.get_exports_dir(self.scraper_name)
                
                # Scraper-specific final output patterns
                scraper_patterns = {
                    "canada_quebec": ["canadaquebecreport"],
                    "Malaysia": ["malaysia"],
                    "Argentina": ["alfabeta_report"],
                    "Taiwan": ["taiwan_drug_code_details"]
                }
                
                patterns = scraper_patterns.get(self.scraper_name, [])
                final_output_files = []
                
                if central_output_dir.exists() and patterns:
                    final_backup_dir = backup_dir / "final_outputs"
                    for item in central_output_dir.iterdir():
                        if item.is_file() and item.suffix.lower() in ['.csv', '.xlsx']:
                            item_lower = item.name.lower()
                            # Check if file matches this scraper's pattern
                            if any(pattern in item_lower for pattern in patterns):
                                backup_path = final_backup_dir / item.name
                                backup_path.parent.mkdir(parents=True, exist_ok=True)
                                shutil.copy2(item, backup_path)
                                final_output_files.append(item.name)
                    
                    if final_output_files:
                        backup_manifest["backed_up"]["final_outputs"] = final_output_files
            
            # Save manifest
            manifest_path = backup_dir / "manifest.json"
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(backup_manifest, f, indent=2, ensure_ascii=False)
            
            return {
                "status": "ok",
                "backup_dir": str(backup_dir),
                "run_id": self.run_id,
                "manifest": backup_manifest
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Backup failed: {str(e)}"
            }
    
    def create_run_folder(self) -> Path:
        """Create deterministic run folder structure."""
        if not self.run_id:
            raise RuntimeError("Run ID not set. Call create_backup() first.")
        
        # Use PathManager if available, otherwise fall back to repo-relative paths
        # Create run directory structure
        self.run_dir = self.runs_dir / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "logs").mkdir(exist_ok=True)
        (self.run_dir / "artifacts").mkdir(exist_ok=True)
        (self.run_dir / "exports").mkdir(exist_ok=True)
        
        return self.run_dir

    def _record_run_start(self, run_dir: Path, backup_dir: Optional[str] = None) -> None:
        """Record run start in the run ledger (best-effort)."""
        if not self.run_ledger or not self.run_id:
            return
        try:
            pipeline = {
                "source": self.scraper_name,
                "run_type": "workflow",
                "environment": "local",
            }
            paths = {
                "run_dir": str(run_dir),
                "backup_dir": backup_dir,
            }
            self.run_ledger.record_run_start(
                run_id=self.run_id,
                scraper_name=self.scraper_name,
                run_dir=run_dir,
                pipeline=pipeline,
                paths=paths,
            )
        except Exception as e:
            logging.getLogger(__name__).warning("Run ledger start failed: %s", e)

    def _record_run_end(
        self,
        status: "RunStatus",
        run_dir: Optional[Path],
        backup_dir: Optional[str] = None,
        manifest_path: Optional[Path] = None,
        outputs: Optional[List[str]] = None,
        error_message: Optional[str] = None,
        duration_seconds: Optional[float] = None,
    ) -> None:
        """Record run end in the run ledger (best-effort)."""
        if not self.run_ledger or not self.run_id or not run_dir:
            return
        try:
            artifacts: Dict[str, List[str]] = {
                "logs": [],
                "outputs": outputs or [],
                "exports": [],
                "manifests": [],
            }
            log_path = run_dir / "logs" / "run.log"
            if log_path.exists():
                artifacts["logs"].append(str(log_path))
            if manifest_path and manifest_path.exists():
                artifacts["manifests"].append(str(manifest_path))

            metrics: Dict[str, Any] = {}
            if duration_seconds is not None:
                metrics["duration_seconds"] = duration_seconds

            error = {"message": error_message} if error_message else None
            paths = {
                "run_dir": str(run_dir),
                "backup_dir": backup_dir,
                "manifest": str(manifest_path) if manifest_path else None,
            }
            self.run_ledger.record_run_end(
                run_id=self.run_id,
                status=status,
                run_dir=run_dir,
                artifacts=artifacts,
                metrics=metrics,
                error=error,
                paths=paths,
            )
        except Exception as e:
            logging.getLogger(__name__).warning("Run ledger end failed: %s", e)
    
    def setup_logging(self, run_dir: Path) -> logging.Logger:
        """Setup logging for this run."""
        log_file = run_dir / "logs" / "run.log"
        
        logger = logging.getLogger(f"{self.scraper_name}_{self.run_id}")
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        logger.handlers.clear()
        
        # File handler
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    def run(self, scraper: ScraperInterface, progress_callback: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
        """
        Execute the complete workflow.
        
        Args:
            scraper: Scraper instance implementing ScraperInterface
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dict with run status and results
        """
        def progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            else:
                print(msg)
        
        logger = None
        run_dir = None
        run_started_monotonic = None
        ledger_completed = False

        def record_end(
            status: "RunStatus",
            error_message: Optional[str] = None,
            manifest_path: Optional[Path] = None,
            outputs: Optional[List[str]] = None,
            backup_dir: Optional[str] = None,
        ) -> None:
            nonlocal ledger_completed
            if ledger_completed:
                return
            duration_seconds = None
            if run_started_monotonic is not None:
                duration_seconds = time.monotonic() - run_started_monotonic
            self._record_run_end(
                status=status,
                run_dir=run_dir,
                backup_dir=backup_dir,
                manifest_path=manifest_path,
                outputs=outputs,
                error_message=error_message,
                duration_seconds=duration_seconds,
            )
            ledger_completed = True
        try:
            # Step 1: Acquire lock
            progress(f"[{self.scraper_name}] Acquiring execution lock...")
            if not self.acquire_lock():
                return {
                    "status": "error",
                    "message": "Another instance is already running. Please wait for it to complete."
                }
            
            try:
                # Step 2: Create backup (MANDATORY FIRST STEP)
                progress(f"[{self.scraper_name}] Creating backup (mandatory first step)...")
                backup_result = self.create_backup()
                if backup_result["status"] != "ok":
                    return backup_result
                
                progress(f"[{self.scraper_name}] Backup created: {backup_result['backup_dir']}")
                
                # Step 3: Create run folder
                progress(f"[{self.scraper_name}] Creating run folder...")
                run_dir = self.create_run_folder()
                progress(f"[{self.scraper_name}] Run folder: {run_dir}")

                run_started_monotonic = time.monotonic()
                self._record_run_start(run_dir, backup_dir=str(backup_result.get("backup_dir")))
                
                # Start metrics tracking (network consumption and active time)
                if self.metrics_integration and self.metrics_integration.is_available():
                    try:
                        self.metrics_integration.on_run_start(self.run_id, self.scraper_name)
                        if logger:
                            logger.info(f"Started metrics tracking for run: {self.run_id}")
                    except Exception as e:
                        if logger:
                            logger.warning(f"Failed to start metrics tracking: {e}")
                
                # Step 4: Setup logging
                logger = self.setup_logging(run_dir)
                logger.info(f"Starting run: {self.run_id}")
                logger.info(f"Scraper: {self.scraper_name}")
                logger.info(f"Run directory: {run_dir}")
                
                # Step 5: Validate inputs
                progress(f"[{self.scraper_name}] Validating inputs...")
                validation_result = scraper.validate_inputs()
                if logger:
                    logger.info(f"Validation result: {validation_result}")
                if validation_result.get("status") not in ("ok", "warning"):
                    record_end(
                        status=RunStatus.FAILED,
                        error_message=f"Input validation failed: {validation_result.get('message', 'Unknown error')}",
                        backup_dir=str(backup_result.get("backup_dir")),
                    )
                    # Complete metrics tracking for failed validation
                    if self.metrics_integration and self.metrics_integration.is_available():
                        try:
                            self.metrics_integration.on_run_complete(self.run_id, "failed")
                        except Exception:
                            pass
                    return {
                        "status": "error",
                        "message": f"Input validation failed: {validation_result.get('message', 'Unknown error')}",
                        "run_id": self.run_id,
                        "run_dir": str(run_dir)
                    }
                
                # Step 6: Run steps
                progress(f"[{self.scraper_name}] Running scraper steps...")
                steps_result = scraper.run_steps(run_dir)
                if logger:
                    logger.info(f"Steps result: {steps_result}")
                if steps_result.get("status") != "ok":
                    record_end(
                        status=RunStatus.FAILED,
                        error_message=f"Steps execution failed: {steps_result.get('message', 'Unknown error')}",
                        backup_dir=str(backup_result.get("backup_dir")),
                    )
                    # Complete metrics tracking for failed run
                    if self.metrics_integration and self.metrics_integration.is_available():
                        try:
                            self.metrics_integration.on_run_complete(self.run_id, "failed")
                        except Exception:
                            pass
                    return {
                        "status": "error",
                        "message": f"Steps execution failed: {steps_result.get('message', 'Unknown error')}",
                        "run_id": self.run_id,
                        "run_dir": str(run_dir)
                    }
                
                # Step 7: Write outputs
                progress(f"[{self.scraper_name}] Writing outputs...")
                outputs_result = scraper.write_outputs(run_dir)
                if logger:
                    logger.info(f"Outputs result: {outputs_result}")
                
                # Step 8: Create manifest
                manifest = {
                    "run_id": self.run_id,
                    "scraper_name": self.scraper_name,
                    "start_time": backup_result["manifest"]["backup_time"],
                    "end_time": datetime.now().isoformat(),
                    "status": "completed",
                    "inputs": validation_result.get("inputs", []),
                    "outputs": outputs_result.get("outputs", []),
                    "backup_dir": str(backup_result["backup_dir"])
                }
                
                manifest_path = run_dir / "manifest.json"
                with open(manifest_path, 'w', encoding='utf-8') as f:
                    json.dump(manifest, f, indent=2, ensure_ascii=False)

                record_end(
                    status=RunStatus.COMPLETED,
                    manifest_path=manifest_path,
                    outputs=outputs_result.get("outputs", []),
                    backup_dir=str(backup_result.get("backup_dir")),
                )
                
                # Complete metrics tracking
                if self.metrics_integration and self.metrics_integration.is_available():
                    try:
                        metrics = self.metrics_integration.on_run_complete(self.run_id, "completed")
                        if metrics and logger:
                            logger.info(f"Metrics - Duration: {metrics.active_duration_seconds:.2f}s, "
                                       f"Network: {metrics.network_total_gb:.4f} GB")
                    except Exception as e:
                        if logger:
                            logger.warning(f"Failed to complete metrics tracking: {e}")
                
                if logger:
                    logger.info(f"Run completed successfully: {self.run_id}")
                progress(f"[{self.scraper_name}] Run completed: {self.run_id}")
                
                # Clean up Chrome instances before releasing lock (scraper-specific only)
                progress(f"[{self.scraper_name}] Cleaning up Chrome instances...")
                try:
                    from core.browser.chrome_pid_tracker import terminate_scraper_pids
                    terminate_scraper_pids(self.scraper_name, self.repo_root, silent=True)
                except Exception as e:
                    if logger:
                        logger.warning(f"Error during Chrome cleanup: {e}")
                
                # Release and delete lock after successful completion
                # Suppress any lock deletion warnings since workflow succeeded
                progress(f"[{self.scraper_name}] Releasing execution lock...")
                self.release_lock(silent=True)  # Silent mode - don't show warnings after success
                if logger:
                    logger.info("Execution lock released")

                return {
                    "status": "ok",
                    "run_id": self.run_id,
                    "run_dir": str(run_dir),
                    "backup_dir": str(backup_result["backup_dir"]),
                    "manifest": manifest,
                    "outputs": outputs_result.get("outputs", [])
                }

            finally:
                # Clean up Chrome instances before releasing lock (scraper-specific only)
                try:
                    from core.browser.chrome_pid_tracker import terminate_scraper_pids
                    terminate_scraper_pids(self.scraper_name, self.repo_root, silent=True)
                except Exception as e:
                    if logger:
                        logger.warning(f"Error during Chrome cleanup: {e}")
                
                # Always release lock (in case of early return or error)
                # Will no-op if already released due to _lock_released flag
                self.release_lock(silent=True)
                
        except Exception as e:
            record_end(
                status=RunStatus.FAILED,
                error_message=str(e),
                backup_dir=str(backup_result.get("backup_dir")) if "backup_result" in locals() else None,
            )
            # Complete metrics tracking for exception
            if self.metrics_integration and self.metrics_integration.is_available() and self.run_id:
                try:
                    self.metrics_integration.on_run_complete(self.run_id, "failed")
                except Exception:
                    pass
            if logger:
                logger.error(f"Workflow error: {str(e)}", exc_info=True)
            else:
                print(f"Workflow error: {str(e)}")
            # Lock will be released by finally block, no need to release here
            return {
                "status": "error",
                "message": f"Workflow execution failed: {str(e)}",
                "run_id": self.run_id,
                "run_dir": str(self.run_dir) if self.run_dir else None
            }


def recover_stale_pipelines(scraper_names: List[str] = None) -> Dict[str, Any]:
    """
    Recover stale 'running' pipelines on app startup.

    This function should be called when the application starts to handle
    scenarios where the system crashed or was terminated unexpectedly
    while a pipeline was running.

    Logic:
    - For each scraper, find all runs with status='running'
    - Mark the LATEST 'running' run as 'resume' (can be resumed)
    - Mark all OTHER 'running' runs as 'stopped' (cannot be resumed)
    - Also recover stale checkpoints

    Args:
        scraper_names: List of scraper names to check. If None, checks common scrapers.

    Returns:
        Dict with recovery results:
        {
            "checkpoint_recovery": {scraper_name: bool, ...},
            "run_ledger_recovery": {"resumed": [...], "stopped": [...]},
            "total_recovered": int
        }
    """
    logger = logging.getLogger(__name__)

    if scraper_names is None:
        # Default list of scrapers (include Russia for recovery)
        scraper_names = [
            "India", "Malaysia", "Argentina", "Russia", "CanadaOntario", "canada_quebec",
            "Chile", "Korea", "NewZealand", "SouthAfrica", "Thailand"
        ]

    result = {
        "checkpoint_recovery": {},
        "run_ledger_recovery": {"resumed": [], "stopped": []},
        "total_recovered": 0
    }

    # 1. Recover stale checkpoints
    if _CHECKPOINT_AVAILABLE:
        try:
            checkpoint_results = recover_all_stale_checkpoints(scraper_names)
            result["checkpoint_recovery"] = checkpoint_results
            for scraper_name, recovered in checkpoint_results.items():
                if recovered:
                    result["total_recovered"] += 1
                    logger.info(f"Recovered stale checkpoint for {scraper_name}")
        except Exception as e:
            logger.warning(f"Failed to recover stale checkpoints: {e}")

    # 2. Recover stale run ledger entries
    if _RUN_LEDGER_AVAILABLE:
        try:
            ledger = FileRunLedger()
            ledger_result = ledger.recover_stale_runs()
            result["run_ledger_recovery"] = ledger_result
            result["total_recovered"] += len(ledger_result.get("resumed", []))
            result["total_recovered"] += len(ledger_result.get("stopped", []))
        except Exception as e:
            logger.warning(f"Failed to recover stale run ledger entries: {e}")

    if result["total_recovered"] > 0:
        logger.info(f"Startup recovery: recovered {result['total_recovered']} stale pipeline(s)")
    else:
        logger.debug("Startup recovery: no stale pipelines found")

    return result


def get_resumable_pipelines(scraper_names: List[str] = None) -> Dict[str, Dict[str, Any]]:
    """
    Get list of pipelines that can be resumed.

    Args:
        scraper_names: List of scraper names to check. If None, checks common scrapers.

    Returns:
        Dict mapping scraper name to resumable run info:
        {
            "Malaysia": {
                "run_id": "...",
                "last_step": 2,
                "last_step_name": "Product Details",
                "next_step": 3,
                "created_at": "..."
            },
            ...
        }
    """
    if scraper_names is None:
        scraper_names = [
            "India", "Malaysia", "Argentina", "Russia", "CanadaOntario", "canada_quebec",
            "Chile", "Korea", "NewZealand", "SouthAfrica", "Thailand"
        ]

    resumable = {}

    for scraper_name in scraper_names:
        try:
            # Check checkpoint first
            if _CHECKPOINT_AVAILABLE:
                cp = get_checkpoint_manager(scraper_name)
                if cp and cp.is_resumable():
                    info = cp.get_checkpoint_info()
                    metadata = cp.get_metadata()
                    resumable[scraper_name] = {
                        "source": "checkpoint",
                        "last_step": info.get("last_completed_step"),
                        "last_step_name": metadata.get("current_step_name", "Unknown"),
                        "next_step": info.get("next_step", 0),
                        "last_run": info.get("last_run"),
                        "status": cp.get_status()
                    }
                    continue

            # Check run ledger
            if _RUN_LEDGER_AVAILABLE:
                ledger = FileRunLedger()
                run = ledger.get_resumable_run(scraper_name)
                if run:
                    resumable[scraper_name] = {
                        "source": "run_ledger",
                        "run_id": run.run_id,
                        "created_at": run.created_at,
                        "status": run.status.value
                    }
        except Exception:
            pass

    return resumable
