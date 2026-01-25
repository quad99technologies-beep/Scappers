#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration Manager - Single Source of Truth

Provides deterministic configuration loading and path management.
All runtime files are created under the repository root directory.

CRITICAL RULES:
- NEVER writes .env or platform.env files anywhere
- NEVER loads env from repo root, CWD, output/, backups/, runs/, or sessions/
- ALWAYS uses absolute paths under repository root/
- Enforces single-instance lock to prevent EXE runaway sessions
"""

import os
import sys
import json
import logging
import atexit
import time
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Global lock handle for single-instance enforcement
_app_lock_handle = None
_app_lock_file = None
_app_lock_released = False


class ConfigManager:
    """
    Centralized configuration and path manager.
    
    All paths resolve to: repository root directory
    Config files are read-only from: repository root/config/
    """
    
    _app_root: Optional[Path] = None
    _initialized: bool = False
    _loaded_env: Dict[str, Dict[str, str]] = {}
    
    @classmethod
    def _detect_repo_root(cls) -> Path:
        """Detect repository root by looking for core/ directory"""
        # Start from this file's location
        current = Path(__file__).resolve()
        # This file is in core/, so repo root is parent
        repo_root = current.parent.parent
        # Verify by checking for common repo files
        if (repo_root / "core").exists() and (repo_root / "core" / "config_manager.py").exists():
            return repo_root
        # Fallback: assume current directory is repo root
        return Path.cwd()
    
    @classmethod
    def get_app_root(cls) -> Path:
        """
        Get application root directory: repository root
        
        Returns:
            Path to repository root (always absolute)
        """
        if cls._app_root is None:
            cls._app_root = cls._detect_repo_root()
        
        return cls._app_root
    
    @classmethod
    def ensure_dirs(cls) -> None:
        """
        Ensure all required directories exist under app root.
        Must be called before any file operations.
        """
        app_root = cls.get_app_root()
        
        # Create all required directories
        dirs = [
            app_root / "config",
            app_root / "input",
            app_root / "output",
            app_root / "exports",  # Exports in root folder, not in output/
            app_root / "runs",  # Runs in root folder, not in output/ (used by WorkflowRunner)
            app_root / "backups",  # Backups in root folder, not in output/
            app_root / "sessions",
            # Note: logs/ folder removed - logs are stored in runs/{run_id}/logs/ instead
            app_root / "cache",
        ]
        
        for dir_path in dirs:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Create scraper-specific subdirectories
        scraper_names = ["CanadaQuebec", "Malaysia", "Argentina", "Belarus", "CanadaOntario", "NorthMacedonia", "Netherlands", "Tender_Chile", "India", "Russia", "Taiwan"]
        for scraper_name in scraper_names:
            (app_root / "input" / scraper_name).mkdir(parents=True, exist_ok=True)
            (app_root / "output" / scraper_name).mkdir(parents=True, exist_ok=True)
            (app_root / "exports" / scraper_name).mkdir(parents=True, exist_ok=True)  # Exports in root folder
            (app_root / "backups" / scraper_name).mkdir(parents=True, exist_ok=True)  # Backups in root folder
        
        cls._initialized = True
        logger.debug(f"Initialized app directories under: {app_root}")
    
    @classmethod
    def env_paths(cls, scraper_name: str) -> Tuple[Path, Path]:
        """
        Get environment file paths for a scraper.
        
        Args:
            scraper_name: Name of the scraper (e.g., "CanadaQuebec", "Malaysia")
        
        Returns:
            Tuple of (platform_env_path, scraper_env_path)
            Both paths are absolute and under repository root/config/
        """
        config_dir = cls.get_app_root() / "config"
        platform_env = config_dir / "platform.env"
        scraper_env = config_dir / f"{scraper_name}.env"
        
        return (platform_env, scraper_env)
    
    @classmethod
    def load_env(cls, scraper_name: str, required_keys: Optional[list] = None) -> Dict[str, str]:
        """
        Load environment variables in strict deterministic order:
        1. platform.env (must exist)
        2. {scraper_name}.env (optional, overrides platform)
        
        Args:
            scraper_name: Name of the scraper
            required_keys: Optional list of required env keys to validate
        
        Returns:
            Dict of loaded environment variables
        
        Raises:
            FileNotFoundError: If platform.env does not exist
            ValueError: If required keys are missing
        """
        if not cls._initialized:
            cls.ensure_dirs()
        
        config_dir = cls.get_app_root() / "config"
        platform_env, scraper_env = cls.env_paths(scraper_name)
        
        loaded_vars = {}
        
        # Step 1: Load platform.env (MUST exist)
        if not platform_env.exists():
            raise FileNotFoundError(
                f"Required config file not found: {platform_env}\n"
                f"Please create platform.env in: {config_dir}\n"
                f"You can copy .env.example from the repo as a template."
            )
        
        logger.info(f"Loading platform config from: {platform_env}")
        try:
            load_dotenv(platform_env, override=False)
        except Exception as e:
            logger.warning(f"Failed to parse platform.env file (line 6 or later may have syntax error): {e}")
            logger.warning("Continuing with manual parsing...")
        
        # Track what was loaded
        try:
            with open(platform_env, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        try:
                            key = line.split('=', 1)[0].strip()
                            loaded_vars[key] = os.getenv(key, '')
                        except Exception as e:
                            logger.warning(f"Skipping invalid line {line_num} in {platform_env}: {line[:50]}")
        except Exception as e:
            logger.error(f"Failed to read platform.env file: {e}")
        
        # Step 2: Load scraper-specific .env (optional, overrides platform)
        if scraper_env.exists():
            logger.info(f"Loading scraper config from: {scraper_env}")
            try:
                load_dotenv(scraper_env, override=True)
            except Exception as e:
                logger.warning(f"Failed to parse {scraper_env.name} file (may have syntax error): {e}")
                logger.warning("Continuing with manual parsing...")
            
            # Track overrides
            try:
                with open(scraper_env, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            try:
                                key = line.split('=', 1)[0].strip()
                                loaded_vars[key] = os.getenv(key, '')
                            except Exception as e:
                                logger.warning(f"Skipping invalid line {line_num} in {scraper_env.name}: {line[:50]}")
            except Exception as e:
                logger.error(f"Failed to read {scraper_env.name} file: {e}")
        else:
            logger.debug(f"Scraper config not found (optional): {scraper_env}")
        
        # Step 3: Validate required keys if specified
        if required_keys:
            missing = []
            for key in required_keys:
                if not os.getenv(key):
                    missing.append(key)
            
            if missing:
                raise ValueError(
                    f"Missing required environment variables: {', '.join(missing)}\n"
                    f"Please configure them in: {scraper_env if scraper_env.exists() else platform_env}"
                )
        
        cls._loaded_env[scraper_name] = dict(loaded_vars)
        return loaded_vars

    @classmethod
    def get_env_value(cls, scraper_name: str, key: str, default: Optional[str] = None) -> str:
        """
        Get a configuration value loaded by ConfigManager.

        Precedence:
        1. OS environment (runtime overrides)
        2. Loaded env files (platform.env then scraper env)
        3. Default
        """
        if scraper_name not in cls._loaded_env:
            try:
                cls.load_env(scraper_name)
            except FileNotFoundError:
                cls.ensure_dirs()
        if key in os.environ:
            return os.environ.get(key, default if default is not None else "")
        return cls._loaded_env.get(scraper_name, {}).get(key, default if default is not None else "")
    
    @classmethod
    def validate(cls) -> Dict[str, Any]:
        """
        Validate configuration setup.
        
        Returns:
            Dict with validation results
        """
        app_root = cls.get_app_root()
        config_dir = app_root / "config"
        platform_env = config_dir / "platform.env"
        
        results = {
            "app_root": str(app_root),
            "app_root_exists": app_root.exists(),
            "config_dir": str(config_dir),
            "config_dir_exists": config_dir.exists(),
            "platform_env": str(platform_env),
            "platform_env_exists": platform_env.exists(),
            "errors": [],
            "warnings": []
        }
        
        if not platform_env.exists():
            results["errors"].append(f"platform.env not found: {platform_env}")
        
        # Check for env files in wrong locations (output/, backups/, etc.)
        wrong_locations = []
        for pattern in ["*.env", "platform.env"]:
            # Check output directories
            output_dir = app_root / "output"
            if output_dir.exists():
                for env_file in output_dir.rglob(pattern):
                    wrong_locations.append(str(env_file))
            
            # Check backups
            backups_dir = app_root / "backups"
            if backups_dir.exists():
                for env_file in backups_dir.rglob(pattern):
                    wrong_locations.append(str(env_file))
            
            # Check runs
            runs_dir = app_root / "runs"  # Runs in root folder
            if runs_dir.exists():
                for env_file in runs_dir.rglob(pattern):
                    wrong_locations.append(str(env_file))
        
        if wrong_locations:
            results["warnings"].append(
                f"Found {len(wrong_locations)} env file(s) in wrong locations:\n" +
                "\n".join(f"  - {loc}" for loc in wrong_locations[:10])
            )
        
        return results
    
    @classmethod
    def get_config_dir(cls) -> Path:
        """Get config directory path"""
        return cls.get_app_root() / "config"
    
    @classmethod
    def get_input_dir(cls, scraper_name: Optional[str] = None) -> Path:
        """
        Get input directory path.
        
        Args:
            scraper_name: Optional scraper name for scraper-specific input folder
        
        Returns:
            Path to input directory (scraper-specific if name provided)
        """
        base = cls.get_app_root() / "input"
        if scraper_name:
            return base / scraper_name
        return base
    
    @classmethod
    def get_output_dir(cls, scraper_name: Optional[str] = None) -> Path:
        """
        Get output directory path.
        
        Args:
            scraper_name: Optional scraper name for scraper-specific output folder
        
        Returns:
            Path to output directory (scraper-specific if name provided)
        """
        base = cls.get_app_root() / "output"
        if scraper_name:
            return base / scraper_name
        return base
    
    @classmethod
    def get_exports_dir(cls, scraper_name: Optional[str] = None) -> Path:
        """
        Get exports directory path.
        
        Args:
            scraper_name: Optional scraper name for scraper-specific exports folder
        
        Returns:
            Path to exports directory (scraper-specific if name provided)
        """
        base = cls.get_app_root() / "exports"  # Exports in root folder
        if scraper_name:
            return base / scraper_name
        return base
    
    @classmethod
    def get_runs_dir(cls) -> Path:
        """Get runs directory path - in root folder, not in output/"""
        return cls.get_app_root() / "runs"  # Runs in root folder
    
    @classmethod
    def get_backups_dir(cls, scraper_name: Optional[str] = None) -> Path:
        """
        Get backups directory path.
        
        Args:
            scraper_name: Optional scraper name for scraper-specific backups folder
        
        Returns:
            Path to backups directory (scraper-specific if name provided)
        """
        base = cls.get_app_root() / "backups"  # Backups in root folder, not in output/
        if scraper_name:
            return base / scraper_name
        return base
    
    @classmethod
    def get_sessions_dir(cls) -> Path:
        """Get sessions directory path"""
        return cls.get_app_root() / "sessions"
    
    @classmethod
    def get_logs_dir(cls) -> Path:
        """
        Get logs directory path.
        Note: Logs are actually stored in runs/{run_id}/logs/run.log when using WorkflowRunner.
        This method is kept for backward compatibility but the folder is not created by default.
        """
        return cls.get_app_root() / "logs"
    
    @classmethod
    def get_cache_dir(cls) -> Path:
        """Get cache directory path"""
        return cls.get_app_root() / "cache"
    
    @classmethod
    def acquire_lock(cls) -> bool:
        """
        Acquire single-instance lock to prevent multiple EXE sessions.
        
        Returns:
            True if lock acquired, False if another instance is running
        """
        global _app_lock_handle, _app_lock_file, _app_lock_released
        
        if _app_lock_released:
            # Lock was already released, cannot re-acquire
            return False
        
        if not cls._initialized:
            cls.ensure_dirs()
        
        lock_file = cls.get_sessions_dir() / "app.lock"
        _app_lock_file = lock_file
        
        # Helper function to check if process is running
        def is_process_running(pid: int) -> bool:
            """Check if a process with given PID is still running"""
            try:
                if sys.platform == "win32":
                    result = subprocess.run(
                        ['tasklist', '/FI', f'PID eq {pid}'],
                        capture_output=True,
                        text=True,
                        timeout=2,
                        creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0
                    )
                    # tasklist returns "INFO: No tasks are running" if PID not found
                    return "No tasks" not in result.stdout and str(pid) in result.stdout
                else:
                    # Unix: send signal 0 to check if process exists
                    os.kill(pid, 0)
                    return True
            except (OSError, ProcessLookupError, subprocess.TimeoutExpired, subprocess.SubprocessError):
                return False
        
        try:
            if sys.platform == "win32":
                # Windows: use exclusive file creation
                try:
                    _app_lock_handle = open(lock_file, 'x')  # 'x' mode fails if exists
                    _app_lock_handle.write(f"{os.getpid()}\n{time.time()}\n")
                    _app_lock_handle.flush()
                    
                    # Register cleanup on exit
                    atexit.register(cls.release_lock)
                    
                    return True
                except FileExistsError:
                    # Check if lock is stale (older than 5 minutes) or process is dead
                    if lock_file.exists():
                        try:
                            # First check if process is still running (faster check)
                            # Try to read lock file with retry for Windows file locking issues
                            lock_pid = None
                            max_read_retries = 3
                            for read_attempt in range(max_read_retries):
                                try:
                                    with open(lock_file, 'r') as f:
                                        lock_content = f.read().strip().split('\n')
                                        if lock_content and lock_content[0].isdigit():
                                            lock_pid = int(lock_content[0])
                                            break  # Successfully read PID
                                except (IOError, PermissionError, OSError) as e:
                                    # On Windows, file might be locked by another process
                                    if read_attempt < max_read_retries - 1:
                                        # Wait a bit and retry
                                        time.sleep(0.1 * (read_attempt + 1))
                                        continue
                                    else:
                                        # Last attempt failed, log and fall through to age check
                                        if "WinError 32" in str(e) or "being used by another process" in str(e):
                                            logger.debug(f"Lock file is in use, will check age instead")
                                        else:
                                            logger.warning(f"Could not read lock file: {e}, checking age instead")
                                        break
                            
                            # If we successfully read the PID, check if process is running
                            if lock_pid is not None:
                                # Check if process exists on Windows
                                if sys.platform == "win32":
                                    try:
                                        result = subprocess.run(
                                            ['tasklist', '/FI', f'PID eq {lock_pid}'],
                                            capture_output=True,
                                            text=True,
                                            timeout=2,
                                            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0
                                        )
                                        # tasklist returns "INFO: No tasks are running" if PID not found
                                        if "No tasks" in result.stdout or str(lock_pid) not in result.stdout:
                                            # Process is dead, remove stale lock
                                            logger.info(f"Removing stale lock file (process {lock_pid} not running)")
                                            lock_file.unlink()
                                            _app_lock_handle = open(lock_file, 'x')
                                            _app_lock_handle.write(f"{os.getpid()}\n{time.time()}\n")
                                            _app_lock_handle.flush()
                                            atexit.register(cls.release_lock)
                                            return True
                                        else:
                                            # Process is running, cannot acquire lock
                                            return False
                                    except (subprocess.TimeoutExpired, subprocess.SubprocessError, OSError) as e:
                                        logger.debug(f"Could not check process status: {e}, checking lock age instead")
                                        # Fall through to age check (don't return, continue to age check below)
                                else:
                                    # Unix: try to send signal 0 to check if process exists
                                    try:
                                        os.kill(lock_pid, 0)
                                        # Process exists, cannot acquire lock
                                        return False
                                    except (OSError, ProcessLookupError):
                                        # Process is dead, remove stale lock
                                        logger.info(f"Removing stale lock file (process {lock_pid} not running)")
                                        lock_file.unlink()
                                        _app_lock_handle = open(lock_file, 'x')
                                        _app_lock_handle.write(f"{os.getpid()}\n{time.time()}\n")
                                        _app_lock_handle.flush()
                                        atexit.register(cls.release_lock)
                                        return True
                            
                            # Fallback: Check lock file age (if we couldn't read PID or check process)
                            try:
                                mtime = lock_file.stat().st_mtime
                                age_seconds = time.time() - mtime
                                
                                if age_seconds > 300:  # 5 minutes
                                    # Stale lock, remove it
                                    logger.info(f"Removing stale lock file (age: {age_seconds:.0f} seconds)")
                                    # Try to remove with retry for Windows file locking
                                    max_unlink_retries = 3
                                    for unlink_attempt in range(max_unlink_retries):
                                        try:
                                            lock_file.unlink()
                                            break  # Success
                                        except (OSError, PermissionError) as e:
                                            if "WinError 32" in str(e) or "being used by another process" in str(e):
                                                if unlink_attempt < max_unlink_retries - 1:
                                                    time.sleep(0.2 * (unlink_attempt + 1))
                                                    continue
                                                else:
                                                    logger.debug(f"Lock file still in use, will retry on next startup")
                                                    return False
                                            else:
                                                raise  # Re-raise other errors
                                    
                                    # Try to create new lock
                                    try:
                                        _app_lock_handle = open(lock_file, 'x')
                                        _app_lock_handle.write(f"{os.getpid()}\n{time.time()}\n")
                                        _app_lock_handle.flush()
                                        atexit.register(cls.release_lock)
                                        return True
                                    except FileExistsError:
                                        # Lock was recreated by another process, cannot acquire
                                        return False
                                else:
                                    # Lock is recent and process check failed, assume it's active
                                    return False
                            except (OSError, PermissionError) as e:
                                logger.debug(f"Could not check lock file age: {e}")
                                return False
                                
                        except (OSError, PermissionError) as e:
                            logger.warning(f"Could not check/remove lock file: {e}")
                            return False
                    
                    return False
            else:
                # Unix-like: use fcntl
                try:
                    import fcntl
                    _app_lock_handle = open(lock_file, 'w')
                    fcntl.flock(_app_lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    _app_lock_handle.write(f"{os.getpid()}\n{time.time()}\n")
                    _app_lock_handle.flush()
                    atexit.register(cls.release_lock)
                    return True
                except (IOError, ImportError):
                    if _app_lock_handle:
                        try:
                            _app_lock_handle.close()
                        except:
                            pass
                        _app_lock_handle = None
                    return False
        except Exception as e:
            logger.error(f"Failed to acquire lock: {e}")
            if _app_lock_handle:
                try:
                    _app_lock_handle.close()
                except:
                    pass
                _app_lock_handle = None
            return False
    
    @classmethod
    def release_lock(cls) -> None:
        """Release single-instance lock"""
        global _app_lock_handle, _app_lock_file, _app_lock_released
        
        if _app_lock_released:
            return
        
        _app_lock_released = True
        
        # Close handle
        if _app_lock_handle:
            try:
                if sys.platform != "win32":
                    try:
                        import fcntl
                        fcntl.flock(_app_lock_handle.fileno(), fcntl.LOCK_UN)
                    except (ImportError, IOError):
                        pass
                _app_lock_handle.close()
                if sys.platform == "win32":
                    time.sleep(0.5)  # Give Windows time to release file handle
            except Exception:
                pass
            finally:
                _app_lock_handle = None
        
        # Delete lock file
        if _app_lock_file and _app_lock_file.exists():
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    _app_lock_file.unlink()
                    return
                except (PermissionError, FileNotFoundError):
                    if attempt < max_retries - 1:
                        time.sleep(0.2 * (attempt + 1))
                    continue


# Convenience functions for backward compatibility
def get_app_root() -> Path:
    """Get application root directory"""
    return ConfigManager.get_app_root()


def ensure_dirs() -> None:
    """Ensure all required directories exist"""
    ConfigManager.ensure_dirs()


def load_env(scraper_name: str, required_keys: Optional[list] = None) -> Dict[str, str]:
    """Load environment variables for scraper"""
    return ConfigManager.load_env(scraper_name, required_keys)


def acquire_lock() -> bool:
    """Acquire single-instance lock"""
    return ConfigManager.acquire_lock()


def release_lock() -> None:
    """Release single-instance lock"""
    ConfigManager.release_lock()
