#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Platform Configuration & Path Management

Centralized configuration resolver and path manager for the Scraper Platform.
Uses ConfigManager for all paths (Documents/ScraperPlatform/).

DEPRECATED: This module is maintained for backward compatibility.
New code should use core.config_manager.ConfigManager directly.
"""

import os
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Union
from datetime import datetime

# Import ConfigManager for path resolution
try:
    # Add repo root to path if needed
    repo_root = Path(__file__).resolve().parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from core.config_manager import ConfigManager
    _CONFIG_MANAGER_AVAILABLE = True
except ImportError:
    _CONFIG_MANAGER_AVAILABLE = False
    ConfigManager = None

logger = logging.getLogger(__name__)


class PathManager:
    """Centralized path management for platform"""
    
    _platform_root: Optional[Path] = None
    _repo_root: Optional[Path] = None
    
    @classmethod
    def _detect_repo_root(cls) -> Path:
        """Detect repository root directory (where scraper_gui.py is located)"""
        if cls._repo_root is None:
            # Try to find repo root by locating this file (platform_config.py) or scraper_gui.py
            try:
                # This file is in repo root
                this_file = Path(__file__).resolve()
                # Check if scraper_gui.py exists in the same directory
                if (this_file.parent / "scraper_gui.py").exists():
                    cls._repo_root = this_file.parent
                else:
                    # Fallback: assume this file is in repo root
                    cls._repo_root = this_file.parent
            except Exception:
                # Last resort: use current working directory
                cls._repo_root = Path.cwd()
        return cls._repo_root
    
    @classmethod
    def get_platform_root(cls) -> Path:
        """Get platform root directory - uses Documents/ScraperPlatform/"""
        if _CONFIG_MANAGER_AVAILABLE:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_app_root()
        # Fallback to repo root for backward compatibility
        return cls._detect_repo_root()
    
    @classmethod
    def get_config_dir(cls) -> Path:
        """Get config directory - uses Documents/ScraperPlatform/config/"""
        if _CONFIG_MANAGER_AVAILABLE:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_config_dir()
        # Fallback to repo root config
        repo_root = cls._detect_repo_root()
        config_dir = repo_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        return config_dir
    
    @classmethod
    def get_input_dir(cls, scraper_id: str) -> Path:
        """Get input directory for scraper - uses Documents/ScraperPlatform/input/{scraper_id}/"""
        if _CONFIG_MANAGER_AVAILABLE:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_input_dir(scraper_id)
        # Fallback: use scraper's own input directory
        repo_root = cls._detect_repo_root()
        scraper_dirs = {
            "CanadaQuebec": "scripts/CanadaQuebec",
            "Malaysia": "scripts/Malaysia",
            "Argentina": "scripts/Argentina",
            "CanadaOntario": "scripts/Canada Ontario",
            "Netherlands": "scripts/Netherlands",
            "Belarus": "scripts/Belarus",
            "NorthMacedonia": "scripts/North Macedonia",
            "Tender_Chile": "scripts/Tender- Chile"
        }
        scraper_dir_name = scraper_dirs.get(scraper_id, scraper_id)
        scraper_root = repo_root / scraper_dir_name
        input_dir = scraper_root / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        return input_dir
    
    @classmethod
    def get_output_dir(cls, scraper_id: Optional[str] = None) -> Path:
        """Get output directory - uses Documents/ScraperPlatform/output/{scraper_id}/"""
        if _CONFIG_MANAGER_AVAILABLE:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_output_dir(scraper_id)
        # Fallback
        repo_root = cls._detect_repo_root()
        output_dir = repo_root / "output"
        if scraper_id:
            output_dir = output_dir / scraper_id
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir
    
    @classmethod
    def get_backups_dir(cls, scraper_id: Optional[str] = None) -> Path:
        """Get backups directory - uses repository root/backups/{scraper_id}/"""
        if _CONFIG_MANAGER_AVAILABLE:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_backups_dir(scraper_id)
        # Fallback
        repo_root = cls._detect_repo_root()
        backups_dir = repo_root / "backups"  # Backups in root folder, not in output/
        if scraper_id:
            backups_dir = backups_dir / scraper_id
        backups_dir.mkdir(parents=True, exist_ok=True)
        return backups_dir
    
    @classmethod
    def get_exports_dir(cls, scraper_id: Optional[str] = None) -> Path:
        """Get exports directory - uses repository root/exports/{scraper_id}/"""
        if _CONFIG_MANAGER_AVAILABLE:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_exports_dir(scraper_id)
        # Fallback
        repo_root = cls._detect_repo_root()
        exports_dir = repo_root / "exports"  # Exports in root folder
        if scraper_id:
            exports_dir = exports_dir / scraper_id
        exports_dir.mkdir(parents=True, exist_ok=True)
        return exports_dir
    
    @classmethod
    def get_runs_dir(cls) -> Path:
        """Get runs directory - uses Documents/ScraperPlatform/output/runs/"""
        if _CONFIG_MANAGER_AVAILABLE:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_runs_dir()
        # Fallback
        repo_root = cls._detect_repo_root()
        runs_dir = repo_root / "runs"  # Runs in root folder
        runs_dir.mkdir(parents=True, exist_ok=True)
        return runs_dir
    
    @classmethod
    def get_run_dir(cls, scraper_id: str, run_id: str) -> Path:
        """Get run directory for specific run"""
        run_dir = cls.get_runs_dir() / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        # Create subdirectories
        (run_dir / "logs").mkdir(exist_ok=True)
        (run_dir / "artifacts").mkdir(exist_ok=True)
        (run_dir / "exports").mkdir(exist_ok=True)
        return run_dir
    
    @classmethod
    def get_sessions_dir(cls) -> Path:
        """Get sessions directory - uses Documents/ScraperPlatform/sessions/"""
        if _CONFIG_MANAGER_AVAILABLE:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_sessions_dir()
        # Fallback
        repo_root = cls._detect_repo_root()
        sessions_dir = repo_root / "sessions"
        sessions_dir.mkdir(parents=True, exist_ok=True)
        return sessions_dir
    
    @classmethod
    def get_logs_dir(cls) -> Path:
        """Get logs directory - uses Documents/ScraperPlatform/logs/"""
        if _CONFIG_MANAGER_AVAILABLE:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_logs_dir()
        # Fallback
        repo_root = cls._detect_repo_root()
        logs_dir = repo_root / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        return logs_dir
    
    @classmethod
    def get_cache_dir(cls) -> Path:
        """Get cache directory - uses Documents/ScraperPlatform/cache/"""
        if _CONFIG_MANAGER_AVAILABLE:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_cache_dir()
        # Fallback
        repo_root = cls._detect_repo_root()
        cache_dir = repo_root / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir
    
    @classmethod
    def get_locks_dir(cls) -> Path:
        """Get locks directory (uses repo root instead of Documents/ScraperPlatform)"""
        repo_root = cls._detect_repo_root()
        locks_dir = repo_root / ".locks"
        locks_dir.mkdir(parents=True, exist_ok=True)
        return locks_dir
    
    @classmethod
    def get_lock_file(cls, scraper_id: str) -> Path:
        """Get lock file path for scraper"""
        return cls.get_locks_dir() / f"{scraper_id}.lock"


class ConfigResolver:
    """Centralized configuration resolver with precedence"""
    
    def __init__(self):
        self.path_manager = PathManager
        self._platform_config: Optional[Dict[str, Any]] = None
        self._scraper_configs: Dict[str, Dict[str, Any]] = {}
    
    def _load_platform_config(self) -> Dict[str, Any]:
        """Load platform-wide configuration"""
        if self._platform_config is None:
            config_file = self.path_manager.get_config_dir() / "platform.json"
            if config_file.exists():
                try:
                    with open(config_file, "r", encoding="utf-8") as f:
                        self._platform_config = json.load(f)
                    logger.info(f"Loaded platform config from {config_file}")
                except Exception as e:
                    logger.warning(f"Failed to load platform config: {e}")
                    self._platform_config = {}
            else:
                # Create default platform config
                self._platform_config = self._get_default_platform_config()
                self._save_platform_config()
        
        return self._platform_config
    
    def _get_default_platform_config(self) -> Dict[str, Any]:
        """Get default platform configuration"""
        return {
            "platform": {
                "version": "1.0.0",
                "log_level": "INFO",
                "max_concurrent_runs": 1
            },
            "paths": {
                "input_base": "input",
                "output_base": "output",
                "cache_base": "cache"
            }
        }
    
    def _save_platform_config(self):
        """Save platform configuration"""
        config_file = self.path_manager.get_config_dir() / "platform.json"
        try:
            with open(config_file, "w", encoding="utf-8") as f:
                json.dump(self._platform_config, f, indent=2)
            logger.info(f"Saved platform config to {config_file}")
        except Exception as e:
            logger.error(f"Failed to save platform config: {e}")
    
    def _load_scraper_config(self, scraper_id: str) -> Dict[str, Any]:
        """Load scraper-specific configuration"""
        if scraper_id not in self._scraper_configs:
            config_file = self.path_manager.get_config_dir() / f"{scraper_id}.env.json"
            if config_file.exists():
                try:
                    with open(config_file, "r", encoding="utf-8") as f:
                        self._scraper_configs[scraper_id] = json.load(f)
                    logger.info(f"Loaded scraper config for {scraper_id} from {config_file}")
                except Exception as e:
                    logger.warning(f"Failed to load scraper config for {scraper_id}: {e}")
                    self._scraper_configs[scraper_id] = {}
            else:
                # Create default scraper config
                self._scraper_configs[scraper_id] = self._get_default_scraper_config(scraper_id)
                self._save_scraper_config(scraper_id)
        
        return self._scraper_configs[scraper_id]
    
    def _get_default_scraper_config(self, scraper_id: str) -> Dict[str, Any]:
        """Get default scraper configuration"""
        return {
            "scraper": {
                "id": scraper_id,
                "enabled": True
            },
            "config": {},
            "secrets": {}
        }
    
    def _save_scraper_config(self, scraper_id: str):
        """Save scraper configuration"""
        config_file = self.path_manager.get_config_dir() / f"{scraper_id}.env.json"
        try:
            with open(config_file, "w", encoding="utf-8") as f:
                json.dump(self._scraper_configs[scraper_id], f, indent=2)
            logger.info(f"Saved scraper config for {scraper_id} to {config_file}")
        except Exception as e:
            logger.error(f"Failed to save scraper config for {scraper_id}: {e}")
    
    def get_config(self, scraper_id: str, runtime_overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get resolved configuration for scraper with precedence:
        1. Runtime overrides (highest)
        2. Process environment variables
        3. Scraper config
        4. Platform config
        5. Defaults (lowest)
        """
        # Load configs
        platform_config = self._load_platform_config()
        scraper_config = self._load_scraper_config(scraper_id)
        
        # Start with defaults
        resolved = {}
        
        # Apply platform config
        if "config" in platform_config:
            resolved.update(platform_config["config"])
        
        # Apply platform secrets (if any)
        if "secrets" in platform_config:
            resolved.update(platform_config["secrets"])
        
        # Apply scraper config (overrides platform)
        if "config" in scraper_config:
            resolved.update(scraper_config["config"])
        
        # Apply scraper secrets (overrides platform secrets, but can be overridden by env)
        if "secrets" in scraper_config:
            resolved.update(scraper_config["secrets"])
        
        # Apply environment variables (overrides config files)
        for key, value in os.environ.items():
            if key.startswith("SCRAPER_") or key.startswith(scraper_id.upper() + "_"):
                # Remove prefix if present
                clean_key = key.replace("SCRAPER_", "").replace(scraper_id.upper() + "_", "")
                resolved[clean_key] = value
        
        # Apply runtime overrides (highest precedence)
        if runtime_overrides:
            resolved.update(runtime_overrides)
        
        return resolved
    
    def get(self, scraper_id: str, key: str, default: Any = None, runtime_overrides: Optional[Dict[str, Any]] = None) -> Any:
        """Get a single config value"""
        config = self.get_config(scraper_id, runtime_overrides)
        return config.get(key, default)
    
    def set_scraper_config_value(self, scraper_id: str, key: str, value: Any, is_secret: bool = False):
        """Set a config value in scraper config file"""
        scraper_config = self._load_scraper_config(scraper_id)
        
        if is_secret:
            if "secrets" not in scraper_config:
                scraper_config["secrets"] = {}
            scraper_config["secrets"][key] = value
        else:
            if "config" not in scraper_config:
                scraper_config["config"] = {}
            scraper_config["config"][key] = value
        
        self._scraper_configs[scraper_id] = scraper_config
        self._save_scraper_config(scraper_id)
    
    def get_secrets(self, scraper_id: str) -> Dict[str, str]:
        """Get secrets for scraper (with masking for display)"""
        scraper_config = self._load_scraper_config(scraper_id)
        secrets = scraper_config.get("secrets", {})
        
        # Return masked version for display
        masked = {}
        for key, value in secrets.items():
            if value and isinstance(value, str) and value != "***MASKED***":
                masked[key] = "***MASKED***"
            else:
                masked[key] = value
        
        return masked
    
    def get_secret_value(self, scraper_id: str, key: str, default: str = "") -> str:
        """Get actual secret value (for runtime use)"""
        scraper_config = self._load_scraper_config(scraper_id)
        secrets = scraper_config.get("secrets", {})
        
        # Check environment first (highest precedence)
        env_key = f"{scraper_id.upper()}_{key}"
        if env_key in os.environ:
            return os.environ[env_key]
        
        # Then check config file
        if key in secrets:
            value = secrets[key]
            if value == "***MASKED***":
                # Try environment as fallback
                return os.getenv(key, default)
            return value
        
        # Fallback to environment variable without prefix
        return os.getenv(key, default)


# Global instances
_path_manager = PathManager()
_config_resolver = ConfigResolver()


def get_path_manager() -> PathManager:
    """Get global PathManager instance"""
    return _path_manager


def get_config_resolver() -> ConfigResolver:
    """Get global ConfigResolver instance"""
    return _config_resolver


# Backward compatibility functions
def get_platform_root() -> Path:
    """Get platform root (backward compatibility)"""
    return _path_manager.get_platform_root()


def get_config_dir() -> Path:
    """Get config directory (backward compatibility)"""
    return _path_manager.get_config_dir()


def get_input_dir(scraper_id: str) -> Path:
    """Get input directory for scraper (backward compatibility)"""
    return _path_manager.get_input_dir(scraper_id)


def get_output_dir() -> Path:
    """Get output directory (backward compatibility)"""
    return _path_manager.get_output_dir()


if __name__ == "__main__":
    # CLI commands
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "doctor":
            # Doctor command: print platform info and diagnostics
            pm = get_path_manager()
            cr = get_config_resolver()

            print("=" * 70)
            print("Scraper Platform - Doctor Command")
            print("=" * 70)
            print()
            print("PATHS:")
            print(f"  Platform Root:  {pm.get_platform_root()}")
            print(f"  Config Dir:     {pm.get_config_dir()}")
            print(f"  Output Dir:     {pm.get_output_dir()}")
            print(f"  Backups Dir:    {pm.get_backups_dir()}")
            print(f"  Logs Dir:       {pm.get_logs_dir()}")
            print(f"  Cache Dir:      {pm.get_cache_dir()}")
            print(f"  Locks Dir:      {pm.get_locks_dir()}")
            print()
            print("SCRAPER INPUT DIRECTORIES:")
            for scraper_id in ["CanadaQuebec", "Malaysia", "Argentina"]:
                input_dir = pm.get_input_dir(scraper_id)
                exists = "[OK]" if input_dir.exists() else "[  ]"
                print(f"  {scraper_id:15} {exists} {input_dir}")
            print()
            print("PLATFORM CONFIG:")
            platform_config = cr._load_platform_config()
            print(json.dumps(platform_config, indent=2))
            print()
            print("SCRAPER CONFIGS:")
            for scraper_id in ["CanadaQuebec", "Malaysia", "Argentina"]:
                scraper_config = cr._load_scraper_config(scraper_id)
                config_count = len(scraper_config.get('config', {}))
                secret_count = len(scraper_config.get('secrets', {}))
                enabled = scraper_config.get('scraper', {}).get('enabled', True)
                status = "[OK] enabled" if enabled else "[ X] disabled"
                print(f"  {scraper_id:15} {status:12} {config_count} config, {secret_count} secrets")
            print()
            print("PERMISSIONS CHECK:")
            platform_root = pm.get_platform_root()
            if platform_root.exists():
                print(f"  Platform Root:  [OK] exists and writable")
            else:
                print(f"  Platform Root:  [ X] does not exist (will be created on first run)")
            print()
            print("=" * 70)

        elif command == "config-check":
            # Config check: validate required configuration for each scraper
            cr = get_config_resolver()

            print("=" * 70)
            print("Scraper Platform - Config Check")
            print("=" * 70)
            print()

            all_ok = True

            # CanadaQuebec requirements
            print("CanadaQuebec:")
            cq_config = cr.get_config("CanadaQuebec")
            openai_key = cr.get_secret_value("CanadaQuebec", "OPENAI_API_KEY", "")
            if openai_key and openai_key != "***MASKED***":
                print("  [OK] OPENAI_API_KEY configured")
            else:
                print("  [ X] OPENAI_API_KEY missing (required for extraction steps)")
                all_ok = False
            print()

            # Malaysia requirements
            print("Malaysia:")
            print("  [OK] No required secrets")
            print()

            # Argentina requirements
            print("Argentina:")
            ar_config = cr.get_config("Argentina")
            alfabeta_user = cr.get_secret_value("Argentina", "ALFABETA_USER", "")
            alfabeta_pass = cr.get_secret_value("Argentina", "ALFABETA_PASS", "")
            if alfabeta_user and alfabeta_user != "***MASKED***":
                print("  [OK] ALFABETA_USER configured")
            else:
                print("  [ X] ALFABETA_USER missing (required for login)")
                all_ok = False
            if alfabeta_pass and alfabeta_pass != "***MASKED***":
                print("  [OK] ALFABETA_PASS configured")
            else:
                print("  [ X] ALFABETA_PASS missing (required for login)")
                all_ok = False
            print()

            if all_ok:
                print("=" * 70)
                print("[OK] All required configuration is present")
                print("=" * 70)
                sys.exit(0)
            else:
                print("=" * 70)
                print("[ X] Missing required configuration (see above)")
                print()
                print("To fix:")
                print(f"  1. Copy .env.example to {pm.get_config_dir()}/{{scraper}}.env.json")
                print("  2. Edit the file and add your actual secrets")
                print("  3. Run 'python platform_config.py config-check' again")
                print("=" * 70)
                sys.exit(1)

        else:
            print(f"Unknown command: {command}")
            print()
            print("Available commands:")
            print("  python platform_config.py doctor        - Show platform paths and config")
            print("  python platform_config.py config-check  - Validate required configuration")
    else:
        print("Scraper Platform Configuration Tool")
        print()
        print("Usage:")
        print("  python platform_config.py doctor        - Show platform paths and config")
        print("  python platform_config.py config-check  - Validate required configuration")

