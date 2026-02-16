"""
Configuration Loader for Russia Scraper (Facade for Core ConfigManager)

This module wraps platform_config.py for centralized path management.
Includes legacy support for loading config from config/Russia.env.json.
"""
import os
import sys
import json
from pathlib import Path
from typing import Optional, List

# Ensure core is in path
_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.config.config_manager import ConfigManager, get_env_bool as _get_bool, get_env_int as _get_int, get_env_float as _get_float

SCRAPER_ID = "Russia"

def _load_legacy_json_config():
    """Load legacy JSON config into os.environ so ConfigManager picks it up."""
    try:
        config_dir = ConfigManager.get_config_dir()
        json_file = config_dir / f"{SCRAPER_ID}.env.json"
        
        if json_file.exists():
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Load "config" section
            if "config" in data and isinstance(data["config"], dict):
                for k, v in data["config"].items():
                    if k.startswith("_"): continue
                    # Set in os.environ if not already set (keep existing env vars as overrides)
                    if k not in os.environ:
                        if isinstance(v, (bool, int, float)):
                            os.environ[k] = str(v).lower() if isinstance(v, bool) else str(v)
                        elif isinstance(v, str):
                            os.environ[k] = v
                        elif isinstance(v, list):
                            # Dump list as JSON string or comma-sep?
                            # getenv_list expects JSON or comma-sep.
                            os.environ[k] = json.dumps(v)
                            
            # Load "scraper" section id
            if "scraper" in data and "id" in data["scraper"]:
                if "SCRAPER_ID" not in os.environ:
                    os.environ["SCRAPER_ID"] = data["scraper"]["id"]
    except Exception as e:
        print(f"Warning: Error loading legacy JSON config: {e}")

# Initialize
ConfigManager.ensure_dirs()
ConfigManager.load_env(SCRAPER_ID)
_load_legacy_json_config()

# --- Path Accessors ---

def get_repo_root() -> Path:
    return ConfigManager.get_app_root()

def get_base_dir() -> Path:
    return ConfigManager.get_app_root()

def get_central_output_dir() -> Path:
    return ConfigManager.get_exports_dir(SCRAPER_ID)

def get_input_dir(subpath: str = None) -> Path:
    base = ConfigManager.get_input_dir(SCRAPER_ID)
    if subpath:
        return base / subpath
    return base

def get_output_dir(subpath: str = None) -> Path:
    base = ConfigManager.get_output_dir(SCRAPER_ID)
    if subpath:
        return base / subpath
    return base

def get_backup_dir() -> Path:
    return ConfigManager.get_backups_dir(SCRAPER_ID)

# --- Environment Accessors ---

def load_env_file():
    """No-op, already loaded on import."""
    pass

def getenv(key: str, default: str = None) -> str:
    # Use ConfigManager which checks os.environ first (populated by JSON loader if needed)
    val = ConfigManager.get_env_value(SCRAPER_ID, key, default)
    return val if val is not None else ""

def getenv_int(key: str, default: int = 0) -> int:
    return _get_int(SCRAPER_ID, key, default)

def getenv_float(key: str, default: float = 0.0) -> float:
    return _get_float(SCRAPER_ID, key, default)

def getenv_bool(key: str, default: bool = False) -> bool:
    return _get_bool(SCRAPER_ID, key, default)

def getenv_list(key: str, default: list = None) -> list:
    if default is None: default = []
    val = getenv(key)
    if not val: return default
    
    # Try JSON
    try:
        return json.loads(val)
    except (json.JSONDecodeError, ValueError):
        # Comma sep
        return [v.strip() for v in val.split(",") if v.strip()]

def require_env(key: str) -> str:
    val = getenv(key)
    if not val:
        raise ValueError(f"Required environment variable '{key}' is not set.")
    return val

# --- Diagnostic ---
if __name__ == "__main__":
    print("=" * 60)
    print("Russia Config Loader - Diagnostic (Facade)")
    print("=" * 60)
    print(f"Scraper ID: {SCRAPER_ID}")
    print(f"Input Dir: {get_input_dir()}")
    print(f"Output Dir: {get_output_dir()}")
    print(f"Sample env: SCRIPT_01_HEADLESS={getenv('SCRIPT_01_HEADLESS')}")
