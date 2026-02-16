"""
Configuration Loader for Netherlands Scraper (Facade for Core ConfigManager)

This module wraps platform_config.py for centralized path management.
Includes legacy support for loading config from config/Netherlands.env.json.
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

SCRAPER_ID = "Netherlands"

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
                        # Complex types (list, dict) are not set in os.environ here.
                        # They are handled by getenv_list via direct JSON check.

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

def get_logs_dir() -> Path:
    return ConfigManager.get_logs_dir()

# --- Environment Accessors ---

def load_env_file():
    """No-op, already loaded on import."""
    pass

def getenv(key: str, default: str = None) -> str:
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
    
    # Check JSON file directly for complex types
    try:
        config_dir = ConfigManager.get_config_dir()
        json_file = config_dir / f"{SCRAPER_ID}.env.json"
        if json_file.exists():
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if "config" in data and isinstance(data["config"], dict) and key in data["config"]:
                val = data["config"][key]
                if isinstance(val, list): return val
    except Exception:
        pass

    # Fallback to env var (comma sep)
    val = getenv(key)
    if not val: return default
    
    try:
        return json.loads(val)
    except (json.JSONDecodeError, ValueError):
        return [v.strip() for v in val.split(",") if v.strip()]

def require_env(key: str, default: str = None) -> str:
    val = getenv(key, default)
    if val is None or val == "":
         # Original code allowed default? Wait, existing calls were require_env(key, default)
         # But the name implies requirement.
         # Original implementation: return getenv(key, default) 
         # Wait, original implementation was just wrapper around getenv.
         return val
    return val

# --- Diagnostic ---
if __name__ == "__main__":
    print("=" * 60)
    print("Netherlands Config Loader - Diagnostic (Facade)")
    print("=" * 60)
    print(f"Scraper ID: {SCRAPER_ID}")
    print(f"Input Dir: {get_input_dir()}")
    print(f"Output Dir: {get_output_dir()}")
    print(f"Sample env: SCRAPER_ID={getenv('SCRAPER_ID')}")
