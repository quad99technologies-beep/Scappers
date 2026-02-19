"""
Configuration Loader for Belarus Scraper (Facade for Core ConfigManager)

This module wraps platform_config.py for centralized path management.
Includes legacy support for loading config from config/Belarus.env.json.
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

from core.config.scraper_config_factory import create_config

SCRAPER_ID = "Belarus"
config = create_config(SCRAPER_ID)

# --- Path Accessors ---
def get_repo_root() -> Path: return config.get_repo_root()
def get_base_dir() -> Path: return config.get_base_dir()
def get_central_output_dir() -> Path: return config.get_central_output_dir()
def get_input_dir(subpath=None) -> Path: return config.get_input_dir(subpath)
def get_output_dir(subpath=None) -> Path: return config.get_output_dir(subpath)
def get_backup_dir() -> Path: return config.get_backup_dir()

# --- Environment Accessors ---
def load_env_file(): pass
def getenv(key: str, default: str = "") -> str: return config.getenv(key, default)
def getenv_int(key: str, default: int = 0) -> int: return config.getenv_int(key, default)
def getenv_float(key: str, default: float = 0.0) -> float: return config.getenv_float(key, default)
def getenv_bool(key: str, default: bool = False) -> bool: return config.getenv_bool(key, default)
def getenv_list(key: str, default: list = None) -> list: return config.getenv_list(key, default)

def require_env(key: str) -> str:
    val = getenv(key)
    if not val:
        raise ValueError(f"Required environment variable '{key}' is not set.")
    return val

# --- Diagnostic ---
if __name__ == "__main__":
    print("=" * 60)
    print("Belarus Config Loader - Diagnostic (Facade)")
    print("=" * 60)
    print(f"Scraper ID: {SCRAPER_ID}")
    print(f"Input Dir: {get_input_dir()}")
    print(f"Output Dir: {get_output_dir()}")
    print(f"Sample env: SCRIPT_01_HEADLESS={getenv('SCRIPT_01_HEADLESS')}")
