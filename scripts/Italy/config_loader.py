
"""
Configuration Loader for Italy Scraper (Platform Config Integration)

This module wraps platform_config.py for centralized path management.
All configuration is loaded from config/Italy.env.json.
"""
import os
import sys
from pathlib import Path

# Add repo root to path for platform_config import
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))


def get_repo_root() -> Path:
    """Get repository root directory (parent of scraper directories)."""
    return _repo_root


def get_central_output_dir() -> Path:
    """Get central exports directory for final reports - uses Documents/ScraperPlatform/output/exports/Italy/"""
    if _PLATFORM_CONFIG_AVAILABLE:
        exports_dir = ConfigManager.get_exports_dir(SCRAPER_ID)
        exports_dir.mkdir(parents=True, exist_ok=True)
        return exports_dir
    else:
        repo_root = get_repo_root()
        central_output = repo_root / "output"
        central_output.mkdir(parents=True, exist_ok=True)
        return central_output

try:
    from core.config.config_manager import ConfigManager
    _PLATFORM_CONFIG_AVAILABLE = True
except ImportError:
    _PLATFORM_CONFIG_AVAILABLE = False
    PathManager = None
    ConfigResolver = None

# Scraper ID for this scraper
SCRAPER_ID = "Italy"


def load_env_file():
    """
    Load environment variables from platform.env and Italy.env.
    Must be called before using getenv() in scripts.
    """
    try:
        repo_root = get_repo_root()
        import sys
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        
        from core.config.config_manager import ConfigManager
        ConfigManager.ensure_dirs()
        ConfigManager.load_env(SCRAPER_ID)
    except (ImportError, FileNotFoundError, ValueError):
        # Fallback
        pass


def getenv(key: str, default: str = None) -> str:
    if _PLATFORM_CONFIG_AVAILABLE:
        try:
            value = ConfigManager.get_config_value(SCRAPER_ID, key, default if default is not None else "")
            return value if value is not None else (default if default is not None else "")
        except Exception:
            return os.getenv(key, default)
    return os.getenv(key, default)


def get_base_dir() -> Path:
    if _PLATFORM_CONFIG_AVAILABLE:
        return ConfigManager.get_app_root()
    else:
        return Path(__file__).resolve().parents[1]


def get_output_dir(subpath: str = None) -> Path:
    output_dir_str = getenv("OUTPUT_DIR", "")
    if output_dir_str and Path(output_dir_str).is_absolute():
        base = Path(output_dir_str)
    else:
        if _PLATFORM_CONFIG_AVAILABLE:
            base = ConfigManager.get_output_dir(SCRAPER_ID)
            base.mkdir(parents=True, exist_ok=True)
        else:
            repo_root = get_repo_root()
            base = repo_root / "output"
            base.mkdir(parents=True, exist_ok=True)

    if subpath:
        result = base / subpath
        result.mkdir(parents=True, exist_ok=True)
        return result
    return base
