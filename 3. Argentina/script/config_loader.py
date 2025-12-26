"""
Configuration Loader for Argentina Scraper (New - Platform Config Integration)

This module provides centralized config and path management for Argentina scraper.
Integrates with platform_config.py while maintaining backward compatibility.

Precedence (highest to lowest):
1. Runtime overrides
2. Environment variables (OS-level)
3. Platform config (Documents/ScraperPlatform/config/)
4. Legacy .env files (backward compatibility)
5. Hardcoded defaults
"""
import os
import sys
from pathlib import Path

# Add repo root to path for platform_config import
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Try to import platform_config (preferred)
try:
    from platform_config import PathManager, ConfigResolver, get_path_manager, get_config_resolver
    _PLATFORM_CONFIG_AVAILABLE = True
except ImportError:
    _PLATFORM_CONFIG_AVAILABLE = False
    PathManager = None
    ConfigResolver = None

# Scraper ID for this scraper
SCRAPER_ID = "Argentina"

# Try to load dotenv if available (legacy)
try:
    from dotenv import load_dotenv
    # Load .env from scraper root
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
except ImportError:
    pass


def getenv(key: str, default: str = None) -> str:
    """
    Get environment variable with fallback to default.
    Integrates with platform_config if available.

    Args:
        key: Environment variable name
        default: Default value if not found

    Returns:
        Environment variable value or default
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        cr = get_config_resolver()
        return cr.get(SCRAPER_ID, key, default if default is not None else "")
    return os.getenv(key, default)


def getenv_int(key: str, default: int = 0) -> int:
    """Get environment variable as integer."""
    try:
        val = getenv(key, str(default))
        return int(val)
    except (ValueError, TypeError):
        return default


def getenv_bool(key: str, default: bool = False) -> bool:
    """Get environment variable as boolean."""
    val = getenv(key, "").strip().lower()
    if val in ("1", "true", "yes", "on"):
        return True
    elif val in ("0", "false", "no", "off", ""):
        return False
    return default


def get_base_dir() -> Path:
    """
    Get base directory for Argentina scraper.

    With platform_config: Returns platform root
    Legacy mode: Returns parent of script folder
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_platform_root()
    else:
        # Legacy: relative to script location
        return Path(__file__).resolve().parents[1]


def get_input_dir(subpath: str = None) -> Path:
    """
    Get input directory.

    Args:
        subpath: Optional subdirectory under input/
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        base = pm.get_input_dir(SCRAPER_ID)
    else:
        base = get_base_dir() / "Input"  # Note: Argentina uses capital I

    if subpath:
        return base / subpath
    return base


def get_output_dir(subpath: str = None) -> Path:
    """
    Get output directory.

    Args:
        subpath: Optional subdirectory under output/
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        base = pm.get_output_dir()
    else:
        base = get_base_dir() / "Output"  # Note: Argentina uses capital O

    if subpath:
        return base / subpath
    return base


def get_backup_dir() -> Path:
    """Get backup directory."""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_backups_dir()
    else:
        return get_base_dir() / "backups"


def get_logs_dir() -> Path:
    """Get logs directory."""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_logs_dir()
    else:
        return get_base_dir() / "logs"


# Configuration values (commonly used in Argentina scripts)
ALFABETA_USER = getenv("ALFABETA_USER", "")
ALFABETA_PASS = getenv("ALFABETA_PASS", "")
HEADLESS = getenv_bool("HEADLESS", False)
MAX_ROWS = getenv_int("MAX_ROWS", 0)

# Proxy configuration
PROXY_1 = getenv("PROXY_1", "")
PROXY_2 = getenv("PROXY_2", "")
PROXY_3 = getenv("PROXY_3", "")

# Diagnostic function
if __name__ == "__main__":
    print("=" * 60)
    print("Argentina Config Loader - Diagnostic")
    print("=" * 60)
    print(f"Platform Config Available: {_PLATFORM_CONFIG_AVAILABLE}")
    print(f"Scraper ID: {SCRAPER_ID}")
    print()
    print("Paths:")
    print(f"  Base Dir: {get_base_dir()}")
    print(f"  Input Dir: {get_input_dir()}")
    print(f"  Output Dir: {get_output_dir()}")
    print(f"  Backup Dir: {get_backup_dir()}")
    print(f"  Logs Dir: {get_logs_dir()}")
    print()
    print("Config Values:")
    print(f"  Headless: {HEADLESS}")
    print(f"  Max Rows: {MAX_ROWS}")
    print(f"  AlfaBeta User Set: {'Yes' if ALFABETA_USER else 'No'}")
    print(f"  AlfaBeta Pass Set: {'Yes' if ALFABETA_PASS else 'No'}")
    print(f"  Proxy 1 Set: {'Yes' if PROXY_1 else 'No'}")
    print(f"  Proxy 2 Set: {'Yes' if PROXY_2 else 'No'}")
    print(f"  Proxy 3 Set: {'Yes' if PROXY_3 else 'No'}")
