"""
Configuration Loader for Netherlands Scraper (Platform Config Integration)

This module provides centralized config and path management for Netherlands scraper.
Integrates with platform_config.py to read from config/Netherlands.env.json.

Precedence (highest to lowest):
1. Runtime overrides
2. Environment variables (OS-level)
3. Platform config (config/Netherlands.env.json)
4. Hardcoded defaults
"""
import os
import sys
from pathlib import Path

# Add repo root to path for platform_config import
# Now: scripts/Netherlands/config_loader.py -> parents[2] = repo root
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))


def get_repo_root() -> Path:
    """Get repository root directory (parent of scraper directories)."""
    return _repo_root


def get_central_output_dir() -> Path:
    """Get central exports directory for final reports - uses Documents/ScraperPlatform/output/exports/Netherlands/"""
    if _PLATFORM_CONFIG_AVAILABLE:
        # Migrated: get_path_manager() -> ConfigManager
        exports_dir = ConfigManager.get_exports_dir(SCRAPER_ID)  # Scraper-specific exports
        exports_dir.mkdir(parents=True, exist_ok=True)
        return exports_dir
    else:
        # Fallback: use repo root output
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
SCRAPER_ID = "Netherlands"


def getenv(key: str, default: str = None) -> str:
    """
    Get environment variable with fallback to default.
    Integrates with platform_config if available.
    Checks both 'config' and 'secrets' sections.

    Args:
        key: Environment variable name
        default: Default value if not found

    Returns:
        Environment variable value or default (always as string)
    """
    # First check environment variables (highest precedence)
    env_val = os.getenv(key)
    if env_val is not None:
        return env_val
    
    if _PLATFORM_CONFIG_AVAILABLE:
        try:
            # Check config section (ConfigManager handles both config and secrets)
            val = ConfigManager.get_config_value(SCRAPER_ID, key, None)
            if val is not None:
                # Convert to string in case JSON config returns boolean/int/float
                return str(val)
        except Exception:
            pass
    
    # Return default if nothing found
    return default if default is not None else ""


def getenv_int(key: str, default: int = 0) -> int:
    """Get environment variable as integer."""
    try:
        val = getenv(key, str(default))
        return int(val)
    except (ValueError, TypeError):
        return default


def getenv_float(key: str, default: float = 0.0) -> float:
    """Get environment variable as float."""
    try:
        val = getenv(key, str(default))
        return float(val)
    except (ValueError, TypeError):
        return default


def getenv_bool(key: str, default: bool = False) -> bool:
    """Get environment variable as boolean."""
    val = getenv(key, default)

    # Handle case where val might already be a boolean (from JSON config)
    if isinstance(val, bool):
        return val

    # Convert to string and process
    val_str = str(val).strip().lower()
    if val_str in ("1", "true", "yes", "on"):
        return True
    elif val_str in ("0", "false", "no", "off", ""):
        return False
    return default


def getenv_list(key: str, default: list = None) -> list:
    """
    Get environment variable as list.
    Supports comma-separated strings or JSON arrays.

    Args:
        key: Environment variable name
        default: Default list if not found

    Returns:
        List of values
    """
    val = getenv(key)

    # If not found or empty, return default
    if not val:
        return default if default is not None else []

    # If already a list (from JSON config), return it
    if isinstance(val, list):
        return val

    # If string, split by comma
    if isinstance(val, str):
        return [item.strip() for item in val.split(',') if item.strip()]

    return default if default is not None else []


def require_env(key: str, default: str = None) -> str:
    """
    Get environment variable with fallback to default.
    Similar to getenv but emphasizes that the value should exist.

    Args:
        key: Environment variable name
        default: Default value if not found

    Returns:
        Environment variable value or default
    """
    return getenv(key, default)


def load_env_file():
    """
    Load environment variables from .env file.
    This is a no-op function for compatibility with scripts that expect it.

    Since we're using platform_config.py with JSON configuration files,
    we don't need to load .env files. This function exists only for
    backward compatibility with existing scripts.
    """
    pass


def get_base_dir() -> Path:
    """
    Get base directory for Netherlands scraper.

    With platform_config: Returns platform root
    Legacy mode: Returns parent of scripts folder
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        # Migrated: get_path_manager() -> ConfigManager
        return ConfigManager.get_app_root()
    else:
        # Legacy: relative to script location
        return Path(__file__).resolve().parents[1]


def get_input_dir(subpath: str = None) -> Path:
    """
    Get input directory - uses Documents/ScraperPlatform/input/Netherlands/

    Args:
        subpath: Optional subdirectory under input/
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        # Migrated: get_path_manager() -> ConfigManager
        base = ConfigManager.get_input_dir(SCRAPER_ID)  # Scraper-specific input
        base.mkdir(parents=True, exist_ok=True)
    else:
        base = get_base_dir() / "input"

    if subpath:
        return base / subpath
    return base


def get_output_dir(subpath: str = None) -> Path:
    """
    Get output directory - uses Documents/ScraperPlatform/output/Netherlands/
    
    Scraper-specific output directory for organized file management.

    Args:
        subpath: Optional subdirectory under output/
    """
    # First check if OUTPUT_DIR is explicitly set (absolute path or environment variable)
    output_dir_str = getenv("OUTPUT_DIR", "")
    if output_dir_str and Path(output_dir_str).is_absolute():
        base = Path(output_dir_str)
    else:
        # Use scraper-specific platform output directory
        if _PLATFORM_CONFIG_AVAILABLE:
            # Migrated: get_path_manager() -> ConfigManager
            base = ConfigManager.get_output_dir(SCRAPER_ID)  # Scraper-specific output
            base.mkdir(parents=True, exist_ok=True)
        else:
            # Fallback: use repo root output (legacy)
            repo_root = get_repo_root()
            base = repo_root / "output" / SCRAPER_ID
            base.mkdir(parents=True, exist_ok=True)

    if subpath:
        result = base / subpath
        result.mkdir(parents=True, exist_ok=True)
        return result
    return base


def get_backup_dir() -> Path:
    """Get backup directory - uses Documents/ScraperPlatform/output/backups/Netherlands/"""
    if _PLATFORM_CONFIG_AVAILABLE:
        # Migrated: get_path_manager() -> ConfigManager
        backup_dir = ConfigManager.get_backups_dir(SCRAPER_ID)  # Scraper-specific backups
        backup_dir.mkdir(parents=True, exist_ok=True)
        return backup_dir
    else:
        return get_base_dir() / "backups"


def get_logs_dir() -> Path:
    """Get logs directory."""
    if _PLATFORM_CONFIG_AVAILABLE:
        # Migrated: get_path_manager() -> ConfigManager
        return pm.get_logs_dir()
    else:
        return get_base_dir() / "logs"


# Diagnostic function
if __name__ == "__main__":
    print("=" * 60)
    print("Netherlands Config Loader - Diagnostic")
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
