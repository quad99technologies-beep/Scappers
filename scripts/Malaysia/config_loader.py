"""
Configuration Loader for Malaysia Scraper (Platform Config Integration)

This module wraps platform_config.py for centralized path management.
All configuration is loaded from config/Malaysia.env.json.

Precedence (highest to lowest):
1. Runtime overrides
2. Environment variables (OS-level)
3. Platform config (config/Malaysia.env.json)
4. Hardcoded defaults
"""
import os
import sys
from pathlib import Path

# Add repo root to path for platform_config import
# Now: scripts/Malaysia/config_loader.py -> parents[2] = repo root
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))


def get_repo_root() -> Path:
    """Get repository root directory (parent of scraper directories)."""
    return _repo_root


def get_central_output_dir() -> Path:
    """Get central exports directory for final reports - uses Documents/ScraperPlatform/output/exports/Malaysia/"""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        exports_dir = pm.get_exports_dir(SCRAPER_ID)  # Scraper-specific exports
        exports_dir.mkdir(parents=True, exist_ok=True)
        return exports_dir
    else:
        # Fallback: use repo root output
        repo_root = get_repo_root()
        central_output = repo_root / "output"
        central_output.mkdir(parents=True, exist_ok=True)
        return central_output

# Try to import platform_config (preferred)
try:
    from platform_config import PathManager, ConfigResolver, get_path_manager, get_config_resolver
    _PLATFORM_CONFIG_AVAILABLE = True
except ImportError:
    _PLATFORM_CONFIG_AVAILABLE = False
    PathManager = None
    ConfigResolver = None

# Scraper ID for this scraper
SCRAPER_ID = "Malaysia"


def load_env_file():
    """
    Load environment variables from platform.env and Malaysia.env.
    Must be called before using getenv() in scripts.
    """
    try:
        # Add repo root to path for ConfigManager
        repo_root = get_repo_root()
        import sys
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        
        from core.config_manager import ConfigManager
        ConfigManager.ensure_dirs()
        ConfigManager.load_env(SCRAPER_ID)
    except (ImportError, FileNotFoundError, ValueError) as e:
        # Fallback: try to load from dotenv if available
        try:
            from dotenv import load_dotenv
            # Try to find .env file in config directory
            repo_root = get_repo_root()  # Get repo_root for fallback
            config_dir = repo_root / "config"
            env_file = config_dir / f"{SCRAPER_ID}.env"
            if env_file.exists():
                try:
                    load_dotenv(env_file, override=True)
                except Exception as parse_error:
                    print(f"Warning: Could not parse {env_file.name} file: {parse_error}")
            # Also try platform.env
            platform_env = config_dir / "platform.env"
            if platform_env.exists():
                try:
                    load_dotenv(platform_env, override=False)
                except Exception as parse_error:
                    print(f"Warning: Could not parse platform.env file: {parse_error}")
        except ImportError:
            pass  # dotenv not available, continue without loading


def getenv(key: str, default: str = None) -> str:
    """
    Get environment variable with fallback to default.
    Now integrates with platform_config if available.

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


def require_env(key: str) -> str:
    """
    Require environment variable from config file. Raises error if not found.
    
    Args:
        key: Environment variable name
        
    Returns:
        Environment variable value
        
    Raises:
        ValueError: If the environment variable is not set
    """
    value = getenv(key)
    if value is None or value == "":
        raise ValueError(f"Required environment variable '{key}' is not set in config file. Please add it to config/Malaysia.env.json")
    return value


def getenv_int(key: str, default: int = 0) -> int:
    """Get environment variable as integer"""
    try:
        value = getenv(key, str(default))
        return int(value)
    except (ValueError, TypeError):
        return default


def getenv_float(key: str, default: float = 0.0) -> float:
    """Get environment variable as float"""
    try:
        value = getenv(key, str(default))
        return float(value)
    except (ValueError, TypeError):
        return default


def getenv_bool(key: str, default: bool = False) -> bool:
    """Get environment variable as boolean"""
    value = getenv(key, str(default))
    # Handle case where value is already a boolean (from JSON config)
    if isinstance(value, bool):
        return value
    # Convert to string and check
    value_str = str(value).lower()
    return value_str in ("true", "1", "yes", "on")


def getenv_list(key: str, default: list = None) -> list:
    """Get environment variable as list (handles JSON arrays)"""
    if default is None:
        default = []
    if _PLATFORM_CONFIG_AVAILABLE:
        cr = get_config_resolver()
        value = cr.get(SCRAPER_ID, key, default)
    else:
        value = os.getenv(key)
        if value is None:
            return default
        # Try to parse as JSON if it's a string
        if isinstance(value, str):
            try:
                import json
                value = json.loads(value)
            except (json.JSONDecodeError, ValueError):
                # If not JSON, treat as comma-separated string
                value = [v.strip() for v in value.split(",") if v.strip()]
    
    # Handle case where value is already a list (from JSON config)
    if isinstance(value, list):
        return value
    # If it's a string, try to parse
    if isinstance(value, str):
        try:
            import json
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return [v.strip() for v in value.split(",") if v.strip()]
    return default if value is None else [value]


def get_base_dir() -> Path:
    """
    Get base directory for Malaysia scraper.

    With platform_config: Returns platform root
    Legacy mode: Returns parent of scripts folder
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_platform_root()
    else:
        # Legacy: relative to script location
        return Path(__file__).resolve().parents[1]


def get_input_dir(subpath: str = None) -> Path:
    """
    Get input directory - uses Documents/ScraperPlatform/input/Malaysia/

    Args:
        subpath: Optional subdirectory under input/
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        base = pm.get_input_dir(SCRAPER_ID)  # Scraper-specific input
        base.mkdir(parents=True, exist_ok=True)
    else:
        base = get_base_dir() / "input"

    if subpath:
        return base / subpath
    return base


def get_output_dir(subpath: str = None) -> Path:
    """
    Get output directory - uses Documents/ScraperPlatform/output/Malaysia/
    
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
            pm = get_path_manager()
            base = pm.get_output_dir(SCRAPER_ID)  # Scraper-specific output
            base.mkdir(parents=True, exist_ok=True)
        else:
            # Fallback: use repo root output (legacy)
            repo_root = get_repo_root()
            base = repo_root / "output"
            base.mkdir(parents=True, exist_ok=True)

    if subpath:
        result = base / subpath
        result.mkdir(parents=True, exist_ok=True)
        return result
    return base


def get_backup_dir() -> Path:
    """Get backup directory - scraper-specific: backups/Malaysia/"""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_backups_dir(SCRAPER_ID)  # Pass scraper ID for scraper-specific backup folder
    else:
        # Fallback: use repo root backups folder
        repo_root = get_repo_root()
        backup_dir = repo_root / "backups" / SCRAPER_ID
        backup_dir.mkdir(parents=True, exist_ok=True)
        return backup_dir


# Diagnostic function
if __name__ == "__main__":
    print("=" * 60)
    print("Malaysia Config Loader - Diagnostic")
    print("=" * 60)
    print(f"Platform Config Available: {_PLATFORM_CONFIG_AVAILABLE}")
    print(f"Scraper ID: {SCRAPER_ID}")
    print()
    print("Paths:")
    print(f"  Base Dir: {get_base_dir()}")
    print(f"  Input Dir: {get_input_dir()}")
    print(f"  Output Dir: {get_output_dir()}")
    print(f"  Backup Dir: {get_backup_dir()}")
    print()
    print("Sample Config Values:")
    print(f"  SCRIPT_01_URL: {getenv('SCRIPT_01_URL', 'not set')}")
    print(f"  SCRIPT_02_HEADLESS: {getenv('SCRIPT_02_HEADLESS', 'not set')}")
