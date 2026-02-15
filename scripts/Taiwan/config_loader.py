"""
Configuration Loader for Taiwan Scraper (Platform Config Integration)

This module wraps platform_config.py for centralized path management.
All configuration is loaded from config/Taiwan.env.json.

Precedence (highest to lowest):
1. Runtime overrides
2. Environment variables (OS-level)
3. Platform config (config/Taiwan.env.json)
4. Hardcoded defaults
"""
import os
import sys
from pathlib import Path

# Add repo root to path for platform_config import
# scripts/Taiwan/config_loader.py -> parents[2] = repo root
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))


def get_repo_root() -> Path:
    """Get repository root directory (parent of scraper directories)."""
    return _repo_root


def get_central_output_dir() -> Path:
    """Get central exports directory for final reports - exports/Taiwan/."""
    if _PLATFORM_CONFIG_AVAILABLE:
        # Migrated: get_path_manager() -> ConfigManager
        exports_dir = ConfigManager.get_exports_dir(SCRAPER_ID)
        exports_dir.mkdir(parents=True, exist_ok=True)
        return exports_dir
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

SCRAPER_ID = "Taiwan"


def load_env_file():
    """
    Load environment variables from platform.env and Taiwan.env.
    Must be called before using getenv() in scripts.
    """
    try:
        repo_root = get_repo_root()
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        from core.config.config_manager import ConfigManager
        ConfigManager.ensure_dirs()
        ConfigManager.load_env(SCRAPER_ID)
    except (ImportError, FileNotFoundError, ValueError):
        try:
            from dotenv import load_dotenv
            config_dir = get_repo_root() / "config"
            env_file = config_dir / f"{SCRAPER_ID}.env"
            if env_file.exists():
                load_dotenv(env_file, override=True)
            platform_env = config_dir / "platform.env"
            if platform_env.exists():
                load_dotenv(platform_env, override=False)
        except ImportError:
            pass


def getenv(key: str, default: str = None) -> str:
    """
    Get environment variable with fallback to default.
    Integrates with platform_config if available.
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        cr = get_config_resolver()
        return cr.get(SCRAPER_ID, key, default if default is not None else "")
    return os.getenv(key, default)


def getenv_int(key: str, default: int = 0) -> int:
    """Get environment variable as integer."""
    try:
        value = getenv(key, str(default))
        return int(value)
    except (ValueError, TypeError):
        return default


def getenv_float(key: str, default: float = 0.0) -> float:
    """Get environment variable as float."""
    try:
        value = getenv(key, str(default))
        return float(value)
    except (ValueError, TypeError):
        return default


def getenv_bool(key: str, default: bool = False) -> bool:
    """Get environment variable as boolean."""
    value = getenv(key, str(default))
    if isinstance(value, bool):
        return value
    value_str = str(value).lower()
    return value_str in ("true", "1", "yes", "on")


def getenv_list(key: str, default: list = None) -> list:
    """Get environment variable as list (handles JSON arrays)."""
    if default is None:
        default = []
    if _PLATFORM_CONFIG_AVAILABLE:
        cr = get_config_resolver()
        value = cr.get(SCRAPER_ID, key, default)
    else:
        value = os.getenv(key)
        if value is None:
            return default
        if isinstance(value, str):
            try:
                import json
                value = json.loads(value)
            except (json.JSONDecodeError, ValueError):
                value = [v.strip() for v in value.split(",") if v.strip()]
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            import json
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return [v.strip() for v in value.split(",") if v.strip()]
    return default if value is None else [value]


def get_base_dir() -> Path:
    """Get base directory for Taiwan scraper."""
    if _PLATFORM_CONFIG_AVAILABLE:
        # Migrated: get_path_manager() -> ConfigManager
        return ConfigManager.get_app_root()
    return Path(__file__).resolve().parents[1]


def get_input_dir(subpath: str = None) -> Path:
    """Get input directory - input/Taiwan/."""
    if _PLATFORM_CONFIG_AVAILABLE:
        # Migrated: get_path_manager() -> ConfigManager
        base = ConfigManager.get_input_dir(SCRAPER_ID)
        base.mkdir(parents=True, exist_ok=True)
    else:
        base = get_base_dir() / "input"
    if subpath:
        return base / subpath
    return base


def get_output_dir(subpath: str = None) -> Path:
    """Get output directory - output/Taiwan/."""
    output_dir_str = getenv("OUTPUT_DIR", "")
    if output_dir_str and Path(output_dir_str).is_absolute():
        base = Path(output_dir_str)
    else:
        if _PLATFORM_CONFIG_AVAILABLE:
            # Migrated: get_path_manager() -> ConfigManager
            base = ConfigManager.get_output_dir(SCRAPER_ID)
            base.mkdir(parents=True, exist_ok=True)
        else:
            repo_root = get_repo_root()
            base = repo_root / "output" / SCRAPER_ID
            base.mkdir(parents=True, exist_ok=True)
    if subpath:
        result = base / subpath
        result.mkdir(parents=True, exist_ok=True)
        return result
    return base


def get_backup_dir() -> Path:
    """Get backup directory - backups/Taiwan/."""
    if _PLATFORM_CONFIG_AVAILABLE:
        # Migrated: get_path_manager() -> ConfigManager
        return ConfigManager.get_backups_dir(SCRAPER_ID)
    repo_root = get_repo_root()
    backup_dir = repo_root / "backups" / SCRAPER_ID
    backup_dir.mkdir(parents=True, exist_ok=True)
    return backup_dir


if __name__ == "__main__":
    print("=" * 60)
    print("Taiwan Config Loader - Diagnostic")
    print("=" * 60)
    print(f"Platform Config Available: {_PLATFORM_CONFIG_AVAILABLE}")
    print(f"Scraper ID: {SCRAPER_ID}")
    print()
    print("Paths:")
    print(f"  Base Dir: {get_base_dir()}")
    print(f"  Input Dir: {get_input_dir()}")
    print(f"  Output Dir: {get_output_dir()}")
    print(f"  Backup Dir: {get_backup_dir()}")
