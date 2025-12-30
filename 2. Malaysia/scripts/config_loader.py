"""
Configuration Loader for Malaysia Scraper (Updated for Platform Config Integration)

This module now wraps platform_config.py for centralized path management.
Maintains backward compatibility with legacy .env files.
Uses only standard library for .env parsing to avoid dependencies.

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


def get_repo_root() -> Path:
    """Get repository root directory (parent of scraper directories)."""
    return _repo_root


def get_central_output_dir() -> Path:
    """Get central output directory at repo root level for final reports."""
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


def _find_env_file() -> Path:
    """
    Find .env file by searching multiple locations.
    Prioritizes platform root (repository root where scraper_gui.py is).

    Returns:
        Path to .env file if found, None otherwise.
    """
    script_dir = Path(__file__).resolve().parent

    # Try 1: Platform root (repository root, 2 levels up from scripts/)
    platform_root_env = script_dir.parents[1] / ".env"
    if platform_root_env.exists():
        return platform_root_env

    # Try 2: Scraper root (parent of scripts directory)
    scraper_root_env = script_dir.parent / ".env"
    if scraper_root_env.exists():
        return scraper_root_env

    # Try 3: Current working directory
    cwd_env = Path.cwd() / ".env"
    if cwd_env.exists():
        return cwd_env

    # Try 4: Parent of current working directory
    parent_env = Path.cwd().parent / ".env"
    if parent_env.exists():
        return parent_env

    # Try 5: Same directory as script
    same_dir_env = script_dir / ".env"
    if same_dir_env.exists():
        return same_dir_env

    return None


def load_env_file(env_path: Path = None, debug: bool = False) -> None:
    """
    Load environment variables from .env file.

    Args:
        env_path: Path to .env file. If None, searches for .env in common locations.
        debug: If True, print debug information about loading process.
    """
    if env_path is None:
        env_path = _find_env_file()
        if env_path is None:
            if debug:
                print(f"[DEBUG] .env file not found in any search location")
            return
    elif not env_path.exists():
        if debug:
            print(f"[DEBUG] .env file not found at: {env_path}")
        return

    if debug:
        print(f"[DEBUG] Loading .env file from: {env_path}")

    try:
        loaded_count = 0
        with open(env_path, "r", encoding="utf-8-sig") as f:
            for line_num, line in enumerate(f, 1):
                original_line = line
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith("#"):
                    continue

                # Parse KEY=VALUE
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()

                    if not key:
                        continue

                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]

                    # Only set if not already in environment
                    if key not in os.environ:
                        os.environ[key] = value
                        loaded_count += 1
                        if debug:
                            print(f"[DEBUG] Loaded: {key} = {value}")
                    elif debug:
                        print(f"[DEBUG] Skipped {key} (already in environment)")

        if debug:
            print(f"[DEBUG] Loaded {loaded_count} environment variables from .env")
    except Exception as e:
        if debug:
            print(f"[DEBUG] Error loading .env file: {e}")
        pass


# Load .env file on import (legacy behavior)
load_env_file()


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
    Get input directory.

    Args:
        subpath: Optional subdirectory under input/
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        base = pm.get_input_dir(SCRAPER_ID)
    else:
        base = get_base_dir() / "input"

    if subpath:
        return base / subpath
    return base


def get_output_dir(subpath: str = None) -> Path:
    """
    Get output directory. Always prefers local scraper output directory over platform directory.

    Args:
        subpath: Optional subdirectory under output/
    """
    # First check if OUTPUT_DIR is explicitly set (absolute path or environment variable)
    output_dir_str = getenv("OUTPUT_DIR", "")
    if output_dir_str and Path(output_dir_str).is_absolute():
        base = Path(output_dir_str)
    else:
        # Always prefer local scraper output directory (parent of scripts directory)
        scraper_root = Path(__file__).resolve().parents[1]
        base = scraper_root / "Output"  # Note: Malaysia uses capital O
        base.mkdir(parents=True, exist_ok=True)

    if subpath:
        result = base / subpath
        result.mkdir(parents=True, exist_ok=True)
        return result
    return base


def get_backup_dir() -> Path:
    """Get backup directory."""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_backups_dir()
    else:
        return get_base_dir() / "Backup"


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
