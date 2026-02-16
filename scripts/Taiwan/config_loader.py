"""
Configuration Loader for Taiwan Scraper (uses shared ScraperConfig).
"""
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.config.scraper_config import ScraperConfig

SCRAPER_ID = "Taiwan"
_cfg = ScraperConfig(SCRAPER_ID)

# Re-export for scripts that import from config_loader
get_repo_root = _cfg.get_repo_root
getenv = _cfg.getenv
getenv_int = _cfg.getenv_int
getenv_float = _cfg.getenv_float
getenv_bool = _cfg.getenv_bool
getenv_list = _cfg.getenv_list
get_base_dir = _cfg.get_repo_root
get_input_dir = _cfg.get_input_dir
get_output_dir = _cfg.get_output_dir
get_backup_dir = _cfg.get_backup_dir
get_central_output_dir = _cfg.get_central_output_dir


def load_env_file() -> None:
    """Load environment variables. Must be called before getenv()."""
    _cfg.load_env()


if __name__ == "__main__":
    load_env_file()
    print("=" * 60)
    print("Taiwan Config Loader - Diagnostic")
    print("=" * 60)
    print(f"Scraper ID: {SCRAPER_ID}")
    print()
    print("Paths:")
    print(f"  Base Dir: {get_base_dir()}")
    print(f"  Input Dir: {get_input_dir()}")
    print(f"  Output Dir: {get_output_dir()}")
    print(f"  Backup Dir: {get_backup_dir()}")
