"""
Configuration Loader for Canada Ontario Scraper (Facade for Core ConfigManager)

This module provides centralized config and path management for Canada Ontario scraper.
It acts as a facade, delegating all logic to core.config.config_manager.ConfigManager.
"""
import sys
from pathlib import Path
from typing import Optional

_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.config.scraper_config_factory import create_config

SCRAPER_ID = "CanadaOntario"
config = create_config(SCRAPER_ID)

# --- Path Accessors ---
def get_repo_root() -> Path: return config.get_repo_root()
def get_base_dir() -> Path: return config.get_base_dir()
def get_central_output_dir() -> Path: return config.get_central_output_dir()
def get_input_dir(subpath=None) -> Path: return config.get_input_dir(subpath)
def get_output_dir(subpath=None) -> Path: return config.get_output_dir(subpath)
def get_backup_dir() -> Path: return config.get_backup_dir()
def get_logs_dir() -> Path: return config.get_output_dir("logs")

# --- Environment Accessors ---
def load_env_file() -> None: pass  # no-op, already loaded on import
def getenv(key: str, default: str = "") -> str: return config.getenv(key, default)
def getenv_int(key: str, default: int = 0) -> int: return config.getenv_int(key, default)
def getenv_float(key: str, default: float = 0.0) -> float: return config.getenv_float(key, default)
def getenv_bool(key: str, default: bool = False) -> bool: return config.getenv_bool(key, default)
def getenv_list(key: str, default: list = None) -> list: return config.getenv_list(key, default or [])


def get_run_id() -> str:
    """Get or generate a run id for this process."""
    # Check for external run_id from GUI/Telegram/API sync first
    run_id = getenv("CANADA_ONTARIO_RUN_ID", "") or getenv("RUN_ID", "")
    if run_id:
        return run_id
    # Fallback: read from .current_run_id file written by step 0
    try:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            file_run_id = run_id_file.read_text(encoding="utf-8").strip()
            if file_run_id:
                return file_run_id
    except Exception:
        pass
    from datetime import datetime
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def get_run_dir(run_id: Optional[str] = None) -> Path:
    """Get run directory for the current run."""
    from core.config.config_manager import ConfigManager
    run_id = run_id or get_run_id()
    run_dir = ConfigManager.get_runs_dir() / run_id
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "artifacts").mkdir(parents=True, exist_ok=True)
    (run_dir / "exports").mkdir(parents=True, exist_ok=True)
    return run_dir


def get_proxy_config() -> dict:
    """Get proxy configuration for requests (optional)."""
    proxy_url = getenv("PROXY_URL", "").strip()
    if not proxy_url:
        return {}
    return {"http": proxy_url, "https": proxy_url}


# File names
PRODUCTS_CSV_NAME = getenv("PRODUCTS_CSV_NAME", "products.csv")
MANUFACTURER_MASTER_CSV_NAME = getenv("MANUFACTURER_MASTER_CSV_NAME", "manufacturer_master.csv")

# Final report configuration
FINAL_REPORT_NAME_PREFIX = getenv("FINAL_REPORT_NAME_PREFIX", "canadaontarioreport_")
FINAL_REPORT_DATE_FORMAT = getenv("FINAL_REPORT_DATE_FORMAT", "%d%m%Y")

# EAP prices configuration
EAP_PRICES_URL = getenv("EAP_PRICES_URL", "https://www.ontario.ca/page/exceptional-access-program-product-prices")
EAP_PRICES_CSV_NAME = getenv("EAP_PRICES_CSV_NAME", "ontario_eap_prices.csv")

# Static values
STATIC_CURRENCY = getenv("STATIC_CURRENCY", "CAD")
STATIC_REGION = getenv("STATIC_REGION", "NORTH AMERICA")


# Diagnostic function
if __name__ == "__main__":
    print("=" * 60)
    print("Canada Ontario Config Loader - Diagnostic (Facade)")
    print("=" * 60)
    print(f"Scraper ID: {SCRAPER_ID}")
    print(f"Base Dir: {get_base_dir()}")
    print(f"Input Dir: {get_input_dir()}")
    print(f"Output Dir: {get_output_dir()}")
    print(f"Backup Dir: {get_backup_dir()}")

# Validation configs
MAX_BAD_ROW_RATIO = getenv_float("MAX_BAD_ROW_RATIO", 0.3)
PAGE_VALIDATION_RETRIES = getenv_int("PAGE_VALIDATION_RETRIES", 3)

# Browser config
USE_BROWSER = getenv_bool("USE_BROWSER", False)
CHROME_PID_TRACKING_AVAILABLE = getenv_bool("CHROME_PID_TRACKING_AVAILABLE", True)

