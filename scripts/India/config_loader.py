"""
Configuration Loader for India NPPA Pharma Sahi Daam Scraper (Facade for Core ConfigManager)

This module provides centralized config and path management for India scraper.
It acts as a facade, delegating all logic to core.config.config_manager.ConfigManager.
"""
import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.config.scraper_config_factory import create_config

SCRAPER_ID = "India"
config = create_config(SCRAPER_ID)

# --- Path Accessors ---
def get_repo_root() -> Path: return config.get_repo_root()
def get_base_dir() -> Path: return config.get_base_dir()
def get_central_output_dir() -> Path: return config.get_central_output_dir()
def get_input_dir(subpath=None) -> Path: return config.get_input_dir(subpath)
def get_output_dir(subpath=None) -> Path: return config.get_output_dir(subpath)
def get_backup_dir() -> Path: return config.get_backup_dir()

def get_download_dir() -> Path:
    """Get the download directory for browser downloads."""
    base = get_output_dir("downloads")
    base.mkdir(parents=True, exist_ok=True)
    return base

# --- Environment Accessors ---
def load_env_file() -> None: pass  # no-op, already loaded on import
def getenv(key: str, default: str = "") -> str: return config.getenv(key, default)
def getenv_int(key: str, default: int = 0) -> int: return config.getenv_int(key, default)
def getenv_float(key: str, default: float = 0.0) -> float: return config.getenv_float(key, default)
def getenv_bool(key: str, default: bool = False) -> bool: return config.getenv_bool(key, default)
def getenv_list(key: str, default: list = None) -> list: return config.getenv_list(key, default or [])


def check_vpn_connection() -> bool:
    """
    Check if VPN is connected (if required).
    Returns True if VPN check passes or is disabled.
    """
    vpn_required = getenv_bool("VPN_REQUIRED", False)
    vpn_check_enabled = getenv_bool("VPN_CHECK_ENABLED", False)
    vpn_check_host = getenv("VPN_CHECK_HOST", "8.8.8.8")
    vpn_check_port = getenv_int("VPN_CHECK_PORT", 53)

    if not vpn_check_enabled:
        return True

    if not vpn_required:
        print("[VPN] VPN not required, skipping check", flush=True)
        return True

    print(f"[VPN] Checking connection to {vpn_check_host}:{vpn_check_port}...", flush=True)

    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((vpn_check_host, vpn_check_port))
        sock.close()

        if result == 0:
            print("[VPN] Connection check passed", flush=True)
            return True
        else:
            print(f"[VPN] Connection check failed (error code: {result})", flush=True)
            return False
    except Exception as e:
        print(f"[VPN] Connection check error: {e}", flush=True)
        return False


# --- Diagnostic ---
if __name__ == "__main__":
    print("=" * 60)
    print("India Config Loader - Diagnostic (Facade)")
    print("=" * 60)
    print(f"Scraper ID: {SCRAPER_ID}")
    print(f"Input Dir: {get_input_dir()}")
    print(f"Output Dir: {get_output_dir()}")
