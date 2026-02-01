"""
Configuration Loader for India NPPA Pharma Sahi Daam Scraper (Platform Config Integration)

Loads configuration from config/India.env.json with fallback to .env.
"""
import os
import sys
from pathlib import Path

_resolved_file = Path(__file__).resolve()
_script_dir = _resolved_file.parent
_parents = _resolved_file.parents
if len(_parents) >= 3:
    _repo_root = _parents[2]
else:
    _repo_root = _parents[-1]

if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

SCRAPER_ID = "India"

_LOCAL_INPUT_BASE = _script_dir / "input" / SCRAPER_ID
_LOCAL_OUTPUT_BASE = _script_dir / "output" / SCRAPER_ID

def _ensure_dir(base: Path, fallback: Path) -> Path:
    try:
        base.mkdir(parents=True, exist_ok=True)
        return base
    except OSError:
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback

try:
    from platform_config import get_path_manager, get_config_resolver
    _PLATFORM_CONFIG_AVAILABLE = True
except ImportError:
    _PLATFORM_CONFIG_AVAILABLE = False
    get_path_manager = None
    get_config_resolver = None


def get_repo_root() -> Path:
    return _repo_root


def load_env_file() -> None:
    try:
        from core.config_manager import ConfigManager
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


def getenv(key: str, default: str = None):
    env_val = os.getenv(key)
    if env_val is not None and env_val != "":
        return env_val
    if _PLATFORM_CONFIG_AVAILABLE:
        cr = get_config_resolver()
        return cr.get(SCRAPER_ID, key, default if default is not None else "")
    return os.getenv(key, default)


def getenv_int(key: str, default: int = 0) -> int:
    try:
        return int(getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


def getenv_float(key: str, default: float = 0.0) -> float:
    try:
        return float(getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


def getenv_bool(key: str, default: bool = False) -> bool:
    value = getenv(key, str(default))
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("true", "1", "yes", "on")


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


def getenv_list(key: str, default: list = None) -> list:
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
            except Exception:
                value = [v.strip() for v in value.split(",") if v.strip()]
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            import json
            return json.loads(value)
        except Exception:
            return [v.strip() for v in value.split(",") if v.strip()]
    return default if value is None else [value]


def get_output_dir(subpath: str = None) -> Path:
    output_dir_str = getenv("OUTPUT_DIR", "")
    if output_dir_str and Path(output_dir_str).is_absolute():
        base = Path(output_dir_str)
    else:
        if _PLATFORM_CONFIG_AVAILABLE:
            pm = get_path_manager()
            base = pm.get_output_dir(SCRAPER_ID)
        else:
            base = get_repo_root() / "output" / SCRAPER_ID
    base = _ensure_dir(base, _LOCAL_OUTPUT_BASE)
    if subpath:
        result = base / subpath
        result.mkdir(parents=True, exist_ok=True)
        return result
    return base


def get_input_dir(subpath: str = None) -> Path:
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        base = pm.get_input_dir(SCRAPER_ID)
    else:
        base = get_repo_root() / "input" / SCRAPER_ID
    base = _ensure_dir(base, _LOCAL_INPUT_BASE)
    if subpath:
        result = base / subpath
        result.mkdir(parents=True, exist_ok=True)
        return result
    return base


def get_backup_dir() -> Path:
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_backups_dir(SCRAPER_ID)
    base = get_repo_root() / "backups" / SCRAPER_ID
    base.mkdir(parents=True, exist_ok=True)
    return base


def get_download_dir() -> Path:
    """Get the download directory for browser downloads."""
    base = get_output_dir("downloads")
    base.mkdir(parents=True, exist_ok=True)
    return base


def get_central_output_dir() -> Path:
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        exports_dir = pm.get_exports_dir(SCRAPER_ID)
        exports_dir.mkdir(parents=True, exist_ok=True)
        return exports_dir
    repo_root = get_repo_root()
    central_output = repo_root / "output"
    central_output.mkdir(parents=True, exist_ok=True)
    return central_output
