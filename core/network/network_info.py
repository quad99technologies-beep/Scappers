"""
Network Information Module

Provides functionality to detect and track network configuration for scrapers.
Shows whether Tor, VPN, or Direct connection is being used and the current IP address.
"""

import socket
import subprocess
import json
import time
from pathlib import Path
from typing import Optional, Dict, Tuple
from dataclasses import dataclass, asdict
from enum import Enum


class NetworkType(Enum):
    """Types of network connections"""
    TOR = "Tor"
    VPN = "VPN"
    PROXY = "Proxy"
    DIRECT = "Direct"
    UNKNOWN = "Unknown"


@dataclass
class NetworkInfo:
    """Network information for a scraper"""
    scraper_name: str
    network_type: str
    ip_address: str
    exit_ip: Optional[str] = None  # For Tor/VPN exit IP
    port: Optional[int] = None
    status: str = "unknown"
    last_updated: float = 0.0
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "NetworkInfo":
        return cls(**data)


def is_port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    """Check if a port is open on a host"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def check_tor_running(host: str = "127.0.0.1", socks_port: int = 9050, control_port: int = 9051) -> Tuple[bool, Optional[int]]:
    """
    Check if Tor is running.
    Returns (is_running, detected_port)
    """
    # Check SOCKS ports
    for port in [socks_port, 9150, 9050]:
        if is_port_open(host, port):
            return True, port
    
    # Check control port as fallback
    if is_port_open(host, control_port):
        return True, None
    
    return False, None


def get_public_ip_direct(timeout: float = 10.0) -> Optional[str]:
    """Get public IP address directly (without proxy)"""
    try:
        import urllib.request
        urls = [
            "https://api.ipify.org?format=json",
            "https://ifconfig.co/json",
            "https://ipinfo.io/json",
        ]
        for url in urls:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read().decode("utf-8", "ignore"))
                    return data.get("ip") or data.get("IP")
            except Exception:
                continue
    except Exception:
        pass
    return None


def get_public_ip_via_tor(socks_port: int = 9050, timeout: float = 10.0) -> Optional[str]:
    """Get public IP address through Tor SOCKS proxy"""
    try:
        import socks
        import socket as socket_module
        import urllib.request
        
        # Save original socket
        original_socket = socket_module.socket
        
        # Set up SOCKS proxy
        socks.set_default_proxy(socks.SOCKS5, "127.0.0.1", socks_port)
        socket_module.socket = socks.socksocket
        
        try:
            urls = [
                "https://api.ipify.org?format=json",
                "https://ifconfig.co/json",
            ]
            for url in urls:
                try:
                    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        data = json.loads(resp.read().decode("utf-8", "ignore"))
                        return data.get("ip") or data.get("IP")
                except Exception:
                    continue
        finally:
            # Restore original socket
            socket_module.socket = original_socket
    except ImportError:
        # socks library not available, try manual method
        pass
    except Exception:
        pass
    return None


def try_auto_start_tor(config: Optional[dict] = None, wait_seconds: int = 30) -> bool:
    """
    Best-effort auto-start of standalone Tor on 127.0.0.1:9050 (control 9051).
    Uses Tor Browser's tor.exe if present. Returns True if Tor is running after the attempt.
    """
    if config is not None and not config.get("AUTO_START_TOR_PROXY", True):
        return False
    if check_tor_running()[0]:
        return True
    home = Path.home()
    tor_exe_candidates = [
        home / "OneDrive" / "Desktop" / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
        home / "Desktop" / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
    ]
    tor_exe = next((p for p in tor_exe_candidates if p.exists()), None)
    if not tor_exe:
        return False
    torrc = Path("C:/TorProxy/torrc")
    data_dir = Path("C:/TorProxy/data")
    data_dir.mkdir(parents=True, exist_ok=True)
    torrc.parent.mkdir(parents=True, exist_ok=True)
    desired_torrc = (
        "DataDirectory C:\\TorProxy\\data\n"
        "SocksPort 9050\n"
        "ControlPort 9051\n"
        "CookieAuthentication 1\n"
    )
    try:
        torrc.write_text(desired_torrc, encoding="ascii")
    except Exception:
        return False
    try:
        subprocess.Popen(
            [str(tor_exe), "-f", str(torrc)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
    except Exception:
        return False
    deadline = time.time() + min(wait_seconds, 90)
    while time.time() < deadline:
        if check_tor_running()[0]:
            return True
        time.sleep(1)
    return False


def detect_network_type(scraper_name: str, config: Optional[dict] = None) -> NetworkInfo:
    """
    Detect network type and IP for a scraper based on its configuration.
    
    Args:
        scraper_name: Name of the scraper
        config: Optional configuration dictionary
        
    Returns:
        NetworkInfo object with detected network information
    """
    info = NetworkInfo(
        scraper_name=scraper_name,
        network_type=NetworkType.UNKNOWN.value,
        ip_address="unknown",
        status="checking",
        last_updated=time.time()
    )
    
    # Load config if not provided
    if config is None:
        config = load_scraper_config(scraper_name)
    
    if not config:
        info.status = "no_config"
        return info
    
    # Check for Tor configuration
    # First check if Tor is explicitly disabled
    tor_enabled = config.get("TOR_ENABLED", True)
    if isinstance(tor_enabled, str):
        tor_enabled = tor_enabled.strip().lower() in ("1", "true", "yes", "on")
    
    # Only check other Tor settings if TOR_ENABLED is not explicitly False
    tor_control_port = config.get("TOR_CONTROL_PORT", 0)
    if isinstance(tor_control_port, str):
        try:
            tor_control_port = int(tor_control_port) if tor_control_port else 0
        except ValueError:
            tor_control_port = 0
    tor_socks_port = config.get("TOR_SOCKS_PORT", 9050)
    if isinstance(tor_socks_port, str):
        try:
            tor_socks_port = int(tor_socks_port) if tor_socks_port else 9050
        except ValueError:
            tor_socks_port = 9050
    tor_newnym_enabled = config.get("TOR_NEWNYM_ENABLED", False)
    # Belarus (and similar) use SCRIPT_01_USE_TOR_BROWSER to enable Tor
    use_tor_browser = config.get("SCRIPT_01_USE_TOR_BROWSER", False)
    if isinstance(use_tor_browser, str):
        use_tor_browser = use_tor_browser.strip().lower() in ("1", "true", "yes")
    
    # Only use Tor if TOR_ENABLED is not False AND at least one other Tor setting is present
    if tor_enabled and (tor_control_port or tor_newnym_enabled or use_tor_browser):
        tor_running, detected_port = check_tor_running(
            socks_port=tor_socks_port,
            control_port=tor_control_port or 9051
        )
        
        if tor_running:
            info.network_type = NetworkType.TOR.value
            info.port = detected_port or tor_socks_port
            info.status = "active"
            
            # Get IP through Tor
            exit_ip = get_public_ip_via_tor(info.port)
            if exit_ip:
                info.exit_ip = exit_ip
                info.ip_address = exit_ip
            else:
                info.ip_address = "Tor (IP hidden)"
            
            return info
        else:
            # Tor configured but not running — try auto-start (same logic as Argentina pipeline)
            if config.get("AUTO_START_TOR_PROXY", True):
                if try_auto_start_tor(config, wait_seconds=30):
                    tor_running, detected_port = check_tor_running(
                        socks_port=tor_socks_port,
                        control_port=tor_control_port or 9051
                    )
                    if tor_running:
                        info.network_type = NetworkType.TOR.value
                        info.port = detected_port or tor_socks_port
                        info.status = "active"
                        exit_ip = get_public_ip_via_tor(info.port)
                        if exit_ip:
                            info.exit_ip = exit_ip
                            info.ip_address = exit_ip
                        else:
                            info.ip_address = "Tor (IP hidden)"
                        return info
            info.network_type = NetworkType.TOR.value
            info.status = "tor_configured_but_not_running"
            info.ip_address = "Start Tor to use"
            return info
    
    # Check for VPN configuration
    vpn_required = config.get("VPN_REQUIRED", False)
    vpn_check_enabled = config.get("VPN_CHECK_ENABLED", False)
    
    if vpn_required or vpn_check_enabled:
        info.network_type = NetworkType.VPN.value
        info.status = "configured"
        # Get direct IP (should be VPN IP if VPN is active)
        ip = get_public_ip_direct()
        if ip:
            info.ip_address = ip
        return info
    
    # Check for Proxy configuration
    proxy_list = []
    for i in range(1, 10):
        proxy = config.get(f"PROXY_{i}", "")
        if proxy and not proxy.startswith("https://user-your"):  # Skip placeholder
            proxy_list.append(proxy)
    
    if proxy_list or config.get("PROXY_LIST_FILE"):
        info.network_type = NetworkType.PROXY.value
        info.status = "configured"
        ip = get_public_ip_direct()
        if ip:
            info.ip_address = ip
        return info
    
    # Default to Direct
    info.network_type = NetworkType.DIRECT.value
    info.status = "active"
    ip = get_public_ip_direct()
    if ip:
        info.ip_address = ip
    else:
        info.ip_address = "unknown"
    
    return info


def load_scraper_config(scraper_name: str) -> Optional[dict]:
    """Load configuration for a scraper from its env.json file"""
    try:
        # Resolve to repo root: core/network/network_info.py -> core/network -> core -> repo_root
        repo_root = Path(__file__).resolve().parents[2]
        config_path = repo_root / "config" / f"{scraper_name}.env.json"
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Merge config and secrets sections
                config = data.get("config", {})
                config.update(data.get("secrets", {}))
                return config
    except Exception:
        pass
    return None


def get_network_info_for_scraper(scraper_name: str, force_refresh: bool = False) -> NetworkInfo:
    """
    Get network information for a scraper.
    Caches results for 60 seconds unless force_refresh is True.
    """
    # Resolve to repo root: core/network/network_info.py -> core/network -> core -> repo_root
    repo_root = Path(__file__).resolve().parents[2]
    cache_file = repo_root / ".cache" / "network_info.json"
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Try to load from cache
    if not force_refresh and cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)
                cached_info = cache.get(scraper_name)
                if cached_info:
                    info = NetworkInfo.from_dict(cached_info)
                    # Return cached if less than 60 seconds old
                    if time.time() - info.last_updated < 60:
                        return info
        except Exception:
            pass
    
    # Detect fresh
    info = detect_network_type(scraper_name)
    
    # Save to cache
    try:
        cache = {}
        if cache_file.exists():
            with open(cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)
        cache[scraper_name] = info.to_dict()
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
    except Exception:
        pass
    
    return info


def format_network_status(info: NetworkInfo) -> str:
    """Format network status for display"""
    if info.status == "no_config":
        return "No Config"
    elif info.status == "tor_configured_but_not_running":
        return f"Tor (Not Running)"
    elif info.network_type == NetworkType.TOR.value:
        if info.exit_ip:
            return f"Tor | {info.exit_ip}"
        else:
            return f"Tor | IP Hidden"
    elif info.network_type == NetworkType.VPN.value:
        return f"VPN | {info.ip_address}"
    elif info.network_type == NetworkType.PROXY.value:
        return f"Proxy | {info.ip_address}"
    elif info.network_type == NetworkType.DIRECT.value:
        return f"Direct | {info.ip_address}"
    else:
        return f"Unknown | {info.ip_address}"


if __name__ == "__main__":
    # Test the module
    import sys
    
    scrapers = ["Argentina", "Russia", "Malaysia", "India", "Belarus"]
    
    print("Network Information Test")
    print("=" * 60)
    
    for scraper in scrapers:
        info = get_network_info_for_scraper(scraper, force_refresh=True)
        print(f"\n{scraper}:")
        print(f"  Type: {info.network_type}")
        print(f"  IP: {info.ip_address}")
        print(f"  Exit IP: {info.exit_ip or 'N/A'}")
        print(f"  Port: {info.port or 'N/A'}")
        print(f"  Status: {info.status}")
        print(f"  Display: {format_network_status(info)}")
