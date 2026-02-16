import socket
import logging
import time

log = logging.getLogger(__name__)

def check_vpn_connection(host: str, port: int, required: bool = True) -> bool:
    """
    Check if VPN is connected.
    Returns True if VPN check passes or is disabled.
    """
    if not required:
        return True
    
    log.info(f"[VPN] Checking connection to {host}:{port}...")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            log.info("[VPN] Connection check passed")
            return True
        else:
            log.warning(f"[VPN] Connection check failed (error code: {result})")
            return False
    except Exception as e:
        log.error(f"[VPN] Connection check error: {e}")
        return False

def check_tor_running(host: str = "127.0.0.1", ports: list = [9050, 9150], timeout: int = 2) -> tuple[bool, int]:
    """Check if Tor SOCKS5 proxy is running on any of the provided ports."""
    
    for port in ports:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                log.info(f"[TOR_CHECK] Tor proxy is running on {host}:{port}")
                return True, port
        except Exception:
            continue
    
    log.warning(f"[TOR_CHECK] Tor proxy is not running")
    return False, 0
