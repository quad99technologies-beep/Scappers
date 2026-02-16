import os
import time
import socket
import logging
import subprocess
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

def is_port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

def tor_authenticate(sock, cookie_file: Optional[str] = None) -> bool:
    try:
        if cookie_file:
            cookie = Path(cookie_file).read_bytes()
            cmd = f"AUTHENTICATE {cookie.hex()}\r\n"
        else:
            cmd = "AUTHENTICATE\r\n"
    except Exception:
        cmd = "AUTHENTICATE\r\n"
    
    try:
        sock.sendall(cmd.encode("utf-8"))
        resp = sock.recv(4096).decode("utf-8", "ignore")
        return resp.startswith("250")
    except Exception:
        return False

def get_tor_bootstrap_percent(control_host="127.0.0.1", control_port=9051, cookie_file=None) -> int:
    if control_port <= 0:
        return -1
    try:
        with socket.create_connection((control_host, control_port), timeout=2) as s:
            s.settimeout(2)
            if not tor_authenticate(s, cookie_file=cookie_file):
                return -1
            s.sendall(b"GETINFO status/bootstrap-phase\r\n")
            data = s.recv(4096).decode("utf-8", "ignore")
            for part in data.split():
                if part.startswith("PROGRESS="):
                    try:
                        return int(part.split("=", 1)[1])
                    except Exception:
                        return -1
            return -1
    except Exception:
        return -1

def auto_start_tor_proxy(control_host="127.0.0.1", control_port=9051, socks_port=9050, cookie_authentication=True) -> bool:
    """
    Best-effort auto-start for a standalone Tor daemon.
    Reuses Tor Browser's tor.exe if present.
    """
    # Check if already running
    if control_port > 0 and is_port_open(control_host, control_port):
        return True

    home = Path.home()
    tor_exe_candidates = [
        home / "OneDrive" / "Desktop" / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
        home / "Desktop" / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
        Path("C:/Tor Browser/Browser/TorBrowser/Tor/tor.exe")
    ]
    tor_exe = next((p for p in tor_exe_candidates if p.exists()), None)
    if not tor_exe:
        log.warning("[TOR_AUTO] tor.exe not found; cannot auto-start Tor proxy")
        return False

    torrc = Path("C:/TorProxy/torrc")
    data_dir = Path("C:/TorProxy/data")
    
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        torrc.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    auth_line = "CookieAuthentication 1\n" if cookie_authentication else "CookieAuthentication 0\n"
    desired_torrc = (
        f"DataDirectory {str(data_dir)}\n"
        f"SocksPort {socks_port}\n"
        f"ControlPort {control_port}\n"
        f"{auth_line}"
    )
    
    try:
        torrc.write_text(desired_torrc, encoding="ascii")
    except Exception as e:
        log.warning(f"[TOR_AUTO] Failed to write torrc: {e}")
        return False

    try:
        log.info(f"[TOR_AUTO] Starting Tor proxy: {tor_exe} -f {torrc}")
        subprocess.Popen(
            [str(tor_exe), "-f", str(torrc)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
    except Exception as e:
        log.warning(f"[TOR_AUTO] Failed to start Tor: {e}")
        return False

    # Wait for SOCKS port to open (best-effort)
    deadline = time.time() + 90
    while time.time() < deadline:
        if is_port_open(control_host, control_port):
            log.info(f"[TOR_AUTO] Tor proxy is now running on {control_host}:{control_port}")
            return True
        time.sleep(1)
    
    log.warning("[TOR_AUTO] Tor proxy did not come up within 90s")
    return False

def ensure_tor_proxy_running(control_host="127.0.0.1", control_port=9051, socks_port=9050, auto_start=True, cookie_file=None):
    if control_port <= 0:
        return
    
    if is_port_open(control_host, control_port):
        log.info(f"[TOR] Control port {control_host}:{control_port} is already running")
        return

    # Try to auto-start Tor
    if auto_start:
        log.warning(f"[TOR] Control port {control_host}:{control_port} not reachable; attempting auto-start...")
        if auto_start_tor_proxy(control_host, control_port, socks_port):
            return
    
    log.warning(f"[TOR] Control port {control_host}:{control_port} not reachable; start Tor Browser/tor.exe if required")
