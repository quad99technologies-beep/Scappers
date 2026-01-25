import json
import os
import socket
import subprocess
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List


DEFAULT_IP_URLS = [
    "https://api.ipify.org?format=json",
    "https://ifconfig.co/json",
    "https://ipinfo.io/json",
]


def _parse_ip_from_json(text: str) -> Optional[str]:
    try:
        data = json.loads(text)
    except Exception:
        return None
    for key in ("ip", "IP", "address"):
        v = data.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def get_public_ip_direct(timeout_seconds: float = 10.0, urls: Optional[List[str]] = None) -> Optional[str]:
    """
    Get the machine's public IP (direct, without Tor SOCKS).
    Uses simple HTTPS JSON endpoints. Returns None on failure.
    """
    url_list = urls or DEFAULT_IP_URLS
    headers = {"User-Agent": "Mozilla/5.0"}
    for url in url_list:
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                raw = resp.read().decode("utf-8", "ignore")
            ip = _parse_ip_from_json(raw) or raw.strip()
            if ip:
                return ip
        except Exception:
            continue
    return None


def _socks5_connect(sock: socket.socket, dest_host: str, dest_port: int, timeout_seconds: float) -> None:
    sock.settimeout(timeout_seconds)
    # greeting: VER=5, NMETHODS=1, METHOD=0x00 (no auth)
    sock.sendall(b"\x05\x01\x00")
    resp = sock.recv(2)
    if len(resp) != 2 or resp[0] != 0x05 or resp[1] != 0x00:
        raise RuntimeError("SOCKS5 auth negotiation failed")

    # CONNECT request
    try:
        dest_host.encode("ascii")
        atyp = 0x03  # domain
        host_bytes = dest_host.encode("ascii")
    except Exception:
        atyp = 0x03
        host_bytes = dest_host.encode("utf-8")

    req = b"\x05\x01\x00" + bytes([atyp, len(host_bytes)]) + host_bytes + dest_port.to_bytes(2, "big")
    sock.sendall(req)

    # reply: VER, REP, RSV, ATYP, BND.ADDR, BND.PORT
    rep = sock.recv(4)
    if len(rep) != 4 or rep[0] != 0x05 or rep[1] != 0x00:
        raise RuntimeError("SOCKS5 connect failed")

    atyp = rep[3]
    if atyp == 0x01:  # IPv4
        sock.recv(4)
    elif atyp == 0x04:  # IPv6
        sock.recv(16)
    elif atyp == 0x03:  # domain
        ln = sock.recv(1)
        if ln:
            sock.recv(ln[0])
    sock.recv(2)  # port


def get_public_ip_via_socks(
    socks_host: str,
    socks_port: int,
    timeout_seconds: float = 10.0,
    ip_http_host: str = "api.ipify.org",
) -> Optional[str]:
    """
    Get the exit IP as seen through a SOCKS5 proxy (Tor).

    Uses a plain HTTP endpoint to avoid TLS complexity.
    """
    try:
        with socket.create_connection((socks_host, socks_port), timeout=timeout_seconds) as s:
            _socks5_connect(s, ip_http_host, 80, timeout_seconds=timeout_seconds)
            req = (
                f"GET / HTTP/1.1\r\nHost: {ip_http_host}\r\nConnection: close\r\n"
                f"User-Agent: Mozilla/5.0\r\n\r\n"
            ).encode("ascii")
            s.sendall(req)
            data = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                data += chunk
            text = data.decode("utf-8", "ignore")
            # crude parse: body after headers
            if "\r\n\r\n" in text:
                body = text.split("\r\n\r\n", 1)[1].strip()
            else:
                body = text.strip()
            body = body.splitlines()[0].strip()
            return body or None
    except Exception:
        return None


def run_command(cmd: str, timeout_seconds: int = 60) -> Tuple[bool, str]:
    """
    Run a shell command. Returns (ok, output_tail).
    Intended for VPN reconnect commands (e.g., Surfshark).
    """
    if not cmd or not cmd.strip():
        return False, "empty command"
    try:
        p = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        out = (p.stdout or "") + (p.stderr or "")
        out = out.strip()
        if len(out) > 2000:
            out = out[-2000:]
        return p.returncode == 0, out
    except Exception as e:
        return False, str(e)


def wait_for_ip_change_direct(
    old_ip: Optional[str],
    timeout_seconds: int = 120,
    poll_seconds: float = 2.0,
) -> Optional[str]:
    """Wait until direct public IP changes (Surfshark reconnect)."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        ip = get_public_ip_direct()
        if ip and old_ip and ip != old_ip:
            return ip
        if ip and not old_ip:
            return ip
        time.sleep(poll_seconds)
    return None


def _read_control_line(sock: socket.socket, timeout_seconds: float = 5.0) -> str:
    sock.settimeout(timeout_seconds)
    data = b""
    end = time.monotonic() + timeout_seconds
    while time.monotonic() < end:
        try:
            chunk = sock.recv(1024)
        except socket.timeout:
            break
        if not chunk:
            break
        data += chunk
        for line in data.split(b"\r\n"):
            if line.startswith(b"250") or line.startswith(b"5"):
                return line.decode("utf-8", "ignore")
    return data.decode("utf-8", "ignore").strip().splitlines()[-1] if data else ""


def _send_control(sock: socket.socket, cmd: str) -> str:
    sock.sendall((cmd + "\r\n").encode("utf-8"))
    return _read_control_line(sock)


def _build_auth_cmd(cookie_file: str, password: str) -> str:
    if cookie_file:
        try:
            cookie = Path(cookie_file).read_bytes()
            return f"AUTHENTICATE {cookie.hex()}"
        except Exception:
            pass
    if password:
        safe_pw = password.replace("\\", "\\\\").replace('"', '\\"')
        return f'AUTHENTICATE "{safe_pw}"'
    return "AUTHENTICATE"


def tor_get_circuit_id(host: str, port: int, cookie_file: str = "", password: str = "") -> Optional[str]:
    """
    Best-effort: returns a stable-ish identifier for the current circuit set.
    We parse the first BUILT circuit id from circuit-status.
    """
    try:
        with socket.create_connection((host, port), timeout=5) as s:
            auth = _build_auth_cmd(cookie_file, password)
            if not _send_control(s, auth).startswith("250"):
                return None
            s.sendall(b"GETINFO circuit-status\r\n")
            raw = s.recv(16384).decode("utf-8", "ignore")
            # 250+circuit-status=\n123 BUILT ...\n.\n250 OK
            for line in raw.splitlines():
                line = line.strip()
                if not line or line.startswith("250") or line == ".":
                    continue
                # "<id> BUILT ..."
                parts = line.split()
                if len(parts) >= 2 and parts[1] == "BUILT":
                    return parts[0]
            return None
    except Exception:
        return None


@dataclass
class TorNewnymResult:
    ok: bool
    old_circuit_id: Optional[str]
    new_circuit_id: Optional[str]


def tor_signal_newnym(
    host: str,
    port: int,
    cookie_file: str = "",
    password: str = "",
    cooldown_seconds: int = 10,
) -> TorNewnymResult:
    old_id = tor_get_circuit_id(host, port, cookie_file=cookie_file, password=password)
    try:
        with socket.create_connection((host, port), timeout=5) as s:
            auth = _build_auth_cmd(cookie_file, password)
            if not _send_control(s, auth).startswith("250"):
                return TorNewnymResult(False, old_id, None)
            resp = _send_control(s, "SIGNAL NEWNYM")
            if not resp.startswith("250"):
                return TorNewnymResult(False, old_id, None)
    except Exception:
        return TorNewnymResult(False, old_id, None)

    time.sleep(max(0, int(cooldown_seconds)))
    new_id = tor_get_circuit_id(host, port, cookie_file=cookie_file, password=password)
    return TorNewnymResult(True, old_id, new_id)


def is_port_open(host: str, port: int, timeout_seconds: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except Exception:
        return False


def tor_bootstrap_percent(host: str, port: int, cookie_file: str = "", password: str = "") -> int:
    try:
        with socket.create_connection((host, port), timeout=3) as s:
            auth = _build_auth_cmd(cookie_file, password)
            if not _send_control(s, auth).startswith("250"):
                return -1
            s.sendall(b"GETINFO status/bootstrap-phase\r\n")
            raw = s.recv(4096).decode("utf-8", "ignore")
            for part in raw.split():
                if part.startswith("PROGRESS="):
                    return int(part.split("=", 1)[1])
            return -1
    except Exception:
        return -1


def wait_tor_ready(
    host: str,
    port: int,
    cookie_file: str = "",
    password: str = "",
    timeout_seconds: int = 120,
) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if is_port_open(host, port):
            pct = tor_bootstrap_percent(host, port, cookie_file=cookie_file, password=password)
            if pct == 100:
                return True
        time.sleep(1)
    return False

