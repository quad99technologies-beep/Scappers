"""
Tor SOCKS5 proxy support for httpx-based scrapers.

Provides Tor setup, health checking, auto-start, and periodic NEWNYM
identity rotation — same pattern as Argentina's Selenium Tor integration
but adapted for async httpx clients.

Requirements:
    pip install httpx[socks]   # installs socksio for SOCKS5 support

Usage:
    from core.network.tor_httpx import TorConfig, setup_tor, TorRotator

    tor_cfg = TorConfig.from_env()
    proxy_url = setup_tor(tor_cfg)   # "socks5://127.0.0.1:9050" or None

    async with httpx.AsyncClient(proxy=proxy_url, ...) as client:
        rotator = TorRotator(tor_cfg)
        rotator.start()
        # ... scraping ...
        rotator.stop()
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from core.network.ip_rotation import (
    get_public_ip_via_socks,
    is_port_open,
    tor_bootstrap_percent,
    tor_signal_newnym,
    wait_tor_ready,
)


@dataclass
class TorConfig:
    """Tor proxy configuration — mirrors Argentina's TOR_* env vars."""

    enabled: bool = False
    socks_host: str = "127.0.0.1"
    socks_port: int = 9050
    control_host: str = "127.0.0.1"
    control_port: int = 9051
    control_password: str = ""
    control_cookie_file: str = ""
    newnym_enabled: bool = False
    newnym_interval_seconds: int = 300
    newnym_cooldown_seconds: int = 10
    auto_start: bool = False
    require: bool = False

    # ------------------------------------------------------------------ #
    # Factory
    # ------------------------------------------------------------------ #
    @classmethod
    def from_env(cls, getenv_fn: Callable | None = None) -> "TorConfig":
        """Load Tor config from env / config_loader.

        Args:
            getenv_fn: Custom getenv function (e.g. config_loader.getenv).
                        Falls back to os.getenv.
        """
        g = getenv_fn or os.getenv

        def _bool(key: str, default: bool = False) -> bool:
            val = g(key, str(default))
            if isinstance(val, bool):
                return val
            return str(val).strip().lower() in ("1", "true", "yes", "on")

        def _int(key: str, default: int = 0) -> int:
            try:
                return int(g(key, str(default)))
            except (ValueError, TypeError):
                return default

        return cls(
            enabled=_bool("TOR_ENABLED", False),
            socks_host=(g("TOR_SOCKS_HOST", "127.0.0.1") or "127.0.0.1"),
            socks_port=_int("TOR_SOCKS_PORT", 9050),
            control_host=(g("TOR_CONTROL_HOST", "127.0.0.1") or "127.0.0.1"),
            control_port=_int("TOR_CONTROL_PORT", 9051),
            control_password=(g("TOR_CONTROL_PASSWORD", "") or ""),
            control_cookie_file=(g("TOR_CONTROL_COOKIE_FILE", "") or ""),
            newnym_enabled=_bool("TOR_NEWNYM_ENABLED", False),
            newnym_interval_seconds=_int("TOR_NEWNYM_INTERVAL_SECONDS", 300),
            newnym_cooldown_seconds=_int("TOR_NEWNYM_COOLDOWN_SECONDS", 10),
            auto_start=_bool("AUTO_START_TOR_PROXY", False),
            require=_bool("REQUIRE_TOR_PROXY", False),
        )

    @property
    def proxy_url(self) -> Optional[str]:
        """Return httpx-compatible SOCKS5 proxy URL, or None."""
        if self.enabled:
            return f"socks5://{self.socks_host}:{self.socks_port}"
        return None


# ====================================================================== #
# Tor auto-start (Windows)
# ====================================================================== #

def _find_tor_exe() -> Optional[str]:
    """Locate tor.exe from Tor Browser installation (Windows)."""
    profile = os.environ.get("USERPROFILE", "")
    candidates = [
        Path(profile) / "OneDrive" / "Desktop" / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
        Path(profile) / "Desktop" / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
        Path("C:/TorProxy/tor.exe"),
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return None


def _auto_start_tor(cfg: TorConfig) -> bool:
    """Auto-start a standalone Tor daemon if not already running."""
    tor_exe = _find_tor_exe()
    if not tor_exe:
        print("[TOR] Cannot auto-start: tor.exe not found")
        print("[TOR] Install Tor Browser or place tor.exe in C:\\TorProxy\\")
        return False

    torrc_dir = Path("C:/TorProxy")
    torrc_dir.mkdir(parents=True, exist_ok=True)
    (torrc_dir / "data").mkdir(parents=True, exist_ok=True)

    torrc = torrc_dir / "torrc"
    torrc.write_text(
        f"DataDirectory C:\\TorProxy\\data\n"
        f"SocksPort {cfg.socks_port}\n"
        f"ControlPort {cfg.control_port}\n"
        f"CookieAuthentication 1\n",
        encoding="utf-8",
    )

    print(f"[TOR] Starting Tor daemon: {tor_exe}")
    print(f"[TOR] Config: SOCKS={cfg.socks_port}, Control={cfg.control_port}")
    try:
        # DETACHED_PROCESS = 0x00000008 (Windows)
        creation_flags = 0x00000008 if sys.platform == "win32" else 0
        subprocess.Popen(
            [tor_exe, "-f", str(torrc)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creation_flags,
        )
    except Exception as e:
        print(f"[TOR] Failed to start tor.exe: {e}")
        return False

    # Wait for cookie file to be created (Tor creates it after starting)
    cookie_file = cfg.control_cookie_file or "C:\\TorProxy\\data\\control_auth_cookie"
    cookie_path = Path(cookie_file)
    cookie_wait_start = time.time()
    while time.time() - cookie_wait_start < 10:
        if cookie_path.exists():
            break
        time.sleep(0.5)
    else:
        print(f"[TOR] Warning: Cookie file not created after 10s, continuing anyway...")

    print("[TOR] Waiting for Tor to bootstrap (up to 90s)...")
    if wait_tor_ready(
        cfg.control_host,
        cfg.control_port,
        cookie_file=cookie_file,
        password=cfg.control_password,
        timeout_seconds=90,
    ):
        print("[TOR] Tor bootstrapped successfully!")
        return True

    print("[TOR] Tor failed to bootstrap within 90s")
    return False


# ====================================================================== #
# setup_tor — main entry point
# ====================================================================== #

def setup_tor(cfg: TorConfig) -> Optional[str]:
    """
    Check / auto-start Tor and return the SOCKS5 proxy URL for httpx.

    Returns:
        "socks5://host:port" if Tor is available, None if disabled/unavailable.
    Raises:
        RuntimeError if cfg.require is True and Tor cannot be started.
    """
    if not cfg.enabled:
        print("[TOR] Tor proxy disabled (TOR_ENABLED=0)")
        return None

    # ---- Check if already running ----
    if is_port_open(cfg.socks_host, cfg.socks_port):
        pct = tor_bootstrap_percent(
            cfg.control_host,
            cfg.control_port,
            cookie_file=cfg.control_cookie_file,
            password=cfg.control_password,
        )
        if pct == 100:
            exit_ip = get_public_ip_via_socks(cfg.socks_host, cfg.socks_port)
            print(f"[TOR] Tor running (bootstrap 100%, exit IP: {exit_ip})")
            return cfg.proxy_url
        elif pct > 0:
            print(f"[TOR] Tor bootstrapping ({pct}%), waiting...")
            if wait_tor_ready(
                cfg.control_host,
                cfg.control_port,
                cookie_file=cfg.control_cookie_file,
                password=cfg.control_password,
                timeout_seconds=60,
            ):
                exit_ip = get_public_ip_via_socks(cfg.socks_host, cfg.socks_port)
                print(f"[TOR] Tor ready (exit IP: {exit_ip})")
                return cfg.proxy_url

    # ---- Not running — try auto-start ----
    if cfg.auto_start:
        if _auto_start_tor(cfg):
            exit_ip = get_public_ip_via_socks(cfg.socks_host, cfg.socks_port)
            print(f"[TOR] Tor auto-started (exit IP: {exit_ip})")
            return cfg.proxy_url

    # ---- Failed ----
    if cfg.require:
        raise RuntimeError(
            f"[TOR] Tor SOCKS5 not available at {cfg.socks_host}:{cfg.socks_port} "
            f"and auto-start failed. Start Tor manually with proxies/start_tor_proxy_9050.bat (or the port matching TOR_SOCKS_PORT). See proxies/TOR_PORT_MAPPING.md"
        )

    print("[TOR] Tor not available — proceeding without proxy")
    return None


# ====================================================================== #
# TorRotator — background NEWNYM rotation for async scrapers
# ====================================================================== #

class TorRotator:
    """
    Periodically sends SIGNAL NEWNYM to rotate the Tor exit circuit.

    For httpx scrapers, rotation is transparent — new TCP connections
    automatically use the new circuit. No need to stop/restart workers
    (unlike Selenium where browsers must be recycled).

    Usage::

        rotator = TorRotator(tor_cfg)
        rotator.start()
        # ... do async scraping ...
        rotator.stop()
    """

    def __init__(self, cfg: TorConfig):
        self.cfg = cfg
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self.rotation_count = 0

    def start(self) -> None:
        """Start background NEWNYM rotation loop."""
        if not self.cfg.newnym_enabled:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._rotation_loop())
        print(
            f"[TOR] NEWNYM rotation started "
            f"(every {self.cfg.newnym_interval_seconds}s)"
        )

    def stop(self) -> None:
        """Stop the rotation loop."""
        if self._task:
            self._stop_event.set()
            self._task.cancel()
            self._task = None

    async def _rotation_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                # Sleep in small increments so we can check stop event
                for _ in range(self.cfg.newnym_interval_seconds):
                    if self._stop_event.is_set():
                        return
                    await asyncio.sleep(1)

                old_ip = get_public_ip_via_socks(
                    self.cfg.socks_host, self.cfg.socks_port
                )

                result = tor_signal_newnym(
                    self.cfg.control_host,
                    self.cfg.control_port,
                    cookie_file=self.cfg.control_cookie_file,
                    password=self.cfg.control_password,
                    cooldown_seconds=self.cfg.newnym_cooldown_seconds,
                )

                self.rotation_count += 1
                new_ip = get_public_ip_via_socks(
                    self.cfg.socks_host, self.cfg.socks_port
                )

                if result.ok:
                    print(f"[TOR] NEWNYM #{self.rotation_count}: {old_ip} -> {new_ip}")
                else:
                    print(f"[TOR] NEWNYM #{self.rotation_count} failed (IP: {new_ip})")

        except asyncio.CancelledError:
            pass


# ====================================================================== #
# AsyncRateLimiter — enforce max requests per minute
# ====================================================================== #

class AsyncRateLimiter:
    """
    Token-bucket rate limiter for async scrapers.

    Usage::

        limiter = AsyncRateLimiter(max_per_minute=200)

        async def worker():
            await limiter.acquire()
            # ... make request ...
    """

    def __init__(self, max_per_minute: int = 200):
        self.max_per_minute = max_per_minute
        self._interval = 60.0 / max_per_minute  # seconds between requests
        self._lock = asyncio.Lock()
        self._last_request_time = 0.0
        self.total_requests = 0

    async def acquire(self) -> None:
        """Wait until a request slot is available."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self._interval:
                await asyncio.sleep(self._interval - elapsed)
            self._last_request_time = time.monotonic()
            self.total_requests += 1
