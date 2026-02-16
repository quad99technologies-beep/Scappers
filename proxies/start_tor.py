#!/usr/bin/env python3
"""
Start Tor Proxy from Configuration
==================================

Usage:
    python scripts/proxies/start_tor.py --scraper <ScraperName>
    python scripts/proxies/start_tor.py --port 9050

Reads TOR_SOCKS_PORT and TOR_CONTROL_PORT from the scraper's configuration
and starts a Tor instance with those settings.
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.config.config_manager import ConfigManager

def find_tor_exe():
    """Find tor.exe in common locations."""
    user_profile = os.environ.get("USERPROFILE", "")
    candidates = [
        os.path.join(user_profile, r"OneDrive\Desktop\Tor Browser\Browser\TorBrowser\Tor\tor.exe"),
        os.path.join(user_profile, r"Desktop\Tor Browser\Browser\TorBrowser\Tor\tor.exe"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return None

def start_tor(port, control_port):
    """Start Tor process with specified ports."""
    tor_exe = find_tor_exe()
    if not tor_exe:
        print("[ERROR] tor.exe not found.")
        sys.exit(1)

    proxy_dir = Path(f"C:/TorProxy{port}")
    data_dir = proxy_dir / "data"
    torrc_file = proxy_dir / "torrc"

    # Create directories
    proxy_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    # write torrc
    with open(torrc_file, "w") as f:
        f.write(f"DataDirectory {data_dir}\n")
        f.write(f"SocksPort {port}\n")
        f.write(f"ControlPort {control_port}\n")
        f.write(f"CookieAuthentication 1\n")

    print(f"[INFO] Starting Tor Proxy ({port}/{control_port})...")
    print(f"[INFO]   tor.exe: {tor_exe}")
    print(f"[INFO]   torrc:   {torrc_file}")
    print(f"[INFO]   SOCKS5:  127.0.0.1:{port}")
    print(f"[INFO]   Control: 127.0.0.1:{control_port}")
    print("\n[INFO] Keep this window open while scraping.")
    print("[INFO] Wait for 'Bootstrapped 100%' before starting pipeline.\n")

    # Run tor
    cmd = [tor_exe, "-f", str(torrc_file)]
    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\n[INFO] Stopping Tor.")

def main():
    parser = argparse.ArgumentParser(description="Start Tor Proxy")
    parser.add_argument("--scraper", help="Scraper name to load config from")
    parser.add_argument("--port", type=int, help="Socks port (default 9050)")
    parser.add_argument("--control-port", type=int, help="Control port (default port+1)")

    args = parser.parse_args()

    socks_port = args.port or 9050
    control_port = args.control_port

    if args.scraper:
        try:
            ConfigManager.load_env(args.scraper)
            # Try to get from env (loaded by ConfigManager)
            # Note: ConfigManager loads into os.environ usually but let's check config value
            # Actually ConfigManager.load_env just loads into os.environ? 
            # Let's use get_config_value
            socks_val = ConfigManager.get_config_value(args.scraper, "TOR_SOCKS_PORT", None)
            if socks_val:
                socks_port = int(socks_val)
            
            ctrl_val = ConfigManager.get_config_value(args.scraper, "TOR_CONTROL_PORT", None)
            if ctrl_val:
                control_port = int(ctrl_val)

        except Exception as e:
            print(f"[WARN] Could not load config for {args.scraper}: {e}")
            print(f"[INFO] Falling back to default port {socks_port}")

    if not control_port:
        control_port = socks_port + 1

    start_tor(socks_port, control_port)

if __name__ == "__main__":
    main()
