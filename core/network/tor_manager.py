"""
Core module for managing Tor Browser, Tor Proxy, and Firefox Selenium drivers with Tor.
Extracted from script-specific implementations (Belarus, Argentina) to centralize logic.
"""

import sys
import os
import time
import socket
import logging
import random
import subprocess
import shutil
from pathlib import Path
from typing import Optional, Tuple

from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService

try:
    from webdriver_manager.firefox import GeckoDriverManager
    GECKO_AVAILABLE = True
except ImportError:
    GECKO_AVAILABLE = False

try:
    from core.browser.chrome_instance_tracker import ChromeInstanceTracker
    from core.db.postgres_connection import PostgresDB
    HAS_DB = True
except ImportError:
    HAS_DB = False
    ChromeInstanceTracker = None
    PostgresDB = None

from core.config.config_manager import ConfigManager

def getenv(key: str, default: str = None) -> str:
    return ConfigManager.get_env_value("tor_manager", key, default)

def getenv_bool(key: str, default: bool = False) -> bool:
    val = getenv(key)
    if not val:
        return default
    return str(val).lower() in ("true", "1", "yes", "on")

# Configure logger
logger = logging.getLogger(__name__)


def check_tor_running(host="127.0.0.1", timeout=2) -> Tuple[bool, Optional[int]]:
    """Check if Tor SOCKS5 proxy is running. Returns (bool, port)."""
    for port in [9150, 9050]:  # Tor Browser, Tor service
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                logger.info(f"Tor detected on {host}:{port}")
                return True, port
        except Exception:
            continue
    return False, None


def get_tor_data_dir() -> Path:
    """Get platform-appropriate Tor data directory."""
    if sys.platform == "win32":
        return Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")) / "TorProxy"
    elif sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "TorProxy"
    else:
        return Path.home() / ".tor_proxy"


def auto_start_tor_proxy() -> bool:
    """Best-effort auto-start Tor daemon on 9050."""
    logger.info("Attempting to auto-start Tor proxy...")
    if check_tor_running(timeout=1)[0]:
        return True
    
    home = Path.home()

    # Platform-aware Tor executable search
    tor_exe_candidates = []
    if sys.platform == "win32":
        tor_exe_candidates = [
            home / "OneDrive" / "Desktop" / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
            home / "Desktop" / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
            Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
        ]
    else:
        # Linux / macOS
        tor_exe_candidates = [
            home / "tor-browser" / "Browser" / "TorBrowser" / "Tor" / "tor",
            home / ".local" / "share" / "torbrowser" / "tbb" / "tor",
            Path("/usr/bin/tor"),
            Path("/usr/local/bin/tor"),
        ]
        # Also check PATH
        tor_in_path = shutil.which("tor")
        if tor_in_path:
            tor_exe_candidates.append(Path(tor_in_path))

    tor_exe = next((p for p in tor_exe_candidates if p.exists()), None)
    if not tor_exe:
        logger.error("Could not find Tor executable on this platform")
        return False

    tor_data_dir = get_tor_data_dir()
    torrc = tor_data_dir / "torrc"
    data_dir = tor_data_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    torrc.parent.mkdir(parents=True, exist_ok=True)

    # Platform-aware path separators in torrc content
    data_dir_str = str(data_dir).replace("\\", "/")  # Tor uses forward slashes even on Windows
    try:
        torrc.write_text(
            f"DataDirectory {data_dir_str}\nSocksPort 9050\nControlPort 9051\nCookieAuthentication 1\n",
            encoding="ascii"
        )
    except Exception as e:
        logger.error(f"Failed to write torrc: {e}")
        return False
    
    try:
        creation_flags = 0
        if hasattr(subprocess, "CREATE_NO_WINDOW"):
            creation_flags = subprocess.CREATE_NO_WINDOW
        subprocess.Popen([str(tor_exe), "-f", str(torrc)], creationflags=creation_flags)
        
        # Wait for startup
        for _ in range(30):
            time.sleep(1)
            if check_tor_running(timeout=1)[0]:
                logger.info("Tor started successfully")
                return True
    except Exception as e:
        logger.error(f"Failed to start Tor process: {e}")
    
    return False


def find_firefox_binary() -> Optional[str]:
    """Find Firefox or Tor Browser binary (cross-platform)."""
    firefox_bin = os.getenv("FIREFOX_BINARY", "")
    if firefox_bin and Path(firefox_bin).exists():
        return str(Path(firefox_bin).resolve())

    home = Path.home()
    paths = []

    if sys.platform == "win32":
        userprofile = os.environ.get("USERPROFILE", str(home))
        paths = [
            Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "Tor Browser" / "Browser" / "firefox.exe",
            Path(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")) / "Tor Browser" / "Browser" / "firefox.exe",
            Path(os.environ.get("LOCALAPPDATA", "")) / "Tor Browser" / "Browser" / "firefox.exe",
            Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "Mozilla Firefox" / "firefox.exe",
            Path(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")) / "Mozilla Firefox" / "firefox.exe",
            Path(userprofile) / "Desktop" / "Tor Browser" / "Browser" / "firefox.exe",
            Path(userprofile) / "OneDrive" / "Desktop" / "Tor Browser" / "Browser" / "firefox.exe",
        ]
    elif sys.platform == "darwin":
        paths = [
            Path("/Applications/Tor Browser.app/Contents/MacOS/firefox"),
            home / "Applications" / "Tor Browser.app" / "Contents" / "MacOS" / "firefox",
            Path("/Applications/Firefox.app/Contents/MacOS/firefox"),
        ]
    else:
        # Linux
        paths = [
            home / "tor-browser" / "Browser" / "firefox",
            home / ".local" / "share" / "torbrowser" / "tbb" / "firefox",
            Path("/usr/bin/firefox"),
            Path("/usr/lib/firefox/firefox"),
            Path("/snap/bin/firefox"),
        ]

    for p in paths:
        if p.exists():
            return str(p.resolve())
    if shutil.which("firefox"):
        return shutil.which("firefox")
    return None


def inject_stealth_script(driver):
    """
    Inject stealth/antibot JavaScript to hide webdriver properties.
    Makes browser appear more human-like to anti-bot systems.
    """
    stealth_script = """
    // Hide webdriver property
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });
    
    // Mock plugins array (Chrome-like)
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const plugins = [
                {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format'},
                {name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: ''},
                {name: 'Native Client', filename: 'internal-nacl-plugin', description: ''}
            ];
            plugins.length = 3;
            return plugins;
        },
        configurable: true
    });
    
    // Mock languages
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
        configurable: true
    });
    
    // Mock permissions query
    if (navigator.permissions) {
        const origQuery = navigator.permissions.query.bind(navigator.permissions);
        navigator.permissions.query = (params) => {
            if (params.name === 'notifications') {
                return Promise.resolve({state: 'denied'});
            }
            return origQuery(params);
        };
    }
    """
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": stealth_script})
    except Exception:
        # Fallback for Firefox/non-CDP
        try:
            driver.execute_script(stealth_script)
        except Exception:
            pass


def build_driver_firefox_tor(
    show_browser: bool = True,
    disable_images: bool = True,
    disable_css: bool = True,
    tor_proxy_port: Optional[int] = None,
    run_id: Optional[str] = None,
    scraper_name: str = "Unknown"
) -> webdriver.Firefox:
    """
    Build Firefox driver with Tor proxy configuration.
    
    Args:
        show_browser: Whether to show the browser window (True) or run headless (False)
        disable_images: Whether to disable image loading
        disable_css: Whether to disable CSS loading
        tor_proxy_port: Specific Tor SOCKS port to use. If None, auto-detects.
        run_id: Run ID for tracking browser instances in DB.
        scraper_name: Scraper name for DB tracking.
    """
    if not GECKO_AVAILABLE:
        raise RuntimeError("GeckoDriverManager not available. pip install webdriver-manager")

    # Auto-detect port if not provided
    if tor_proxy_port is None:
        running, port = check_tor_running()
        if running:
            tor_proxy_port = port
        else:
            logger.warning("Tor not detected, attempting auto-start...")
            if auto_start_tor_proxy():
                running, port = check_tor_running()
                if running:
                    tor_proxy_port = port
            
    if tor_proxy_port is None:
        logger.warning("Tor NOT running. Driver will likely fail connection if Tor is required.")

    opts = FirefoxOptions()
    if not show_browser:
        opts.add_argument("--headless")

    opts.set_preference("browser.cache.disk.enable", False)
    opts.set_preference("browser.cache.memory.enable", False)
    opts.set_preference("permissions.default.image", 2 if disable_images else 1)
    opts.set_preference("permissions.default.stylesheet", 2 if disable_css else 1)
    
    # Prefer English language so page loads in English (translate in browser before scraping)
    opts.set_preference("intl.accept_languages", "en-US,en,ru-RU,ru")
    opts.set_preference("general.useragent.locale", "en-US")
    opts.set_preference("dom.webnotifications.enabled", False)

    if tor_proxy_port:
        opts.set_preference("network.proxy.type", 1)
        opts.set_preference("network.proxy.socks", "127.0.0.1")
        opts.set_preference("network.proxy.socks_port", int(tor_proxy_port))
        opts.set_preference("network.proxy.socks_version", 5)
        # Remote DNS is critical for .onion + anonymity
        opts.set_preference("network.proxy.socks_remote_dns", True)
        logger.info(f"Configuring Firefox to use Tor proxy on port {tor_proxy_port}")
    else:
        opts.set_preference("network.proxy.type", 0)

    firefox_bin = find_firefox_binary()
    if firefox_bin:
        opts.binary_location = firefox_bin
    
    gecko_path = GeckoDriverManager().install()
    service = FirefoxService(gecko_path)
    
    driver = webdriver.Firefox(service=service, options=opts)
    
    # Inject stealth/antibot script
    inject_stealth_script(driver)

    # Track Firefox PIDs using standardized ChromeInstanceTracker (works for Firefox too)
    if ChromeInstanceTracker and HAS_DB and run_id and hasattr(driver, "service") and hasattr(driver.service, "process"):
        try:
            pid = driver.service.process.pid
            if pid:
                try:
                    # We need to manually manage the connection for the tracker
                    db = PostgresDB(scraper_name)
                    db.connect()
                    tracker = ChromeInstanceTracker(scraper_name, run_id, db)
                    tracker.register(step_number=1, pid=pid, browser_type="firefox")
                    db.close()
                except Exception as e:
                    logger.warning(f"Failed to track Firefox PID {pid}: {e}")
        except Exception:
            pass
            
    return driver
