import os
import shutil
import socket
import logging
from pathlib import Path
from typing import Callable, Optional, Tuple
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions

log = logging.getLogger(__name__)

# Try to use core tracking if available
try:
    from core.browser.chrome_manager import register_chrome_driver, unregister_chrome_driver, cleanup_all_chrome_instances
    _CHROME_MANAGER_AVAILABLE = True
except ImportError:
    _CHROME_MANAGER_AVAILABLE = False
    def register_chrome_driver(*args): pass
    def unregister_chrome_driver(*args): pass
    def cleanup_all_chrome_instances(*args): pass

def get_chromedriver_path() -> str:
    """Get ChromeDriver path with offline fallback"""
    import glob
    
    home = Path.home()
    wdm_cache_dir = home / ".wdm" / "drivers" / "chromedriver"
    
    # Try cached driver first
    if wdm_cache_dir.exists():
        patterns = [
            str(wdm_cache_dir / "**" / "chromedriver.exe"),
            str(wdm_cache_dir / "**" / "chromedriver"),
        ]
        for pattern in patterns:
            matches = glob.glob(pattern, recursive=True)
            if matches:
                matches.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                return matches[0]
    
    # Try to download
    try:
        return ChromeDriverManager().install()
    except Exception:
        pass
    
    # System chromedriver
    chromedriver_in_path = shutil.which("chromedriver")
    if chromedriver_in_path:
        return chromedriver_in_path
    
    raise RuntimeError("ChromeDriver not found")

def resolve_driver_path(driver_name: str = "chromedriver") -> str:
    """
    Resolve driver path with strict prioritized checking:
    1. Env var strict path (CHROMEDRIVER_PATH) - supports WDAC/AppLocker
    2. Local tools/ directory
    3. System PATH
    4. WebDriverManager (fallback)
    """
    # 1. Strict Env Var
    env_path = os.environ.get(f"{driver_name.upper()}_PATH", "").strip().strip('"')
    if env_path:
        p = Path(env_path)
        if p.exists():
            log.info(f"Using strict {driver_name} path from env: {p}")
            return str(p)
        log.warning(f"Env var {driver_name.upper()}_PATH set to {env_path} but file not found")

    # 2. Local tools directory (common convention)
    # Assumes we are in core/browser/driver_factory.py -> jump up 3 levels to repo root
    try:
        repo_root = Path(__file__).resolve().parents[2]
        local_tool = repo_root / "tools" / (f"{driver_name}.exe" if os.name == 'nt' else driver_name)
        if local_tool.exists():
            log.info(f"Using local {driver_name} from tools: {local_tool}")
            return str(local_tool)
    except Exception:
        pass

    # 3. System PATH
    system_path = shutil.which(driver_name)
    if system_path:
        log.info(f"Using system {driver_name}: {system_path}")
        return system_path

    # 4. WebDriverManager
    try:
        if "chrome" in driver_name:
            p = ChromeDriverManager().install()
            log.info(f"Using managed {driver_name}: {p}")
            return p
        elif "gecko" in driver_name:
            p = GeckoDriverManager().install()
            log.info(f"Using managed {driver_name}: {p}")
            return p
    except Exception as e:
        log.warning(f"WebDriverManager failed for {driver_name}: {e}")

    raise RuntimeError(f"Could not resolve path for {driver_name}")

def create_chrome_driver(headless: bool = True, proxy_args: dict = {}, extra_options: dict = {}) -> webdriver.Chrome:
    """
    Create a Chrome driver with stealth and performance options.
    """
    opts = ChromeOptions()
    
    if headless:
        opts.add_argument("--headless=new")
    
    # Basic options
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    
    # Critical for stability
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-features=IsolateOrigins,site-per-process")
    opts.add_argument("--single-process")
    opts.add_argument("--memory-model=low")
    
    # Binary location
    chrome_bin = os.environ.get("CHROME_BINARY", r"C:\Program Files\Google\Chrome\Application\chrome.exe")
    if os.path.exists(chrome_bin):
         opts.binary_location = chrome_bin

    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option('useAutomationExtension', False)
    
    # User agent
    if 'user_agent' in extra_options:
        opts.add_argument(f"--user-agent={extra_options['user_agent']}")
    
    # Disable images for speed
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    opts.add_experimental_option("prefs", prefs)
    
    try:
        driver_path = resolve_driver_path("chromedriver")
        service = ChromeService(driver_path)
        driver = webdriver.Chrome(service=service, options=opts)
    except Exception as e:
        log.error(f"Failed to create Chrome driver: {e}")
        # Try fallback without service (let Selenium handle it)
        driver = webdriver.Chrome(options=opts)
    
    # Set timeouts
    if 'page_load_timeout' in extra_options:
         driver.set_page_load_timeout(extra_options['page_load_timeout'])
    
    # CDP anti-detection
    try:
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en', 'ru-RU', 'ru'] });
                window.chrome = { runtime: {} };
            '''
        })
    except Exception:
        pass
    
    if _CHROME_MANAGER_AVAILABLE:
        register_chrome_driver(driver)
        
    return driver

def create_firefox_driver(headless: bool = True, tor_config: dict = {}, extra_prefs: dict = {}) -> webdriver.Firefox:
    """Create a Firefox WebDriver instance, optionally with Tor proxy support."""
    options = FirefoxOptions()
    if headless:
        options.add_argument("--headless")
    
    profile = webdriver.FirefoxProfile()
    
    # Disable notifications and popups
    profile.set_preference("dom.webnotifications.enabled", False)
    profile.set_preference("dom.push.enabled", False)
    profile.set_preference("permissions.default.desktop-notification", 2)

    # Disable images, CSS, and fonts for speed (default)
    # can be overridden by extra_prefs
    profile.set_preference("permissions.default.image", 2)
    profile.set_preference("permissions.default.stylesheet", 2)
    profile.set_preference("browser.display.use_document_fonts", 0)
    profile.set_preference("gfx.downloadable_fonts.enabled", False)

    # Tor proxy configuration
    if tor_config.get("enabled"):
        port = tor_config.get("port", 9050)
        profile.set_preference("network.proxy.type", 1)
        profile.set_preference("network.proxy.socks", "127.0.0.1")
        profile.set_preference("network.proxy.socks_port", port)
        profile.set_preference("network.proxy.socks_version", 5)
        profile.set_preference("network.proxy.socks_remote_dns", True)
        log.info(f"[TOR_CONFIG] Using Tor proxy on port {port}")
    
    # Apply extra preferences
    for key, value in extra_prefs.items():
        profile.set_preference(key, value)
    
    options.profile = profile
    options.page_load_strategy = "eager"
    
    try:
        driver_path = resolve_driver_path("geckodriver")
        service = FirefoxService(driver_path)
        driver = webdriver.Firefox(service=service, options=options)
    except Exception as e:
         log.error(f"Failed to create Firefox driver: {e}")
         driver = webdriver.Firefox(options=options)
    
    driver.set_page_load_timeout(120)
    driver.implicitly_wait(10)
    
    return driver

def restart_driver(driver: webdriver.Remote, factory_func: Callable[[], webdriver.Remote]) -> webdriver.Remote:
    """Restart driver with cleanup"""
    log.info("[DRIVER] Restarting browser...")
    try:
        driver.quit()
        if _CHROME_MANAGER_AVAILABLE and isinstance(driver, webdriver.Chrome):
             unregister_chrome_driver(driver)
    except Exception:
        pass
    return factory_func()

def cleanup_drivers():
    """Cleanup all tracked drivers"""
    if _CHROME_MANAGER_AVAILABLE:
        cleanup_all_chrome_instances()
