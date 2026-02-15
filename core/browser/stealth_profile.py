"""
Standardized stealth/anti-bot profile for all scrapers.

Provides comprehensive anti-detection measures:
- Webdriver property hiding
- Mock plugins, languages, chrome runtime
- User agent rotation
- Automation flag disabling

EXCLUDES: Human-like typing simulation (not standardized)
"""

import os
import random

ENABLED = os.getenv("STEALTH_PROFILE_ENABLED", "true").lower() in ("1", "true", "yes", "on")

# Standard user agent pool (rotated randomly)
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# Stealth init script for Playwright (injected into every context)
_STEALTH_INIT_SCRIPT = """
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

// Mock chrome runtime
window.chrome = window.chrome || {};
window.chrome.runtime = window.chrome.runtime || {};
window.chrome.loadTimes = window.chrome.loadTimes || function() {
    return { commitLoadTime: Date.now() / 1000 };
};
window.chrome.csi = window.chrome.csi || function() {
    return { startE: Date.now(), onloadT: Date.now() };
};

// Mock permissions query
if (navigator.permissions) {
    const origQuery = navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.query = (params) => {
        if (params.name === 'notifications') {
            return Promise.resolve({state: Notification.permission});
        }
        return origQuery(params);
    };
}

// Remove Playwright-specific properties
delete window.__playwright;
delete window.__pw_manual;

// Remove Selenium-specific properties (if present)
delete window.__selenium_unwrapped;
delete window.__selenium_evaluate;
delete window.__fxdriver_unwrapped;
delete window.__driver_evaluate;
delete window.__webdriver_evaluate;
delete window.__selenium_IDE_recorder;
delete window.__selenium;
delete window._selenium;
delete window.calledSelenium;
delete window.$cdc_asdjflasutopfhvcZLmcfl_;
delete window.$chrome_asyncScriptInfo;
delete window.__$webdriverAsyncExecutor;
"""


def get_random_user_agent() -> str:
    """Get a random user agent from the pool."""
    return random.choice(_USER_AGENTS)


def get_stealth_init_script() -> str:
    """Get the stealth init script for Playwright contexts."""
    return _STEALTH_INIT_SCRIPT


def apply_selenium(options):
    """
    Apply stealth settings to Selenium Chrome options.
    
    Args:
        options: Selenium ChromeOptions instance
    """
    if not ENABLED:
        return
    
    # Disable automation detection
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Set language
    options.add_argument("--lang=en-US,en")
    
    # Realistic window size
    options.add_argument("--start-maximized")
    options.add_argument("--window-size=1920,1080")
    
    # Set random user agent
    options.add_argument(f"--user-agent={get_random_user_agent()}")
    
    # Additional stealth flags
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)


def apply_playwright(context_kwargs: dict):
    """
    Apply stealth settings to Playwright browser context.
    
    Args:
        context_kwargs: Dict to update with stealth context options
    """
    if not ENABLED:
        return
    
    # Set locale and timezone
    context_kwargs.update({
        "locale": "en-US",
        "timezone_id": "America/New_York",  # Standard timezone (can be overridden)
        "viewport": {"width": 1920, "height": 1080},  # Standard viewport
        "user_agent": get_random_user_agent(),  # Random user agent
    })
    
    # Note: Stealth init script should be injected separately via:
    # context.add_init_script(get_stealth_init_script())


def apply_playwright_with_script(context):
    """
    Apply stealth init script to a Playwright context.
    
    Args:
        context: Playwright BrowserContext instance
    """
    if not ENABLED:
        return
    context.add_init_script(_STEALTH_INIT_SCRIPT)