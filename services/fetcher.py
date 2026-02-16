#!/usr/bin/env python3
"""
Unified Fetch Abstraction Layer for the Scraping Platform.

This module provides a single entry point for all HTTP/Browser fetching:
- HTTP Stealth (curl_cffi / Stealth-Requests)
- Selenium
- Playwright
- API requests
- TOR routing

The fetcher automatically:
- Chooses the best method based on country and URL
- Falls back to browser if HTTP fails
- Logs all fetch operations
- Handles retries with exponential backoff
- Rotates user agents and proxies

Usage:
    from services.fetcher import fetch, FetchResult
    
    result = fetch(url, country="India")
    if result.success:
        html = result.content
    else:
        print(f"Failed: {result.error}")
"""

import os
import sys
import time
import random
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from enum import Enum
from urllib.parse import urlparse

# Add parent directories to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

log = logging.getLogger(__name__)


# =============================================================================
# Fetch Method Enum
# =============================================================================

class FetchMethod(Enum):
    """Available fetch methods."""
    HTTP = "http"
    HTTP_STEALTH = "http_stealth"
    SELENIUM = "selenium"
    PLAYWRIGHT = "playwright"
    API = "api"
    TOR = "tor"
    SCRAPY = "scrapy"


# =============================================================================
# Fetch Result
# =============================================================================

@dataclass
class FetchResult:
    """Result of a fetch operation."""
    url: str
    success: bool
    content: Optional[str] = None
    content_bytes: Optional[bytes] = None
    status_code: Optional[int] = None
    headers: Dict[str, str] = field(default_factory=dict)
    method_used: FetchMethod = FetchMethod.HTTP
    latency_ms: int = 0
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    fallback_used: bool = False
    retry_count: int = 0
    content_hash: Optional[str] = None
    
    def __post_init__(self):
        """Compute content hash if content available."""
        if self.content and not self.content_hash:
            self.content_hash = hashlib.md5(self.content.encode()).hexdigest()
        elif self.content_bytes and not self.content_hash:
            self.content_hash = hashlib.md5(self.content_bytes).hexdigest()


# =============================================================================
# User Agent Pool
# =============================================================================

USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    # Chrome Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    # Firefox Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


def get_random_user_agent() -> str:
    """Get a random user agent."""
    return random.choice(USER_AGENTS)


# =============================================================================
# Country-Specific Configuration
# =============================================================================

# Countries that require TOR
TOR_COUNTRIES = {"Argentina"}

# Countries that have API access
API_COUNTRIES = {"India"}

# Countries that require browser for most pages
BROWSER_REQUIRED_COUNTRIES = {"Russia", "Taiwan"}

# Default fetch order per country
COUNTRY_FETCH_ORDER: Dict[str, List[FetchMethod]] = {
    "Argentina": [FetchMethod.TOR, FetchMethod.SELENIUM, FetchMethod.PLAYWRIGHT],
    "India": [FetchMethod.API, FetchMethod.HTTP_STEALTH, FetchMethod.SELENIUM],
    "Russia": [FetchMethod.SELENIUM, FetchMethod.PLAYWRIGHT, FetchMethod.HTTP_STEALTH],
    "Taiwan": [FetchMethod.PLAYWRIGHT, FetchMethod.SELENIUM, FetchMethod.HTTP_STEALTH],
    "Malaysia": [FetchMethod.HTTP_STEALTH, FetchMethod.PLAYWRIGHT, FetchMethod.SELENIUM],
    "Netherlands": [FetchMethod.HTTP_STEALTH, FetchMethod.PLAYWRIGHT, FetchMethod.SELENIUM],
    "_default": [FetchMethod.HTTP_STEALTH, FetchMethod.PLAYWRIGHT, FetchMethod.SELENIUM],
}


def get_fetch_order(country: str) -> List[FetchMethod]:
    """Get the fetch method order for a country."""
    return COUNTRY_FETCH_ORDER.get(country, COUNTRY_FETCH_ORDER["_default"])


# =============================================================================
# Response Validator
# =============================================================================

# Cloudflare challenge markers
CLOUDFLARE_MARKERS = [
    "cf-browser-verification",
    "Checking your browser",
    "Enable JavaScript and cookies to continue",
    "Just a moment...",
    "_cf_chl_opt",
    "cf-spinner",
    "Attention Required! | Cloudflare",
]

# Captcha markers
CAPTCHA_MARKERS = [
    "captcha",
    "recaptcha",
    "hcaptcha",
    "g-recaptcha",
    "cf-turnstile",
    "arkose",
]

# Block page markers
BLOCK_MARKERS = [
    "Access Denied",
    "403 Forbidden",
    "Request blocked",
    "Too Many Requests",
    "Rate limit exceeded",
]


def validate_response(
    content: str,
    min_length: int = 1000,
    required_elements: Optional[List[str]] = None
) -> Tuple[bool, Optional[str]]:
    """
    Validate if a response is usable.
    
    Args:
        content: Response content
        min_length: Minimum content length
        required_elements: List of required HTML elements (e.g., ["<html", "<a "])
        
    Returns:
        (is_valid, error_reason)
    """
    if not content:
        return False, "empty_response"
    
    if len(content) < min_length:
        return False, "response_too_short"
    
    # Check for Cloudflare
    content_lower = content.lower()
    for marker in CLOUDFLARE_MARKERS:
        if marker.lower() in content_lower:
            return False, "cloudflare_challenge"
    
    # Check for captcha
    for marker in CAPTCHA_MARKERS:
        if marker.lower() in content_lower:
            return False, "captcha"
    
    # Check for block
    for marker in BLOCK_MARKERS:
        if marker.lower() in content_lower:
            return False, "blocked"
    
    # Check required elements
    if required_elements:
        for element in required_elements:
            if element.lower() not in content_lower:
                return False, f"missing_element:{element}"
    
    # Basic HTML check
    if "<html" not in content_lower:
        return False, "not_html"
    
    return True, None


# =============================================================================
# HTTP Fetcher (requests / curl_cffi / Stealth-Requests)
# =============================================================================

def _fetch_http(
    url: str,
    timeout: int = 30,
    headers: Optional[Dict] = None,
    proxies: Optional[Dict] = None,
    verify_ssl: bool = True,
    session: Optional[Any] = None
) -> FetchResult:
    """Basic HTTP fetch using requests."""
    import requests
    
    start_time = time.time()
    
    default_headers = {
        "User-Agent": get_random_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    
    if headers:
        default_headers.update(headers)
    
    try:
        client = session or requests
        response = client.get(
            url,
            headers=default_headers,
            timeout=timeout,
            proxies=proxies,
            verify=verify_ssl,
            allow_redirects=True
        )
        
        latency = int((time.time() - start_time) * 1000)
        
        return FetchResult(
            url=url,
            success=response.status_code == 200,
            content=response.text,
            content_bytes=response.content,
            status_code=response.status_code,
            headers=dict(response.headers),
            method_used=FetchMethod.HTTP,
            latency_ms=latency,
            error_type="http_error" if response.status_code != 200 else None,
            error_message=f"HTTP {response.status_code}" if response.status_code != 200 else None
        )
        
    except requests.Timeout:
        return FetchResult(
            url=url, success=False, method_used=FetchMethod.HTTP,
            error_type="timeout", error_message="Request timed out",
            latency_ms=int((time.time() - start_time) * 1000)
        )
    except requests.ConnectionError as e:
        return FetchResult(
            url=url, success=False, method_used=FetchMethod.HTTP,
            error_type="connection", error_message=str(e),
            latency_ms=int((time.time() - start_time) * 1000)
        )
    except Exception as e:
        return FetchResult(
            url=url, success=False, method_used=FetchMethod.HTTP,
            error_type="exception", error_message=str(e),
            latency_ms=int((time.time() - start_time) * 1000)
        )


def _fetch_http_stealth(
    url: str,
    timeout: int = 30,
    headers: Optional[Dict] = None,
    proxies: Optional[Dict] = None
) -> FetchResult:
    """
    HTTP fetch using curl_cffi for TLS fingerprint impersonation.
    Falls back to regular requests if curl_cffi not available.
    """
    start_time = time.time()
    
    try:
        from curl_cffi import requests as curl_requests
        
        default_headers = {
            "User-Agent": get_random_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        
        if headers:
            default_headers.update(headers)
        
        # Use Chrome impersonation
        response = curl_requests.get(
            url,
            headers=default_headers,
            timeout=timeout,
            proxies=proxies,
            impersonate="chrome120",
            allow_redirects=True
        )
        
        latency = int((time.time() - start_time) * 1000)
        
        return FetchResult(
            url=url,
            success=response.status_code == 200,
            content=response.text,
            content_bytes=response.content,
            status_code=response.status_code,
            headers=dict(response.headers),
            method_used=FetchMethod.HTTP_STEALTH,
            latency_ms=latency,
            error_type="http_error" if response.status_code != 200 else None,
            error_message=f"HTTP {response.status_code}" if response.status_code != 200 else None
        )
        
    except ImportError:
        log.debug("curl_cffi not available, falling back to regular requests")
        result = _fetch_http(url, timeout, headers, proxies)
        result.method_used = FetchMethod.HTTP_STEALTH  # Keep original intent
        return result
        
    except Exception as e:
        return FetchResult(
            url=url, success=False, method_used=FetchMethod.HTTP_STEALTH,
            error_type="exception", error_message=str(e),
            latency_ms=int((time.time() - start_time) * 1000)
        )


# =============================================================================
# TOR Fetcher
# =============================================================================

def _fetch_tor(
    url: str,
    timeout: int = 60,
    headers: Optional[Dict] = None,
    tor_port: int = 9050
) -> FetchResult:
    """Fetch via TOR SOCKS proxy."""
    proxies = {
        "http": f"socks5h://127.0.0.1:{tor_port}",
        "https": f"socks5h://127.0.0.1:{tor_port}",
    }
    
    result = _fetch_http(url, timeout, headers, proxies, verify_ssl=False)
    result.method_used = FetchMethod.TOR
    return result


# =============================================================================
# Selenium Fetcher
# =============================================================================

_selenium_driver = None
_selenium_lock = None


def _get_selenium_driver():
    """Get or create a Selenium WebDriver instance."""
    global _selenium_driver, _selenium_lock
    
    if _selenium_lock is None:
        import threading
        _selenium_lock = threading.Lock()
    
    with _selenium_lock:
        if _selenium_driver is None:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument(f"--user-agent={get_random_user_agent()}")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)
            
            _selenium_driver = webdriver.Chrome(options=options)
            _selenium_driver.set_page_load_timeout(60)
            
            # Apply stealth if available
            try:
                from selenium_stealth import stealth
                stealth(
                    _selenium_driver,
                    languages=["en-US", "en"],
                    vendor="Google Inc.",
                    platform="Win32",
                    webgl_vendor="Intel Inc.",
                    renderer="Intel Iris OpenGL Engine",
                    fix_hairline=True,
                )
            except ImportError:
                pass
        
        return _selenium_driver


def _fetch_selenium(
    url: str,
    timeout: int = 60,
    wait_for_selector: Optional[str] = None,
    wait_seconds: float = 2.0
) -> FetchResult:
    """Fetch using Selenium WebDriver."""
    start_time = time.time()
    
    try:
        driver = _get_selenium_driver()
        
        driver.get(url)
        
        # Wait for page load
        time.sleep(wait_seconds)
        
        # Wait for specific selector if provided
        if wait_for_selector:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            try:
                WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_selector))
                )
            except Exception:
                pass  # Continue anyway
        
        content = driver.page_source
        latency = int((time.time() - start_time) * 1000)
        
        return FetchResult(
            url=url,
            success=True,
            content=content,
            status_code=200,
            method_used=FetchMethod.SELENIUM,
            latency_ms=latency
        )
        
    except Exception as e:
        return FetchResult(
            url=url, success=False, method_used=FetchMethod.SELENIUM,
            error_type="selenium_error", error_message=str(e),
            latency_ms=int((time.time() - start_time) * 1000)
        )


def close_selenium():
    """Close the Selenium driver if open."""
    global _selenium_driver
    if _selenium_driver:
        try:
            _selenium_driver.quit()
        except Exception:
            pass
        _selenium_driver = None


# =============================================================================
# Playwright Fetcher
# =============================================================================

_playwright_instance = None
_playwright_browser = None
_playwright_lock = None


def _get_playwright_browser():
    """Get or create a Playwright browser instance."""
    global _playwright_instance, _playwright_browser, _playwright_lock
    
    if _playwright_lock is None:
        import threading
        _playwright_lock = threading.Lock()
    
    with _playwright_lock:
        if _playwright_browser is None:
            from playwright.sync_api import sync_playwright
            
            _playwright_instance = sync_playwright().start()
            _playwright_browser = _playwright_instance.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ]
            )
        
        return _playwright_browser


def _fetch_playwright(
    url: str,
    timeout: int = 60,
    wait_for_selector: Optional[str] = None,
    wait_seconds: float = 2.0
) -> FetchResult:
    """Fetch using Playwright."""
    start_time = time.time()
    
    try:
        browser = _get_playwright_browser()
        
        context = browser.new_context(
            user_agent=get_random_user_agent(),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        
        page = context.new_page()
        page.set_default_timeout(timeout * 1000)
        
        try:
            page.goto(url, wait_until="domcontentloaded")
            
            # Wait for specific selector if provided
            if wait_for_selector:
                try:
                    page.wait_for_selector(wait_for_selector, timeout=timeout * 1000)
                except Exception:
                    pass
            else:
                time.sleep(wait_seconds)
            
            content = page.content()
            latency = int((time.time() - start_time) * 1000)
            
            return FetchResult(
                url=url,
                success=True,
                content=content,
                status_code=200,
                method_used=FetchMethod.PLAYWRIGHT,
                latency_ms=latency
            )
            
        finally:
            page.close()
            context.close()
            
    except Exception as e:
        return FetchResult(
            url=url, success=False, method_used=FetchMethod.PLAYWRIGHT,
            error_type="playwright_error", error_message=str(e),
            latency_ms=int((time.time() - start_time) * 1000)
        )


def close_playwright():
    """Close the Playwright browser if open."""
    global _playwright_instance, _playwright_browser
    if _playwright_browser:
        try:
            _playwright_browser.close()
        except Exception:
            pass
        _playwright_browser = None
    if _playwright_instance:
        try:
            _playwright_instance.stop()
        except Exception:
            pass
        _playwright_instance = None


# =============================================================================
# Main Fetch Function
# =============================================================================

def fetch(
    url: str,
    country: str = "_default",
    method: Optional[FetchMethod] = None,
    timeout: int = 30,
    headers: Optional[Dict] = None,
    proxies: Optional[Dict] = None,
    validate: bool = True,
    min_length: int = 1000,
    required_elements: Optional[List[str]] = None,
    fallback: bool = True,
    max_retries: int = 2,
    wait_for_selector: Optional[str] = None,
    run_id: Optional[str] = None,
    log_to_db: bool = True,
    session: Optional[Any] = None
) -> FetchResult:
    """
    Unified fetch function with automatic method selection and fallback.
    
    Args:
        url: URL to fetch
        country: Country name (affects fetch method selection)
        method: Force a specific fetch method (default: auto-select)
        timeout: Request timeout in seconds
        headers: Additional headers
        proxies: Proxy configuration
        validate: Validate response content
        min_length: Minimum content length for validation
        required_elements: Required HTML elements for validation
        fallback: Enable fallback to other methods on failure
        max_retries: Maximum retries per method
        wait_for_selector: CSS selector to wait for (browser methods)
        run_id: Pipeline run ID for logging
        log_to_db: Log fetch to database
        
    Returns:
        FetchResult with content or error details
    """
    # Determine fetch order
    if method:
        methods = [method]
    else:
        methods = get_fetch_order(country)
    
    result = None
    fallback_used = False
    total_retries = 0
    
    for method_idx, fetch_method in enumerate(methods):
        if method_idx > 0:
            fallback_used = True
        
        for retry in range(max_retries + 1):
            total_retries = retry
            
            log.debug(f"Fetching {url} with {fetch_method.value} (retry {retry})")
            
            try:
                if fetch_method == FetchMethod.HTTP:
                    result = _fetch_http(url, timeout, headers, proxies, session=session)
                elif fetch_method == FetchMethod.HTTP_STEALTH:
                    result = _fetch_http_stealth(url, timeout, headers, proxies)
                elif fetch_method == FetchMethod.TOR:
                    result = _fetch_tor(url, timeout, headers)
                elif fetch_method == FetchMethod.SELENIUM:
                    result = _fetch_selenium(url, timeout, wait_for_selector)
                elif fetch_method == FetchMethod.PLAYWRIGHT:
                    result = _fetch_playwright(url, timeout, wait_for_selector)
                else:
                    result = _fetch_http(url, timeout, headers, proxies)
                
            except Exception as e:
                log.warning(f"Fetch error with {fetch_method.value}: {e}")
                result = FetchResult(
                    url=url, success=False, method_used=fetch_method,
                    error_type="exception", error_message=str(e)
                )
            
            # Update result metadata
            result.retry_count = total_retries
            result.fallback_used = fallback_used
            
            # Check if successful
            if result.success:
                # Validate if requested
                if validate and result.content:
                    is_valid, error_reason = validate_response(
                        result.content, min_length, required_elements
                    )
                    if not is_valid:
                        log.debug(f"Validation failed: {error_reason}")
                        result.success = False
                        result.error_type = "validation"
                        result.error_message = error_reason
                
                if result.success:
                    break
            
            # Exponential backoff on retry
            if retry < max_retries:
                wait_time = min(2 ** retry, 30)
                log.debug(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
        
        if result.success:
            break
        
        if not fallback:
            break
    
    # Log to database if requested
    if log_to_db:
        try:
            from services.db import log_fetch, get_url_id
            
            url_id = None
            try:
                url_id = get_url_id(url, country)
            except Exception:
                pass
            
            log_fetch(
                url=url,
                method=result.method_used.value,
                success=result.success,
                url_id=url_id,
                run_id=run_id,
                status_code=result.status_code,
                response_bytes=len(result.content_bytes) if result.content_bytes else None,
                latency_ms=result.latency_ms,
                error_type=result.error_type,
                error_message=result.error_message,
                retry_count=result.retry_count,
                fallback_used=result.fallback_used
            )
        except Exception as e:
            log.debug(f"Could not log fetch to DB: {e}")
    
    return result


def fetch_html(
    url: str,
    country: str = "_default",
    **kwargs
) -> Optional[str]:
    """
    Convenience function that returns just the HTML content or None.
    
    Args:
        url: URL to fetch
        country: Country name
        **kwargs: Additional arguments passed to fetch()
        
    Returns:
        HTML content string or None if failed
    """
    result = fetch(url, country, **kwargs)
    return result.content if result.success else None


def fetch_bytes(
    url: str,
    country: str = "_default",
    **kwargs
) -> Optional[bytes]:
    """
    Convenience function that returns just the bytes content or None.
    
    Args:
        url: URL to fetch
        country: Country name
        **kwargs: Additional arguments passed to fetch()
        
    Returns:
        Bytes content or None if failed
    """
    result = fetch(url, country, **kwargs)
    return result.content_bytes if result.success else None


# =============================================================================
# Cleanup
# =============================================================================

def cleanup():
    """Cleanup all browser instances."""
    close_selenium()
    close_playwright()


# Register cleanup on exit
import atexit
atexit.register(cleanup)
