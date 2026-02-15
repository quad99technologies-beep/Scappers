"""
Hybrid Scraping Architecture: Browser + HTTP Client

Combines Selenium/Playwright for JavaScript/login with httpcloack for high-volume requests.
Extracts browser fingerprints and injects them into HTTP client for realistic requests.
"""

import logging
import time
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    from selenium import webdriver
    from selenium.webdriver.remote.webdriver import WebDriver
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    WebDriver = None

try:
    from playwright.sync_api import Page, BrowserContext
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    Page = None
    BrowserContext = None

try:
    import httpcloak
    HTTPCLOAK_AVAILABLE = True
except ImportError:
    HTTPCLOAK_AVAILABLE = False
    httpcloak = None

logger = logging.getLogger(__name__)


@dataclass
class BrowserFingerprint:
    """Extracted browser fingerprint from Selenium/Playwright session."""
    cookies: Dict[str, str]
    headers: Dict[str, str]
    user_agent: str
    viewport: Dict[str, int]
    timezone: str
    language: str
    platform: str
    tls_fingerprint: Optional[str] = None  # TLS fingerprint if available
    extracted_at: str = None
    
    def __post_init__(self):
        if self.extracted_at is None:
            self.extracted_at = datetime.utcnow().isoformat()
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'BrowserFingerprint':
        """Create from dictionary."""
        return cls(**data)


class BrowserFingerprintExtractor:
    """
    Extracts browser fingerprints from Selenium or Playwright sessions.
    
    Responsibilities:
    - Extract cookies, headers, user agent
    - Capture TLS fingerprint (if available)
    - Serialize for use with HTTP client
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
    
    def extract_from_selenium(self, driver: WebDriver) -> BrowserFingerprint:
        """
        Extract fingerprint from Selenium WebDriver session.
        
        Args:
            driver: Selenium WebDriver instance
            
        Returns:
            BrowserFingerprint object
        """
        self.logger.info("[FINGERPRINT] Extracting from Selenium session...")
        
        # Extract cookies
        selenium_cookies = driver.get_cookies()
        cookies = {cookie['name']: cookie['value'] for cookie in selenium_cookies}
        self.logger.debug(f"[FINGERPRINT] Extracted {len(cookies)} cookies")
        
        # Extract user agent
        user_agent = driver.execute_script("return navigator.userAgent;")
        
        # Extract viewport
        viewport_width = driver.execute_script("return window.innerWidth;")
        viewport_height = driver.execute_script("return window.innerHeight;")
        viewport = {"width": viewport_width, "height": viewport_height}
        
        # Extract timezone
        timezone = driver.execute_script("return Intl.DateTimeFormat().resolvedOptions().timeZone;")
        
        # Extract language
        language = driver.execute_script("return navigator.language;")
        
        # Extract platform
        platform = driver.execute_script("return navigator.platform;")
        
        # Extract headers (approximate from browser capabilities)
        headers = {
            "Accept": driver.execute_script("return navigator.mimeTypes.length > 0 ? 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' : '';") or "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": language,
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
        
        fingerprint = BrowserFingerprint(
            cookies=cookies,
            headers=headers,
            user_agent=user_agent,
            viewport=viewport,
            timezone=timezone,
            language=language,
            platform=platform
        )
        
        self.logger.info(f"[FINGERPRINT] Extraction complete: UA={user_agent[:50]}...")
        return fingerprint
    
    def extract_from_playwright(self, page: Page) -> BrowserFingerprint:
        """
        Extract fingerprint from Playwright page session.
        
        Args:
            page: Playwright Page instance
            
        Returns:
            BrowserFingerprint object
        """
        self.logger.info("[FINGERPRINT] Extracting from Playwright session...")
        
        # Extract cookies
        playwright_cookies = page.context.cookies()
        cookies = {cookie['name']: cookie['value'] for cookie in playwright_cookies}
        self.logger.debug(f"[FINGERPRINT] Extracted {len(cookies)} cookies")
        
        # Extract user agent
        user_agent = page.evaluate("() => navigator.userAgent")
        
        # Extract viewport
        viewport = page.viewport_size or {"width": 1366, "height": 768}
        
        # Extract timezone
        timezone = page.evaluate("() => Intl.DateTimeFormat().resolvedOptions().timeZone")
        
        # Extract language
        language = page.evaluate("() => navigator.language")
        
        # Extract platform
        platform = page.evaluate("() => navigator.platform")
        
        # Extract headers from context
        context = page.context
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": language,
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
        
        # Add any extra headers from context
        if hasattr(context, '_options') and 'extra_http_headers' in context._options:
            headers.update(context._options['extra_http_headers'])
        
        fingerprint = BrowserFingerprint(
            cookies=cookies,
            headers=headers,
            user_agent=user_agent,
            viewport=viewport,
            timezone=timezone,
            language=language,
            platform=platform
        )
        
        self.logger.info(f"[FINGERPRINT] Extraction complete: UA={user_agent[:50]}...")
        return fingerprint
    
    def save_fingerprint(self, fingerprint: BrowserFingerprint, file_path: Path):
        """Save fingerprint to JSON file."""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(fingerprint.to_dict(), f, indent=2)
        self.logger.info(f"[FINGERPRINT] Saved to {file_path}")
    
    def load_fingerprint(self, file_path: Path) -> Optional[BrowserFingerprint]:
        """Load fingerprint from JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return BrowserFingerprint.from_dict(data)
        except Exception as e:
            self.logger.warning(f"[FINGERPRINT] Failed to load from {file_path}: {e}")
            return None


class HybridHttpClient:
    """
    HTTP client using httpcloack with injected browser fingerprints.
    
    Responsibilities:
    - Create httpcloack client with realistic fingerprints
    - Inject cookies, headers, user agent from browser session
    - Handle session refresh and rotation
    - Provide high-volume request capabilities
    """
    
    def __init__(
        self,
        fingerprint: Optional[BrowserFingerprint] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize hybrid HTTP client.
        
        Args:
            fingerprint: Browser fingerprint to inject
            logger: Optional logger instance
        """
        if not HTTPCLOAK_AVAILABLE:
            raise ImportError("httpcloak is not installed. Install it with: pip install httpcloak")
        
        self.logger = logger or logging.getLogger(__name__)
        self.fingerprint = fingerprint
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize httpcloack client with fingerprint."""
        if self.fingerprint is None:
            # Create client with default realistic fingerprint
            self.client = httpcloak.Client()
            self.logger.info("[HTTP] Created httpcloack client with default fingerprint")
        else:
            # Create client with custom fingerprint
            config = {
                "user_agent": self.fingerprint.user_agent,
                "viewport": self.fingerprint.viewport,
                "timezone": self.fingerprint.timezone,
                "language": self.fingerprint.language,
                "platform": self.fingerprint.platform,
            }
            
            self.client = httpcloak.Client(**config)
            
            # Set cookies
            if self.fingerprint.cookies:
                for name, value in self.fingerprint.cookies.items():
                    self.client.cookies.set(name, value)
            
            # Set headers
            if self.fingerprint.headers:
                self.client.headers.update(self.fingerprint.headers)
            
            self.logger.info(f"[HTTP] Created httpcloack client with injected fingerprint: UA={self.fingerprint.user_agent[:50]}...")
    
    def update_fingerprint(self, fingerprint: BrowserFingerprint):
        """Update client with new fingerprint."""
        self.fingerprint = fingerprint
        self._initialize_client()
        self.logger.info("[HTTP] Updated client fingerprint")
    
    def get(self, url: str, **kwargs) -> Any:
        """Perform GET request."""
        return self.client.get(url, **kwargs)
    
    def post(self, url: str, **kwargs) -> Any:
        """Perform POST request."""
        return self.client.post(url, **kwargs)
    
    def request(self, method: str, url: str, **kwargs) -> Any:
        """Perform custom HTTP request."""
        return self.client.request(method, url, **kwargs)


class SessionManager:
    """
    Manages browser session lifecycle and fingerprint refresh.
    
    Responsibilities:
    - Track session expiration
    - Refresh browser sessions when needed
    - Rotate fingerprints for load distribution
    - Cache fingerprints for reuse
    """
    
    def __init__(
        self,
        country_code: str,
        cache_dir: Path,
        session_ttl_minutes: int = 60,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize session manager.
        
        Args:
            country_code: Country identifier for session tracking
            cache_dir: Directory to cache fingerprints
            session_ttl_minutes: Session time-to-live in minutes
            logger: Optional logger instance
        """
        self.country_code = country_code
        self.cache_dir = Path(cache_dir) / country_code
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session_ttl = timedelta(minutes=session_ttl_minutes)
        self.logger = logger or logging.getLogger(__name__)
        self.extractor = BrowserFingerprintExtractor(logger=logger)
        
        # Track active sessions
        self.active_sessions: Dict[str, Tuple[BrowserFingerprint, datetime]] = {}
    
    def get_fingerprint_path(self, session_id: str) -> Path:
        """Get path for cached fingerprint."""
        return self.cache_dir / f"fingerprint_{session_id}.json"
    
    def save_fingerprint(self, session_id: str, fingerprint: BrowserFingerprint):
        """Save fingerprint to cache."""
        path = self.get_fingerprint_path(session_id)
        self.extractor.save_fingerprint(fingerprint, path)
        self.active_sessions[session_id] = (fingerprint, datetime.utcnow())
        self.logger.info(f"[SESSION] Saved fingerprint for session {session_id}")
    
    def load_fingerprint(self, session_id: str) -> Optional[BrowserFingerprint]:
        """Load fingerprint from cache."""
        path = self.get_fingerprint_path(session_id)
        fingerprint = self.extractor.load_fingerprint(path)
        
        if fingerprint:
            # Check if expired
            extracted_at = datetime.fromisoformat(fingerprint.extracted_at)
            if datetime.utcnow() - extracted_at < self.session_ttl:
                self.active_sessions[session_id] = (fingerprint, extracted_at)
                self.logger.info(f"[SESSION] Loaded cached fingerprint for session {session_id}")
                return fingerprint
            else:
                self.logger.warning(f"[SESSION] Cached fingerprint for {session_id} expired")
                path.unlink(missing_ok=True)
        
        return None
    
    def is_session_valid(self, session_id: str) -> bool:
        """Check if session is still valid."""
        if session_id not in self.active_sessions:
            return False
        
        fingerprint, created_at = self.active_sessions[session_id]
        age = datetime.utcnow() - created_at
        return age < self.session_ttl
    
    def extract_and_save(
        self,
        session_id: str,
        browser: Union[WebDriver, Page]
    ) -> BrowserFingerprint:
        """
        Extract fingerprint from browser and save.
        
        Args:
            session_id: Unique session identifier
            browser: Selenium WebDriver or Playwright Page
            
        Returns:
            Extracted BrowserFingerprint
        """
        if isinstance(browser, Page):
            fingerprint = self.extractor.extract_from_playwright(browser)
        elif SELENIUM_AVAILABLE and isinstance(browser, WebDriver):
            fingerprint = self.extractor.extract_from_selenium(browser)
        else:
            raise ValueError(f"Unsupported browser type: {type(browser)}")
        
        self.save_fingerprint(session_id, fingerprint)
        return fingerprint
    
    def refresh_session(
        self,
        session_id: str,
        browser: Union[WebDriver, Page]
    ) -> BrowserFingerprint:
        """
        Refresh expired session by extracting new fingerprint.
        
        Args:
            session_id: Session identifier
            browser: Browser instance to extract from
            
        Returns:
            New BrowserFingerprint
        """
        self.logger.info(f"[SESSION] Refreshing session {session_id}")
        fingerprint = self.extract_and_save(session_id, browser)
        return fingerprint
    
    def cleanup_expired(self):
        """Clean up expired sessions from cache."""
        expired = []
        for session_id, (_, created_at) in list(self.active_sessions.items()):
            if datetime.utcnow() - created_at >= self.session_ttl:
                expired.append(session_id)
        
        for session_id in expired:
            path = self.get_fingerprint_path(session_id)
            path.unlink(missing_ok=True)
            del self.active_sessions[session_id]
            self.logger.debug(f"[SESSION] Cleaned up expired session {session_id}")
        
        if expired:
            self.logger.info(f"[SESSION] Cleaned up {len(expired)} expired sessions")
