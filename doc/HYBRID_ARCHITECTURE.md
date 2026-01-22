# Hybrid Scraping Architecture: Browser + HTTP Client

## Architecture Overview

The hybrid architecture combines two complementary layers:

1. **Browser Layer (Selenium/Playwright)**: Handles JavaScript-heavy pages, login flows, and dynamic content
2. **HTTP Layer (httpcloack)**: Handles high-volume requests with realistic browser fingerprints

### Key Benefits

- **Minimize Browser Usage**: Only use browser for login/JavaScript, then switch to HTTP client
- **Scale**: HTTP client handles 10-100x more requests per second
- **Realistic Fingerprints**: Extracted browser data makes HTTP requests indistinguishable
- **Cost Effective**: Browser instances are expensive; HTTP client is lightweight

## Data Flow

```
┌─────────────────────────────────────────────────────────┐
│                   INITIALIZATION                         │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  1. Browser Layer (Selenium/Playwright)                 │
│     - Navigate to login page                            │
│     - Perform login (if required)                       │
│     - Wait for JavaScript to load                       │
│     - Navigate to authenticated pages                   │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  2. Fingerprint Extraction                              │
│     - Extract cookies (session, auth tokens)            │
│     - Extract headers (User-Agent, Accept, etc.)        │
│     - Extract browser characteristics (viewport, etc.)   │
│     - Extract TLS fingerprint (if available)            │
│     - Serialize to JSON                                 │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  3. HTTP Client Initialization                          │
│     - Create httpcloack client                          │
│     - Inject cookies                                    │
│     - Inject headers                                    │
│     - Configure fingerprint                             │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  4. High-Volume Scraping                                │
│     - Use HTTP client for bulk requests                 │
│     - Maintain session via cookies                      │
│     - Rotate fingerprints periodically                  │
│     - Handle rate limiting                              │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  5. Session Refresh (when needed)                       │
│     - Detect session expiration                         │
│     - Re-run browser login                              │
│     - Extract new fingerprint                           │
│     - Update HTTP client                                │
└─────────────────────────────────────────────────────────┘
```

## Layer Responsibilities

### Browser Layer

**Use Cases:**
- Initial login
- JavaScript-rendered content
- Form submissions requiring CSRF tokens
- Pages requiring WebDriver interactions

**Responsibilities:**
- Authenticate user
- Extract session cookies
- Capture page state after JavaScript execution
- Extract CSRF tokens, form data

**Best Practices:**
- Keep browser instances minimal (1-2 per pipeline)
- Close browser immediately after fingerprint extraction
- Use headless mode for efficiency
- Reuse sessions when possible

### HTTP Layer

**Use Cases:**
- Bulk data extraction
- API-like requests
- High-frequency polling
- Large-scale data collection

**Responsibilities:**
- Make lightweight HTTP requests
- Maintain session via cookies
- Mimic browser fingerprints
- Handle rate limiting and retries

**Best Practices:**
- Use connection pooling
- Implement request queuing
- Rotate user agents periodically
- Cache responses when appropriate

## Implementation Steps

### Step 1: Browser Setup and Login

```python
from selenium import webdriver
from core.hybrid_scraper import BrowserFingerprintExtractor, SessionManager

# Initialize browser
driver = webdriver.Chrome()

# Navigate and login
driver.get("https://example.com/login")
driver.find_element(By.ID, "username").send_keys("user")
driver.find_element(By.ID, "password").send_keys("pass")
driver.find_element(By.ID, "login-btn").click()

# Wait for login to complete
WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.ID, "dashboard"))
)
```

### Step 2: Extract Fingerprint

```python
# Extract fingerprint
extractor = BrowserFingerprintExtractor()
fingerprint = extractor.extract_from_selenium(driver)

# Save for later use
session_manager = SessionManager("MY", cache_dir=Path("./cache"))
session_manager.save_fingerprint("session_1", fingerprint)

# Close browser (no longer needed)
driver.quit()
```

### Step 3: Initialize HTTP Client

```python
from core.hybrid_scraper import HybridHttpClient

# Create HTTP client with fingerprint
http_client = HybridHttpClient(fingerprint=fingerprint)

# Now use HTTP client for high-volume requests
response = http_client.get("https://example.com/api/data")
```

### Step 4: High-Volume Scraping

```python
# Bulk requests using HTTP client
urls = ["https://example.com/item/1", "https://example.com/item/2", ...]

for url in urls:
    response = http_client.get(url)
    # Process response
    data = response.json()
    # Save data
```

### Step 5: Session Refresh

```python
# Check if session expired
if not session_manager.is_session_valid("session_1"):
    # Re-login and refresh
    driver = webdriver.Chrome()
    # ... login flow ...
    new_fingerprint = session_manager.refresh_session("session_1", driver)
    http_client.update_fingerprint(new_fingerprint)
    driver.quit()
```

## Session Refresh Strategy

### Per-Country Configuration

Each country pipeline should define:

```python
SESSION_CONFIG = {
    "MY": {  # Malaysia
        "ttl_minutes": 60,
        "refresh_threshold": 0.8,  # Refresh at 80% of TTL
        "max_refresh_attempts": 3,
    },
    "SG": {  # Singapore
        "ttl_minutes": 30,
        "refresh_threshold": 0.9,
        "max_refresh_attempts": 5,
    }
}
```

### Refresh Triggers

1. **Time-based**: Refresh when session age > threshold
2. **Response-based**: Refresh on 401/403 responses
3. **Cookie-based**: Refresh when auth cookies missing
4. **Manual**: Refresh via API call

### Refresh Implementation

```python
class CountrySessionManager:
    def __init__(self, country_code: str, config: Dict):
        self.country_code = country_code
        self.config = config
        self.session_manager = SessionManager(
            country_code,
            cache_dir=Path("./cache"),
            session_ttl_minutes=config["ttl_minutes"]
        )
    
    def ensure_valid_session(self, http_client: HybridHttpClient) -> bool:
        """Ensure HTTP client has valid session."""
        session_id = f"{self.country_code}_session"
        
        # Check if session valid
        if self.session_manager.is_session_valid(session_id):
            # Load existing fingerprint
            fingerprint = self.session_manager.load_fingerprint(session_id)
            if fingerprint:
                http_client.update_fingerprint(fingerprint)
                return True
        
        # Need to refresh
        return self.refresh_session(session_id, http_client)
    
    def refresh_session(self, session_id: str, http_client: HybridHttpClient) -> bool:
        """Refresh session via browser login."""
        for attempt in range(self.config["max_refresh_attempts"]):
            try:
                # Browser login flow
                driver = self._perform_login()
                
                # Extract new fingerprint
                fingerprint = self.session_manager.extract_and_save(
                    session_id, driver
                )
                
                # Update HTTP client
                http_client.update_fingerprint(fingerprint)
                
                driver.quit()
                return True
            except Exception as e:
                logger.error(f"Refresh attempt {attempt+1} failed: {e}")
                if attempt < self.config["max_refresh_attempts"] - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        return False
```

## Dry-Run Auditor Workflow

### Auditor Checks

1. **Login Validation**
   - Navigate to login page
   - Perform login
   - Verify success indicators (selectors, text)

2. **Selector Validation**
   - Check required selectors exist
   - Verify selectors are visible
   - Test selector robustness

3. **Response Code Validation**
   - Test key URLs return expected codes
   - Verify authentication works
   - Check rate limiting responses

4. **Output Schema Validation**
   - Verify CSV/JSON columns
   - Check minimum row count
   - Validate data types

### Auditor Usage

```python
from core.hybrid_auditor import HybridAuditor

# Initialize auditor
auditor = HybridAuditor()

# Validate login
auditor.validate_login(
    browser=driver,
    login_url="https://example.com/login",
    success_indicators=["dashboard", "#user-menu", "Logout"]
)

# Validate selectors
auditor.validate_selectors(
    browser=driver,
    url="https://example.com/data",
    selectors={
        "table": "table.data-table",
        "rows": "table.data-table tbody tr",
        "next_button": "button.next-page"
    }
)

# Validate response codes
auditor.validate_response_codes(
    http_client=http_client,
    urls=[
        ("https://example.com/api/data", 200),
        ("https://example.com/api/auth", 401),  # Should fail without auth
    ]
)

# Validate output schema
auditor.validate_output_schema(
    file_path="output/data.csv",
    expected_columns=["id", "name", "price", "date"],
    min_rows=100
)

# Generate report
report = auditor.generate_report()
print(report)

# Check if audit passed
summary = auditor.get_summary()
if summary["failed"] > 0:
    raise RuntimeError("Audit failed - fix issues before production run")
```

## Failure Signals and Retry Logic

### Failure Detection

```python
class FailureSignals:
    """Detect failure signals from responses."""
    
    LOGIN_FAILED = {
        "status_codes": [401, 403],
        "text_indicators": ["login", "unauthorized", "access denied"],
        "redirect_patterns": ["/login", "/auth"]
    }
    
    SESSION_EXPIRED = {
        "status_codes": [401],
        "cookies_missing": True,
        "text_indicators": ["session expired", "please login"]
    }
    
    RATE_LIMITED = {
        "status_codes": [429, 503],
        "text_indicators": ["rate limit", "too many requests"],
        "retry_after_header": True
    }
    
    @staticmethod
    def detect_login_failure(response) -> bool:
        """Detect login failure."""
        if response.status_code in FailureSignals.LOGIN_FAILED["status_codes"]:
            return True
        
        response_text = response.text.lower()
        for indicator in FailureSignals.LOGIN_FAILED["text_indicators"]:
            if indicator in response_text:
                return True
        
        return False
    
    @staticmethod
    def detect_session_expired(response) -> bool:
        """Detect session expiration."""
        if response.status_code == 401:
            # Check if auth cookies are missing
            if not response.cookies.get("session_id"):
                return True
            
            response_text = response.text.lower()
            for indicator in FailureSignals.SESSION_EXPIRED["text_indicators"]:
                if indicator in response_text:
                    return True
        
        return False
```

### Retry Logic

```python
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class HybridScraperWithRetry:
    def __init__(self, http_client: HybridHttpClient, session_manager: SessionManager):
        self.http_client = http_client
        self.session_manager = session_manager
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    def get_with_retry(self, url: str) -> Any:
        """Get with automatic retry."""
        response = self.http_client.get(url, timeout=10)
        
        # Check for session expiration
        if FailureSignals.detect_session_expired(response):
            # Refresh session and retry
            self.refresh_session()
            response = self.http_client.get(url, timeout=10)
        
        # Check for rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            time.sleep(retry_after)
            response = self.http_client.get(url, timeout=10)
        
        response.raise_for_status()
        return response
```

## Operational Best Practices

### 1. Minimize Browser Usage

- **Use browser only for login**: Extract fingerprint immediately after login
- **Close browser promptly**: Don't keep browser instances running
- **Reuse fingerprints**: Cache and reuse fingerprints across runs
- **Batch browser operations**: Group multiple logins together

### 2. Optimize for Scale

- **Connection pooling**: Reuse HTTP connections
- **Async requests**: Use async HTTP client for high throughput
- **Request queuing**: Queue requests to respect rate limits
- **Caching**: Cache responses when appropriate

### 3. Stability

- **Session refresh**: Automatically refresh expired sessions
- **Error handling**: Graceful degradation on failures
- **Monitoring**: Track session TTL, request success rates
- **Auditing**: Run dry-run auditor before production

### 4. Country-Specific Configuration

```python
# config/hybrid_config.py
COUNTRY_CONFIGS = {
    "MY": {
        "login_url": "https://example.com/login",
        "session_ttl": 3600,
        "rate_limit_rpm": 60,
        "use_browser_for": ["login", "csrf_token_extraction"],
        "audit_selectors": {
            "table": "table.data",
            "next_page": "button.next"
        }
    }
}
```

### 5. Monitoring and Alerting

```python
class HybridScraperMetrics:
    def __init__(self):
        self.metrics = {
            "browser_usage_time": [],
            "http_requests": 0,
            "session_refreshes": 0,
            "failures": 0
        }
    
    def log_browser_usage(self, duration: float):
        """Log browser usage duration."""
        self.metrics["browser_usage_time"].append(duration)
    
    def log_http_request(self):
        """Log HTTP request."""
        self.metrics["http_requests"] += 1
    
    def get_browser_efficiency(self) -> float:
        """Calculate browser efficiency (HTTP requests / browser seconds)."""
        total_browser_time = sum(self.metrics["browser_usage_time"])
        if total_browser_time == 0:
            return float('inf')
        return self.metrics["http_requests"] / total_browser_time
```

## Example: Complete Country Pipeline

```python
# scripts/Country/01_scrape_with_hybrid.py
from core.hybrid_scraper import (
    BrowserFingerprintExtractor,
    HybridHttpClient,
    SessionManager
)
from core.hybrid_auditor import HybridAuditor

def main():
    country_code = "MY"
    session_id = f"{country_code}_session_1"
    
    # Step 1: Browser login
    driver = setup_browser()
    perform_login(driver)
    
    # Step 2: Extract fingerprint
    session_manager = SessionManager(country_code, cache_dir=Path("./cache"))
    fingerprint = session_manager.extract_and_save(session_id, driver)
    driver.quit()  # Close browser immediately
    
    # Step 3: Initialize HTTP client
    http_client = HybridHttpClient(fingerprint=fingerprint)
    
    # Step 4: Dry-run audit
    auditor = HybridAuditor()
    auditor.validate_response_codes(
        http_client,
        [("https://example.com/api/data", 200)]
    )
    if auditor.get_summary()["failed"] > 0:
        raise RuntimeError("Audit failed")
    
    # Step 5: High-volume scraping
    urls = load_urls()
    for url in urls:
        response = http_client.get(url)
        process_response(response)
        
        # Refresh session if needed
        if not session_manager.is_session_valid(session_id):
            driver = setup_browser()
            perform_login(driver)
            fingerprint = session_manager.refresh_session(session_id, driver)
            http_client.update_fingerprint(fingerprint)
            driver.quit()

if __name__ == "__main__":
    main()
```

## Summary

The hybrid architecture provides:

- **10-100x performance improvement** by minimizing browser usage
- **Realistic fingerprints** extracted from actual browser sessions
- **Automatic session management** with refresh capabilities
- **Dry-run auditing** to catch issues before production
- **Scalable and maintainable** design suitable for cron/CI/batch execution

This architecture is production-ready, scalable, and maintains the realism needed for robust web scraping.
