"""
Scrapy middlewares integrating with core platform utilities.

OTelDownloaderMiddleware:
- Records request/response metrics via core.observability.metrics

RandomUserAgentMiddleware:
- Rotates User-Agent from a pool of real Chrome/Firefox/Edge strings

BrowserHeadersMiddleware:
- Adds realistic Sec-Fetch-*, sec-ch-ua, and Referer headers

HumanizeDownloaderMiddleware:
- Adds random delays between requests to mimic human behavior

AntiBotDownloaderMiddleware:
- Detects anti-bot responses (403, 429, 503, captcha pages)
- Applies exponential backoff on detection

ProxyRotationMiddleware:
- Rotates through proxy list (env PROXY_LIST or file)
- Disabled by default; set PROXY_LIST env to enable
"""

import logging
import os
import random
import time
import hashlib

from scrapy import signals
from scrapy.http import Request, Response

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# User-Agent Pool (real browser strings, updated periodically)
# ---------------------------------------------------------------------------

_CHROME_VERSIONS = [
    "120.0.0.0", "121.0.0.0", "122.0.0.0", "123.0.0.0",
    "124.0.0.0", "125.0.0.0", "126.0.0.0", "127.0.0.0",
]

_UA_TEMPLATES = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver} Safari/537.36",
    # Chrome Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver} Safari/537.36",
    # Edge Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver} Safari/537.36 Edg/{ver}",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Chrome Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver} Safari/537.36",
]

def _build_ua_pool():
    pool = []
    for tpl in _UA_TEMPLATES:
        if "{ver}" in tpl:
            for ver in _CHROME_VERSIONS:
                pool.append(tpl.replace("{ver}", ver))
        else:
            pool.append(tpl)
    return pool

UA_POOL = _build_ua_pool()

# sec-ch-ua values matching Chrome versions
_SEC_CH_UA_MAP = {
    "120": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "121": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    "122": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "123": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    "124": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "125": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "126": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    "127": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
}


# ---------------------------------------------------------------------------
# Middlewares
# ---------------------------------------------------------------------------

class OTelDownloaderMiddleware:
    """Record HTTP request metrics via OpenTelemetry."""

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def spider_opened(self, spider):
        self.country = getattr(spider, "country_name", spider.name)
        # Try to init metrics (no-op if OTel not available)
        try:
            from core.observability.metrics import init_metrics
            init_metrics(exporter="none")  # Don't export from middleware; let pipeline control
        except ImportError:
            pass

    def process_request(self, request: Request, spider):
        request.meta["_otel_start"] = time.monotonic()
        return None

    def process_response(self, request: Request, response: Response, spider):
        start = request.meta.get("_otel_start")
        duration_ms = (time.monotonic() - start) * 1000 if start else 0

        try:
            from core.observability.metrics import record_request
            record_request(
                country=self.country,
                status_code=response.status,
                duration_ms=duration_ms,
                method=request.method,
            )
        except ImportError:
            pass

        return response

    def process_exception(self, request, exception, spider):
        try:
            from core.observability.metrics import record_error
            record_error(country=self.country, error_type=type(exception).__name__)
        except ImportError:
            pass
        return None


class PlatformDBLoggingMiddleware:
    """Log fetches to platform DB: urls + fetch_logs tables."""

    def __init__(self):
        self._enabled = False
        self._register_url = None
        self._update_url_status = None
        self._log_fetch = None
        self.country = None
        self.run_id = None

    @classmethod
    def from_crawler(cls, crawler):
        mw = cls()
        crawler.signals.connect(mw.spider_opened, signal=signals.spider_opened)
        return mw

    def spider_opened(self, spider):
        self.country = getattr(spider, "country_name", spider.name)
        self.run_id = getattr(spider, "platform_run_id", None)

        try:
            from services.db import (
                ensure_platform_schema,
                register_url,
                update_url_status,
                log_fetch,
            )
            ensure_platform_schema()
            self._register_url = register_url
            self._update_url_status = update_url_status
            self._log_fetch = log_fetch
            self._enabled = True
        except Exception as exc:
            self._enabled = False
            logger.debug("Platform DB logging disabled: %s", exc)

    def process_request(self, request: Request, spider):
        if not self._enabled:
            return None

        request.meta["_platform_start"] = time.monotonic()

        if "_platform_url_id" not in request.meta:
            try:
                url_id = self._register_url(
                    url=request.url,
                    country=self.country,
                    source=request.meta.get("source"),
                    entity_type=request.meta.get("entity_type"),
                    metadata={
                        "method": request.method,
                        "spider": getattr(spider, "name", ""),
                    },
                )
                request.meta["_platform_url_id"] = url_id
            except Exception:
                pass
        return None

    def process_response(self, request: Request, response: Response, spider):
        if not self._enabled:
            return response

        start = request.meta.get("_platform_start")
        latency_ms = int((time.monotonic() - start) * 1000) if start else None

        try:
            body = response.body or b""
            response_bytes = len(body)
            content_hash = hashlib.md5(body).hexdigest() if body else None
        except Exception:
            response_bytes = None
            content_hash = None

        success = 200 <= response.status < 400
        url_id = request.meta.get("_platform_url_id")

        try:
            user_agent = request.headers.get(b"User-Agent", b"").decode("utf-8", errors="ignore")
        except Exception:
            user_agent = None

        try:
            self._log_fetch(
                url=response.url,
                method="scrapy",
                success=success,
                url_id=url_id,
                run_id=self.run_id,
                status_code=response.status,
                response_bytes=response_bytes,
                latency_ms=latency_ms,
                proxy_used=request.meta.get("proxy"),
                user_agent=user_agent,
                error_type=None if success else "http_error",
                error_message=None if success else f"HTTP {response.status}",
                retry_count=int(request.meta.get("retry_times", 0) or 0),
                fallback_used=False,
            )
        except Exception:
            pass

        if url_id:
            try:
                status = "fetched" if success else "failed"
                self._update_url_status(url_id, status, content_hash=content_hash,
                                        error=None if success else f"HTTP {response.status}")
            except Exception:
                pass

        return response

    def process_exception(self, request: Request, exception, spider):
        if not self._enabled:
            return None

        start = request.meta.get("_platform_start")
        latency_ms = int((time.monotonic() - start) * 1000) if start else None
        url_id = request.meta.get("_platform_url_id")

        try:
            self._log_fetch(
                url=request.url,
                method="scrapy",
                success=False,
                url_id=url_id,
                run_id=self.run_id,
                status_code=None,
                response_bytes=None,
                latency_ms=latency_ms,
                proxy_used=request.meta.get("proxy"),
                user_agent=None,
                error_type=type(exception).__name__,
                error_message=str(exception),
                retry_count=int(request.meta.get("retry_times", 0) or 0),
                fallback_used=False,
            )
        except Exception:
            pass

        if url_id:
            try:
                self._update_url_status(url_id, "failed", error=str(exception))
            except Exception:
                pass
        return None


class PlatformFetcherMiddleware:
    """Fetch via scripts.common.fetcher with platform DB logging."""

    def __init__(self):
        self._enabled = False
        self._upsert_url = None
        self._log_fetch = None
        self._fetch = None
        self._fetch_method = None
        self.country = None
        self.run_id = None
        self._session = None

    @classmethod
    def from_crawler(cls, crawler):
        mw = cls()
        crawler.signals.connect(mw.spider_opened, signal=signals.spider_opened)
        return mw

    def spider_opened(self, spider):
        self.country = getattr(spider, "country_name", spider.name)
        self.run_id = getattr(spider, "platform_run_id", None)

        try:
            from services.db import ensure_platform_schema, upsert_url, log_fetch
            from services.fetcher import fetch, FetchMethod
            import requests

            ensure_platform_schema()
            self._upsert_url = upsert_url
            self._log_fetch = log_fetch
            self._fetch = fetch
            self._fetch_method = FetchMethod.HTTP
            self._session = requests.Session()
            self._enabled = True
        except Exception as exc:
            self._enabled = False
            logger.debug("Platform fetcher disabled: %s", exc)

    def process_request(self, request: Request, spider):
        if not self._enabled:
            return None

        url = request.url
        url_id = None

        try:
            url_id = self._upsert_url(
                url=url,
                country=self.country,
                source=request.meta.get("source"),
                entity_type=request.meta.get("entity_type"),
                metadata={
                    "method": request.method,
                    "spider": getattr(spider, "name", ""),
                },
            )
            request.meta["_platform_url_id"] = url_id
        except Exception:
            pass

        # API endpoints return JSON; skip HTML validation in fetcher.
        is_api = request.meta.get("source") == "api" or "/rest/" in url

        # Convert headers to string dict for fetcher.
        headers = {}
        try:
            for key, value in request.headers.items():
                headers[key.decode("utf-8", errors="ignore")] = value.decode("utf-8", errors="ignore")
        except Exception:
            headers = None
        
        proxy = request.meta.get("proxy")
        proxies = {"http": proxy, "https": proxy} if proxy else None

        result = self._fetch(
            url,
            country=self.country,
            method=self._fetch_method,
            headers=headers,
            proxies=proxies,
            validate=not is_api,
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            fallback=False,
            log_to_db=False,
            session=self._session,
            run_id=self.run_id,
        )

        try:
            response_bytes = len(result.content_bytes) if result.content_bytes else None
            self._log_fetch(
                url=url,
                method=str(result.method_used.value),
                success=result.success,
                url_id=url_id,
                run_id=self.run_id,
                status_code=result.status_code,
                response_bytes=response_bytes,
                latency_ms=result.latency_ms,
                proxy_used=request.meta.get("proxy"),
                user_agent=headers.get("User-Agent") if headers else None,
                error_type=result.error_type,
                error_message=result.error_message,
                retry_count=result.retry_count,
                fallback_used=result.fallback_used,
            )
        except Exception:
            pass

        body = result.content_bytes
        if body is None and result.content is not None:
            try:
                body = result.content.encode("utf-8", errors="ignore")
            except Exception:
                body = b""
        if body is None:
            body = b""

        status = result.status_code or (200 if result.success else 599)

        from scrapy.http import TextResponse
        return TextResponse(
            url=url,
            status=status,
            body=body,
            encoding="utf-8",
            request=request,
        )


class RandomUserAgentMiddleware:
    """Rotate User-Agent from a pool of real browser strings per request."""

    def __init__(self, ua_pool):
        self.ua_pool = ua_pool

    @classmethod
    def from_crawler(cls, crawler):
        return cls(ua_pool=UA_POOL)

    def process_request(self, request: Request, spider):
        ua = random.choice(self.ua_pool)
        request.headers[b"User-Agent"] = ua
        # Store chosen UA in meta so BrowserHeadersMiddleware can match sec-ch-ua
        request.meta["_chosen_ua"] = ua
        return None


class BrowserHeadersMiddleware:
    """
    Add realistic browser fingerprint headers to every request.

    Injects sec-ch-ua, sec-ch-ua-mobile, sec-ch-ua-platform,
    Sec-Fetch-Dest/Mode/Site/User, and Upgrade-Insecure-Requests.
    Matches the Chrome version from the chosen User-Agent.
    """

    def process_request(self, request: Request, spider):
        ua = request.meta.get("_chosen_ua", "")
        headers = request.headers

        # Determine if Chrome-based UA
        is_chrome = "Chrome/" in ua and "Firefox" not in ua
        chrome_ver = ""
        if is_chrome:
            try:
                chrome_ver = ua.split("Chrome/")[1].split(".")[0]
            except (IndexError, AttributeError):
                chrome_ver = "120"

        if is_chrome and chrome_ver:
            sec_ch = _SEC_CH_UA_MAP.get(chrome_ver, _SEC_CH_UA_MAP.get("120", ""))
            headers.setdefault(b"sec-ch-ua", sec_ch)
            headers.setdefault(b"sec-ch-ua-mobile", "?0")

            # Platform from UA
            if "Windows" in ua:
                headers.setdefault(b"sec-ch-ua-platform", '"Windows"')
            elif "Macintosh" in ua:
                headers.setdefault(b"sec-ch-ua-platform", '"macOS"')
            elif "Linux" in ua:
                headers.setdefault(b"sec-ch-ua-platform", '"Linux"')

        # Sec-Fetch headers (browser always sends these)
        is_api = any(x in (request.url or "") for x in ["/api/", "New?", "List?"])
        if is_api:
            headers.setdefault(b"Sec-Fetch-Dest", "empty")
            headers.setdefault(b"Sec-Fetch-Mode", "cors")
            headers.setdefault(b"Sec-Fetch-Site", "same-origin")
        else:
            headers.setdefault(b"Sec-Fetch-Dest", "document")
            headers.setdefault(b"Sec-Fetch-Mode", "navigate")
            headers.setdefault(b"Sec-Fetch-Site", "none")
            headers.setdefault(b"Sec-Fetch-User", "?1")

        headers.setdefault(b"Upgrade-Insecure-Requests", "1")

        # Accept header matching browser type
        if is_api:
            headers.setdefault(b"Accept", "application/json, text/plain, */*")
        elif "Firefox" in ua:
            headers.setdefault(
                b"Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            )
        else:
            headers.setdefault(
                b"Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            )

        # Randomize Accept-Language slightly
        lang_variants = [
            "en-US,en;q=0.9",
            "en-US,en;q=0.8",
            "en-GB,en-US;q=0.9,en;q=0.8",
            "en-US,en;q=0.9,hi;q=0.8",
        ]
        headers.setdefault(b"Accept-Language", random.choice(lang_variants))

        # Connection keep-alive (browsers always send this)
        headers.setdefault(b"Connection", "keep-alive")

        return None


class HumanizeDownloaderMiddleware:
    """Add random delays between requests to appear more human-like."""

    def __init__(self, min_delay: float, max_delay: float):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._last_request_time = 0.0

    @classmethod
    def from_crawler(cls, crawler):
        min_d = float(os.getenv("HUMANIZE_MIN_DELAY", "1.0"))
        max_d = float(os.getenv("HUMANIZE_MAX_DELAY", "3.0"))
        return cls(min_d, max_d)

    def process_request(self, request: Request, spider):
        # Add jitter: occasionally pause longer (simulates reading/thinking)
        if random.random() < 0.05:
            # 5% chance of a longer "reading" pause
            delay = random.uniform(self.max_delay, self.max_delay * 3)
        else:
            delay = random.uniform(self.min_delay, self.max_delay)

        time.sleep(delay)
        return None


class AntiBotDownloaderMiddleware:
    """Detect anti-bot responses and apply exponential backoff."""

    CAPTCHA_MARKERS = [
        b"captcha", b"CAPTCHA", b"recaptcha", b"hcaptcha",
        b"challenge-platform", b"cf-browser-verification",
        b"Just a moment", b"Checking your browser",
    ]

    # Empty or error JSON patterns that indicate a soft block
    SOFT_BLOCK_MARKERS = [
        b'"error"', b'"blocked"', b'"rate_limit"', b'"unauthorized"',
    ]

    def __init__(self, backoff_base: float, backoff_max: float, captcha_pause: float):
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.captcha_pause = captcha_pause
        self._attempt_counts: dict = {}
        self._consecutive_blocks = 0

    @classmethod
    def from_crawler(cls, crawler):
        base = float(os.getenv("ANTIBOT_BACKOFF_BASE", "5"))
        mx = float(os.getenv("ANTIBOT_BACKOFF_MAX", "120"))
        pause = float(os.getenv("ANTIBOT_CAPTCHA_PAUSE", "60"))
        return cls(base, mx, pause)

    def process_response(self, request: Request, response: Response, spider):
        url = request.url

        # Detect blocked status codes
        if response.status in (403, 429, 503):
            self._consecutive_blocks += 1
            return self._handle_block(request, response, spider, f"HTTP {response.status}")

        # Detect captcha in response body
        if response.status == 200 and hasattr(response, "body"):
            body = response.body[:4096]  # Only check first 4KB
            for marker in self.CAPTCHA_MARKERS:
                if marker in body:
                    self._consecutive_blocks += 1
                    return self._handle_block(request, response, spider, "captcha_detected")

        # Clear attempt counter on success
        self._attempt_counts.pop(url, None)
        self._consecutive_blocks = 0
        return response

    def _handle_block(self, request: Request, response: Response, spider, reason: str):
        """Handle a blocked response with exponential backoff."""
        url = request.url
        attempts = self._attempt_counts.get(url, 0) + 1
        self._attempt_counts[url] = attempts

        # Exponential backoff with jitter
        base_delay = min(self.backoff_base * (2 ** (attempts - 1)), self.backoff_max)
        delay = base_delay + random.uniform(0, base_delay * 0.3)

        # If many consecutive blocks, add extra cooldown
        if self._consecutive_blocks >= 5:
            delay = max(delay, 30.0)
            logger.warning("[ANTIBOT] %d consecutive blocks — cooling down %.0fs", self._consecutive_blocks, delay)

        formulation = request.meta.get("formulation", "unknown")
        logger.warning(
            "[ANTIBOT] %s for '%s' (attempt %d, backoff %.0fs) url=%s",
            reason, formulation, attempts, delay, url,
        )
        print(
            f"[DB] ANTIBOT | {reason} | {formulation} | attempt={attempts} backoff={delay:.0f}s",
            flush=True,
        )

        # If too many attempts, let Scrapy's retry middleware handle or give up
        max_retries = int(os.getenv("MAX_RETRIES", "3"))
        if attempts > max_retries:
            logger.warning(
                "[ANTIBOT] Giving up on '%s' after %d attempts", formulation, attempts
            )
            self._attempt_counts.pop(url, None)
            # Mark as blocked via meta so spider can handle
            request.meta["_antibot_blocked"] = True
            request.meta["_antibot_reason"] = reason
            return response

        # Sleep and retry
        time.sleep(delay)
        retry_req = request.copy()
        retry_req.dont_filter = True
        return retry_req


class ProxyRotationMiddleware:
    """
    Rotate through proxies per request.

    Configure via environment:
        PROXY_LIST=http://user:pass@ip1:port,http://user:pass@ip2:port
    or:
        PROXY_FILE=path/to/proxies.txt  (one proxy per line)

    Disabled if neither is set.
    """

    def __init__(self, proxies):
        self.proxies = proxies
        self.enabled = len(proxies) > 0

    @classmethod
    def from_crawler(cls, crawler):
        proxies = []
        proxy_list = os.getenv("PROXY_LIST", "").strip()
        if proxy_list:
            proxies = [p.strip() for p in proxy_list.split(",") if p.strip()]
        else:
            proxy_file = os.getenv("PROXY_FILE", "").strip()
            if proxy_file and os.path.isfile(proxy_file):
                with open(proxy_file, "r") as f:
                    proxies = [line.strip() for line in f if line.strip() and not line.startswith("#")]

        mw = cls(proxies)
        if mw.enabled:
            logger.info("[PROXY] Loaded %d proxies", len(proxies))
        return mw

    def process_request(self, request: Request, spider):
        if self.enabled:
            request.meta["proxy"] = random.choice(self.proxies)
        return None
