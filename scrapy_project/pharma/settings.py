"""
Scrapy settings for the pharma scraper platform.

Integrates with the existing ConfigManager for path resolution
and core utilities for rate limiting.

Hardened for anti-bot evasion:
- Rotating User-Agent pool (Chrome/Firefox/Edge)
- Realistic browser headers (sec-ch-ua, Sec-Fetch-*)
- Human-like pacing with jitter
- Anti-bot detection with exponential backoff
- Proxy rotation (optional, env-driven)
- Cookie persistence
"""

import sys
from pathlib import Path

# Add repo root to sys.path so core/ imports work
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

BOT_NAME = "pharma"
SPIDER_MODULES = ["pharma.spiders"]
NEWSPIDER_MODULE = "pharma.spiders"

# Crawl responsibly
ROBOTSTXT_OBEY = False  # Pharma registries often block robots.txt
DOWNLOAD_DELAY = 2
RANDOMIZE_DOWNLOAD_DELAY = True

# Concurrency (low = human-like)
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4

# Autothrottle (replaces custom rate_limiter for Scrapy spiders)
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 2
AUTOTHROTTLE_MAX_DELAY = 30
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0
AUTOTHROTTLE_DEBUG = False

# Retry (includes 403 for blocked responses)
RETRY_ENABLED = True
RETRY_TIMES = 5
RETRY_HTTP_CODES = [403, 408, 429, 500, 502, 503, 504]

# Timeouts
DOWNLOAD_TIMEOUT = 40

# Cookies (browsers always send cookies)
COOKIES_ENABLED = True
COOKIES_DEBUG = False

# Disable telnet (security)
TELNETCONSOLE_ENABLED = False

# Default headers — overridden per-request by BrowserHeadersMiddleware
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}

# No static USER_AGENT — RandomUserAgentMiddleware rotates per request

# Pipelines
ITEM_PIPELINES = {
    "pharma.pipelines.SQLitePipeline": 300,
}

# Middlewares — ordered by priority (lower = runs first)
DOWNLOADER_MIDDLEWARES = {
    # Disable Scrapy's built-in UA middleware (we use our own)
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,

    # Metrics (runs first)
    "pharma.middlewares.OTelDownloaderMiddleware": 50,
    "pharma.middlewares.PlatformDBLoggingMiddleware": 60,

    # UA rotation (before headers middleware so UA is set)
    "pharma.middlewares.RandomUserAgentMiddleware": 90,

    # Browser fingerprint headers (sec-ch-ua, Sec-Fetch, etc.)
    "pharma.middlewares.BrowserHeadersMiddleware": 95,

    # Human-like delay jitter
    "pharma.middlewares.HumanizeDownloaderMiddleware": 100,

    # Anti-bot detection + backoff
    "pharma.middlewares.AntiBotDownloaderMiddleware": 110,

    # Proxy rotation (disabled unless PROXY_LIST or PROXY_FILE env set)
    "pharma.middlewares.ProxyRotationMiddleware": 120,
}

# DNS cache
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 10000
DNSCACHE_TTL = 3600  # Cache DNS entries for 1 hour to reduce lookups

# Feed exports (disabled — we use SQLite pipeline instead)
# FEEDS = {}

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "[%(levelname)s] %(name)s: %(message)s"

# Job directory for resume support
# Set per spider via: scrapy crawl spider_name -s JOBDIR=output/Country/.scrapy_job
# JOBDIR = None  # Set at runtime

# Feed encoding
FEED_EXPORT_ENCODING = "utf-8"
