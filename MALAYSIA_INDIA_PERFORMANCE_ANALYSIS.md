# Malaysia & India Scraper Performance Analysis

## Executive Summary

Both Malaysia and India scrapers are **better architected** than Argentina, but still have room for improvement. The main issues are related to **resource lifecycle management**, **connection pooling**, and **memory accumulation** over long runs.

---

## 🇲🇾 Malaysia Scraper Analysis

### Architecture Overview
- **Browser**: Playwright (Chromium) with stealth context
- **Pattern**: Context manager-based sessions (`browser_session()`)
- **Scrapers**: MyPriMe (Step 1), Quest3Plus (Step 2), FUKKM (Step 4 - HTTP only)

### Issues Found

#### 1. **Playwright Context Accumulation in Quest3Plus** ⚠️ MEDIUM
**Location**: `scrapers/quest3_scraper.py` line 106

The Quest3 scraper uses a SINGLE browser session for BOTH bulk search AND individual details:
```python
with self.browser_session(headless=headless) as page:
    self._bulk_search(page, repo)          # Stage 1: many operations
    self._individual_phase(page, repo, missing)  # Stage 2: many more operations
```

**Problem**: 
- One page handle used for thousands of operations
- Page state accumulates (cookies, localStorage, memory)
- No intermediate cleanup between stages

**Impact**: Memory growth over time, potential slowdown after many products

**Fix** (non-intrusive):
```python
# In quest3_scraper.py, between stages:
# Clear page state periodically
page.evaluate("window.localStorage.clear(); document.cookie.split(';').forEach(c => document.cookie = c.replace(/^ +/, '').replace(/=.*/, '=;expires=' + new Date().toUTCString() + ';path=/'));")
```

#### 2. **Missing Explicit Garbage Collection** ⚠️ LOW
**Location**: `scrapers/base.py`

No periodic `gc.collect()` calls during long-running operations.

**Fix**: Add to base class:
```python
def periodic_cleanup(self, force_gc_every_n: int = 100):
    """Call this every N operations to prevent memory bloat"""
    self._page_count += 1
    if self._page_count % force_gc_every_n == 0:
        import gc
        gc.collect()
        logger.debug("[PERFORMANCE] Periodic GC: freed cycles")
```

#### 3. **Session Object Not Reused in FUKKM** ✅ GOOD (HTTP scraper)
**Location**: `scrapers/fukkm_scraper.py` line 51

Uses `requests.Session()` properly with connection pooling - **well done**.

#### 4. **No Connection Pool Tuning for FUKKM** ⚠️ LOW
**Location**: `scrapers/fukkm_scraper.py`

Current session setup:
```python
def _create_session(self):
    session = requests.Session()
    # No adapter tuning
```

**Fix**:
```python
def _create_session(self):
    session = requests.Session()
    # Tune connection pool for better performance
    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=20,
        max_retries=Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
```

---

## 🇮🇳 India Scraper Analysis

### Architecture Overview
- **Framework**: Scrapy (asynchronous, Twisted-based)
- **Pattern**: Work-queue with parallel workers
- **Database**: PostgreSQL with atomic claim mechanism

### Issues Found

#### 1. **No Connection Pooling for PostgreSQL** ⚠️ MEDIUM
**Location**: `run_scrapy_india.py`, `india_details.py`

Each spider creates its own DB connection:
```python
# In india_details.py - each worker creates connection
from core.db.postgres_connection import PostgresDB
```

**Problem**: With many parallel workers, this exhausts PostgreSQL connection slots.

**Fix**: Use connection pooling in `PostgresDB` class (already available via `psycopg2.pool`):
```python
# In core/db/postgres_connection.py
from psycopg2 import pool

class PostgresDB:
    _connection_pool = None
    
    @classmethod
    def get_pool(cls, min_conn=2, max_conn=10):
        if cls._connection_pool is None:
            cls._connection_pool = psycopg2.pool.ThreadedConnectionPool(
                min_conn, max_conn,
                host=..., database=..., user=..., password=...
            )
        return cls._connection_pool
```

#### 2. **Scrapy Settings - Conservative Concurrency** ✅ GOOD
**Location**: `scrapy_project/pharma/settings.py`

Current settings are appropriate:
```python
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = 2
AUTOTHROTTLE_ENABLED = True
```

This is **well-tuned** for anti-bot evasion.

#### 3. **Missing DNS Cache TTL** ⚠️ LOW
**Location**: `scrapy_project/pharma/settings.py`

```python
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 10000
# Missing: DNSCACHE_TTL
```

**Fix**:
```python
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 10000
DNSCACHE_TTL = 3600  # 1 hour TTL for DNS entries
```

#### 4. **No HTTP/2 Support** ⚠️ LOW
Modern Scrapy supports HTTP/2 for better performance:

```python
# In settings.py
DOWNLOAD_HANDLERS = {
    'https': 'scrapy.core.downloader.handlers.http2.H2DownloadHandler',
}
```

**Note**: Only if target server supports HTTP/2 (NPPA likely doesn't).

---

## 🔧 Recommended Fixes (Non-Intrusive)

### Priority 1: Malaysia Quest3Plus Page Cleanup

**File**: `scripts/Malaysia/scrapers/quest3_scraper.py`

Add between Stage 1 and Stage 2:
```python
# After _bulk_search completes, before _individual_phase
print("[PERFORMANCE] Clearing page state before individual phase...", flush=True)
try:
    page.evaluate("""
        window.localStorage.clear();
        window.sessionStorage.clear();
        document.cookie.split(';').forEach(c => {
            document.cookie = c.replace(/^ +/, '').replace(/=.*/, 
                '=;expires=' + new Date(0).toUTCString() + ';path=/');
        });
    """)
except Exception as e:
    logger.debug(f"Page cleanup warning: {e}")
```

### Priority 2: Malaysia Base Class GC

**File**: `scripts/Malaysia/scrapers/base.py`

Add method:
```python
def force_gc(self):
    """Force garbage collection to prevent memory bloat"""
    import gc
    gc.collect()
```

### Priority 3: India Connection Pooling

**File**: `core/db/postgres_connection.py`

Add threaded connection pool support for multi-worker scenarios.

### Priority 4: FUKKM Connection Tuning

**File**: `scripts/Malaysia/scrapers/fukkm_scraper.py`

Tune HTTP adapter as shown above.

---

## 📊 Performance Comparison

| Aspect | Argentina | Malaysia | India |
|--------|-----------|----------|-------|
| Browser | Firefox/Selenium | Playwright | N/A (HTTP API) |
| Threading | Multi-threaded | Single-threaded | Scrapy async |
| Resource Mgmt | ❌ Poor | ⚠️ Good | ✅ Good |
| Memory Leaks | ❌ Yes | ⚠️ Minor | ✅ No |
| Connection Pool | ❌ None | ⚠️ Basic | ⚠️ Basic |
| Restart Strategy | ✅ Has restart | ⚠️ Session-based | N/A |

---

## 🎯 Quick Wins

### For Malaysia:
1. **Clear page state** between bulk and individual phases
2. **Add periodic GC** every 100 products
3. **Tune FUKKM connection pool**

### For India:
1. **Add connection pooling** for PostgreSQL
2. **Tune DNS cache TTL**
3. **Monitor worker memory** - add periodic logging

---

## Monitoring Recommendations

Add to both scrapers:
```python
def log_performance_stats():
    import psutil, os, threading
    proc = psutil.Process(os.getpid())
    mem_mb = proc.memory_info().rss / 1024 / 1024
    threads = threading.active_count()
    print(f"[PERF] Memory: {mem_mb:.1f}MB | Threads: {threads}")
```

Call every 50-100 iterations to track resource usage.
