# Enterprise Uniformity Refactor Plan

**Status:** Draft
**Target:** 100% Uniformity across all scrapers
**Reference:** User Request "100% Enterprise uniformity before release"

---

## 🎯 Objective
Bring all scrapers (Major & Minor) to a unified architectural standard to ensure maintainability, observability, and reliability at an Enterprise level.

## 🏗 The "Enterprise Standard" Definition

Every scraper **MUST**:
1.  **Architecture**: Inherit from a unified `BaseScraper` class (to be defined in `core/base_scraper.py`).
2.  **Configuration**: Load config via `core.config_manager`, not local `.env` loaders.
3.  **Proxies**: Use `core.proxy_manager` for all proxy interactions. Custom rotation logic must be moved to this module or a plugin of it.
4.  **Metrics**: Instrument key steps (start, item_scraped, error, finish) using `core.observability` (Prometheus).
5.  **Logging**: Use `logging` module exclusively. `print()` is **FORBIDDEN** except for CLI tool entry points.
6.  **Database**: Use `core.db` modules. No raw `psycopg2` or `sqlite3` connections in scraper scripts.

---

## 📊 Status Matrix

| Scraper | Arch. | Config | Proxies | Metrics | Logging | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Argentina** | ❌ Script | ⚠️ Mixed | ❌ Local | ❌ DB-only | ✅ Good | **Needs Refactor** |
| **Malaysia** | ✅ Class | ✅ Good | ✅ GeoRouter | ✅ Tracker | ✅ Good | **Reference Implementation** |
| **Netherlands**| ❌ Script | ⚠️ Mixed | ❌ Local | ❌ Missing | ⚠️ Mixed | **Needs Refactor** |
| **Canada (ON)**| ❌ Script | ❌ Local | ❌ `requests`| ❌ Missing | ⚠️ Mixed | **Needs Refactor** |
| **Canada (QC)**| ❌ Script | ❌ Local | N/A | ❌ Missing | ⚠️ Mixed | **Needs Refactor** |
| **India** | ⚠️ Scrapy | ✅ Scrapy | ⚠️ Scrapy | ❌ Missing | ⚠️ Mixed | **Needs Adapter** |
| **Chile** | ❌ Script | ❌ Local | ❌ None | ❌ Missing | ❌ Print | **Needs Rewrite** |
| **Others** | ❌ Script | ❌ Local | ❌ None | ❌ Missing | ❌ Print | **Needs Rewrite** |

*(Others: Belarus, Colombia, Peru, Russia, South Korea, Taiwan, Brazil)*

---

## 🗓 Implementation Roadmap

### Phase 1: Core Foundation (1 Day)
1.  **Create `core/base_scraper.py`**:
    *   Define abstract base class.
    *   Built-in `ProxyManager` integration.
    *   Built-in `Metrics` instrumentation.
    *   Built-in `Logger` setup.
    *   Standard `run()` and `stop()` methods.

### Phase 2: Major Scraper Migration (2 Days)
1.  **Refactor Argentina**:
    *   Move `03_alfabeta_selenium_worker.py` logic to `ArgentinaScraper(BaseScraper)`.
    *   Replace `RotationCoordinator` with `ProxyManager` capabilities.
2.  **Refactor Netherlands**:
    *   Wrap `01_fast_scraper.py` into `NetherlandsScraper(BaseScraper)`.

### Phase 3: Minor Scraper Standardization (3 Days)
1.  **Standardize "The Long Tail"**:
    *   Convert Chile, Peru, Colombia, etc., to use the new `BaseScraper`.
    *   Since these are often simpler request-based scrapers, this should be a "Lift & Shift" into the new class structure.

### Phase 4: Scrapy Adapters (1 Day)
1.  **India & Russia**:
    *   Create a `CoreScrapyMiddleware` to bridge Scrapy events to `core.observability`.
    *   Ensure Scrapy uses `core.proxy_manager` for proxy rotation.

## 📝 Actionable Checklist

- [ ] Create `core/base_scraper.py`.
- [ ] Migrate **Argentina** to `BaseScraper`.
- [ ] Migrate **Netherlands** to `BaseScraper`.
- [ ] Migrate **Canada (Ontario)** to `BaseScraper`.
- [ ] Migrate **Canada (Quebec)** to `BaseScraper`.
- [ ] Migrate **Chile** to `BaseScraper`.
- [ ] Migrate **Belarus** to `BaseScraper`.
- [ ] Migrate **Colombia** to `BaseScraper`.
- [ ] Migrate **Peru** to `BaseScraper`.
- [ ] Migrate **South Korea** to `BaseScraper`.
- [ ] Migrate **Taiwan** to `BaseScraper`.
- [ ] Implement `CoreScrapyMiddleware` for **India** and **Russia**.
- [ ] Global `grep` to ensure no `print()` statements remain in production paths.

---

**Signed off by:** Antigravity (Test Lead)
