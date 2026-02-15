# Standardization: Chrome Instance Tracking & Stealth Features

**Date:** February 6, 2026  
**Status:** ✅ Standardized

---

## Overview

This document describes the standardization of two features across all scrapers:
1. **Chrome Instance Tracking** - Unified browser instance tracking
2. **Stealth/Anti-Bot Features** - Standardized anti-detection measures

---

## 1. Chrome Instance Tracking

### Why Standardize?

**Netherlands** had `nl_chrome_instances` table, but **Malaysia** and **Argentina** only used PID files. This inconsistency made it hard to:
- Track browser instances per step/thread
- Detect orphaned browsers
- Monitor browser lifecycle across all scrapers

### Standardized Solution

**Shared Table:** `chrome_instances` (not country-specific)

**Schema:**
```sql
CREATE TABLE chrome_instances (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    scraper_name TEXT NOT NULL,
    step_number INTEGER NOT NULL,
    thread_id INTEGER,
    browser_type TEXT DEFAULT 'chrome',
    pid INTEGER NOT NULL,
    parent_pid INTEGER,
    user_data_dir TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    terminated_at TIMESTAMP,
    termination_reason TEXT
);
```

**Shared Module:** `core.chrome_instance_tracker.ChromeInstanceTracker`

**Usage:**
```python
from core.chrome_instance_tracker import ChromeInstanceTracker

tracker = ChromeInstanceTracker(scraper_name, run_id, db)
instance_id = tracker.register(step_number=1, thread_id=0, pid=12345)
tracker.mark_terminated(instance_id, reason="cleanup")
```

### Migration

- **Migration Script:** `sql/migrations/postgres/006_add_chrome_instances_table.sql`
- **Migrates:** Existing `nl_chrome_instances` data to shared table
- **All Scrapers:** Must use shared `chrome_instances` table going forward

---

## 2. Stealth/Anti-Bot Features

### Why Standardize?

**Malaysia** had comprehensive stealth features (webdriver hiding, mock plugins, user agent rotation), but **Argentina** and **Netherlands** had basic implementations. This inconsistency meant:
- Some scrapers more likely to be detected
- Code duplication across scrapers
- Hard to maintain and improve stealth features

### Standardized Solution

**Shared Module:** `core.stealth_profile` (enhanced)

**Features Included:**
- ✅ Webdriver property hiding (`navigator.webdriver = undefined`)
- ✅ Mock plugins array (Chrome-like plugins)
- ✅ Mock languages (`navigator.languages`)
- ✅ Mock chrome runtime (`window.chrome`)
- ✅ User agent rotation (random selection from pool)
- ✅ Automation flag disabling (`--disable-blink-features=AutomationControlled`)
- ✅ Stealth init script for Playwright
- ✅ Human-like delays (`pause()`, `long_pause()`)

**Features EXCLUDED:**
- ❌ Human-like typing simulation (`human_type()`, `type_delay_ms()`) - NOT standardized

### Usage

**Playwright:**
```python
from core.stealth_profile import apply_playwright, get_stealth_init_script

context_kwargs = {}
apply_playwright(context_kwargs)  # Adds user agent, locale, viewport
context = browser.new_context(**context_kwargs)
context.add_init_script(get_stealth_init_script())  # Injects stealth script
```

**Selenium:**
```python
from core.stealth_profile import apply_selenium

apply_selenium(options)  # Adds stealth flags and user agent
```

---

## 3. Onboarding Checklist Updates

### New Requirements Added

**Item 17: Chrome Instance Tracking (MANDATORY)**
- `[prefix]_chrome_instances` table created OR use shared `chrome_instances` table
- Register browser instances when spawned
- Mark instances as terminated on cleanup
- Track instances per step/thread

**Item 26: Stealth/Anti-Bot Implementation (MANDATORY)**
- Uses `core.stealth_profile` module
- Webdriver hiding, mock plugins, user agent rotation
- Human-like delays (but NOT human typing)
- Stealth init script injected into Playwright contexts

---

## 4. Implementation Status

### Chrome Instance Tracking

| Scraper | Status | Notes |
|---------|--------|-------|
| **Netherlands** | ✅ Has table | Needs migration to shared table |
| **Malaysia** | ⏳ Needs implementation | Currently uses PID files only |
| **Argentina** | ⏳ Needs implementation | Currently uses PID files only |

### Stealth Features

| Scraper | Status | Notes |
|---------|--------|-------|
| **Malaysia** | ✅ Comprehensive | Source of standardization |
| **Netherlands** | ⏳ Needs upgrade | Has basic `core.stealth_profile` |
| **Argentina** | ⏳ Needs upgrade | Has basic fingerprinting only |

---

## 5. Next Steps

1. **Run Migration:** Execute `006_add_chrome_instances_table.sql` to create shared table
2. **Update Netherlands:** Migrate from `nl_chrome_instances` to `chrome_instances`
3. **Add to Malaysia:** Implement chrome instance tracking using `ChromeInstanceTracker`
4. **Add to Argentina:** Implement chrome instance tracking using `ChromeInstanceTracker`
5. **Upgrade Stealth:** Update Netherlands/Argentina to use enhanced `core.stealth_profile`

---

## 6. Benefits

- ✅ **Consistency:** All scrapers use same tracking/stealth approach
- ✅ **Maintainability:** One implementation to maintain
- ✅ **Observability:** Better browser lifecycle monitoring
- ✅ **Reliability:** Better anti-detection across all scrapers
- ✅ **Onboarding:** Clear requirements for new scrapers

---

**Last Updated:** February 6, 2026
