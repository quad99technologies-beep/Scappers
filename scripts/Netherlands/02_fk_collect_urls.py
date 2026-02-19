#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Netherlands FK (Farmacotherapeutisch Kompas) - URL Collection (Step 2)

Collects detail product URLs from the FK listing page using Playwright.
Stores URLs in nl_fk_urls table for Phase 2 scraping.

Usage:
    python 02_fk_collect_urls.py              # Collect URLs (or skip if already collected)
    python 02_fk_collect_urls.py --headed     # Run with visible browser
    python 02_fk_collect_urls.py --limit 50   # Limit to 50 URLs (for testing)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import time
from pathlib import Path
from typing import List, Optional, Set

# Path wiring
SCRIPT_DIR = Path(__file__).resolve().parent
_repo_root = SCRIPT_DIR.parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Force UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from core.utils.logger import get_logger
from core.db.postgres_connection import get_db
from core.pipeline.standalone_checkpoint import run_with_checkpoint

try:
    from core.browser.chrome_pid_tracker import (
        get_chrome_pids_from_playwright_browser,
        save_chrome_pids,
    )
    _PID_TRACKER_AVAILABLE = True
except ImportError:
    _PID_TRACKER_AVAILABLE = False

# Clear conflicting db module
for _m in list(sys.modules.keys()):
    if _m == "db" or _m.startswith("db."):
        del sys.modules[_m]

from config_loader import getenv, getenv_int, getenv_bool, get_output_dir
from db.schema import apply_netherlands_schema
from db.repositories import NetherlandsRepository

log = get_logger(__name__, "Netherlands")

SCRIPT_ID = "Netherlands"
STEP_NUMBER = 2
STEP_NAME = "FK URL Collection"

FK_BASE_URL = "https://www.farmacotherapeutischkompas.nl"


# ---------------------------------------------------------------
# URL Helpers
# ---------------------------------------------------------------

def abs_url(href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return FK_BASE_URL + href
    return FK_BASE_URL + "/" + href


def slug_from_url(url: str) -> str:
    """Extract generic slug from URL path, e.g. '.../r/rivaroxaban' -> 'rivaroxaban'."""
    parts = url.rstrip("/").split("/")
    return parts[-1] if parts else ""


def filter_detail_urls(
    raw_hrefs: List[str],
    limit: Optional[int] = None,
    only_contains: Optional[str] = None,
) -> List[str]:
    """Filter raw hrefs to keep only FK detail product URLs."""
    links: Set[str] = set()
    only_lc = only_contains.lower().strip() if only_contains else None

    for href in raw_hrefs:
        if not href:
            continue
        if "/bladeren/preparaatteksten/" not in href:
            continue
        if "/groep/" in href:
            continue
        # Skip alphabet letter navigation links
        if "/alfabet/" in href:
            tail = href.rstrip("/").split("/")[-1]
            if len(tail) <= 1:
                continue
        if "#medicine-listing" in href or "/zoeken" in href:
            continue

        full = abs_url(href)
        if only_lc and only_lc not in full.lower():
            continue
        links.add(full)
        if limit and len(links) >= limit:
            break

    return sorted(links)


# ---------------------------------------------------------------
# Playwright helpers
# ---------------------------------------------------------------

async def _dismiss_cookie_banner(page) -> None:
    for sel in [
        "button:has-text('Accept')",
        "button:has-text('Akkoord')",
        "button:has-text('OK')",
        "button:has-text('I agree')",
    ]:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=1500)
                await asyncio.sleep(0.5)
                break
        except Exception:
            pass


async def _expand_all_sections(page) -> None:
    """Expand collapsible sections on FK listing pages."""

    async def is_valid() -> bool:
        try:
            await page.evaluate("1")
            return True
        except Exception:
            return False

    # 1. "Unfold all" button
    try:
        if await is_valid():
            btn = page.locator("#button-open-all-sections")
            if await btn.count() > 0:
                await btn.first.click(timeout=3000)
                await asyncio.sleep(1.0)
    except Exception:
        pass

    # 2. Expand closed sections iteratively
    selectors = [
        "section.pat-collapsible.closed h3",
        ".pat-collapsible.closed h3",
        "h3.collapsible-closed",
        "section.closed h3",
        "h3.pat-collapsible-header.closed",
    ]
    for attempt in range(1, 11):
        if not await is_valid():
            break

        found_any = False
        for sel in selectors:
            try:
                if not await is_valid():
                    break
                headers = page.locator(sel)
                cnt = await headers.count()
                if cnt > 0:
                    log.info(f"Expansion pass {attempt}: {cnt} closed sections ({sel})")
                    found_any = True
                    for i in range(cnt):
                        try:
                            if not await is_valid():
                                break
                            await headers.nth(i).click(timeout=1000)
                            await asyncio.sleep(0.1)
                        except Exception:
                            pass
            except Exception:
                continue

        if not found_any:
            break
        await asyncio.sleep(1.5)


async def _auto_scroll(page, max_loops: int = 200, sleep_s: float = 0.6) -> None:
    """Scroll page to trigger lazy loading."""
    last_height = 0
    stuck = 0
    for _ in range(max_loops):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(sleep_s)
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                stuck += 1
            else:
                stuck = 0
            last_height = new_height
            if stuck >= 3:
                break
        except Exception:
            break


# ---------------------------------------------------------------
# Main collection
# ---------------------------------------------------------------

async def collect_fk_urls(
    headless: bool = True,
    limit: Optional[int] = None,
    only_contains: Optional[str] = None,
) -> List[str]:
    """Collect detail URLs from the FK listing page using Playwright."""

    listing_url = getenv("FK_LISTING_URL", FK_BASE_URL + "/bladeren/preparaatteksten/groep")
    max_scroll = getenv_int("FK_SCROLL_MAX_LOOPS", 200)
    scroll_sleep = float(getenv("FK_SCROLL_SLEEP", "0.6"))

    from playwright.async_api import async_playwright

    log.info(f"Launching Playwright (headless={headless}) for FK URL collection")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)

        # Track PIDs
        chrome_pids: Set[int] = set()
        if _PID_TRACKER_AVAILABLE:
            try:
                chrome_pids = get_chrome_pids_from_playwright_browser(browser)
                if chrome_pids:
                    save_chrome_pids(SCRIPT_ID, _repo_root, chrome_pids)
                    log.info(f"Tracked {len(chrome_pids)} Playwright browser PIDs")
            except Exception as e:
                log.warning(f"PID tracking failed: {e}")

        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = await ctx.new_page()

        log.info(f"Navigating to FK listing: {listing_url}")
        await page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(1.0)

        await _dismiss_cookie_banner(page)
        await _expand_all_sections(page)

        log.info("Scrolling to load all lazy content...")
        await _auto_scroll(page, max_loops=max_scroll, sleep_s=scroll_sleep)

        # Collect all hrefs via JS
        raw_hrefs = await page.evaluate(
            "() => Array.from(document.querySelectorAll('a[href]')).map(a => a.getAttribute('href'))"
        )
        log.info(f"Found {len(raw_hrefs)} total anchors on page, filtering...")

        detail_urls = filter_detail_urls(raw_hrefs, limit=limit, only_contains=only_contains)

        await ctx.close()
        await browser.close()

    log.info(f"Collected {len(detail_urls)} FK detail URLs")
    return detail_urls


# ---------------------------------------------------------------
# Run ID helper
# ---------------------------------------------------------------

def _get_run_id() -> str:
    run_id = os.environ.get("NL_RUN_ID", "").strip()
    if run_id:
        return run_id
    run_id_file = get_output_dir() / ".current_run_id"
    if run_id_file.exists():
        try:
            return run_id_file.read_text(encoding="utf-8").strip()
        except Exception:
            pass
    return ""


# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--headed", action="store_true", help="Run browser headed")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of URLs collected")
    ap.add_argument("--only", default="", help="Only collect URLs containing this substring")
    args = ap.parse_args()

    run_id = _get_run_id()
    if not run_id:
        log.error("No run_id found. Run pipeline with --fresh first.")
        raise SystemExit(1)

    log.info(f"Step {STEP_NUMBER}: {STEP_NAME} | run_id={run_id}")

    db = get_db("Netherlands")
    apply_netherlands_schema(db)
    repo = NetherlandsRepository(db, run_id)

    # Resume check: skip if URLs already collected
    existing_count = repo.get_fk_url_count()
    if existing_count > 0:
        log.info(
            f"FK URLs already collected ({existing_count} URLs). "
            "Skipping collection (delete nl_fk_urls to re-collect)."
        )
        return

    headless = not args.headed
    limit = args.limit if args.limit > 0 else None
    only = args.only.strip() or None

    detail_urls = asyncio.run(collect_fk_urls(headless=headless, limit=limit, only_contains=only))

    if not detail_urls:
        log.warning("No FK detail URLs collected!")
        return

    # Build URL records
    url_records = [
        {"url": u, "generic_slug": slug_from_url(u)}
        for u in detail_urls
    ]

    inserted = repo.insert_fk_urls(url_records)
    log.info(f"Inserted {inserted} FK URLs into nl_fk_urls")

    # Register chrome instances in DB
    if _PID_TRACKER_AVAILABLE:
        try:
            from core.browser.chrome_pid_tracker import get_chrome_pids_from_playwright_browser
            # PIDs already terminated when browser.close() was called above
            repo.terminate_all_chrome_instances(reason="step2_complete")
        except Exception:
            pass


if __name__ == "__main__":
    run_with_checkpoint(main, SCRIPT_ID, STEP_NUMBER, STEP_NAME)
