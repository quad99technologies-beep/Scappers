from __future__ import annotations

import csv
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Set, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.ui import WebDriverWait


# =========================================================
# CONFIG
# =========================================================
BASE = "https://www.medicijnkosten.nl"
SEARCH_URL = "https://www.medicijnkosten.nl/zoeken?searchTerm={kw}"

# Input CSV: must contain column "prefix" (e.g., aba, abc, abd...)
INPUT_TERMS_CSV = "input/search_terms.csv"

OUTPUT_DIR = "output"
COLLECTED_URLS_CSV = os.path.join(OUTPUT_DIR, "collected_urls.csv")
PRODUCTS_CSV = os.path.join(OUTPUT_DIR, "products.csv")
SCRAPED_URLS_CSV = os.path.join(OUTPUT_DIR, "scraped_urls.csv")
FAILED_URLS_CSV = os.path.join(OUTPUT_DIR, "failed_urls.csv")

HEADLESS = False
PAGELOAD_TIMEOUT = 90
WAIT_SECONDS_BEFORE_SCROLL = 30

# Scroll requirements
MIN_SCROLL_LOOPS = 20          # hard requirement
MAX_SCROLL_LOOPS = 60
MAX_STUCK_ROUNDS = 12
MICRO_WAIT_ON_STUCK = 0.4

# Persistence / safety
SAVE_URLS_EVERY_N_NEW = 25     # save discovered URLs in small batches during scrolling
SAVE_PROGRESS_EVERY_LOOP = True  # also flush at end of each loop

# Scraping behavior
SKIP_IF_ALREADY_SCRAPED = True
MAX_RETRIES_PER_URL = 2
RETRY_BACKOFF_SECONDS = 6


# =========================================================
# MODELS
# =========================================================
@dataclass
class ProductRow:
    prefix: str
    product_key: str
    url: str
    title: str
    raw_kv: str


# =========================================================
# DRIVER
# =========================================================
def make_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)
    return driver


def wait_dom_ready(driver: webdriver.Chrome, timeout: int = 60) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


# =========================================================
# CSV HELPERS
# =========================================================
def ensure_csv_header(path: str, header: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            f.flush()


def append_csv_row(path: str, row: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(row)
        f.flush()


def load_scraped_urls(path: str) -> Set[str]:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return set()
    out: Set[str] = set()
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            u = (row.get("url") or "").strip()
            if u:
                out.add(u)
    return out


def load_prefixes(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if "prefix" not in (r.fieldnames or []):
            raise ValueError(f"{path} must contain a column named 'prefix'")
        prefixes = []
        for row in r:
            p = (row.get("prefix") or "").strip()
            if p:
                prefixes.append(p.lower())
    return prefixes


# =========================================================
# URL HELPERS
# =========================================================
def is_likely_result_url(href: str) -> bool:
    h = (href or "").lower()
    return (
        "medicijnkosten.nl" in h and (
            "artikel=" in h
            or "/geneesmiddel" in h
            or "/medicijn" in h
        )
    )


def canonicalize_url(href: str) -> Optional[str]:
    """
    Fixes the issue you saw:
      expected: https://www.medicijnkosten.nl/medicijn?artikel=...&id=...
    Some pages return: https://www.medicijnkosten.nl/?artikel=... (or other variants)
    This enforces /medicijn if artikel/id is present.
    """
    if not href:
        return None
    href = href.strip()
    if not href:
        return None

    try:
        u = urlparse(href)

        # Make absolute if needed
        if not u.scheme:
            u = u._replace(scheme="https", netloc="www.medicijnkosten.nl")

        q = parse_qs(u.query)
        artikel = q.get("artikel", [None])[0]
        pid = q.get("id", [None])[0]

        if artikel or pid:
            params = {}
            if artikel:
                params["artikel"] = artikel
            if pid:
                params["id"] = pid
            return urlunparse((u.scheme, u.netloc, "/medicijn", "", urlencode(params, doseq=False), ""))

        # For non-product URLs, strip fragments only
        return urlunparse(u._replace(fragment=""))

    except Exception:
        return None


def parse_total_results(driver: webdriver.Chrome) -> Optional[int]:
    """
    Tries to read something like '5.048 zoekresultaten' from body text.
    If not found, returns None.
    """
    try:
        text = driver.find_element(By.TAG_NAME, "body").text.lower()
        m = re.search(r"(\d[\d\.]*)\s+zoekresultaten", text)
        if m:
            return int(m.group(1).replace(".", ""))
    except Exception:
        pass
    return None


# =========================================================
# COLLECTION (scroll page and persist URLs during loops)
# =========================================================
def collect_urls_for_prefix(prefix: str, scraped_urls: Set[str]) -> List[str]:
    driver = make_driver()
    urls: List[str] = []
    seen: Set[str] = set()
    pending_to_save: List[str] = []

    try:
        url = SEARCH_URL.format(kw=prefix)
        print(f"\n[PREFIX] {prefix}")
        print(f"[{prefix}] OPEN {url}")
        driver.get(url)
        wait_dom_ready(driver)

        print(f"[{prefix}] wait {WAIT_SECONDS_BEFORE_SCROLL}s before scrolling")
        time.sleep(WAIT_SECONDS_BEFORE_SCROLL)

        expected = parse_total_results(driver)
        print(f"[{prefix}] expected_total={expected}")

        stuck = 0
        last_height = 0

        for loop in range(1, MAX_SCROLL_LOOPS + 1):
            anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
            added = 0

            for a in anchors:
                try:
                    href = a.get_attribute("href")
                except WebDriverException:
                    continue

                if not href or not is_likely_result_url(href):
                    continue

                cu = canonicalize_url(href)
                if not cu:
                    continue

                # ✅ Skip already scraped URLs (from past runs)
                if SKIP_IF_ALREADY_SCRAPED and cu in scraped_urls:
                    continue

                if cu not in seen:
                    seen.add(cu)
                    urls.append(cu)
                    pending_to_save.append(cu)
                    added += 1

            # ✅ Persist frequently (so you never lose progress)
            if pending_to_save and (len(pending_to_save) >= SAVE_URLS_EVERY_N_NEW or SAVE_PROGRESS_EVERY_LOOP):
                for u in pending_to_save:
                    append_csv_row(COLLECTED_URLS_CSV, [prefix, u])
                pending_to_save.clear()

            height = driver.execute_script("return document.body.scrollHeight") or 0
            grew = height != last_height
            last_height = height

            if added == 0 and not grew:
                stuck += 1
            else:
                stuck = 0

            print(f"[{prefix}] LOOP {loop} | new_urls={len(urls)} | +{added} | stuck={stuck}")

            # stop rules
            if loop >= MIN_SCROLL_LOOPS:
                if expected and len(urls) >= int(expected * 0.98):
                    print(f"[{prefix}] reached ~98% of expected")
                    break
                if stuck >= MAX_STUCK_ROUNDS:
                    print(f"[{prefix}] stuck after min scrolls")
                    break

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            if stuck:
                time.sleep(MICRO_WAIT_ON_STUCK)

        # final flush
        if pending_to_save:
            for u in pending_to_save:
                append_csv_row(COLLECTED_URLS_CSV, [prefix, u])
            pending_to_save.clear()

        print(f"[{prefix}] COLLECT DONE | total_new_urls={len(urls)}")
        return urls

    finally:
        driver.quit()


# =========================================================
# SCRAPING
# =========================================================
def mark_scraped(url: str, prefix: str) -> None:
    append_csv_row(SCRAPED_URLS_CSV, [url, prefix, datetime.now().isoformat(timespec="seconds")])


def mark_failed(url: str, prefix: str, err: str) -> None:
    append_csv_row(FAILED_URLS_CSV, [url, prefix, datetime.now().isoformat(timespec="seconds"), err[:300]])


def scrape_product(driver: webdriver.Chrome, prefix: str, url: str) -> ProductRow:
    driver.get(url)
    wait_dom_ready(driver)

    # Title (best effort)
    title = ""
    try:
        title = driver.find_element(By.TAG_NAME, "h1").text.strip()
    except Exception:
        try:
            title = driver.title.strip()
        except Exception:
            title = ""

    # Extract "label : value" style pairs from page text as a simple robust fallback
    body_text = ""
    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
    except Exception:
        body_text = ""

    # Make a stable-ish product key
    # Prefer id=... if present else artikel=...
    u = urlparse(url)
    q = parse_qs(u.query)
    pid = (q.get("id", [None])[0] or "").strip()
    artikel = (q.get("artikel", [None])[0] or "").strip()
    product_key = pid or artikel or url

    # Build raw_kv from lines containing ":"
    kv_lines = []
    for line in (body_text or "").splitlines():
        s = line.strip()
        if not s:
            continue
        if ":" in s and len(s) < 200:
            kv_lines.append(re.sub(r"\s+", " ", s))
    raw_kv = " | ".join(kv_lines[:200])  # limit size

    return ProductRow(prefix=prefix, product_key=product_key, url=url, title=title, raw_kv=raw_kv)


def scrape_urls_for_prefix(prefix: str, urls: List[str], scraped_urls: Set[str]) -> int:
    """
    Scrape each URL in a single driver session.
    Persist after EACH success:
      - append to products.csv
      - append to scraped_urls.csv
      - update in-memory scraped_urls set (so same run also skips)
    """
    if not urls:
        print(f"[{prefix}] No URLs to scrape.")
        return 0

    driver = make_driver()
    scraped_count = 0
    total = len(urls)

    try:
        for i, url in enumerate(urls, start=1):
            if SKIP_IF_ALREADY_SCRAPED and url in scraped_urls:
                print(f"[{prefix}] SKIP {i}/{total} already scraped")
                continue

            ok = False
            last_err = ""
            for attempt in range(1, MAX_RETRIES_PER_URL + 1):
                try:
                    row = scrape_product(driver, prefix, url)

                    # ✅ store product immediately
                    append_csv_row(PRODUCTS_CSV, [row.prefix, row.product_key, row.url, row.title, row.raw_kv])

                    # ✅ mark scraped immediately (so reruns skip)
                    mark_scraped(url, prefix)
                    scraped_urls.add(url)

                    scraped_count += 1
                    ok = True
                    print(f"[{prefix}] SCRAPED {i}/{total} {row.title[:80]}")
                    break

                except Exception as e:
                    last_err = f"{type(e).__name__}: {e}"
                    print(f"[{prefix}] ERROR {i}/{total} attempt {attempt}/{MAX_RETRIES_PER_URL} -> {last_err}")
                    time.sleep(RETRY_BACKOFF_SECONDS * attempt)

            if not ok:
                mark_failed(url, prefix, last_err)
                print(f"[{prefix}] FAILED {i}/{total} stored in failed log")

        return scraped_count

    finally:
        driver.quit()


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    # Ensure output CSVs exist (headers)
    ensure_csv_header(COLLECTED_URLS_CSV, ["prefix", "url"])
    ensure_csv_header(PRODUCTS_CSV, ["prefix", "product_key", "url", "title", "raw_kv"])
    ensure_csv_header(SCRAPED_URLS_CSV, ["url", "prefix", "ts"])
    ensure_csv_header(FAILED_URLS_CSV, ["url", "prefix", "ts", "error"])

    # Load already scraped URLs (across reruns)
    scraped_urls = load_scraped_urls(SCRAPED_URLS_CSV)
    print(f"[INIT] Already scraped URLs: {len(scraped_urls)}")

    prefixes = load_prefixes(INPUT_TERMS_CSV)
    print(f"[INIT] Loaded prefixes: {len(prefixes)} from {INPUT_TERMS_CSV}")

    grand_new_urls = 0
    grand_scraped = 0

    for prefix in prefixes:
        # 1) Collect new URLs for this prefix (persisted during loops)
        urls = collect_urls_for_prefix(prefix, scraped_urls)
        grand_new_urls += len(urls)

        # 2) Scrape them (skips already scraped; persists per row)
        scraped_now = scrape_urls_for_prefix(prefix, urls, scraped_urls)
        grand_scraped += scraped_now

        print(f"[{prefix}] SUMMARY new_urls={len(urls)} scraped_now={scraped_now}")

    print("\n[DONE]")
    print(f"Total new URLs discovered (excluding already-scraped): {grand_new_urls}")
    print(f"Total scraped this run: {grand_scraped}")
    print(f"Scraped URLs database size now: {len(scraped_urls)}")
    print(f"Outputs:\n - {COLLECTED_URLS_CSV}\n - {PRODUCTS_CSV}\n - {SCRAPED_URLS_CSV}\n - {FAILED_URLS_CSV}")


if __name__ == "__main__":
    main()
