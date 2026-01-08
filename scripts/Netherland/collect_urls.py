from __future__ import annotations

import csv
import os
import re
import time
import threading
from queue import Queue, Empty
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, ParseResult

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, StaleElementReferenceException


# =========================
# CONFIG
# =========================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
KEYWORDS_CSV = os.path.join(SCRIPT_DIR, "input", "search_terms.csv")   # input keyword csv
KEYWORD_COL = "prefix"         # Column name in CSV (or None for first column)

BASE_URL = "https://www.medicijnkosten.nl/zoeken?searchTerm={kw}"

OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
FINAL_OUTPUT_CSV = os.path.join(OUTPUT_DIR, "unique_urls.csv")

BROWSERS = 1

# Timing requirements:
STAGGER_SECONDS = 30           # start browser 2 after 30s, browser 3 after 30s
WAIT_AFTER_LOAD_SECONDS = 30   # wait 30 seconds after page loads before scrolling

# Scroll behavior:
MAX_STUCK_ROUNDS = 10          # allow more rounds before declaring stuck
SCROLL_TO_BOTTOM = True
MICRO_WAIT_ON_STUCK = 0.4      # minimal wait ONLY when page shows no growth (not every scroll)

PAGE_LOAD_TIMEOUT = 90
HEADLESS = False

# Incremental save (optional)
SAVE_EVERY_NEW_URLS = 300


# =========================
# GLOBAL SHARED STATE
# =========================
unique_urls_lock = threading.Lock()
unique_urls: set[str] = set()
unique_rows: list[dict] = []   # {"url":..., "keyword":..., "browser":...}

save_lock = threading.Lock()
new_urls_since_save = 0


# =========================
# HELPERS
# =========================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def read_keywords(path: str, col: str | None) -> list[str]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError("CSV must have a header row.")
        if col is None:
            col = reader.fieldnames[0]
        if col not in reader.fieldnames:
            raise RuntimeError(f"Column '{col}' not found. Available: {reader.fieldnames}")

        out: list[str] = []
        for row in reader:
            kw = (row.get(col) or "").strip()
            if kw:
                out.append(kw)
        return out


def safe_write_csv(path: str) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["url", "keyword", "browser"])
        w.writeheader()
        w.writerows(unique_rows)


def looks_rate_limited(driver: webdriver.Chrome) -> bool:
    try:
        html = (driver.page_source or "").lower()
    except Exception:
        return False
    return (
        "too many requests" in html
        or "429" in html
        or "temporarily blocked" in html
        or ("te veel" in html and "verzoek" in html)
    )


def parse_total_results(driver: webdriver.Chrome) -> int | None:
    # e.g. "5048 zoekresultaten"
    try:
        text = driver.find_element(By.TAG_NAME, "body").text.lower()
        m = re.search(r"(\d[\d\.]*)\s+zoekresultaten", text)
        if m:
            return int(m.group(1).replace(".", ""))
    except Exception:
        return None
    return None


def canonicalize_url(href: str) -> str | None:
    if not href:
        return None
    href = href.strip()
    if not href:
        return None

    try:
        u = urlparse(href)
        q = parse_qs(u.query)

        # Canonicalize by artikel id if present
        if "artikel" in q and q["artikel"]:
            artikel = q["artikel"][0]
            new_query = urlencode({"artikel": artikel})
            u2 = ParseResult(
                scheme=u.scheme,
                netloc=u.netloc,
                path=u.path,
                params=u.params,
                query=new_query,
                fragment=""
            )
            return urlunparse(u2)

        # Otherwise remove fragment
        return urlunparse(u._replace(fragment=""))
    except Exception:
        return href


def is_likely_result_url(href: str) -> bool:
    h = (href or "").lower()
    return (
        "medicijnkosten.nl" in h and (
            "artikel=" in h
            or "/geneesmiddel" in h
            or "/medicijn" in h
        )
    )


# =========================
# SELENIUM
# =========================
def make_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


# =========================
# EXTRACT + SCROLL
# =========================
def extract_result_links(driver: webdriver.Chrome, keyword: str, browser_id: int) -> int:
    global new_urls_since_save

    new_count = 0
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")

    for a in anchors:
        try:
            href = a.get_attribute("href") or ""
        except (StaleElementReferenceException, WebDriverException):
            continue

        if not href or not is_likely_result_url(href):
            continue

        url = canonicalize_url(href)
        if not url:
            continue

        with unique_urls_lock:
            if url not in unique_urls:
                unique_urls.add(url)
                unique_rows.append({"url": url, "keyword": keyword, "browser": str(browser_id)})
                new_count += 1

    if new_count:
        with save_lock:
            new_urls_since_save += new_count
            if new_urls_since_save >= SAVE_EVERY_NEW_URLS:
                safe_write_csv(FINAL_OUTPUT_CSV)
                new_urls_since_save = 0

    return new_count


def scroll_until_done(driver: webdriver.Chrome, keyword: str, browser_id: int) -> None:
    # First, get expected total BEFORE starting to scroll
    expected_total = parse_total_results(driver)
    if expected_total:
        print(f"[B{browser_id}] '{keyword}': expected_total={expected_total} - starting scroll")
    else:
        print(f"[B{browser_id}] '{keyword}': expected_total not found - will scroll until stuck")

    stuck = 0
    last_height = 0
    keyword_new = 0
    min_collected_for_stuck = None  # Minimum collected before checking stuck

    # Calculate minimum threshold: 90% of expected, or None if expected_total is unknown
    if expected_total and expected_total > 0:
        min_collected_for_stuck = int(expected_total * 0.0)
        print(f"[B{browser_id}] '{keyword}': stuck detection will activate after {min_collected_for_stuck} URLs collected (90%)")

    while True:
        added = extract_result_links(driver, keyword, browser_id)
        keyword_new += added

        try:
            height = driver.execute_script("return document.body.scrollHeight;") or 0
        except Exception:
            height = 0

        grew = (height != last_height)
        last_height = height

        # stuck detection - ONLY check if we've collected at least 90% of expected results
        # If expected_total is unknown, allow stuck detection (fallback)
        can_check_stuck = (min_collected_for_stuck is None) or (keyword_new >= min_collected_for_stuck)
        
        if can_check_stuck:
            if added == 0 and not grew:
                stuck += 1
            else:
                stuck = 0
        else:
            # Reset stuck counter if we haven't reached 90% yet
            stuck = 0

        with unique_urls_lock:
            global_total = len(unique_urls)

        progress_pct = (keyword_new / expected_total * 100) if expected_total and expected_total > 0 else 0
        print(
            f"[B{browser_id}] '{keyword}': +{added} | keyword_new={keyword_new}/{expected_total or '?'} "
            f"({progress_pct:.1f}%) | global_unique={global_total} | stuck={stuck if can_check_stuck else 'N/A'}"
        )

        # Stop early if we got most of expected results
        if expected_total and keyword_new >= int(expected_total * 0.98):
            print(f"[B{browser_id}] '{keyword}': reached ~98% of expected total -> stop")
            break

        # Only check stuck if we've reached the minimum threshold
        if can_check_stuck and stuck >= MAX_STUCK_ROUNDS:
            print(f"[B{browser_id}] '{keyword}': stopping (stuck {stuck} rounds after 90% collected) -> stop")
            break

        # Scroll "back-to-back": NO fixed waits here
        if SCROLL_TO_BOTTOM:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        else:
            driver.execute_script("window.scrollBy(0, 2000);")

        # Minimal wait ONLY when page isn't changing (still satisfies "no wait" in normal case)
        if stuck > 0:
            time.sleep(MICRO_WAIT_ON_STUCK)




# =========================
# KEYWORD PROCESSING
# =========================
def process_keyword(driver: webdriver.Chrome, keyword: str, browser_id: int) -> None:
    url = BASE_URL.format(kw=keyword)
    print(f"[B{browser_id}] OPEN {url}")

    driver.get(url)

    # Only while opening / loading: handle rate limit
    if looks_rate_limited(driver):
        print(f"[B{browser_id}] RATE LIMITED on '{keyword}'. Waiting 60s then refresh...")
        time.sleep(60)
        driver.refresh()

    # Requirement: wait 30s AFTER open, BEFORE scrolling
    print(f"[B{browser_id}] '{keyword}': wait {WAIT_AFTER_LOAD_SECONDS}s before scrolling")
    time.sleep(WAIT_AFTER_LOAD_SECONDS)

    print(f"[B{browser_id}] '{keyword}': start scrolling")
    scroll_until_done(driver, keyword, browser_id)
    print(f"[B{browser_id}] '{keyword}': done")


# =========================
# WORKERS
# =========================
def worker(browser_id: int, q: Queue[str]) -> None:
    driver = None
    try:
        driver = make_driver()
        while True:
            try:
                kw = q.get_nowait()
            except Empty:
                break

            try:
                process_keyword(driver, kw, browser_id)
            except Exception as e:
                print(f"[B{browser_id}] ERROR keyword='{kw}': {e}")
            finally:
                q.task_done()

    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def main():
    ensure_dir(OUTPUT_DIR)

    keywords = read_keywords(KEYWORDS_CSV, KEYWORD_COL)
    if not keywords:
        raise SystemExit("No keywords found in CSV.")

    q: Queue[str] = Queue()
    for kw in keywords:
        q.put(kw)

    threads: list[threading.Thread] = []

    # Start browsers staggered exactly as you want
    for i in range(BROWSERS):
        t = threading.Thread(target=worker, args=(i + 1, q), daemon=True)
        t.start()
        threads.append(t)

        if i < BROWSERS - 1:
            print(f"[MAIN] started browser {i+1}. waiting {STAGGER_SECONDS}s to start next...")
            time.sleep(STAGGER_SECONDS)

    q.join()
    for t in threads:
        t.join()

    # Final output (unique)
    safe_write_csv(FINAL_OUTPUT_CSV)

    print(f"[DONE] Global unique URLs: {len(unique_urls)}")
    print(f"[DONE] Saved: {FINAL_OUTPUT_CSV}")


if __name__ == "__main__":
    main()
