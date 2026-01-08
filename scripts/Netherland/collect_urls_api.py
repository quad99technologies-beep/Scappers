from __future__ import annotations

import csv
import os
import re
import time
import random
import html as html_lib
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin

import requests

# Optional: better parsing
try:
    from bs4 import BeautifulSoup  # pip install beautifulsoup4
except Exception:
    BeautifulSoup = None

# Selenium (fallback)
USE_SELENIUM_FALLBACK = True
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
except Exception:
    USE_SELENIUM_FALLBACK = False
    webdriver = None
    Options = None


# =========================
# CONFIG
# =========================
BASE_URL = "https://www.medicijnkosten.nl"
SEARCH_URL = f"{BASE_URL}/zoeken"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(SCRIPT_DIR, "input")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
DEBUG_DIR = os.path.join(OUTPUT_DIR, "_debug_html")

INPUT_KEYWORDS_CSV = os.path.join(INPUT_DIR, "search_terms.csv")  # expects column: prefix
KEYWORD_COL = "prefix"

OUTPUT_CSV = os.path.join(OUTPUT_DIR, "unique_urls.csv")

START_PAGE = 1
MAX_PAGES_PER_KEYWORD = 5000  # safety cap

# Requests retry/backoff
MAX_RETRIES_PER_PAGE = 6
BASE_DELAY = 1.2
MAX_DELAY = 90.0
JITTER = 0.8

SAVE_EVERY_NEW = 100

# JS shell detector
MIN_HTML_LEN_FOR_RESULTS = 20000

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


# =========================
# UTIL
# =========================
def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def now_ms() -> int:
    return int(time.time() * 1000)


def load_keywords() -> list[str]:
    if not os.path.exists(INPUT_KEYWORDS_CSV):
        return []
    with open(INPUT_KEYWORDS_CSV, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or KEYWORD_COL not in r.fieldnames:
            raise RuntimeError(f"Input CSV must contain column '{KEYWORD_COL}'. Found: {r.fieldnames}")
        out = []
        for row in r:
            kw = (row.get(KEYWORD_COL) or "").strip().lower()
            if kw:
                out.append(kw)
        return out


def dump_debug_html(keyword: str, page: int, status: int, html_text: str, tag: str) -> None:
    ensure_dir(DEBUG_DIR)
    path = os.path.join(DEBUG_DIR, f"{keyword}_p{page}_status{status}_{tag}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text or "")
    print(f"[DEBUG] wrote HTML => {path} (len={len(html_text or '')})")


def looks_blocked(status: int, text: str) -> bool:
    t = (text or "").lower()
    return (
        status in (403, 429, 503)
        or "too many requests" in t
        or "captcha" in t
        or ("te veel" in t and "verzoek" in t)
        or "cloudflare" in t
        or "access denied" in t
    )


def canonicalize_url(href: str) -> str | None:
    if not href:
        return None
    href = href.strip()
    if not href:
        return None

    if href.startswith("/"):
        href = urljoin(BASE_URL, href)

    try:
        u = urlparse(href)
        q = parse_qs(u.query)

        # stable dedupe on artikel
        if "artikel" in q and q["artikel"]:
            artikel = q["artikel"][0]
            new_q = urlencode({"artikel": artikel})
            return urlunparse(u._replace(query=new_q, fragment=""))

        return urlunparse(u._replace(fragment=""))
    except Exception:
        return href


def is_result_link(href: str) -> bool:
    h = (href or "").lower()
    return ("medicijnkosten.nl" in h) and ("artikel=" in h or "/medicijn" in h or "/geneesmiddel" in h)


def extract_links(html_text: str) -> list[str]:
    if not html_text:
        return []
    html_text = html_lib.unescape(html_text)

    links: list[str] = []

    if BeautifulSoup is not None:
        soup = BeautifulSoup(html_text, "html.parser")
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if href.startswith("?"):
                href = "/zoeken" + href
            if not href.startswith("http"):
                href = urljoin(BASE_URL, href)
            if is_result_link(href):
                c = canonicalize_url(href)
                if c:
                    links.append(c)
        return links

    for href in re.findall(r'href=["\']([^"\']+)["\']', html_text, flags=re.I):
        href = href.strip()
        if not href:
            continue
        if href.startswith("?"):
            href = "/zoeken" + href
        if not href.startswith("http"):
            href = urljoin(BASE_URL, href)
        if is_result_link(href):
            c = canonicalize_url(href)
            if c:
                links.append(c)
    return links


def load_existing_output(path: str) -> tuple[set[str], list[dict]]:
    uniq: set[str] = set()
    rows: list[dict] = []
    if not os.path.exists(path):
        return uniq, rows
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            u = (row.get("url") or "").strip()
            if not u:
                continue
            uniq.add(u)
            rows.append({"url": u, "keyword": (row.get("keyword") or "").strip(), "source": (row.get("source") or "resume").strip()})
    return uniq, rows


def save_output(path: str, rows: list[dict]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["url", "keyword", "source"])
        w.writeheader()
        w.writerows(rows)


# =========================
# REQUESTS SESSION
# =========================
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": BASE_URL + "/",
    })
    return s


def warmup(session: requests.Session) -> None:
    try:
        r = session.get(BASE_URL + "/", timeout=60)
        print(f"[INIT] warmup GET / status={r.status_code} cookies={len(session.cookies)}")
    except Exception as e:
        print(f"[INIT] warmup failed: {e}")


def build_params(keyword: str, page: int) -> dict:
    # match HAR-like params (works even if site ignores some)
    return {
        "searchTerm": keyword,
        "page": str(page),
        "sorting": "",
        "debugMode": "",
        "_": str(now_ms()),
    }


# =========================
# SELENIUM HELPERS (pagination + infinite scroll)
# =========================
def make_driver() -> webdriver.Chrome:
    assert webdriver is not None and Options is not None
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    # keep visible (recommended) to handle any interactive challenge
    # opts.add_argument("--headless=new")
    return webdriver.Chrome(options=opts)


def selenium_scroll_to_bottom(driver, max_scrolls: int = 80, stable_rounds: int = 4) -> None:
    """
    Works for infinite scroll pages.
    Scrolls down until page height stops changing for `stable_rounds` iterations.
    """
    same = 0
    last_h = 0
    for _ in range(max_scrolls):
        h = driver.execute_script("return document.body.scrollHeight")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.35 + random.random() * 0.25)
        h2 = driver.execute_script("return document.body.scrollHeight")
        if h2 == h or h2 == last_h:
            same += 1
        else:
            same = 0
        last_h = h2
        if same >= stable_rounds:
            break


def selenium_collect_keyword(keyword: str, uniq: set[str], rows: list[dict]) -> int:
    """
    Selenium fallback that:
      - opens paginated pages (?page=1,2,3...)
      - scrolls each page to force lazy content (if any)
      - extracts links
      - stops when page produces 0 new links twice
    """
    if not USE_SELENIUM_FALLBACK:
        print("[SEL] Selenium not available. Install selenium + chromedriver.")
        return 0

    driver = None
    new_for_kw = 0
    zero_new_pages = 0

    try:
        driver = make_driver()

        for page in range(START_PAGE, MAX_PAGES_PER_KEYWORD + 1):
            url = f"{SEARCH_URL}?searchTerm={keyword}&page={page}"
            print(f"[SEL] '{keyword}' OPEN {url}")
            driver.get(url)

            # Let initial JS settle a bit
            time.sleep(1.8 + random.random() * 0.6)

            # Try to load all items on the page (handles infinite scroll)
            selenium_scroll_to_bottom(driver)

            html_text = driver.page_source or ""
            if page == 1:
                dump_debug_html(keyword, page, 200, html_text, "selenium_page")

            links = extract_links(html_text)
            if not links:
                print(f"[SEL] '{keyword}' page={page} no links -> stop")
                return new_for_kw

            added = 0
            for u in links:
                if u not in uniq:
                    uniq.add(u)
                    rows.append({"url": u, "keyword": keyword, "source": "selenium"})
                    added += 1

            new_for_kw += added

            print(f"[SEL] '{keyword}' page={page} +{added} | kw_new={new_for_kw} | global_unique={len(uniq)}")

            if added == 0:
                zero_new_pages += 1
            else:
                zero_new_pages = 0

            # Stop when we see 2 consecutive pages with 0 new URLs (pagination end)
            if zero_new_pages >= 2:
                print(f"[SEL] '{keyword}' page={page} consecutive 0-new pages -> stop")
                return new_for_kw

        print(f"[SEL] '{keyword}' reached MAX_PAGES_PER_KEYWORD cap -> stop")
        return new_for_kw

    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


# =========================
# SCRAPE KEYWORD (requests-first, selenium fallback)
# =========================
def scrape_keyword(session: requests.Session, keyword: str, uniq: set[str], rows: list[dict], delay_state: dict) -> int:
    new_for_kw = 0
    print(f"[KW] '{keyword}' start")

    for page in range(START_PAGE, MAX_PAGES_PER_KEYWORD + 1):
        last_err = None

        for attempt in range(1, MAX_RETRIES_PER_PAGE + 1):
            delay = min(delay_state["delay"], MAX_DELAY)
            time.sleep(delay + random.random() * JITTER)

            try:
                session.headers["Referer"] = f"{SEARCH_URL}?searchTerm={keyword}"
                params = build_params(keyword, page)

                r = session.get(SEARCH_URL, params=params, timeout=60)

                if looks_blocked(r.status_code, r.text):
                    delay_state["delay"] = min(delay_state["delay"] * 2.0 + 1.0, MAX_DELAY)
                    print(f"[KW] '{keyword}' page={page} BLOCK {r.status_code} attempt={attempt}/{MAX_RETRIES_PER_PAGE} backoff={delay_state['delay']:.2f}s")
                    last_err = f"blocked {r.status_code}"
                    continue

                delay_state["delay"] = max(BASE_DELAY, delay_state["delay"] * 0.92)

                text = r.text or ""

                # If JS shell on page 1, switch to Selenium for full keyword (pagination)
                if page == 1 and len(text) < MIN_HTML_LEN_FOR_RESULTS:
                    dump_debug_html(keyword, page, r.status_code, text, "requests_shell")
                    print(f"[KW] '{keyword}' page=1 JS shell (len={len(text)}). Switching to Selenium keyword crawl…")
                    return selenium_collect_keyword(keyword, uniq, rows)

                links = extract_links(text)
                if not links:
                    if page == 1:
                        dump_debug_html(keyword, page, r.status_code, text, "requests_nolinks")
                        print("[DEBUG] first 300 chars:", text[:300].replace("\n", " "))
                    print(f"[KW] '{keyword}' page={page} no links → done")
                    return new_for_kw

                added = 0
                for u in links:
                    if u not in uniq:
                        uniq.add(u)
                        rows.append({"url": u, "keyword": keyword, "source": "requests"})
                        added += 1

                new_for_kw += added
                print(f"[KW] '{keyword}' page={page} +{added} | kw_new={new_for_kw} | global_unique={len(uniq)} | delay={delay_state['delay']:.2f}s")

                # next page
                break

            except Exception as e:
                last_err = str(e)
                delay_state["delay"] = min(delay_state["delay"] * 1.5 + 0.6, MAX_DELAY)
                print(f"[KW] '{keyword}' page={page} ERROR {e} attempt={attempt}/{MAX_RETRIES_PER_PAGE} delay={delay_state['delay']:.2f}s")

        else:
            print(f"[KW] '{keyword}' page={page} FAILED after retries: {last_err}")
            # if requests failed hard mid-keyword, finish keyword with Selenium
            print(f"[KW] '{keyword}' switching to Selenium due to repeated failures…")
            return new_for_kw + selenium_collect_keyword(keyword, uniq, rows)

    print(f"[KW] '{keyword}' reached MAX_PAGES_PER_KEYWORD cap -> stop")
    return new_for_kw


# =========================
# MAIN
# =========================
def main() -> None:
    ensure_dir(OUTPUT_DIR)

    uniq, rows = load_existing_output(OUTPUT_CSV)
    if uniq:
        print(f"[INIT] resume: loaded {len(uniq)} unique URLs from {OUTPUT_CSV}")
    else:
        print("[INIT] starting fresh (no existing output)")

    keywords = load_keywords()
    if not keywords:
        raise RuntimeError(f"No keywords found in {INPUT_KEYWORDS_CSV}")

    session = make_session()
    warmup(session)

    delay_state = {"delay": BASE_DELAY}
    new_since_save = 0

    for idx, kw in enumerate(keywords, start=1):
        added = scrape_keyword(session, kw, uniq, rows, delay_state)
        new_since_save += added

        if new_since_save >= SAVE_EVERY_NEW:
            save_output(OUTPUT_CSV, rows)
            print(f"[SAVE] wrote {len(rows)} rows to {OUTPUT_CSV}")
            new_since_save = 0

        print(f"[PROGRESS] {idx}/{len(keywords)} done | global_unique={len(uniq)}")

    save_output(OUTPUT_CSV, rows)
    print(f"[DONE] global_unique={len(uniq)} saved={OUTPUT_CSV}")


if __name__ == "__main__":
    main()

