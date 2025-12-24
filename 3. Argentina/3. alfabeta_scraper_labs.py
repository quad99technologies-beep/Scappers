#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AlfaBeta scraper — STRICT (Product • Company) match, with ROBUST coverage extraction
-----------------------------------------------------------------------------------
- Keeps your strict pair match (exact Product + Company from CSV).
- Fills the five coverage fields reliably: SIFAR_detail, PAMI_AF, IOMA_detail, IOMA_AF, IOMA_OS.
- Also captures product basics: product_name, company, active_ingredient, therapeutic_class,
  description, price_ars, date, import_status, coverage_json.
- **No slug/product_url/brand** junk.

INPUT
  ./input/Productlist.csv   (columns: Product, Company)

OUTPUTS
  ./output/alfabeta_products_by_product.csv
  ./output/alfabeta_progress.csv
  ./output/alfabeta_errors.csv
  ./output/debug/not_found/*.png|.html
  ./output/debug/error/*.png|.html

Usage
  pip install selenium webdriver-manager
  python alfabeta_scraper_strict_robust_v2.py --headless
  python alfabeta_scraper_strict_robust_v2.py --no-headless --threads 6
"""

import os, re, csv, json, time, random, argparse, logging, threading, tempfile
from pathlib import Path
from datetime import datetime
from queue import Queue, Empty
from typing import Optional, Tuple, List, Dict, Any

try:
    import psutil  # optional
except Exception:
    psutil = None

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.alert import Alert
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# ====== CONFIG ======
USERNAME = "vishwambhar080@gmail.com"
PASSWORD = "iacG@V3hK8LrFUL"

INPUT_FILE = Path("./input/Productlist.csv")
OUTPUT_DIR = Path("./output")
OUT_CSV    = OUTPUT_DIR / "alfabeta_products_by_product.csv"
PROGRESS   = OUTPUT_DIR / "alfabeta_progress.csv"
ERRORS     = OUTPUT_DIR / "alfabeta_errors.csv"
DEBUG_ERR  = OUTPUT_DIR / "debug/error"
DEBUG_NF   = OUTPUT_DIR / "debug/not_found"
for d in [OUTPUT_DIR, DEBUG_ERR, DEBUG_NF]:
    d.mkdir(parents=True, exist_ok=True)

HUB_URL = "https://www.alfabeta.net/precio/srv"
REQUEST_PAUSE_BASE = 0.20
REQUEST_PAUSE_JITTER = (0.05, 0.20)

OUT_FIELDS = [
    "input_company", "input_product_name",
    "company", "product_name",
    "active_ingredient", "therapeutic_class",
    "description", "price_ars", "date", "scraped_at",
    # five coverage fields (priority)
    "SIFAR_detail", "PAMI_AF", "IOMA_detail", "IOMA_AF", "IOMA_OS",
    # extras
    "import_status", "coverage_json"
]

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("alfabeta")

# ====== LOCKS ======
CSV_LOCK = threading.Lock()
PROGRESS_LOCK = threading.Lock()
ERROR_LOCK = threading.Lock()

# ====== UTILS ======

def ts() -> str:
    return datetime.now().isoformat(timespec="seconds")

def normalize_ws(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def strip_accents(s: str) -> str:
    import unicodedata as _u
    return "".join(ch for ch in _u.normalize("NFKD", s or "") if not _u.combining(ch))

def nk(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", strip_accents((s or "").strip())).lower()

def human_pause():
    time.sleep(REQUEST_PAUSE_BASE + random.uniform(*REQUEST_PAUSE_JITTER))

def ar_money_to_float(s: str) -> Optional[float]:
    if not s: return None
    t = re.sub(r"[^\d\.,]", "", s.strip())
    if not t: return None
    # AR: dot thousands, comma decimals
    t = t.replace(".", "").replace(",", ".")
    try: return float(t)
    except ValueError: return None

def parse_date(s: str) -> Optional[str]:
    """Accepts '(24/07/25)' or '24/07/25' or '24-07-2025' → '2025-07-24'"""
    s = (s or "").strip()
    m = re.search(r"\((\d{2})/(\d{2})/(\d{2})\)", s) or re.search(r"\b(\d{2})/(\d{2})/(\d{2})\b", s)
    if m:
        d,mn,y = map(int, m.groups()); y += 2000
        try: return datetime(y,mn,d).date().isoformat()
        except: return None
    m = re.search(r"\b(\d{2})-(\d{2})-(\d{4})\b", s)
    if m:
        d,mn,y = map(int, m.groups())
        try: return datetime(y,mn,d).date().isoformat()
        except: return None
    return None

# ====== CSV IO ======

def ensure_headers():
    if not OUT_CSV.exists():
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=OUT_FIELDS).writeheader()
    if not PROGRESS.exists():
        with open(PROGRESS, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["input_company","input_product_name","timestamp","records_found"])
    if not ERRORS.exists():
        with open(ERRORS, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["input_company","input_product_name","timestamp","error_message"])

def load_progress_set() -> set:
    done = set()
    if PROGRESS.exists():
        with open(PROGRESS, encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                done.add((nk(row["input_company"]), nk(row["input_product_name"])) )
    return done

def append_progress(company: str, product: str, count: int):
    with PROGRESS_LOCK, open(PROGRESS, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([company, product, ts(), count])

def append_error(company: str, product: str, msg: str):
    with ERROR_LOCK, open(ERRORS, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([company, product, ts(), msg[:5000]])

def append_rows(rows: List[Dict[str, Any]]):
    if not rows: return
    with CSV_LOCK, open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=OUT_FIELDS, extrasaction="ignore").writerows(rows)

def save_debug(driver, folder: Path, tag: str):
    try:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        png = folder / f"{tag}_{stamp}.png"
        html = folder / f"{tag}_{stamp}.html"
        driver.save_screenshot(str(png))
        html.write_text(driver.page_source, encoding="utf-8")
    except Exception as e:
        log.warning(f"Could not save debug for {tag}: {e}")

# ====== DRIVER / LOGIN ======

def setup_driver(headless=True):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    # cache mitigations
    opts.add_argument("--incognito")
    opts.add_argument("--disable-application-cache")
    opts.add_argument("--disk-cache-size=0")
    opts.add_argument(f"--disk-cache-dir={tempfile.mkdtemp(prefix='alfabeta-cache-')}")
    opts.add_argument(f"--user-data-dir={tempfile.mkdtemp(prefix='alfabeta-profile-')}")
    # stability
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--lang=es-AR")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.set_page_load_timeout(60)
    return drv

def is_login_page(driver) -> bool:
    try:
        return bool(driver.find_elements(By.ID, "usuario")) and bool(driver.find_elements(By.ID, "clave"))
    except Exception:
        return False

def do_login(driver):
    try:
        user = driver.find_element(By.ID, "usuario")
        pwd  = driver.find_element(By.ID, "clave")
        user.clear(); user.send_keys(USERNAME)
        pwd.clear();  pwd.send_keys(PASSWORD)
        try:
            driver.find_element(By.XPATH, "//input[@value='Enviar']").click()
        except Exception:
            pwd.send_keys(Keys.ENTER)
        try:
            WebDriverWait(driver, 2).until(EC.alert_is_present())
            Alert(driver).accept()
        except Exception:
            pass
        WebDriverWait(driver, 20).until(lambda d: not is_login_page(d))
    except Exception as e:
        raise RuntimeError(f"Login failed: {e}")

def go_hub_authenticated(driver):
    for _ in range(3):
        driver.get(HUB_URL)
        if is_login_page(driver):
            do_login(driver)
            driver.get(HUB_URL)
        if not is_login_page(driver):
            return
    raise RuntimeError("Could not get authenticated access to HUB.")

def guard_auth_and(func):
    def wrapper(driver, *a, **kw):
        if is_login_page(driver):
            go_hub_authenticated(driver)
        try:
            out = func(driver, *a, **kw)
        except Exception:
            if is_login_page(driver):
                go_hub_authenticated(driver)
                out = func(driver, *a, **kw)
            else:
                raise
        if is_login_page(driver):
            go_hub_authenticated(driver)
        return out
    return wrapper

# ====== SEARCH / RESULTS ======

@guard_auth_and
def search_in_products(driver, product_term: str):
    go_hub_authenticated(driver)
    form = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr")))
    box = form.find_element(By.NAME, "patron")
    box.clear(); box.send_keys(product_term); box.send_keys(Keys.ENTER)
    WebDriverWait(driver, 20).until(lambda d: d.find_elements(By.CSS_SELECTOR, "a.rprod, form[name^='pr']"))

def enumerate_pairs(driver) -> List[Dict[str, Any]]:
    out = []
    for a in driver.find_elements(By.CSS_SELECTOR, "a.rprod"):
        prod_txt = normalize_ws(a.text) or ""
        href = a.get_attribute("href") or ""
        m = re.search(r"document\.(pr\d+)\.submit", href)
        pr_form = m.group(1) if m else None
        comp_txt = ""
        try:
            rlab = a.find_element(By.XPATH, "following-sibling::a[contains(@class,'rlab')][1]")
            comp_txt = normalize_ws(rlab.text) or ""
        except NoSuchElementException:
            pass
        out.append({"prod": prod_txt, "comp": comp_txt, "pr_form": pr_form})
    return out

@guard_auth_and
def open_exact_pair(driver, product: str, company: str) -> bool:
    rows = enumerate_pairs(driver)
    matches = [r for r in rows if nk(r["prod"]) == nk(product) and nk(r["comp"]) == nk(company)]
    if not matches: return False
    pr = matches[0]["pr_form"]
    if not pr: return False
    driver.execute_script(f"if (document.{pr}) document.{pr}.submit();")
    WebDriverWait(driver, 20).until(
        lambda d: "presentacion" in d.page_source.lower() or d.find_elements(By.CSS_SELECTOR, "tr.lproducto span.tproducto")
    )
    return True

# ====== PRODUCT PAGE PARSING ======

def get_text_safe(root, css):
    try:
        el = root.find_element(By.CSS_SELECTOR, css)
        txt = el.get_attribute("innerText")
        if not txt:
            txt = el.get_attribute("innerHTML")
        return normalize_ws(txt)
    except Exception:
        return None

def collect_coverage(pres_el) -> Dict[str, Any]:
    """Robust coverage parser: normalizes payer keys and reads innerHTML to catch AF/OS in <b> tags."""
    cov: Dict[str, Any] = {}
    try:
        cob = pres_el.find_element(By.CSS_SELECTOR, "table.coberturas")
    except Exception:
        return cov

    current_payer = None
    for tr in cob.find_elements(By.CSS_SELECTOR, "tr"):
        # Payer name (fallback to innerHTML)
        try:
            payer_el = tr.find_element(By.CSS_SELECTOR, "td.obrasn")
            payer_text = normalize_ws(payer_el.get_attribute("innerText")) or normalize_ws(payer_el.get_attribute("innerHTML"))
            if payer_text:
                current_payer = strip_accents(payer_text).upper()
                cov.setdefault(current_payer, {})
        except Exception:
            pass

        # Detail/description
        try:
            detail = normalize_ws(tr.find_element(By.CSS_SELECTOR, "td.obrasd").get_attribute("innerText"))
            if current_payer and detail:
                cov[current_payer]["detail"] = detail
        except Exception:
            pass

        # Amounts: check both left/right amount cells, use innerText first
        for sel in ("td.importesi", "td.importesd"):
            try:
                txt = tr.find_element(By.CSS_SELECTOR, sel).get_attribute("innerText")
                if not txt:
                    txt = tr.find_element(By.CSS_SELECTOR, sel).get_attribute("innerHTML")
                    txt = re.sub(r'<[^>]*>', '', txt)
                for tag, amt in re.findall(r"(AF|OS)[^<]*?[\$]?([\d\.,]+)", txt or "", flags=re.I):
                    val = ar_money_to_float(amt)
                    if val is not None and current_payer:
                        cov[current_payer][tag.upper()] = val
            except Exception:
                pass
    return cov

@guard_auth_and
def extract_rows(driver, in_company, in_product):
    # Header/meta from the product page
    active = get_text_safe(driver, "tr.sproducto td.textoe i")           # active_ingredient
    therap = get_text_safe(driver, "tr.sproducto td.textor i")           # therapeutic_class
    comp   = get_text_safe(driver, "tr.lproducto td.textor .defecto") or \
             get_text_safe(driver, "td.textoe b")                        # company
    pname  = get_text_safe(driver, "tr.lproducto span.tproducto")        # product_name

    rows: List[Dict[str, Any]] = []
    pres = driver.find_elements(By.CSS_SELECTOR, "td.dproducto > table.presentacion")
    for p in pres:
        desc  = get_text_safe(p, "td.tddesc")
        price = get_text_safe(p, "td.tdprecio")
        datev = get_text_safe(p, "td.tdfecha")

        import_status = get_text_safe(p, "td.import")  # may be None
        cov = collect_coverage(p)

        rows.append({
            "input_company": in_company,
            "input_product_name": in_product,
            "company": comp,
            "product_name": pname,
            "active_ingredient": active,
            "therapeutic_class": therap,
            "description": desc,
            "price_ars": ar_money_to_float(price or ""),
            "date": parse_date(datev or ""),
            "scraped_at": ts(),

            # Five priority coverage fields
            "SIFAR_detail": (cov.get("SIFAR") or {}).get("detail"),
            "PAMI_AF":      (cov.get("PAMI")  or {}).get("AF"),
            "IOMA_detail":  (cov.get("IOMA")  or {}).get("detail"),
            "IOMA_AF":      (cov.get("IOMA")  or {}).get("AF"),
            "IOMA_OS":      (cov.get("IOMA")  or {}).get("OS"),

            "import_status": import_status,
            "coverage_json": json.dumps(cov, ensure_ascii=False)
        })

    # Fallback if no presentation rows found
    if not rows:
        rows.append({
            "input_company": in_company,
            "input_product_name": in_product,
            "company": comp,
            "product_name": pname,
            "active_ingredient": active,
            "therapeutic_class": therap,
            "description": None,
            "price_ars": None,
            "date": None,
            "scraped_at": ts(),
            "SIFAR_detail": None, "PAMI_AF": None, "IOMA_detail": None, "IOMA_AF": None, "IOMA_OS": None,
            "import_status": None,
            "coverage_json": "{}"
        })
    return rows

# ====== WORKER ======

def worker(q: Queue, args, skip_set: set):
    driver = setup_driver(headless=args.headless)
    while True:
        try:
            in_product, in_company = q.get(timeout=2)
        except Empty:
            break
        try:
            if psutil:
                try:
                    if psutil.cpu_percent(interval=0.1) > 90:
                        time.sleep(0.5)
                except Exception:
                    pass

            if (nk(in_company), nk(in_product)) in skip_set:
                q.task_done(); continue

            search_in_products(driver, in_product)
            if not open_exact_pair(driver, in_product, in_company):
                save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                append_progress(in_company, in_product, 0)
                log.info(f"[NOT_FOUND] {in_company} | {in_product}")
                q.task_done(); continue

            rows = extract_rows(driver, in_company, in_product)
            if rows:
                append_rows(rows)
                append_progress(in_company, in_product, len(rows))
                log.info(f"[SUCCESS] {in_company} | {in_product} → {len(rows)}")
            else:
                save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                append_progress(in_company, in_product, 0)
                log.info(f"[NOT_FOUND] (0 rows) {in_company} | {in_product}")
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            append_error(in_company, in_product, msg)
            save_debug(driver, DEBUG_ERR, f"{in_company}_{in_product}")
            log.error(f"[ERROR] {in_company} | {in_product}: {msg}")
        finally:
            q.task_done()
            human_pause()
    try:
        driver.quit()
    except Exception:
        pass

# ====== MAIN ======

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads", type=int, default=0)
    ap.add_argument("--min-threads", type=int, default=1)
    ap.add_argument("--max-threads", type=int, default=1)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--headless", dest="headless", action="store_true", default=True)
    g.add_argument("--no-headless", dest="headless", action="store_false")
    args = ap.parse_args()
    args.max_threads = max(1, min(int(args.max_threads), 1))

    ensure_headers()
    skip_set = load_progress_set()

    # load targets
    targets: List[Tuple[str, str]] = []
    with open(INPUT_FILE, encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        headers = {nk(h): h for h in (r.fieldnames or [])}
        pcol = headers.get(nk("Product")) or headers.get("product") or "Product"
        ccol = headers.get(nk("Company")) or headers.get("company") or "Company"
        for row in r:
            prod = (row.get(pcol) or "").strip()
            comp = (row.get(ccol) or "").strip()
            if prod and comp:
                targets.append((prod, comp))

    # thread count
    if args.threads > 0:
        n = args.threads
    else:
        cpu = os.cpu_count() or 2
        n = min(max(cpu, args.min_threads), args.max_threads)
        if psutil:
            try:
                if psutil.cpu_percent(interval=0.5) > 70:
                    n = max(1, args.min_threads)
            except Exception:
                pass

    log.info(f"Starting with {n} threads (min={args.min_threads}, max={args.max_threads}); targets={len(targets)}")

    q = Queue()
    for t in targets: q.put(t)
    threads = [threading.Thread(target=worker, args=(q, args, skip_set), daemon=True) for _ in range(n)]
    for t in threads: t.start()
    for t in threads: t.join()
    log.info("All done.")

if __name__ == "__main__":
    main()
