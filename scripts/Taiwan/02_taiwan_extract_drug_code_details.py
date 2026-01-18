import csv
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

# Add repo root to path for shared imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add script dir to path for local config_loader
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

try:
    from config_loader import load_env_file, getenv, getenv_bool, get_output_dir, get_central_output_dir
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False

    def getenv(key: str, default: str = "") -> str:
        return os.getenv(key, default)

    def getenv_bool(key: str, default: bool = False) -> bool:
        val = os.getenv(key, str(default))
        return str(val).lower() in ("true", "1", "yes", "on")

    def get_output_dir() -> Path:
        return Path(__file__).parent

    def get_central_output_dir() -> Path:
        return Path(__file__).parent

try:
    from core.chrome_pid_tracker import (
        get_chrome_pids_from_driver,
        save_chrome_pids,
        terminate_scraper_pids,
    )
except ImportError:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None
    terminate_scraper_pids = None

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

SCRAPER_NAME = "Taiwan"

OUTPUT_DIR = get_output_dir()
EXPORTS_DIR = get_central_output_dir()
IN_CODES = OUTPUT_DIR / getenv("SCRIPT_02_IN_CODES", "taiwan_drug_code_urls.csv")
OUT_DETAILS = EXPORTS_DIR / getenv("SCRIPT_02_OUT_DETAILS", "taiwan_drug_code_details.csv")

SEEN_LICIDS = OUTPUT_DIR / getenv("SCRIPT_02_SEEN_LICIDS", "seen_licids.txt")
SEEN_COMPANIES = OUTPUT_DIR / getenv("SCRIPT_02_SEEN_COMPANIES", "seen_companies.txt")

WAIT_TIMEOUT = 30
PAGE_LOAD_TIMEOUT = 60
SLEEP = 0.25

DETAILS_READY_XPATH = (
    "//*[contains(normalize-space(),'Certificate Information') or "
    "contains(normalize-space(),'\u8b49\u66f8\u8cc7\u6599')]"
)
DETAILS_DATA_SELECTOR = "p.searchFormItem, div.searchFormData"
NO_DATA_MARKERS = [
    "No data",
    "No Data",
    "NO DATA",
    "No results",
    "No result",
    "No records",
    "No Record",
    "No matching",
    "No matching records",
    "\u67e5\u7121\u8cc7\u6599",
    "\u7121\u8cc7\u6599",
    "\u67e5\u7121\u6b64\u8cc7\u6599",
    "\u7121\u6b64\u8cc7\u6599",
]


def configure_realtime_output() -> None:
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    try:
        sys.stderr.reconfigure(line_buffering=True)
    except Exception:
        pass


def percent(current: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((current / total) * 100, 1)


def ensure_driver() -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    if getenv_bool("SCRIPT_02_HEADLESS", False):
        opts.add_argument("--headless=new")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids(SCRAPER_NAME, _repo_root, pids)
        except Exception:
            pass
    return driver


def ensure_out_header():
    if OUT_DETAILS.exists():
        return
    with open(OUT_DETAILS, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "Drug Code",
            "licId URL",
            # Certificate
            "Valid Date (ROC)",
            "Valid Date (AD)",
            "Original certificate date / issuance date (ROC)",
            "Types of licenses",
            "Customs clearance document number",
            "Chinese Product Name",
            "English product name",
            "Indications",
            "Dosage form",
            "Package",
            "Drug Category",
            "ATC code",
            "Principal Components (Brief Description)",
            "Restricted items",
            # Applicant
            "Drug Company Name",
            "Drugstore address",
            # Manufacturer
            "Manufacturer code",
            "Factory",
            "Manufacturer Name",
            "Manufacturing plant address",
            "Manufacturing plant company address",
            "Country of manufacture",
            "process",
        ])


def load_csv_codes() -> List[Tuple[str, str]]:
    rows = []
    with open(IN_CODES, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            code = (row.get("Drug Code") or "").strip()
            url = (row.get("Drug Code URL") or "").strip()
            if code and url:
                rows.append((code, url))
    return rows


def load_seen(path: str) -> Set[str]:
    if not Path(path).exists():
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return set(x.strip() for x in f if x.strip())


def append_seen(path: str, value: str):
    with open(path, "a", encoding="utf-8") as f:
        f.write(value + "\n")


def roc_to_ad(roc: str) -> str:
    if not roc:
        return ""
    roc = roc.strip().replace("/", "-").replace(".", "-")
    m = re.match(r"^(\d{2,3})-(\d{1,2})-(\d{1,2})$", roc)
    if not m:
        return roc
    y = int(m.group(1)) + 1911
    mm = int(m.group(2))
    dd = int(m.group(3))
    return f"{y:04d}-{mm:02d}-{dd:02d}"


def has_no_results(driver) -> bool:
    page_source = driver.page_source or ""
    for marker in NO_DATA_MARKERS:
        if marker in page_source:
            return True
    return False


def wait_page_ready(driver, wait) -> bool:
    try:
        wait.until(
            lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
        )
        wait.until(
            lambda d: d.find_elements(By.CSS_SELECTOR, DETAILS_DATA_SELECTOR)
            or d.find_elements(By.XPATH, DETAILS_READY_XPATH)
            or has_no_results(d)
        )
        return True
    except TimeoutException:
        return False


def get_by_label(driver, label_en: str, label_zh: str = "") -> str:
    # Works with your structure: p.searchFormItem + following div.searchFormData
    if label_zh:
        xp = (
            f"//p[contains(@class,'searchFormItem') and "
            f"(normalize-space()='{label_en}' or normalize-space()='{label_zh}')]"
            f"/following::div[contains(@class,'searchFormData')][1]"
        )
    else:
        xp = (
            f"//p[contains(@class,'searchFormItem') and normalize-space()='{label_en}']"
            f"/following::div[contains(@class,'searchFormData')][1]"
        )
    els = driver.find_elements(By.XPATH, xp)
    if not els:
        return ""
    return (els[0].text or "").strip()


def scrape_license(driver, wait, url: str) -> Dict[str, str]:
    try:
        driver.get(url)
    except TimeoutException:
        try:
            driver.execute_script("window.stop();")
        except Exception:
            pass
    if not wait_page_ready(driver, wait):
        print(f"[WARN] Timed out waiting for details page: {url}", flush=True)
        return {}
    if has_no_results(driver):
        print(f"[WARN] No details found for URL: {url}", flush=True)
        return {}
    time.sleep(SLEEP)

    out = {}
    out["Valid Date (ROC)"] = get_by_label(driver, "Valid Date", "有效日期")
    out["Valid Date (AD)"] = roc_to_ad(out["Valid Date (ROC)"])

    out["Original certificate date / issuance date (ROC)"] = get_by_label(
        driver, "Original certificate date / certificate issuance date", "原始發證日期 / 發證日期"
    )
    out["Types of licenses"] = get_by_label(driver, "Types of licenses", "許可證種類")
    out["Customs clearance document number"] = get_by_label(driver, "Customs clearance document number", "通關簽審文件編號")
    out["Chinese Product Name"] = get_by_label(driver, "Chinese Product Name", "中文品名")
    out["English product name"] = get_by_label(driver, "English product name", "英文品名")
    out["Indications"] = get_by_label(driver, "Indications", "適應症")
    out["Dosage form"] = get_by_label(driver, "Dosage form", "劑型")
    out["Package"] = get_by_label(driver, "Package", "包裝")
    out["Drug Category"] = get_by_label(driver, "Drug Category", "藥品類別")
    out["ATC code"] = get_by_label(driver, "Pharmacological therapy classification (ATC code)", "藥理治療分類(ATC code)")
    out["Principal Components (Brief Description)"] = get_by_label(driver, "Principal Components (Brief Description)", "主成分略述")
    out["Restricted items"] = get_by_label(driver, "Restricted items", "限制項目")

    out["Drug Company Name"] = get_by_label(driver, "Drug Company Name", "藥商名稱")
    out["Drugstore address"] = get_by_label(driver, "Drugstore address", "藥商地址")

    out["Manufacturer code"] = get_by_label(driver, "Manufacturer code", "製造廠代碼")
    out["Factory"] = get_by_label(driver, "Factory", "廠別")
    out["Manufacturer Name"] = get_by_label(driver, "Manufacturer Name", "製造廠名稱")
    out["Manufacturing plant address"] = get_by_label(driver, "Manufacturing plant address", "製造廠地址")
    out["Manufacturing plant company address"] = get_by_label(driver, "Manufacturing plant company address", "製造廠公司地址")
    out["Country of manufacture"] = get_by_label(driver, "Country of manufacture", "製造廠國別")
    out["process"] = get_by_label(driver, "process", "製程")

    return out


def main():
    configure_realtime_output()
    if terminate_scraper_pids:
        try:
            terminate_scraper_pids(SCRAPER_NAME, _repo_root, silent=True)
        except Exception:
            pass

    if not IN_CODES.exists():
        raise FileNotFoundError(f"Missing {IN_CODES}. Run script 01 first.")

    ensure_out_header()

    seen_licids = load_seen(SEEN_LICIDS)
    seen_companies = load_seen(SEEN_COMPANIES)

    rows = load_csv_codes()
    total_rows = len(rows)
    if total_rows:
        print(
            f"[PROGRESS] Extracting details: 0/{total_rows} (0.0%) - Starting",
            flush=True,
        )
    else:
        print("[INFO] No rows found in input codes file.", flush=True)

    driver = ensure_driver()
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        for idx, (drug_code, url) in enumerate(rows, 1):
            # licId is the query param value
            licid = ""
            m = re.search(r"[?&]licId=([^&]+)", url)
            if m:
                licid = m.group(1)

            if licid and licid in seen_licids:
                progress_pct = percent(idx, total_rows)
                print(
                    f"[PROGRESS] Extracting details: {idx}/{total_rows} ({progress_pct}%) - "
                    f"Skipped {drug_code} (licId {licid})",
                    flush=True,
                )
                continue

            progress_pct = percent(idx, total_rows)
            print(
                f"[PROGRESS] Extracting details: {idx}/{total_rows} ({progress_pct}%) - "
                f"{drug_code} (licId {licid or 'unknown'})",
                flush=True,
            )

            details = scrape_license(driver, wait, url)
            company = (details.get("Drug Company Name") or "").strip()

            # Dedup company: if already processed, blank out company-level fields
            if company and company in seen_companies:
                # Keep Drug Code / URL / dates, but avoid repeating heavy company fields
                # (You asked: don’t scrape same company name again)
                details["Drugstore address"] = ""
                details["Manufacturer code"] = ""
                details["Factory"] = ""
                details["Manufacturer Name"] = ""
                details["Manufacturing plant address"] = ""
                details["Manufacturing plant company address"] = ""
                details["Country of manufacture"] = ""
                details["process"] = ""
                print(f"[DETAIL] Dedup company details: {company}", flush=True)
            else:
                if company:
                    seen_companies.add(company)
                    append_seen(SEEN_COMPANIES, company)
                    print(f"[DETAIL] New company captured: {company}", flush=True)

            with open(OUT_DETAILS, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    drug_code,
                    url,
                    details.get("Valid Date (ROC)", ""),
                    details.get("Valid Date (AD)", ""),
                    details.get("Original certificate date / issuance date (ROC)", ""),
                    details.get("Types of licenses", ""),
                    details.get("Customs clearance document number", ""),
                    details.get("Chinese Product Name", ""),
                    details.get("English product name", ""),
                    details.get("Indications", ""),
                    details.get("Dosage form", ""),
                    details.get("Package", ""),
                    details.get("Drug Category", ""),
                    details.get("ATC code", ""),
                    details.get("Principal Components (Brief Description)", ""),
                    details.get("Restricted items", ""),
                    details.get("Drug Company Name", ""),
                    details.get("Drugstore address", ""),
                    details.get("Manufacturer code", ""),
                    details.get("Factory", ""),
                    details.get("Manufacturer Name", ""),
                    details.get("Manufacturing plant address", ""),
                    details.get("Manufacturing plant company address", ""),
                    details.get("Country of manufacture", ""),
                    details.get("process", ""),
                ])

            if licid:
                seen_licids.add(licid)
                append_seen(SEEN_LICIDS, licid)

            time.sleep(SLEEP)

        print("DONE:", OUT_DETAILS, flush=True)

    finally:
        driver.quit()
        if terminate_scraper_pids:
            try:
                terminate_scraper_pids(SCRAPER_NAME, _repo_root, silent=True)
            except Exception:
                pass


if __name__ == "__main__":
    main()
