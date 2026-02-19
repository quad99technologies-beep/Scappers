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
    from core.browser.chrome_pid_tracker import (
        get_chrome_pids_from_driver,
        terminate_scraper_pids,
    )
    from core.browser.chrome_instance_tracker import ChromeInstanceTracker
    from core.db.postgres_connection import PostgresDB
except ImportError:
    get_chrome_pids_from_driver = None
    terminate_scraper_pids = None
    ChromeInstanceTracker = None
    PostgresDB = None

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

SCRAPER_NAME = "Taiwan"

# DB imports
try:
    from core.db.connection import CountryDB
    from db.schema import apply_taiwan_schema
    from db.repositories import TaiwanRepository
    HAS_DB = True
except ImportError:
    HAS_DB = False

SCRAPER_NAME = "Taiwan"

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
    
    # Try to register with ChromeInstanceTracker (only if DB/RunID available)
    run_id = os.environ.get("TW_RUN_ID")
    if ChromeInstanceTracker and run_id:
        try:
            from core.db.connection import CountryDB
            db = CountryDB("Taiwan") 
            db.connect()
            tracker = ChromeInstanceTracker("Taiwan", run_id, db)
            pid = driver.service.process.pid if hasattr(driver.service, 'process') else None
            if pid:
                pids = get_chrome_pids_from_driver(driver) if get_chrome_pids_from_driver else {pid}
                tracker.register(step_number=2, pid=pid, browser_type="chrome", child_pids=pids)
            db.close()
        except Exception:
            pass

    return driver


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
    
    if not HAS_DB:
        print("[ERROR] Database support not available. Cannot run Taiwan extractor.")
        sys.exit(1)

    # Resolve run_id
    run_id = os.environ.get("TW_RUN_ID", "").strip()
    if not run_id:
        print("[ERROR] No TW_RUN_ID. Set it or run pipeline from step 0.")
        sys.exit(1)

    # Initialize database
    db = CountryDB("Taiwan")
    db.connect()
    apply_taiwan_schema(db)
    repo = TaiwanRepository(db, run_id)
    repo.start_run(mode="resume")

    # Load tasks (drug codes from step 1)
    tasks = repo.get_all_drug_codes()
    total_tasks = len(tasks)
    
    # Get completed details to skip
    completed_codes = repo.get_completed_keys(step_number=2)
    
    if total_tasks:
        print(
            f"[PROGRESS] Extracting details: {len(completed_codes)}/{total_tasks} ({percent(len(completed_codes), total_tasks)}%)",
            flush=True,
        )
    else:
        print("[INFO] No drug codes found in database for this run.", flush=True)
        return

    driver = ensure_driver()
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        for idx, task in enumerate(tasks, 1):
            drug_code = task.get("drug_code")
            url = task.get("drug_code_url")
            
            if drug_code in completed_codes:
                continue

            repo.mark_progress(2, "extract_details", drug_code, "in_progress")
            
            progress_pct = percent(idx, total_tasks)
            print(
                f"[PROGRESS] Extracting details: {idx}/{total_tasks} ({progress_pct}%) - {drug_code}",
                flush=True,
            )

            details_data = scrape_license(driver, wait, url)
            if not details_data:
                repo.mark_progress(2, "extract_details", drug_code, "failed", error_message="Scraping failed")
                continue

            # Save to DB
            db_detail = {
                "drug_code": drug_code,
                "lic_id_url": url,
                "valid_date_roc": details_data.get("Valid Date (ROC)"),
                "valid_date_ad": details_data.get("Valid Date (AD)"),
                "original_certificate_date": details_data.get("Original certificate date / issuance date (ROC)"),
                "license_type": details_data.get("Types of licenses"),
                "customs_doc_number": details_data.get("Customs clearance document number"),
                "chinese_product_name": details_data.get("Chinese Product Name"),
                "english_product_name": details_data.get("English product name"),
                "indications": details_data.get("Indications"),
                "dosage_form": details_data.get("Dosage form"),
                "package": details_data.get("Package"),
                "drug_category": details_data.get("Drug Category"),
                "atc_code": details_data.get("ATC code"),
                "principal_components": details_data.get("Principal Components (Brief Description)"),
                "restricted_items": details_data.get("Restricted items"),
                "drug_company_name": details_data.get("Drug Company Name"),
                "drugstore_address": details_data.get("Drugstore address"),
                "manufacturer_code": details_data.get("Manufacturer code"),
                "factory": details_data.get("Factory"),
                "manufacturer_name": details_data.get("Manufacturer Name"),
                "manufacturing_plant_address": details_data.get("Manufacturing plant address"),
                "manufacturing_plant_company_address": details_data.get("Manufacturing plant company address"),
                "country_of_manufacture": details_data.get("Country of manufacture"),
                "process_description": details_data.get("process"),
            }
            
            repo.insert_drug_details([db_detail])
            repo.mark_progress(2, "extract_details", drug_code, "completed")

            time.sleep(SLEEP)

        print("\n[DONE] Detail extraction complete.")

    except KeyboardInterrupt:
        print("\n[STOP] Interrupted by user.")
    except Exception as e:
        print(f"\n[FATAL] {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        if db:
            db.close()


if __name__ == "__main__":
    main()
