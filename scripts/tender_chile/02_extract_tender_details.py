#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 2/5: Extract Tender Details (WDAC/AppLocker safe)
=====================================================
Reads from PostgreSQL table tc_tender_redirects and extracts tender + lot details.
PostgreSQL is the ONLY source of truth.

IMPORTANT:
- No webdriver-manager (Windows policy blocks .wdm executables).
- Set env var CHROMEDRIVER_PATH to an allowlisted chromedriver.exe.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException


# ----------------------------
# Repo root + script dir wiring
# ----------------------------
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Config loader (optional)
try:
    from config_loader import (
        load_env_file, getenv_int, getenv_bool,
        get_output_dir as _get_output_dir
    )
    load_env_file()
    _CONFIG = True
except Exception:
    _CONFIG = False


def clean(s):
    return re.sub(r"\s+", " ", str(s or "")).strip()


def _strip_accents(s: str) -> str:
    if not s:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def norm_key(s: str) -> str:
    return _strip_accents(clean(s).upper()).upper()


QUALITY_KEYS = [
    "TECNICO",
    "TECHNICAL",
    "CALIDAD TECNICA DE LOS BIENES O SERVICIOS",
    "TECNICA",
    "CALIDAD DEL PRODUCTO",
    "EVALUACION TECNICA",
    "EVALUACION TECNICA DE MEDICAMENTOS",
    "TECHNICAL PROPOSAL OF THE OFFER",
]

PRICE_KEYS = [
    "PRECIO",
    "ECONOMIC",
    "PRICE",
    "OFERTA ECONOMICA",
    "ECONOMICA",
]


def extract_numeric_quantity(raw_qty: str) -> str:
    s = clean(raw_qty)
    if not s:
        return ""
    m = re.search(r"(\d[\d\.,\s]*)", s)
    if not m:
        return ""
    token = m.group(1).replace(" ", "")
    if "," in token and "." in token:
        if token.rfind(",") > token.rfind("."):
            token = token.replace(".", "").split(",", 1)[0]
        else:
            token = token.replace(",", "").split(".", 1)[0]
    elif "," in token:
        token = "".join(token.split(","))
    elif "." in token:
        token = "".join(token.split("."))
    token = re.sub(r"\D", "", token)
    return token


def validate_url(url: str) -> bool:
    return isinstance(url, str) and "DetailsAcquisition.aspx" in url


OUTPUT_FILENAME = "tender_details.csv"  # CSV export only (PostgreSQL is source of truth)

if _CONFIG:
    MAX_TENDERS = getenv_int("MAX_TENDERS", 100)
    HEADLESS_MODE = getenv_bool("HEADLESS", True)
else:
    MAX_TENDERS = int(os.getenv("MAX_TENDERS", "100"))
    HEADLESS_MODE = os.getenv("HEADLESS", "True").lower() == "true"


def get_output_dir() -> Path:
    if _CONFIG:
        return _get_output_dir()
    return _repo_root / "output" / "Tender_Chile"


# ==========================================================
# WDAC/AppLocker SAFE ChromeDriver resolver (NO webdriver-manager)
# ==========================================================
def _resolve_chromedriver_path_strict() -> Optional[str]:
    env_path = (os.getenv("CHROMEDRIVER_PATH") or "").strip().strip('"')
    if env_path:
        p = Path(env_path)
        if p.exists():
            return str(p)
        raise RuntimeError(f"CHROMEDRIVER_PATH is set but file not found: {env_path}")

    try:
        from core.browser.chrome_manager import get_chromedriver_path  # type: ignore
        p2 = get_chromedriver_path()
        if p2 and Path(p2).exists():
            return str(Path(p2))
    except Exception:
        pass

    return None


def build_driver(headless: bool = False) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--lang=es-CL")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    
    # Aggressive performance optimizations
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-logging")
    opts.add_argument("--disable-web-security")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-javascript")  # Disable JS if not needed for static content

    # Block images and CSS for faster loading (we only need HTML)
    prefs = {
        "profile.managed_default_content_settings.images": 2,  # Block images
        "profile.managed_default_content_settings.stylesheets": 2,  # Block CSS
        "profile.managed_default_content_settings.javascript": 2,  # Block JavaScript
        "profile.default_content_setting_values.notifications": 2,  # Block notifications
        "disk-cache-size": 4096,  # Minimal cache
    }
    opts.add_experimental_option("prefs", prefs)

    # Apply stealth/anti-bot features
    try:
        from core.browser.stealth_profile import apply_selenium
        apply_selenium(opts)
    except ImportError:
        pass  # Stealth profile not available, continue without it

    # keep performance logs capability (your network parsing depends on it)
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    chrome_bin = (os.getenv("CHROME_BINARY") or "").strip().strip('"')
    if chrome_bin:
        opts.binary_location = chrome_bin

    driver_path = _resolve_chromedriver_path_strict()

    try:
        if driver_path:
            driver = webdriver.Chrome(service=Service(driver_path), options=opts)
        else:
            driver = webdriver.Chrome(options=opts)  # Selenium Manager fallback
    except OSError as e:
        msg = str(e)
        if "4551" in msg or "Application Control policy" in msg:
            raise RuntimeError(
                "Windows Application Control policy blocked chromedriver.exe.\n"
                "Fix:\n"
                "  1) Put chromedriver.exe in an allowlisted folder (example: D:\\quad99\\tools\\chromedriver.exe)\n"
                "  2) Set env var CHROMEDRIVER_PATH to that full path\n"
                "     PowerShell: setx CHROMEDRIVER_PATH \"D:\\quad99\\tools\\chromedriver.exe\"\n"
                "  3) Restart terminal and rerun.\n"
            ) from e
        raise

    driver.set_page_load_timeout(120)
    
    # Track Chrome PIDs in DB for pipeline stop cleanup
    try:
        from core.browser.chrome_pid_tracker import get_chrome_pids_from_driver
        from core.browser.chrome_instance_tracker import ChromeInstanceTracker
        from core.db.connection import CountryDB
        run_id = os.getenv("TENDER_CHILE_RUN_ID", "").strip()
        if not run_id:
            run_id_file = get_output_dir() / ".current_run_id"
            if run_id_file.exists():
                run_id = run_id_file.read_text(encoding="utf-8").strip()
        pids = get_chrome_pids_from_driver(driver)
        if pids and run_id:
            driver_pid = driver.service.process.pid if hasattr(driver.service, 'process') else list(pids)[0]
            db = CountryDB("Tender_Chile")
            db.connect()
            try:
                tracker = ChromeInstanceTracker("Tender_Chile", run_id, db)
                tracker.register(step_number=2, pid=driver_pid, browser_type="chrome", child_pids=pids)
            finally:
                db.close()
    except Exception:
        pass
    
    return driver


def extract_tender_data(driver) -> Dict[str, Any]:
    tender_data = {
        "Tender ID": "",
        "Tender Title": "",
        "TENDERING AUTHORITY": "",
        "PROVINCE": "",
        "Closing Date": "",
        "Price Evaluation ratio": None,
        "Quality Evaluation ratio": None,
        "Other Evaluation ratio": None,
    }

    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    # Title
    try:
        tender_data["Tender Title"] = clean(driver.find_element(By.ID, "lblFicha1Nombre").text)
    except Exception:
        pass

    # Tender ID
    try:
        tender_data["Tender ID"] = clean(driver.find_element(By.ID, "lblNumLicitacion").text)
    except Exception:
        pass

    # Authority
    try:
        tender_data["TENDERING AUTHORITY"] = clean(driver.find_element(By.ID, "lnkFicha2Razon").text)
    except Exception:
        try:
            tender_data["TENDERING AUTHORITY"] = clean(driver.find_element(By.ID, "lblFicha2Razon").text)
        except Exception:
            pass

    # Province (robust)
    province_val = ""
    candidate_ids = [
        "lblFicha2Region",
        "lblFicha2Comuna",
        "lblRegion",
        "lblComuna",
        "lblFicha2Ubicacion",
        "lblUbicacion",
    ]
    for cid in candidate_ids:
        try:
            txt = clean(driver.find_element(By.ID, cid).text)
            if txt:
                province_val = txt
                break
        except Exception:
            continue

    if not province_val:
        try:
            body_text = clean(driver.find_element(By.TAG_NAME, "body").text)
            m = re.search(r"(?:REGI[ÓO]N)\s*:\s*([A-Za-zÁÉÍÓÚÜÑáéíóúüñ\-\s]+)", body_text, flags=re.IGNORECASE)
            if m:
                province_val = clean(m.group(1))
            if not province_val:
                m = re.search(r"(?:COMUNA)\s*:\s*([A-Za-zÁÉÍÓÚÜÑáéíóúüñ\-\s]+)", body_text, flags=re.IGNORECASE)
                if m:
                    province_val = clean(m.group(1))
        except Exception:
            pass

    tender_data["PROVINCE"] = province_val

    # Closing
    try:
        tender_data["Closing Date"] = clean(driver.find_element(By.ID, "lblFicha3Cierre").text)
    except Exception:
        pass

    # Ratios
    try:
        criteria_rows = driver.find_elements(By.CSS_SELECTOR, "#grvCriterios tr.estiloSeparador")
        for row in criteria_rows:
            try:
                nombre_elem = row.find_element(By.CSS_SELECTOR, "[id*='lblNombreCriterio']")
                ponderacion_elem = row.find_element(By.CSS_SELECTOR, "[id*='lblPonderacion']")

                nombre = norm_key(nombre_elem.text)
                ponderacion_text = clean(ponderacion_elem.text)

                pct_match = re.search(r"(\d+)%", ponderacion_text)
                if not pct_match:
                    continue

                pct = int(pct_match.group(1))

                if any(k in nombre for k in PRICE_KEYS):
                    tender_data["Price Evaluation ratio"] = pct
                elif any(k in nombre for k in QUALITY_KEYS):
                    tender_data["Quality Evaluation ratio"] = pct
                else:
                    tender_data["Other Evaluation ratio"] = (tender_data["Other Evaluation ratio"] or 0) + pct
            except Exception:
                continue
    except Exception:
        pass

    return tender_data


def extract_items_from_html(driver) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    try:
        producto_elems = driver.find_elements(By.CSS_SELECTOR, "[id^='grvProducto_ctl'][id*='lblProducto']")
        for producto_elem in producto_elems:
            try:
                elem_id = producto_elem.get_attribute("id")
                control_match = re.search(r"(grvProducto_ctl\d+)", elem_id)
                if not control_match:
                    continue

                control_prefix = control_match.group(1)
                cantidad_elem = driver.find_element(By.ID, f"{control_prefix}_lblCantidad")
                unidad_elem = driver.find_element(By.ID, f"{control_prefix}_lblUnidad")
                descripcion_elem = driver.find_element(By.ID, f"{control_prefix}_lblDescripcion")
                categoria_elem = driver.find_element(By.ID, f"{control_prefix}_lblCategoria")

                items.append({
                    "CodigoProducto": clean(categoria_elem.text),
                    "NombreProducto": clean(producto_elem.text),
                    "Descripcion": clean(descripcion_elem.text),
                    "Cantidad": extract_numeric_quantity(cantidad_elem.text),
                    "Unidad": clean(unidad_elem.text),
                })
            except Exception:
                continue
    except Exception:
        pass
    return items


def extract_items_from_network(driver) -> List[Dict[str, Any]]:
    logs = driver.get_log("performance")
    for entry in logs:
        try:
            msg = json.loads(entry["message"])["message"]
            if msg.get("method") != "Network.responseReceived":
                continue

            response = msg["params"]["response"]
            url = response.get("url", "")
            if not url:
                continue

            if ("RfbService" not in url and "GetItems" not in url and "Item" not in url and "Producto" not in url):
                continue

            request_id = msg["params"]["requestId"]
            body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
            text = body.get("body", "")
            if not text:
                continue

            data = json.loads(text)

            if isinstance(data, dict):
                # direct list values
                for v in data.values():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        if any(k in v[0] for k in ["CodigoProducto", "NombreProducto", "Descripcion", "Cantidad"]):
                            return v
                    if isinstance(v, dict):
                        for nv in v.values():
                            if isinstance(nv, list) and nv and isinstance(nv[0], dict):
                                if any(k in nv[0] for k in ["CodigoProducto", "NombreProducto", "Descripcion", "Cantidad"]):
                                    return nv
        except Exception:
            continue

    return []


def extract_single_tender(url: str, headless: bool = False) -> Optional[Dict[str, Any]]:
    if not validate_url(url):
        print(f"[WARN] Invalid URL: {url}")
        return None

    driver = None
    instance_id = None
    try:
        driver = build_driver(headless=headless)
        
        # Get instance ID if tracking was successful
        try:
            from core.browser.chrome_instance_tracker import ChromeInstanceTracker
            run_id = os.getenv("TENDER_CHILE_RUN_ID", "")
            if run_id and hasattr(driver.service, 'process'):
                from core.db.connection import CountryDB
                db = CountryDB("Tender_Chile")
                tracker = ChromeInstanceTracker("Tender_Chile", run_id, db)
                pid = driver.service.process.pid
                # Instance already registered in build_driver, just get the ID
                # We'll mark it terminated in finally
        except Exception:
            pass

        for attempt in range(2):
            try:
                driver.get(url)
                break
            except WebDriverException:
                if attempt == 0:
                    time.sleep(3)
                else:
                    raise

        time.sleep(2)  # Reduced from 6s - no images/CSS/JS to load

        tender_data = extract_tender_data(driver)

        items = extract_items_from_network(driver)
        if not items:
            items = extract_items_from_html(driver)
        if not items:
            items = []

        rows: List[Dict[str, Any]] = []
        if items:
            for i, it in enumerate(items, start=1):
                rows.append({
                    **tender_data,
                    "Lot Number": i,
                    "Unique Lot ID": it.get("CodigoProducto", ""),
                    "Generic name": clean(it.get("NombreProducto", "")),
                    "Lot Title": clean(it.get("Descripcion", "")),
                    "Quantity": extract_numeric_quantity(str(it.get("Cantidad", ""))),
                    "Source URL": url,
                })
        else:
            rows.append({
                **tender_data,
                "Lot Number": None,
                "Unique Lot ID": None,
                "Generic name": None,
                "Lot Title": None,
                "Quantity": None,
                "Source URL": url,
            })

        return {"tender_data": tender_data, "lots": rows, "url": url}

    except Exception as e:
        print(f"[ERROR] Error extracting tender: {e}")
        return None
    finally:
        if driver:
            # Mark instance as terminated before quitting
            try:
                from core.browser.chrome_instance_tracker import ChromeInstanceTracker
                run_id = os.getenv("TENDER_CHILE_RUN_ID", "")
                if run_id:
                    from core.db.connection import CountryDB
                    db = CountryDB("Tender_Chile")
                    tracker = ChromeInstanceTracker("Tender_Chile", run_id, db)
                    pid = driver.service.process.pid if hasattr(driver.service, 'process') else None
                    if pid:
                        # Find and mark instance as terminated
                        instances = tracker.list_instances(step_number=2)
                        for inst in instances:
                            if inst.get('pid') == pid and not inst.get('terminated_at'):
                                tracker.mark_terminated(inst['instance_id'], reason="cleanup")
            except Exception:
                pass
            driver.quit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract tender and lot data from MercadoPublico")
    parser.add_argument("url", nargs="?", default=None, help="MercadoPublico tender URL (optional)")
    args = parser.parse_args()

    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.url:
        result = extract_single_tender(args.url, headless=HEADLESS_MODE)
        if not result:
            sys.exit(1)
        df = pd.DataFrame(result["lots"])
        out = output_dir / "tender_details_single.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"[OK] CSV written: {out}")
        return

    # Read from PostgreSQL (PostgreSQL is the ONLY source of truth)
    run_id = os.getenv("TENDER_CHILE_RUN_ID", "")
    if not run_id:
        print("[ERROR] TENDER_CHILE_RUN_ID environment variable not set")
        sys.exit(1)

    try:
        from core.db.connection import CountryDB
        from db.repositories import ChileRepository
        
        db = CountryDB("Tender_Chile")
        db.connect()
        repo = ChileRepository(db, run_id)
        
        # Read redirect URLs from database
        redirects = repo.get_all_tender_redirects()
        tender_urls = []
        for r in redirects:
            # Extract tender_details_url from redirect_url (redirect_url contains the qs parameter)
            redirect_url = (r.get('redirect_url') or '').strip()
            if redirect_url and validate_url(redirect_url):
                tender_urls.append(redirect_url)
        
        print(f"Reading: PostgreSQL table 'tc_tender_redirects'")
        print(f"   Found {len(tender_urls)} redirect URL(s) in database")
        
        db.close()
    except Exception as e:
        print(f"[ERROR] Failed to read from PostgreSQL: {e}")
        print(f"[ERROR] Make sure Step 1 completed successfully and tc_tender_redirects table has data")
        sys.exit(1)

    if not tender_urls:
        print("[ERROR] No valid tender URLs found in database")
        print("[INFO] Run Step 1 first to populate tc_tender_redirects table")
        sys.exit(1)

    # Save to PostgreSQL (PostgreSQL is the ONLY source of truth)
    # Incremental saving with crash recovery
    try:
        from core.db.connection import CountryDB
        from db.repositories import ChileRepository
        
        db = CountryDB("Tender_Chile")
        db.connect()
        repo = ChileRepository(db, run_id)
        
        # Get already-processed URLs to enable resume
        processed_urls = set()
        try:
            existing_details = repo.get_all_tender_details()
            processed_urls = {d.get('source_url', '').strip() for d in existing_details if d.get('source_url')}
            if processed_urls:
                print(f"[RESUME] Found {len(processed_urls)} already-processed URLs, will skip them")
        except Exception:
            pass
        
        all_rows: List[Dict[str, Any]] = []
        batch_details = []
        total_saved = 0
        skipped_count = 0
        BATCH_SIZE = 10  # Commit every 10 tenders for efficiency
        
        for i, url in enumerate(tender_urls[:MAX_TENDERS], start=1):
            # Skip if this exact URL was already processed
            if url in processed_urls:
                skipped_count += 1
                if skipped_count <= 10:  # Only show first 10 skips
                    print(f"[{i}/{min(len(tender_urls), MAX_TENDERS)}] SKIP (already processed): {url[:120]}")
                elif skipped_count == 11:
                    print(f"[RESUME] ... skipping more already-processed tenders (will show count at end)")
                continue
            
            print(f"[{i}/{min(len(tender_urls), MAX_TENDERS)}] Processing: {url[:120]}")
            result = extract_single_tender(url, headless=HEADLESS_MODE)
            if result:
                all_rows.extend(result["lots"])
                
                # Prepare tender detail for database
                tender_data = result.get("tender_data", {})
                tender_id = tender_data.get("Tender ID", "").strip()
                if tender_id:
                    batch_details.append({
                        "tender_id": tender_id,
                        "tender_name": tender_data.get("Tender Title", ""),
                        "tender_status": "",  # Not available in this step
                        "publication_date": "",  # Not available in this step
                        "closing_date": tender_data.get("Closing Date", ""),
                        "organization": tender_data.get("TENDERING AUTHORITY", ""),
                        "province": tender_data.get("PROVINCE", ""),
                        "contact_info": "",  # Not available in this step
                        "description": "",  # Not available in this step
                        "currency": "CLP",
                        "estimated_amount": None,  # Not available in this step
                        "source_url": url,
                    })
                    
                    # Save batch when it reaches BATCH_SIZE
                    if len(batch_details) >= BATCH_SIZE:
                        count = repo.insert_tender_details_bulk(batch_details)
                        db.commit()  # Commit to ensure data is saved
                        total_saved += count
                        print(f"[DB] Batch saved: {count} tenders (total: {total_saved})")
                        batch_details.clear()
            
            time.sleep(0.3)  # Further reduced for faster processing
        
        # Save any remaining batch
        if batch_details:
            count = repo.insert_tender_details_bulk(batch_details)
            db.commit()
            total_saved += count
            print(f"[DB] Final batch saved: {count} tenders (total: {total_saved})")
        
        if skipped_count > 0:
            print(f"[RESUME] Skipped {skipped_count} already-processed tenders")
        
        db.close()
    except Exception as e:
        import traceback
        print(f"[ERROR] Failed during tender extraction: {e}")
        traceback.print_exc()
        print(f"[WARN] Attempting fallback to CSV-only mode...")
        # Fallback: process URLs without DB save
        all_rows = []
        for i, url in enumerate(tender_urls[:MAX_TENDERS], start=1):
            print(f"[{i}/{min(len(tender_urls), MAX_TENDERS)}] {url[:120]}")
            result = extract_single_tender(url, headless=HEADLESS_MODE)
            if result:
                all_rows.extend(result["lots"])
            time.sleep(2)

    if not all_rows:
        print("[ERROR] No tender data extracted.")
        sys.exit(1)

    # Also write CSV for export (PostgreSQL is source of truth, CSV is export only)
    df = pd.DataFrame(all_rows)
    col_order = [
        "Tender ID", "Tender Title", "TENDERING AUTHORITY", "PROVINCE", "Closing Date",
        "Price Evaluation ratio", "Quality Evaluation ratio", "Other Evaluation ratio",
        "Lot Number", "Unique Lot ID", "Generic name", "Lot Title", "Quantity", "Source URL"
    ]
    df = df.reindex(columns=col_order)

    out = output_dir / OUTPUT_FILENAME
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[OK] CSV export written: {out}")


if __name__ == "__main__":
    main()
