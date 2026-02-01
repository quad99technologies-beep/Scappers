#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 3/5: Extract Tender Details (WDAC/AppLocker safe)
=====================================================
Reads output/Tender_Chile/tender_redirect_urls.csv and extracts tender + lot details.

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


INPUT_FILENAME = "tender_redirect_urls.csv"
OUTPUT_FILENAME = "tender_details.csv"
REQUIRED_INPUT_COLUMN = "tender_details_url"

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
        from core.chrome_manager import get_chromedriver_path  # type: ignore
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
    try:
        driver = build_driver(headless=headless)

        for attempt in range(2):
            try:
                driver.get(url)
                break
            except WebDriverException:
                if attempt == 0:
                    time.sleep(3)
                else:
                    raise

        time.sleep(6)

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

    input_path = output_dir / INPUT_FILENAME
    if not input_path.exists():
        print(f"[ERROR] {input_path} not found. Run Script 1 first.")
        sys.exit(1)

    # Validate required column
    with open(input_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if REQUIRED_INPUT_COLUMN not in (reader.fieldnames or []):
            print(f"[ERROR] Required column '{REQUIRED_INPUT_COLUMN}' not found in {input_path}")
            print(f"   Available columns: {', '.join(reader.fieldnames or [])}")
            sys.exit(1)

    tender_urls: List[str] = []
    with open(input_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u = (row.get(REQUIRED_INPUT_COLUMN) or "").strip()
            if u and (u.startswith("http://") or u.startswith("https://")) and validate_url(u):
                tender_urls.append(u)

    if not tender_urls:
        print("[ERROR] No valid tender URLs found in CSV")
        sys.exit(1)

    all_rows: List[Dict[str, Any]] = []
    for i, url in enumerate(tender_urls[:MAX_TENDERS], start=1):
        print(f"[{i}/{min(len(tender_urls), MAX_TENDERS)}] {url[:80]}")
        result = extract_single_tender(url, headless=HEADLESS_MODE)
        if result:
            all_rows.extend(result["lots"])
        time.sleep(2)

    if not all_rows:
        print("[ERROR] No tender data extracted.")
        sys.exit(1)

    df = pd.DataFrame(all_rows)
    col_order = [
        "Tender ID", "Tender Title", "TENDERING AUTHORITY", "PROVINCE", "Closing Date",
        "Price Evaluation ratio", "Quality Evaluation ratio", "Other Evaluation ratio",
        "Lot Number", "Unique Lot ID", "Generic name", "Lot Title", "Quantity", "Source URL"
    ]
    df = df.reindex(columns=col_order)

    out = output_dir / OUTPUT_FILENAME
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[OK] CSV written: {out}")


if __name__ == "__main__":
    main()
