#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 2/5: Get Redirect URLs (WDAC/AppLocker safe)
=================================================
Reads input/Tender_Chile/TenderList.csv, builds tender DetailsAcquisition URLs,
opens each in Selenium to capture the final redirect URL (contains qs=...),
then writes output/Tender_Chile/tender_redirect_urls.csv.

IMPORTANT:
- No webdriver-manager (Windows policy blocks .wdm executables).
- Set env var CHROMEDRIVER_PATH to an allowlisted chromedriver.exe.
"""

from __future__ import annotations

import csv
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


# ------------------------------------------------------------
# Repo root + script dir path wiring (keep same repo convention)
# ------------------------------------------------------------
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Optional config_loader (your repo uses this)
try:
    from config_loader import (
        load_env_file,
        getenv_int,
        getenv_bool,
        get_input_dir as _get_input_dir,
        get_output_dir as _get_output_dir,
    )
    load_env_file()
    _CONFIG = True
except Exception:
    _CONFIG = False


# ----------------------------
# Config
# ----------------------------
if _CONFIG:
    MAX_TENDERS = getenv_int("MAX_TENDERS", 100)
    HEADLESS = getenv_bool("HEADLESS", True)
    WAIT_SECONDS = getenv_int("WAIT_SECONDS", 60)
else:
    MAX_TENDERS = int(os.getenv("MAX_TENDERS", "100"))
    HEADLESS = os.getenv("HEADLESS", "True").lower() == "true"
    WAIT_SECONDS = int(os.getenv("WAIT_SECONDS", "60"))

OUTPUT_FILENAME = "tender_redirect_urls.csv"
REQUIRED_OUTPUT_COLUMNS = [
    "original_url",
    "redirect_url",
    "qs_parameter",
    "tender_details_url",
    "tender_award_url",
]


# ----------------------------
# Paths
# ----------------------------
def get_input_dir() -> Path:
    if _CONFIG:
        return _get_input_dir()
    return _repo_root / "input" / "Tender_Chile"


def get_output_dir() -> Path:
    if _CONFIG:
        return _get_output_dir()
    return _repo_root / "output" / "Tender_Chile"


# ----------------------------
# CSV dialect + encoding detect
# ----------------------------
def detect_csv_dialect_and_encoding(path: Path) -> Tuple[str, str]:
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1", "windows-1252"]
    sample = None
    used_enc = "utf-8"

    for enc in encodings:
        try:
            sample = path.read_text(encoding=enc, errors="strict")[:8192]
            used_enc = enc
            break
        except Exception:
            continue

    if not sample:
        return (",", "utf-8")

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return (dialect.delimiter, used_enc)
    except Exception:
        delims = [",", ";", "\t", "|"]
        counts = {d: 0 for d in delims}
        lines = [ln for ln in sample.splitlines()[:10] if ln.strip()]
        for ln in lines:
            for d in delims:
                counts[d] += ln.count(d)
        best = max(counts.items(), key=lambda x: x[1])[0]
        return (best if counts[best] > 0 else ",", used_enc)


# ----------------------------
# URL helpers
# ----------------------------
def validate_url_format(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    u = url.strip()
    return u.startswith("http://") or u.startswith("https://")


def extract_qs_from_url(url: str) -> Optional[str]:
    m = re.search(r"[?&]qs=([^&]+)", url)
    return m.group(1) if m else None


def convert_to_details_url(value: str) -> str:
    """
    If value is a URL, return it.
    Else treat as tender id/code and convert to DetailsAcquisition URL.
    """
    v = (value or "").strip()
    if validate_url_format(v):
        return v

    return (
        "https://www.mercadopublico.cl/Procurement/Modules/RFB/"
        f"DetailsAcquisition.aspx?idlicitacion={v}"
    )


def extract_url_from_csv_row(row: Dict[str, Any]) -> Optional[str]:
    """
    Your input file shows: CN Document Number
    We also support a few common variants.
    """
    id_cols = [
        "CN Document Number",
        "CodigoExterno", "CódigoExterno",
        "IDLicitacion", "idlicitacion",
        "Tender ID", "TenderId",
        "ID", "id",
    ]
    url_cols = ["URL", "Url", "Link", "link", "Source URL", "Detail URL"]

    # 1) if a real URL column exists, use it
    for c in url_cols:
        if c in row and row[c]:
            val = str(row[c]).strip()
            if validate_url_format(val) and "mercadopublico.cl" in val:
                return val

    # 2) else build URL from ID columns
    for c in id_cols:
        if c in row and row[c]:
            return convert_to_details_url(str(row[c]))

    # 3) else any cell that looks like MP URL
    for _, v in row.items():
        if isinstance(v, str):
            vv = v.strip()
            if validate_url_format(vv) and "mercadopublico.cl" in vv:
                return vv

    return None


# ==========================================================
# WDAC/AppLocker SAFE ChromeDriver resolver (NO webdriver-manager)
# ==========================================================
def _resolve_chromedriver_path_strict() -> Optional[str]:
    """
    STRICT order:
      1) CHROMEDRIVER_PATH env var (recommended + required in WDAC machines)
      2) core.chrome_manager.get_chromedriver_path() ONLY if it returns an existing path
      3) None -> Selenium Manager last resort (may still be blocked)
    """
    env_path = (os.getenv("CHROMEDRIVER_PATH") or "").strip().strip('"')
    if env_path:
        p = Path(env_path)
        if p.exists():
            return str(p)
        raise RuntimeError(f"CHROMEDRIVER_PATH is set but file not found: {env_path}")

    # Optional repo helper (but must not return .wdm in your environment)
    try:
        from core.chrome_manager import get_chromedriver_path  # type: ignore
        p2 = get_chromedriver_path()
        if p2 and Path(p2).exists():
            return str(Path(p2))
    except Exception:
        pass

    return None


def _build_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--lang=es-CL")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    chrome_bin = (os.getenv("CHROME_BINARY") or "").strip().strip('"')
    if chrome_bin:
        opts.binary_location = chrome_bin

    driver_path = _resolve_chromedriver_path_strict()

    try:
        if driver_path:
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=opts)
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

    driver.set_page_load_timeout(WAIT_SECONDS)
    return driver


def get_redirect_url(start_url: str) -> str:
    """
    Open start_url in Selenium and wait until current_url stabilizes.
    Returns final URL (usually includes qs=...).
    """
    if not validate_url_format(start_url):
        raise ValueError(f"Invalid URL: {start_url}")

    driver = _build_driver()
    try:
        driver.get(start_url)

        last = ""
        stable = 0
        end_time = time.time() + WAIT_SECONDS

        while time.time() < end_time:
            cur = driver.current_url
            if cur == last:
                stable += 1
            else:
                stable = 0
                last = cur

            if stable >= 3:  # stable for ~1.5s
                return cur

            time.sleep(0.5)

        return driver.current_url
    finally:
        driver.quit()


def write_output(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=REQUIRED_OUTPUT_COLUMNS)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    input_dir = get_input_dir()
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Your real input: TenderList.csv
    candidates = [
        input_dir / "TenderList.csv",
        input_dir / "TenderList.CSV",
        input_dir / "tender_list.csv",
        input_dir / "TenderList.csv".lower(),
    ]
    input_path = next((p for p in candidates if p.exists()), None)
    if not input_path:
        print(f"[ERROR] Input not found in: {input_dir}")
        for p in candidates:
            print(f"  - {p.name}")
        sys.exit(1)

    delim, enc = detect_csv_dialect_and_encoding(input_path)
    print(f"Reading: {input_path}")
    print(f"   Detected delimiter: '{delim}' | encoding: {enc}")

    with open(input_path, "r", encoding=enc, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        rows = list(reader)
        print(f"   Available columns: {', '.join(reader.fieldnames or [])}")
        if rows:
            print("\n   Sample row (first row):")
            for k, v in list(rows[0].items())[:5]:
                print(f"     {k}: {v}")

    if not rows:
        print("[ERROR] Input CSV has no rows")
        sys.exit(1)

    out_rows: List[Dict[str, Any]] = []
    total = min(len(rows), MAX_TENDERS)

    for i, row in enumerate(rows[:total], start=1):
        start_url = extract_url_from_csv_row(row)
        if not start_url:
            print(f"   [WARN] Row {i}: no tender id/url found -> skip")
            continue

        try:
            final_url = get_redirect_url(start_url)
            qs = extract_qs_from_url(final_url) or ""

            details_url = final_url
            award_url = final_url.replace("DetailsAcquisition.aspx", "Results.aspx")

            out_rows.append({
                "original_url": start_url,
                "redirect_url": final_url,
                "qs_parameter": qs,
                "tender_details_url": details_url,
                "tender_award_url": award_url,
            })
            print(f"   [OK] Row {i}: URL ready -> {details_url}")
        except Exception as e:
            print(f"   [ERROR] Row {i}: {e}")

    if not out_rows:
        print("[ERROR] No tender URLs processed successfully")
        sys.exit(1)

    out_path = output_dir / OUTPUT_FILENAME
    write_output(out_path, out_rows)
    print(f"[OK] Found {len(out_rows)} tender URLs to process")
    print(f"[OK] Wrote: {out_path}")


if __name__ == "__main__":
    main()
