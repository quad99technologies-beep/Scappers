# -*- coding: utf-8 -*-
"""
ANNEXE V Extractor (REGULAR REGEX VERSION) - Step 7
"""
from pathlib import Path
import os
import re
import csv
import logging
import unicodedata
import sys
import time
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

import pdfplumber

# Core imports
script_path = Path(__file__).resolve().parent
sys.path.insert(0, str(script_path))

from config_loader import (
    get_base_dir, get_split_pdf_dir, get_csv_output_dir,
    get_env_bool, ANNEXE_V_PDF_NAME, ANNEXE_V_CSV_NAME,
    STATIC_CURRENCY, STATIC_REGION, FINAL_COLUMNS
)
from db_handler import DBHandler

# --- CONFIG ---
INPUT_DIR = get_split_pdf_dir()
OUTPUT_DIR = get_csv_output_dir()
INPUT_PDF = INPUT_DIR / ANNEXE_V_PDF_NAME
OUTPUT_CSV = OUTPUT_DIR / ANNEXE_V_CSV_NAME
DB_ENABLED = get_env_bool("DB_ENABLED", False)

RE_DIN = re.compile(r"^\d{6,9}$")
RE_PACK_ONLY = re.compile(r"^\d{1,4}$")
RE_VOL = re.compile(r"^\d{1,4}(?:[.,]\d+)?\s?(mL|ml|L)$", re.I)
RE_VOL_TWO = re.compile(r"^\d{1,4}(?:[.,]\d+)?\s+(mL|ml|L)$", re.I)
RE_ALLCAPS = re.compile(r"^[A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ0-9/ '\-().]+:?$")
RE_STRENGTH = re.compile(r"(?i)(\d+(?:[.,]\d+)?)\s*(mg|g|mcg|µg|U|UI|IU)\s*(?:/|$|\s)")

HDR_WORDS = {"CODE", "MARQUE", "FABRICANT", "FORMAT", "COUT", "COÛT", "PRIX", "UNITAIRE"}

def norm_spaces(s: str) -> str:
    return (s or "").replace("\u00A0", " ").strip()

def strip_acc(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def upper_key(s: str) -> str:
    return strip_acc(norm_spaces(s)).upper()

def french_to_float(s: str) -> Optional[float]:
    if s is None: return None
    t = norm_spaces(s)
    t = re.sub(r"[^\d,.\s]", "", t).replace("\s+", "")
    if not t: return None
    if "," in t and "." in t: t = t.replace(".", "").replace(",", ".")
    elif "," in t: t = t.replace(",", ".")
    try: return float(t)
    except: return None

def page_to_lines(page):
    words = page.extract_words(x_tolerance=2, y_tolerance=2) or []
    if not words: return []
    words.sort(key=lambda w: (round(w["top"], 1), w["x0"]))
    lines = []
    cur = []
    cur_top = None
    for w in words:
        if cur_top is None: cur_top = w["top"]; cur = [w]
        elif abs(w["top"] - cur_top) <= 2: cur.append(w)
        else:
            cur.sort(key=lambda z: z["x0"])
            lines.append({"tokens": cur, "text": " ".join(t["text"] for t in cur)})
            cur_top = w["top"]; cur = [w]
    if cur:
        cur.sort(key=lambda z: z["x0"])
        lines.append({"tokens": cur, "text": " ".join(t["text"] for t in cur)})
    return lines

def find_din_token_idx(tokens):
    for i, t in enumerate(tokens):
        if RE_DIN.match(t["text"].strip()): return i
    return None

def is_generic_header(text):
    s = norm_spaces(text).rstrip(":").strip()
    if not s or re.match(r"^\d", s): return False
    up = upper_key(s)
    if any(w in up for w in HDR_WORDS): return False
    return bool(RE_ALLCAPS.match(s))

def is_form_line(text):
    t = text.lower()
    return any(w in t for w in ["sol. inj.", "caps.", "co.", "susp.", "pd.", "ppb"])

def brand_and_manufacturer(after_tokens):
    # Heuristic: split by largest gap
    if not after_tokens: return None, None
    gaps = []
    for a, b in zip(after_tokens, after_tokens[1:]):
        gaps.append(b["x0"] - a["x1"])
    if not gaps: return after_tokens[0]["text"], "N/A"
    max_gap = max(gaps)
    if max_gap > 10:
        split_idx = gaps.index(max_gap) + 1
        return " ".join(t["text"] for t in after_tokens[:split_idx]), " ".join(t["text"] for t in after_tokens[split_idx:])
    return " ".join(t["text"] for t in after_tokens), "N/A"

def main():
    print(f"Starting Annexe V Extraction (Regex Mode)")
    if not INPUT_PDF.exists():
        print(f"Error: {INPUT_PDF} not found.")
        return

    db = DBHandler() if DB_ENABLED else None
    run_id = os.getenv("PIPELINE_RUN_ID", f"manual_v_{int(time.time())}")
    
    total_rows = 0
    start_time = time.time()

    with pdfplumber.open(INPUT_PDF) as pdf:
        total_pages = len(pdf.pages)
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=FINAL_COLUMNS)
            writer.writeheader()
            
            cur_generic = ""
            cur_formline = ""

            for p_idx in range(total_pages):
                page = pdf.pages[p_idx]
                lines = page_to_lines(page)
                page_rows = []

                for i, line in enumerate(lines):
                    text = line["text"]
                    if is_generic_header(text):
                        cur_generic = text.rstrip(':').strip()
                        cur_formline = ""
                        continue
                    
                    din_idx = find_din_token_idx(line["tokens"])
                    if din_idx is None and is_form_line(text):
                        cur_formline = text.strip()
                        continue
                    
                    if din_idx is not None:
                        din = line["tokens"][din_idx]["text"].strip()
                        after = line["tokens"][din_idx+1:]
                        brand, manu = brand_and_manufacturer(after)
                        
                        # Find prices on this line or next
                        prices = []
                        price_text = " ".join(t["text"] for t in after)
                        prices = re.findall(r"\d+[,.]\d+", price_text)
                        
                        cost = prices[0] if len(prices) > 0 else "0.0"
                        unit = prices[1] if len(prices) > 1 else cost
                        
                        row = {
                            "Generic Name": cur_generic or "N/A",
                            "Currency": STATIC_CURRENCY,
                            "Ex Factory Wholesale Price": cost,
                            "Unit Price": unit,
                            "Region": STATIC_REGION,
                            "Product Group": brand or "N/A",
                            "Marketing Authority": manu or "N/A",
                            "Local Pack Description": cur_formline or text,
                            "Formulation": cur_formline.split()[0] if cur_formline else "N/A",
                            "Fill Size": 1,
                            "Strength": "N/A",
                            "Strength Unit": "N/A",
                            "LOCAL_PACK_CODE": din
                        }
                        writer.writerow(row)
                        page_rows.append(row)
                        total_rows += 1
                
                if db and page_rows:
                    db.save_rows("annexe_v", page_rows, run_id)
                
                if (p_idx + 1) % 50 == 0:
                    print(f"Processed {p_idx+1}/{total_pages} pages... Total rows: {total_rows}")

    duration = time.time() - start_time
    if db:
        db.log_step(run_id, "Extract Annexe V", "COMPLETED", total_rows, duration)
    print(f"Extraction complete. Total rows: {total_rows}")

if __name__ == "__main__":
    main()
