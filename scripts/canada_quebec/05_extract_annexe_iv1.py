import pdfplumber
import csv
import re
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any
import os
import time

# Add parent directory to path to import config_loader
script_path = Path(__file__).resolve().parent
sys.path.insert(0, str(script_path))

from config_loader import (
    get_split_pdf_dir, get_csv_output_dir, 
    DB_ENABLED
)
from db_handler import DBHandler

INPUT_PDF = get_split_pdf_dir() / "annexe_iv1.pdf"
OUTPUT_CSV = get_csv_output_dir() / "annexe_iv1.csv"

RE_DIN = re.compile(r"\b(\d{6,8})\b")

class RowData:
    def __init__(self):
        self.generic_name = ""
        self.formulation = "" 
        self.din = ""
        self.brand = ""
        self.manufacturer = ""
        self.format_str = ""
        self.price = ""
        self.unit_price = ""
        self.page_num = 0
        self.annexe = "IV.1"

def extract_rows():
    if not INPUT_PDF.exists():
        print(f"File not found: {INPUT_PDF}")
        return []

    all_rows = []
    with pdfplumber.open(INPUT_PDF) as pdf:
        current_generic = ""
        current_formulation = ""
        
        for i, page in enumerate(pdf.pages):
            page_num = i + 1
            words = page.extract_words(x_tolerance=3, y_tolerance=3)
            
            # Robust grouping by Y coordinate
            lines = []
            if not words: continue
            words.sort(key=lambda w: w['top'])
            cur_line = [words[0]]
            for w in words[1:]:
                if abs(w['top'] - cur_line[-1]['top']) < 3:
                    cur_line.append(w)
                else:
                    lines.append(cur_line)
                    cur_line = [w]
            lines.append(cur_line)
            
            for line in lines:
                line.sort(key=lambda w: w['x0'])
                text = " ".join([w['text'] for w in line])
                
                # Check for Generic Header (All caps, ending with :)
                if re.match(r"^[A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ0-9/ '\-().]+ :?$", text) or (text.isupper() and len(text) > 10):
                    current_generic = text.rstrip(':').strip()
                    current_formulation = ""
                    continue
                
                # Check for Formulation line (usually right after generic)
                if any(w in text.lower() for w in ["sol. inj.", "caps.", "co.", "susp.", "pd."]) and not RE_DIN.search(text):
                    current_formulation = text.strip()
                    continue

                # Check for DIN row
                din_match = RE_DIN.search(text)
                if din_match:
                    row = RowData()
                    row.din = din_match.group(1)
                    row.page_num = page_num
                    row.generic_name = current_generic
                    row.formulation = current_formulation
                    
                    # Columns in IV.1: [DIN] [Brand] [Manufacturer] [Pack] [Price] [Unit Price]
                    # We'll use X-anchors if possible, or split by parts
                    # Standard X for IV.1:
                    # DIN: ~50, Brand: ~150, Manu: ~300, Pack: ~400, Price: ~450
                    
                    brand_words = [w['text'] for w in line if 90 < w['x0'] < 250]
                    manu_words = [w['text'] for w in line if 250 <= w['x0'] < 400]
                    price_words = [w['text'] for w in line if w['x0'] >= 400]
                    
                    row.brand = " ".join(brand_words)
                    row.manufacturer = " ".join(manu_words)
                    
                    prices = re.findall(r"\d+[,.]\d+", " ".join(price_words))
                    if len(prices) >= 1: row.price = prices[0]
                    if len(prices) >= 2: row.unit_price = prices[1]
                    
                    pack_search = re.search(r"\b(\d+)\b", " ".join(price_words))
                    if pack_search: row.format_str = pack_search.group(1)
                    
                    all_rows.append(row)
                    
    return all_rows

if __name__ == "__main__":
    start_time = time.time()
    rows = extract_rows()
    duration = time.time() - start_time
    PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID", f"manual_iv1_{int(time.time())}")
    
    if DB_ENABLED:
        db = DBHandler()
        db.save_rows("annexe_iv1", rows, PIPELINE_RUN_ID)
        db.log_step(PIPELINE_RUN_ID, "Extract Annexe IV.1", "COMPLETED", len(rows), duration)
    
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=["Generic Name", "Formulation", "DIN", "Brand", "Manufacturer", "Format", "Price", "Unit Price", "Page"])
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "Generic Name": r.generic_name, "Formulation": r.formulation, "DIN": r.din,
                "Brand": r.brand, "Manufacturer": r.manufacturer, "Format": r.format_str,
                "Price": r.price, "Unit Price": r.unit_price, "Page": r.page_num
            })
    print(f"Extracted {len(rows)} rows from IV.1")
