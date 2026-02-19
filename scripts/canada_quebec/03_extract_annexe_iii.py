import pdfplumber
import csv
import re
import sys
import os
import time
from pathlib import Path
from typing import List, Dict, Optional, Any

# Add parent directory to path to import config_loader
script_path = Path(__file__).resolve().parent
sys.path.insert(0, str(script_path))

from config_loader import (
    get_split_pdf_dir, get_csv_output_dir, 
    DB_ENABLED, STATIC_CURRENCY, STATIC_REGION
)
from db_handler import DBHandler

INPUT_PDF = get_split_pdf_dir() / "annexe_iii.pdf"
OUTPUT_CSV = get_csv_output_dir() / "annexe_iii.csv"

RE_PACK_SIZE = re.compile(r"\b(\d{1,4})\b$") # Usually at the end of the line

class RowData:
    def __init__(self):
        self.generic_name = "N/A"
        self.formulation = "" 
        self.din = "N/A" # Annexe III often lacks DINs in this table
        self.brand = ""
        self.manufacturer = ""
        self.format_str = ""
        self.price = "0.0"
        self.unit_price = "0.0"
        self.page_num = 0
        self.annexe = "III"

def extract_annexe_iii():
    if not INPUT_PDF.exists():
        print(f"Skipping Annexe III: {INPUT_PDF} not found.")
        return []

    print(f"Extracting Annexe III: {INPUT_PDF}")
    rows = []
    
    with pdfplumber.open(INPUT_PDF) as pdf:
        for i, page in enumerate(pdf.pages):
            words = page.extract_words(x_tolerance=3, y_tolerance=3)
            # Group by line
            lines = {}
            for w in words:
                top = round(w['top'])
                if top not in lines: lines[top] = []
                lines[top].append(w)
            
            for top in sorted(lines.keys()):
                line_words = sorted(lines[top], key=lambda w: w['x0'])
                text = " ".join([w['text'] for w in line_words])
                
                # Skip headers
                if "Fabricant" in text or "ANNEXE" in text:
                    continue
                
                # Annexe III rows: [Manufacturer] [Brand] [Formulation] [Pack Size]
                # Spacing is key. 
                # Manufacturer is usually the first word(s).
                # Pack size is the last word (integer).
                
                parts = text.split()
                if len(parts) < 3: continue
                
                row = RowData()
                row.page_num = i + 1
                
                # Manufacturers in Annexe III are often one or two words (Apotex, Otsuka Can, etc.)
                # We'll use X-coordinates for better precision
                
                # Let's use simple column split based on X
                manu_words = [w['text'] for w in line_words if w['x0'] < 100]
                brand_form_words = [w['text'] for w in line_words if 100 <= w['x0'] < 350]
                pack_words = [w['text'] for w in line_words if w['x0'] >= 350]
                
                row.manufacturer = " ".join(manu_words)
                brand_form = " ".join(brand_form_words)
                
                # Split brand and formulation (heuristic: brand is first 1-2 words)
                bf_parts = brand_form.split(None, 1)
                if len(bf_parts) > 1:
                    row.brand = bf_parts[0]
                    row.formulation = bf_parts[1]
                else:
                    row.brand = brand_form
                    
                row.format_str = " ".join(pack_words)
                
                if row.manufacturer and row.brand:
                    rows.append(row)
                    
    return rows

if __name__ == "__main__":
    start_time = time.time()
    rows = extract_annexe_iii()
    duration = time.time() - start_time
    
    PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID", f"manual_iii_{int(time.time())}")
    
    if DB_ENABLED:
        db = DBHandler()
        db.save_rows("annexe_iii", rows, PIPELINE_RUN_ID)
        db.log_step(PIPELINE_RUN_ID, "Extract Annexe III", "COMPLETED", len(rows), duration)
    
    # Always save CSV for inspection
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(["Manufacturer", "Brand", "Formulation", "Format", "Page"])
        for r in rows:
            writer.writerow([r.manufacturer, r.brand, r.formulation, r.format_str, r.page_num])
            
    print(f"Annexe III: Extracted {len(rows)} rows.")
