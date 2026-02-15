import pdfplumber
import csv
import re
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any

# Add parent directory to path to import config_loader if needed
script_path = Path(__file__).resolve().parent
sys.path.insert(0, str(script_path))

import os
import time

try:
    from config_loader import (
        get_split_pdf_dir, get_csv_output_dir, 
        ANNEXE_IV2_PDF_NAME, ANNEXE_IV2_CSV_NAME,
        DB_ENABLED
    )
    from db_handler import DBHandler
    INPUT_DIR = get_split_pdf_dir()
    OUTPUT_DIR = get_csv_output_dir()
    INPUT_PDF = INPUT_DIR / ANNEXE_IV2_PDF_NAME
    OUTPUT_CSV = OUTPUT_DIR / ANNEXE_IV2_CSV_NAME 
except ImportError:
    # Fallback paths
    BASE_DIR = Path(r"D:\quad99\Scrappers")
    INPUT_PDF = BASE_DIR / "output" / "CanadaQuebec" / "split_pdf" / "annexe_iv2.pdf"
    OUTPUT_CSV = BASE_DIR / "output" / "CanadaQuebec" / "annexe_iv2_robust.csv"
    OUTPUT_DIR = OUTPUT_CSV.parent
    DB_ENABLED = False

# --- Configuration ---
# Annexe IV.2 is tabular "Stable Agents".
# Column Thresholds (Approximate from inspection):
# DIN ~ 104
# Brand ~ 161
# Manu ~ 268
# Format ~ 340 (and counts)
# Price ~ 386
# Unit ~ 437

COL_X_DIN = 100
COL_X_BRAND = 155
COL_X_MANU = 260
COL_X_FORMAT = 330
COL_X_PRICE = 380
COL_X_UNIT = 430

RE_DIN = re.compile(r"^\d{6,8}$")
RE_FORM_STRENGTH = re.compile(r"(?i)\b(mg|ml|mcg|g|u\.i\.|unit)\b")

# --- Data Structures ---
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
        self.annexe = "IV.2"

def clean_text(text: str) -> str:
    if not text: return ""
    return text.replace("\xa0", " ").strip()

def extract_rows_from_pdf():
    print(f"Opening PDF: {INPUT_PDF}")
    
    all_raw_rows = []
    
    with pdfplumber.open(INPUT_PDF) as pdf:
        print(f"Total Pages: {len(pdf.pages)}")
        
        current_generic = ""
        current_form = ""
        
        for i, page in enumerate(pdf.pages):
            page_num = i + 1
            words = page.extract_words(keep_blank_chars=False, x_tolerance=2, y_tolerance=2)
            
            # Group by line
            lines = {}
            for w in words:
                top = round(w['top'])
                if top not in lines: lines[top] = []
                lines[top].append(w)
            
            sorted_tops = sorted(lines.keys())
            
            for top in sorted_tops:
                line_words = sorted(lines[top], key=lambda w: w['x0'])
                line_text = " ".join([w['text'] for w in line_words])
                
                # Check for DIN in DIN column
                din_found = None
                words_in_din_col = [w for w in line_words if w['x0'] < COL_X_BRAND]
                for w in words_in_din_col:
                    txt = clean_text(w['text'])
                    if RE_DIN.match(txt):
                        din_found = txt
                        break
                
                if din_found:
                    row = RowData()
                    row.page_num = page_num
                    row.annexe = "IV.2"
                    row.generic_name = current_generic
                    row.formulation = current_form
                    row.din = din_found
                    
                    # Columns
                    brand_words = [w['text'] for w in line_words if COL_X_BRAND <= w['x0'] < COL_X_MANU]
                    manu_words = [w['text'] for w in line_words if COL_X_MANU <= w['x0'] < COL_X_FORMAT]
                    format_words = [w['text'] for w in line_words if COL_X_FORMAT <= w['x0'] < COL_X_PRICE]
                    price_words = [w['text'] for w in line_words if COL_X_PRICE <= w['x0'] < COL_X_UNIT]
                    unit_words = [w['text'] for w in line_words if w['x0'] >= COL_X_UNIT]
                    
                    row.brand = clean_text(" ".join(brand_words))
                    row.manufacturer = clean_text(" ".join(manu_words))
                    row.format_str = clean_text(" ".join(format_words))
                    
                    # Prices might spill
                    # IV.2 usually has distinct columns for Price and Unit Price
                    if price_words:
                        row.price = clean_text(" ".join(price_words))
                    if unit_words:
                        row.unit_price = clean_text(" ".join(unit_words))
                    
                    all_raw_rows.append(row)
                else:
                    # Context / Header / Form
                    # Similar heuristics to Annexe V
                    # If line starts very left and is bold/caps -> Generic
                    # If line contains strength units -> Form
                    
                    # If words are only in Generic area (left)
                    is_data_col = any(w['x0'] > COL_X_MANU for w in line_words)
                    
                    if not is_data_col:
                        if RE_FORM_STRENGTH.search(line_text):
                            current_form = clean_text(line_text)
                        elif len(line_text) > 3 and not re.match(r"^Page", line_text):
                            # Assume Generic header
                            current_generic = clean_text(line_text)
                            current_form = ""

    return all_raw_rows

def write_to_csv(rows: List[RowData], output_path: Path):
    print(f"Writing {len(rows)} rows to {output_path}")
    fieldnames = [
        "Generic Name", "Formulation", "DIN", "Brand", "Manufacturer", 
        "Format", "Price", "Unit Price", "Page", "Annexe"
    ]
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "Generic Name": r.generic_name,
                "Formulation": r.formulation,
                "DIN": r.din,
                "Brand": r.brand,
                "Manufacturer": r.manufacturer,
                "Format": r.format_str,
                "Price": r.price,
                "Unit Price": r.unit_price,
                "Page": r.page_num,
                "Annexe": r.annexe
            })

if __name__ == "__main__":
    start_time = time.time()
    
    PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID", f"manual_iv2_{int(time.time())}")
    
    rows = extract_rows_from_pdf()
    duration = time.time() - start_time
    
    if DB_ENABLED:
        try:
            db = DBHandler()
            db.save_rows("annexe_iv2", rows, PIPELINE_RUN_ID)
            
            meta = {
                "input_pdf": str(INPUT_PDF),
                "total_pages": rows[-1].page_num if rows else 0,
            }
            db.log_step(PIPELINE_RUN_ID, "Extract Annexe IV.2", "COMPLETED", len(rows), duration, meta)
            print(f"Data saved to DB for run {PIPELINE_RUN_ID}")
        except Exception as e:
             print(f"DB Error: {e}")
             if DB_ENABLED:
                 pass
    else:
        output_path = OUTPUT_DIR / f"annexe_iv2_{PIPELINE_RUN_ID}.csv"
        write_to_csv(rows, output_path)
    
    print(f"Extraction complete: {len(rows)} rows.")
