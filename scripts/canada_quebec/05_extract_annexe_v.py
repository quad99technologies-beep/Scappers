import pdfplumber
import csv
import re
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any

# Add parent directory to path to import config_loader if needed, 
# but we will try to be self-contained for robustness per plan, 
# while still using the shared config for paths if possible.
script_path = Path(__file__).resolve().parent
sys.path.insert(0, str(script_path))

import os
import time

try:
    from config_loader import (
        get_split_pdf_dir, get_csv_output_dir, 
        ANNEXE_V_PDF_NAME, ANNEXE_V_CSV_NAME,
        DB_ENABLED
    )
    from db_handler import DBHandler
    INPUT_DIR = get_split_pdf_dir()
    OUTPUT_DIR = get_csv_output_dir()
    INPUT_PDF = INPUT_DIR / ANNEXE_V_PDF_NAME
    OUTPUT_CSV = OUTPUT_DIR / ANNEXE_V_CSV_NAME # Use a new name for now to avoid overwriting immediately
except ImportError:
    # Fallback paths if config_loader fails
    BASE_DIR = Path(r"D:\quad99\Scrappers")
    INPUT_PDF = BASE_DIR / "output" / "CanadaQuebec" / "split_pdf" / "annexe_v.pdf"
    OUTPUT_CSV = BASE_DIR / "output" / "CanadaQuebec" / "annexe_v_robust.csv"
    OUTPUT_DIR = OUTPUT_CSV.parent
    DB_ENABLED = False

# --- Configuration ---
# X-coordinates thresholds (approximate, based on analysis)
# These define the STARTS of columns. 
# DIN ~ 144
# Brand ~ 182
# Manufacturer ~ 275
# Format ~ 358
# Price ~ 410 (Unit Price ~ 450+)
COL_X_DIN = 140
COL_X_BRAND = 180
COL_X_MANU = 270
COL_X_FORMAT = 350
COL_X_PRICE = 410

# Y-tolerance for grouping words into lines
Y_TOLERANCE = 3

# Regex for validation
RE_DIN = re.compile(r"^\d{6,8}$")
RE_PRICE = re.compile(r"^\d{1,3}[,.]\d{2,}$") # e.g. 4,04 or 100,50
RE_FORM_STRENGTH = re.compile(r"(?i)\b(mg|ml|mcg|g|u\.i\.|unit)\b") # Basic check for form lines

# --- Data Structures ---
class RowData:
    def __init__(self):
        self.generic_name = ""
        self.formulation = "" # includes strength
        self.din = ""
        self.brand = ""
        self.manufacturer = ""
        self.format_str = ""
        self.price = ""
        self.unit_price = ""
        self.page_num = 0

def clean_text(text: str) -> str:
    """Cleans text: removes extra spaces, handled non-breaking spaces."""
    if not text: return ""
    return text.replace("\xa0", " ").strip()

def is_header_line(line_words: List[Dict]) -> bool:
    """
    Detects if a line is a 'Generic Name' header.
    Heuristics: 
    - No DIN (Start x < 150)
    - All caps (mostly)
    - Bold font (often, but pdfplumber font info can be flaky)
    - Positioned at the start of a block
    """
    # If it has a DIN, it's definitely not a header
    for w in line_words:
        if w['x0'] < COL_X_BRAND and RE_DIN.match(w['text']):
            return False
            
    text = " ".join([w['text'] for w in line_words])
    
    # Check if it looks like a drug name (All caps, maybe some numbers/symbols)
    # Exclude page numbers or table headers
    if re.match(r"^PAGE\s+\d+", text, re.I): return False
    if "MARQUE DE COMMERCE" in text: return False
    if "CODE" in text and "PRIX" in text: return False
    
    # Generic names in this PDF are usually uppercase
    # But some might have lowercase (e.g. pH helpers). 
    # Let's rely on position (left-aligned) and lack of other columns.
    
    # If words appear in Manufacturer or Price columns, strict no.
    for w in line_words:
        if w['x0'] > COL_X_MANU:
            return False
            
    return True

def is_form_line(line_words: List[Dict]) -> bool:
    """
    Detects if a line is a 'Form/Strength' line.
    Ex: "Co.   10 mg" or "Sol. Inj.   50 mg/mL"
    """
    # Must not have DIN
    for w in line_words:
        if w['x0'] < COL_X_BRAND and RE_DIN.match(w['text']):
            return False
            
    text = " ".join([w['text'] for w in line_words])
    
    # Check for formulation keywords or strength units
    if RE_FORM_STRENGTH.search(text):
        return True
        
    return False

def extract_rows_from_pdf():
    print(f"Opening PDF: {INPUT_PDF}")
    
    all_raw_rows = []
    
    # DB Batch Logic
    db_batch = []
    BATCH_SIZE_PAGES = 50
    db = None
    if DB_ENABLED:
        try:
            db = DBHandler()
        except:
            pass
    
    with pdfplumber.open(INPUT_PDF) as pdf:
        total_pages = len(pdf.pages)
        print(f"Total Pages: {total_pages}")
        
        current_generic = ""
        current_form = ""
        
        for i, page in enumerate(pdf.pages):
            page_num = i + 1
            if page_num % 10 == 0:
                print(f"Processing page {page_num}...")
                
            words = page.extract_words(keep_blank_chars=False, x_tolerance=1, y_tolerance=Y_TOLERANCE)
            
            # Group words by line (using top coordinate)
            lines = {}
            for w in words:
                # Round top to nearest integer or half-integer to group roughly aligned words
                top = round(w['top'] / 2) * 2 # round to nearest 2 pixels
                if top not in lines:
                    lines[top] = []
                lines[top].append(w)
            
            sorted_tops = sorted(lines.keys())
            
            page_new_rows = []
            
            for top in sorted_tops:
                line_words = sorted(lines[top], key=lambda w: w['x0'])
                line_text = " ".join([w['text'] for w in line_words])
                
                # 1. Identify Line Type
                
                # Check for DIN
                din_found = None
                words_in_din_col = [w for w in line_words if w['x0'] < COL_X_BRAND]
                for w in words_in_din_col:
                    txt = clean_text(w['text'])
                    if RE_DIN.match(txt):
                        din_found = txt
                        break
                
                if din_found:
                    # It's a Data Row
                    row = RowData()
                    row.page_num = page_num
                    row.generic_name = current_generic
                    row.formulation = current_form
                    row.din = din_found
                    
                    # Extract other columns based on X position
                    brand_words = [w['text'] for w in line_words if COL_X_BRAND <= w['x0'] < COL_X_MANU]
                    manu_words = [w['text'] for w in line_words if COL_X_MANU <= w['x0'] < COL_X_FORMAT]
                    format_words = [w['text'] for w in line_words if COL_X_FORMAT <= w['x0'] < COL_X_PRICE]
                    price_words = [w['text'] for w in line_words if w['x0'] >= COL_X_PRICE]
                    
                    row.brand = clean_text(" ".join(brand_words))
                    row.manufacturer = clean_text(" ".join(manu_words))
                    row.format_str = clean_text(" ".join(format_words))
                    
                    # CLEANUP
                    row.brand = row.brand.replace("MARQUE DE COMMERCE", "").replace("(EMBALLAGE", "").strip()
                    row.manufacturer = row.manufacturer.replace("FABRICANT", "").replace("COMBIN)", "").strip()
                    
                    # Prices
                    cleaned_prices = [p for p in price_words if re.search(r"\d", p)]
                    
                    # RECOVERY: Check if Price merged into Format
                    price_in_fmt = re.search(r"(\s\d{1,5}[,.]\d{2})$", row.format_str)
                    if price_in_fmt:
                         extracted_price = price_in_fmt.group(1).strip()
                         row.format_str = row.format_str[:price_in_fmt.start()].strip()
                         cleaned_prices.insert(0, extracted_price)
                    
                    if len(cleaned_prices) >= 1:
                        row.price = cleaned_prices[0]
                    if len(cleaned_prices) >= 2:
                        row.unit_price = cleaned_prices[1]
                        
                    all_raw_rows.append(row)
                    page_new_rows.append(row)
                    
                    # Add to DB batch
                    if DB_ENABLED:
                        db_batch.append(row)
                    
                else:
                    # Not a DIN row
                    if is_header_line(line_words):
                        current_generic = clean_text(line_text)
                        current_form = "" 
                    elif is_form_line(line_words):
                        current_form = clean_text(line_text)
                    else:
                        # Continuation Logic
                        if all_raw_rows:
                            last_row = all_raw_rows[-1]
                            
                            brand_cont = [w['text'] for w in line_words if COL_X_BRAND <= w['x0'] < COL_X_MANU]
                            manu_cont = [w['text'] for w in line_words if COL_X_MANU <= w['x0'] < COL_X_FORMAT]
                            
                            updated = False
                            if brand_cont:
                                append_str = clean_text(" ".join(brand_cont))
                                last_row.brand += " " + append_str
                                updated = True
                            if manu_cont:
                                append_str = clean_text(" ".join(manu_cont))
                                last_row.manufacturer += " " + append_str
                                updated = True
                            
                            # If updated, ensure DB gets update (via next batch or immediate)
                            # If last_row was already saved, it's not in db_batch?
                            # If it's in DB batch, it will be saved with update.
                            # If it was saved in prev batch, we need to mark it for re-save?
                            # Simplest logic: add to db_batch again if updated? No, duplicates in batch.
                            # Just let upsert handle it if we re-add.
                            if updated and DB_ENABLED and last_row not in db_batch:
                                db_batch.append(last_row) # Re-add to batch for update
            
            # Batch Save Trigger
            if DB_ENABLED and db and (page_num % BATCH_SIZE_PAGES == 0 or i == total_pages - 1):
                if db_batch:
                    try:
                        print(f"Saving batch of {len(db_batch)} rows to DB (Page {page_num})")
                        db.save_rows("annexe_v", db_batch, PIPELINE_RUN_ID)
                        db_batch = [] # Clear batch
                    except Exception as e:
                        print(f"Error saving batch: {e}")
    
    return all_raw_rows

def write_to_csv(rows: List[RowData], output_path: Path):
    print(f"Writing {len(rows)} rows to {output_path}")
    fieldnames = [
        "Generic Name", "Formulation", "DIN", "Brand", "Manufacturer", 
        "Format", "Price", "Unit Price", "Page"
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
                "Page": r.page_num
            })

if __name__ == "__main__":
    start_time = time.time()
    
    PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID", f"manual_v_{int(time.time())}")
    
    rows = extract_rows_from_pdf()
    duration = time.time() - start_time
    
    if DB_ENABLED:
        try:
            db = DBHandler()
            db.save_rows("annexe_v", rows, PIPELINE_RUN_ID)
            
            meta = {
                "input_pdf": str(INPUT_PDF),
                "total_pages": rows[-1].page_num if rows else 0,
            }
            db.log_step(PIPELINE_RUN_ID, "Extract Annexe V", "COMPLETED", len(rows), duration, meta)
            print(f"Data saved to DB for run {PIPELINE_RUN_ID}")
        except Exception as e:
             print(f"DB Error: {e}")
             if DB_ENABLED:
                 pass
    else:
        output_path = OUTPUT_DIR / f"annexe_v_{PIPELINE_RUN_ID}.csv"
        write_to_csv(rows, output_path)
    
    print(f"Extraction complete: {len(rows)} rows.")
