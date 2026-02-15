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
        ANNEXE_IV1_PDF_NAME, ANNEXE_IV1_CSV_NAME,
        DB_ENABLED
    )
    from db_handler import DBHandler
    INPUT_DIR = get_split_pdf_dir()
    OUTPUT_DIR = get_csv_output_dir()
    INPUT_PDF = INPUT_DIR / ANNEXE_IV1_PDF_NAME
    OUTPUT_CSV = OUTPUT_DIR / ANNEXE_IV1_CSV_NAME 
except ImportError:
    # Fallback paths
    BASE_DIR = Path(r"D:\quad99\Scrappers")
    INPUT_PDF = BASE_DIR / "output" / "CanadaQuebec" / "split_pdf" / "annexe_iv1.pdf"
    OUTPUT_CSV = BASE_DIR / "output" / "CanadaQuebec" / "annexe_iv1_robust.csv"
    OUTPUT_DIR = OUTPUT_CSV.parent
    DB_ENABLED = False

# --- Configuration ---
# Annexe IV.1 is text-heavy "Exception Drugs".
# We use a DIN-centric approach.
RE_DIN = re.compile(r"\b(\d{6,8})\b")
RE_PRICE = re.compile(r"(\d{1,5}(?:[.,]\d{2})?)") # Capture potential price

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
        self.annexe = "IV.1"

def clean_text(text: str) -> str:
    if not text: return ""
    return text.replace("\xa0", " ").strip()

def extract_rows_from_pdf():
    print(f"Opening PDF: {INPUT_PDF}")
    
    all_raw_rows = []
    
    with pdfplumber.open(INPUT_PDF) as pdf:
        print(f"Total Pages: {len(pdf.pages)}")
        
        current_generic = "" # Context might be harder to capture here, but we'll try
        
        for i, page in enumerate(pdf.pages):
            page_num = i + 1
            if page_num % 10 == 0:
                print(f"Processing page {page_num}...")
                
            words = page.extract_words(keep_blank_chars=False, x_tolerance=2, y_tolerance=2)
            
            # Group by line
            lines = {}
            for w in words:
                top = round(w['top']) 
                if top not in lines: lines[top] = []
                lines[top].append(w)
            
            sorted_tops = sorted(lines.keys())
            
            # Simple line text for regex
            for top in sorted_tops:
                line_words = sorted(lines[top], key=lambda w: w['x0'])
                line_text = " ".join([w['text'] for w in line_words])
                
                # Check for DIN
                din_match = RE_DIN.search(line_text)
                if din_match:
                    din_val = din_match.group(1)
                    
                    # DINs in IV.1 usually imply a product row
                    row = RowData()
                    row.page_num = page_num
                    row.din = din_val
                    row.generic_name = "See Description" # Placeholder as generic context is complex in IV.1
                    
                    # Heuristic parsing of the line
                    # Usually: [DIN] [Brand] [Manufacturer] [Strength/Format]? [Price]?
                    # OR: [Text description] [DIN] ...
                    
                    # Let's try to extract Price if present (numeric with decimals)
                    # Exclude the DIN itself from price search
                    text_without_din = line_text.replace(din_val, "")
                    
                    # Look for prices (digits + comma/dot + digits)
                    prices = re.findall(r"\b\d{1,5}[,.]\d{2,4}\b", text_without_din)
                    if prices:
                        row.price = prices[0]
                        if len(prices) > 1:
                            row.unit_price = prices[1]
                    
                    # Brand/Manufacturer are hard to distinguish without columns.
                    # We'll dump the rest of the text into "Brand" for now to check later
                    # or refine if we see column consistency.
                    
                    # Try to separate by X coordinates if possible?
                    # Let's see if we can find the DIN word object
                    din_word_obj = next((w for w in line_words if din_val in w['text']), None)
                    
                    if din_word_obj:
                         din_x = din_word_obj['x0']
                         # Text to right of DIN
                         right_text_words = [w['text'] for w in line_words if w['x0'] > din_word_obj['x0'] + 30] 
                         # Text to left of DIN
                         left_text_words = [w['text'] for w in line_words if w['x0'] < din_word_obj['x0'] - 10]
                         
                         name_candidate = " ".join(right_text_words)
                         # If price is in name_candidate, remove it
                         if row.price:
                             name_candidate = name_candidate.replace(row.price, "").strip()
                         
                         row.brand = clean_text(name_candidate)
                         
                         # Sometimes Generic/Brand is on the left?
                         if left_text_words:
                             row.manufacturer = clean_text(" ".join(left_text_words))
                    else:
                        row.brand = clean_text(text_without_din)
                        
                    all_raw_rows.append(row)
                
                else:
                    # Non-DIN line. 
                    # Could be generic name header? 
                    # In IV.1, it's often a paragraph of text describing the condition for exception.
                    # We might ignore for now or capture as "Context".
                    pass

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
    
    # Get Run ID
    PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID", f"manual_iv1_{int(time.time())}")
    
    rows = extract_rows_from_pdf()
    duration = time.time() - start_time
    
    if DB_ENABLED:
        try:
            db = DBHandler()
            db.save_rows("annexe_iv1", rows, PIPELINE_RUN_ID)
            
            # Log step stats
            meta = {
                "input_pdf": str(INPUT_PDF),
                "total_pages": rows[-1].page_num if rows else 0,
            }
            db.log_step(PIPELINE_RUN_ID, "Extract Annexe IV.1", "COMPLETED", len(rows), duration, meta)
            print(f"Data saved to DB for run {PIPELINE_RUN_ID}")
        except Exception as e:
            print(f"DB Error: {e}")
            # Fallback to CSV if DB fails? Or just print error.
            if DB_ENABLED: # Still might be true but connection failed
                 # Maybe write CSV as backup
                 pass
    else:
        # Fallback to CSV
        output_path = OUTPUT_DIR / f"annexe_iv1_{PIPELINE_RUN_ID}.csv"
        write_to_csv(rows, output_path)
    
    print(f"Extraction complete: {len(rows)} rows.")
