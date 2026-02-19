import pdfplumber
import csv
import re
import sys
import os
import time
from pathlib import Path

# Add parent directory to path to import config_loader
script_path = Path(__file__).resolve().parent
sys.path.insert(0, str(script_path))

from config_loader import get_split_pdf_dir, DB_ENABLED

INPUT_PDF = get_split_pdf_dir() / "annexe_iv.pdf"

def extract_annexe_iv():
    if not INPUT_PDF.exists():
        print(f"Skipping Annexe IV: {INPUT_PDF} not found.")
        return []

    print(f"Scanning Annexe IV for data rows: {INPUT_PDF}")
    re_din = re.compile(r"\b\d{6,8}\b")
    
    rows_found = 0
    with pdfplumber.open(INPUT_PDF) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            dins = re_din.findall(text)
            rows_found += len(dins)
            
    print(f"Annexe IV scan complete. Found {rows_found} DIN-like patterns.")
    print("Note: Annexe IV is primarily therapeutic criteria text.")
    return rows_found

if __name__ == "__main__":
    start_time = time.time()
    count = extract_annexe_iv()
    duration = time.time() - start_time
    
    PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID", f"manual_iv_{int(time.time())}")
    
    if DB_ENABLED:
        try:
            from db_handler import DBHandler
            db = DBHandler()
            db.log_step(PIPELINE_RUN_ID, "Extract Annexe IV", "COMPLETED", count, duration, {"note": "Scan only, criteria-heavy section"})
        except:
            pass
