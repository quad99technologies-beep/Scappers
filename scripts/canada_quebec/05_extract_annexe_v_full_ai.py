
import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypedDict

import pdfplumber
from openai import AsyncOpenAI
from pypdf import PdfReader

# Add parent to path
script_path = Path(__file__).resolve().parent
if script_path.exists():
    sys.path.insert(0, str(script_path))

# Load Config
from config_loader import (
    ANNEXE_V_CSV_NAME,
    ANNEXE_V_MAX_ROWS,
    ANNEXE_V_PDF_NAME,
    ANNEXE_V_START_PAGE_1IDX,
    FINAL_COLUMNS,
    LOG_FILE_ANNEXE_V,
    OPENAI_API_KEY,
    OPENAI_MODEL,
    STATIC_CURRENCY,
    STATIC_REGION,
    get_base_dir,
    get_csv_output_dir,
    get_split_pdf_dir,
    DB_ENABLED,
)
from db_handler import DBHandler

# --- CONFIG ---
BASE_DIR = get_base_dir()
INPUT_DIR = get_split_pdf_dir()
OUTPUT_DIR = get_csv_output_dir()
INPUT_PDF = INPUT_DIR / ANNEXE_V_PDF_NAME
OUTPUT_CSV = OUTPUT_DIR / f"full_ai_{ANNEXE_V_CSV_NAME}"
LOG_FILE = OUTPUT_DIR / f"full_ai_{LOG_FILE_ANNEXE_V}"

# Retry/Rate limit config
MAX_CONCURRENT_PAGES = 5  # Conservative to avoid rate limits
MAX_RETRIES = 3
OPENAI_MODEL_NAME = "gpt-4o-mini"
SYSTEM_PROMPT = """
You are a pharmaceutical data extraction expert.
Extract tabular data from the provided raw text of a PDF page (Quebec List of Drugs).
The columns represent:
- Generic Name (often a header above rows, or implicit)
- Formulation (e.g. tablet, injection)
- DIN (Drug Identification Number, 6-9 digits)
- Brand Name
- Manufacturer
- Format (pack size)
- Price (Ex Factory Wholesale Price)
- Unit Price (Price per unit)

RULES:
1. Return purely JSON output with a "rows" array.
2. Each row object must have keys: "Generic", "DIN", "Brand", "Manufacturer", "Format", "Price", "UnitPrice", "Formulation", "Strength", "StrengthUnit".
3. If Generic Name is a header, propagate it to all subsequent rows until a new header appears.
4. Convert prices (e.g. "10,50") to numbers (10.50).
5. DIN must be numeric string (6-8 chars).
6. Ignore footers like dates (YYYY-MM) or page numbers.
"""

# JSON Schema for validation
SCHEMA = {
    "type": "object",
    "properties": {
        "rows": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "Generic": {"type": ["string", "null"]},
                    "DIN": {"type": "string"},
                    "Brand": {"type": ["string", "null"]},
                    "Manufacturer": {"type": ["string", "null"]},
                    "Format": {"type": ["string", "null"]},
                    "Price": {"type": ["number", "null"]},
                    "UnitPrice": {"type": ["number", "null"]},
                    "Formulation": {"type": ["string", "null"]},
                    "Strength": {"type": ["string", "number", "null"]},
                    "StrengthUnit": {"type": ["string", "null"]}
                },
                "required": ["DIN", "Price"]
            }
        }
    }
}


class ExtractedRow(TypedDict):
    Generic: Optional[str]
    DIN: str
    Brand: Optional[str]
    Manufacturer: Optional[str]
    Format: Optional[str]
    Price: Optional[float]
    UnitPrice: Optional[float]
    Formulation: Optional[str]
    Strength: Optional[float]
    StrengthUnit: Optional[str]


# --- LOGGING ---
logging.basicConfig(
    filename=str(LOG_FILE),
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- CLIENT ---
client = AsyncOpenAI(api_key=OPENAI_API_KEY)


def clean_extracted_text(text: str) -> str:
    """Basic cleanup of raw PDF text."""
    if not text:
        return ""
    # Remove excessive newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


async def extract_page_ai(page_text: str, page_num: int) -> List[ExtractedRow]:
    """Extract rows from a single page using OpenAI."""
    if not page_text or len(page_text) < 50:
        return []

    for attempt in range(MAX_RETRIES):
        try:
            response = await client.chat.completions.create(
                model=OPENAI_MODEL_NAME,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"Extract data from this text (Page {page_num}):\n\n{page_text[:15000]}"}
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "drug_extraction",
                        "schema": SCHEMA,
                        "strict": True
                    }
                },
                temperature=0.0
            )
            
            content = response.choices[0].message.content
            if not content:
                return []
                
            data = json.loads(content)
            rows = data.get("rows", [])
            return rows

        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{MAX_RETRIES} failed for Page {page_num}: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Failed to extract Page {page_num} after retries.")
                return []
    return []


async def process_page(sem: asyncio.Semaphore, pdf_path: str, page_idx: int) -> Tuple[int, List[ExtractedRow]]:
    """Process a single page with semaphor to limit concurrency."""
    async with sem:
        try:
            reader = PdfReader(pdf_path)
            # Efficiently read just one page? pypdf lazy loads, so this is okayish.
            # Ideally we'd pass the reader, but it's not thread-safe/async-safe across tasks easily.
            # Local reader instance per task is safer but overhead.
            # Optimization: pass raw text if pre-extracted.
            # Let's assume we pass raw text to avoid file I/O bottlenecks in async loop.
            pass
        except Exception:
            return page_idx, []

    return page_idx, []


async def main():
    print(f"Starting FULL AI Extraction using {OPENAI_MODEL_NAME}...")
    print(f"Input PDF: {INPUT_PDF}")
    print(f"Output CSV: {OUTPUT_CSV}")
    
    # 1. READ RAW TEXT FIRST (Synchronously to avoid pypdf async issues)
    print("Reading PDF text...")
    raw_pages = []
    with pdfplumber.open(INPUT_PDF) as pdf:
        total_pages = len(pdf.pages)
        start_idx = ANNEXE_V_START_PAGE_1IDX - 1
        end_idx = total_pages
        if ANNEXE_V_MAX_ROWS:
             # Heuristic: 20 rows per page approx
             max_pages = (int(ANNEXE_V_MAX_ROWS) // 10) + 1
             end_idx = min(end_idx, start_idx + max_pages)

        for i in range(start_idx, end_idx):
            page = pdf.pages[i]
            text = page.extract_text()
            raw_pages.append((i + 1, text))

    print(f"Loaded {len(raw_pages)} pages for processing.")
    
    # 2. PROCESS ASYNC
    sem = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
    tasks = []
    
    async def worker(p_num, p_text):
        async with sem:
            print(f"Processing Page {p_num}...", flush=True)
            return await extract_page_ai(p_text, p_num)

    start_time = time.time()
    results = await asyncio.gather(*[worker(p[0], p[1]) for p in raw_pages])
    duration = time.time() - start_time
    
    # 3. SAVE RESULTS

    print(f"Extracted {len(all_rows)} rows in {duration:.2f}s.")
    
    if DB_ENABLED:
        # 4. SAVE TO DB IF ENABLED
        try:
            print("Saving to Database...")
            db = DBHandler()
            run_id = os.getenv("PIPELINE_RUN_ID")
            if not run_id:
                run_id = f"manual_{int(time.time())}"
                print(f"No PIPELINE_RUN_ID found. Using generated: {run_id}")
            
            # Convert extracted rows to DB-friendly format
            db_rows = []
            for r in all_rows:
                db_rows.append({
                    "generic_name": r.get("Generic", ""),
                    "formulation": r.get("Formulation") or "",
                    "din": r.get("DIN", "").replace(" ", ""),
                    "brand": r.get("Brand", ""),
                    "manufacturer": r.get("Manufacturer", ""),
                    "format_str": r.get("Format", ""),
                    "price": str(r.get("Price", "")),
                    "unit_price": str(r.get("UnitPrice", "")),
                    "page_num": 0, # Optimization: could map page num if tracked better
                    "annexe": "V"
                })
            
            db.save_rows("annexe_v", db_rows, run_id)
            
            # Log step
            meta = {
                "input_pdf": str(INPUT_PDF),
                "total_rows_extracted": len(all_rows)
            }
            db.log_step(run_id, "Extract Annexe V", "COMPLETED", len(all_rows), duration, meta)
            print("Database save complete. (CSV skipped as per config)")
            
        except Exception as e:
            print(f"[ERROR] Failed to save to DB: {e}")
            # Fallback to CSV on error?
            # User said "we dont want any csv". But if DB fails, we lose data.
            # Let's skip CSV for now as requested.
            pass
            
    else:
        # MAP TO FINAL CSV FORMAT
        import csv
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=FINAL_COLUMNS)
            writer.writeheader()
            
            for r in all_rows:
                # Map AI keys to CSV keys
                final_row = {
                    "Generic Name": r.get("Generic", ""),
                    "Currency": STATIC_CURRENCY,
                    "Ex Factory Wholesale Price": r.get("Price"),
                    "Unit Price": r.get("UnitPrice"),
                    "Region": STATIC_REGION,
                    "Product Group": r.get("Brand") or r.get("Generic"),
                    "Marketing Authority": r.get("Manufacturer"),
                    "Local Pack Description": r.get("Format") or r.get("Brand"), # Fallback
                    "Formulation": r.get("Formulation"),
                    "Fill Size": None, # Could parse from Format
                    "Strength": r.get("Strength"),
                    "Strength Unit": r.get("StrengthUnit"),
                    "LOCAL_PACK_CODE": r.get("DIN", "").zfill(8)
                }
                writer.writerow(final_row)

        print(f"Saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
