
import sys
import os
import pdfplumber
import re
import json
import logging
import concurrent.futures
from pathlib import Path

# Add repo root to path for core imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Italy-specific imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.db.connection import CountryDB
from db.repositories import ItalyRepository
from config_loader import load_env_file

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
PDF_DIR = r"d:\quad99\Scrappers\data\Italy\pdfs"

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def parse_pdf(pdf_path, metadata_lookup):
    extracted_items = []
    filename = os.path.basename(pdf_path)
    # Extract item_id from filename (format: id_attachid_name.pdf)
    parts = filename.split('_')
    item_id = parts[0] if len(parts) > 0 else None
    
    current_meta = metadata_lookup.get(item_id, {})
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            full_text = ""
            for page in pdf.pages:
                full_text += page.extract_text() + "\n"
                
            # Normalize text for regex
            text = full_text.replace("\n", "  ")
            
            # Find all AICs
            aic_matches = list(re.finditer(r"AIC\s+n\.?\s*(\d{6,})", text, re.IGNORECASE))
            
            for match in aic_matches:
                aic = match.group(1)
                start_index = match.start()
                
                context_start = max(0, start_index - 500)
                context_end = min(len(text), start_index + 1000)
                context = text[context_start:context_end]
                
                item = {
                    "source_pdf": filename,
                    "determina_id": item_id,
                    "aic": aic,
                    "product_name": None,
                    "pack_description": None,
                    "price_ex_factory": None,
                    "price_public": None,
                    "company": None
                }
                
                # Extract Price Ex Factory
                ex_factory_match = re.search(r"Prezzo\s+ex[- ]?factory.*?(?:€|EUR)\s*([\d,.]+)", context, re.IGNORECASE)
                if ex_factory_match:
                    item["price_ex_factory"] = ex_factory_match.group(1).replace(",", ".").replace("€", "").strip()

                # Extract Price Public
                public_match = re.search(r"Prezzo\s+al\s+pubblico.*?(?:€|EUR)\s*([\d,.]+)", context, re.IGNORECASE)
                if public_match:
                    item["price_public"] = public_match.group(1).replace(",", ".").replace("€", "").strip()
                
                # Extract Product Name / Pack Description
                pre_aic_text = text[max(0, start_index - 300):start_index]
                confezione_match = re.search(r"Confezione\s+(.*)", pre_aic_text, re.IGNORECASE)
                if confezione_match:
                    conf_text = confezione_match.group(1).strip()
                    item["pack_description"] = clean_text(conf_text)
                    item["product_name"] = conf_text.split(" ")[0]
                    
                extracted_items.append(item)
                
    except Exception as e:
        logger.error(f"Error parsing {filename}: {e}")
        
    return extracted_items

def main():
    load_env_file()
    run_id = os.environ.get("ITALY_RUN_ID")
    if not run_id:
        logger.error("ITALY_RUN_ID not found in environment.")
        sys.exit(1)

    db = CountryDB("Italy")
    repo = ItalyRepository(db, run_id)
    
    # Load metadata from DB
    determinas = repo.get_determinas()
    metadata_lookup = {d.get("determina_id"): d for d in determinas}
    
    if not os.path.exists(PDF_DIR):
        logger.error(f"PDF directory not found: {PDF_DIR}")
        db.close()
        return

    files = [os.path.join(PDF_DIR, f) for f in os.listdir(PDF_DIR) if f.endswith(".pdf")]
    logger.info(f"Found {len(files)} PDFs to process.")
    
    all_extracted = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_file = {executor.submit(parse_pdf, f, metadata_lookup): f for f in files}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_file)):
            try:
                data = future.result()
                all_extracted.extend(data)
            except Exception as e:
                logger.error(f"Error processing file: {e}")
            
            if i % 10 == 0:
                logger.info(f"Processed {i}/{len(files)} files.")
                
    logger.info(f"Extraction complete. Found {len(all_extracted)} items.")
    
    # Save results to DB
    if all_extracted:
        repo.insert_products(all_extracted)
        logger.info(f"Saved {len(all_extracted)} products to database.")
    
    db.close()

if __name__ == "__main__":
    main()
