
#!/usr/bin/env python3

import os
import sys
import re
import logging
import json
import pdfplumber
import concurrent.futures
from pathlib import Path

# Path setup
_repo_root = Path(__file__).resolve().parents[3]
_italy_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo_root))
sys.path.insert(0, str(_italy_dir))

from core.db.connection import CountryDB
from db.repositories import ItalyRepository

logger = logging.getLogger(__name__)


PDF_DIR = r"d:\quad99\Scrappers\data\Italy\pdfs"
JSON_DIR = r"d:\quad99\Scrappers\data\Italy\json"

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def parse_pdf(pdf_path):
    extracted_items = []
    filename = os.path.basename(pdf_path)
    parts = filename.split('_')
    item_id = parts[0] if len(parts) > 0 else None
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            full_text = ""
            for page in pdf.pages:
                full_text += page.extract_text() + "\n"
                
            text = full_text.replace("\n", "  ")
            aic_matches = list(re.finditer(r"AIC\s+n\.?\s*(\d{6,})", text, re.IGNORECASE))
            
            for match in aic_matches:
                aic = match.group(1)
                start_index = match.start()
                context_start = max(0, start_index - 500)
                context_end = min(len(text), start_index + 1000)
                context = text[context_start:context_end]
                
                item = {
                    "determina_id": item_id,
                    "aic": aic,
                    "source_pdf": filename,
                    "product_name": None,
                    "pack_description": None,
                    "price_ex_factory": None,
                    "price_public": None
                }
                
                # Regex logic (same as original)

                # Regex logic - Improved for Italy Scenarios
                # Matches: "Prezzo ex factory ... € 10,00" or "Prezzo ex-factory (IVA esclusa) € 10,00"
                # Limit intervening chars to 100 to avoid false positives across lines
                ex_factory_match = re.search(r"Prezzo\s+ex[- ]?factory.{0,100}?(?:€|EUR)\s*([\d,.]+)", context, re.IGNORECASE | re.DOTALL)
                if ex_factory_match:
                    try:
                        val_str = ex_factory_match.group(1).replace(".", "").replace(",", ".")
                        item["price_ex_factory"] = float(val_str)
                    except ValueError:
                        pass

                public_match = re.search(r"Prezzo\s+al\s+pubblico.{0,100}?(?:€|EUR)\s*([\d,.]+)", context, re.IGNORECASE | re.DOTALL)
                if public_match:
                    try:
                        val_str = public_match.group(1).replace(".", "").replace(",", ".")
                        item["price_public"] = float(val_str)
                    except ValueError:
                        pass

                
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

def parse_json(json_path):
    extracted_items = []
    filename = os.path.basename(json_path)
    item_id = filename.replace(".json", "")
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        text = data.get("testo", "")
        if not text:
            return []
            
        # Text format: ... - A.I.C. n. 049930011 - Prezzo € 24,24
        aic_matches = list(re.finditer(r"A\.?I\.?C\.?\s*n\.?\s*(\d{6,})", text, re.IGNORECASE))
        
        for match in aic_matches:
            aic = match.group(1)
            start_index = match.start()
            context_end = min(len(text), start_index + 200) # Short context
            context = text[start_index:context_end]
            
            item = {
                "determina_id": item_id,
                "aic": aic,
                "source_pdf": filename, # Using json filename as source
                "product_name": None,
                "pack_description": None,
                "price_ex_factory": None,
                "price_public": None
            }
            
            # Simple Price Regex for this format
            price_match = re.search(r"Prezzo\s*(?:€|EUR)\s*([\d,.]+)", context, re.IGNORECASE)
            if price_match:
                try:
                    val_str = price_match.group(1).replace(".", "").replace(",", ".")
                    item["price_public"] = float(val_str)
                except ValueError:
                    pass
            
            # Try to get product name from text before AIC
            pre_aic = text[max(0, start_index - 100):start_index]
            lines = pre_aic.split('\n')
            last_line = lines[-1].strip()
            last_line = last_line.replace("Specialita' medicinali:", "").strip()
            item["product_name"] = last_line
            
            extracted_items.append(item)
            
    except Exception as e:
        logger.error(f"Error parsing JSON {filename}: {e}")
        
    return extracted_items

def main():
    run_id = os.environ.get("ITALY_RUN_ID", "manual_run")
    db = CountryDB("Italy")
    repo = ItalyRepository(db, run_id)
    

    files = [os.path.join(PDF_DIR, f) for f in os.listdir(PDF_DIR) if f.endswith(".pdf")]
    
    json_files = []
    if os.path.exists(JSON_DIR):
        json_files = [os.path.join(JSON_DIR, f) for f in os.listdir(JSON_DIR) if f.endswith(".json")]
        
    logger.info(f"Step 3: Extracting from {len(files)} PDFs and {len(json_files)} JSONs")
    
    repo.clear_step_data(3) # Clear previous extraction for this run
    
    extracted_batch = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_file = {executor.submit(parse_pdf, f): f for f in files}
        # Add JSON files
        future_to_file.update({executor.submit(parse_json, f): f for f in json_files})
        
        for future in concurrent.futures.as_completed(future_to_file):
            items = future.result()
            extracted_batch.extend(items)
            
            if len(extracted_batch) > 100:
                repo.insert_products(extracted_batch)
                extracted_batch = []
                
    if extracted_batch:
        repo.insert_products(extracted_batch)
        
    logger.info("Step 3 Complete.")

if __name__ == "__main__":
    main()
