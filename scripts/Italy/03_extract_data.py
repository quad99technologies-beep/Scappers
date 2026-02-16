
import pdfplumber
import re
import os
import json
import logging
import concurrent.futures

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
PDF_DIR = r"d:\quad99\Scrappers\data\Italy\pdfs"
DETERMINAS_LIST = r"d:\quad99\Scrappers\data\Italy\determinas_list.jsonl"
OUTPUT_FILE = r"d:\quad99\Scrappers\data\Italy\extracted_data.jsonl"

def load_metadata(filepath):
    """Load metadata from the list file to map ID to Dates/Context."""
    meta = {}
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    item = json.loads(line)
                    meta[item.get("id")] = item
    return meta

METADATA = load_metadata(DETERMINAS_LIST)

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def parse_pdf(pdf_path):
    extracted_items = []
    filename = os.path.basename(pdf_path)
    # Extract item_id from filename (format: id_attachid_name.pdf)
    parts = filename.split('_')
    item_id = parts[0] if len(parts) > 0 else None
    
    current_meta = METADATA.get(item_id, {})
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            full_text = ""
            for page in pdf.pages:
                full_text += page.extract_text() + "\n"
                
            # Parsing Logic
            # 1. Identify "Confezione" blocks
            # Pattern: "Confezione" followed by description, then "AIC n." ... "Prezzo"
            
            # Helper to find blocks
            # We look for AIC n. first as anchor?
            
            # Improved Regex Strategy:
            # Look for segments like:
            # Confezione <Description>
            # AIC n. <Code>
            # ...
            # Prezzo ex-factory ... <Price>
            # Prezzo al pubblico ... <Price>
            
            # Normalize text for regex
            text = full_text.replace("\n", "  ") # Double space to preserve some boundaries?
            
            # Find all AICs
            # AIC n. 052594013
            aic_matches = list(re.finditer(r"AIC\s+n\.?\s*(\d{6,})", text, re.IGNORECASE))
            
            for match in aic_matches:
                aic = match.group(1)
                start_index = match.start()
                
                # Context window around AIC
                # Look backwards for "Confezione" or Product Name
                # Look forwards for Prices
                
                context_start = max(0, start_index - 500)
                context_end = min(len(text), start_index + 1000)
                context = text[context_start:context_end]
                
                item = {
                    "source_pdf": filename,
                    "determina_id": item_id,
                    "determina_date": current_meta.get("dataPubblicazione"),
                    "aic": aic,
                    "product_name": None,
                    "pack_description": None,
                    "price_ex_factory": None,
                    "price_public": None,
                    "company": None # Hard to extract reliably without more anchors
                }
                
                # Extract Price Ex Factory
                # Prezzo ex-factory (IVA esclusa) € 6,37
                ex_factory_match = re.search(r"Prezzo\s+ex[- ]?factory.*?(?:€|EUR)\s*([\d,.]+)", context, re.IGNORECASE)
                if ex_factory_match:
                    item["price_ex_factory"] = ex_factory_match.group(1).replace(",", ".").replace("€", "").strip()

                # Extract Price Public
                # Prezzo al pubblico (IVA inclusa) € 10,52
                public_match = re.search(r"Prezzo\s+al\s+pubblico.*?(?:€|EUR)\s*([\d,.]+)", context, re.IGNORECASE)
                if public_match:
                    item["price_public"] = public_match.group(1).replace(",", ".").replace("€", "").strip()
                
                # Extract Product Name / Pack Description
                # Usually precedes AIC. Look for "Confezione"
                # "Confezione AUGMENTIN “875 mg/125 mg ...” 12 bustine ..."
                
                # We can search backwards from AIC for "Confezione"
                pre_aic_text = text[max(0, start_index - 300):start_index]
                confezione_match = re.search(r"Confezione\s+(.*)", pre_aic_text, re.IGNORECASE)
                if confezione_match:
                    conf_text = confezione_match.group(1).strip()
                    # Improve clean up
                    # It might capture "AUGMENTIN ... AIC n." -> handled by start_index limit
                    # Basic extraction
                    item["pack_description"] = clean_text(conf_text)
                    
                    # Try to extract Product Name (first word or capitalized string?)
                    # "AUGMENTIN"
                    item["product_name"] = conf_text.split(" ")[0] # Naive
                    
                extracted_items.append(item)
                
    except Exception as e:
        logger.error(f"Error parsing {filename}: {e}")
        
    return extracted_items

def main():
    files = [os.path.join(PDF_DIR, f) for f in os.listdir(PDF_DIR) if f.endswith(".pdf")]
    logger.info(f"Found {len(files)} PDFs to process.")
    
    all_extracted = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_file = {executor.submit(parse_pdf, f): f for f in files}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_file)):
            try:
                data = future.result()
                all_extracted.extend(data)
            except Exception as e:
                logger.error(f"Error processing file: {e}")
            
            if i % 10 == 0:
                logger.info(f"Processed {i}/{len(files)} files.")
                
    logger.info(f"Extraction complete. Found {len(all_extracted)} items.")
    
    # Save results
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for item in all_extracted:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    
    logger.info(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
