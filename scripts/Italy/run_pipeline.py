
import os
import subprocess
import sys
import logging
import json
import pandas as pd
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPTS_DIR = r"d:\quad99\Scrappers\scripts\Italy"
DATA_DIR = r"d:\quad99\Scrappers\data\Italy"
OUTPUT_EXCEL = os.path.join(DATA_DIR, f"italy_pricing_{datetime.now().strftime('%Y%m%d')}.xlsx")

def run_step(script_name):
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    logger.info(f"Running step: {script_name}")
    try:
        subprocess.run([sys.executable, script_path], check=True)
        logger.info(f"Step {script_name} completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Step {script_name} failed with error: {e}")
        sys.exit(1)

def transform_to_excel():
    run_id = os.environ.get("ITALY_RUN_ID")
    if not run_id:
        logger.error("ITALY_RUN_ID not found.")
        return

    from core.db.connection import CountryDB
    from db.repositories import ItalyRepository
    
    db = CountryDB("Italy")
    repo = ItalyRepository(db, run_id)
    
    data = repo.get_products_for_export()
    db.close()
    
    if not data:
        logger.warning("No data found for export in database.")
        return

    # Transform to required schema
    transformed = []
    for item in data:
        row = {
            "Country": "ITALY (GAZZETTA / AIFA)",
            "Company": item.get("company", ""), # Need to improve extraction
            "Product Group": item.get("product_name", ""),
            "Local Product Name": item.get("pack_description", ""), # Or combine name + desc
            "Generic Name": "",
            "Indication": "", # From PDF text analysis?
            "Pack Size": "", # Parse from description?
            "Effective Start Date": item.get("determina_date", "")[:10] if item.get("determina_date") else "", # YYYY-MM-DD
            "Effective End Date": "",
            "Currency": "EUR",
            "Ex Factory Wholesale Price": item.get("price_ex_factory", ""),
            "Public with vat price": item.get("price_public", ""),
            "VAT Percent": "", # Could be inferred?
            "Margin Rule": "",
            "Package Notes": "",
            "Discontinued": "",
            "Region": "EUROPE",
            "WHO ATC Code": "", # Could extract if present
            "Marketing Authority": "",
            "Local Pack Description": item.get("pack_description", ""),
            "Formulation": "", # Parse
            "Fill Unit": "", # Parse
            "Fill Size": "", # Parse
            "Pack Unit": "", # Parse
            "Strength": "", # Parse
            "Strength Unit": "", # Parse
            "Brand Type": "Branded" if item.get("product_name") else "", # Guess
            "Import Type": "",
            "Combination Molecule": "",
            "Source": "PRICENTRIC", # Or AIFA?
            "Quality Control Status": "",
            "Quality Control Notes": "",
            "Client": "VALUE NEEDED",
            "Client Pack Description": "",
            "LOCAL_PACK_CODE": item.get("aic", ""),
            "APPROVAL_DT": "",
            "DDD Value": "",
            "DDD Unit": "",
            "Source File": item.get("source_pdf", "")
        }
        transformed.append(row)
        
    df = pd.DataFrame(transformed)
    df.to_excel(OUTPUT_EXCEL, index=False)
    logger.info(f"Saved {len(df)} rows to {OUTPUT_EXCEL}")

def main():
    # step 1: List
    run_step("01_list_determinas.py")
    
    # step 2: Download
    run_step("02_download_pdfs.py")
    
    # step 3: Extract
    run_step("03_extract_data.py")
    
    # step 4: Transform & Save
    transform_to_excel()
    
    logger.info("Pipeline completed.")

if __name__ == "__main__":
    main()
