
#!/usr/bin/env python3
import os
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path

# Path setup
_repo_root = Path(__file__).resolve().parents[3]
_italy_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo_root))
sys.path.insert(0, str(_italy_dir))

from core.db.connection import CountryDB
from db.repositories import ItalyRepository

def main():
    run_id = os.environ.get("ITALY_RUN_ID", "manual_run")
    db = CountryDB("Italy")
    repo = ItalyRepository(db, run_id)
    
    DATA_DIR = r"d:\quad99\Scrappers\data\Italy"
    OUTPUT_EXCEL = os.path.join(DATA_DIR, f"italy_pricing_{datetime.now().strftime('%Y%m%d')}.xlsx")
    
    products = repo.get_products_for_export()
    
    print(f"Exporting {len(products)} rows...")
    
    # Transform
    transformed = []
    for item in products:
        row = {
            "Country": "ITALY (GAZZETTA / AIFA)",
            "Company": item.get("company", ""),
            "Product Group": item.get("product_name", ""),
            "Local Product Name": item.get("pack_description", ""),
            "Generic Name": "",
            "Indication": "",
            "Pack Size": "",
            "Effective Start Date": str(item.get("publish_date", ""))[:10],
            "Effective End Date": "",
            "Currency": "EUR",
            "Ex Factory Wholesale Price": item.get("price_ex_factory", ""),
            "Public with vat price": item.get("price_public", ""),
            "VAT Percent": "",
            "Margin Rule": "",
            "Package Notes": "",
            "Discontinued": "",
            "Region": "EUROPE",
            "WHO ATC Code": "",
            "Marketing Authority": "",
            "Local Pack Description": item.get("pack_description", ""),
            "Formulation": "",
            "Fill Unit": "",
            "Fill Size": "",
            "Pack Unit": "",
            "Strength": "",
            "Strength Unit": "",
            "Brand Type": "Branded" if item.get("product_name") else "",
            "Import Type": "",
            "Combination Molecule": "",
            "Source": "PRICENTRIC",
            "Quality Control Status": "",
            "Quality Control Notes": "",
            "Client": "VALUE NEEDED",
            "Client Pack Description": "",
            "LOCAL_PACK_CODE": item.get("aic_code", ""),
            "APPROVAL_DT": "",
            "DDD Value": "",
            "DDD Unit": "",
            "Source File": item.get("source_pdf", "")
        }
        transformed.append(row)
        
    df = pd.DataFrame(transformed)
    df.to_excel(OUTPUT_EXCEL, index=False)
    print(f"Saved to {OUTPUT_EXCEL}")

if __name__ == "__main__":
    main()
