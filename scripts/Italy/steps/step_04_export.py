
#!/usr/bin/env python3
import os
import sys
import logging
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
from config_loader import get_central_output_dir

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OUTPUT_DIR = get_central_output_dir() / "runs"

def main():
    run_id = os.environ.get("ITALY_RUN_ID", "manual_run")
    db = CountryDB("Italy")
    repo = ItalyRepository(db, run_id)
    
    logger.info(f"Step 4: Generating reports for RunID: {run_id}")
    
    products = repo.get_products_for_export()
    if not products:
        logger.warning(f"No products found for run {run_id}")
        return

    try:
        repo.upsert_stat("*", 4, "products_export_candidates", len(products))
    except Exception:
        pass
        
    df = pd.DataFrame(products)
    
    # Ensure typology exists (might be NaN)
    if 'typology' not in df.columns:
        df['typology'] = 'UNKNOWN'
    else:
        df['typology'] = df['typology'].fillna('UNKNOWN')

    # Separate by keyword if available; fall back to typology.
    if "source_keyword" not in df.columns:
        df["source_keyword"] = ""
    df["source_keyword"] = df["source_keyword"].fillna("")

    df_det = df[df["source_keyword"].str.upper() == "DET"]
    df_riduzione = df[df["source_keyword"].str.lower() == "riduzione"]

    # Backward-compat: if keyword not populated, approximate riduzione by typology=MSF.
    if df_det.empty and df_riduzione.empty:
        df_riduzione = df[df["typology"] == "MSF"]
        df_det = df[df["typology"] != "MSF"]
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Helper to save
    def save_excel(dataframe, name_suffix):
        if dataframe.empty:
            logger.info(f"No data for {name_suffix}, skipping export.")
            return None
        filename = f"Italy_Pricing_{name_suffix}_{timestamp}.xlsx"
        path = OUTPUT_DIR / filename
        dataframe.to_excel(str(path), index=False)
        logger.info(f"Exported {name_suffix}: {path} ({len(dataframe)} rows)")
        return path

    rid_path = save_excel(df_riduzione, "Riduzione")
    det_path = save_excel(df_det, "DET")

    try:
        repo.upsert_stat("Riduzione", 4, "rows_exported", int(len(df_riduzione)))
        repo.upsert_stat("DET", 4, "rows_exported", int(len(df_det)))
        repo.upsert_stat("*", 4, "files_exported", int(bool(rid_path)) + int(bool(det_path)))
    except Exception:
        pass
    
    logger.info("Step 4 Complete.")

if __name__ == "__main__":
    main()
