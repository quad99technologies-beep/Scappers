#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Canada Ontario Final Output Generator

Reads products.csv and generates final output report with standardized columns.
Saves to exports/CanadaOntario/ with timestamp.

Output format:
- PCID (blank, can be populated later)
- Country
- Company (manufacturer_name)
- Local Product Name (blank or derived)
- Generic Name
- Effective Start Date (blank)
- Public With VAT Price
- Reimbursement Category
- Reimbursement Amount
- Co-Pay Amount
- Local Pack Description (brand_name_strength_dosage)
- LOCAL_PACK_CODE
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add repo root to path for core imports (MUST be before any core imports)
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add script directory to path for config_loader import (must be first to avoid loading another scraper's config)
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Clear conflicting config_loader when run in same process as other scrapers (e.g. GUI)
if "config_loader" in sys.modules:
    del sys.modules["config_loader"]

from config_loader import (
    get_output_dir, get_central_output_dir,
    FINAL_REPORT_NAME_PREFIX, FINAL_REPORT_DATE_FORMAT,
    PRODUCTS_CSV_NAME, STATIC_CURRENCY, STATIC_REGION,
    get_run_id, get_run_dir, getenv_bool,
)
from core.utils.logger import setup_standard_logger
from core.progress.progress_tracker import StandardProgress
from core.db.postgres_connection import PostgresDB
from scraper_utils import parse_float  # canonical implementation shared with steps 01 & 02

import pandas as pd

# Paths
OUTPUT_DIR = get_output_dir()
CENTRAL_OUTPUT_DIR = get_central_output_dir()  # This is exports/CanadaOntario/
INPUT_CSV = OUTPUT_DIR / PRODUCTS_CSV_NAME

# Generate date-based filename: canadaontarioreport_ddmmyyyy.csv
date_str = datetime.now().strftime(FINAL_REPORT_DATE_FORMAT)
# Save directly to exports folder (final output folder)
OUTPUT_CSV = CENTRAL_OUTPUT_DIR / f"{FINAL_REPORT_NAME_PREFIX}{date_str}.csv"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CENTRAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Module-level logger (also used inside insert_final_output_to_db before main() sets up its own).
import logging as _logging
logger = _logging.getLogger(__name__)

# Module-level DB connection and repository — shared to avoid per-call connections.
try:
    from db.repositories import CanadaOntarioRepository
    from db.schema import apply_canada_ontario_schema
except ImportError:
    from scripts.canada_ontario.db.repositories import CanadaOntarioRepository
    from scripts.canada_ontario.db.schema import apply_canada_ontario_schema

_run_id_for_repo = get_run_id()
_DB = PostgresDB("CanadaOntario")
_DB.connect()
apply_canada_ontario_schema(_DB)
_REPO = CanadaOntarioRepository(_DB, _run_id_for_repo)


def insert_final_output_to_db(df: pd.DataFrame) -> None:
    """Insert final output via CanadaOntarioRepository."""
    if df.empty:
        logger.warning("[DB] No final output to insert - dataframe is empty")
        return
    try:
        outputs = [
            {
                "pcid": row.get("PCID", ""),
                "country": "CANADA",
                "region": "NORTH AMERICA",
                "company": row.get("Company", ""),
                "local_product_name": row.get("Local Product Name", ""),
                "generic_name": row.get("Generic Name", ""),
                "unit_price": parse_float(row.get("Public With VAT Price")),
                "public_with_vat_price": parse_float(row.get("Public With VAT Price")),
                "public_without_vat_price": None,
                "eap_price": None,
                "currency": "CAD",
                "reimbursement_category": row.get("Reimbursement Category", ""),
                "reimbursement_amount": parse_float(row.get("Reimbursement Amount")),
                "copay_amount": parse_float(row.get("Co-Pay Amount")),
                "benefit_status": "",
                "interchangeability": "",
                "din": row.get("LOCAL_PACK_CODE", ""),
                "strength": "",
                "dosage_form": "",
                "pack_size": "",
                "local_pack_description": row.get("Local Pack Description", ""),
                "local_pack_code": row.get("LOCAL_PACK_CODE", ""),
                "effective_start_date": row.get("Effective Start Date", ""),
                "effective_end_date": "",
                "source": "PRICENTRIC",
            }
            for _, row in df.iterrows()
        ]
        count = _REPO.insert_final_output(outputs)
        logger.info(f"[DB] Inserted {count} rows via repository to co_final_output")
    except Exception as e:
        logger.error(f"[DB] Failed to insert final output: {e}")


def determine_reimbursement_category(row: pd.Series) -> tuple:
    """
    Determine reimbursement category and amounts based on Ontario rules.
    
    Returns: (category, reimbursement_amount, copay_amount)
    """
    reimbursable_price = parse_float(row.get("reimbursable_price"))
    copay = parse_float(row.get("copay"))
    public_with_vat = parse_float(row.get("public_with_vat"))
    
    # If reimbursable_price exists and is > 0, product is reimbursable
    if reimbursable_price is not None and reimbursable_price > 0:
        # Determine category based on copay
        if copay is not None and copay > 0:
            category = "REIMBURSABLE WITH COPAY"
        else:
            category = "FULLY REIMBURSABLE"
        return category, reimbursable_price, copay if copay is not None else 0.0
    else:
        # No reimbursement
        return "NON REIMBURSABLE", None, copay if copay is not None else None


def main():
    """Main entry point."""
    global logger
    run_id = get_run_id()
    run_dir = get_run_dir(run_id)
    logger = setup_standard_logger(
        "canada_ontario_output",
        scraper_name="CanadaOntario",
        log_file=run_dir / "logs" / "final_output.log",
    )
    progress = StandardProgress(
        "canada_ontario_output",
        total=3,
        unit="steps",
        logger=logger,
        state_path=CENTRAL_OUTPUT_DIR / "output_progress.json",
        log_every=1,
    )
    logger.info("Canada Ontario: Generating final report...")
    
    # Load data: DB first (primary), CSV fallback (legacy)
    progress.update(0, message="load data", force=True)
    df = None
    db_only = getenv_bool("DB_ONLY", True)
    
    try:
        from core.db.postgres_connection import PostgresDB
        import importlib.util
        _co_dir = Path(__file__).resolve().parent
        
        # Robustly load repositories and schema
        repo_file = _co_dir / "db" / "repositories.py"
        schema_file = _co_dir / "db" / "schema.py"
        
        spec_repo = importlib.util.spec_from_file_location("co_db_repo", str(repo_file))
        co_db_repo = importlib.util.module_from_spec(spec_repo)
        spec_repo.loader.exec_module(co_db_repo)
        CanadaOntarioRepository = co_db_repo.CanadaOntarioRepository
        
        spec_schema = importlib.util.spec_from_file_location("co_db_schema", str(schema_file))
        co_db_schema = importlib.util.module_from_spec(spec_schema)
        spec_schema.loader.exec_module(co_db_schema)
        apply_canada_ontario_schema = co_db_schema.apply_canada_ontario_schema

        db = PostgresDB("CanadaOntario")
        db.connect()
        apply_canada_ontario_schema(db)
        repo = CanadaOntarioRepository(db, get_run_id())
        products = repo.get_all_products()
        db.close()
        if products:
            df = pd.DataFrame(products)
            # Map DB columns to expected names for downstream processing
            df = df.rename(columns={
                "local_pack_description": "brand_name_strength_dosage",
                "manufacturer": "manufacturer_name",
                "din": "local_pack_code",
                "interchangeability": "interchangeable",
                "therapeutic_notes": "therapeutic_notes_requirements",
            })
            logger.info("Loaded %s rows from co_products (DB)", len(df))
    except Exception as e:
        logger.debug("DB load failed: %s", e)
    
    if df is None or df.empty:
        if db_only:
             raise RuntimeError(f"No product data found in database for run_id {get_run_id()}.")
             
        if not INPUT_CSV.exists():
            raise FileNotFoundError(
                f"No product data found. Run script 01 (extract_product_details.py) first.\n"
                f"DB query failed and CSV not found: {INPUT_CSV}"
            )
        df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
        logger.info("Loaded %s rows from %s (CSV fallback)", len(df), INPUT_CSV.name)
    
    # Build final output dataframe
    progress.update(1, message="process data", force=True)
    
    # Initialize final dataframe
    df_final = pd.DataFrame()
    
    # Standard columns (matching other scrapers' format)
    df_final["PCID"] = ""  # Blank, can be populated later via mapping
    
    # Country
    df_final["Country"] = "CANADA"
    
    # Company (from manufacturer_name)
    df_final["Company"] = df["manufacturer_name"].fillna("").astype(str)
    
    # Local Product Name (blank for now, can be derived from brand_name_strength_dosage if needed)
    df_final["Local Product Name"] = ""
    
    # Generic Name
    df_final["Generic Name"] = df["generic_name"].fillna("").astype(str)
    
    # Effective Start Date (blank for now)
    df_final["Effective Start Date"] = ""
    
    # Public With VAT Price
    df_final["Public With VAT Price"] = df["public_with_vat"].apply(parse_float)
    
    # Reimbursement fields
    reimbursement_data = df.apply(determine_reimbursement_category, axis=1)
    df_final["Reimbursement Category"] = [x[0] for x in reimbursement_data]
    df_final["Reimbursement Amount"] = [x[1] for x in reimbursement_data]
    df_final["Co-Pay Amount"] = [x[2] for x in reimbursement_data]
    
    # Local Pack Description (from brand_name_strength_dosage)
    df_final["Local Pack Description"] = df["brand_name_strength_dosage"].fillna("").astype(str)
    
    # LOCAL_PACK_CODE
    df_final["LOCAL_PACK_CODE"] = df["local_pack_code"].fillna("").astype(str)
    
    # Additional Ontario-specific fields (optional, for reference)
    df_final["Price Type"] = df["price_type"].fillna("").astype(str)  # UNIT/PACK
    df_final["Interchangeable"] = df["interchangeable"].fillna("").astype(str)
    df_final["Limited Use"] = df["limited_use"].fillna("").astype(str)
    df_final["Therapeutic Notes"] = df["therapeutic_notes_requirements"].fillna("").astype(str)
    
    # Static values
    df_final["Currency"] = STATIC_CURRENCY
    df_final["Region"] = STATIC_REGION
    
    # Reorder columns to match standard format (PCID first, then standard columns)
    final_cols = [
        "PCID",
        "Country",
        "Company",
        "Local Product Name",
        "Generic Name",
        "Effective Start Date",
        "Public With VAT Price",
        "Reimbursement Category",
        "Reimbursement Amount",
        "Co-Pay Amount",
        "Local Pack Description",
        "LOCAL_PACK_CODE",
        "Price Type",
        "Interchangeable",
        "Limited Use",
        "Therapeutic Notes",
        "Currency",
        "Region",
    ]
    
    # Ensure all columns exist
    for col in final_cols:
        if col not in df_final.columns:
            df_final[col] = ""
    
    df_final = df_final[final_cols].copy()
    
    # Write output CSV
    progress.update(2, message="write output", force=True)
    df_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig", float_format="%.2f")
    
    # Save to DB
    logger.info("[DB] Migrating final output to co_final_output table...")
    insert_final_output_to_db(df_final)
    logger.info("Final output migration complete")
    
    progress.update(3, message="complete", force=True)
    
    logger.info(f"OK: Final report generated: {OUTPUT_CSV}")
    logger.info(f"Total rows: {len(df_final):,}")


if __name__ == "__main__":
    main()
