"""
Consolidate product details and costs into a single file.

This script:
- Reads from database (preferred) or CSV files (fallback)
- Generates final report in the standard format
- Saves to database and/or CSV
"""

from __future__ import annotations

import sys
import os
from datetime import datetime
from typing import Optional

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

from pathlib import Path
import pandas as pd
from config_loader import load_env_file, require_env, getenv, getenv_list, get_output_dir

# Add script directory to path
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Database imports
try:
    from db.repositories import NetherlandsRepository
    from db.schema import apply_netherlands_schema
    from core.db.postgres_connection import get_db
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False
    NetherlandsRepository = None

from core.pipeline.standalone_checkpoint import run_with_checkpoint

# Load environment variables from .env file
load_env_file()

# Paths - Use ConfigManager output directory instead of local output folder
output_base_dir_path = getenv("SCRIPT_03_OUTPUT_BASE_DIR", "")
if output_base_dir_path and Path(output_base_dir_path).is_absolute():
    OUTPUT_BASE_DIR = Path(output_base_dir_path)
else:
    OUTPUT_BASE_DIR = get_output_dir()

CONSOLIDATED_FILE = OUTPUT_BASE_DIR / require_env("SCRIPT_03_CONSOLIDATED_FILE", "consolidated_products.csv")
FINAL_REPORT_FILE = OUTPUT_BASE_DIR / "final_report.csv"

OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)


def consolidate_product_details(run_id: str = None) -> None:
    """Process packs data into standardized final report format.

    Args:
        run_id: Optional run ID for database operations. If not provided, tries environment variable, then generates one from timestamp.
    """
    # Generate run_id if not provided
    # Priority: 1) function parameter, 2) environment variable, 3) generate new
    if run_id is None:
        run_id = os.environ.get("NL_RUN_ID")
        if not run_id:
            run_id = f"nl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    print("=" * 60, flush=True)
    print("NETHERLANDS STEP 3: CONSOLIDATE RESULTS", flush=True)
    print(f"Run ID: {run_id}", flush=True)
    print("=" * 60, flush=True)

    # Try DB-first approach
    repo: Optional[NetherlandsRepository] = None
    if DB_AVAILABLE:
        try:
            db = get_db("Netherlands")
            apply_netherlands_schema(db)
            repo = NetherlandsRepository(db, run_id)
            repo.ensure_run_in_ledger(mode="resume")
            print("[DB] Database connection established", flush=True)

            # Check if we have data in nl_packs table
            packs_count = repo.get_packs_count()

            if packs_count > 0:
                print(f"[DB] Found {packs_count} packs in database", flush=True)
                print("[DB] Generating final report...", flush=True)

                # Export final report in standard format
                final_count = repo.export_final_report(FINAL_REPORT_FILE)
                print(f"[DB] Exported {final_count} rows to final report", flush=True)
                print(f"[DB] Final report: {FINAL_REPORT_FILE}", flush=True)

                # Also export consolidated for backward compatibility
                repo.export_consolidated_csv(CONSOLIDATED_FILE)
                print(f"[DB] Consolidated file: {CONSOLIDATED_FILE}", flush=True)

                print(f"\n[SUCCESS] Consolidation complete", flush=True)
                print(f"  Total rows: {final_count:,}", flush=True)
                return

            print("[DB] No data in nl_packs table, falling back to CSV files...", flush=True)

        except Exception as e:
            print(f"[DB] WARNING: Database operation failed: {e}", flush=True)
            import traceback
            traceback.print_exc()
            print("[DB] Falling back to CSV files...", flush=True)

    # Fallback to CSV-based consolidation
    # For Netherlands, the main data comes from packs.csv (Step 1)
    PACKS_CSV = OUTPUT_BASE_DIR / "packs.csv"
    
    if not PACKS_CSV.exists():
        print(f"ERROR: Packs file not found: {PACKS_CSV}", flush=True)
        print("Please run Script 01 first to generate packs.csv", flush=True)
        return

    print(f"Reading packs from: {PACKS_CSV}", flush=True)

    try:
        print(f"  -> Loading packs CSV file...", flush=True)
        df_packs = pd.read_csv(PACKS_CSV, dtype=str, keep_default_na=False)
        print(f"  -> Loaded {len(df_packs):,} pack rows, {len(df_packs.columns)} columns", flush=True)

        print(f"[PROGRESS] Consolidating: Processing {len(df_packs):,} packs", flush=True)

        # Create final report dataframe with standard columns
        final_report = pd.DataFrame()
        final_report["PCID"] = ""
        final_report["Country"] = "NETHERLANDS"
        final_report["Company"] = df_packs.get("manufacturer", "")
        final_report["Product Group"] = df_packs.get("product_group", df_packs.get("local_pack_description", "").str.split().str[0])
        final_report["Local Product Name"] = final_report["Product Group"]
        final_report["Generic Name"] = df_packs.get("active_substance", "")
        final_report["Indication"] = ""
        final_report["Pack Size"] = "1"  # As confirmed in requirements
        final_report["Start Date"] = df_packs.get("start_date", "")
        final_report["End Date"] = df_packs.get("end_date", "")
        final_report["Currency"] = df_packs.get("currency", "EUR")
        final_report["Unit Price"] = df_packs.get("unit_price", "")
        final_report["Pharmacy Purchase Price"] = df_packs.get("ppp_ex_vat", "")
        final_report["PPP VAT"] = df_packs.get("ppp_vat", "")
        final_report["VAT Percent"] = "9"
        final_report["Reimbursable Status"] = df_packs.get("reimbursable_status", "")
        final_report["Reimbursable Rate"] = df_packs.get("reimbursable_rate", "")
        final_report["Co-Pay Price"] = df_packs.get("copay_price", "")
        final_report["Copayment Percent"] = df_packs.get("copay_percent", "")
        final_report["Margin Rule"] = df_packs.get("margin_rule", "632 Medicijnkosten Drugs4")
        final_report["Local Pack Description"] = df_packs.get("local_pack_description", "")
        final_report["Formulation"] = df_packs.get("formulation", "")
        final_report["Strength Size"] = df_packs.get("strength_size", "")
        final_report["LOCAL_PACK_CODE"] = df_packs.get("local_pack_code", "")
        final_report["Customized Column 1"] = df_packs.get("reimbursement_message", "")

        # Save final report
        print(f"  -> Saving final report...", flush=True)
        final_report.to_csv(FINAL_REPORT_FILE, index=False, encoding="utf-8")
        
        # Also save consolidated file for backward compatibility
        df_packs.to_csv(CONSOLIDATED_FILE, index=False, encoding="utf-8")
        
        print(f"[PROGRESS] Consolidating: {len(final_report)}/{len(final_report)} (100%)", flush=True)

        print(f"\n[SUCCESS] Created final report: {FINAL_REPORT_FILE}", flush=True)
        print(f"  Total rows: {len(final_report):,}", flush=True)
        print(f"  Columns: {len(final_report.columns)}", flush=True)

    except Exception as e:
        print(f"\n[ERROR] Failed to process product details: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_with_checkpoint(
        consolidate_product_details,
        "Netherlands",
        3,
        "Consolidate Results",
        output_files=[str(CONSOLIDATED_FILE), str(FINAL_REPORT_FILE)]
    )
