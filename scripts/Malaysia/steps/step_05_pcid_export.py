#!/usr/bin/env python3
"""
Step 5: PCID Mapping & CSV Export

- Loads PCID reference CSV into DB
- SQL JOIN across all tables → pcid_mappings
- Exports mapped + not-mapped CSVs
- Generates coverage & diff reports
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_malaysia_dir = Path(__file__).resolve().parents[2]
if str(_malaysia_dir) not in sys.path:
    sys.path.insert(0, str(_malaysia_dir))

_script_dir = Path(__file__).resolve().parents[1]
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Ensure Malaysia directory is at the front of sys.path to prioritize local 'db' package
# This fixes conflict with core/db which might be in sys.path
sys.path = [p for p in sys.path if not Path(p).name == 'core']

# Re-insert Malaysia directory at the front
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

from config_loader import load_env_file, get_output_dir, get_central_output_dir, get_input_dir
load_env_file()


def _get_run_id() -> str:
    run_id = os.environ.get("MALAYSIA_RUN_ID")
    if not run_id:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            run_id = run_id_file.read_text(encoding="utf-8").strip()
    if not run_id:
        raise RuntimeError("No MALAYSIA_RUN_ID found. Run Step 0 first.")
    return run_id



def main() -> None:
    # Fix import conflict: ensure local 'db' package is prioritized over core/db
    import sys
    from pathlib import Path
    
    _step_parent = Path(__file__).resolve().parent # steps/
    malaysia_dir = str(_step_parent.parent) # scripts/Malaysia
    
    sys.path = [p for p in sys.path if not Path(p).name == 'core']
    
    if malaysia_dir in sys.path:
        sys.path.remove(malaysia_dir)
    sys.path.insert(0, malaysia_dir)

    if 'db' in sys.modules:
        del sys.modules['db']

    from core.db.connection import CountryDB
    from db.repositories import MalaysiaRepository
    from exports.csv_exporter import MalaysiaExporter

    output_dir = get_output_dir()
    exports_dir = get_central_output_dir()
    exports_dir.mkdir(parents=True, exist_ok=True)
    input_dir = get_input_dir()

    db = CountryDB("Malaysia")
    run_id = _get_run_id()
    repo = MalaysiaRepository(db, run_id)
    repo.ensure_run_in_ledger(mode="resume")  # ensure run exists before my_export_reports insert

    from config_loader import require_env, getenv_float, getenv_list

    # ── 1. Load PCID reference ──────────────────────────────────────────
    # DATA SOURCE: Load from global 'pcid_mapping' table (populated via UI Input Tab)
    print("[STEP 5] Loading PCID reference from global 'pcid_mapping'...", flush=True)
    with db.cursor() as cur:
        # Clear local reference table
        cur.execute("DELETE FROM my_pcid_reference")
        
        # Copy from global source to local reference
        # Mapping: 
        #   pcid -> pcid
        #   local_pack_code -> local_pack_code
        #   presentation -> presentation
        #   generic_name -> generic_name
        #   local_pack_description -> description
        #   (package_number, product_group not present in global table, left null)
        cur.execute("""
            INSERT INTO my_pcid_reference 
            (pcid, local_pack_code, presentation, generic_name, description)
            SELECT 
                pcid, 
                local_pack_code, 
                presentation, 
                generic_name, 
                local_pack_description
            FROM pcid_mapping 
            WHERE source_country = 'Malaysia'
        """)
        loaded_count = cur.rowcount
        print(f"  -> Loaded {loaded_count:,} rows from global source", flush=True)

    # Check if loaded (legacy check)
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM my_pcid_reference")
        pcid_count = cur.fetchone()[0]

    if pcid_count == 0:
        print(f"  [WARNING] No PCID data found for Malaysia in global 'pcid_mapping' table")
        print(f"  [WARNING] Proceeding without PCID mapping (all items will be unmapped).")
    else:
        print(f"  -> Verified {pcid_count:,} PCID reference rows in database", flush=True)
    
    print(f"[PROGRESS] Loading data: PCID mapping ready (1/2)", flush=True)

    # ── 2. Generate PCID mappings via SQL JOIN ──────────────────────────
    print("[STEP 5] Generating PCID mappings...", flush=True)
    vat = getenv_float("SCRIPT_05_DEFAULT_VAT_PERCENT", 0.0)
    product_key_columns = getenv_list("SCRIPT_05_LOOKUP_KEY_COLUMNS_PRODUCTS", ["registration_no"])
    pcid_key_columns = getenv_list("SCRIPT_05_LOOKUP_KEY_COLUMNS_PCID", ["local_pack_code"])
    mapping_count = repo.generate_pcid_mappings(
        vat_percent=vat,
        product_key_columns=product_key_columns,
        pcid_key_columns=pcid_key_columns,
    )
    print(f"  -> Generated {mapping_count:,} mappings", flush=True)
    print(f"[PROGRESS] Loading data: Mappings generated (2/2) (100%)", flush=True)

    # ── 3. Export CSVs ──────────────────────────────────────────────────
    print("[STEP 5] Exporting CSV files...", flush=True)
    final_columns = getenv_list("SCRIPT_05_FINAL_COLUMNS", [])

    exporter = MalaysiaExporter(
        repo,
        exports_dir,
        output_dir,
        final_columns,
        product_key_columns=product_key_columns,
        pcid_key_columns=pcid_key_columns,
    )
    exporter.export_all()

    # ── 4. Stats summary ────────────────────────────────────────────────
    stats = repo.get_run_stats()
    mapped = stats["pcid_mapped"]
    not_mapped = stats["pcid_not_mapped"]
    no_data = len(repo.get_pcid_reference_no_data(
        product_key_columns=product_key_columns,
        pcid_key_columns=pcid_key_columns,
    ))
    total = mapped + not_mapped
    pct = (mapped / total * 100) if total else 0

    print(f"\n[SUMMARY]", flush=True)
    print(f"   Total records: {total:,}", flush=True)
    print(f"   Mapped: {mapped:,} ({pct:.1f}%)", flush=True)
    print(f"   Not Mapped: {not_mapped:,}", flush=True)
    print(f"   PCID No Data: {no_data:,}", flush=True)
    print(f"\n[DONE] Step 5 complete", flush=True)


if __name__ == "__main__":
    from core.pipeline.standalone_checkpoint import run_with_checkpoint
    run_with_checkpoint(main, "Malaysia", 5, "Generate PCID Mapped")
