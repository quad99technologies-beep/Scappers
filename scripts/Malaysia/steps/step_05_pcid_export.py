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

_script_dir = Path(__file__).resolve().parents[1]
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

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

    from config_loader import require_env, getenv_float, getenv_list

    # ── 1. Load PCID reference ──────────────────────────────────────────
    print("[STEP 5] Loading PCID reference CSV...", flush=True)
    pcid_filename = require_env("SCRIPT_05_PCID_MAPPING")
    pcid_path = (input_dir / pcid_filename).resolve()

    if not pcid_path.exists():
        # Try alternative filenames
        for alt in ["PCID Mapping - Malaysia.csv", "Malaysia_PCID.csv"]:
            alt_path = input_dir / alt
            if alt_path.exists():
                pcid_path = alt_path
                print(f"  -> Using alternative: {alt_path.name}", flush=True)
                break
        else:
            raise FileNotFoundError(f"PCID mapping file not found: {pcid_path}")

    import csv as csv_mod
    pcid_rows = []
    with open(pcid_path, "r", encoding="utf-8-sig") as f:
        reader = csv_mod.DictReader(f)
        for row in reader:
            pcid_rows.append(dict(row))

    pcid_count = repo.load_pcid_reference(pcid_rows)
    print(f"  -> Loaded {pcid_count:,} PCID reference rows", flush=True)
    print(f"[PROGRESS] Loading data: PCID mapping loaded (1/2)", flush=True)

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
    from core.standalone_checkpoint import run_with_checkpoint
    run_with_checkpoint(main, "Malaysia", 5, "Generate PCID Mapped")
