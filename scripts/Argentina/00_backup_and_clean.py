#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 0 - Backup, Clean, DB Init & run_id registration (Argentina).

What it does:
- Backs up the output folder (same behaviour as legacy script)
- Cleans output (keeps runs/backups)
- Applies PostgreSQL schema for Argentina (ar_ tables)
- Generates/persists a run_id and registers it in run_ledger
- (Optional) Seeds dictionary + PCID reference tables from input CSVs if present
"""

import csv
import os
from pathlib import Path
from typing import List, Dict
import sys

# Add repo root to path for shared imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import (
    get_output_dir,
    get_backup_dir,
    get_central_output_dir,
    load_env_file,
    get_input_dir,
    DICTIONARY_FILE,
    PCID_MAPPING_FILE,
    IGNORE_LIST_FILE,
)
from core.shared_utils import backup_output_folder, clean_output_folder
from core.db.models import generate_run_id
from core.db.connection import CountryDB
from db.schema import apply_argentina_schema
from db.repositories import ArgentinaRepository

load_env_file()

OUTPUT_DIR = get_output_dir()
BACKUP_DIR = get_backup_dir()
CENTRAL_OUTPUT_DIR = get_central_output_dir()
INPUT_DIR = get_input_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    """Best-effort CSV reader with UTF-8 BOM fallback."""
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin1"]
    for enc in encodings:
        try:
            with path.open(encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                return [dict(row) for row in reader]
        except UnicodeDecodeError:
            continue
        except Exception:
            break
    return []


def seed_dictionary(repo: ArgentinaRepository) -> int:
    dict_path = INPUT_DIR / DICTIONARY_FILE
    if not dict_path.exists():
        return 0
    rows = _read_csv_rows(dict_path)
    entries = []
    for r in rows:
        # Use first two columns as es/en
        keys = list(r.keys())
        if len(keys) < 2:
            continue
        es = r.get(keys[0], "") or ""
        en = r.get(keys[1], "") or ""
        if es.strip() and en.strip():
            entries.append({"es": es.strip(), "en": en.strip(), "source": "file"})
    if not entries:
        return 0
    replaced = repo.replace_dictionary(entries)
    try:
        repo.log_input_upload("ar_dictionary", str(dict_path), replaced, replaced_previous=1, uploaded_by="pipeline")
    except Exception:
        pass
    return replaced


def seed_pcid_reference(repo: ArgentinaRepository) -> int:
    pcid_path = INPUT_DIR / PCID_MAPPING_FILE
    if not pcid_path.exists():
        # Try common alternative filename
        alt = INPUT_DIR / "PCID Mapping - Argentina.csv"
        if alt.exists():
            pcid_path = alt
        else:
            return 0
    rows = _read_csv_rows(pcid_path)
    replaced = repo.replace_pcid_reference(rows)
    try:
        repo.log_input_upload("ar_pcid_reference", str(pcid_path), replaced, replaced_previous=1, uploaded_by="pipeline")
    except Exception:
        pass
    return replaced


def seed_ignore_list(repo: ArgentinaRepository) -> int:
    ignore_path = INPUT_DIR / IGNORE_LIST_FILE
    if not ignore_path.exists():
        return 0
    rows = _read_csv_rows(ignore_path)
    replaced = repo.replace_ignore_list(rows)
    try:
        repo.log_input_upload("ar_ignore_list", str(ignore_path), replaced, replaced_previous=1, uploaded_by="pipeline")
    except Exception:
        pass
    return replaced


def main() -> None:
    print("\n" + "=" * 80)
    print("STEP 0 - BACKUP, CLEAN, DB INIT (ARGENTINA)")
    print("=" * 80 + "\n")

    # 1) Backup
    print("[1/4] Creating backup of output folder...")
    backup_result = backup_output_folder(
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=CENTRAL_OUTPUT_DIR,
        exclude_dirs=[str(BACKUP_DIR)],
    )
    status = backup_result.get("status")
    if status == "ok":
        print(f"[OK] Backup: {backup_result['backup_folder']}")
    elif status == "skipped":
        print(f"[SKIP] {backup_result['message']}")
    else:
        print(f"[ERROR] {backup_result['message']}")
        return

    # 2) Clean
    print("\n[2/4] Cleaning output folder...")
    clean_result = clean_output_folder(
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=CENTRAL_OUTPUT_DIR,
        keep_files=[],
        keep_dirs=["runs", "backups"],
    )
    if clean_result["status"] != "ok":
        print(f"[ERROR] {clean_result.get('message')}")
        return
    print(f"[OK] Cleaned ({clean_result['files_deleted']} files removed)")

    # 3) DB init + run_id
    print("\n[3/4] Applying PostgreSQL schema and generating run_id...")
    db = CountryDB("Argentina")
    apply_argentina_schema(db)
    run_id = os.environ.get("ARGENTINA_RUN_ID") or generate_run_id()
    os.environ["ARGENTINA_RUN_ID"] = run_id
    run_id_file = OUTPUT_DIR / ".current_run_id"
    run_id_file.write_text(run_id, encoding="utf-8")
    print(f"[OK] run_id = {run_id} (saved to {run_id_file})")

    repo = ArgentinaRepository(db, run_id)
    repo.start_run(mode="fresh")
    print("[OK] run_ledger entry created")

    # 4) Seed dictionary + PCID reference + ignore list into DB
    print("\n[4/4] Seeding reference tables (dictionary, PCID, ignore list)...")
    dict_count = seed_dictionary(repo)
    pcid_count = seed_pcid_reference(repo)
    ignore_count = seed_ignore_list(repo)
    print(f"[OK] Dictionary rows: {dict_count} | PCID reference rows: {pcid_count} | Ignore rows: {ignore_count}")

    print("\n" + "=" * 80)
    print("Backup, cleanup, DB init complete. Ready for pipeline.")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    from core.standalone_checkpoint import run_with_checkpoint

    run_with_checkpoint(main, "Argentina", 0, "Backup and Clean + DB Init", output_files=None)
