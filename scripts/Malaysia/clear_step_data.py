#!/usr/bin/env python3
"""
Utility to clear Malaysia pipeline data by step/run_id.

Use cases:
- Re-run a step from scratch: clear that step (optionally downstream) first.
- Keep progress: skip clearing and just resume the step.
"""

import argparse
import os
import sys
from pathlib import Path

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Add scripts/Malaysia to path
SCRIPT_DIR = Path(__file__).parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from config_loader import load_env_file, get_output_dir

load_env_file()


def resolve_run_id(cli_run_id: str = None) -> str:
    """Resolve run_id from CLI, env var, or .current_run_id."""
    if cli_run_id:
        return cli_run_id
    run_id = os.environ.get("MALAYSIA_RUN_ID")
    if not run_id:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            run_id = run_id_file.read_text(encoding="utf-8").strip()
    if not run_id:
        raise RuntimeError("No run_id found. Pass --run-id or ensure .current_run_id exists.")
    return run_id


def main():
    parser = argparse.ArgumentParser(description="Clear Malaysia pipeline data for a given step.")
    parser.add_argument("--step", type=int, required=True, choices=[1, 2, 3, 4, 5],
                        help="Step number to clear (1=products, 2=product_details, 3=consolidated, 4=reimbursable, 5=pcid_mappings)")
    parser.add_argument("--downstream", action="store_true",
                        help="Also clear all downstream steps/tables.")
    parser.add_argument("--run-id", dest="run_id", help="Run ID to operate on (default: env or .current_run_id)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without executing.")
    args = parser.parse_args()

    from core.db.connection import CountryDB
    from db.repositories import MalaysiaRepository

    run_id = resolve_run_id(args.run_id)
    db = CountryDB("Malaysia")
    repo = MalaysiaRepository(db, run_id)

    table_map = repo._STEP_TABLE_MAP  # static map of step -> tables
    steps = [s for s in sorted(table_map) if s == args.step or (args.downstream and s >= args.step)]
    tables = [repo._table(t) for s in steps for t in table_map[s]]

    print(f"Run ID: {run_id}")
    print(f"Step(s) to clear: {steps}")
    print(f"Tables to clear: {', '.join(tables)}")

    if args.dry_run:
        print("Dry run: no data deleted.")
        return

    deleted = repo.clear_step_data(args.step, include_downstream=args.downstream)
    for tbl, count in deleted.items():
        print(f"Cleared {count} rows from {tbl}")
    print("Done.")


if __name__ == "__main__":
    main()
