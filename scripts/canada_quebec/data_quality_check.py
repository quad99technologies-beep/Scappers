#!/usr/bin/env python3
"""Canada Quebec scraper data-quality guard."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pandas.errors import EmptyDataError

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config_loader import load_env_file, get_output_dir, getenv

PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"


@dataclass
class CheckResult:
    category: str
    description: str
    status: str
    detail: str


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_db():
    """Get a DB connection for CanadaQuebec."""
    try:
        from core.db.postgres_connection import get_db
        return get_db("CanadaQuebec")
    except Exception as exc:
        print(f"[DQ] DB connection failed: {exc}")
        return None


def _q1(db, sql: str, params: tuple = ()) -> Optional[Tuple]:
    """Execute query and return first row as tuple, or None."""
    cur = db.execute(sql, params)
    return cur.fetchone()


def _qall(db, sql: str, params: tuple = ()) -> List[Tuple]:
    """Execute query and return all rows as list of tuples."""
    cur = db.execute(sql, params)
    return cur.fetchall()


def _get_run_id(output_dir: Path) -> Optional[str]:
    run_id_env = os.environ.get("PIPELINE_RUN_ID")
    if run_id_env:
        return run_id_env.strip()
    run_file = output_dir / ".current_run_id"
    if run_file.exists():
        return run_file.read_text(encoding="utf-8").strip() or None
    return None


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_db_table_counts(db, run_id: Optional[str]) -> List[CheckResult]:
    """Check row counts for all cq_ tables."""
    results: List[CheckResult] = []
    if db is None:
        return [CheckResult("DB", "Connection", FAIL, "No database connection")]

    tables = {
        "cq_annexe_data": ("Annexe data entries", 100),
        "cq_step_progress": ("Step progress entries", None),
        "cq_export_reports": ("Export reports", None),
        "cq_errors": ("Error log", None),
    }

    for table, (label, min_rows) in tables.items():
        try:
            if run_id:
                row = _q1(db, f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (run_id,))
            else:
                row = _q1(db, f"SELECT COUNT(*) FROM {table}")
            count = row[0] if row else 0

            if min_rows is not None:
                status = PASS if count >= min_rows else (WARN if count > 0 else FAIL)
            else:
                status = PASS
            results.append(CheckResult("DB", f"{label} rows", status, f"{count:,} rows"))
        except Exception as exc:
            results.append(CheckResult("DB", f"{label} rows", WARN, f"Query failed: {exc}"))
    return results


def check_annexe_data_quality(db, run_id: Optional[str]) -> List[CheckResult]:
    """Check data quality within cq_annexe_data."""
    results: List[CheckResult] = []
    if db is None:
        return results

    where = "WHERE run_id = %s" if run_id else ""
    params = (run_id,) if run_id else ()

    # Check annexe type distribution
    try:
        rows = _qall(db,
            f"SELECT annexe_type, COUNT(*) FROM cq_annexe_data {where} GROUP BY annexe_type ORDER BY annexe_type",
            params,
        )
        if rows:
            detail_parts = [f"{r[0]}: {r[1]:,}" for r in rows]
            results.append(CheckResult("Distribution", "Annexe type breakdown", PASS, " | ".join(detail_parts)))
        else:
            results.append(CheckResult("Distribution", "Annexe type breakdown", WARN, "No data"))
    except Exception as exc:
        results.append(CheckResult("Distribution", "Annexe type breakdown", WARN, f"Check failed: {exc}"))

    # Check null key fields
    null_checks = [
        ("generic_name", "Generic name"),
        ("din", "DIN code"),
        ("brand", "Brand name"),
        ("manufacturer", "Manufacturer"),
        ("price", "Price"),
    ]

    for column, label in null_checks:
        try:
            if run_id:
                row = _q1(db, f"SELECT COUNT(*) FROM cq_annexe_data WHERE run_id = %s AND ({column} IS NULL OR TRIM(CAST({column} AS TEXT)) = '')", (run_id,))
                total_row = _q1(db, f"SELECT COUNT(*) FROM cq_annexe_data WHERE run_id = %s", (run_id,))
            else:
                row = _q1(db, f"SELECT COUNT(*) FROM cq_annexe_data WHERE {column} IS NULL OR TRIM(CAST({column} AS TEXT)) = ''")
                total_row = _q1(db, f"SELECT COUNT(*) FROM cq_annexe_data")
            null_count = row[0] if row else 0
            total = total_row[0] if total_row else 0
            if total == 0:
                status = WARN
                detail = "No rows to check"
            else:
                pct = null_count / total * 100
                status = PASS if pct < 5 else (WARN if pct < 20 else FAIL)
                detail = f"{null_count}/{total} null/empty ({pct:.1f}%)"
            results.append(CheckResult("Nulls", label, status, detail))
        except Exception as exc:
            results.append(CheckResult("Nulls", label, WARN, f"Check failed: {exc}"))

    # Check for duplicate DINs within same annexe_type
    try:
        row = _q1(db,
            f"SELECT COUNT(*) FROM ("
            f"  SELECT din, annexe_type FROM cq_annexe_data {where}"
            f"  GROUP BY din, annexe_type HAVING COUNT(*) > 1"
            f") sub",
            params,
        )
        dups = row[0] if row else 0
        status = PASS if dups == 0 else WARN
        results.append(CheckResult("Duplicates", "DIN uniqueness per annexe", status, f"{dups} duplicate DIN groups"))
    except Exception as exc:
        results.append(CheckResult("Duplicates", "DIN uniqueness per annexe", WARN, f"Check failed: {exc}"))

    return results


def check_pdf_split_files(output_dir: Path) -> List[CheckResult]:
    """Check that split PDF files exist."""
    results: List[CheckResult] = []
    split_dir = output_dir / "split_pdf"

    for pdf_name in ("annexe_iv1.pdf", "annexe_iv2.pdf", "annexe_v.pdf"):
        pdf_path = split_dir / pdf_name
        if pdf_path.exists():
            size = pdf_path.stat().st_size
            status = PASS if size > 1000 else WARN
            results.append(CheckResult("Files", f"Split PDF: {pdf_name}", status, f"{size:,} bytes"))
        else:
            results.append(CheckResult("Files", f"Split PDF: {pdf_name}", FAIL, "Missing"))

    # Check index.json
    index_path = split_dir / "index.json"
    if index_path.exists():
        try:
            data = json.loads(index_path.read_text(encoding="utf-8"))
            results.append(CheckResult("Files", "PDF split index.json", PASS, f"{len(data)} entries"))
        except Exception as exc:
            results.append(CheckResult("Files", "PDF split index.json", WARN, f"Parse error: {exc}"))
    else:
        results.append(CheckResult("Files", "PDF split index.json", WARN, "Missing"))

    return results


def check_csv_outputs(output_dir: Path) -> List[CheckResult]:
    """Check extracted CSV outputs."""
    results: List[CheckResult] = []
    csv_dir = output_dir / "csv"

    csv_files = {
        "annexe_iv1_extracted.csv": "Annexe IV.1 CSV",
        "annexe_iv2_extracted.csv": "Annexe IV.2 CSV",
        "annexe_v_extracted.csv": "Annexe V CSV",
    }

    for filename, label in csv_files.items():
        csv_path = csv_dir / filename
        if not csv_path.exists():
            csv_path = output_dir / filename
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, on_bad_lines="skip")
                status = PASS if len(df) > 0 else WARN
                results.append(CheckResult("CSVs", label, status, f"{len(df):,} rows, {len(df.columns)} columns"))
            except EmptyDataError:
                results.append(CheckResult("CSVs", label, WARN, "File is empty"))
            except Exception as exc:
                results.append(CheckResult("CSVs", label, FAIL, f"Read error: {exc}"))
        else:
            results.append(CheckResult("CSVs", label, WARN, "File not found"))

    return results


def check_export_files() -> List[CheckResult]:
    """Check final export files in exports/CanadaQuebec/."""
    results: List[CheckResult] = []
    exports_dir = REPO_ROOT / "exports" / "CanadaQuebec"

    if not exports_dir.exists():
        return [CheckResult("Exports", "Export directory", WARN, "exports/CanadaQuebec/ not found")]

    report_pattern = "canadaquebecreport_*.csv"
    matches = sorted(exports_dir.glob(report_pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if matches:
        latest = matches[0]
        try:
            df = pd.read_csv(latest, dtype=str, keep_default_na=False, on_bad_lines="skip")
            status = PASS if len(df) > 50 else WARN
            results.append(CheckResult("Exports", "Final report CSV", status, f"{len(df):,} rows ({latest.name})"))
        except Exception as exc:
            results.append(CheckResult("Exports", "Final report CSV", FAIL, f"Read error: {exc}"))
    else:
        results.append(CheckResult("Exports", "Final report CSV", WARN, "No report CSV found"))

    return results


# ---------------------------------------------------------------------------
# Report formatting & persistence
# ---------------------------------------------------------------------------

def format_table(results: List[CheckResult]) -> List[str]:
    headers = ["Category", "Check", "Status", "Detail"]
    col_widths = [
        max(len(r.category) for r in results) if results else len(headers[0]),
        max(len(r.description) for r in results) if results else len(headers[1]),
        max(len(r.status) for r in results) if results else len(headers[2]),
    ]
    col_widths = [max(w, len(h)) for w, h in zip(col_widths, headers)]
    lines = []
    header_line = f"{'Category'.ljust(col_widths[0])} | {'Check'.ljust(col_widths[1])} | {'Status'.ljust(col_widths[2])} | Detail"
    lines.append(header_line)
    lines.append("-" * len(header_line))
    for r in results:
        lines.append(f"{r.category.ljust(col_widths[0])} | {r.description.ljust(col_widths[1])} | {r.status.ljust(col_widths[2])} | {r.detail}")
    return lines


def persist_report(results: List[CheckResult], base_dir: Path):
    reports_dir = base_dir / "data_quality"
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    txt_path = reports_dir / f"canada_quebec_data_quality_{ts}.txt"
    json_path = reports_dir / f"canada_quebec_data_quality_{ts}.json"
    with txt_path.open("w", encoding="utf-8") as fh:
        fh.write("Canada Quebec Data Quality Report\n")
        fh.write("=" * 80 + "\n")
        for line in format_table(results):
            fh.write(line + "\n")
        fh.write(f"\nGenerated: {datetime.now().isoformat()}\n")
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump([r.__dict__ for r in results], fh, indent=2, ensure_ascii=False)
    return txt_path, json_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    load_env_file()
    output_dir = get_output_dir()
    run_id = _get_run_id(output_dir)

    db = _get_db()
    results: List[CheckResult] = []

    results.extend(check_db_table_counts(db, run_id))
    results.extend(check_annexe_data_quality(db, run_id))
    results.extend(check_pdf_split_files(output_dir))
    results.extend(check_csv_outputs(output_dir))
    results.extend(check_export_files())

    if db:
        try:
            db.close()
        except Exception:
            pass

    for line in format_table(results):
        print(line)

    txt_path, json_path = persist_report(results, output_dir)
    pass_count = sum(1 for r in results if r.status == PASS)
    warn_count = sum(1 for r in results if r.status == WARN)
    fail_count = sum(1 for r in results if r.status == FAIL)

    print(f"\nData quality summary:")
    print(f"  PASS: {pass_count:,}")
    print(f"  WARN: {warn_count:,}")
    print(f"  FAIL: {fail_count:,}")
    print(f"Report: {txt_path}")
    print(f"JSON:   {json_path}")

    sys.exit(1 if fail_count else 0)


if __name__ == "__main__":
    main()
