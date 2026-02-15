#!/usr/bin/env python3
"""Argentina scraper data-quality guard."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config_loader import load_env_file, get_output_dir

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
    """Get a DB connection for Argentina."""
    try:
        from core.db.postgres_connection import get_db
        return get_db("Argentina")
    except Exception as exc:
        print(f"[DQ] DB connection failed: {exc}")
        return None


def _q1(db, sql: str, params: tuple = ()) -> Optional[Tuple]:
    """Execute query and return first row as tuple, or None."""
    cur = db.execute(sql, params)
    return cur.fetchone()


def _get_run_id(output_dir: Path) -> Optional[str]:
    run_id_env = os.environ.get("ARGENTINA_RUN_ID")
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
    """Check row counts for all ar_ tables."""
    results: List[CheckResult] = []
    if db is None:
        return [CheckResult("DB", "Connection", FAIL, "No database connection")]

    tables = {
        "ar_product_index": ("Product index", 100),
        "ar_products": ("Scraped products", 50),
        "ar_products_translated": ("Translated products", 0),
        "ar_dictionary": ("Dictionary entries", 100),
        "ar_errors": ("Error log", None),
        "ar_scrape_stats": ("Scrape stats", None),
    }

    for table, (label, min_rows) in tables.items():
        try:
            if run_id and table not in ("ar_dictionary",):
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


def check_null_key_fields(db, run_id: Optional[str]) -> List[CheckResult]:
    """Check for null values in critical columns."""
    results: List[CheckResult] = []
    if db is None:
        return results

    checks = [
        ("ar_product_index", "product", "Product name in index"),
        ("ar_product_index", "company", "Company in index"),
        ("ar_products", "product_name", "Product name in scraped"),
        ("ar_products", "company", "Company in scraped"),
        ("ar_products", "price_ars", "ARS price"),
    ]

    for table, column, label in checks:
        try:
            if run_id:
                row = _q1(db, f"SELECT COUNT(*) FROM {table} WHERE run_id = %s AND ({column} IS NULL OR TRIM({column}) = '')", (run_id,))
                total_row = _q1(db, f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (run_id,))
            else:
                row = _q1(db, f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL OR TRIM({column}) = ''")
                total_row = _q1(db, f"SELECT COUNT(*) FROM {table}")
            null_count = row[0] if row else 0
            total = total_row[0] if total_row else 0
            if total == 0:
                status = WARN
                detail = "No rows to check"
            else:
                pct = null_count / total * 100
                status = PASS if pct < 1 else (WARN if pct < 10 else FAIL)
                detail = f"{null_count}/{total} null/empty ({pct:.1f}%)"
            results.append(CheckResult("Nulls", label, status, detail))
        except Exception as exc:
            results.append(CheckResult("Nulls", label, WARN, f"Check failed: {exc}"))
    return results


def check_duplicates(db, run_id: Optional[str]) -> List[CheckResult]:
    """Check for duplicate records."""
    results: List[CheckResult] = []
    if db is None:
        return results

    try:
        where = "WHERE run_id = %s" if run_id else ""
        params = (run_id,) if run_id else ()
        row = _q1(db, f"SELECT COUNT(*) - COUNT(DISTINCT record_hash) FROM ar_products {where}", params)
        dups = row[0] if row else 0
        status = PASS if dups == 0 else WARN
        results.append(CheckResult("Duplicates", "Product record_hash uniqueness", status, f"{dups} duplicates"))
    except Exception as exc:
        results.append(CheckResult("Duplicates", "Product uniqueness", WARN, f"Check failed: {exc}"))

    try:
        row = _q1(db, "SELECT COUNT(*) - COUNT(DISTINCT es) FROM ar_dictionary")
        dups = row[0] if row else 0
        status = PASS if dups == 0 else WARN
        results.append(CheckResult("Duplicates", "Dictionary uniqueness", status, f"{dups} duplicate terms"))
    except Exception as exc:
        results.append(CheckResult("Duplicates", "Dictionary uniqueness", WARN, f"Check failed: {exc}"))

    return results


def check_scrape_coverage(db, run_id: Optional[str]) -> List[CheckResult]:
    """Check scrape coverage: how many products have been successfully scraped."""
    results: List[CheckResult] = []
    if db is None or not run_id:
        return results

    try:
        total_row = _q1(db, "SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s", (run_id,))
        scraped_row = _q1(db, "SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s AND status = 'scraped'", (run_id,))
        total = total_row[0] if total_row else 0
        scraped = scraped_row[0] if scraped_row else 0
        if total > 0:
            pct = scraped / total * 100
            status = PASS if pct >= 90 else (WARN if pct >= 70 else FAIL)
            detail = f"{scraped}/{total} scraped ({pct:.1f}%)"
        else:
            status = WARN
            detail = "No product index entries"
        results.append(CheckResult("Coverage", "Scrape coverage", status, detail))
    except Exception as exc:
        results.append(CheckResult("Coverage", "Scrape coverage", WARN, f"Check failed: {exc}"))

    return results


def check_translation_coverage(db, run_id: Optional[str]) -> List[CheckResult]:
    """Check how many products have been translated."""
    results: List[CheckResult] = []
    if db is None or not run_id:
        return results

    try:
        products_row = _q1(db, "SELECT COUNT(*) FROM ar_products WHERE run_id = %s", (run_id,))
        translated_row = _q1(db, "SELECT COUNT(*) FROM ar_products_translated WHERE run_id = %s", (run_id,))
        products = products_row[0] if products_row else 0
        translated = translated_row[0] if translated_row else 0
        if products > 0:
            pct = translated / products * 100
            status = PASS if pct >= 95 else (WARN if pct >= 80 else FAIL)
            detail = f"{translated}/{products} translated ({pct:.1f}%)"
        else:
            status = WARN
            detail = "No products to translate"
        results.append(CheckResult("Coverage", "Translation coverage", status, detail))
    except Exception as exc:
        results.append(CheckResult("Coverage", "Translation coverage", WARN, f"Check failed: {exc}"))

    return results


def check_export_files(output_dir: Path) -> List[CheckResult]:
    """Check that expected export files exist."""
    results: List[CheckResult] = []
    exports_dir = REPO_ROOT / "exports" / "Argentina"

    for suffix in ("pcid_mapping", "pcid_missing", "pcid_no_data", "pcid_oos"):
        pattern = f"alfabeta_Report_*_{suffix}.csv"
        matches = sorted(exports_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True) if exports_dir.exists() else []
        if matches:
            size = matches[0].stat().st_size
            status = PASS if size > 100 else WARN
            results.append(CheckResult("Exports", f"{suffix} export", status, f"{size:,} bytes ({matches[0].name})"))
        else:
            results.append(CheckResult("Exports", f"{suffix} export", WARN, "No export file found"))

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
    txt_path = reports_dir / f"argentina_data_quality_{ts}.txt"
    json_path = reports_dir / f"argentina_data_quality_{ts}.json"
    with txt_path.open("w", encoding="utf-8") as fh:
        fh.write("Argentina Data Quality Report\n")
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
    results.extend(check_null_key_fields(db, run_id))
    results.extend(check_duplicates(db, run_id))
    results.extend(check_scrape_coverage(db, run_id))
    results.extend(check_translation_coverage(db, run_id))
    results.extend(check_export_files(output_dir))

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
