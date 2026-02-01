#!/usr/bin/env python3
'''Malaysia scraper data-quality guard.'''

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pandas.errors import EmptyDataError

from config_loader import (
    load_env_file,
    require_env,
    getenv,
    getenv_list,
    get_output_dir,
    get_central_output_dir,
)

try:
    from core.db.connection import CountryDB
    from db.repositories import MalaysiaRepository
except ImportError:  # pragma: no cover - optional when Postgres unavailable
    CountryDB = None
    MalaysiaRepository = None

PASS = 'PASS'
WARN = 'WARN'
FAIL = 'FAIL'

@dataclass
class CheckResult:
    category: str
    description: str
    status: str
    detail: str

PRICE_COLUMN_REQUIREMENTS = [
    ("Registration", ["registration", "pendaftaran"]),
    ("Generic Name", ["generic"]),
    ("Brand Name", ["brand", "dagangan"]),
    ("Pack Description", ["deskripsi", "packaging"]),
    ("Pack Unit", ["unit", "sku"]),
    ("Pack Size", ["kuantiti", "quantity", "packsize"]),
    ("Unit Price", ["unitprice", "hargaunit", "retailpriceperunit"]),
    ("Pack Price", ["packprice", "retailpriceperpack", "hargaperpek"]),
]

BULK_COUNT_COLUMNS = [
    "timestamp",
    "keyword",
    "page_rows",
    "csv_rows",
    "difference",
    "status",
    "reason",
    "csv_file",
]

PCID_COLUMN_CHECKS = ["LOCAL_PACK_CODE", "PCID Mapping"]

def normalize_column(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name or "").lower())


def find_column(columns: List[str], keywords: List[str]) -> Optional[str]:
    normalized_columns = {col: normalize_column(col) for col in columns}
    for col, normalized in normalized_columns.items():
        if any(keyword in normalized for keyword in keywords):
            return col
    return None


def analyze_numeric_column(series: pd.Series) -> Tuple[int, int, int, pd.Series, pd.Series]:
    raw = series.fillna("").astype(str)
    populated_mask = raw.str.strip().astype(bool)
    cleaned = raw.str.replace(r"[^0-9.\-]+", "", regex=True)
    numeric = pd.to_numeric(cleaned, errors="coerce")
    valid_mask = populated_mask & numeric.notna()
    populated = int(populated_mask.sum())
    valid = int(valid_mask.sum())
    invalid = int((populated_mask & ~numeric.notna()).sum())
    return populated, valid, invalid, numeric, valid_mask


def blank_rate(df: pd.DataFrame, column: str) -> Optional[float]:
    if column not in df.columns or df.empty:
        return None
    blank = df[column].astype(str).str.strip() == ""
    return float(blank.sum()) / len(df) * 100.0


def duplicate_summary(df: pd.DataFrame, subset: List[str]) -> Tuple[int, Optional[int]]:
    if df is None or df.empty or not all(col in df.columns for col in subset):
        return 0, None
    dup = df.duplicated(subset=subset)
    count = int(dup.sum())
    return count, len(df) if len(df) else None


def _get_run_id(output_dir: Path) -> Optional[str]:
    run_id_env = os.environ.get("MALAYSIA_RUN_ID")
    if run_id_env:
        return run_id_env.strip()
    run_file = output_dir / ".current_run_id"
    if run_file.exists():
        value = run_file.read_text(encoding="utf-8").strip()
        if value:
            return value
    return None


def find_latest_export(base_dir: Path, filename: str) -> Optional[Path]:
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    if not suffix:
        suffix = ""
    pattern = f"{stem}_*{suffix}"
    matches = sorted(
        (p for p in base_dir.glob(pattern) if p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return matches[0] if matches else None


def format_table(results: List[CheckResult]) -> List[str]:
    headers = ["Category", "Check", "Status", "Detail"]
    col_widths = [
        max(len(res.category) for res in results + [CheckResult("Category", "", "", "")]),
        max(len(res.description) for res in results + [CheckResult("", "Check", "", "")]),
        max(len(res.status) for res in results + [CheckResult("", "", "Status", "")]),
    ]
    lines = []
    header_line = (
        f"{'Category'.ljust(col_widths[0])} | "
        f"{'Check'.ljust(col_widths[1])} | "
        f"{'Status'.ljust(col_widths[2])} | Detail"
    )
    sep = "-" * len(header_line)
    lines.append(header_line)
    lines.append(sep)
    for res in results:
        lines.append(
            f"{res.category.ljust(col_widths[0])} | "
            f"{res.description.ljust(col_widths[1])} | "
            f"{res.status.ljust(col_widths[2])} | {res.detail}"
        )
    return lines


def persist_report(results: List[CheckResult], base_dir: Path) -> Tuple[Path, Path]:
    reports_dir = base_dir / "data_quality"
    reports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    txt_path = reports_dir / f"malaysia_data_quality_{timestamp}.txt"
    json_path = reports_dir / f"malaysia_data_quality_{timestamp}.json"
    with txt_path.open("w", encoding="utf-8") as fh:
        fh.write("Malaysia Data Quality Report\n")
        fh.write("=" * 80 + "\n")
        for line in format_table(results):
            fh.write(line + "\n")
        fh.write("\n")
        fh.write(f"Generated: {datetime.now().isoformat()}\n")
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump([res.__dict__ for res in results], fh, indent=2, ensure_ascii=False)
    return txt_path, json_path

def safe_float_env(key: str, default: float) -> float:
    try:
        return float(getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


def check_file_presence(
    configs: List[Tuple[str, str, Optional[Path], bool]],
) -> List[CheckResult]:
    results: List[CheckResult] = []
    for category, description, path, required in configs:
        if path and path.exists():
            size = path.stat().st_size
            status = PASS if size > 0 else WARN
            detail = f"{size:,} bytes"
        elif required:
            status = FAIL
            detail = "missing file"
        else:
            status = WARN
            detail = "optional file not present"
        results.append(CheckResult(category, f"{description} presence", status, detail))
    return results


def load_csv_frames(
    targets: Dict[str, Optional[Path]],
    labels: Dict[str, str],
    results: List[CheckResult],
) -> Dict[str, Optional[pd.DataFrame]]:
    frames: Dict[str, Optional[pd.DataFrame]] = {}
    for key, path in targets.items():
        label = labels.get(key, key)
        if not path or not path.exists():
            frames[key] = None
            continue
        try:
            df = pd.read_csv(
                path,
                dtype=str,
                keep_default_na=False,
                na_values=["", "NA", "N/A"],
                on_bad_lines="skip",
            )
            results.append(
                CheckResult(
                    "Read",
                    f"Load {label}",
                    PASS,
                    f"{len(df):,} rows, {len(df.columns)} columns",
                )
            )
            frames[key] = df
        except EmptyDataError:
            frames[key] = pd.DataFrame()
            results.append(
                CheckResult("Read", f"Load {label}", WARN, "File is empty")
            )
        except Exception as exc:
            frames[key] = None
            results.append(
                CheckResult("Read", f"Load {label}", FAIL, str(exc))
            )
    return frames


def check_price_dataframe(df: Optional[pd.DataFrame]) -> List[CheckResult]:
    results: List[CheckResult] = []
    label = "MyPriMe prices"
    if df is None:
        return [CheckResult("Schema", f"{label} schema", FAIL, "File not loaded")]
    if df.empty:
        return [CheckResult("Schema", f"{label} schema", WARN, "File contains no rows")]

    columns = df.columns.tolist()
    found: Dict[str, str] = {}
    missing: List[str] = []
    for requirement, keywords in PRICE_COLUMN_REQUIREMENTS:
        match = find_column(columns, keywords)
        if match:
            found[requirement] = match
        else:
            missing.append(requirement)

    status = FAIL if missing else PASS
    detail = (
        f"Missing columns: {', '.join(missing)}"
        if missing
        else f"Matched columns: {', '.join(found.values())}"
    )
    results.append(CheckResult("Schema", f"{label} schema", status, detail))

    reg_col = found.get("Registration")
    if reg_col:
        dup_count, total_rows = duplicate_summary(df, [reg_col])
        dup_status = WARN if dup_count else PASS
        dup_detail = (
            f"{dup_count} duplicates over {total_rows} rows" if total_rows else f"{dup_count} duplicates"
        )
        results.append(
            CheckResult("Duplicates", f"{label} registration uniqueness", dup_status, dup_detail)
        )
        rate = blank_rate(df, reg_col)
        if rate is not None:
            rate_status = WARN if rate > 1.0 else PASS
            results.append(
                CheckResult(
                    "Quality",
                    f"{label} registration completeness",
                    rate_status,
                    f"{rate:.1f}% empty registration numbers",
                )
            )

    unit_col = found.get("Unit Price")
    pack_col = found.get("Pack Price")
    unit_metrics = None
    pack_metrics = None

    if unit_col:
        unit_metrics = analyze_numeric_column(df[unit_col])
        populated, valid, invalid, numeric, valid_mask = unit_metrics
        invalid_ratio = (invalid / populated * 100.0) if populated else 0.0
        status_numeric = WARN if invalid_ratio > 5.0 else PASS
        results.append(
            CheckResult(
                "Numeric",
                "Unit price parsing",
                status_numeric,
                f"{invalid} of {populated} populated values not numeric ({invalid_ratio:.1f}%)",
            )
        )
        positive = int((numeric[valid_mask] > 0).sum())
        positive_ratio = (positive / valid * 100.0) if valid else 0.0
        positive_status = PASS if positive_ratio >= 80 else WARN
        results.append(
            CheckResult(
                "Numeric",
                "Unit price positivity",
                positive_status,
                f"{positive}/{valid} positive values ({positive_ratio:.1f}%)",
            )
        )

    if pack_col:
        pack_metrics = analyze_numeric_column(df[pack_col])
        p_populated, p_valid, p_invalid, p_numeric, p_mask = pack_metrics
        pack_invalid_ratio = (p_invalid / p_populated * 100.0) if p_populated else 0.0
        pack_status = WARN if pack_invalid_ratio > 5.0 else PASS
        results.append(
            CheckResult(
                "Numeric",
                "Pack price parsing",
                pack_status,
                f"{p_invalid} of {p_populated} populated values not numeric ({pack_invalid_ratio:.1f}%)",
            )
        )

    if unit_metrics and pack_metrics:
        _, unit_valid, _, unit_numeric, unit_mask = unit_metrics
        _, pack_valid, _, pack_numeric, pack_mask = pack_metrics
        overlap = unit_mask & pack_mask
        if overlap.any():
            negative = int((pack_numeric[overlap] < unit_numeric[overlap]).sum())
            status = WARN if negative else PASS
            results.append(
                CheckResult(
                    "Consistency",
                    "Pack price vs unit price",
                    status,
                    f"{negative} rows have pack price < unit price (checked {int(overlap.sum())} rows)",
                )
            )

    return results


def check_product_details(df: Optional[pd.DataFrame], required_columns: List[str]) -> List[CheckResult]:
    results: List[CheckResult] = []
    label = "Quest3+ product details"
    if df is None:
        return [CheckResult("Schema", f"{label}", FAIL, "File not loaded")]
    if df.empty:
        return [CheckResult("Schema", f"{label}", WARN, "File has no rows")]

    missing_cols = [col for col in required_columns if col not in df.columns]
    status = FAIL if missing_cols else PASS
    detail = (
        f"Missing columns: {', '.join(missing_cols)}"
        if missing_cols
        else f"Contains required columns: {', '.join(required_columns)}"
    )
    results.append(CheckResult("Schema", f"{label}", status, detail))

    if required_columns:
        reg_col = required_columns[0]
        dup_count, total_rows = duplicate_summary(df, [reg_col])
        dup_status = WARN if dup_count else PASS
        dup_detail = (
            f"{dup_count} duplicates over {total_rows} rows" if total_rows else f"{dup_count} duplicates"
        )
        results.append(
            CheckResult("Duplicates", f"{label} uniqueness", dup_status, dup_detail)
        )

    for column in ("Product Name", "Holder"):
        if column not in df.columns:
            continue
        rate = blank_rate(df, column)
        if rate is None:
            continue
        column_status = WARN if rate > 10.0 else PASS
        results.append(
            CheckResult(
                "Nulls",
                f"{label} {column} completeness",
                column_status,
                f"{rate:.1f}% of rows missing {column}",
            )
        )

    return results


def check_consolidated(df: Optional[pd.DataFrame], registration_column: str) -> List[CheckResult]:
    results: List[CheckResult] = []
    label = "Consolidated products"
    if df is None:
        return [CheckResult("Schema", label, FAIL, "File not loaded")]
    if df.empty:
        return [CheckResult("Schema", label, WARN, "File has no rows")]

    required = [registration_column, "Product Name", "Holder"]
    missing = [col for col in required if col not in df.columns]
    status = FAIL if missing else PASS
    detail = (
        f"Missing columns: {', '.join(missing)}"
        if missing
        else f"Contains consolidated columns"
    )
    results.append(CheckResult("Schema", label, status, detail))

    if registration_column in df.columns:
        dup_count, total_rows = duplicate_summary(df, [registration_column])
        dup_status = WARN if dup_count else PASS
        dup_detail = (
            f"{dup_count} duplicates over {total_rows} rows"
            if total_rows
            else f"{dup_count} duplicates"
        )
        results.append(
            CheckResult("Duplicates", f"{label} by registration", dup_status, dup_detail)
        )

    for column in ("Product Name", "Holder"):
        if column not in df.columns:
            continue
        rate = blank_rate(df, column)
        if rate is None:
            continue
        status = WARN if rate > 5.0 else PASS
        results.append(
            CheckResult(
                "Nulls",
                f"{label} {column}",
                status,
                f"{rate:.1f}% missing values",
            )
        )

    return results


def check_bulk_counts(df: Optional[pd.DataFrame]) -> List[CheckResult]:
    results: List[CheckResult] = []
    label = "Bulk search counts"
    if df is None:
        return [CheckResult("Schema", label, FAIL, "File not loaded")]
    if df.empty:
        return [CheckResult("Schema", label, WARN, "File contains no rows")]

    missing = [col for col in BULK_COUNT_COLUMNS if col not in df.columns]
    status = FAIL if missing else PASS
    detail = (
        f"Missing columns: {', '.join(missing)}"
        if missing
        else f"Contains {len(df):,} records"
    )
    results.append(CheckResult("Schema", label, status, detail))

    if not missing:
        page = pd.to_numeric(df["page_rows"], errors="coerce")
        csv_vals = pd.to_numeric(df["csv_rows"], errors="coerce")
        diff = pd.to_numeric(df["difference"], errors="coerce")
        mask = page.notna() & csv_vals.notna() & diff.notna()
        calc = page - csv_vals
        mismatch = int(((calc - diff).abs() > 0.5)[mask].sum()) if mask.any() else 0
        status = WARN if mismatch else PASS
        detail = (
            f"{mismatch} mismatched difference rows" if mismatch else "All differences consistent"
        )
        results.append(
            CheckResult("Consistency", f"{label} differences", status, detail)
        )

    return results


def check_bulk_results(df: Optional[pd.DataFrame]) -> List[CheckResult]:
    results: List[CheckResult] = []
    label = "Quest3+ bulk results"
    if df is None:
        return [CheckResult("Schema", label, FAIL, "File not loaded")]
    if df.empty:
        return [CheckResult("Schema", label, WARN, "File contains no rows")]

    reg_col = find_column(df.columns.tolist(), ["registration"])
    if not reg_col:
        return [CheckResult("Schema", label, FAIL, "Registration column not detected")]

    results.append(
        CheckResult(
            "Schema",
            label,
            PASS,
            f"Registration column '{reg_col}' present, {len(df):,} rows",
        )
    )
    return results


def check_missing_regnos(df: Optional[pd.DataFrame]) -> List[CheckResult]:
    results: List[CheckResult] = []
    label = "Missing registration numbers"
    if df is None:
        return [CheckResult("Content", label, FAIL, "File not loaded")]
    if df.empty:
        return [CheckResult("Content", label, WARN, "No rows captured")]
    first_col = df.columns[0]
    non_blank = df[first_col].astype(str).str.strip().astype(bool).sum()
    results.append(
        CheckResult(
            "Content",
            label,
            PASS,
            f"{non_blank}/{len(df):,} entries populated in '{first_col}'",
        )
    )
    return results


def check_fully_reimbursable(df: Optional[pd.DataFrame]) -> List[CheckResult]:
    results: List[CheckResult] = []
    label = "Fully reimbursable drugs"
    if df is None:
        return [CheckResult("Schema", label, FAIL, "File not loaded")]
    if df.empty:
        return [CheckResult("Schema", label, WARN, "File contains no rows")]

    generic_col = find_column(df.columns.tolist(), ["generic"])
    if not generic_col:
        return [CheckResult("Schema", label, FAIL, "Generic Name column missing")]

    rate = blank_rate(df, generic_col)
    status = WARN if (rate is not None and rate > 50.0) else PASS
    detail = f"{rate:.1f}% blank values in '{generic_col}'" if rate is not None else "All rows populated"
    results.append(CheckResult("Quality", label, status, detail))
    return results


def check_pcid_exports(
    mapped_df: Optional[pd.DataFrame],
    not_mapped_df: Optional[pd.DataFrame],
    mapped_path: Optional[Path],
    not_mapped_path: Optional[Path],
    stats: Optional[Dict[str, int]],
) -> List[CheckResult]:
    results: List[CheckResult] = []
    now = datetime.now()

    def file_checks(
        df: Optional[pd.DataFrame],
        path: Optional[Path],
        description: str,
    ) -> None:
        if path is None:
            results.append(
                CheckResult("Exports", description, WARN, "Export file not found")
            )
            return
        if df is None:
            results.append(
                CheckResult("Exports", description, FAIL, "File could not be parsed")
            )
            return

        missing_cols = [col for col in PCID_COLUMN_CHECKS if col not in df.columns]
        col_status = FAIL if missing_cols else PASS
        col_detail = (
            f"Missing columns: {', '.join(missing_cols)}"
            if missing_cols
            else f"Contains {len(df.columns)} columns"
        )
        results.append(CheckResult("Schema", description, col_status, col_detail))

        if "LOCAL_PACK_CODE" in df.columns:
            duplicates, total = duplicate_summary(df, ["LOCAL_PACK_CODE"])
            dup_status = WARN if duplicates else PASS
            dup_detail = (
                f"{duplicates} duplicates over {total} rows"
                if total
                else f"{duplicates} duplicates"
            )
            results.append(CheckResult("Duplicates", description, dup_status, dup_detail))

        if "PCID Mapping" in df.columns:
            empty_pcid = int(df["PCID Mapping"].astype(str).str.strip().eq("").sum())
            pcid_status = WARN if empty_pcid else PASS
            results.append(
                CheckResult(
                    "Quality",
                    description,
                    pcid_status,
                    f"{empty_pcid} rows without PCID values",
                )
            )

        if path.exists():
            age = now - datetime.fromtimestamp(path.stat().st_mtime)
            age_status = WARN if age > timedelta(days=1) else PASS
            results.append(
                CheckResult(
                    "Freshness",
                    description,
                    age_status,
                    f"Last modified {int(age.total_seconds() // 3600)}h ago",
                )
            )

    file_checks(mapped_df, mapped_path, "PCID mapped export")
    file_checks(not_mapped_df, not_mapped_path, "PCID not mapped export")

    mapped_count = len(mapped_df) if mapped_df is not None else 0
    not_mapped_count = len(not_mapped_df) if not_mapped_df is not None else 0
    total = mapped_count + not_mapped_count
    if total:
        coverage = mapped_count / total * 100.0
        high_threshold = safe_float_env("SCRIPT_05_PCID_COVERAGE_HIGH_THRESHOLD", 90.0)
        med_threshold = safe_float_env("SCRIPT_05_PCID_COVERAGE_MEDIUM_THRESHOLD", 70.0)
        if coverage >= high_threshold:
            coverage_status = PASS
        elif coverage >= med_threshold:
            coverage_status = WARN
        else:
            coverage_status = FAIL
        results.append(
            CheckResult(
                "Coverage",
                "Final PCID coverage",
                coverage_status,
                f"{coverage:.1f}% PCID mapping ({mapped_count}/{total})",
            )
        )
    else:
        results.append(
            CheckResult(
                "Coverage",
                "Final PCID coverage",
                WARN,
                "No PCID rows exported",
            )
        )

    if stats:
        stats_total = stats.get("pcid_mapped", 0) + stats.get("pcid_not_mapped", 0)
        discrepancy = abs(stats_total - total)
        if discrepancy:
            results.append(
                CheckResult(
                    "Consistency",
                    "PCID exports vs DB",
                    WARN,
                    f"{discrepancy} rows difference compared to run stats",
                )
            )

    return results


def check_coverage_report(path: Optional[Path]) -> List[CheckResult]:
    label = "Final coverage report"
    if path is None:
        return [CheckResult("Reporting", label, FAIL, "Report file missing")]
    if not path.exists():
        return [CheckResult("Reporting", label, FAIL, "Report file missing")]

    results: List[CheckResult] = []
    try:
        snippet = path.read_text(encoding="utf-8", errors="ignore").upper()
        contains = "DATA QUALITY METRICS" in snippet
        status = PASS if contains else WARN
        detail = "Contains data quality section" if contains else "Missing 'Data Quality Metrics' section"
        results.append(CheckResult("Reporting", label, status, detail))
    except Exception as exc:
        return [CheckResult("Reporting", label, FAIL, f"Read failure: {exc}")]

    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    age_status = WARN if age > timedelta(days=1) else PASS
    results.append(
        CheckResult(
            "Freshness",
            label,
            age_status,
            f"Modified {int(age.total_seconds() // 3600)}h ago",
        )
    )
    return results


def check_db_stats(
    repo: Optional['MalaysiaRepository'],
    run_id: Optional[str],
    stats: Optional[Dict[str, int]] = None,
) -> List[CheckResult]:
    results: List[CheckResult] = []
    if repo is None or not run_id:
        return [
            CheckResult(
                "DB",
                "Run metadata",
                WARN,
                "Run ID or database connection unavailable; skipping DB stats",
            )
        ]
    if stats is None:
        try:
            stats = repo.get_run_stats()
        except Exception as exc:
            return [
                CheckResult("DB", "Run metadata", WARN, f"Failed to read stats: {exc}")
            ]

    products = stats.get("products", 0)
    details = stats.get("product_details", 0)
    consolidated = stats.get("consolidated", 0)
    reimbursable = stats.get("reimbursable", 0)
    mapped = stats.get("pcid_mapped", 0)
    not_mapped = stats.get("pcid_not_mapped", 0)

    status = WARN if products == 0 else PASS
    detail = f"{products} products scraped"
    results.append(CheckResult("DB", "MyPriMe ingestion", status, detail))

    detail = f"{details} product details captured"
    status = WARN if details > products else PASS
    results.append(CheckResult("DB", "Quest3+ coverage", status, detail))

    status = WARN if consolidated > details else PASS
    results.append(
        CheckResult(
            "DB",
            "Consolidation",
            status,
            f"{consolidated} consolidated rows",
        )
    )

    results.append(
        CheckResult(
            "DB",
            "Reimbursable table",
            PASS if reimbursable > 0 else WARN,
            f"{reimbursable} reimbursable rows tracked",
        )
    )

    total_pcid = mapped + not_mapped
    if total_pcid:
        coverage = mapped / total_pcid * 100.0
    else:
        coverage = 0.0
    high_threshold = safe_float_env("SCRIPT_05_PCID_COVERAGE_HIGH_THRESHOLD", 90.0)
    med_threshold = safe_float_env("SCRIPT_05_PCID_COVERAGE_MEDIUM_THRESHOLD", 70.0)
    if coverage >= high_threshold:
        coverage_status = PASS
    elif coverage >= med_threshold:
        coverage_status = WARN
    else:
        coverage_status = FAIL
    results.append(
        CheckResult(
            "DB",
            "PCID mapping coverage",
            coverage_status,
            f"{coverage:.1f}% ({mapped}/{total_pcid})",
        )
    )

    return results
def main() -> None:
    load_env_file()
    output_dir = get_output_dir()
    exports_dir = get_central_output_dir()
    run_id = _get_run_id(output_dir)

    db = None
    repo = None
    stats: Optional[Dict[str, int]] = None
    db_error: Optional[str] = None
    if MalaysiaRepository and CountryDB and run_id:
        try:
            db = CountryDB("Malaysia")
            repo = MalaysiaRepository(db, run_id)
            stats = repo.get_run_stats()
        except Exception as exc:  # pragma: no cover - best-effort diagnostics
            stats = None
            db_error = str(exc)

    db_has_data = bool(stats and stats.get("products", 0) > 0)

    price_path = output_dir / require_env("SCRIPT_01_OUTPUT_CSV")
    details_path = output_dir / require_env("SCRIPT_02_OUT_FINAL")
    bulk_results_path = output_dir / require_env("SCRIPT_02_OUT_BULK")
    missing_regnos_path = output_dir / require_env("SCRIPT_02_OUT_MISSING")
    bulk_counts_path = output_dir / getenv("SCRIPT_02_OUT_COUNT_REPORT", "bulk_search_counts.csv")
    consolidated_path = output_dir / require_env("SCRIPT_03_CONSOLIDATED_FILE")
    fully_reimbursable_path = output_dir / require_env("SCRIPT_04_OUT_CSV")
    coverage_report_path = output_dir / require_env("SCRIPT_05_COVERAGE_REPORT")

    mapped_export_path = find_latest_export(exports_dir, require_env("SCRIPT_05_OUT_MAPPED"))
    not_mapped_export_path = find_latest_export(exports_dir, require_env("SCRIPT_05_OUT_NOT_MAPPED"))

    files_config = [
        ("Ingestion", "MyPriMe prices file", price_path, not db_has_data),
        ("Ingestion", "Quest3+ product details", details_path, not db_has_data),
        ("Ingestion", "Quest3+ bulk results", bulk_results_path, not db_has_data),
        ("Ingestion", "Quest3+ missing registrations", missing_regnos_path, not db_has_data),
        ("Ingestion", "Quest3+ bulk counts", bulk_counts_path, not db_has_data),
        ("Consolidation", "Consolidated products", consolidated_path, not db_has_data),
        ("Ingestion", "Fully reimbursable list", fully_reimbursable_path, not db_has_data),
        ("Reporting", "Final coverage report", coverage_report_path, True),
        ("Exports", "PCID mapped export", mapped_export_path, False),
        ("Exports", "PCID not mapped export", not_mapped_export_path, False),
    ]

    results: List[CheckResult] = []
    results.extend(check_file_presence(files_config))

    csv_targets = {
        "prices": price_path,
        "details": details_path,
        "bulk_results": bulk_results_path,
        "missing_regnos": missing_regnos_path,
        "bulk_counts": bulk_counts_path,
        "consolidated": consolidated_path,
        "fully_reimbursable": fully_reimbursable_path,
        "pcid_mapped": mapped_export_path,
        "pcid_not_mapped": not_mapped_export_path,
    }

    csv_labels = {
        "prices": "MyPriMe prices",
        "details": "Quest3+ product details",
        "bulk_results": "Quest3+ bulk results",
        "missing_regnos": "Quest3+ missing registrations",
        "bulk_counts": "Bulk search counts",
        "consolidated": "Consolidated products",
        "fully_reimbursable": "Fully reimbursable list",
        "pcid_mapped": "PCID mapped export",
        "pcid_not_mapped": "PCID not mapped export",
    }

    frames = load_csv_frames(csv_targets, csv_labels, results)

    required_columns = getenv_list(
        "SCRIPT_03_REQUIRED_COLUMNS", ["Registration No", "Product Name", "Holder"]
    )
    consolidated_column = require_env("SCRIPT_03_OUTPUT_COLUMN_REGISTRATION")

    if MalaysiaRepository and CountryDB and run_id and stats is None:
        results.append(
            CheckResult(
                "DB",
                "Database connection",
                WARN,
                f"Could not initialize MalaysiaRepository: {db_error or 'unknown error'}",
            )
        )

    results.extend(check_price_dataframe(frames["prices"]))
    results.extend(check_product_details(frames["details"], required_columns))
    results.extend(check_bulk_results(frames["bulk_results"]))
    results.extend(check_missing_regnos(frames["missing_regnos"]))
    results.extend(check_bulk_counts(frames["bulk_counts"]))
    results.extend(check_consolidated(frames["consolidated"], consolidated_column))
    results.extend(check_fully_reimbursable(frames["fully_reimbursable"]))
    results.extend(
        check_pcid_exports(
            frames["pcid_mapped"],
            frames["pcid_not_mapped"],
            mapped_export_path,
            not_mapped_export_path,
            stats,
        )
    )
    results.extend(check_coverage_report(coverage_report_path))
    results.extend(check_db_stats(repo, run_id, stats))

    if db:
        try:
            db.close()
        except Exception:
            pass

    for line in format_table(results):
        print(line)

    txt_path, json_path = persist_report(results, output_dir)
    pass_count = sum(1 for res in results if res.status == PASS)
    warn_count = sum(1 for res in results if res.status == WARN)
    fail_count = sum(1 for res in results if res.status == FAIL)

    print("\nData quality summary:")
    print(f"  PASS: {pass_count:,}")
    print(f"  WARN: {warn_count:,}")
    print(f"  FAIL: {fail_count:,}")
    print(f"Report written to: {txt_path}")
    print(f"JSON summary: {json_path}")

    sys.exit(1 if fail_count else 0)

if __name__ == '__main__':
    main()
