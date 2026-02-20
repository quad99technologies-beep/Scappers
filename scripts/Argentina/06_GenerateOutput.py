#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Final export (DB-only).

Reads:
  - ar_products_translated (current run_id)
  - pcid_mapping (shared, source_country='Argentina')

Writes:
  - alfabeta_Report_<date>_pcid_mapping.csv
  - alfabeta_Report_<date>_pcid_missing.csv
  - alfabeta_Report_<date>_pcid_oos.csv (empty placeholder)
  - alfabeta_Report_<date>_pcid_no_data.csv
"""

from __future__ import annotations

import sys
import os
from pathlib import Path

# Add project root to sys.path to allow 'core' imports
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Add script dir to sys.path to allow local imports
script_dir = Path(__file__).resolve().parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

import os
import re
import sys
import tempfile
import unicodedata
from pathlib import Path
from datetime import datetime
from typing import List, Optional

import pandas as pd

# Ensure Argentina directory is at the front of sys.path to prioritize local 'db' package
# This fixes conflict with core/db which might be in sys.path
import sys
from pathlib import Path
sys.path = [p for p in sys.path if not Path(p).name == 'core']
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

from config_loader import (
    get_output_dir,
    get_central_output_dir,
    OUTPUT_REPORT_PREFIX,
    DATE_FORMAT,
    EXCLUDE_PRICE,
)
from core.db.connection import CountryDB
from core.db.models import generate_run_id
from db.schema import apply_argentina_schema
from db.repositories import ArgentinaRepository
from core.utils.pcid_mapper import PcidMapper
from core.utils.pcid_export import categorize_products, PcidExportResult, safe_write_csv


def _atomic_replace(src: Path, dest: Path) -> None:
    """Atomically replace dest with src (same filesystem)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    os.replace(str(src), str(dest))


def _atomic_df_to_csv(df: pd.DataFrame, dest: Path, **kwargs) -> None:
    """Write DataFrame to CSV without risking a 0-byte/partial dest on crash."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=dest.name, suffix=".tmp", dir=str(dest.parent))
    tmp_path = Path(tmp_name)
    os.close(fd)
    try:
        df.to_csv(tmp_path, **kwargs)
        _atomic_replace(tmp_path, dest)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def _get_run_id(output_dir: Path) -> str:
    rid = os.environ.get("ARGENTINA_RUN_ID")
    if rid:
        return rid
    run_id_file = output_dir / ".current_run_id"
    if run_id_file.exists():
        txt = run_id_file.read_text(encoding="utf-8").strip()
        if txt:
            return txt
    rid = generate_run_id()
    os.environ["ARGENTINA_RUN_ID"] = rid
    run_id_file.write_text(rid, encoding="utf-8")
    return rid


def parse_money(x: Optional[object]) -> Optional[float]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().replace("\u00a0", "")
    if not s:
        return None
    sl = s.lower()
    if sl in {"nan", "none", "null"}:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", s):
        try:
            return float(s)
        except Exception:
            return None
    if re.fullmatch(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?", s):
        try:
            return float(s.replace(",", ""))
        except Exception:
            return None
    if re.fullmatch(r"\d{1,3}(?:\.\d{3})+,\d{2}", s):
        try:
            return float(s.replace(".", "").replace(",", "."))
        except Exception:
            return None
    if re.fullmatch(r"\d+,\d{2}", s):
        try:
            return float(s.replace(",", "."))
        except Exception:
            return None
    m = re.search(r"(\d+[.,]?\d*)", s)
    if not m:
        return None
    token = m.group(1)
    if "." in token and "," in token:
        if token.rfind(",") > token.rfind("."):
            token = token.replace(".", "").replace(",", ".")
        else:
            token = token.replace(",", "")
    else:
        if "," in token and "." not in token:
            token = token.replace(",", ".")
    try:
        return float(token)
    except Exception:
        return None


def _norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower()) if s is not None else ""


def normalize_cell(s: Optional[object]) -> Optional[str]:
    if s is None or pd.isna(s):
        return None
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = "".join(ch for ch in s if ord(ch) < 128 or ch.isspace())
    return s.strip()


def normalize_df_strings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object or str(df[col].dtype) == "string":
            df[col] = df[col].map(normalize_cell)
    return df


def get(row, *names, default=None):
    for n in names:
        if n in row:
            val = row[n]
            if val is None:
                continue
            if isinstance(val, float) and pd.isna(val):
                continue
            sval = str(val).strip()
            if not sval or sval.lower() in {"nan", "none", "null"}:
                continue
            return val
    return default


def compute_ri_fields(row):
    ioma_os = get(row, "ioma_os", "IOMA_OS")
    ioma_af = get(row, "ioma_af", "IOMA_AF")
    pami_af = get(row, "pami_af", "PAMI_AF")
    ioma_detail = get(row, "ioma_detail", "IOMA_detail", default="")
    import_stat = get(row, "import_status", default="")

    def contains(text, *needles):
        try:
            t = str(text).lower()
        except Exception:
            return False
        return any(n.lower() in t for n in needles)

    has_ioma = (ioma_os is not None) or (ioma_af is not None) or contains(ioma_detail, "ioma")
    has_pami = (not has_ioma) and (pami_af is not None)
    is_imported = contains(import_stat, "importado", "imported")

    if has_ioma:
        return "IOMA", parse_money(ioma_os), parse_money(ioma_af), "IOMA-preferred (OS->Reimb, AF->Copay)"
    if has_pami:
        return "PAMI-only", None, parse_money(pami_af), "PAMI-only-AF-as-Copay"
    if is_imported:
        return "IMPORTED", None, None, "Imported-fallback"
    return None, None, None, "No-scheme"


def run_pcid_categorization(df: pd.DataFrame, mapping_data: List[dict]) -> PcidExportResult:
    """
    Categorize products into mapped/missing/oos/no_data using PcidMapper.
    """
    df = df.copy()
    df = normalize_df_strings(df)

    env_mapping = os.environ.get("PCID_MAPPING_ARGENTINA", "")

    if env_mapping:
        mapper = PcidMapper.from_env_string(env_mapping)
        print(f"[INFO] Using PCID mapping from env: {env_mapping}")
    else:
        print("[INFO] Using default PCID mapping strategies (PCID_MAPPING_ARGENTINA not set)")
        strategies = [
            {
                "Company": "company",
                "Local Product Name": "local_product_name",
                "Generic Name": "generic_name",
                "Local Pack Description": "local_pack_description",
            }
        ]
        mapper = PcidMapper(strategies)

    mapper.build_reference_store(mapping_data)

    products = df.to_dict("records")
    return categorize_products(products, mapper)


def mapping_rows_to_output(mapping_df: pd.DataFrame, country_value: str, output_cols: List[str]) -> pd.DataFrame:
    def col_or_blank(name: str) -> pd.Series:
        if name in mapping_df.columns:
            return mapping_df[name].astype("string").fillna("")
        return pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string")

    out = pd.DataFrame(
        {
            "PCID": col_or_blank("pcid"),
            "Country": country_value,
            "Company": col_or_blank("company"),
            "Local Product Name": col_or_blank("local_product_name"),
            "Generic Name": col_or_blank("generic_name"),
            "Effective Start Date": pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string"),
            "Public With VAT Price": pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string"),
            "Reimbursement Category": pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string"),
            "Reimbursement Amount": pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string"),
            "Co-Pay Amount": pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string"),
            "Local Pack Description": col_or_blank("local_pack_description"),
        }
    )
    return normalize_df_strings(out.reindex(columns=output_cols))


def main() -> None:
    # Write directly to central exports dir (no intermediate output/ directory)
    exports_dir = get_central_output_dir()
    exports_dir.mkdir(parents=True, exist_ok=True)

    # Keep output_dir for run_id file only
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    run_id = _get_run_id(output_dir)

    db = CountryDB("Argentina")
    apply_argentina_schema(db)
    repo = ArgentinaRepository(db, run_id)
    repo.ensure_run_in_ledger(mode="resume")  # ensure run exists before ar_export_reports insert

    output_cols = [
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
    ]

    # Load PCID reference from pcid_mapping table (single source of truth)
    # User must upload new CSV via GUI to update this table
    from core.data.pcid_mapping import PCIDMapping
    pcid_mapping = PCIDMapping("Argentina", db)
    pcid_rows = pcid_mapping.get_all()
    
    mapping_data = []
    if pcid_rows:
        mapping_data = [
            {
                "pcid": m.pcid,
                "company": m.company,
                "local_product_name": m.local_product_name,
                "generic_name": m.generic_name,
                "local_pack_description": m.local_pack_description,
            }
            for m in pcid_rows
        ]
        mapping_df = pd.DataFrame(mapping_data)
    else:
        mapping_df = pd.DataFrame(
            columns=["pcid", "company", "local_product_name", "generic_name", "local_pack_description"]
        )
    print(f"[PCID] Loaded {len(mapping_df)} rows from pcid_mapping table", flush=True)

    # Load translated products (preferred)
    with db.cursor(dict_cursor=True) as cur:
        cur.execute("SELECT * FROM ar_products_translated WHERE run_id = %s", (run_id,))
        rows = cur.fetchall()

    # Fallback: if translation step was skipped/failed, use raw ar_products for this run_id
    using_raw_products = False
    if not rows:
        with db.cursor(dict_cursor=True) as cur:
            cur.execute(
                "SELECT id, run_id, company, product_name, active_ingredient, therapeutic_class, "
                "description, price_ars, date, sifar_detail, pami_af, pami_os, ioma_detail, ioma_af, "
                "ioma_os, import_status, coverage_json FROM ar_products WHERE run_id = %s ORDER BY id",
                (run_id,),
            )
            rows = cur.fetchall()
        if rows:
            using_raw_products = True
            print("[WARNING] No ar_products_translated for this run_id; using ar_products (raw, untranslated).", flush=True)
            print("[WARNING] Re-run step 5 (Translate) then step 6 for translated export.", flush=True)

    if not rows:
        print("[WARNING] No products for this run_id (ar_products_translated and ar_products both empty).", flush=True)
        print("[WARNING] Writing empty output files. Re-run from step 3 (Selenium) if you expected data.", flush=True)
        df_final = normalize_df_strings(pd.DataFrame(columns=output_cols))
    else:
        df = pd.DataFrame(rows)
        df = normalize_df_strings(df)

        # Compute RI fields
        ri = df.apply(
            lambda r: pd.Series(
                compute_ri_fields(r),
                index=["Reimbursement Category", "Reimbursement Amount", "Co-Pay Amount", "rule_label"],
            ),
            axis=1,
        )
        df = pd.concat([df, ri], axis=1)

        # Map to output columns
        df["Country"] = "ARGENTINA"
        df["Company"] = df.get("company")
        df["Local Product Name"] = df.get("product_name")
        df["Generic Name"] = df.get("active_ingredient")
        df["Effective Start Date"] = df.get("date")
        df["Local Pack Description"] = df.get("description")

        if not EXCLUDE_PRICE:
            df["Public With VAT Price"] = df.get("price_ars").apply(parse_money)
        else:
            df["Public With VAT Price"] = pd.NA

        df["Reimbursement Amount"] = df["Reimbursement Amount"].apply(parse_money)
        df["Co-Pay Amount"] = df["Co-Pay Amount"].apply(parse_money)

        for c in output_cols:
            if c not in df.columns:
                df[c] = pd.NA

        df_final = normalize_df_strings(df[output_cols].copy())

    print(f"[COUNT] translated_rows={len(df_final)}", flush=True)

    # Categorize using shared PcidMapper utility
    result = run_pcid_categorization(df_final, mapping_data)

    print(f"[COUNT] mapped={len(result.mapped)} missing={len(result.missing)} oos={len(result.oos)}", flush=True)

    today_str = datetime.now().strftime(DATE_FORMAT)
    out_prefix = f"{OUTPUT_REPORT_PREFIX}{today_str}"

    # Convert result lists to DataFrames for consistent float formatting
    def _write_result_csv(rows, path):
        if rows:
            _df = pd.DataFrame(rows)
            _df = normalize_df_strings(_df.reindex(columns=output_cols))
        else:
            _df = pd.DataFrame(columns=output_cols)
        _atomic_df_to_csv(_df, path, index=False, encoding="utf-8-sig", float_format="%.2f")
        return len(_df)

    out_mapped = exports_dir / f"{out_prefix}_pcid_mapping.csv"
    out_missing = exports_dir / f"{out_prefix}_pcid_missing.csv"
    out_oos = exports_dir / f"{out_prefix}_pcid_oos.csv"
    out_no_data = exports_dir / f"{out_prefix}_pcid_no_data.csv"

    _write_result_csv(result.mapped, out_mapped)
    _write_result_csv(result.missing, out_missing)
    _write_result_csv(result.oos, out_oos)

    # No-data: reference rows that were never matched (from mapper tracking)
    no_data_rows = [
        {
            "PCID": ref.get("pcid", ""),
            "Country": "ARGENTINA",
            "Company": ref.get("company", ""),
            "Local Product Name": ref.get("local_product_name", ""),
            "Generic Name": ref.get("generic_name", ""),
            "Effective Start Date": "",
            "Public With VAT Price": "",
            "Reimbursement Category": "",
            "Reimbursement Amount": "",
            "Co-Pay Amount": "",
            "Local Pack Description": ref.get("local_pack_description", ""),
        }
        for ref in result.no_data
    ]
    _write_result_csv(no_data_rows, out_no_data)

    # Export report tracking
    repo.log_export_report("pcid_mapping", str(out_mapped), len(result.mapped))
    repo.log_export_report("pcid_missing", str(out_missing), len(result.missing))
    repo.log_export_report("pcid_oos", str(out_oos), len(result.oos))
    repo.log_export_report("pcid_no_data", str(out_no_data), len(no_data_rows))

    # Finish run in run_ledger
    try:
        stats = repo.get_stats()
        repo.finish_run(
            status="completed",
            items_scraped=stats.get("products", 0),
            items_exported=len(df_final),
        )
    except Exception:
        pass

    print(f"[OK] Wrote: {out_mapped}")
    print(f"[OK] Wrote: {out_missing}")
    print(f"[OK] Wrote: {out_oos}")
    print(f"[OK] Wrote: {out_no_data}")
    print(f"[OK] All exports written to: {exports_dir}")


if __name__ == "__main__":
    main()
