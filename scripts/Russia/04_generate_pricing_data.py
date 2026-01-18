#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate the Russia Pricing Data template from russia_ved_report.csv.
Uses Dictionary.csv (Russian, English) to translate selected text columns.
Writes a missing dictionary report for untranslated Cyrillic values.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path

# Add repo root and script dir to path for config_loader
_repo_root = Path(__file__).resolve().parents[2]
_script_dir = Path(__file__).parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

try:
    from config_loader import load_env_file, get_input_dir, get_output_dir, get_central_output_dir
    load_env_file()
except Exception:

    def get_input_dir() -> Path:
        return _repo_root / "input" / "Russia"

    def get_output_dir() -> Path:
        return _repo_root / "output" / "Russia"

    def get_central_output_dir() -> Path:
        return _repo_root / "exports" / "Russia"

from translation_utils import (
    clean_price,
    format_date_ddmmyyyy,
    load_dictionary,
    open_csv_reader,
    translate_value,
    write_missing_report,
)

OUTPUT_COLUMNS = [
    "PCID",
    "Country",
    "Company",
    "Product Group",
    "Local Product Name",
    "Generic Name",
    "Indication",
    "Pack Size",
    "Start Date",
    "End Date",
    "Currency",
    "Ex-Factory Wholesale Price",
    "Pharmacy Purchase Price",
    "PPE VAT",
    "PPI VAT",
    "VAT Percent",
    "Reimbursable Status",
    "Reimbursable Rate",
    "Co-Pay Price",
    "Copayment Percent",
    "Margin Rule",
    "Local Pack Description",
    "Formulation",
    "Strength Size",
    "LOCAL_PACK_CODE",
    "Customized Column 1",
]

TRANSLATE_COLUMNS = [
    "Company",
    "Product Group",
    "Local Product Name",
    "Generic Name",
    "Indication",
    "Local Pack Description",
    "Formulation",
    "Strength Size",
]

def first_non_empty(row, keys):
    for key in keys:
        val = row.get(key)
        if val is None:
            continue
        text = str(val).strip()
        if text:
            return text
    return ""


def map_report_row(row):
    out = {col: "" for col in OUTPUT_COLUMNS}
    out["Country"] = "Russia"

    trade_name = first_non_empty(row, ["Trade_Name", "TN"])
    inn = first_non_empty(row, ["INN"])
    manufacturer = first_non_empty(row, ["Manufacturer_Country"])
    release_form = first_non_empty(row, ["Release_Form"])
    start_date = first_non_empty(row, ["Price_Start_Date", "Start_Date_Text", "Raw_Date_Text"])
    price = first_non_empty(row, ["Registered_Price_RUB"])
    ean = first_non_empty(row, ["EAN"])

    out["Company"] = manufacturer
    out["Product Group"] = trade_name
    out["Generic Name"] = inn
    out["Start Date"] = format_date_ddmmyyyy(start_date)
    out["Currency"] = "RUB" if price else ""
    out["Ex-Factory Wholesale Price"] = clean_price(price)
    out["Local Pack Description"] = release_form
    out["LOCAL_PACK_CODE"] = ean

    return out


def generate_report(input_path: Path, output_path: Path, missing_path: Path, dict_path: Path):
    mapping, english_set = load_dictionary(dict_path)

    handle, reader = open_csv_reader(input_path)
    with handle:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        miss_counter = Counter()
        miss_cols = defaultdict(set)

        with output_path.open("w", encoding="utf-8-sig", newline="") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
            writer.writeheader()

            for row in reader:
                out_row = map_report_row(row)

                for col in TRANSLATE_COLUMNS:
                    if col in out_row:
                        out_row[col] = translate_value(
                            out_row.get(col, ""),
                            mapping,
                            english_set,
                            col,
                            miss_counter,
                            miss_cols,
                        )

                writer.writerow(out_row)

        write_missing_report(missing_path, miss_counter, miss_cols)


def main() -> int:
    output_dir = get_output_dir()
    central_dir = get_central_output_dir()
    dict_dir = get_input_dir()

    default_input = output_dir / "russia_ved_report.csv"
    default_output = output_dir / "russia_pricing_data.csv"
    default_missing = output_dir / "russia_pricing_missing_dictionary.csv"
    default_central = central_dir / "Russia_Pricing_Data.csv"
    default_dict = dict_dir / "Dictionary.csv"

    parser = argparse.ArgumentParser(description="Generate Russia pricing data template")
    parser.add_argument("--input", type=Path, default=default_input, help="Input CSV path")
    parser.add_argument("--output", type=Path, default=default_output, help="Output CSV path")
    parser.add_argument("--central-output", type=Path, default=default_central, help="Central output CSV path")
    parser.add_argument("--missing-report", type=Path, default=default_missing, help="Missing dictionary report CSV path")
    parser.add_argument("--dictionary", type=Path, default=default_dict, help="Dictionary CSV path")
    parser.add_argument("--no-central", action="store_true", help="Skip writing central output copy")
    args = parser.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")

    generate_report(args.input, args.output, args.missing_report, args.dictionary)

    if not args.no_central:
        args.central_output.parent.mkdir(parents=True, exist_ok=True)
        args.central_output.write_bytes(args.output.read_bytes())

    print(f"[OK] Wrote pricing data: {args.output}")
    if not args.no_central:
        print(f"[OK] Wrote central copy: {args.central_output}")
    print(f"[OK] Wrote missing dictionary report: {args.missing_report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
