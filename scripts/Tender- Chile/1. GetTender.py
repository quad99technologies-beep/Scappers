"""
Script 1: Load Tender List (User Provided)
=========================================
Copies the user-provided tender list from input/Tender_Chile into the
standard output CSV location and normalizes delimiter/encoding.

INPUTS:
  - input/Tender_Chile/{SCRIPT_01_INPUT_CSV}

OUTPUTS:
  - output/Tender_Chile/{SCRIPT_01_OUTPUT_CSV}
"""

from __future__ import annotations

import csv
import shutil
from pathlib import Path

from config_loader import load_env_file, getenv, get_input_dir, get_output_dir


def detect_delimiter(file_path: Path) -> tuple[str, str]:
    delimiters = [",", ";", "\t", "|"]
    delimiter_counts = {d: 0 for d in delimiters}
    encodings = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]

    content = None
    used_encoding = None
    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding, newline="") as f:
                content = f.read(8192)
            used_encoding = encoding
            break
        except (UnicodeDecodeError, UnicodeError):
            continue

    if not content:
        return (",", "utf-8")

    lines = content.split("\n")[:5]
    for line in lines:
        if line.strip():
            for delim in delimiters:
                delimiter_counts[delim] += line.count(delim)

    detected = max(delimiter_counts.items(), key=lambda x: x[1])
    return (detected[0] if detected[1] > 0 else ",", used_encoding or "utf-8")


def normalize_csv(input_path: Path, output_path: Path) -> None:
    delimiter, encoding = detect_delimiter(input_path)
    if delimiter == ",":
        try:
            with open(input_path, "r", encoding=encoding, newline="") as infile:
                content = infile.read()
            output_path.write_text(content, encoding="utf-8-sig")
            return
        except Exception:
            pass

    with open(input_path, "r", encoding=encoding, newline="") as infile:
        reader = csv.reader(infile, delimiter=delimiter)
        with open(output_path, "w", encoding="utf-8-sig", newline="") as outfile:
            writer = csv.writer(outfile, delimiter=",", quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                writer.writerow(row)


def main() -> None:
    load_env_file()
    input_name = getenv("SCRIPT_01_INPUT_CSV", "TenderList.csv")
    output_name = getenv("SCRIPT_01_OUTPUT_CSV", "tender_list.csv")

    input_path = get_input_dir() / input_name
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_name

    if not input_path.exists():
        raise FileNotFoundError(f"Input tender list not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if input_path.suffix.lower() != ".csv":
        raise ValueError(f"Input must be CSV: {input_path.name}")

    normalize_csv(input_path, output_path)
    print(f"[OK] Tender list loaded: {output_path}")


if __name__ == "__main__":
    main()
