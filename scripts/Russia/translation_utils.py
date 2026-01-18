#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shared helpers for Russia dictionary translation and CSV handling.
"""

from __future__ import annotations

import csv
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

PREFERRED_ENCODINGS = ["utf-8-sig", "utf-8", "cp1251", "cp1252", "latin1"]
CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")


def normalize_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.replace("\u00A0", " ")
    text = unicodedata.normalize("NFC", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def is_numeric_like(text: str) -> bool:
    if not text:
        return False
    candidate = text.strip()
    if candidate.endswith("%"):
        candidate = candidate[:-1]
    candidate = re.sub(r"[A-Za-z]", "", candidate)
    candidate = candidate.replace(" ", "").replace(",", "")
    try:
        float(candidate)
        return True
    except ValueError:
        return False


def clean_price(value: str) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    cleaned = re.sub(r"[^\d,.-]", "", text).replace(",", ".")
    try:
        return f"{float(cleaned):.2f}"
    except ValueError:
        return text


def format_date_ddmmyyyy(value: str) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""

    match = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", text)
    if match:
        year, month, day = match.groups()
        return f"{day.zfill(2)}/{month.zfill(2)}/{year}"

    match = re.search(r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})", text)
    if match:
        part1, part2, year = match.groups()
        p1 = int(part1)
        p2 = int(part2)
        if p1 <= 12 and p2 > 12:
            month = part1
            day = part2
        else:
            day = part1
            month = part2
        return f"{str(day).zfill(2)}/{str(month).zfill(2)}/{year}"

    return text


def open_csv_reader(path: Path):
    last_err = None
    for enc in PREFERRED_ENCODINGS:
        try:
            handle = path.open("r", encoding=enc, newline="")
            sample = handle.read(4096)
            handle.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
            except Exception:
                dialect = csv.excel
                dialect.delimiter = ","
            reader = csv.DictReader(handle, dialect=dialect)
            if reader.fieldnames:
                return handle, reader
            handle.close()
        except Exception as exc:
            last_err = exc
            try:
                handle.close()
            except Exception:
                pass
    raise RuntimeError(f"Failed to read CSV: {path} ({last_err})")


def load_dictionary(dict_path: Path):
    if not dict_path.exists():
        raise FileNotFoundError(f"Dictionary file not found: {dict_path}")

    last_err = None
    for enc in PREFERRED_ENCODINGS:
        try:
            with dict_path.open("r", encoding=enc, newline="") as f:
                sample = f.read(4096)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
                except Exception:
                    dialect = csv.excel
                    dialect.delimiter = ","
                reader = csv.reader(f, dialect=dialect)
                first_row = next(reader, None)
                if not first_row:
                    continue
                is_header = False
                if len(first_row) >= 2:
                    head_left = normalize_text(first_row[0])
                    head_right = normalize_text(first_row[1])
                    if head_left in ("russian", "ru") and head_right in ("english", "en"):
                        is_header = True

                mapping = {}
                english_set = set()

                if not is_header:
                    rows_iter = [first_row] + list(reader)
                else:
                    rows_iter = reader

                for row in rows_iter:
                    if not row or len(row) < 2:
                        continue
                    ru_raw = str(row[0]).strip()
                    en_raw = str(row[1]).strip()
                    if ru_raw:
                        mapping[normalize_text(ru_raw)] = en_raw
                    if en_raw:
                        english_set.add(normalize_text(en_raw))

                if mapping:
                    return mapping, english_set
        except Exception as exc:
            last_err = exc
            continue

    raise RuntimeError(f"Failed to load dictionary: {dict_path} ({last_err})")


def translate_value(value: str, mapping, english_set, colname: str, miss_counter, miss_cols):
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    if is_numeric_like(raw):
        return raw

    key = normalize_text(raw)
    if key in mapping:
        return mapping[key]
    if key in english_set:
        return raw
    if not CYRILLIC_RE.search(raw):
        return raw

    miss_counter[raw] += 1
    miss_cols[raw].add(colname)
    return raw


def write_missing_report(path: Path, miss_counter, miss_cols) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for value in sorted(miss_counter, key=lambda k: (-miss_counter[k], k.lower())):
        rows.append(
            {
                "value": value,
                "count": miss_counter[value],
                "example_columns": ",".join(sorted(miss_cols[value])),
            }
        )
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["value", "count", "example_columns"])
        writer.writeheader()
        writer.writerows(rows)
