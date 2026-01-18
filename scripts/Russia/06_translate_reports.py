#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Translate Russia export reports to English.

Generates separate EN copies of the VED and Excluded reports by default.
Use --in-place to overwrite the original reports with English content.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

try:
    from argostranslate import package as argos_package
    from argostranslate import translate as argos_translate
except Exception:
    argos_package = None
    argos_translate = None

try:
    from deep_translator import GoogleTranslator
except Exception:
    GoogleTranslator = None


CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")
TEXT_COLUMNS = ["Trade_Name", "INN", "Manufacturer_Country", "Release_Form"]


class Translator:
    def translate(self, text: str) -> str:
        raise NotImplementedError

    def translate_batch(self, batch: Sequence[str]) -> Optional[List[str]]:
        return [self.translate(item) for item in batch]


class ArgosTranslator(Translator):
    def translate(self, text: str) -> str:
        return argos_translate.translate(text, "ru", "en")


class GoogleTranslatorWrapper(Translator):
    def __init__(self) -> None:
        self._translator = GoogleTranslator(source="auto", target="en")

    def translate(self, text: str) -> str:
        return self._translator.translate(text)

    def translate_batch(self, batch: Sequence[str]) -> Optional[List[str]]:
        translated = self._translator.translate_batch(list(batch))
        if isinstance(translated, list) and len(translated) == len(batch):
            return translated
        return None


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _has_reports(base: Path) -> bool:
    report_names = [
        "Russia_VED_Report.csv",
        "Russia_Excluded_Report.csv",
        "russia_ved_report.csv",
        "russia_excluded_report.csv",
    ]
    return any((base / name).exists() for name in report_names)


def _get_candidate_dirs(root: Path) -> List[Path]:
    candidates = []
    try:
        from config_loader import get_central_output_dir, get_output_dir
        candidates.extend([get_central_output_dir(), get_output_dir()])
    except Exception:
        pass
    candidates.extend([root / "exports" / "Russia", root / "output" / "Russia"])
    # Deduplicate while preserving order
    unique = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return unique


def detect_input_dir(root: Path) -> Optional[Path]:
    for candidate in _get_candidate_dirs(root):
        if _has_reports(candidate):
            return candidate
    return None


def find_input_file(base: Path, candidates: Sequence[str]) -> Optional[Path]:
    for name in candidates:
        candidate = base / name
        if candidate.exists():
            return candidate
    return None


def load_cache(cache_path: Path) -> Dict[str, str]:
    if not cache_path.exists():
        return {}
    try:
        with cache_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return {str(k): str(v) for k, v in data.items()}
    except Exception:
        return {}


def save_cache(cache_path: Path, data: Dict[str, str]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = cache_path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    tmp_path.replace(cache_path)


def needs_translation(value: str, cache: Dict[str, str]) -> bool:
    return bool(value) and CYRILLIC_RE.search(value) and value not in cache


def collect_unique_values(paths: Iterable[Path], cache: Dict[str, str]) -> List[str]:
    unique = set()
    for path in paths:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for col in TEXT_COLUMNS:
                    val = (row.get(col) or "").strip()
                    if needs_translation(val, cache):
                        unique.add(val)
    return sorted(unique)


def translate_batch(translator: Translator, batch: Sequence[str], retries: int = 3) -> Optional[List[str]]:
    for attempt in range(retries):
        try:
            translated = translator.translate_batch(list(batch))
            if isinstance(translated, list) and len(translated) == len(batch):
                return translated
        except Exception:
            pass
        time.sleep(1 + attempt)
    return None


def translate_text(translator: Translator, text: str, retries: int = 3) -> str:
    for attempt in range(retries):
        try:
            translated = translator.translate(text)
            if translated:
                return translated
        except Exception:
            pass
        time.sleep(1 + attempt)
    return text


def ensure_argos_ru_en() -> bool:
    if not argos_package or not argos_translate:
        return False
    try:
        installed = argos_package.get_installed_packages()
        for pkg in installed:
            if pkg.from_code == "ru" and pkg.to_code == "en":
                return True
        available = argos_package.get_available_packages()
        ru_en = next((p for p in available if p.from_code == "ru" and p.to_code == "en"), None)
        if not ru_en:
            return False
        download_path = ru_en.download()
        argos_package.install_from_path(download_path)
        return True
    except Exception:
        return False


def get_translator() -> Translator:
    if ensure_argos_ru_en():
        print("[INFO] Using Argos Translate (offline RU->EN).", flush=True)
        return ArgosTranslator()
    if not GoogleTranslator:
        print("[ERROR] No translation engine available.", flush=True)
        sys.exit(1)
    print("[INFO] Using GoogleTranslator (online RU->EN).", flush=True)
    return GoogleTranslatorWrapper()


def translate_values(values: Sequence[str], cache: Dict[str, str], cache_path: Path) -> None:
    if not values:
        return

    translator = get_translator()
    total = len(values)
    batch_size = 50

    for start in range(0, total, batch_size):
        batch = values[start : start + batch_size]
        translated = translate_batch(translator, batch)
        if translated is None:
            translated = [translate_text(translator, item) for item in batch]

        for src, dst in zip(batch, translated):
            cache[src] = dst if dst else src

        print(
            f"[PROGRESS] Translated {min(start + batch_size, total)} / {total} unique values",
            flush=True,
        )
        save_cache(cache_path, cache)

    save_cache(cache_path, cache)


def translate_file(input_path: Path, output_path: Path, cache: Dict[str, str]) -> None:
    print(f"[INFO] Writing English report: {output_path}")
    with input_path.open("r", encoding="utf-8-sig", newline="") as f_in, output_path.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f_out:
        reader = csv.DictReader(f_in)
        fieldnames = reader.fieldnames or []
        writer = csv.DictWriter(f_out, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        row_count = 0
        for row in reader:
            for col in TEXT_COLUMNS:
                if col in row and row[col]:
                    val = row[col]
                    if CYRILLIC_RE.search(val):
                        row[col] = cache.get(val, val)
            writer.writerow(row)
            row_count += 1
            if row_count % 20000 == 0:
                print(f"[PROGRESS] {output_path.name}: {row_count} rows written", flush=True)


def translate_in_place(input_path: Path, cache: Dict[str, str]) -> None:
    tmp_path = input_path.with_suffix(input_path.suffix + ".tmp")
    translate_file(input_path, tmp_path, cache)
    tmp_path.replace(input_path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Translate Russia reports to English")
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite the original reports with English translations.",
    )
    args = parser.parse_args()

    root = repo_root()
    input_dir = detect_input_dir(root)
    if not input_dir:
        print("[ERROR] Could not locate Russia export/output reports.")
        return 1

    ved_input = find_input_file(
        input_dir, ["Russia_VED_Report.csv", "russia_ved_report.csv"]
    )
    excluded_input = find_input_file(
        input_dir, ["Russia_Excluded_Report.csv", "russia_excluded_report.csv"]
    )
    if not ved_input or not excluded_input:
        print("[ERROR] Missing VED or Excluded report input.")
        return 1

    cache_path = root / "cache" / "russia_translation_cache_en.json"
    cache = load_cache(cache_path)

    values = collect_unique_values([ved_input, excluded_input], cache)
    print(f"[INFO] Unique values to translate: {len(values)}")
    translate_values(values, cache, cache_path)

    if args.in_place:
        translate_in_place(ved_input, cache)
        translate_in_place(excluded_input, cache)
        print("[DONE] English reports written in place.")
        print(f"  - {ved_input}")
        print(f"  - {excluded_input}")
    else:
        ved_output = input_dir / "Russia_VED_Report_EN.csv"
        excluded_output = input_dir / "Russia_Excluded_Report_EN.csv"
        translate_file(ved_input, ved_output, cache)
        translate_file(excluded_input, excluded_output, cache)
        print("[DONE] English reports generated.")
        print(f"  - {ved_output}")
        print(f"  - {excluded_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
