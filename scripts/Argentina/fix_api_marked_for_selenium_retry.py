#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fix for cases where Selenium failures were marked as Source=api even when API steps are disabled.

This converts rows that match the "selenium timeout -> api pending" signature back to Source=selenium,
so the 3-round Selenium wrapper can retry them in later rounds / next runs.
"""

import csv
import shutil
from pathlib import Path


def _norm(v: str) -> str:
    return (v or "").strip().lower()


def fix_file(path: Path) -> int:
    if not path.exists():
        raise FileNotFoundError(str(path))

    backup = path.with_suffix(path.suffix + ".bak")
    if not backup.exists():
        shutil.copy2(path, backup)

    # Read with utf-8-sig (the pipeline writes with utf-8-sig).
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    changed = 0
    for row in rows:
        # Signature produced by the worker when it gives up and "moves to API":
        # Source=api + Scraped_By_Selenium=yes + Scraped_By_API=no + Selenium_Records=0 + API_Records=0
        if _norm(row.get("Source")) != "api":
            continue
        if _norm(row.get("Scraped_By_Selenium")) != "yes":
            continue
        if _norm(row.get("Scraped_By_API")) != "no":
            continue
        if _norm(row.get("Selenium_Records") or "0") not in ("0", ""):
            continue
        if _norm(row.get("API_Records") or "0") not in ("0", ""):
            continue

        row["Source"] = "selenium"
        row["Scraped_By_Selenium"] = "no"
        # Keep Scraped_By_API=no, and keep records as 0.
        row["Selenium_Records"] = "0"
        row["API_Records"] = "0"
        changed += 1

    if changed:
        # Ensure required columns exist.
        for col in ("Source", "Scraped_By_Selenium", "Scraped_By_API", "Selenium_Records", "API_Records"):
            if col not in fieldnames:
                fieldnames.append(col)
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return changed


if __name__ == "__main__":
    target = Path(__file__).resolve().parents[2] / "output" / "Argentina" / "Productlist_with_urls.csv"
    n = fix_file(target)
    print(f"[FIX] Updated rows: {n}")
    print(f"[FIX] File: {target}")
