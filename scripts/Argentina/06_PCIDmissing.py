#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
from config_loader import get_output_dir, OUTPUT_REPORT_PREFIX, OUTPUT_PCID_MISSING

# Folders
OUTPUT_DIR = get_output_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Find latest alfabeta_Report_*.xlsx
candidates = sorted(OUTPUT_DIR.glob(f"{OUTPUT_REPORT_PREFIX}*.xlsx"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True)
if not candidates:
    raise FileNotFoundError(f"No {OUTPUT_REPORT_PREFIX}*.xlsx found in {OUTPUT_DIR}. Run script 04 first to generate the report.")
report_path = candidates[0]

# Load
df = pd.read_excel(report_path)

# Normalize headers (strip spaces)
df.columns = [str(c).strip() for c in df.columns]

# Resolve required columns (tolerant to small variations)
aliases = {
    "Company": ["Company", "company", "lab_name", "Lab", "Laboratorio"],
    "Local Product Name": ["Local Product Name", "product_name", "Product"],
    "Generic Name": ["Generic Name", "active_ingredient", "Generic"],
    "Local Pack Description": ["Local Pack Description", "description", "Presentation", "Presentación"],
    "PCID": ["PCID", "pcid", "PcId"],
}
def pick(colname):
    for a in aliases[colname]:
        if a in df.columns:
            return a
        al = a.lower()
        for c in df.columns:
            if c.lower() == al:
                return c
    raise ValueError(f"Missing column in report: {colname}. Have: {list(df.columns)}")

c_company = pick("Company")
c_lpn     = pick("Local Product Name")
c_gn      = pick("Generic Name")
c_lpd     = pick("Local Pack Description")
c_pcid    = pick("PCID")

# MISSING-PCID mask:
# True if the cell is NaN OR becomes empty after trimming whitespace
pcid_series = df[c_pcid]
mask_missing = pcid_series.isna() | pcid_series.astype(str).str.strip().eq("")

# Keep only rows with missing PCID; dedupe on the 4-tuple
cols_out = [c_company, c_lpn, c_gn, c_lpd, c_pcid]
missing = (df.loc[mask_missing, cols_out]
           .rename(columns={
               c_company: "Company",
               c_lpn: "Local Product Name",
               c_gn: "Generic Name",
               c_lpd: "Local Pack Description",
               c_pcid: "PCID",
           })
           .drop_duplicates()
           .sort_values(["Company","Local Product Name","Generic Name","Local Pack Description"], kind="stable")
           .reset_index(drop=True))

# Save
out_path = OUTPUT_DIR / OUTPUT_PCID_MISSING
with pd.ExcelWriter(out_path, engine="xlsxwriter") as xlw:
    missing.to_excel(xlw, index=False, sheet_name="missing_pcid")

print(f"[OK] Latest report: {report_path.name}")
print(f"[OK] Missing PCID file saved: {out_path}")
print(f"[OK] Rows without PCID: {len(missing)}")
