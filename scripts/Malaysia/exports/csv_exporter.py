#!/usr/bin/env python3
"""
Malaysia CSV Exporter

Exports data from the database to CSV files:
- PCID Mapped products (with PCID)
- PCID Not Mapped products (without PCID)
- Coverage/diff reports
"""

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd


class MalaysiaExporter:
    """Export Malaysia data from DB to CSV files."""

    def __init__(
        self,
        repo,  # MalaysiaRepository
        exports_dir: Path,
        output_dir: Path,
        final_columns: List[str],
        product_key_columns: Optional[List[str]] = None,
        pcid_key_columns: Optional[List[str]] = None,
    ):
        self.repo = repo
        self.exports_dir = exports_dir
        self.output_dir = output_dir
        self.final_columns = final_columns
        self.product_key_columns = product_key_columns or []
        self.pcid_key_columns = pcid_key_columns or []
        self.date_str = datetime.now().strftime("%d%m%Y")

    def export_all(self) -> Dict[str, Path]:
        """Export all CSV files and reports. Returns dict of paths."""
        paths = {}

        # Export mapped rows
        mapped_path = self._export_mapped()
        paths["mapped"] = mapped_path
        try:
            self.repo.log_export_report(
                report_type="pcid_mapped",
                file_path=str(mapped_path),
                row_count=self.repo.get_mapped_count(),
            )
        except Exception:
            pass

        # Export not mapped rows
        not_mapped_path = self._export_not_mapped()
        paths["not_mapped"] = not_mapped_path
        try:
            self.repo.log_export_report(
                report_type="pcid_not_mapped",
                file_path=str(not_mapped_path),
                row_count=self.repo.get_not_mapped_count(),
            )
        except Exception:
            pass

        # Export OOS rows
        oos_path = self._export_oos()
        paths["oos"] = oos_path
        try:
            self.repo.log_export_report(
                report_type="pcid_oos",
                file_path=str(oos_path),
                row_count=self.repo.get_oos_count(),
            )
        except Exception:
            pass

        # Export PCID rows with no matching local data
        no_data_path = self._export_pcid_no_data()
        paths["pcid_no_data"] = no_data_path
        try:
            self.repo.log_export_report(
                report_type="pcid_no_data",
                file_path=str(no_data_path),
                row_count=None,
            )
        except Exception:
            pass

        # Generate coverage report
        report_path = self._generate_coverage_report()
        paths["coverage_report"] = report_path
        try:
            self.repo.log_export_report(
                report_type="coverage_report",
                file_path=str(report_path),
                row_count=None,
            )
        except Exception:
            pass

        # Generate diff summary
        diff_path = self._generate_diff_summary(mapped_path)
        if diff_path:
            paths["diff_summary"] = diff_path
            try:
                self.repo.log_export_report(
                    report_type="diff_summary",
                    file_path=str(diff_path),
                    row_count=None,
                )
            except Exception:
                pass

        return paths

    def _export_mapped(self) -> Path:
        """Export rows WITH PCID mapping."""
        rows = self.repo.get_pcid_mapped_rows()
        df = self._rows_to_dataframe(rows)

        filename = f"malaysia_pcid_mapped_{self.date_str}.csv"
        out_path = self.exports_dir / filename

        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] Wrote {len(df):,} MAPPED rows to: {out_path}", flush=True)
        return out_path

    def _export_not_mapped(self) -> Path:
        """Export rows WITHOUT PCID mapping."""
        rows = self.repo.get_pcid_not_mapped_rows()
        df = self._rows_to_dataframe(rows)

        filename = f"malaysia_pcid_not_mapped_{self.date_str}.csv"
        out_path = self.exports_dir / filename

        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] Wrote {len(df):,} NOT MAPPED rows to: {out_path}", flush=True)
        return out_path

    def _export_oos(self) -> Path:
        """Export rows with OOS PCID."""
        rows = self.repo.get_pcid_oos_rows()
        df = self._rows_to_dataframe(rows)

        filename = f"malaysia_pcid_oos_{self.date_str}.csv"
        out_path = self.exports_dir / filename

        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] Wrote {len(df):,} OOS rows to: {out_path}", flush=True)
        return out_path

    def _export_pcid_no_data(self) -> Path:
        """Export PCID reference rows that have no matching local data."""
        rows = self.repo.get_pcid_reference_no_data(
            product_key_columns=self.product_key_columns,
            pcid_key_columns=self.pcid_key_columns,
        )

        df = pd.DataFrame(rows)
        column_mapping = {
            "pcid": "PCID Mapping",
            "local_pack_code": "LOCAL_PACK_CODE",
            "package_number": "Package Number",
            "product_group": "Product Group",
            "generic_name": "Generic Name",
            "description": "Description",
            "product_name": "Local Product Name",
            "holder": "Company",
            "search_method": "Search Method",
        }
        if not df.empty:
            df = df.rename(columns=column_mapping)

        out_columns = [
            "PCID Mapping",
            "LOCAL_PACK_CODE",
            "Package Number",
            "Product Group",
            "Local Product Name",
            "Company",
            "Search Method",
            "Generic Name",
            "Description",
        ]
        if df.empty:
            df = pd.DataFrame(columns=out_columns)
        else:
            df = df.reindex(columns=out_columns)

        filename = f"malaysia_pcid_no_data_{self.date_str}.csv"
        out_path = self.exports_dir / filename
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] Wrote {len(df):,} PCID NO DATA rows to: {out_path}", flush=True)
        return out_path

    def _rows_to_dataframe(self, rows: List[Dict]) -> pd.DataFrame:
        """Convert DB rows to DataFrame with final column order."""
        if not rows:
            return pd.DataFrame(columns=self.final_columns)

        df = pd.DataFrame(rows)

        # Map DB columns to output columns
        column_mapping = {
            "pcid": "PCID Mapping",
            "local_pack_code": "LOCAL_PACK_CODE",
            "package_number": "Package Number",
            "country": "Country",
            "company": "Company",
            "product_group": "Product Group",
            "local_product_name": "Local Product Name",
            "generic_name": "Generic Name",
            "description": "Local Pack Description",
            "pack_size": "Pack Size",
            "currency": "Currency",
            "public_without_vat_price": "Public without VAT Price",
            "public_with_vat_price": "Public with VAT Price",
            "vat_percent": "VAT Percent",
            "reimbursable_status": "Reimbursable Status",
            "region": "Region",
            "marketing_authority": "Marketing Authority",
            "source": "Source",
            "unit_price": "Unit Price",
            "strength": "Strength",
            "formulation": "Formulation",
        }

        # Rename columns
        df = df.rename(columns=column_mapping)

        # Add computed columns
        if "Reimbursable Status" in df.columns:
            df["Reimbursable Rate"] = df["Reimbursable Status"].apply(
                lambda x: "100.00%" if x == "FULLY REIMBURSABLE" else "0.00%"
            )
            df["Copayment Percent"] = df["Reimbursable Status"].apply(
                lambda x: "0.00%" if x == "FULLY REIMBURSABLE" else "100.00%"
            )

        # Ensure all final columns exist
        out = pd.DataFrame(index=df.index)
        for col in self.final_columns:
            if col in df.columns:
                out[col] = df[col]
            else:
                out[col] = np.nan

        return out

    def _generate_coverage_report(self) -> Path:
        """Generate comprehensive coverage report."""
        stats = self.repo.get_run_stats()

        report_path = self.exports_dir / "malaysia_coverage_report.txt"

        total = stats["pcid_mapped"] + stats["pcid_not_mapped"]
        pcid_coverage = (stats["pcid_mapped"] / total * 100) if total else 0

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("MALAYSIA MEDICINE PRICE SCRAPER - FINAL DATA COVERAGE REPORT\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Run ID: {self.repo.run_id}\n")
            f.write("\n")

            # Executive Summary
            f.write("=" * 80 + "\n")
            f.write("EXECUTIVE SUMMARY\n")
            f.write("=" * 80 + "\n")
            f.write(f"Total Products Processed:       {total:,}\n")
            f.write(f"Products with PCID Mapping:     {stats['pcid_mapped']:,} ({pcid_coverage:.2f}%)\n")
            f.write(f"Products without PCID Mapping:  {stats['pcid_not_mapped']:,} ({100-pcid_coverage:.2f}%)\n")
            f.write("\n")

            # Data Source Statistics
            f.write("=" * 80 + "\n")
            f.write("DATA SOURCE STATISTICS\n")
            f.write("=" * 80 + "\n")
            f.write(f"Products from MyPriMe (Step 1): {stats['products']:,}\n")
            f.write(f"Product Details (Step 2):       {stats['product_details']:,}\n")
            f.write(f"Consolidated Products (Step 3): {stats['consolidated']:,}\n")
            f.write(f"Reimbursable Drugs (Step 4):    {stats['reimbursable']:,}\n")
            f.write("\n")

            # Coverage Analysis
            f.write("=" * 80 + "\n")
            f.write("COVERAGE ANALYSIS\n")
            f.write("=" * 80 + "\n")

            # Detail coverage
            if stats['products'] > 0:
                detail_coverage = (stats['product_details'] / stats['products'] * 100)
                f.write(f"Detail Coverage:                {detail_coverage:.2f}%\n")
                f.write(f"  ({stats['product_details']:,} / {stats['products']:,} products have details)\n")

            f.write(f"PCID Mapping Coverage:          {pcid_coverage:.2f}%\n")
            f.write("\n")

            # Recommendations
            f.write("=" * 80 + "\n")
            f.write("RECOMMENDATIONS\n")
            f.write("=" * 80 + "\n")

            if stats['pcid_not_mapped'] > 0:
                f.write(f"[ACTION] {stats['pcid_not_mapped']:,} products need PCID mappings.\n")
                f.write("   Add mappings to input/PCID Mapping - Malaysia.csv\n")
                f.write("   See malaysia_pcid_not_mapped.csv for the list.\n")
                f.write("\n")

            if pcid_coverage >= 90:
                f.write("[OK] Excellent PCID mapping coverage (>90%)\n")
            elif pcid_coverage >= 70:
                f.write("[WARNING] Good coverage, but improvements possible.\n")
            else:
                f.write("[ERROR] Low PCID mapping coverage. Add more mappings.\n")

            f.write("\n")
            f.write("=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        print(f"[REPORT] Coverage report: {report_path}", flush=True)
        return report_path

    def _generate_diff_summary(self, new_mapped_path: Path) -> Optional[Path]:
        """Compare with previous export and generate diff summary."""
        # Find previous mapped file
        previous = self._find_previous_export(
            "malaysia_pcid_mapped_*.csv",
            new_mapped_path
        )

        if not previous:
            print("[DIFF] No previous export to compare.", flush=True)
            return None

        try:
            new_df = pd.read_csv(new_mapped_path, dtype=str, keep_default_na=False)
            old_df = pd.read_csv(previous, dtype=str, keep_default_na=False)
        except Exception as exc:
            print(f"[DIFF] Could not read files: {exc}", flush=True)
            return None

        key_col = "LOCAL_PACK_CODE"
        if key_col not in new_df.columns or key_col not in old_df.columns:
            print(f"[DIFF] Key column '{key_col}' missing.", flush=True)
            return None

        new_keys = self._extract_key_set(new_df, key_col)
        old_keys = self._extract_key_set(old_df, key_col)

        new_only = sorted(new_keys - old_keys)
        removed = sorted(old_keys - new_keys)
        shared = sorted(new_keys & old_keys)

        summary_path = self.exports_dir / f"report_diff_malaysia_{self.date_str}.txt"

        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write(f"Malaysia PCID-Mapped Report Diff ({self.date_str})\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"New report:      {new_mapped_path.name}\n")
            f.write(f"Compared to:     {previous.name}\n")
            f.write(f"Key column used: {key_col}\n")
            f.write("\n")
            f.write(f"New entries:     {len(new_only):,}\n")
            f.write(f"Removed entries: {len(removed):,}\n")
            f.write(f"Unchanged:       {len(shared):,}\n")
            f.write("\n")

            if new_only:
                f.write(f"Sample new keys: {', '.join(new_only[:5])}\n")
            if removed:
                f.write(f"Sample removed:  {', '.join(removed[:5])}\n")

        print(f"[DIFF] Diff summary: {summary_path}", flush=True)
        return summary_path

    def _find_previous_export(self, pattern: str, current_path: Path) -> Optional[Path]:
        """Find most recent export matching pattern, excluding current."""
        candidates = [
            p for p in self.exports_dir.glob(pattern)
            if p.is_file() and p.resolve() != current_path.resolve()
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0]

    def _extract_key_set(self, df: pd.DataFrame, column: str) -> Set[str]:
        """Extract normalized key values from column."""
        values = df[column].dropna().astype(str).str.strip()
        return {v for v in values if v}
