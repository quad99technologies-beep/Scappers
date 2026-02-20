"""
Standardized PCID export categorization.

All country export scripts should use categorize_products() to split
their products into 4 standard output categories:
  - mapped:  valid PCID match
  - missing: no match found
  - oos:     matched to OOS reference
  - no_data: reference PCIDs with no scraped match
"""

import csv
import os
import logging
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class PcidExportResult:
    """Result of categorizing products against PCID reference."""
    mapped: List[Dict] = field(default_factory=list)
    missing: List[Dict] = field(default_factory=list)
    oos: List[Dict] = field(default_factory=list)
    no_data: List[Dict] = field(default_factory=list)


def categorize_products(
    products: List[Dict],
    mapper,  # PcidMapper instance (reference store already built)
    pcid_field: str = "pcid",
    enrich_from_match: Optional[Dict[str, str]] = None,
) -> PcidExportResult:
    """
    Categorize products into 4 groups using a PcidMapper.

    Args:
        products: List of product dicts (scraped data).
        mapper: PcidMapper instance with build_reference_store() already called.
        pcid_field: Name of PCID field in reference data (default "pcid").
        enrich_from_match: Optional mapping {output_key: reference_key} to copy
            fields from the matched reference row into the product dict.

    Returns:
        PcidExportResult with mapped, missing, oos, no_data lists.
    """
    mapped = []
    missing = []
    oos = []

    for product in products:
        match, category = mapper.categorize_match(product)

        if category == "mapped":
            product["PCID"] = match[pcid_field]
            if enrich_from_match:
                for out_key, ref_key in enrich_from_match.items():
                    if match.get(ref_key):
                        product[out_key] = match[ref_key]
            mapped.append(product)
        elif category == "oos":
            product["PCID"] = "OOS"
            oos.append(product)
        else:
            product["PCID"] = ""
            missing.append(product)

    no_data = mapper.get_unmatched_references()

    total = len(products)
    logger.info(
        "[PCID] Categorized %d products: mapped=%d, missing=%d, oos=%d, no_data=%d",
        total, len(mapped), len(missing), len(oos), len(no_data),
    )

    return PcidExportResult(
        mapped=mapped,
        missing=missing,
        oos=oos,
        no_data=no_data,
    )


def safe_write_csv(path: Path, rows: List[Dict], columns: List[str]) -> int:
    """Atomic CSV write using tempfile + rename. Returns row count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", dir=str(path.parent))
    try:
        with os.fdopen(tmp_fd, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        os.replace(tmp_path, str(path))
        return len(rows)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def write_standard_exports(
    result: PcidExportResult,
    exports_dir: Path,
    prefix: str,
    date_stamp: str,
    product_columns: List[str],
    no_data_columns: List[str],
    no_data_field_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Tuple[Path, int]]:
    """
    Write the 4 standard export CSVs.

    Args:
        result: PcidExportResult from categorize_products().
        exports_dir: Directory for output files.
        prefix: File name prefix (e.g. "north_macedonia", "alfabeta_Report").
        date_stamp: Date string for file names (e.g. "20022026").
        product_columns: Column names for mapped/missing/oos CSVs.
        no_data_columns: Column names for the no_data CSV.
        no_data_field_map: Optional mapping {output_col: reference_key} to
            transform reference dicts into output rows for no_data CSV.

    Returns:
        Dict of {report_type: (path, row_count)}.
    """
    exports_dir.mkdir(parents=True, exist_ok=True)
    files = {}

    # mapped
    p = exports_dir / f"{prefix}_pcid_mapped_{date_stamp}.csv"
    c = safe_write_csv(p, result.mapped, product_columns)
    files["pcid_mapped"] = (p, c)

    # missing
    p = exports_dir / f"{prefix}_pcid_missing_{date_stamp}.csv"
    c = safe_write_csv(p, result.missing, product_columns)
    files["pcid_missing"] = (p, c)

    # oos
    p = exports_dir / f"{prefix}_pcid_oos_{date_stamp}.csv"
    c = safe_write_csv(p, result.oos, product_columns)
    files["pcid_oos"] = (p, c)

    # no_data (reference rows)
    no_data_rows = result.no_data
    if no_data_field_map and result.no_data:
        no_data_rows = [
            {out_k: ref.get(ref_k, "") for out_k, ref_k in no_data_field_map.items()}
            for ref in result.no_data
        ]
    p = exports_dir / f"{prefix}_pcid_no_data_{date_stamp}.csv"
    c = safe_write_csv(p, no_data_rows, no_data_columns)
    files["pcid_no_data"] = (p, c)

    return files
