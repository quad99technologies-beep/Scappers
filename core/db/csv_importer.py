#!/usr/bin/env python3
"""
CSV-to-database importer for input tables and PCID mappings.

Supports:
- Import any CSV into a PostgreSQL table (with column mapping)
- Replace or append modes
- Auto-detect CSV encoding and delimiter
- Validation before import
- Upload tracking via input_uploads table

Usage:
    from core.db.csv_importer import CSVImporter

    importer = CSVImporter(db)
    result = importer.import_csv(
        csv_path="input/India/formulations_part1.csv",
        table="input_formulations",
        column_map={"Generic Name": "generic_name"},
        mode="replace",
    )
    print(result)  # ImportResult(rows=540, status="ok", ...)
"""

import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from psycopg2 import IntegrityError as PgIntegrityError
except ImportError:
    PgIntegrityError = type(None)  # Dummy type that won't match

logger = logging.getLogger(__name__)

# Common CSV encodings to try in order
_ENCODINGS = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]

# Country → list of input table configs
# Each config: (table_name, display_name, expected_columns_hint)
INPUT_TABLE_REGISTRY: Dict[str, List[dict]] = {
    "India": [
        {
            "table": "input_formulations",
            "display": "Formulations",
            "column_map": {"Generic Name": "generic_name", "formulation": "generic_name", "name": "generic_name"},
            "required": ["generic_name"],
        },
    ],
    "Argentina": [
        {
            "table": "ignore_list",
            "display": "Ignore List",
            "column_map": {
                "Company": "company",
                "Product": "product",
                "company": "company",
                "product": "product",
            },
            "required": ["company", "product"],
        },
        {
            "table": "dictionary",
            "display": "Dictionary",
            "column_map": {
                "Spanish": "es",
                "English": "en",
                "es": "es",
                "en": "en",
                "source": "es",
                "target": "en",
                "source_term": "es",
                "translated_term": "en",
            },
            "required": ["es"],
        },
    ],
    "Malaysia": [
        {
            "table": "input_products",
            "display": "Products",
            "column_map": {
                "Product Name": "product_name",
                "Product Type": "product_name",
                "Registration No": "registration_no",
                "registration_number": "registration_no",
                "Registration No / Notification No": "registration_no",
            },
            "required": ["product_name"],
        },
    ],
    "Belarus": [
        {
            "table": "input_generic_names",
            "display": "Generic Names",
            "column_map": {"Generic Name": "generic_name", "name": "generic_name", "INN": "generic_name"},
            "required": ["generic_name"],
        },
        {
            "table": "input_dictionary",
            "display": "Dictionary (RU→EN)",
            "column_map": {
                "Russian": "source_term",
                "English": "translated_term",
                "RU": "source_term",
                "EN": "translated_term",
                "source": "source_term",
                "target": "translated_term",
                "source_term": "source_term",
                "translated_term": "translated_term",
            },
            "required": ["source_term"],
        },
    ],
    "Taiwan": [
        {
            "table": "input_atc_prefixes",
            "display": "ATC Prefixes",
            "column_map": {"ATC Code": "atc_code", "atc_code": "atc_code", "Description": "description"},
            "required": ["atc_code"],
        },
    ],
    "Tender_Chile": [
        {
            "table": "input_tender_list",
            "display": "Tender List",
            "column_map": {
                "CN Document Number": "tender_id",
                "CN": "tender_id",
                "TenderID": "tender_id",
                "Tender ID": "tender_id",
                "tender_id": "tender_id",
                "Description": "description",
                "description": "description",
                "URL": "url",
                "url": "url",
                "Url": "url",
            },
            "required": ["tender_id"],
        },
    ],
    "Russia": [
        {
            "table": "input_dictionary",
            "display": "Dictionary (RU→EN)",
            "column_map": {
                "Russian": "source_term",
                "English": "translated_term",
                "RU": "source_term",
                "EN": "translated_term",
                "source": "source_term",
                "target": "translated_term"
            },
            "required": ["source_term"],
        },
    ],
    "NorthMacedonia": [
        {
            "table": "input_dictionary",
            "display": "Dictionary (MK→EN)",
            "column_map": {
                "Macedonian": "source_term",
                "English": "translated_term",
                "MK": "source_term",
                "EN": "translated_term",
                "source": "source_term",
                "target": "translated_term",
                "source_term": "source_term",
                "translated_term": "translated_term",
            },
            "required": ["source_term"],
        },
    ],
    "North Macedonia": [
        {
            "table": "input_dictionary",
            "display": "Dictionary (MK→EN)",
            "column_map": {
                "Macedonian": "source_term",
                "English": "translated_term",
                "MK": "source_term",
                "EN": "translated_term",
                "source": "source_term",
                "target": "translated_term",
                "source_term": "source_term",
                "translated_term": "translated_term",
            },
            "required": ["source_term"],
        },
    ],
}

# PCID mapping config (shared across countries)
PCID_MAPPING_CONFIG = {
    "table": "pcid_mapping",
    "display": "PCID Mapping",
    "column_map": {
        # Argentina style
        "PCID": "pcid",
        "Company": "company",
        "Local Product Name": "local_product_name",
        "Generic Name": "generic_name",
        "Local Pack Description": "local_pack_description",
        # Malaysia style
        "LOCAL_PACK_CODE": "local_pack_code",
        "PCID Mapping": "pcid",
        "Presentation": "presentation",
        "PACK_SIZE": "presentation",
        "Pack Size": "presentation",
        # Belarus/others
        "INN": "generic_name",
        "Trade Name": "local_product_name",
        "Dosage Form": "local_pack_description",
    },
    "required": [],  # Flexible — different countries use different columns
}


@dataclass
class ImportResult:
    status: str  # "ok" | "error" | "warning"
    rows_imported: int = 0
    rows_skipped: int = 0
    message: str = ""
    table: str = ""
    source_file: str = ""
    columns_mapped: List[str] = field(default_factory=list)
    columns_unmapped: List[str] = field(default_factory=list)


class CSVImporter:
    """Import CSV files into PostgreSQL input tables."""

    def __init__(self, db):
        """
        Initialize CSV importer.

        Args:
            db: PostgresDB instance
        """
        self.db = db

    def validate_csv(
        self, csv_path: Path, column_map: Dict[str, str], required: List[str]
    ) -> Dict[str, Any]:
        """
        Validate a CSV against the expected schema before import.

        Returns:
            {
                "valid": True/False,
                "errors": [...],
                "warnings": [...],
                "csv_columns": [...],
                "mapped": {"csv_col": "db_col", ...},
                "unmapped": [...],
                "missing_required": [...],
            }
        """
        result: Dict[str, Any] = {
            "valid": True, "errors": [], "warnings": [],
            "csv_columns": [], "mapped": {}, "unmapped": [], "missing_required": [],
        }

        if not csv_path.exists():
            result["valid"] = False
            result["errors"].append(f"File not found: {csv_path}")
            return result

        try:
            encoding = self._detect_encoding(csv_path)
            delimiter = self._detect_delimiter(csv_path, encoding)
            with csv_path.open("r", encoding=encoding, newline="") as fh:
                reader = csv.DictReader(fh, delimiter=delimiter)
                csv_columns = [c.strip() for c in (reader.fieldnames or [])]
        except Exception as exc:
            result["valid"] = False
            result["errors"].append(f"Cannot read CSV: {exc}")
            return result

        result["csv_columns"] = csv_columns

        # Map columns
        for col in csv_columns:
            if col in column_map:
                result["mapped"][col] = column_map[col]
            else:
                result["unmapped"].append(col)

        if not result["mapped"]:
            result["valid"] = False
            result["errors"].append(
                f"No CSV columns match the expected mapping. CSV has: {csv_columns}"
            )
            return result

        # Check required DB columns are covered
        mapped_db_cols = set(result["mapped"].values())
        for req in required:
            if req not in mapped_db_cols:
                result["missing_required"].append(req)

        if result["missing_required"]:
            result["valid"] = False
            result["errors"].append(
                f"Missing required columns: {result['missing_required']}"
            )

        if result["unmapped"]:
            result["warnings"].append(
                f"Unmapped columns (will be ignored): {result['unmapped']}"
            )

        return result

    def get_schema_info(self, country: str) -> List[Dict[str, Any]]:
        """Get schema info for all input tables of a country."""
        configs = INPUT_TABLE_REGISTRY.get(country, [])
        info = []
        for cfg in configs:
            info.append({
                "table": cfg["table"],
                "display": cfg["display"],
                "expected_csv_columns": list(cfg["column_map"].keys()),
                "db_columns": list(set(cfg["column_map"].values())),
                "required": cfg.get("required", []),
            })
        # Add PCID mapping
        info.append({
            "table": PCID_MAPPING_CONFIG["table"],
            "display": PCID_MAPPING_CONFIG["display"],
            "expected_csv_columns": list(PCID_MAPPING_CONFIG["column_map"].keys()),
            "db_columns": list(set(PCID_MAPPING_CONFIG["column_map"].values())),
            "required": PCID_MAPPING_CONFIG.get("required", []),
        })
        return info

    def export_table_csv(self, table: str, output_path: Path, country: str = "") -> str:
        """Export a table's current data to CSV. Returns message."""
        actual_table = self.db.table_name(table) if hasattr(self.db, "table_name") else table
        conn = self.db.connect()
        try:
            cur = conn.cursor()
            if table == "pcid_mapping" and country:
                cur.execute(f"SELECT * FROM {actual_table} WHERE source_country = %s", (country,))
            else:
                cur.execute(f"SELECT * FROM {actual_table}")
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        except Exception as exc:
            return f"Error reading table: {exc}"

        if not rows:
            return f"Table '{table}' is empty — nothing to export."

        with output_path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.writer(fh)
            writer.writerow(columns)
            writer.writerows(rows)

        return f"Exported {len(rows)} rows to {output_path.name}"

    def preview_csv(self, csv_path: Path, max_rows: int = 10) -> Dict[str, Any]:
        """
        Preview a CSV file: detect encoding, columns, sample rows.

        Returns:
            {
                "encoding": "utf-8-sig",
                "delimiter": ",",
                "columns": ["Col1", "Col2"],
                "row_count": 540,
                "sample_rows": [{...}, ...],
            }
        """
        encoding = self._detect_encoding(csv_path)
        delimiter = self._detect_delimiter(csv_path, encoding)

        with csv_path.open("r", encoding=encoding, newline="") as fh:
            reader = csv.DictReader(fh, delimiter=delimiter)
            columns = list(reader.fieldnames or [])
            sample = []
            total = 0
            for row in reader:
                total += 1
                if len(sample) < max_rows:
                    sample.append(dict(row))

        return {
            "encoding": encoding,
            "delimiter": delimiter,
            "columns": columns,
            "row_count": total,
            "sample_rows": sample,
        }

    def import_csv(
        self,
        csv_path: Path,
        table: str,
        column_map: Dict[str, str],
        mode: str = "replace",
        country: str = "",
    ) -> ImportResult:
        """
        Import a CSV file into a PostgreSQL table.

        Args:
            csv_path: Path to CSV file.
            table: Target table name.
            column_map: {csv_column: db_column} mapping.
            mode: "replace" (DELETE + INSERT) or "append" (INSERT only).
            country: Country name (for pcid_mapping.source_country).

        Returns:
            ImportResult with status and stats.
        """
        if not csv_path.exists():
            return ImportResult(status="error", message=f"File not found: {csv_path}", table=table)

        # Try multiple encodings with full file read to avoid mid-file encoding failures
        rows = []
        columns_unmapped = []
        encoding_used = None
        last_error = None

        for encoding in _ENCODINGS:
            try:
                delimiter = self._detect_delimiter(csv_path, encoding)
                with csv_path.open("r", encoding=encoding, newline="", errors="strict") as fh:
                    reader = csv.DictReader(fh, delimiter=delimiter)
                    csv_columns = list(reader.fieldnames or [])

                    # Build active column mapping (only columns present in CSV)
                    active_map = {}
                    columns_unmapped = []
                    for csv_col in csv_columns:
                        csv_col_stripped = csv_col.strip()
                        if csv_col_stripped in column_map:
                            active_map[csv_col_stripped] = column_map[csv_col_stripped]
                        else:
                            columns_unmapped.append(csv_col_stripped)

                    if not active_map:
                        return ImportResult(
                            status="error",
                            message=f"No columns matched. CSV has: {csv_columns}",
                            table=table,
                            columns_unmapped=columns_unmapped,
                        )

                    rows = []
                    for row in reader:
                        mapped_row = {}
                        for csv_col, db_col in active_map.items():
                            value = (row.get(csv_col) or "").strip()
                            if value:
                                mapped_row[db_col] = value
                        if mapped_row:
                            if country and table == "pcid_mapping":
                                mapped_row["source_country"] = country
                            rows.append(mapped_row)

                    encoding_used = encoding
                    break  # Success - exit encoding loop

            except (UnicodeDecodeError, UnicodeError) as exc:
                last_error = exc
                continue  # Try next encoding
            except Exception as exc:
                last_error = exc
                continue  # Try next encoding

        if encoding_used is None:
            return ImportResult(status="error", message=f"CSV read error (tried all encodings): {last_error}", table=table)

        if not rows:
            return ImportResult(status="warning", message="No valid rows found in CSV", table=table)

        # Resolve actual table name (PostgresDB uses country prefix for input tables)
        actual_table = self.db.table_name(table) if hasattr(self.db, "table_name") else table

        # Write to DB
        conn = self.db.connect()

        try:
            cur = conn.cursor()
            if mode == "replace":
                if table == "pcid_mapping" and country:
                    cur.execute(f"DELETE FROM {actual_table} WHERE source_country = %s", (country,))
                else:
                    cur.execute(f"DELETE FROM {actual_table}")
                conn.commit()  # Commit the delete before inserting

            # Determine columns from first row
            db_columns = list(rows[0].keys())
            placeholders = ", ".join(["%s"] * len(db_columns))
            col_str = ", ".join(db_columns)

            # Use ON CONFLICT DO NOTHING to skip duplicates without rolling back
            # This avoids the PostgreSQL issue where rollback after error loses all previous inserts
            sql = f"INSERT INTO {actual_table} ({col_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

            skipped = 0
            inserted = 0
            for row in rows:
                values = tuple(row.get(c, "") for c in db_columns)
                cur.execute(sql, values)
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1

            conn.commit()

            # Log upload (store actual table name for consistency)
            cur.execute(
                "INSERT INTO input_uploads (table_name, source_file, row_count, replaced_previous, source_country) "
                "VALUES (%s, %s, %s, %s, %s)",
                (
                    actual_table,
                    csv_path.name,
                    inserted,
                    1 if mode == "replace" else 0,
                    country if table == "pcid_mapping" and country else None,
                ),
            )
            conn.commit()

        except Exception as exc:
            conn.rollback()
            return ImportResult(status="error", message=f"DB write error: {exc}", table=table)

        return ImportResult(
            status="ok",
            rows_imported=inserted,
            rows_skipped=skipped,
            message=f"Imported {inserted} rows into {table}",
            table=table,
            source_file=csv_path.name,
            columns_mapped=list(active_map.values()),
            columns_unmapped=columns_unmapped,
        )

    def get_table_info(self, table: str, country: str = "") -> Dict[str, Any]:
        """Get row count and last upload info for a table."""
        actual_table = self.db.table_name(table) if hasattr(self.db, "table_name") else table
        conn = self.db.connect()

        try:
            cur = conn.cursor()
            if table == "pcid_mapping" and country:
                cur.execute(f"SELECT COUNT(*) FROM {actual_table} WHERE source_country = %s", (country,))
            else:
                cur.execute(f"SELECT COUNT(*) FROM {actual_table}")
            count = cur.fetchone()[0]
        except Exception:
            count = 0

        try:
            cur = conn.cursor()
            if table == "pcid_mapping" and country:
                cur.execute(
                    "SELECT source_file, row_count, uploaded_at FROM input_uploads "
                    "WHERE table_name = %s AND source_country = %s "
                    "ORDER BY uploaded_at DESC LIMIT 1",
                    (actual_table, country),
                )
            else:
                cur.execute(
                    "SELECT source_file, row_count, uploaded_at FROM input_uploads "
                    "WHERE table_name = %s ORDER BY uploaded_at DESC LIMIT 1",
                    (actual_table,),
                )
            upload = cur.fetchone()
            last_upload = {
                "source_file": upload[0],
                "row_count": upload[1],
                "uploaded_at": upload[2],
            } if upload else None
        except Exception:
            last_upload = None

        return {"table": table, "row_count": count, "last_upload": last_upload}

    def get_table_rows(self, table: str, limit: int = 100, country: str = "") -> List[Dict]:
        """Fetch rows from a table for preview."""
        actual_table = self.db.table_name(table) if hasattr(self.db, "table_name") else table
        conn = self.db.connect()

        try:
            cur = conn.cursor()
            if table == "pcid_mapping" and country:
                cur.execute(
                    f"SELECT * FROM {actual_table} WHERE source_country = %s LIMIT %s",
                    (country, limit),
                )
            else:
                cur.execute(f"SELECT * FROM {actual_table} LIMIT %s", (limit,))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception:
            return []

    @staticmethod
    def _detect_encoding(path: Path) -> str:
        for enc in _ENCODINGS:
            try:
                with path.open("r", encoding=enc) as f:
                    f.read(4096)
                return enc
            except (UnicodeDecodeError, UnicodeError):
                continue
        return "utf-8"

    @staticmethod
    def _detect_delimiter(path: Path, encoding: str) -> str:
        with path.open("r", encoding=encoding) as f:
            sample = f.read(4096)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            return dialect.delimiter
        except csv.Error:
            return ","
