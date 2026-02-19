#!/usr/bin/env python3
"""
Canada Ontario database repository - all DB access in one place.

Provides methods for:
- Inserting/querying products, manufacturers, and EAP prices
- Final output generation and retrieval (EVERSANA format)
- PCID mapping management
- Sub-step progress tracking
- Run lifecycle management
"""

import logging
from typing import Dict, List, Optional

from core.db.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class CanadaOntarioRepository(BaseRepository):
    """All database operations for Canada Ontario scraper (PostgreSQL backend)."""

    SCRAPER_NAME = "CanadaOntario"
    TABLE_PREFIX = "co"

    _STEP_TABLE_MAP = {
        1: ("products", "manufacturers", "search_summary"),
        2: ("eap_prices",),
        3: ("final_output", "pcid_mappings"),
    }


    def __init__(self, db, run_id: str):
        super().__init__(db, run_id)

    # clear_step_data, lifecycle, and progress methods inherited from BaseRepository

    # ------------------------------------------------------------------
    # Products (Step 1)
    # ------------------------------------------------------------------

    def insert_products(self, products: List[Dict]) -> int:
        """Bulk insert products."""
        if not products:
            return 0

        table = self._table("products")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, local_pack_description, generic_name, manufacturer, manufacturer_code,
             din, strength, dosage_form, pack_size, unit_price,
             reimbursable_price, public_with_vat, copay,
             interchangeability, benefit_status, price_type,
             limited_use, therapeutic_notes, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self.db.cursor() as cur:
            for product in products:
                cur.execute(sql, (
                    self.run_id,
                    product.get("local_pack_description"),
                    product.get("generic_name"),
                    product.get("manufacturer"),
                    product.get("manufacturer_code"),
                    product.get("din"),
                    product.get("strength"),
                    product.get("dosage_form"),
                    product.get("pack_size"),
                    product.get("unit_price"),
                    product.get("reimbursable_price"),
                    product.get("public_with_vat"),
                    product.get("copay"),
                    product.get("interchangeability"),
                    product.get("benefit_status"),
                    product.get("price_type"),
                    product.get("limited_use"),
                    product.get("therapeutic_notes"),
                    product.get("source_url"),
                ))
                count += 1

        logger.info("Inserted %d products", count)
        self._db_log(f"OK | co_products inserted={count} | run_id={self.run_id}")
        return count

    def get_products_count(self) -> int:
        """Get total products for this run."""
        table = self._table("products")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_products(self) -> List[Dict]:
        """Get all products as list of dicts."""
        table = self._table("products")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Manufacturers (Step 1)
    # ------------------------------------------------------------------

    def insert_manufacturers(self, manufacturers: List[Dict]) -> int:
        """Bulk insert manufacturers."""
        if not manufacturers:
            return 0

        table = self._table("manufacturers")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, manufacturer_code, manufacturer_name, address)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (run_id, manufacturer_code) DO UPDATE SET
                manufacturer_name = EXCLUDED.manufacturer_name,
                address = EXCLUDED.address
        """

        with self.db.cursor() as cur:
            for m in manufacturers:
                cur.execute(sql, (
                    self.run_id,
                    m.get("manufacturer_code"),
                    m.get("manufacturer_name"),
                    m.get("address"),
                ))
                count += 1

        logger.info("Inserted %d manufacturers", count)
        self._db_log(f"OK | co_manufacturers inserted={count} | run_id={self.run_id}")
        return count

    def get_manufacturers_count(self) -> int:
        """Get total manufacturers for this run."""
        table = self._table("manufacturers")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_manufacturers(self) -> List[Dict]:
        """Get all manufacturers as list of dicts."""
        table = self._table("manufacturers")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # EAP Prices (Step 2)
    # ------------------------------------------------------------------

    def insert_eap_prices(self, prices: List[Dict]) -> int:
        """Bulk insert EAP prices."""
        if not prices:
            return 0

        table = self._table("eap_prices")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, din, product_name, generic_name, strength,
             dosage_form, eap_price, currency, effective_date, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self.db.cursor() as cur:
            for price in prices:
                cur.execute(sql, (
                    self.run_id,
                    price.get("din"),
                    price.get("product_name"),
                    price.get("generic_name"),
                    price.get("strength"),
                    price.get("dosage_form"),
                    price.get("eap_price"),
                    price.get("currency", "CAD"),
                    price.get("effective_date"),
                    price.get("source_url"),
                ))
                count += 1

        logger.info("Inserted %d EAP prices", count)
        self._db_log(f"OK | co_eap_prices inserted={count} | run_id={self.run_id}")
        return count

    def get_eap_prices_count(self) -> int:
        """Get total EAP prices for this run."""
        table = self._table("eap_prices")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_eap_prices(self) -> List[Dict]:
        """Get all EAP prices as list of dicts."""
        table = self._table("eap_prices")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Final Output (Step 3 - EVERSANA format)
    # ------------------------------------------------------------------

    def insert_final_output(self, outputs: List[Dict]) -> int:
        """Bulk insert final output data (EVERSANA format)."""
        if not outputs:
            return 0

        table = self._table("final_output")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, pcid, country, region, company, local_product_name,
             generic_name, unit_price, public_with_vat_price, public_without_vat_price,
             eap_price, currency, reimbursement_category, reimbursement_amount,
             copay_amount, benefit_status, interchangeability, din, strength,
             dosage_form, pack_size, local_pack_description, local_pack_code,
             effective_start_date, effective_end_date, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, din, pack_size) DO UPDATE SET
                pcid = EXCLUDED.pcid,
                company = EXCLUDED.company,
                unit_price = EXCLUDED.unit_price,
                public_with_vat_price = EXCLUDED.public_with_vat_price,
                eap_price = EXCLUDED.eap_price,
                reimbursement_category = EXCLUDED.reimbursement_category,
                reimbursement_amount = EXCLUDED.reimbursement_amount,
                copay_amount = EXCLUDED.copay_amount
        """

        with self.db.cursor() as cur:
            for out in outputs:
                cur.execute(sql, (
                    self.run_id,
                    out.get("pcid"),
                    out.get("country", "CANADA"),
                    out.get("region", "NORTH AMERICA"),
                    out.get("company"),
                    out.get("local_product_name"),
                    out.get("generic_name"),
                    out.get("unit_price"),
                    out.get("public_with_vat_price"),
                    out.get("public_without_vat_price"),
                    out.get("eap_price"),
                    out.get("currency", "CAD"),
                    out.get("reimbursement_category"),
                    out.get("reimbursement_amount"),
                    out.get("copay_amount"),
                    out.get("benefit_status"),
                    out.get("interchangeability"),
                    out.get("din"),
                    out.get("strength"),
                    out.get("dosage_form"),
                    out.get("pack_size"),
                    out.get("local_pack_description"),
                    out.get("local_pack_code"),
                    out.get("effective_start_date"),
                    out.get("effective_end_date"),
                    out.get("source", "PRICENTRIC"),
                ))
                count += 1

        logger.info("Inserted %d final output entries", count)
        self._db_log(f"OK | co_final_output inserted={count} | run_id={self.run_id}")
        return count

    def get_final_output_count(self) -> int:
        """Get total final output entries for this run."""
        table = self._table("final_output")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_final_output(self) -> List[Dict]:
        """Get all final output entries as list of dicts."""
        table = self._table("final_output")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s ORDER BY id", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_final_output_by_pcid(self, pcid: str) -> List[Dict]:
        """Get final output entries by PCID."""
        table = self._table("final_output")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s AND pcid = %s", (self.run_id, pcid))
            return [dict(row) for row in cur.fetchall()]

    def get_final_output_by_din(self, din: str) -> List[Dict]:
        """Get final output entries by DIN."""
        table = self._table("final_output")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s AND din = %s", (self.run_id, din))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # PCID Mappings
    # ------------------------------------------------------------------

    def insert_pcid_mappings(self, mappings: List[Dict]) -> int:
        """Bulk insert PCID mappings."""
        if not mappings:
            return 0

        table = self._table("pcid_mappings")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, pcid, local_pack_code, presentation, product_name,
             generic_name, manufacturer, country, region, currency,
             unit_price, public_with_vat_price, eap_price, reimbursement_category,
             effective_date, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, pcid, local_pack_code) DO UPDATE SET
                presentation = EXCLUDED.presentation,
                product_name = EXCLUDED.product_name,
                generic_name = EXCLUDED.generic_name,
                manufacturer = EXCLUDED.manufacturer,
                unit_price = EXCLUDED.unit_price,
                public_with_vat_price = EXCLUDED.public_with_vat_price,
                eap_price = EXCLUDED.eap_price,
                reimbursement_category = EXCLUDED.reimbursement_category
        """

        with self.db.cursor() as cur:
            for m in mappings:
                cur.execute(sql, (
                    self.run_id,
                    m.get("pcid"),
                    m.get("local_pack_code"),
                    m.get("presentation"),
                    m.get("product_name"),
                    m.get("generic_name"),
                    m.get("manufacturer"),
                    m.get("country", "CANADA"),
                    m.get("region", "NORTH AMERICA"),
                    m.get("currency", "CAD"),
                    m.get("unit_price"),
                    m.get("public_with_vat_price"),
                    m.get("eap_price"),
                    m.get("reimbursement_category"),
                    m.get("effective_date"),
                    m.get("source", "PRICENTRIC"),
                ))
                count += 1

        logger.info("Inserted %d PCID mappings", count)
        self._db_log(f"OK | co_pcid_mappings inserted={count} | run_id={self.run_id}")
        return count

    def get_pcid_mappings_count(self) -> int:
        """Get total PCID mappings for this run."""
        table = self._table("pcid_mappings")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_pcid_mappings(self) -> List[Dict]:
        """Get all PCID mappings as list of dicts."""
        table = self._table("pcid_mappings")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Export report tracking
    # ------------------------------------------------------------------

    def log_export_report(self, report_type: str, row_count: int = None,
                          export_format: str = "db") -> None:
        """Track an export/report for this run (DB-only, no file path)."""
        table = self._table("export_reports")
        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, report_type, row_count, export_format)
                VALUES (%s, %s, %s, %s)
            """, (self.run_id, report_type, row_count, export_format))

    # ------------------------------------------------------------------
    # Stats / reporting helpers
    # ------------------------------------------------------------------

    def get_run_stats(self) -> Dict:
        """Get comprehensive stats for this run."""
        return {
            "products": self.get_products_count(),
            "manufacturers": self.get_manufacturers_count(),
            "eap_prices": self.get_eap_prices_count(),
            "final_output": self.get_final_output_count(),
            "pcid_mappings": self.get_pcid_mappings_count(),
        }

    def insert_search_summary(self, key: str, search_type: str, expected: Optional[int], 
                              parsed: int, unique: int, duplicates: int, status: str = "PASS") -> None:
        """Insert or update search summary for a specific key (e.g. letter) and type."""
        table = self._table("search_summary")
        sql = f"""
            INSERT INTO {table}
            (run_id, search_key, search_type, expected_count, parsed_count, unique_count, duplicate_count, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, search_key, search_type) DO UPDATE SET
                expected_count = EXCLUDED.expected_count,
                parsed_count = EXCLUDED.parsed_count,
                unique_count = EXCLUDED.unique_count,
                duplicate_count = EXCLUDED.duplicate_count,
                status = EXCLUDED.status,
                created_at = CURRENT_TIMESTAMP
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, key, search_type, expected, parsed, unique, duplicates, status))
