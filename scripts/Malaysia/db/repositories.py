#!/usr/bin/env python3
"""
Malaysia database repository - all DB access in one place.

Provides methods for:
- Inserting/querying products, details, reimbursable drugs, PCID mappings
- Sub-step progress tracking (keyword/regno/page level resume)
- Run lifecycle management
"""

import logging
from typing import Dict, List, Optional, Set
from datetime import datetime

from core.db.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class MalaysiaRepository(BaseRepository):
    """All database operations for Malaysia scraper (PostgreSQL backend)."""

    SCRAPER_NAME = "Malaysia"
    TABLE_PREFIX = "my"

    _STEP_TABLE_MAP = {
        1: ("products",),
        2: ("product_details",),
        3: ("consolidated_products",),
        4: ("reimbursable_drugs",),
        5: ("pcid_mappings",),
    }

    def __init__(self, db, run_id: str):
        super().__init__(db, run_id)

    # Progress methods (mark_progress, is_progress_completed, get_completed_keys)
    # are inherited from BaseRepository.

    def get_progress_summary(self, step_number: int) -> Dict[str, int]:
        """Get count of each status for a step."""
        table = self._table("step_progress")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT status, COUNT(*) as cnt FROM {table}
                WHERE run_id = %s AND step_number = %s
                GROUP BY status
            """, (self.run_id, step_number))
            rows = cur.fetchall()
            result = {}
            for row in rows:
                if isinstance(row, tuple):
                    result[row[0]] = row[1]
                else:
                    result[row["status"]] = row["cnt"]
            return result

    # ------------------------------------------------------------------
    # Bulk search counts (Step 2)
    # ------------------------------------------------------------------

    def log_bulk_search_count(
        self,
        keyword: str,
        page_rows: Optional[int],
        csv_rows: Optional[int],
        status: str,
        reason: str = None,
        csv_file: str = None,
    ) -> None:
        """Insert/update per-keyword bulk search row counts."""
        table = self._table("bulk_search_counts")
        diff = None
        if page_rows is not None and csv_rows is not None:
            diff = page_rows - csv_rows
        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, keyword, page_rows, csv_rows, difference, status, reason, csv_file)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, keyword) DO UPDATE SET
                    page_rows = EXCLUDED.page_rows,
                    csv_rows = EXCLUDED.csv_rows,
                    difference = EXCLUDED.difference,
                    status = EXCLUDED.status,
                    reason = EXCLUDED.reason,
                    csv_file = EXCLUDED.csv_file,
                    logged_at = CURRENT_TIMESTAMP
            """, (
                self.run_id, keyword, page_rows, csv_rows, diff, status, reason, csv_file
            ))

    # ------------------------------------------------------------------
    # Products (Step 1 - MyPriMe)
    # ------------------------------------------------------------------

    def insert_products(self, products: List[Dict]) -> int:
        """Bulk insert products from MyPriMe scraping."""
        if not products:
            return 0

        table = self._table("products")

        # If we are resuming a run, the table may already contain rows for the
        # same run_id (e.g., previous attempt crashed mid-step). To keep the
        # step idempotent and avoid duplicate rows/row-count mismatches, purge
        # existing rows for this run before inserting fresh data.
        existing = self.get_product_count()
        if existing > 0:
            with self.db.cursor() as cur:
                cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (self.run_id,))
            self._db_log(
                f"RESET | my_products cleared previous rows for run_id={self.run_id} count={existing}"
            )

        sql = f"""
            INSERT INTO {table}
            (run_id, registration_no, product_name, generic_name,
             dosage_form, strength, pack_size, pack_unit,
             manufacturer, unit_price, retail_price, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        attempted = 0
        for batch_start in range(0, len(products), 500):
            batch = products[batch_start:batch_start + 500]
            with self.db.cursor() as cur:
                for p in batch:
                    dosage_form = p.get("dosage_form") or ""
                    cur.execute(sql, (
                        self.run_id,
                        p.get("registration_no", ""),
                        p.get("product_name"),
                        p.get("generic_name"),
                        dosage_form,
                        p.get("strength"),
                        p.get("pack_size"),
                        p.get("pack_unit"),
                        p.get("manufacturer"),
                    p.get("unit_price"),
                    p.get("retail_price"),
                    p.get("source_url"),
                ))
                attempted += 1
        db_count = self.get_product_count()
        logger.info("Inserted %d products into DB (attempted %d)", db_count, attempted)
        self._db_log(f"OK | my_products count={db_count} | run_id={self.run_id}")
        return db_count

    def get_product_count(self) -> int:
        """Get total products for this run."""
        table = self._table("products")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_registration_nos(self) -> Set[str]:
        """Get all registration numbers from products table."""
        table = self._table("products")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT registration_no FROM {table} WHERE run_id = %s", (self.run_id,))
            return {row[0] if isinstance(row, tuple) else row["registration_no"] for row in cur.fetchall()}

    # ------------------------------------------------------------------
    # Product Details (Step 2 - Quest3Plus)
    # ------------------------------------------------------------------

    def insert_product_detail(self, registration_no: str, product_name: str,
                              holder: str, search_method: str = "bulk",
                              holder_address: str = None,
                              source_url: str = None) -> None:
        """Insert a single product detail."""
        table = self._table("product_details")
        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, registration_no, product_name, holder,
                 holder_address, search_method, source_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, registration_no) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    holder = EXCLUDED.holder,
                    holder_address = EXCLUDED.holder_address,
                    search_method = EXCLUDED.search_method,
                    source_url = EXCLUDED.source_url
            """, (
                self.run_id, registration_no, product_name, holder,
                holder_address, search_method, source_url,
            ))

    def insert_product_details_bulk(self, details: List[Dict],
                                    search_method: str = "bulk") -> int:
        """Bulk insert product details."""
        if not details:
            return 0
        table = self._table("product_details")
        count = 0
        with self.db.cursor() as cur:
            for d in details:
                cur.execute(f"""
                    INSERT INTO {table}
                    (run_id, registration_no, product_name, holder,
                     holder_address, search_method, source_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, registration_no) DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        holder = EXCLUDED.holder,
                        holder_address = EXCLUDED.holder_address,
                        search_method = EXCLUDED.search_method,
                        source_url = EXCLUDED.source_url
                """, (
                    self.run_id,
                    d.get("registration_no", d.get("Registration No", "")),
                    d.get("product_name", d.get("Product Name")),
                    d.get("holder", d.get("Holder")),
                    d.get("holder_address", d.get("Holder Address")),
                    search_method,
                    d.get("source_url"),
                ))
                count += 1
        logger.info("Inserted %d product details (method=%s)", count, search_method)
        self._db_log(f"OK | my_product_details inserted={count} method={search_method} | run_id={self.run_id}")
        return count

    def get_detail_count(self) -> int:
        """Count product details for this run."""
        table = self._table("product_details")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_detailed_registration_nos(self) -> Set[str]:
        """Get registration numbers that have details."""
        table = self._table("product_details")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT registration_no FROM {table} WHERE run_id = %s", (self.run_id,))
            return {row[0] if isinstance(row, tuple) else row["registration_no"] for row in cur.fetchall()}

    def get_missing_registration_nos(self) -> Set[str]:
        """Get regnos in products but not in product_details."""
        products_table = self._table("products")
        details_table = self._table("product_details")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT p.registration_no
                FROM {products_table} p
                LEFT JOIN {details_table} pd
                    ON p.registration_no = pd.registration_no
                    AND pd.run_id = %s
                WHERE p.run_id = %s AND pd.id IS NULL
            """, (self.run_id, self.run_id))
            return {row[0] if isinstance(row, tuple) else row["registration_no"] for row in cur.fetchall()}

    # ------------------------------------------------------------------
    # Consolidated Products (Step 3)
    # ------------------------------------------------------------------

    def consolidate(self) -> int:
        """Consolidate product details into consolidated_products table.
        Deduplicates by registration_no, keeps most recent."""
        consolidated_table = self._table("consolidated_products")
        details_table = self._table("product_details")

        with self.db.cursor() as cur:
            # Clear any existing consolidation for this run
            cur.execute(f"DELETE FROM {consolidated_table} WHERE run_id = %s", (self.run_id,))

            cur.execute(f"""
                INSERT INTO {consolidated_table}
                (run_id, registration_no, product_name, holder, search_method)
                SELECT %s, registration_no, product_name, holder, search_method
                FROM {details_table}
                WHERE run_id = %s
                  AND product_name IS NOT NULL
                  AND product_name != ''
                  AND holder IS NOT NULL
                  AND holder != ''
                GROUP BY registration_no, product_name, holder, search_method
            """, (self.run_id, self.run_id))
            count = cur.rowcount
        logger.info("Consolidated %d products", count)
        self._db_log(f"OK | my_consolidated_products inserted={count} | run_id={self.run_id}")
        return count

    def get_consolidated_count(self) -> int:
        table = self._table("consolidated_products")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    # ------------------------------------------------------------------
    # Reimbursable Drugs (Step 4 - FUKKM)
    # ------------------------------------------------------------------

    def insert_reimbursable_drugs(self, drugs: List[Dict], source_page: int = 0) -> int:
        """Insert reimbursable drugs from a single page."""
        if not drugs:
            return 0
        table = self._table("reimbursable_drugs")
        count = 0
        with self.db.cursor() as cur:
            for d in drugs:
                cur.execute(f"""
                    INSERT INTO {table}
                    (run_id, drug_name, registration_no, dosage_form,
                     strength, pack_size, manufacturer, source_page, source_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, drug_name, dosage_form, strength) DO UPDATE SET
                        registration_no = EXCLUDED.registration_no,
                        pack_size = EXCLUDED.pack_size,
                        manufacturer = EXCLUDED.manufacturer,
                        source_page = EXCLUDED.source_page,
                        source_url = EXCLUDED.source_url
                """, (
                    self.run_id,
                    d.get("drug_name"),
                    d.get("registration_no"),
                    d.get("dosage_form"),
                    d.get("strength"),
                    d.get("pack_size"),
                    d.get("manufacturer"),
                    source_page,
                    d.get("source_url"),
                ))
                count += 1
        logger.info("Inserted %d reimbursable drugs (page %d)", count, source_page)
        self._db_log(f"OK | my_reimbursable_drugs inserted={count} page={source_page} | run_id={self.run_id}")
        return count

    def get_reimbursable_count(self) -> int:
        table = self._table("reimbursable_drugs")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_reimbursable_drug_names(self) -> Set[str]:
        """Get all reimbursable drug names (lowercased for matching)."""
        table = self._table("reimbursable_drugs")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT DISTINCT LOWER(drug_name) as dn FROM {table} WHERE run_id = %s", (self.run_id,))
            result = set()
            for row in cur.fetchall():
                val = row[0] if isinstance(row, tuple) else row["dn"]
                if val:
                    result.add(val)
            return result

    # ------------------------------------------------------------------
    # PCID Mappings (Step 5)
    # ------------------------------------------------------------------

    def load_pcid_reference(self, rows: List[Dict]) -> int:
        """Load PCID reference CSV data into pcid_reference table."""
        table = self._table("pcid_reference")
        with self.db.cursor() as cur:
            cur.execute(f"DELETE FROM {table}")  # Clear old data
        count = 0
        with self.db.cursor() as cur:
            for r in rows:
                cur.execute(f"""
                    INSERT INTO {table}
                    (pcid, local_pack_code, presentation, package_number, product_group,
                     generic_name, description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (local_pack_code, presentation) DO UPDATE SET
                        pcid = EXCLUDED.pcid,
                        package_number = EXCLUDED.package_number,
                        product_group = EXCLUDED.product_group,
                        generic_name = EXCLUDED.generic_name,
                        description = EXCLUDED.description
                """, (
                    r.get("pcid", r.get("PCID", r.get("Pcid", r.get("PCID Mapping")))),
                    r.get("local_pack_code", r.get("LOCAL_PACK_CODE", r.get("Local Pack Code", ""))),
                    r.get("presentation", r.get("Presentation", r.get("PACK_SIZE", r.get("Pack Size", "")))),
                    r.get("package_number", r.get("Package Number")),
                    r.get("product_group", r.get("Product Group")),
                    r.get("generic_name", r.get("Generic Name")),
                    r.get("description", r.get("Description")),
                ))
                count += 1
        logger.info("Loaded %d PCID reference rows", count)
        self._db_log(f"OK | my_pcid_reference loaded={count} | run_id={self.run_id}")
        return count

    
    def _build_lookup_key_expr(self, table_alias: str, columns: Optional[List[str]]) -> str:
        """Build normalized lookup key SQL expression for a table alias.

        Each column is normalized individually (strip non-alphanumeric, uppercase)
        then joined with CHR(0) separator to prevent boundary collisions
        (e.g. 'AB'||'CD' vs 'A'||'BCD' both becoming 'ABCD').
        """
        cols = columns or []
        if not cols:
            cols = ["registration_no"]
        normalized_parts = [
            f"UPPER(REGEXP_REPLACE(COALESCE({table_alias}.{col}, ''), '[^A-Za-z0-9]', '', 'g'))"
            for col in cols
        ]
        if len(normalized_parts) > 1:
            # Join with NUL separator between normalized parts
            joined = (" || CHR(0) || ").join(normalized_parts)
            return f"({joined})"
        return normalized_parts[0]

    def generate_pcid_mappings(
        self,
        vat_percent: float = 0.0,
        product_key_columns: Optional[List[str]] = None,
        pcid_key_columns: Optional[List[str]] = None,
    ) -> int:
        """Join products + details + reimbursable + pcid_reference → pcid_mappings."""
        products_table = self._table("products")
        consolidated_table = self._table("consolidated_products")
        reimbursable_table = self._table("reimbursable_drugs")
        pcid_ref_table = self._table("pcid_reference")
        mappings_table = self._table("pcid_mappings")

        product_key_expr = self._build_lookup_key_expr("p", product_key_columns or ["registration_no"])
        pcid_key_expr = self._build_lookup_key_expr("pr", pcid_key_columns or ["local_pack_code"])

        with self.db.cursor() as cur:
            # Ensure schema is up to date when step 0 is skipped (resume runs).
            try:
                cur.execute(f"ALTER TABLE {mappings_table} ADD COLUMN IF NOT EXISTS search_method TEXT")
            except Exception:
                pass
            # Add presentation column if not exists (migration)
            try:
                cur.execute(f"ALTER TABLE {mappings_table} ADD COLUMN IF NOT EXISTS presentation TEXT")
            except Exception:
                pass
            try:
                cur.execute(f"ALTER TABLE {pcid_ref_table} ADD COLUMN IF NOT EXISTS presentation TEXT")
            except Exception:
                pass
            cur.execute(f"DELETE FROM {mappings_table} WHERE run_id = %s", (self.run_id,))

            cur.execute(f"""
                INSERT INTO {mappings_table} (
                    run_id, pcid, local_pack_code, presentation, package_number,
                    country, company, product_group,
                    local_product_name, generic_name, description,
                    pack_size, currency,
                    public_without_vat_price, public_with_vat_price,
                    vat_percent, reimbursable_status,
                    region, marketing_authority, source,
                    unit_price, strength, formulation, search_method
                )
                SELECT
                    %s as run_id,
                    pr.pcid,
                    p.registration_no as local_pack_code,
                    pr.presentation,
                    pr.package_number,
                    'MALAYSIA' as country,
                    cp.holder as company,
                    pr.product_group,
                    cp.product_name as local_product_name,
                    p.generic_name,
                    pr.description,
                    p.pack_size,
                    'MYR' as currency,
                    p.retail_price as public_without_vat_price,
                    p.retail_price as public_with_vat_price,
                    %s as vat_percent,
                    CASE
                        WHEN rd.drug_name IS NOT NULL THEN 'FULLY REIMBURSABLE'
                        ELSE NULL
                    END as reimbursable_status,
                    'MALAYSIA' as region,
                    cp.holder as marketing_authority,
                    'PRICENTRIC' as source,
                    p.unit_price,
                    p.strength,
                    p.dosage_form as formulation,
                    cp.search_method as search_method
                FROM (
                    SELECT DISTINCT ON (registration_no)
                        run_id,
                        registration_no,
                        product_name,
                        generic_name,
                        dosage_form,
                        strength,
                        pack_size,
                        unit_price,
                        retail_price
                    FROM {products_table}
                    WHERE run_id = %s
                    ORDER BY registration_no, scraped_at DESC, id DESC
                ) p
                LEFT JOIN {consolidated_table} cp
                    ON p.registration_no = cp.registration_no
                    AND cp.run_id = %s
                LEFT JOIN LATERAL (
                    SELECT drug_name
                    FROM {reimbursable_table} rd
                    WHERE LOWER(p.generic_name) = LOWER(rd.drug_name)
                      AND rd.run_id = %s
                    ORDER BY rd.id ASC
                    LIMIT 1
                ) rd ON TRUE
                LEFT JOIN {pcid_ref_table} pr
                    ON {product_key_expr} = {pcid_key_expr}
            """, (self.run_id, vat_percent, self.run_id, self.run_id, self.run_id))
            count = cur.rowcount
        logger.info("Generated %d PCID mappings", count)
        self._db_log(f"OK | my_pcid_mappings inserted={count} | run_id={self.run_id}")
        return count

    def get_mapped_count(self) -> int:
        """Count rows that have a valid PCID mapping (excludes OOS)."""
        table = self._table("pcid_mappings")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE run_id = %s AND pcid IS NOT NULL AND pcid != ''
                  AND UPPER(pcid) != 'OOS'
            """, (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_not_mapped_count(self) -> int:
        """Count rows without a PCID mapping."""
        table = self._table("pcid_mappings")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE run_id = %s AND (pcid IS NULL OR pcid = '')
            """, (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_oos_count(self) -> int:
        """Count rows with OOS PCID."""
        table = self._table("pcid_mappings")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE run_id = %s AND UPPER(pcid) = 'OOS'
            """, (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_pcid_mapped_rows(self) -> List[Dict]:
        """Get all rows with valid PCID mapping (excludes OOS)."""
        table = self._table("pcid_mappings")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"""
                SELECT * FROM {table}
                WHERE run_id = %s AND pcid IS NOT NULL AND pcid != ''
                  AND UPPER(pcid) != 'OOS'
                ORDER BY local_pack_code
            """, (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_pcid_not_mapped_rows(self) -> List[Dict]:
        """Get all rows without PCID mapping."""
        table = self._table("pcid_mappings")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"""
                SELECT * FROM {table}
                WHERE run_id = %s AND (pcid IS NULL OR pcid = '')
                ORDER BY local_pack_code
            """, (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_pcid_oos_rows(self) -> List[Dict]:
        """Get all rows with OOS PCID mapping."""
        table = self._table("pcid_mappings")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"""
                SELECT * FROM {table}
                WHERE run_id = %s AND UPPER(pcid) = 'OOS'
                ORDER BY local_pack_code
            """, (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_pcid_reference_no_data(
        self,
        product_key_columns: Optional[List[str]] = None,
        pcid_key_columns: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Get PCID reference rows that have no matching MyPriMe (Step 1) product data.

        Note: We still try to enrich these rows with Quest3 (Step 2/3) consolidated
        product details when available, so the export is useful for debugging.
        """
        products_table = self._table("products")
        consolidated_table = self._table("consolidated_products")
        pcid_ref_table = self._table("pcid_reference")

        product_key_expr = self._build_lookup_key_expr("p", product_key_columns or ["registration_no"])
        pcid_key_expr = self._build_lookup_key_expr("pr", pcid_key_columns or ["local_pack_code"])
        consolidated_key_expr = self._build_lookup_key_expr("cp", ["registration_no"])

        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"""
                SELECT
                    pr.pcid,
                    pr.local_pack_code,
                    pr.presentation,
                    pr.package_number,
                    pr.product_group,
                    pr.generic_name,
                    pr.description,
                    cp.product_name,
                    cp.holder,
                    cp.search_method
                FROM {pcid_ref_table} pr
                LEFT JOIN (
                    SELECT DISTINCT ON (registration_no)
                        registration_no,
                        product_name,
                        holder,
                        search_method
                    FROM {consolidated_table}
                    WHERE run_id = %s
                    ORDER BY registration_no, id DESC
                ) cp
                    ON {consolidated_key_expr} = {pcid_key_expr}
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM (
                        SELECT DISTINCT ON (registration_no)
                            registration_no,
                            generic_name,
                            dosage_form,
                            strength,
                            pack_size
                        FROM {products_table}
                        WHERE run_id = %s
                        ORDER BY registration_no, scraped_at DESC, id DESC
                    ) p
                    WHERE {product_key_expr} = {pcid_key_expr}
                )
                ORDER BY pr.local_pack_code
            """, (self.run_id, self.run_id))
            return [dict(row) for row in cur.fetchall()]


    # ------------------------------------------------------------------
    # HTTP request logging
    # ------------------------------------------------------------------

    def log_request(self, url: str, method: str = "GET",
                    status_code: int = None, response_bytes: int = None,
                    elapsed_ms: float = None, error: str = None) -> None:
        """Log an HTTP request (best-effort)."""
        try:
            from core.db.tracking import log_http_request

            log_http_request(
                self.db,
                self.run_id,
                self.SCRAPER_NAME,
                url,
                method=method,
                status_code=status_code,
                response_bytes=response_bytes,
                elapsed_ms=elapsed_ms,
                error=error,
            )
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Stats / reporting helpers
    # ------------------------------------------------------------------

    def get_detail_counts_by_search_method(self) -> Dict[str, int]:
        """Count product details by search method (bulk vs individual)."""
        table = self._table("product_details")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT COALESCE(search_method, 'unknown') as method, COUNT(*) as cnt
                FROM {table}
                WHERE run_id = %s
                GROUP BY COALESCE(search_method, 'unknown')
            """, (self.run_id,))
            rows = cur.fetchall()
            result: Dict[str, int] = {}
            for row in rows:
                if isinstance(row, tuple):
                    result[row[0]] = row[1]
                else:
                    result[row["method"]] = row["cnt"]
            return result

    def get_run_stats(self) -> Dict:
        """Get comprehensive stats for this run."""
        return {
            "products": self.get_product_count(),
            "product_details": self.get_detail_count(),
            "consolidated": self.get_consolidated_count(),
            "reimbursable": self.get_reimbursable_count(),
            "pcid_mapped": self.get_mapped_count(),
            "pcid_not_mapped": self.get_not_mapped_count(),
        }

    # ------------------------------------------------------------------
    # Export report tracking
    # ------------------------------------------------------------------

    def log_export_report(self, report_type: str, file_path: str,
                          row_count: Optional[int] = None) -> None:
        """Track an export/report file for this run."""
        table = self._table("export_reports")
        with self.db.cursor() as cur:
            # Safe create for resume runs when step 0 was skipped.
            try:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id SERIAL PRIMARY KEY,
                        run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                        report_type TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        row_count INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            except Exception:
                pass
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, report_type, file_path, row_count)
                VALUES (%s, %s, %s, %s)
            """, (self.run_id, report_type, file_path, row_count))
