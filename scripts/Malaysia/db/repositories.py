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

logger = logging.getLogger(__name__)


class MalaysiaRepository:
    """All database operations for Malaysia scraper (PostgreSQL backend)."""

    def __init__(self, db, run_id: str):
        """
        Initialize repository.

        Args:
            db: PostgresDB instance
            run_id: Current run ID
        """
        self.db = db
        self.run_id = run_id

    def _table(self, name: str) -> str:
        """Get table name with Malaysia prefix."""
        return f"my_{name}"

    def _db_log(self, message: str) -> None:
        """Emit a [DB] activity log line for GUI activity panel."""
        try:
            print(f"[DB] {message}", flush=True)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Utility: clear step data
    # ------------------------------------------------------------------

    _STEP_TABLE_MAP = {
        1: ("products",),
        2: ("product_details",),
        3: ("consolidated_products",),
        4: ("reimbursable_drugs",),
        5: ("pcid_mappings",),
    }

    def clear_step_data(self, step: int, include_downstream: bool = False) -> Dict[str, int]:
        """
        Delete data for the given step (and optionally downstream steps) for this run_id.

        Args:
            step: Pipeline step number (1-5)
            include_downstream: If True, also clear tables for all later steps.

        Returns:
            Dict mapping full table name -> rows deleted.
        """
        if step not in self._STEP_TABLE_MAP:
            raise ValueError(f"Unsupported step {step}; valid steps: {sorted(self._STEP_TABLE_MAP)}")

        steps = [s for s in sorted(self._STEP_TABLE_MAP) if s == step or (include_downstream and s >= step)]
        deleted: Dict[str, int] = {}
        with self.db.cursor() as cur:
            for s in steps:
                for short_name in self._STEP_TABLE_MAP[s]:
                    table = self._table(short_name)
                    cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (self.run_id,))
                    deleted[table] = cur.rowcount
        try:
            self.db.commit()
        except Exception:
            # Connection class may autocommit; ignore if commit is unavailable.
            pass

        self._db_log(f"CLEAR | steps={steps} tables={','.join(deleted)} run_id={self.run_id}")
        return deleted

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------

    def start_run(self, mode: str = "fresh") -> None:
        """Register a new run in run_ledger."""
        from core.db.models import run_ledger_start
        sql, params = run_ledger_start(self.run_id, "Malaysia", mode=mode)
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"OK | run_ledger start | run_id={self.run_id} mode={mode}")

    def finish_run(self, status: str, items_scraped: int = 0,
                   items_exported: int = 0, error_message: str = None) -> None:
        """Mark run as finished."""
        from core.db.models import run_ledger_finish
        sql, params = run_ledger_finish(
            self.run_id, status,
            items_scraped=items_scraped,
            items_exported=items_exported,
            error_message=error_message,
        )
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"FINISH | run_ledger updated | status={status} items={items_scraped}")

    def resume_run(self) -> None:
        """Mark existing run as resumed."""
        from core.db.models import run_ledger_resume
        sql, params = run_ledger_resume(self.run_id)
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"RESUME | run_ledger updated | run_id={self.run_id}")

    # ------------------------------------------------------------------
    # Step progress (sub-step resume)
    # ------------------------------------------------------------------

    def mark_progress(self, step_number: int, step_name: str,
                      progress_key: str, status: str,
                      error_message: str = None) -> None:
        """Mark a sub-step progress item."""
        now = datetime.now().isoformat()
        table = self._table("step_progress")

        sql = f"""
            INSERT INTO {table}
            (run_id, step_number, step_name, progress_key, status,
             error_message, started_at, completed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                step_name = EXCLUDED.step_name,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                started_at = CASE
                    WHEN EXCLUDED.status = 'in_progress' THEN EXCLUDED.started_at
                    ELSE COALESCE({table}.started_at, EXCLUDED.started_at)
                END,
                completed_at = CASE
                    WHEN EXCLUDED.status IN ('completed', 'failed', 'skipped') THEN EXCLUDED.completed_at
                    WHEN EXCLUDED.status = 'in_progress' THEN NULL
                    ELSE {table}.completed_at
                END
        """

        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id, step_number, step_name, progress_key, status,
                error_message,
                now if status == "in_progress" else None,
                now if status in ("completed", "failed", "skipped") else None,
            ))

    def is_progress_completed(self, step_number: int, progress_key: str) -> bool:
        """Check if a sub-step item is completed."""
        table = self._table("step_progress")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT status FROM {table}
                WHERE run_id = %s AND step_number = %s AND progress_key = %s
            """, (self.run_id, step_number, progress_key))
            row = cur.fetchone()
            if row is None:
                return False
            status = row[0] if isinstance(row, tuple) else row["status"]
            return status == "completed"

    def get_completed_keys(self, step_number: int) -> Set[str]:
        """Get all completed progress keys for a step."""
        table = self._table("step_progress")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT progress_key FROM {table}
                WHERE run_id = %s AND step_number = %s AND status = 'completed'
            """, (self.run_id, step_number))
            rows = cur.fetchall()
            return {row[0] if isinstance(row, tuple) else row["progress_key"] for row in rows}

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
                    (pcid, local_pack_code, package_number, product_group,
                     generic_name, description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (local_pack_code) DO UPDATE SET
                        pcid = EXCLUDED.pcid,
                        package_number = EXCLUDED.package_number,
                        product_group = EXCLUDED.product_group,
                        generic_name = EXCLUDED.generic_name,
                        description = EXCLUDED.description
                """, (
                    r.get("pcid", r.get("PCID", r.get("Pcid", r.get("PCID Mapping")))),
                    r.get("local_pack_code", r.get("LOCAL_PACK_CODE", r.get("Local Pack Code", ""))),
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
        """Build normalized lookup key SQL expression for a table alias."""
        cols = columns or []
        if not cols:
            cols = ["registration_no"]
        parts = [f"COALESCE({table_alias}.{col}, '')" for col in cols]
        concat_expr = " || ".join(parts) if len(parts) > 1 else parts[0]
        return f"UPPER(REGEXP_REPLACE({concat_expr}, '[^A-Za-z0-9]', '', 'g'))"

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
            cur.execute(f"DELETE FROM {mappings_table} WHERE run_id = %s", (self.run_id,))

            cur.execute(f"""
                INSERT INTO {mappings_table} (
                    run_id, pcid, local_pack_code, package_number,
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
                        registration_no,
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
        """Count rows that have a PCID mapping."""
        table = self._table("pcid_mappings")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE run_id = %s AND pcid IS NOT NULL AND pcid != ''
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

    def get_pcid_mapped_rows(self) -> List[Dict]:
        """Get all rows with PCID mapping as list of dicts."""
        table = self._table("pcid_mappings")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"""
                SELECT * FROM {table}
                WHERE run_id = %s AND pcid IS NOT NULL AND pcid != ''
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
        """Log an HTTP request."""
        with self.db.cursor() as cur:
            cur.execute("""
                INSERT INTO http_requests
                (run_id, url, method, status_code, response_bytes, elapsed_ms, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (self.run_id, url, method, status_code, response_bytes,
                  elapsed_ms, error))

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
