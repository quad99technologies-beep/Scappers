#!/usr/bin/env python3
"""
Argentina database repository (PostgreSQL backend).

Centralises all DB access for the Argentina scraper so the pipeline can move
away from CSV inputs/progress files. Mirrors the Malaysia repository pattern.
"""

import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import logging
import hashlib
import re
from typing import Set, Tuple

# Add repo root to path for core imports (MUST be before any core imports)
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.base_repository import BaseRepository
from core.utils.text_utils import nk

try:
    from psycopg2.extras import execute_values
    _HAS_EXECUTE_VALUES = True
except ImportError:
    _HAS_EXECUTE_VALUES = False

logger = logging.getLogger(__name__)


class ArgentinaRepository(BaseRepository):
    """All Argentina-specific DB operations."""

    SCRAPER_NAME = "Argentina"
    TABLE_PREFIX = "ar"

    _STEP_TABLE_MAP = {
        1: ("product_index",),  # Step 1: Get Product List
        2: ("product_index",),  # Step 2: Prepare URLs (same table)
        3: ("products",),       # Step 3: Selenium Scrape
        4: ("products",),       # Step 4: API Scrape (same table)
        5: ("products_translated",),  # Step 5: Translate
        6: ("export_reports",),  # Step 6: Generate Output
    }

    def __init__(self, db, run_id: str):
        super().__init__(db, run_id)

    # ------------------------------------------------------------------ #
    # Product index / queue (prepared URLs replacement)                  #
    # ------------------------------------------------------------------ #
    def upsert_product_index(self, rows: Sequence[Dict]) -> int:
        """
        Bulk upsert product/company pairs into ar_product_index.
        Each row dict must have keys: product, company, url (optional).
        Uses execute_values for 5-10x faster bulk inserts.
        """
        if not rows:
            return 0

        sql = """
            INSERT INTO ar_product_index
            (run_id, product, company, url, loop_count, total_records, status)
            VALUES %s
            ON CONFLICT (run_id, company, product) DO UPDATE SET
                url = EXCLUDED.url,
                loop_count = EXCLUDED.loop_count,
                total_records = EXCLUDED.total_records,
                status = EXCLUDED.status,
                updated_at = CURRENT_TIMESTAMP
        """
        tuples = [
            (
                self.run_id,
                r.get("product", ""),
                r.get("company", ""),
                r.get("url"),
                r.get("loop_count", 0) or 0,
                r.get("total_records", 0) or 0,
                r.get("status", "pending"),
            )
            for r in rows
        ]

        BATCH = 500
        inserted = 0
        with self.db.cursor() as cur:
            if _HAS_EXECUTE_VALUES:
                for i in range(0, len(tuples), BATCH):
                    batch = tuples[i : i + BATCH]
                    execute_values(cur, sql, batch, page_size=BATCH)
                    inserted += len(batch)
            else:
                row_sql = """
                    INSERT INTO ar_product_index
                    (run_id, product, company, url, loop_count, total_records, status)
                    VALUES (%s, %s, %s, %s, COALESCE(%s,0), COALESCE(%s,0), %s)
                    ON CONFLICT (run_id, company, product) DO UPDATE SET
                        url = EXCLUDED.url,
                        loop_count = EXCLUDED.loop_count,
                        total_records = EXCLUDED.total_records,
                        status = EXCLUDED.status,
                        updated_at = CURRENT_TIMESTAMP
                """
                for t in tuples:
                    cur.execute(row_sql, t)
                    inserted += 1
        self._db_log(f"OK | ar_product_index upserted={inserted} | run_id={self.run_id}")
        return inserted

    def set_urls(self, rows: Sequence[Dict]) -> int:
        """
        Update URLs for products in product_index (keeps loop counters).
        rows: [{product, company, url}]
        Uses a temp-table join for bulk URL updates (much faster than row-by-row).
        """
        if not rows:
            return 0

        if _HAS_EXECUTE_VALUES and len(rows) > 10:
            # Bulk path: load into a temp table, then UPDATE ... FROM
            tuples = [
                (r.get("url"), self.run_id, r.get("company"), r.get("product"))
                for r in rows
            ]
            with self.db.cursor() as cur:
                cur.execute("""
                    CREATE TEMP TABLE _tmp_url_update (
                        url TEXT, run_id TEXT, company TEXT, product TEXT
                    ) ON COMMIT DROP
                """)
                execute_values(
                    cur,
                    "INSERT INTO _tmp_url_update (url, run_id, company, product) VALUES %s",
                    tuples,
                    page_size=500,
                )
                cur.execute("""
                    UPDATE ar_product_index pi
                       SET url = t.url,
                           updated_at = CURRENT_TIMESTAMP
                      FROM _tmp_url_update t
                     WHERE pi.run_id = t.run_id
                       AND pi.company = t.company
                       AND pi.product = t.product
                """)
                updated = cur.rowcount
        else:
            sql = """
                UPDATE ar_product_index
                   SET url = %s,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE run_id = %s AND company = %s AND product = %s
            """
            updated = 0
            with self.db.cursor() as cur:
                for r in rows:
                    cur.execute(sql, (r.get("url"), self.run_id, r.get("company"), r.get("product")))
                    updated += cur.rowcount
        self._db_log(f"OK | ar_product_index urls_updated={updated} | run_id={self.run_id}")
        return updated

    def get_all_product_index(self) -> List[Dict]:
        """Return all product index rows for this run."""
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(
                """
                SELECT id, product, company, url, loop_count, total_records, status
                  FROM ar_product_index
                 WHERE run_id = %s
                 ORDER BY id
                """,
                (self.run_id,),
            )
            return [dict(row) for row in cur.fetchall()]

    def get_product_index_count(self) -> int:
        with self.db.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_urls_prepared_count(self) -> int:
        with self.db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s AND url IS NOT NULL AND url <> ''",
                (self.run_id,),
            )
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_pending_products(self, max_loop: int = 5, limit: int = 500) -> List[Dict]:
        """
        Fetch products that still need scraping (Total Records = 0, loop_count < max_loop).
        
        Includes products that:
        - Have total_records = 0 (no data scraped yet)
        - Have loop_count < max_loop (haven't exceeded max attempts)
        - Have status in ('pending','failed','in_progress')

        Important:
        - Do NOT filter on scraped_by_selenium here. That flag indicates an item has been
          touched by Selenium at least once, but retries are governed by status + loop_count.
          Filtering on scraped_by_selenium can permanently exclude pending retries.
        """
        sql = """
            SELECT id, product, company, url, loop_count, total_records
            FROM ar_product_index
            WHERE run_id = %s
              AND total_records = 0
              AND loop_count < %s
              AND status IN ('pending','failed','in_progress')
            ORDER BY loop_count ASC, id ASC
            LIMIT %s
        """
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(sql, (self.run_id, max_loop, limit))
            return [dict(row) for row in cur.fetchall()]

    def claim_pending_products(self, worker_id: str, max_loop: int = 5, limit: int = 10) -> List[Dict]:
        """
        Atomically fetch and lock pending products for distributed processing.
        Uses SKIP LOCKED to prevent race conditions between workers.
        Marks items as 'in_progress' and assigns worker_id.
        """
        sql = """
            UPDATE ar_product_index
            SET status = 'in_progress',
                last_attempt_at = CURRENT_TIMESTAMP,
                last_attempt_source = 'selenium',
                error_message = NULL  -- Clear previous errors on retry
            WHERE id IN (
                SELECT id
                FROM ar_product_index
                WHERE run_id = %s
                  AND total_records = 0
                  AND loop_count < %s
                  AND status IN ('pending', 'failed') -- Don't pick up 'in_progress' to avoid stealing from active workers
                ORDER BY loop_count ASC, id ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, product, company, url, loop_count, total_records
        """
        try:
            with self.db.cursor(dict_cursor=True) as cur:
                cur.execute(sql, (self.run_id, max_loop, limit))
                results = [dict(row) for row in cur.fetchall()]
                self.db.commit()  # Commit the claim immediately
                if results:
                    self._db_log(f"CLAIMED | worker={worker_id} count={len(results)}")
                return results
        except Exception as e:
            self.db.rollback()
            logger.error(f"[CLAIM_ERROR] Failed to claim products: {e}")
            return []

    def reset_product_status(self, product_id: int, status: str = 'pending', error_msg: str = None):
        """Reset product status (e.g. for requeue)."""
        sql = """
            UPDATE ar_product_index
            SET status = %s,
                worker_id = NULL,
                error_message = %s,
                last_attempt_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """
        try:
            with self.db.cursor() as cur:
                cur.execute(sql, (status, error_msg, product_id))
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            logger.error(f"[RESET_ERROR] Failed to reset product {product_id}: {e}")

    def mark_attempt(
        self,
        product_id: int,
        loop_count: int,
        total_records: int = 0,
        status: str = "in_progress",
        source: str = None,
        error_message: str = None,
    ) -> None:
        sql = """
            UPDATE ar_product_index
            SET loop_count = %s,
                total_records = %s,
                status = %s,
                last_attempt_at = CURRENT_TIMESTAMP,
                last_attempt_source = COALESCE(%s, last_attempt_source),
                error_message = %s,
                scraped_by_selenium = CASE WHEN %s IN ('selenium','selenium_product','selenium_company') THEN TRUE ELSE scraped_by_selenium END,
                scraped_by_api = CASE WHEN %s = 'api' THEN TRUE ELSE scraped_by_api END,
                scrape_source = CASE WHEN %s > 0 THEN COALESCE(%s, scrape_source) ELSE scrape_source END,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s AND run_id = %s
        """
        with self.db.cursor() as cur:
            cur.execute(
                sql,
                (
                    loop_count,
                    total_records,
                    status,
                    source,
                    error_message,
                    source,
                    source,
                    total_records,
                    source,
                    product_id,
                    self.run_id,
                ),
            )

    def mark_attempt_by_name(
        self,
        company: str,
        product: str,
        loop_count: int,
        total_records: int = 0,
        status: str = "in_progress",
        source: str = None,
        error_message: str = None,
    ) -> None:
        """Convenience when product_index id is not available."""
        sql = """
            UPDATE ar_product_index
               SET loop_count = %s,
                   total_records = %s,
                   status = %s,
                   last_attempt_at = CURRENT_TIMESTAMP,
                   last_attempt_source = COALESCE(%s, last_attempt_source),
                   error_message = %s,
                   scraped_by_selenium = CASE WHEN %s IN ('selenium','selenium_product','selenium_company') THEN TRUE ELSE scraped_by_selenium END,
                   scraped_by_api = CASE WHEN %s = 'api' THEN TRUE ELSE scraped_by_api END,
                   scrape_source = CASE WHEN %s > 0 THEN COALESCE(%s, scrape_source) ELSE scrape_source END,
                   updated_at = CURRENT_TIMESTAMP
             WHERE run_id = %s AND company = %s AND product = %s
        """
        with self.db.cursor() as cur:
            cur.execute(
                sql,
                (
                    loop_count,
                    total_records,
                    status,
                    source,
                    error_message,
                    source,
                    source,
                    total_records,
                    source,
                    self.run_id,
                    company,
                    product,
                ),
            )

    def bump_attempt(
        self,
        company: str,
        product: str,
        total_records: int = 0,
        status: str = "in_progress",
        source: str = None,
        error_message: str = None,
    ):
        """Increment loop_count by 1 for company/product."""
        sql = """
            UPDATE ar_product_index
               SET loop_count = COALESCE(loop_count,0) + 1,
                   total_records = %s,
                   status = %s,
                   last_attempt_at = CURRENT_TIMESTAMP,
                   last_attempt_source = COALESCE(%s, last_attempt_source),
                   error_message = %s,
                   scraped_by_selenium = CASE WHEN %s IN ('selenium','selenium_product','selenium_company') THEN TRUE ELSE scraped_by_selenium END,
                   scraped_by_api = CASE WHEN %s = 'api' THEN TRUE ELSE scraped_by_api END,
                   scrape_source = CASE WHEN %s > 0 THEN COALESCE(%s, scrape_source) ELSE scrape_source END,
                   updated_at = CURRENT_TIMESTAMP
             WHERE run_id = %s AND company = %s AND product = %s
        """
        with self.db.cursor() as cur:
            cur.execute(
                sql,
                (
                    total_records,
                    status,
                    source,
                    error_message,
                    source,
                    source,
                    total_records,
                    source,
                    self.run_id,
                    company,
                    product,
                ),
            )

    def mark_api_result(
        self,
        company: str,
        product: str,
        total_records: int,
        status: str = "completed",
        error_message: str = None,
    ) -> None:
        """Update product_index for API results without altering loop_count."""
        sql = """
            UPDATE ar_product_index
               SET total_records = %s,
                   status = %s,
                   last_attempt_at = CURRENT_TIMESTAMP,
                   last_attempt_source = 'api',
                   error_message = %s,
                   scraped_by_api = TRUE,
                   scrape_source = CASE WHEN %s > 0 THEN 'api' ELSE scrape_source END,
                   updated_at = CURRENT_TIMESTAMP
             WHERE run_id = %s AND company = %s AND product = %s
        """
        with self.db.cursor() as cur:
            cur.execute(
                sql,
                (
                    total_records,
                    status,
                    error_message,
                    total_records,
                    self.run_id,
                    company,
                    product,
                ),
            )

    # ------------------------------------------------------------------ #
    # Scraped product rows                                              #
    # ------------------------------------------------------------------ #
    def insert_products(self, rows: Sequence[Dict], source: str = "selenium") -> int:
        """
        Insert scraped rows into ar_products.
        Expected keys: input_company, input_product_name, company, product_name,
                       active_ingredient, therapeutic_class, description,
                       price_ars/price_raw, date, sifar_detail, pami_af, pami_os,
                       ioma_detail, ioma_af, ioma_os, import_status, coverage_json
        """
        if not rows:
            return 0

        def _val(row: Dict, *keys: str):
            """Fetch value from row using multiple possible keys (case variants)."""
            for k in keys:
                if k in row:
                    return row.get(k)
            # try common upper/lower variants
            for k in keys:
                if k.lower() in row:
                    return row.get(k.lower())
                if k.upper() in row:
                    return row.get(k.upper())
            return None

        def _parse_price(value) -> Optional[Decimal]:
            """Robustly coerce Argentina money strings into Decimals (preserve cents)."""
            if value is None:
                return None
            if isinstance(value, (int, float)):
                try:
                    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                except (TypeError, ValueError, InvalidOperation):
                    return None

            s = str(value).strip()
            if not s:
                return None
            s = s.replace("\u00a0", "").replace(" ", "")
            sl = s.lower()
            if sl in {"nan", "none", "null"}:
                return None

            token = re.sub(r"[^\d,.\-]", "", s)
            if not token or token in {".", ",", "-", ""}:
                return None

            negative = token.startswith("-")
            if negative:
                token = token[1:]

            if not token:
                return None

            if "." in token and "," in token:
                if token.rfind(",") > token.rfind("."):
                    token = token.replace(".", "").replace(",", ".")
                else:
                    token = token.replace(",", "")
            elif "," in token:
                token = token.replace(",", ".")

            normalized = token
            try:
                decimal_value = Decimal(normalized)
            except InvalidOperation:
                try:
                    decimal_value = Decimal(normalized.replace(",", ""))
                except InvalidOperation:
                    return None

            if negative:
                decimal_value = -decimal_value

            try:
                return decimal_value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            except InvalidOperation:
                return None

        def _hash_for_row(row: Dict) -> str:
            parts = [
                str(source or ""),
                str(_val(row, "input_company", "Company") or ""),
                str(_val(row, "input_product_name", "Product", "product") or ""),
                str(_val(row, "company", "Company") or ""),
                str(_val(row, "product_name", "Product Name", "product_name") or ""),
                str(_val(row, "active_ingredient", "Active Ingredient", "active") or ""),
                str(_val(row, "therapeutic_class", "Therapeutic Class", "therapeutic") or ""),
                str(_val(row, "description", "Description") or ""),
                str(_val(row, "price_raw", "price_ars_raw", "price") or ""),
                str(_val(row, "date", "Date") or ""),
                str(_val(row, "sifar_detail", "SIFAR_detail") or ""),
                str(_val(row, "pami_af", "PAMI_AF") or ""),
                str(_val(row, "pami_os", "PAMI_OS") or ""),
                str(_val(row, "ioma_detail", "IOMA_detail") or ""),
                str(_val(row, "ioma_af", "IOMA_AF") or ""),
                str(_val(row, "ioma_os", "IOMA_OS") or ""),
                str(_val(row, "import_status", "Import_Status", "import") or ""),
                str(_val(row, "coverage_json", "coverage") or ""),
            ]
            stable = "|".join(p.replace("\r", " ").replace("\n", " ").strip() for p in parts)
            return hashlib.sha1(stable.encode("utf-8", errors="ignore")).hexdigest()

        # Pre-compute all tuples (price parsing happens once per row, not per DB call)
        # Use dict to deduplicate by hash (website may have duplicate presentation data)
        tuples_dict = {}  # hash -> tuple
        for r in rows:
            price_ars = _val(r, "price_ars", "price_ARS", "Price_ARS", "price")
            price_raw = _val(r, "price_raw", "price_ars_raw", "price")
            parsed_price = _parse_price(price_ars)
            if parsed_price is None:
                parsed_price = _parse_price(price_raw)
            fallback_raw = _val(r, "price_ars")
            if price_raw is None and fallback_raw is not None:
                price_raw = fallback_raw
            if parsed_price is None:
                parsed_price = _parse_price(fallback_raw)
            if price_raw is not None:
                if isinstance(price_raw, (int, float, Decimal)):
                    price_raw = f"{price_raw}"
                price_raw_str = str(price_raw).strip()
                if price_raw_str and price_raw_str.lower() not in {"nan", "none", "null"}:
                    price_raw = price_raw_str
                else:
                    price_raw = None
            price_ars = parsed_price

            row_hash = _hash_for_row(r)
            # Deduplicate: keep first occurrence of each hash
            if row_hash not in tuples_dict:
                tuples_dict[row_hash] = (
                    self.run_id,
                    row_hash,
                    _val(r, "input_company", "Company"),
                    _val(r, "input_product_name", "Product", "product"),
                    _val(r, "company", "Company"),
                    _val(r, "product_name", "Product Name", "product_name"),
                    _val(r, "active_ingredient", "Active Ingredient", "active"),
                    _val(r, "therapeutic_class", "Therapeutic Class", "therapeutic"),
                    _val(r, "description", "Description"),
                    price_ars,
                    price_raw,
                    _val(r, "date", "Date"),
                    _val(r, "sifar_detail", "SIFAR_detail"),
                    _val(r, "pami_af", "PAMI_AF"),
                    _val(r, "pami_os", "PAMI_OS"),
                    _val(r, "ioma_detail", "IOMA_detail"),
                    _val(r, "ioma_af", "IOMA_AF"),
                    _val(r, "ioma_os", "IOMA_OS"),
                    _val(r, "import_status", "Import_Status", "import"),
                    _val(r, "coverage_json", "coverage"),
                    source,
                )

        tuples = list(tuples_dict.values())

        BATCH = 500
        inserted = 0
        with self.db.cursor() as cur:
            if _HAS_EXECUTE_VALUES:
                sql_ev = """
                    INSERT INTO ar_products
                    (run_id, record_hash, input_company, input_product_name, company, product_name,
                     active_ingredient, therapeutic_class, description, price_ars, price_raw,
                     date, scraped_at, sifar_detail, pami_af, pami_os, ioma_detail,
                     ioma_af, ioma_os, import_status, coverage_json, source)
                    VALUES %s
                    ON CONFLICT (run_id, record_hash) DO UPDATE SET
                        input_company = EXCLUDED.input_company,
                        input_product_name = EXCLUDED.input_product_name,
                        company = EXCLUDED.company,
                        product_name = EXCLUDED.product_name,
                        active_ingredient = EXCLUDED.active_ingredient,
                        therapeutic_class = EXCLUDED.therapeutic_class,
                        description = EXCLUDED.description,
                        price_ars = EXCLUDED.price_ars,
                        price_raw = EXCLUDED.price_raw,
                        date = EXCLUDED.date,
                        sifar_detail = EXCLUDED.sifar_detail,
                        pami_af = EXCLUDED.pami_af,
                        pami_os = EXCLUDED.pami_os,
                        ioma_detail = EXCLUDED.ioma_detail,
                        ioma_af = EXCLUDED.ioma_af,
                        ioma_os = EXCLUDED.ioma_os,
                        import_status = EXCLUDED.import_status,
                        coverage_json = EXCLUDED.coverage_json,
                        source = EXCLUDED.source,
                        scraped_at = CURRENT_TIMESTAMP
                """
                # Template adds scraped_at = CURRENT_TIMESTAMP via the VALUES row
                tpl = ("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                for i in range(0, len(tuples), BATCH):
                    batch = tuples[i : i + BATCH]
                    execute_values(cur, sql_ev, batch, template=tpl, page_size=BATCH)
                    inserted += len(batch)
            else:
                sql_row = """
                    INSERT INTO ar_products
                    (run_id, record_hash, input_company, input_product_name, company, product_name,
                     active_ingredient, therapeutic_class, description, price_ars, price_raw,
                     date, scraped_at, sifar_detail, pami_af, pami_os, ioma_detail,
                     ioma_af, ioma_os, import_status, coverage_json, source)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (run_id, record_hash) DO UPDATE SET
                        input_company = EXCLUDED.input_company,
                        input_product_name = EXCLUDED.input_product_name,
                        company = EXCLUDED.company,
                        product_name = EXCLUDED.product_name,
                        active_ingredient = EXCLUDED.active_ingredient,
                        therapeutic_class = EXCLUDED.therapeutic_class,
                        description = EXCLUDED.description,
                        price_ars = EXCLUDED.price_ars,
                        price_raw = EXCLUDED.price_raw,
                        date = EXCLUDED.date,
                        sifar_detail = EXCLUDED.sifar_detail,
                        pami_af = EXCLUDED.pami_af,
                        pami_os = EXCLUDED.pami_os,
                        ioma_detail = EXCLUDED.ioma_detail,
                        ioma_af = EXCLUDED.ioma_af,
                        ioma_os = EXCLUDED.ioma_os,
                        import_status = EXCLUDED.import_status,
                        coverage_json = EXCLUDED.coverage_json,
                        source = EXCLUDED.source,
                        scraped_at = CURRENT_TIMESTAMP
                """
                for t in tuples:
                    cur.execute(sql_row, t)
                    inserted += 1
        logger.info("Inserted/updated %d product rows (source=%s)", inserted, source)
        self._db_log(f"OK | ar_products upserted={inserted} source={source} | run_id={self.run_id}")
        return inserted

    def log_error(self, company: str, product: str, message: str) -> None:
        """Log error to ar_errors (all errors must be in DB)."""
        sql = """
            INSERT INTO ar_errors (run_id, input_company, input_product_name, error_message)
            VALUES (%s, %s, %s, %s)
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, company, product, message[:5000] if message else ""))

    def log_artifact(
        self,
        company: str,
        product: str,
        artifact_type: str,
        file_path: str,
    ) -> None:
        """Log artifact (e.g. screenshot_before_api) to ar_artifacts - all artifacts in DB."""
        sql = """
            INSERT INTO ar_artifacts (run_id, input_company, input_product_name, artifact_type, file_path)
            VALUES (%s, %s, %s, %s, %s)
        """
        with self.db.cursor() as cur:
            cur.execute(
                sql,
                (self.run_id, company, product, artifact_type, file_path),
            )
        self._db_log(f"OK | ar_artifacts logged | type={artifact_type} path={file_path}")

    def is_product_already_scraped(self, company: str, product: str) -> bool:
        """Check if a product already has data (total_records > 0 in ar_product_index)."""
        try:
            with self.db.cursor() as cur:
                cur.execute(
                    "SELECT total_records FROM ar_product_index WHERE run_id=%s AND company=%s AND product=%s",
                    (self.run_id, company, product),
                )
                row = cur.fetchone()
                if not row:
                    return False
                val = row[0] if isinstance(row, tuple) else row.get("total_records")
                return (val or 0) > 0
        except Exception:
            return False

    def combine_skip_sets(self) -> Set[Tuple[str, str]]:
        """Combine skip sources from DB in a single query for speed."""
        skip_set: Set[Tuple[str, str]] = set()
        output_count = progress_count = 0
        try:
            with self.db.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT input_company AS company, input_product_name AS product, 'output' AS src
                      FROM ar_products
                     WHERE run_id = %s
                    UNION
                    SELECT company, product, 'progress' AS src
                      FROM ar_product_index
                     WHERE run_id = %s
                       AND (COALESCE(total_records,0) > 0 OR status = 'completed')
                    """,
                    (self.run_id, self.run_id),
                )
                for row in cur.fetchall():
                    c = (row[0] or "") if isinstance(row, tuple) else (row.get("company") or "")
                    p = (row[1] or "") if isinstance(row, tuple) else (row.get("product") or "")
                    src = row[2] if isinstance(row, tuple) else (row.get("src") or "")
                    
                    # Normalize keys using core nk
                    key = (nk(c), nk(p))
                    if key[0] and key[1]:
                        skip_set.add(key)
                        if src == "output":
                            output_count += 1
                        elif src == "progress":
                            progress_count += 1
        except Exception as e:
            logger.warning(f"[SKIP_SET] Combined query failed: {e}")
            # Fallback not implemented because direct DB access should work or fail
            return set()
            
        logger.info(f"[SKIP_SET] Loaded skip_set size = {len(skip_set)} (output={output_count}, progress={progress_count})")
        return skip_set

    # ------------------------------------------------------------------ #
    # Translation (ar_products_translated)                               #
    # ------------------------------------------------------------------ #
    def clear_translated(self) -> None:
        with self.db.cursor() as cur:
            cur.execute("DELETE FROM ar_products_translated WHERE run_id = %s", (self.run_id,))
        self._db_log(f"RESET | ar_products_translated cleared | run_id={self.run_id}")

    def insert_translated(self, rows: Sequence[Dict]) -> int:
        """Insert translated rows into ar_products_translated.
        Uses execute_values for bulk inserts."""
        if not rows:
            return 0

        tuples = [
            (
                self.run_id,
                r.get("product_id"),
                r.get("company"),
                r.get("product_name"),
                r.get("active_ingredient"),
                r.get("therapeutic_class"),
                r.get("description"),
                r.get("price_ars"),
                r.get("date"),
                r.get("sifar_detail"),
                r.get("pami_af"),
                r.get("pami_os"),
                r.get("ioma_detail"),
                r.get("ioma_af"),
                r.get("ioma_os"),
                r.get("import_status"),
                r.get("coverage_json"),
                r.get("translation_source"),
            )
            for r in rows
        ]

        BATCH = 500
        inserted = 0
        with self.db.cursor() as cur:
            if _HAS_EXECUTE_VALUES:
                sql_ev = """
                    INSERT INTO ar_products_translated
                    (run_id, product_id, company, product_name, active_ingredient,
                     therapeutic_class, description, price_ars, date,
                     sifar_detail, pami_af, pami_os, ioma_detail, ioma_af, ioma_os,
                     import_status, coverage_json, translation_source)
                    VALUES %s
                    ON CONFLICT (run_id, product_id) DO UPDATE SET
                        company = EXCLUDED.company,
                        product_name = EXCLUDED.product_name,
                        active_ingredient = EXCLUDED.active_ingredient,
                        therapeutic_class = EXCLUDED.therapeutic_class,
                        description = EXCLUDED.description,
                        price_ars = EXCLUDED.price_ars,
                        date = EXCLUDED.date,
                        sifar_detail = EXCLUDED.sifar_detail,
                        pami_af = EXCLUDED.pami_af,
                        pami_os = EXCLUDED.pami_os,
                        ioma_detail = EXCLUDED.ioma_detail,
                        ioma_af = EXCLUDED.ioma_af,
                        ioma_os = EXCLUDED.ioma_os,
                        import_status = EXCLUDED.import_status,
                        coverage_json = EXCLUDED.coverage_json,
                        translation_source = EXCLUDED.translation_source,
                        translated_at = CURRENT_TIMESTAMP
                """
                for i in range(0, len(tuples), BATCH):
                    batch = tuples[i : i + BATCH]
                    execute_values(cur, sql_ev, batch, page_size=BATCH)
                    inserted += len(batch)
            else:
                sql_row = """
                    INSERT INTO ar_products_translated
                    (run_id, product_id, company, product_name, active_ingredient,
                     therapeutic_class, description, price_ars, date,
                     sifar_detail, pami_af, pami_os, ioma_detail, ioma_af, ioma_os,
                     import_status, coverage_json, translation_source)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (run_id, product_id) DO UPDATE SET
                        company = EXCLUDED.company,
                        product_name = EXCLUDED.product_name,
                        active_ingredient = EXCLUDED.active_ingredient,
                        therapeutic_class = EXCLUDED.therapeutic_class,
                        description = EXCLUDED.description,
                        price_ars = EXCLUDED.price_ars,
                        date = EXCLUDED.date,
                        sifar_detail = EXCLUDED.sifar_detail,
                        pami_af = EXCLUDED.pami_af,
                        pami_os = EXCLUDED.pami_os,
                        ioma_detail = EXCLUDED.ioma_detail,
                        ioma_af = EXCLUDED.ioma_af,
                        ioma_os = EXCLUDED.ioma_os,
                        import_status = EXCLUDED.import_status,
                        coverage_json = EXCLUDED.coverage_json,
                        translation_source = EXCLUDED.translation_source,
                        translated_at = CURRENT_TIMESTAMP
                """
                for t in tuples:
                    cur.execute(sql_row, t)
                    inserted += 1
        self._db_log(f"OK | ar_products_translated upserted={inserted} | run_id={self.run_id}")
        return inserted

    # ------------------------------------------------------------------ #
    # Dictionary + PCID                                                 #
    # ------------------------------------------------------------------ #
    def replace_dictionary(self, entries: Iterable[Dict[str, str]]) -> int:
        """Replace ar_dictionary with provided ES->EN entries."""
        entry_list = list(entries)
        # Deduplicate by 'es' so ON CONFLICT DO UPDATE does not affect the same row twice in one command
        by_es: Dict[str, Dict[str, str]] = {}
        for e in entry_list:
            by_es[e.get("es", "")] = e
        entry_list = list(by_es.values())
        with self.db.cursor() as cur:
            cur.execute("TRUNCATE ar_dictionary")
        if not entry_list:
            return 0
        tuples = [(e.get("es"), e.get("en"), e.get("source", "file")) for e in entry_list]
        BATCH = 500
        inserted = 0
        with self.db.cursor() as cur:
            if _HAS_EXECUTE_VALUES:
                sql_ev = """
                    INSERT INTO ar_dictionary (es, en, source) VALUES %s
                    ON CONFLICT (es) DO UPDATE SET en = EXCLUDED.en, source = EXCLUDED.source,
                        updated_at = CURRENT_TIMESTAMP
                """
                for i in range(0, len(tuples), BATCH):
                    batch = tuples[i : i + BATCH]
                    execute_values(cur, sql_ev, batch, page_size=BATCH)
                    inserted += len(batch)
            else:
                sql_row = """
                    INSERT INTO ar_dictionary (es, en, source) VALUES (%s, %s, %s)
                    ON CONFLICT (es) DO UPDATE SET en = EXCLUDED.en, source = EXCLUDED.source,
                        updated_at = CURRENT_TIMESTAMP
                """
                for t in tuples:
                    cur.execute(sql_row, t)
                    inserted += 1
        self._db_log(f"OK | ar_dictionary replaced={inserted}")
        return inserted

    def upsert_dictionary_entries(self, entries: Iterable[Dict[str, str]]) -> int:
        """Upsert ES->EN dictionary entries without truncating the table."""
        entry_list = list(entries)
        # Deduplicate by 'es' to avoid CardinalityViolation in ON CONFLICT DO UPDATE
        by_es: Dict[str, Dict[str, str]] = {}
        for e in entry_list:
            by_es[e.get("es", "")] = e
        entry_list = list(by_es.values())
        if not entry_list:
            return 0
        tuples = [(e.get("es"), e.get("en"), e.get("source", "manual")) for e in entry_list]
        BATCH = 500
        inserted = 0
        with self.db.cursor() as cur:
            if _HAS_EXECUTE_VALUES:
                sql_ev = """
                    INSERT INTO ar_dictionary (es, en, source) VALUES %s
                    ON CONFLICT (es) DO UPDATE SET en = EXCLUDED.en, source = EXCLUDED.source,
                        updated_at = CURRENT_TIMESTAMP
                """
                for i in range(0, len(tuples), BATCH):
                    batch = tuples[i : i + BATCH]
                    execute_values(cur, sql_ev, batch, page_size=BATCH)
                    inserted += len(batch)
            else:
                sql_row = """
                    INSERT INTO ar_dictionary (es, en, source) VALUES (%s, %s, %s)
                    ON CONFLICT (es) DO UPDATE SET en = EXCLUDED.en, source = EXCLUDED.source,
                        updated_at = CURRENT_TIMESTAMP
                """
                for t in tuples:
                    cur.execute(sql_row, t)
                    inserted += 1
        self._db_log(f"OK | ar_dictionary upserted={inserted}")
        return inserted

    # Argentina uses the shared pcid_mapping table (source_country='Argentina') as the single source.
    # GUI Input page and Step 0 both read/write this table.
    _PCID_SOURCE_COUNTRY = "Argentina"

    def replace_pcid_reference(self, rows: Iterable[Dict[str, str]]) -> int:
        """Replace Argentina rows in shared pcid_mapping table (single source for GUI + pipeline)."""
        row_list = list(rows)
        with self.db.cursor() as cur:
            cur.execute("DELETE FROM pcid_mapping WHERE source_country = %s", (self._PCID_SOURCE_COUNTRY,))
        if not row_list:
            return 0
        tuples = [
            (
                r.get("pcid") or r.get("PCID"),
                r.get("company") or r.get("Company"),
                r.get("local_product_name") or r.get("Local Product Name"),
                r.get("generic_name") or r.get("Generic Name"),
                r.get("local_pack_description") or r.get("Local Pack Description"),
                self._PCID_SOURCE_COUNTRY,
            )
            for r in row_list
        ]
        BATCH = 500
        inserted = 0
        with self.db.cursor() as cur:
            if _HAS_EXECUTE_VALUES:
                sql_ev = """
                    INSERT INTO pcid_mapping
                    (pcid, company, local_product_name, generic_name, local_pack_description, source_country)
                    VALUES %s
                """
                for i in range(0, len(tuples), BATCH):
                    batch = tuples[i : i + BATCH]
                    execute_values(cur, sql_ev, batch, page_size=BATCH)
                    inserted += len(batch)
            else:
                sql_row = """
                    INSERT INTO pcid_mapping
                    (pcid, company, local_product_name, generic_name, local_pack_description, source_country)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                for t in tuples:
                    cur.execute(sql_row, t)
                    inserted += 1
        self._db_log(f"OK | pcid_mapping (Argentina) replaced={inserted}")
        return inserted

    def log_export_report(self, report_type: str, file_path: str, row_count: Optional[int]) -> None:
        sql = """
            INSERT INTO ar_export_reports (run_id, report_type, file_path, row_count)
            VALUES (%s, %s, %s, %s)
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, report_type, file_path, row_count))
        self._db_log(f"OK | ar_export_reports type={report_type} rows={row_count}")

    def log_input_upload(
        self,
        table_name: str,
        source_file: str,
        row_count: int,
        replaced_previous: int = 0,
        uploaded_by: str = "pipeline",
    ) -> None:
        """Log input file loads (dictionary/PCID) into common input_uploads table."""
        sql = """
            INSERT INTO input_uploads
            (table_name, source_file, row_count, replaced_previous, uploaded_by)
            VALUES (%s, %s, %s, %s, %s)
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (table_name, source_file, row_count, replaced_previous, uploaded_by))
        self._db_log(f"OK | input_uploads table={table_name} rows={row_count}")

    def get_latest_input_upload(self, table_name: str) -> Optional[Dict]:
        sql = """
            SELECT table_name, source_file, row_count, uploaded_at, replaced_previous, uploaded_by
              FROM input_uploads
             WHERE table_name = %s
             ORDER BY uploaded_at DESC
             LIMIT 1
        """
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(sql, (table_name,))
            row = cur.fetchone()
            return dict(row) if row else None

    # ------------------------------------------------------------------ #
    # Scrape Stats Snapshots                                            #
    # ------------------------------------------------------------------ #
    def snapshot_scrape_stats(self, event_type: str, tor_ip: str = None) -> None:
        """Insert a progress snapshot only if counts changed since last snapshot."""
        self.db.execute("""
            INSERT INTO ar_scrape_stats (run_id, event_type, total_combinations, with_records, zero_records, tor_ip)
            SELECT %s, %s, cur.total, cur.wr, cur.zr, %s
            FROM (
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE total_records > 0) AS wr,
                       COUNT(*) FILTER (WHERE total_records = 0) AS zr
                FROM ar_product_index WHERE run_id = %s
            ) cur
            WHERE NOT EXISTS (
                SELECT 1 FROM ar_scrape_stats prev
                WHERE prev.run_id = %s
                  AND prev.total_combinations = cur.total
                  AND prev.with_records = cur.wr
                  AND prev.zero_records = cur.zr
                ORDER BY prev.id DESC LIMIT 1
            )
        """, (self.run_id, event_type, tor_ip, self.run_id, self.run_id))

    # ------------------------------------------------------------------ #
    # Translation Cache (delegates to core.translation)                 #
    # ------------------------------------------------------------------ #
    # Note: These methods now delegate to core.translation.TranslationCache
    # for unified caching across all scrapers. The underlying table schema
    # remains compatible (legacy schema with source_text as unique key).
    
    def _get_translation_cache(self):
        """Lazy initialization of unified translation cache."""
        if not hasattr(self, '_translation_cache'):
            # Import here to avoid circular dependency
            import sys
            from pathlib import Path
            repo_root = Path(__file__).resolve().parents[3]
            if str(repo_root) not in sys.path:
                sys.path.insert(0, str(repo_root))
            from core.translation import get_cache
            self._translation_cache = get_cache("argentina")
        return self._translation_cache
    
    def get_translation_cache(self, source_lang: str = 'es', target_lang: str = 'en') -> Dict[str, str]:
        """Load all translation cache entries from DB.
        
        DEPRECATED: Use get_cached_translation() for individual lookups.
        Loading entire cache is memory-intensive.
        """
        cache = {}
        try:
            sql = """
                SELECT source_text, translated_text
                FROM ar_translation_cache
                WHERE source_language = %s AND target_language = %s
            """
            with self.db.cursor() as cur:
                cur.execute(sql, (source_lang, target_lang))
                for row in cur.fetchall():
                    cache[row[0]] = row[1]
        except Exception as e:
            print(f"[WARNING] Failed to load translation cache from DB: {e}")
        return cache

    def save_translation_cache(self, cache: Dict[str, str], source_lang: str = 'es', target_lang: str = 'en') -> None:
        """Save translation cache entries to DB (upsert).
        
        DEPRECATED: Use save_single_translation() or unified cache directly.
        """
        if not cache:
            return
        tcache = self._get_translation_cache()
        count = 0
        for source_text, translated_text in cache.items():
            if tcache.set(source_text, translated_text, source_lang, target_lang):
                count += 1
        print(f"[OK] Saved {count}/{len(cache)} translations to cache")

    def get_cached_translation(self, source_text: str, source_lang: str = 'es', target_lang: str = 'en') -> Optional[str]:
        """Get a single cached translation using unified cache."""
        return self._get_translation_cache().get(source_text, source_lang, target_lang)

    def save_single_translation(self, source_text: str, translated_text: str, source_lang: str = 'es', target_lang: str = 'en') -> None:
        """Save a single translation to cache using unified cache."""
        self._get_translation_cache().set(source_text, translated_text, source_lang, target_lang)

    # ------------------------------------------------------------------ #
    # Stats                                                             #
    # ------------------------------------------------------------------ #
    def get_stats(self) -> Dict:
        """Fetch all pipeline stats in a single round-trip query."""
        sql = """
            SELECT
                (SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s),
                (SELECT COUNT(*) FROM ar_products WHERE run_id = %s),
                (SELECT COUNT(*) FROM ar_products_translated WHERE run_id = %s),
                (SELECT COALESCE(SUM(row_count), 0) FROM ar_export_reports
                  WHERE run_id = %s AND report_type = 'pcid_mapping'),
                (SELECT COALESCE(SUM(row_count), 0) FROM ar_export_reports
                  WHERE run_id = %s AND report_type = 'pcid_missing')
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id,) * 5)
            row = cur.fetchone()
        return {
            "product_index": row[0],
            "products": row[1],
            "translated": row[2],
            "pcid_mapped": row[3],
            "pcid_not_mapped": row[4],
        }
