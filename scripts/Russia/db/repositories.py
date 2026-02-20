#!/usr/bin/env python3
"""
Russia database repository - all DB access in one place.

Provides methods for:
- Inserting/querying VED products, excluded products, translated data
- Sub-step progress tracking (page-level resume)
- Failed pages tracking for retry mechanism
- Run lifecycle management
"""

import logging
from typing import Dict, List, Optional, Set
from datetime import datetime

from core.db.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class RussiaRepository(BaseRepository):
    """All database operations for Russia scraper (PostgreSQL backend)."""

    SCRAPER_NAME = "Russia"
    TABLE_PREFIX = "ru"

    def __init__(self, db, run_id: str):
        """
        Initialize repository.

        Args:
            db: PostgresDB instance
            run_id: Current run ID
        """
        super().__init__(db, run_id)

    # ------------------------------------------------------------------
    # Input tables (ru_input_dictionary for translation)
    # ------------------------------------------------------------------

    def get_translation_dictionary_rows(self) -> List[tuple]:
        """
        Load translation dictionary from ru_input_dictionary table.
        Returns list of (source_term, translated_term) for RU->EN lookup.
        """
        try:
            sql = """
                SELECT source_term, translated_term
                FROM ru_input_dictionary
                WHERE source_term IS NOT NULL AND source_term != ''
                  AND translated_term IS NOT NULL AND translated_term != ''
            """
            with self.db.cursor() as cur:
                cur.execute(sql)
                return [(row[0], row[1]) for row in cur.fetchall()]
        except Exception as e:
            logger.warning("ru_input_dictionary not available: %s", e)
            return []

    # ------------------------------------------------------------------
    # Step progress (sub-step resume)
    # ------------------------------------------------------------------

    def mark_progress(self, step_number: int, step_name: str,
                      progress_key: str, status: str,
                      error_message: str = None, log_details: str = None,
                      metrics: dict = None, url: str = None) -> None:
        """Mark a sub-step progress item with optional metrics and URL."""
        now = datetime.now()
        
        # Default metrics
        metrics = metrics or {}
        rows_found = metrics.get('rows_found', 0)
        ean_found = metrics.get('ean_found', 0)
        rows_scraped = metrics.get('rows_scraped', 0)
        rows_inserted = metrics.get('rows_inserted', 0)
        ean_missing = metrics.get('ean_missing', 0)
        db_count_before = metrics.get('db_count_before', 0)
        db_count_after = metrics.get('db_count_after', 0)
        
        sql = """
            INSERT INTO ru_step_progress
            (run_id, step_number, step_name, progress_key, status,
             error_message, log_details, url, rows_found, ean_found, rows_scraped,
             rows_inserted, ean_missing, db_count_before, db_count_after,
             started_at, completed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                step_name = EXCLUDED.step_name,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                log_details = COALESCE(EXCLUDED.log_details, ru_step_progress.log_details),
                url = COALESCE(EXCLUDED.url, ru_step_progress.url),
                rows_found = EXCLUDED.rows_found,
                ean_found = EXCLUDED.ean_found,
                rows_scraped = EXCLUDED.rows_scraped,
                rows_inserted = EXCLUDED.rows_inserted,
                ean_missing = EXCLUDED.ean_missing,
                db_count_before = EXCLUDED.db_count_before,
                db_count_after = EXCLUDED.db_count_after,
                started_at = CASE
                    WHEN EXCLUDED.status = 'in_progress' THEN EXCLUDED.started_at
                    WHEN ru_step_progress.started_at IS NULL THEN EXCLUDED.started_at
                    ELSE ru_step_progress.started_at
                END,
                completed_at = CASE
                    WHEN EXCLUDED.status IN ('completed', 'failed', 'skipped') THEN EXCLUDED.completed_at
                    WHEN EXCLUDED.status = 'in_progress' THEN NULL
                    ELSE ru_step_progress.completed_at
                END
        """
        
        # Always set started_at to now for new records, so even if we complete directly,
        # started_at will be set. For existing records, the ON CONFLICT logic handles it.
        started_at = now
        completed_at = now if status in ('completed', 'failed', 'skipped') else None
        
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, step_number, step_name, progress_key,
                            status, error_message, log_details, url,
                            rows_found, ean_found, rows_scraped, rows_inserted,
                            ean_missing, db_count_before, db_count_after,
                            started_at, completed_at))
        
        self._db_log(f"PROGRESS | step={step_number} key={progress_key} status={status}")

    def get_completed_keys(self, step_number: int) -> Set[str]:
        """Get set of completed progress keys for a step."""
        sql = """
            SELECT progress_key FROM ru_step_progress
            WHERE run_id = %s AND step_number = %s AND status = 'completed'
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, step_number))
            return {row[0] for row in cur.fetchall()}

    def get_failed_keys(self, step_number: int) -> List[Dict]:
        """Get list of failed progress keys for retry."""
        sql = """
            SELECT progress_key, error_message FROM ru_step_progress
            WHERE run_id = %s AND step_number = %s AND status = 'failed'
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, step_number))
            return [{"key": row[0], "error": row[1]} for row in cur.fetchall()]

    def get_completed_keys_for_run(self, step_number: int, run_id: str) -> Set[str]:
        """Get set of completed progress keys for a specific run_id."""
        sql = """
            SELECT progress_key FROM ru_step_progress
            WHERE run_id = %s AND step_number = %s AND status = 'completed'
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (run_id, step_number))
            return {row[0] for row in cur.fetchall()}

    def get_all_run_completed_pages(self, step_number: int, exclude_run_id: str = None) -> Dict[str, int]:
        """Get count of completed pages for all runs."""
        # Determine page key pattern based on step number
        # Step 1 = VED (ved_page:), Step 2 = Excluded (excluded_page:)
        if step_number == 1:
            page_pattern = 'ved_page:%'
        elif step_number == 2:
            page_pattern = 'excluded_page:%'
        else:
            page_pattern = '%_page:%'
        
        if exclude_run_id:
            sql = """
                SELECT run_id, COUNT(*) as page_count 
                FROM ru_step_progress
                WHERE step_number = %s 
                AND status = 'completed'
                AND progress_key LIKE %s
                AND run_id != %s
                GROUP BY run_id
            """
            params = (step_number, page_pattern, exclude_run_id)
        else:
            sql = """
                SELECT run_id, COUNT(*) as page_count 
                FROM ru_step_progress
                WHERE step_number = %s 
                AND status = 'completed'
                AND progress_key LIKE %s
                GROUP BY run_id
            """
            params = (step_number, page_pattern)
        
        with self.db.cursor() as cur:
            cur.execute(sql, params)
            return {row[0]: row[1] for row in cur.fetchall()}

    def get_latest_run_id(self) -> Optional[str]:
        """Get the latest run_id for this scraper."""
        sql = """
            SELECT run_id FROM run_ledger
            WHERE status IN ('completed', 'stopped', 'resumed', 'resume', 'running')
            ORDER BY started_at DESC
            LIMIT 1
        """
        with self.db.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] if row else None

    def get_latest_run_id_excluding(self, exclude_run_id: str = None) -> Optional[str]:
        """Get the latest run_id excluding a specific run_id."""
        if exclude_run_id:
            sql = """
                SELECT run_id FROM run_ledger
                WHERE status IN ('completed', 'stopped', 'resumed', 'resume', 'running')
                AND run_id != %s
                ORDER BY started_at DESC
                LIMIT 1
            """
            params = (exclude_run_id,)
        else:
            sql = """
                SELECT run_id FROM run_ledger
                WHERE status IN ('completed', 'stopped', 'resumed', 'resume', 'running')
                ORDER BY started_at DESC
                LIMIT 1
            """
            params = ()
        
        with self.db.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if row else None

    # ------------------------------------------------------------------
    # VED Products (Step 1)
    # ------------------------------------------------------------------

    def insert_ved_product(self, product: Dict) -> None:
        """Insert a single VED product (no dedup; same item_id can appear on multiple pages)."""
        sql = """
            INSERT INTO ru_ved_products
            (run_id, item_id, tn, inn, manufacturer_country, release_form, ean,
             registered_price_rub, start_date_text, page_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id,
                product.get('item_id'),
                product.get('tn'),
                product.get('inn'),
                product.get('manufacturer_country'),
                product.get('release_form'),
                product.get('ean'),
                product.get('registered_price_rub'),
                product.get('start_date_text'),
                product.get('page_number')
            ))

    def insert_ved_products_bulk(self, products: List[Dict]) -> int:
        """Insert multiple VED products."""
        if not products:
            return 0
        
        count = 0
        for product in products:
            self.insert_ved_product(product)
            count += 1
        
        self._db_log(f"INSERT | {count} VED products")
        return count

    def get_ved_product_count(self) -> int:
        """Get count of VED products for this run."""
        return self.get_ved_product_count_for_run(self.run_id)

    def get_ved_product_count_for_run(self, run_id: str) -> int:
        """Get count of VED products for a specific run_id."""
        if not run_id:
            return 0
        sql = "SELECT COUNT(*) FROM ru_ved_products WHERE run_id = %s"
        with self.db.cursor() as cur:
            cur.execute(sql, (run_id,))
            return cur.fetchone()[0] or 0

    def get_ved_product_count_for_page(self, page_number: int) -> int:
        """Get count of VED products for a specific page in this run."""
        sql = "SELECT COUNT(*) FROM ru_ved_products WHERE run_id = %s AND page_number = %s"
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, page_number))
            return cur.fetchone()[0] or 0

    def get_ved_products(self) -> List[Dict]:
        """Get all VED products for this run."""
        return self.get_ved_products_for_run(self.run_id)

    def get_ved_products_for_run(self, run_id: str) -> List[Dict]:
        """Get all VED products for a specific run_id (for Process/Translate when pipeline run has no VED data)."""
        sql = """
            SELECT item_id, tn, inn, manufacturer_country, release_form, ean,
                   registered_price_rub, start_date_text
            FROM ru_ved_products WHERE run_id = %s
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (run_id,))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def delete_ved_products_for_page(self, page_number: int) -> int:
        """Delete VED products for a specific page (to prevent duplicates on retry)."""
        sql = "DELETE FROM ru_ved_products WHERE run_id = %s AND page_number = %s"
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, page_number))
            return cur.rowcount

    def get_best_ved_run_id(self) -> Optional[str]:
        """Return run_id that has the most VED products (for Process/Translate when current run has 0)."""
        sql = """
            SELECT run_id FROM ru_ved_products
            GROUP BY run_id ORDER BY COUNT(*) DESC LIMIT 1
        """
        with self.db.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] if row else None

    def get_existing_item_ids(self) -> Set[str]:
        """Get set of existing item_ids for deduplication (current run only)."""
        sql = "SELECT item_id FROM ru_ved_products WHERE run_id = %s"
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id,))
            return {row[0] for row in cur.fetchall()}

    def get_all_existing_item_ids(self) -> Set[str]:
        """Get set of existing item_ids from ALL runs for deduplication."""
        sql = "SELECT DISTINCT item_id FROM ru_ved_products"
        with self.db.cursor() as cur:
            cur.execute(sql)
            return {row[0] for row in cur.fetchall()}
    
    def item_id_exists(self, item_id: str) -> bool:
        """Check if item_id exists in current run (fast DB check)."""
        sql = "SELECT 1 FROM ru_ved_products WHERE run_id = %s AND item_id = %s LIMIT 1"
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, item_id))
            return cur.fetchone() is not None

    # ------------------------------------------------------------------
    # Excluded Products (Step 2)
    # ------------------------------------------------------------------

    def insert_excluded_product(self, product: Dict) -> None:
        """Insert a single excluded product. No dedup - data as on website."""
        sql = """
            INSERT INTO ru_excluded_products
            (run_id, item_id, tn, inn, manufacturer_country, release_form, ean,
             registered_price_rub, start_date_text, page_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id,
                product.get('item_id'),
                product.get('tn'),
                product.get('inn'),
                product.get('manufacturer_country'),
                product.get('release_form'),
                product.get('ean'),
                product.get('registered_price_rub'),
                product.get('start_date_text'),
                product.get('page_number')
            ))

    def insert_excluded_products_bulk(self, products: List[Dict]) -> int:
        """Insert multiple excluded products."""
        if not products:
            return 0
        
        count = 0
        for product in products:
            self.insert_excluded_product(product)
            count += 1
        
        self._db_log(f"INSERT | {count} excluded products")
        return count

    def get_excluded_product_count(self) -> int:
        """Get count of excluded products for this run."""
        return self.get_excluded_product_count_for_run(self.run_id)

    def get_excluded_product_count_for_run(self, run_id: str) -> int:
        """Get count of excluded products for a specific run_id."""
        if not run_id:
            return 0
        sql = "SELECT COUNT(*) FROM ru_excluded_products WHERE run_id = %s"
        with self.db.cursor() as cur:
            cur.execute(sql, (run_id,))
            return cur.fetchone()[0] or 0

    def get_excluded_products(self) -> List[Dict]:
        """Get all excluded products for this run."""
        return self.get_excluded_products_for_run(self.run_id)

    def get_excluded_products_for_run(self, run_id: str) -> List[Dict]:
        """Get all excluded products for a specific run_id (for Process/Translate when pipeline run has no Excluded data)."""
        sql = """
            SELECT item_id, tn, inn, manufacturer_country, release_form, ean,
                   registered_price_rub, start_date_text
            FROM ru_excluded_products WHERE run_id = %s
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (run_id,))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def delete_excluded_products_for_page(self, page_number: int) -> int:
        """Delete excluded products for a specific page (to prevent duplicates on retry)."""
        sql = "DELETE FROM ru_excluded_products WHERE run_id = %s AND page_number = %s"
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, page_number))
            return cur.rowcount

    def get_best_excluded_run_id(self) -> Optional[str]:
        """Return run_id that has the most excluded products (for Process/Translate when current run has 0)."""
        sql = """
            SELECT run_id FROM ru_excluded_products
            GROUP BY run_id ORDER BY COUNT(*) DESC LIMIT 1
        """
        with self.db.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] if row else None

    # ------------------------------------------------------------------
    # Atomic Page Claiming (for parallel workers)
    # ------------------------------------------------------------------

    def claim_next_page(self, last_page: int, step_number: int = 1,
                        source_type: str = "ved") -> Optional[int]:
        """
        Atomically claim the next unclaimed page for a worker.

        Uses INSERT ... ON CONFLICT DO NOTHING to handle the race where
        two workers try to claim the same page simultaneously.  If a
        conflict occurs (no row returned), retries up to 5 times to
        claim the next available page.

        Returns: page number claimed, or None if all pages are done.
        """
        import time as _time

        prefix = f"{source_type}_page"
        step_name = "VED Scrape" if source_type == "ved" else "Excluded Scrape"
        # Skip pages that are completed/ean_missing/skipped (done).
        # DO re-claim pages stuck as 'in_progress' from a crashed/paused run
        # (ON CONFLICT updates their started_at so they get re-scraped).
        sql = """
            INSERT INTO ru_step_progress
                (run_id, step_number, step_name, progress_key, status, started_at)
            SELECT %s, %s, %s, %s || ':' || pg.num, 'in_progress', NOW()
            FROM (
                SELECT num FROM generate_series(1, %s) AS gs(num)
                WHERE NOT EXISTS (
                    SELECT 1 FROM ru_step_progress sp
                    WHERE sp.run_id = %s AND sp.step_number = %s
                      AND sp.progress_key = %s || ':' || gs.num
                      AND sp.status IN ('completed', 'ean_missing', 'skipped')
                )
                AND NOT EXISTS (
                    SELECT 1 FROM ru_failed_pages fp
                    WHERE fp.run_id = %s AND fp.page_number = gs.num
                      AND fp.source_type = %s AND fp.status = 'pending'
                )
                ORDER BY gs.num
                LIMIT 1
            ) pg
            ON CONFLICT (run_id, step_number, progress_key)
                DO UPDATE SET status = 'in_progress', started_at = NOW()
                WHERE ru_step_progress.status = 'in_progress'
            RETURNING split_part(progress_key, ':', 2)::int
        """
        params = (
            self.run_id, step_number, step_name, prefix,
            last_page,
            self.run_id, step_number, prefix,
            self.run_id, source_type
        )

        # Retry a few times in case of race-condition conflict
        for attempt in range(5):
            with self.db.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                if row:
                    return row[0]
            # No row → either all done OR race conflict.  Brief pause then retry.
            _time.sleep(0.05 * (attempt + 1))

        return None

    # ------------------------------------------------------------------
    # Failed Pages (Step 3 - Retry mechanism)
    # ------------------------------------------------------------------

    def record_failed_page(self, page_number: int, source_type: str, error_message: str) -> None:
        """Record a failed page for retry."""
        sql = """
            INSERT INTO ru_failed_pages
            (run_id, page_number, source_type, error_message, status)
            VALUES (%s, %s, %s, %s, 'pending')
            ON CONFLICT (run_id, page_number, source_type) DO UPDATE SET
                error_message = EXCLUDED.error_message,
                retry_count = ru_failed_pages.retry_count + 1,
                last_retry_at = CURRENT_TIMESTAMP,
                status = 'pending'
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, page_number, source_type, error_message))

    def get_failed_pages(self, source_type: str = None) -> List[Dict]:
        """Get pending failed pages for retry."""
        if source_type:
            sql = """
                SELECT page_number, source_type, retry_count
                FROM ru_failed_pages
                WHERE run_id = %s AND source_type = %s AND status = 'pending'
                ORDER BY page_number
            """
            params = (self.run_id, source_type)
        else:
            sql = """
                SELECT page_number, source_type, retry_count
                FROM ru_failed_pages
                WHERE run_id = %s AND status = 'pending'
                ORDER BY source_type, page_number
            """
            params = (self.run_id,)
        
        with self.db.cursor() as cur:
            cur.execute(sql, params)
            return [
                {"page_number": row[0], "source_type": row[1], "retry_count": row[2]}
                for row in cur.fetchall()
            ]

    def mark_failed_page_resolved(self, page_number: int, source_type: str) -> None:
        """Mark a failed page as resolved after successful retry."""
        sql = """
            UPDATE ru_failed_pages
            SET status = 'resolved', last_retry_at = CURRENT_TIMESTAMP
            WHERE run_id = %s AND page_number = %s AND source_type = %s
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, page_number, source_type))

    # ------------------------------------------------------------------
    # Translated Products (Step 3)
    # ------------------------------------------------------------------

    # DB allows only translation_method IN ('dictionary', 'ai', 'none')
    _ALLOWED_TRANSLATION_METHODS = frozenset(('dictionary', 'ai', 'none'))

    def insert_translated_product(self, product: Dict) -> None:
        """Insert a translated product. Normalizes translation_method for DB constraint."""
        raw_method = product.get('translation_method', 'none')
        translation_method = (
            raw_method if raw_method in self._ALLOWED_TRANSLATION_METHODS else 'dictionary'
        )
        sql = """
            INSERT INTO ru_translated_products
            (run_id, item_id, tn_ru, tn_en, inn_ru, inn_en,
             manufacturer_country_ru, manufacturer_country_en,
             release_form_ru, release_form_en, ean, registered_price_rub,
             start_date_text, start_date_iso, translation_method)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, item_id) DO UPDATE SET
                tn_ru = EXCLUDED.tn_ru,
                tn_en = EXCLUDED.tn_en,
                inn_ru = EXCLUDED.inn_ru,
                inn_en = EXCLUDED.inn_en,
                manufacturer_country_ru = EXCLUDED.manufacturer_country_ru,
                manufacturer_country_en = EXCLUDED.manufacturer_country_en,
                release_form_ru = EXCLUDED.release_form_ru,
                release_form_en = EXCLUDED.release_form_en,
                ean = EXCLUDED.ean,
                registered_price_rub = EXCLUDED.registered_price_rub,
                start_date_text = EXCLUDED.start_date_text,
                start_date_iso = EXCLUDED.start_date_iso,
                translation_method = EXCLUDED.translation_method,
                translated_at = CURRENT_TIMESTAMP
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id,
                product.get('item_id'),
                product.get('tn_ru'),
                product.get('tn_en'),
                product.get('inn_ru'),
                product.get('inn_en'),
                product.get('manufacturer_country_ru'),
                product.get('manufacturer_country_en'),
                product.get('release_form_ru'),
                product.get('release_form_en'),
                product.get('ean'),
                product.get('registered_price_rub'),
                product.get('start_date_text'),
                product.get('start_date_iso'),
                translation_method,
            ))

    def insert_translated_products_bulk(self, products: List[Dict]) -> int:
        """Insert multiple translated products."""
        if not products:
            return 0
        
        count = 0
        for product in products:
            self.insert_translated_product(product)
            count += 1
        
        self._db_log(f"INSERT | {count} translated products")
        return count

    def get_translated_products(self) -> List[Dict]:
        """Get all translated products for this run."""
        sql = """
            SELECT item_id, tn_ru, tn_en, inn_ru, inn_en,
                   manufacturer_country_ru, manufacturer_country_en,
                   release_form_ru, release_form_en, ean, registered_price_rub,
                   start_date_text, start_date_iso, translation_method
            FROM ru_translated_products WHERE run_id = %s
            ORDER BY item_id
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id,))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Export Ready (Step 4)
    # ------------------------------------------------------------------

    def insert_export_ready(self, product: Dict) -> None:
        """Insert export-ready formatted product."""
        sql = """
            INSERT INTO ru_export_ready
            (run_id, item_id, trade_name_en, inn_en, manufacturer_country_en,
             dosage_form_en, ean, registered_price_rub, start_date_iso, source_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, item_id) DO UPDATE SET
                trade_name_en = EXCLUDED.trade_name_en,
                inn_en = EXCLUDED.inn_en,
                manufacturer_country_en = EXCLUDED.manufacturer_country_en,
                dosage_form_en = EXCLUDED.dosage_form_en,
                ean = EXCLUDED.ean,
                registered_price_rub = EXCLUDED.registered_price_rub,
                start_date_iso = EXCLUDED.start_date_iso,
                source_type = EXCLUDED.source_type,
                formatted_at = CURRENT_TIMESTAMP
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id,
                product.get('item_id'),
                product.get('trade_name_en'),
                product.get('inn_en'),
                product.get('manufacturer_country_en'),
                product.get('dosage_form_en'),
                product.get('ean'),
                product.get('registered_price_rub'),
                product.get('start_date_iso'),
                product.get('source_type')
            ))

    def get_export_ready_count(self, source_type: str = None) -> int:
        """Get count of export-ready products."""
        if source_type:
            sql = """
                SELECT COUNT(*) FROM ru_export_ready
                WHERE run_id = %s AND source_type = %s
            """
            params = (self.run_id, source_type)
        else:
            sql = "SELECT COUNT(*) FROM ru_export_ready WHERE run_id = %s"
            params = (self.run_id,)
        
        with self.db.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0] or 0

    def get_export_ready_products(self, source_type: str = None) -> List[Dict]:
        """Get export-ready products for CSV export."""
        if source_type:
            sql = """
                SELECT item_id, trade_name_en, inn_en, manufacturer_country_en,
                       dosage_form_en, ean, registered_price_rub, start_date_iso, source_type
                FROM ru_export_ready
                WHERE run_id = %s AND source_type = %s
                ORDER BY item_id
            """
            params = (self.run_id, source_type)
        else:
            sql = """
                SELECT item_id, trade_name_en, inn_en, manufacturer_country_en,
                       dosage_form_en, ean, registered_price_rub, start_date_iso, source_type
                FROM ru_export_ready
                WHERE run_id = %s
                ORDER BY source_type, item_id
            """
            params = (self.run_id,)
        
        with self.db.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Utility: clear step data
    # ------------------------------------------------------------------

    _STEP_TABLE_MAP = {
        1: ("ved_products",),
        2: ("excluded_products",),
        3: ("translated_products", "failed_pages"),
        4: ("export_ready",),
    }

    def clear_step_data(self, step: int, include_downstream: bool = False) -> Dict[str, int]:
        """
        Delete data for the given step (and optionally downstream steps) for this run_id.
        Also clears step progress and relevant failed pages.
        """
        if step not in self._STEP_TABLE_MAP:
            raise ValueError(f"Unsupported step {step}; valid steps: {sorted(self._STEP_TABLE_MAP)}")

        steps = [s for s in sorted(self._STEP_TABLE_MAP) if s == step or (include_downstream and s >= step)]
        deleted: Dict[str, int] = {}
        
        with self.db.cursor() as cur:
            for s in steps:
                # Delete main data tables
                for table in self._STEP_TABLE_MAP[s]:
                    # Special handling for failed_pages in step 3 (it contains mixed data)
                    if s == 3 and table == "failed_pages":
                        continue  # handled separately or preserved? 
                        # actually step 3 consumes failed pages. If we clear step 3, we usually want to re-run the retry logic.
                        # But wait, step 3 *updates* failed pages to 'resolved'.
                        # Ideally 'clearing' step 3 should reset failed pages to 'pending'.
                        # For now, let's just stick to the map unless it's step 1 or 2 specific.
                        pass
                    
                    cur.execute(f"DELETE FROM ru_{table} WHERE run_id = %s", (self.run_id,))
                    deleted[f"ru_{table}"] = cur.rowcount
                
                # Delete progress tracking for this step
                cur.execute("DELETE FROM ru_step_progress WHERE run_id = %s AND step_number = %s", (self.run_id, s))
                deleted[f"ru_step_progress (step {s})"] = cur.rowcount

                # Helper: delete specific failed pages generated by Step 1 or Step 2
                source_type = None
                if s == 1:
                    source_type = 'ved'
                elif s == 2:
                    source_type = 'excluded'
                
                if source_type:
                    cur.execute("DELETE FROM ru_failed_pages WHERE run_id = %s AND source_type = %s", (self.run_id, source_type))
                    if cur.rowcount > 0:
                        deleted[f"ru_failed_pages ({source_type})"] = cur.rowcount
        
        try:
            self.db.commit()
        except Exception:
            pass

        self._db_log(f"CLEAR | steps={steps} tables={','.join(deleted)} run_id={self.run_id}")
        return deleted

    # ------------------------------------------------------------------
    # Translation Cache (delegates to core.translation)
    # ------------------------------------------------------------------
    # Note: These methods now delegate to core.translation.TranslationCache
    # for unified caching across all scrapers.
    
    def _get_translation_cache(self):
        """Lazy initialization of unified translation cache."""
        if not hasattr(self, '_translation_cache'):
            import sys
            from pathlib import Path
            repo_root = Path(__file__).resolve().parents[3]
            if str(repo_root) not in sys.path:
                sys.path.insert(0, str(repo_root))
            from core.translation import get_cache
            self._translation_cache = get_cache("russia")
        return self._translation_cache

    def get_translation_cache(self, source_lang: str = 'ru', target_lang: str = 'en') -> Dict[str, str]:
        """Load all translation cache entries from DB.
        
        DEPRECATED: Use get_cached_translation() for individual lookups.
        """
        cache = {}
        try:
            sql = """
                SELECT source_text, translated_text
                FROM ru_translation_cache
                WHERE source_language = %s AND target_language = %s
            """
            with self.db.cursor() as cur:
                cur.execute(sql, (source_lang, target_lang))
                for row in cur.fetchall():
                    cache[row[0]] = row[1]
        except Exception as e:
            print(f"[WARNING] Failed to load translation cache from DB: {e}")
        return cache

    def save_translation_cache(self, cache: Dict[str, str], source_lang: str = 'ru', target_lang: str = 'en') -> None:
        """Save translation cache entries to DB (upsert).
        
        DEPRECATED: Use save_single_translation() or unified cache.
        """
        if not cache:
            return
        tcache = self._get_translation_cache()
        count = 0
        for source_text, translated_text in cache.items():
            if tcache.set(source_text, translated_text, source_lang, target_lang):
                count += 1
        print(f"[OK] Saved {count}/{len(cache)} translations to cache")

    def get_cached_translation(self, source_text: str, source_lang: str = 'ru', target_lang: str = 'en') -> Optional[str]:
        """Get a single cached translation using unified cache."""
        return self._get_translation_cache().get(source_text, source_lang, target_lang)

    # ------------------------------------------------------------------
    # Error Tracking (ru_errors)
    # ------------------------------------------------------------------

    def log_error(self, error_type: str, error_message: str, context: Dict = None, 
                  step_number: int = None, step_name: str = None) -> None:
        """Log an error to ru_errors table."""
        import json
        sql = """
            INSERT INTO ru_errors
            (run_id, error_type, error_message, context, step_number, step_name)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        try:
            context_json = json.dumps(context, default=str) if context else None
            with self.db.cursor() as cur:
                cur.execute(sql, (
                    self.run_id,
                    error_type,
                    error_message,
                    context_json,
                    step_number,
                    step_name
                ))
            self._db_log(f"ERROR | {error_type}: {error_message}")
        except Exception as e:
            # Fallback logging if DB fails
            print(f"[DB-ERROR] Failed to log error to DB: {e}", flush=True)

    def get_errors(self, run_id: str = None) -> List[Dict]:
        """Get errors for a specific run (defaults to current run)."""
        target_run_id = run_id or self.run_id
        sql = """
            SELECT error_type, error_message, context, step_number, step_name, created_at
            FROM ru_errors WHERE run_id = %s ORDER BY created_at DESC
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (target_run_id,))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Validation Results (ru_validation_results)
    # ------------------------------------------------------------------

    def log_validation_result(self, validation_type: str, table_name: str, status: str,
                            record_id: int = None, field_name: str = None, 
                            validation_rule: str = None, message: str = None, 
                            severity: str = 'info') -> None:
        """Log a validation result."""
        sql = """
            INSERT INTO ru_validation_results
            (run_id, validation_type, table_name, record_id, field_name, 
             validation_rule, status, message, severity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self.db.cursor() as cur:
                cur.execute(sql, (
                    self.run_id,
                    validation_type,
                    table_name,
                    record_id,
                    field_name,
                    validation_rule,
                    status,
                    message,
                    severity
                ))
        except Exception as e:
            print(f"[DB-ERROR] Failed to log validation result: {e}", flush=True)

    def get_validation_results(self, run_id: str = None, status: str = None) -> List[Dict]:
        """Get validation results, optionally filtering by status."""
        target_run_id = run_id or self.run_id
        sql = "SELECT * FROM ru_validation_results WHERE run_id = %s"
        params = [target_run_id]
        
        if status:
            sql += " AND status = %s"
            params.append(status)
            
        sql += " ORDER BY validated_at DESC"
        
        with self.db.cursor() as cur:
            cur.execute(sql, tuple(params))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Statistics (ru_statistics)
    # ------------------------------------------------------------------

    def log_statistic(self, metric_name: str, metric_value: float, 
                     step_number: int = None, metric_type: str = 'count',
                     category: str = None, description: str = None) -> None:
        """Log a statistic/metric value."""
        sql = """
            INSERT INTO ru_statistics
            (run_id, step_number, metric_name, metric_value, metric_type, category, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self.db.cursor() as cur:
                cur.execute(sql, (
                    self.run_id,
                    step_number,
                    metric_name,
                    metric_value,
                    metric_type,
                    category,
                    description
                ))
        except Exception as e:
            print(f"[DB-ERROR] Failed to log statistic: {e}", flush=True)

    def get_statistics(self, run_id: str = None, category: str = None) -> List[Dict]:
        """Get statistics for a run."""
        target_run_id = run_id or self.run_id
        sql = "SELECT * FROM ru_statistics WHERE run_id = %s"
        params = [target_run_id]
        
        if category:
            sql += " AND category = %s"
            params.append(category)
            
        sql += " ORDER BY step_number, metric_name"
        
        with self.db.cursor() as cur:
            cur.execute(sql, tuple(params))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def save_single_translation(self, source_text: str, translated_text: str, source_lang: str = 'ru', target_lang: str = 'en') -> None:
        """Save a single translation to cache using unified cache."""
        self._get_translation_cache().set(source_text, translated_text, source_lang, target_lang)
