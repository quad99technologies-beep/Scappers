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

logger = logging.getLogger(__name__)


class RussiaRepository:
    """All database operations for Russia scraper (PostgreSQL backend)."""

    def __init__(self, db, run_id: str):
        """
        Initialize repository.

        Args:
            db: PostgresDB instance
            run_id: Current run ID
        """
        self.db = db
        self.run_id = run_id

    def _db_log(self, message: str) -> None:
        """Emit a [DB] activity log line for GUI activity panel."""
        try:
            print(f"[DB] {message}", flush=True)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------

    def start_run(self, mode: str = "fresh") -> None:
        """Register a new run in run_ledger."""
        from core.db.models import run_ledger_start
        sql, params = run_ledger_start(self.run_id, "Russia", mode=mode)
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
        now = datetime.now()
        
        sql = """
            INSERT INTO ru_step_progress
            (run_id, step_number, step_name, progress_key, status,
             error_message, started_at, completed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                step_name = EXCLUDED.step_name,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                started_at = CASE
                    WHEN EXCLUDED.status = 'in_progress' THEN EXCLUDED.started_at
                    ELSE COALESCE(ru_step_progress.started_at, EXCLUDED.started_at)
                END,
                completed_at = CASE
                    WHEN EXCLUDED.status IN ('completed', 'failed', 'skipped') THEN EXCLUDED.completed_at
                    WHEN EXCLUDED.status = 'in_progress' THEN NULL
                    ELSE ru_step_progress.completed_at
                END
        """
        
        started_at = now if status == 'in_progress' else None
        completed_at = now if status in ('completed', 'failed', 'skipped') else None
        
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, step_number, step_name, progress_key,
                            status, error_message, started_at, completed_at))
        
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

    # ------------------------------------------------------------------
    # VED Products (Step 1)
    # ------------------------------------------------------------------

    def insert_ved_product(self, product: Dict) -> None:
        """Insert a single VED product."""
        sql = """
            INSERT INTO ru_ved_products
            (run_id, item_id, tn, inn, manufacturer_country, release_form, ean,
             registered_price_rub, start_date_text, page_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, item_id) DO UPDATE SET
                tn = EXCLUDED.tn,
                inn = EXCLUDED.inn,
                manufacturer_country = EXCLUDED.manufacturer_country,
                release_form = EXCLUDED.release_form,
                ean = EXCLUDED.ean,
                registered_price_rub = EXCLUDED.registered_price_rub,
                start_date_text = EXCLUDED.start_date_text,
                page_number = EXCLUDED.page_number,
                scraped_at = CURRENT_TIMESTAMP
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
        sql = "SELECT COUNT(*) FROM ru_ved_products WHERE run_id = %s"
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id,))
            return cur.fetchone()[0] or 0

    def get_ved_products(self) -> List[Dict]:
        """Get all VED products for this run."""
        sql = """
            SELECT item_id, tn, inn, manufacturer_country, release_form, ean,
                   registered_price_rub, start_date_text
            FROM ru_ved_products WHERE run_id = %s
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id,))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_existing_item_ids(self) -> Set[str]:
        """Get set of existing item_ids for deduplication."""
        sql = "SELECT item_id FROM ru_ved_products WHERE run_id = %s"
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id,))
            return {row[0] for row in cur.fetchall()}

    # ------------------------------------------------------------------
    # Excluded Products (Step 2)
    # ------------------------------------------------------------------

    def insert_excluded_product(self, product: Dict) -> None:
        """Insert a single excluded product."""
        sql = """
            INSERT INTO ru_excluded_products
            (run_id, item_id, tn, inn, manufacturer_country, release_form, ean,
             registered_price_rub, start_date_text, page_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, item_id) DO UPDATE SET
                tn = EXCLUDED.tn,
                inn = EXCLUDED.inn,
                manufacturer_country = EXCLUDED.manufacturer_country,
                release_form = EXCLUDED.release_form,
                ean = EXCLUDED.ean,
                registered_price_rub = EXCLUDED.registered_price_rub,
                start_date_text = EXCLUDED.start_date_text,
                page_number = EXCLUDED.page_number,
                scraped_at = CURRENT_TIMESTAMP
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
        sql = "SELECT COUNT(*) FROM ru_excluded_products WHERE run_id = %s"
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id,))
            return cur.fetchone()[0] or 0

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
                status = CASE
                    WHEN ru_failed_pages.retry_count >= 2 THEN 'failed_permanently'
                    ELSE 'pending'
                END
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
            DELETE FROM ru_failed_pages
            WHERE run_id = %s AND page_number = %s AND source_type = %s
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, page_number, source_type))

    # ------------------------------------------------------------------
    # Translated Products (Step 3)
    # ------------------------------------------------------------------

    def insert_translated_product(self, product: Dict) -> None:
        """Insert a translated product."""
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
                product.get('translation_method', 'none')
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
        """
        if step not in self._STEP_TABLE_MAP:
            raise ValueError(f"Unsupported step {step}; valid steps: {sorted(self._STEP_TABLE_MAP)}")

        steps = [s for s in sorted(self._STEP_TABLE_MAP) if s == step or (include_downstream and s >= step)]
        deleted: Dict[str, int] = {}
        
        with self.db.cursor() as cur:
            for s in steps:
                for table in self._STEP_TABLE_MAP[s]:
                    cur.execute(f"DELETE FROM ru_{table} WHERE run_id = %s", (self.run_id,))
                    deleted[f"ru_{table}"] = cur.rowcount
        
        try:
            self.db.commit()
        except Exception:
            pass

        self._db_log(f"CLEAR | steps={steps} tables={','.join(deleted)} run_id={self.run_id}")
        return deleted
