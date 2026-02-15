#!/usr/bin/env python3
"""
Netherlands database repository - all DB access in one place.

Provides methods for:
- URL collection from medicijnkosten.nl (Step 1a)
- Pack/pricing data from medicijnkosten.nl (Step 1b)
- Product details from farmacotherapeutischkompas.nl (Step 2a)
- Cost data from farmacotherapeutischkompas.nl (Step 2b)
- Data consolidation (Step 3)
- Chrome instance tracking
- Error logging
- Export functions (CSV generation from DB)
- Sub-step progress tracking
- Run lifecycle management
"""

import csv
import json
import logging
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class NetherlandsRepository:
    """All database operations for Netherlands scraper (PostgreSQL backend)."""

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
        """Get table name with Netherlands prefix."""
        return f"nl_{name}"

    def _db_log(self, message: str) -> None:
        """Emit a [DB] activity log line for GUI activity panel."""
        # Skip Chrome instance messages to reduce console noise
        if "chrome_instance" in message.lower() or "chrome_instances" in message.lower() or "orphaned_chrome" in message.lower():
            return
        try:
            print(f"[DB] {message}", flush=True)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Transaction support
    # ------------------------------------------------------------------

    @contextmanager
    def transaction(self):
        """Context manager for explicit transactions."""
        try:
            yield
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    # ------------------------------------------------------------------
    # Utility: clear step data
    # ------------------------------------------------------------------

    _STEP_TABLE_MAP = {
        1: ("collected_urls", "packs"),
        2: ("details", "costs"),
        3: ("consolidated",),
    }

    def clear_step_data(self, step: int, include_downstream: bool = False) -> Dict[str, int]:
        """
        Delete data for the given step (and optionally downstream steps) for this run_id.

        Args:
            step: Pipeline step number (1-2)
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
            pass

        self._db_log(f"CLEAR | steps={steps} tables={','.join(deleted)} run_id={self.run_id}")
        return deleted

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------

    def start_run(self, mode: str = "fresh") -> None:
        """Register a new run in run_ledger."""
        from core.db.models import run_ledger_start
        sql, params = run_ledger_start(self.run_id, "Netherlands", mode=mode)
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

    def ensure_run_in_ledger(self, mode: str = "resume") -> None:
        """Ensure this run_id exists in run_ledger (insert if missing)."""
        from core.db.models import run_ledger_ensure_exists
        sql, params = run_ledger_ensure_exists(self.run_id, "Netherlands", mode=mode)
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"OK | run_ledger ensure | run_id={self.run_id}")

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

    # ------------------------------------------------------------------
    # Products (Step 1)
    # ------------------------------------------------------------------

    def insert_products(self, products: List[Dict]) -> int:
        """Bulk insert product data from FarmacotherapeutischKompas."""
        if not products:
            return 0

        table = self._table("products")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, product_url, product_name, brand_name, generic_name,
             atc_code, dosage_form, strength, pack_size, manufacturer,
             source_prefix)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, product_url) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                brand_name = EXCLUDED.brand_name,
                generic_name = EXCLUDED.generic_name,
                atc_code = EXCLUDED.atc_code,
                dosage_form = EXCLUDED.dosage_form,
                strength = EXCLUDED.strength,
                pack_size = EXCLUDED.pack_size,
                manufacturer = EXCLUDED.manufacturer,
                source_prefix = EXCLUDED.source_prefix
        """

        with self.db.cursor() as cur:
            for product in products:
                cur.execute(sql, (
                    self.run_id,
                    product.get("product_url"),
                    product.get("product_name"),
                    product.get("brand_name"),
                    product.get("generic_name"),
                    product.get("atc_code"),
                    product.get("dosage_form"),
                    product.get("strength"),
                    product.get("pack_size"),
                    product.get("manufacturer"),
                    product.get("source_prefix"),
                ))
                count += 1

        logger.info("Inserted %d product entries", count)
        self._db_log(f"OK | nl_products inserted={count} | run_id={self.run_id}")
        return count

    def get_products_count(self) -> int:
        """Get total product entries for this run."""
        table = self._table("products")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_products(self) -> List[Dict]:
        """Get all product entries as list of dicts."""
        table = self._table("products")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Reimbursement (Step 2)
    # ------------------------------------------------------------------

    def insert_reimbursement(self, data: List[Dict]) -> int:
        """Bulk insert reimbursement pricing data."""
        if not data:
            return 0

        table = self._table("reimbursement")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, product_url, product_name, reimbursement_price,
             pharmacy_purchase_price, list_price, supplement, currency,
             reimbursement_status, indication, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, product_url, product_name) DO UPDATE SET
                reimbursement_price = EXCLUDED.reimbursement_price,
                pharmacy_purchase_price = EXCLUDED.pharmacy_purchase_price,
                list_price = EXCLUDED.list_price,
                supplement = EXCLUDED.supplement,
                currency = EXCLUDED.currency,
                reimbursement_status = EXCLUDED.reimbursement_status,
                indication = EXCLUDED.indication,
                source_url = EXCLUDED.source_url
        """

        with self.db.cursor() as cur:
            for item in data:
                cur.execute(sql, (
                    self.run_id,
                    item.get("product_url"),
                    item.get("product_name"),
                    item.get("reimbursement_price"),
                    item.get("pharmacy_purchase_price"),
                    item.get("list_price"),
                    item.get("supplement"),
                    item.get("currency", "EUR"),
                    item.get("reimbursement_status"),
                    item.get("indication"),
                    item.get("source_url"),
                ))
                count += 1

        logger.info("Inserted %d reimbursement entries", count)
        self._db_log(f"OK | nl_reimbursement inserted={count} | run_id={self.run_id}")
        return count

    def get_reimbursement_count(self) -> int:
        """Get total reimbursement entries for this run."""
        table = self._table("reimbursement")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_reimbursement(self) -> List[Dict]:
        """Get all reimbursement entries as list of dicts."""
        table = self._table("reimbursement")
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

    @staticmethod
    def get_latest_incomplete_run(db) -> Optional[str]:
        """
        Find the best run_id to resume from database.

        Pattern from Argentina:
        - Prefer latest run explicitly marked 'resume'
        - Otherwise prefer runs with data (items_scraped > 0) and most recent
        - Incomplete = status in ('running', 'partial', 'resume', 'stopped')

        Returns run_id if found, None otherwise.
        """
        try:
            with db.cursor() as cur:
                # 1) Prefer latest resumable run explicitly marked as resume
                cur.execute("""
                    SELECT run_id
                    FROM run_ledger
                    WHERE scraper_name = 'Netherlands'
                      AND status = 'resume'
                    ORDER BY started_at DESC
                    LIMIT 1
                """)
                row = cur.fetchone()
                if row:
                    return row[0]

                # 2) Otherwise prefer running/partial with most data, then recency
                cur.execute("""
                    SELECT run_id
                    FROM run_ledger
                    WHERE scraper_name = 'Netherlands'
                      AND status IN ('running', 'partial')
                    ORDER BY COALESCE(items_scraped, 0) DESC NULLS LAST, started_at DESC
                    LIMIT 1
                """)
                row = cur.fetchone()
                if row:
                    return row[0]

                # 3) Fallback to latest stopped run
                cur.execute("""
                    SELECT run_id
                    FROM run_ledger
                    WHERE scraper_name = 'Netherlands'
                      AND status = 'stopped'
                    ORDER BY started_at DESC
                    LIMIT 1
                """)
                row = cur.fetchone()
                return row[0] if row else None
        except Exception as e:
            print(f"[DB] Could not check for incomplete runs: {e}")
            return None

    @staticmethod
    def stop_other_resume_runs(db, keep_run_id: str) -> int:
        """Mark other resume runs as stopped to avoid multiple resumable IDs."""
        if not keep_run_id:
            return 0
        try:
            with db.cursor() as cur:
                cur.execute("""
                    UPDATE run_ledger
                    SET status = 'stopped', ended_at = CURRENT_TIMESTAMP
                    WHERE scraper_name = 'Netherlands'
                      AND status = 'resume'
                      AND run_id <> %s
                """, (keep_run_id,))
                return cur.rowcount
        except Exception:
            return 0

    @staticmethod
    def get_run_progress(db, run_id: str) -> Dict:
        """
        Get detailed progress for a specific run.
        Returns dict with URLs collected, products scraped, etc.
        """
        try:
            with db.cursor() as cur:
                # Get counts from various tables
                cur.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM nl_collected_urls WHERE run_id = %s) as urls_collected,
                        (SELECT COUNT(*) FROM nl_packs WHERE run_id = %s) as products_scraped,
                        (SELECT COUNT(*) FROM nl_consolidated WHERE run_id = %s) as consolidated,
                        (SELECT status FROM run_ledger WHERE run_id = %s) as status,
                        (SELECT started_at FROM run_ledger WHERE run_id = %s) as started_at
                """, (run_id, run_id, run_id, run_id, run_id))
                row = cur.fetchone()
                if row:
                    return {
                        'run_id': run_id,
                        'urls_collected': row[0] or 0,
                        'products_scraped': row[1] or 0,
                        'consolidated': row[2] or 0,
                        'status': row[3] or 'unknown',
                        'started_at': row[4]
                    }
        except Exception as e:
            print(f"[DB] Could not get run progress: {e}")
        return {}

    def get_run_stats(self) -> Dict:
        """Get comprehensive stats for this run (single query for all counts).

        Returns empty dict with defaults if tables don't exist yet.
        """
        empty = {
            'urls_collected': 0, 'urls_scraped': 0, 'urls_failed': 0,
            'urls_pending': 0, 'packs_total': 0, 'details_total': 0,
            'costs_total': 0, 'consolidated_total': 0, 'error_count': 0,
            'prefixes_completed': 0,
            'collected_urls_count': 0, 'packs_count': 0, 'details_count': 0,
            'costs_count': 0, 'consolidated_count': 0, 'exports_count': 0,
            'run_exists': False,
        }

        try:
            sql = """
                SELECT
                    (SELECT COUNT(*) FROM nl_collected_urls WHERE run_id = %s) as urls_collected,
                    (SELECT COUNT(*) FROM nl_collected_urls WHERE run_id = %s AND packs_scraped = 'success') as urls_scraped,
                    (SELECT COUNT(*) FROM nl_collected_urls WHERE run_id = %s AND packs_scraped = 'failed') as urls_failed,
                    (SELECT COUNT(*) FROM nl_collected_urls WHERE run_id = %s AND packs_scraped = 'pending') as urls_pending,
                    (SELECT COUNT(*) FROM nl_packs WHERE run_id = %s) as packs_total,
                    (SELECT COUNT(*) FROM nl_details WHERE run_id = %s) as details_total,
                    (SELECT COUNT(*) FROM nl_costs WHERE run_id = %s) as costs_total,
                    (SELECT COUNT(*) FROM nl_consolidated WHERE run_id = %s) as consolidated_total,
                    (SELECT COUNT(*) FROM nl_errors WHERE run_id = %s) as error_count,
                    (SELECT COUNT(DISTINCT progress_key) FROM nl_step_progress
                     WHERE run_id = %s AND step_number = 1 AND status = 'completed') as prefixes_completed,
                    (SELECT COUNT(*) FROM nl_export_reports WHERE run_id = %s) as exports_total,
                    (SELECT 1 FROM run_ledger WHERE run_id = %s LIMIT 1) as run_exists
            """
            with self.db.cursor() as cur:
                cur.execute(sql, (self.run_id,) * 12)
                row = cur.fetchone()
                if row is None:
                    return empty
                return {
                    # Legacy names (for backward compatibility)
                    'urls_collected': row[0] or 0,
                    'urls_scraped': row[1] or 0,
                    'urls_failed': row[2] or 0,
                    'urls_pending': row[3] or 0,
                    'packs_total': row[4] or 0,
                    'details_total': row[5] or 0,
                    'costs_total': row[6] or 0,
                    'consolidated_total': row[7] or 0,
                    'error_count': row[8] or 0,
                    'prefixes_completed': row[9] or 0,
                    # New names (for pipeline runner)
                    'collected_urls_count': row[0] or 0,
                    'packs_count': row[4] or 0,
                    'details_count': row[5] or 0,
                    'costs_count': row[6] or 0,
                    'consolidated_count': row[7] or 0,
                    'exports_count': row[10] or 0,
                    'run_exists': row[11] is not None,
                }
        except Exception as e:
            self._db_log(f"WARN | get_run_stats failed: {e}")
            return empty

    # ==================================================================
    # COLLECTED URLS (Step 1a) - replaces collected_urls.csv
    # ==================================================================

    def insert_collected_urls(self, urls: List[Dict], batch_size: int = 500) -> int:
        """
        Bulk insert collected URLs from medicijnkosten.nl.

        Args:
            urls: List of URL records with keys: prefix, title, active_substance,
                  manufacturer, document_type, price_text, reimbursement, url, url_with_id
            batch_size: Number of records per batch

        Returns:
            Number of rows inserted
        """
        if not urls:
            return 0

        table = self._table("collected_urls")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, prefix, title, active_substance, manufacturer, document_type,
             price_text, reimbursement, url, url_with_id, packs_scraped, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, url) DO UPDATE SET
                prefix = EXCLUDED.prefix,
                title = EXCLUDED.title,
                active_substance = EXCLUDED.active_substance,
                manufacturer = EXCLUDED.manufacturer,
                document_type = EXCLUDED.document_type,
                price_text = EXCLUDED.price_text,
                reimbursement = EXCLUDED.reimbursement,
                url_with_id = EXCLUDED.url_with_id,
                packs_scraped = CASE
                    WHEN {table}.packs_scraped = 'success' THEN {table}.packs_scraped
                    ELSE 'pending'
                END,
                error_message = CASE
                    WHEN {table}.packs_scraped = 'success' THEN {table}.error_message
                    ELSE NULL
                END,
                retry_count = CASE
                    WHEN {table}.url_with_id IS DISTINCT FROM EXCLUDED.url_with_id THEN 0
                    ELSE {table}.retry_count
                END
        """

        with self.db.cursor() as cur:
            for url_rec in urls:
                cur.execute(sql, (
                    self.run_id,
                    url_rec.get("prefix", ""),
                    url_rec.get("title", ""),
                    url_rec.get("active_substance", ""),
                    url_rec.get("manufacturer", ""),
                    url_rec.get("document_type", ""),
                    url_rec.get("price_text", ""),
                    url_rec.get("reimbursement", ""),
                    url_rec.get("url", ""),
                    url_rec.get("url_with_id", ""),
                    url_rec.get("packs_scraped", "pending"),
                    url_rec.get("error", ""),
                ))
                count += 1

                # Commit in batches
                if count % batch_size == 0:
                    self.db.commit()

        self.db.commit()
        self._db_log(f"OK | nl_collected_urls inserted={count} | run_id={self.run_id}")
        return count

    def get_collected_url_count(self) -> int:
        """Get total collected URLs for this run."""
        table = self._table("collected_urls")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row.get("count", 0)

    def get_pending_scrape_urls(self, limit: int = 100) -> List[Dict]:
        """Get URLs that need scraping (pending status)."""
        table = self._table("collected_urls")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, prefix, url, url_with_id, title, manufacturer
                FROM {table}
                WHERE run_id = %s AND packs_scraped = 'pending'
                ORDER BY id
                LIMIT %s
            """, (self.run_id, limit))
            rows = cur.fetchall()
            return [
                {
                    "id": row[0],
                    "prefix": row[1],
                    "url": row[2],
                    "url_with_id": row[3],
                    "title": row[4],
                    "manufacturer": row[5],
                }
                for row in rows
            ]

    def get_scraped_url_keys(self) -> Set[str]:
        """Get set of URLs that have been successfully scraped (canonical, no-id)."""
        table = self._table("collected_urls")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT url FROM {table}
                WHERE run_id = %s AND packs_scraped = 'success'
            """, (self.run_id,))
            rows = cur.fetchall()
            return {row[0] for row in rows if row[0]}

    def get_collected_url_keys(self) -> Set[str]:
        """Get set of all collected URLs (canonical, no-id)."""
        table = self._table("collected_urls")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT url FROM {table}
                WHERE run_id = %s
            """, (self.run_id,))
            rows = cur.fetchall()
            return {row[0] for row in rows if row[0]}

    def mark_url_scraped(self, url: str, status: str = 'success',
                         error: str = None) -> None:
        """
        Mark a URL as scraped with given status.

        Args:
            url: The canonical URL (no id parameter)
            status: 'success', 'failed', or 'skipped'
            error: Error message if status is 'failed'
        """
        table = self._table("collected_urls")
        now = datetime.now()

        with self.db.cursor() as cur:
            # Get current retry_count first
            cur.execute(f"""
                SELECT retry_count FROM {table}
                WHERE run_id = %s AND url = %s
            """, (self.run_id, url))
            row = cur.fetchone()
            current_retry_count = row[0] if row else 0
            
            # Maximum retries per URL (configurable via environment variable, default 2)
            import os
            MAX_RETRIES = int(os.getenv("MAX_TOTAL_RETRIES", "2"))
            
            if status == 'failed' and current_retry_count >= MAX_RETRIES:
                # Already retried 2 times - mark as permanently failed
                error_msg = (error or "")[:500]
                if error_msg:
                    error_msg = f"{error_msg} (Permanently failed after {current_retry_count} retries)"
                cur.execute(f"""
                    UPDATE {table}
                    SET packs_scraped = 'failed',
                        error_message = %s,
                        scraped_at = %s,
                        retry_count = %s
                    WHERE run_id = %s AND url = %s
                """, (error_msg, now, current_retry_count, self.run_id, url))
            else:
                # Normal update - increment retry_count
                cur.execute(f"""
                    UPDATE {table}
                    SET packs_scraped = %s,
                        error_message = %s,
                        scraped_at = %s,
                        retry_count = retry_count + 1
                    WHERE run_id = %s AND url = %s
                """, (status, (error or "")[:500], now, self.run_id, url))
        self.db.commit()

    def update_url_status(self, url_id: int, status: str, error_message: str = None) -> None:
        """
        Update URL status by ID (used during scraping for immediate status updates).
        
        Args:
            url_id: The ID of the collected URL record
            status: 'pending', 'success', 'failed', or 'skipped'
            error_message: Optional error message
        """
        table = self._table("collected_urls")
        now = datetime.now()
        
        with self.db.cursor() as cur:
            cur.execute(f"""
                UPDATE {table}
                SET packs_scraped = %s,
                    error_message = %s,
                    scraped_at = %s
                WHERE id = %s
            """, (status, (error_message or "")[:500], now, url_id))
        self.db.commit()

    def get_failed_urls_by_prefix(self) -> Dict[str, List[str]]:
        """Get failed URLs grouped by prefix, excluding URLs that have already been retried MAX_TOTAL_RETRIES+ times."""
        table = self._table("collected_urls")
        import os
        MAX_RETRIES = int(os.getenv("MAX_TOTAL_RETRIES", "2"))
        with self.db.cursor() as cur:
            # Only get failed URLs that haven't exceeded max retries
            cur.execute(f"""
                SELECT prefix, url, retry_count
                FROM {table}
                WHERE run_id = %s AND packs_scraped = 'failed' AND retry_count < %s
                ORDER BY prefix, url
            """, (self.run_id, MAX_RETRIES))
            rows = cur.fetchall()
            
            result: Dict[str, List[str]] = {}
            for row in rows:
                prefix = row[0] or "unknown"
                url = row[1]
                retry_count = row[2] if len(row) > 2 else 0
                if prefix not in result:
                    result[prefix] = []
                result[prefix].append(url)
            return result
    
    def get_permanently_failed_count(self) -> int:
        """Get count of URLs that have been retried MAX_TOTAL_RETRIES+ times (permanently failed)."""
        table = self._table("collected_urls")
        import os
        MAX_RETRIES = int(os.getenv("MAX_TOTAL_RETRIES", "2"))
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE run_id = %s AND packs_scraped = 'failed' AND retry_count >= %s
            """, (self.run_id, MAX_RETRIES))
            row = cur.fetchone()
            return row[0] if row else 0
    
    def delete_failed_urls(self, include_permanent: bool = False) -> int:
        """Delete failed URLs from collected_urls table.

        Args:
            include_permanent: If True, delete all failed rows.
                If False, keep permanently failed rows (retry_count >= MAX_TOTAL_RETRIES).
        """
        table = self._table("collected_urls")
        import os
        max_retries = int(os.getenv("MAX_TOTAL_RETRIES", "2"))
        with self.db.cursor() as cur:
            if include_permanent:
                cur.execute(f"""
                    DELETE FROM {table}
                    WHERE run_id = %s AND packs_scraped = 'failed'
                """, (self.run_id,))
            else:
                cur.execute(f"""
                    DELETE FROM {table}
                    WHERE run_id = %s
                      AND packs_scraped = 'failed'
                      AND retry_count < %s
                """, (self.run_id, max_retries))
            count = cur.rowcount
        self.db.commit()
        return count

    def get_completed_prefixes(self) -> Set[str]:
        """Get all completed prefixes for Step 1."""
        return self.get_completed_keys(step_number=1)

    def mark_prefix_completed(self, prefix: str) -> None:
        """Mark a prefix as completed in Step 1."""
        self.mark_progress(
            step_number=1,
            step_name="collect_and_scrape",
            progress_key=prefix.strip().lower(),
            status="completed"
        )
        self.db.commit()

    def is_prefix_completed(self, prefix: str) -> bool:
        """Check if a prefix is completed in Step 1."""
        return self.is_progress_completed(
            step_number=1,
            progress_key=prefix.strip().lower()
        )

    # ==================================================================
    # PACKS (Step 1b) - replaces packs.csv
    # ==================================================================

    def insert_packs(self, packs: List[Dict], batch_size: int = 500, log_db: bool = True) -> int:
        """
        Bulk insert pack/pricing data from medicijnkosten.nl.

        Args:
            packs: List of pack records with pricing data
            batch_size: Number of records per batch

        Returns:
            Number of rows inserted
        """
        if not packs:
            return 0

        table = self._table("packs")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, collected_url_id, start_date, end_date, currency, unit_price, ppp_ex_vat, ppp_vat,
             vat_percent, reimbursable_status, reimbursable_rate, copay_price, copay_percent,
             deductible, ri_with_vat, margin_rule, product_group, local_pack_description, active_substance, manufacturer,
             formulation, strength_size, local_pack_code, reimbursement_message, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, source_url, local_pack_code) DO UPDATE SET
                collected_url_id = COALESCE(EXCLUDED.collected_url_id, {table}.collected_url_id),
                unit_price = EXCLUDED.unit_price,
                ppp_ex_vat = EXCLUDED.ppp_ex_vat,
                ppp_vat = EXCLUDED.ppp_vat,
                reimbursable_status = EXCLUDED.reimbursable_status,
                reimbursable_rate = EXCLUDED.reimbursable_rate,
                copay_price = EXCLUDED.copay_price,
                copay_percent = EXCLUDED.copay_percent,
                deductible = EXCLUDED.deductible,
                ri_with_vat = EXCLUDED.ri_with_vat,
                product_group = EXCLUDED.product_group,
                active_substance = EXCLUDED.active_substance,
                manufacturer = EXCLUDED.manufacturer,
                reimbursement_message = EXCLUDED.reimbursement_message
        """

        with self.db.cursor() as cur:
            for pack in packs:
                # Parse start_date from string if needed
                start_date = pack.get("start_date")
                if isinstance(start_date, str) and start_date:
                    try:
                        start_date = datetime.strptime(start_date, "%d-%m-%Y").date()
                    except ValueError:
                        start_date = None
                elif isinstance(start_date, str) and not start_date:
                    start_date = None

                # Parse end_date from string if needed
                end_date = pack.get("end_date")
                if isinstance(end_date, str) and end_date:
                    try:
                        end_date = datetime.strptime(end_date, "%d-%m-%Y").date()
                    except ValueError:
                        end_date = None
                elif isinstance(end_date, str) and not end_date:
                    end_date = None

                cur.execute(sql, (
                    self.run_id,
                    pack.get("collected_url_id"),
                    start_date,
                    end_date,
                    pack.get("currency", "EUR"),
                    self._parse_decimal(pack.get("unit_price")),
                    self._parse_decimal(pack.get("ppp_ex_vat")),
                    self._parse_decimal(pack.get("ppp_vat")),
                    self._parse_decimal(pack.get("vat_percent", 9.0)),
                    pack.get("reimbursable_status", ""),
                    pack.get("reimbursable_rate", ""),
                    self._parse_decimal(pack.get("copay_price")),
                    pack.get("copay_percent", ""),
                    self._parse_decimal(pack.get("deductible")),
                    self._parse_decimal(pack.get("ri_with_vat")),
                    pack.get("margin_rule", ""),
                    pack.get("product_group", ""),
                    pack.get("local_pack_description", ""),
                    pack.get("active_substance", ""),
                    pack.get("manufacturer", ""),
                    pack.get("formulation", ""),
                    pack.get("strength_size", ""),
                    pack.get("local_pack_code", ""),
                    pack.get("reimbursement_message", ""),
                    pack.get("source_url", ""),
                ))
                count += 1

                if count % batch_size == 0:
                    self.db.commit()

        self.db.commit()
        if log_db:
            self._db_log(f"OK | nl_packs inserted={count} | run_id={self.run_id}")
        return count

    def _parse_decimal(self, value: Any) -> Optional[float]:
        """Parse a value to decimal/float, handling various formats."""
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # Remove currency symbols and whitespace
            s = value.replace("€", "").replace("$", "").strip()
            if not s:
                return None
            # Handle European format (comma as decimal separator)
            if "," in s and "." in s:
                # e.g., "1.234,56" -> "1234.56"
                s = s.replace(".", "").replace(",", ".")
            elif "," in s:
                # e.g., "12,34" -> "12.34"
                s = s.replace(",", ".")
            try:
                return float(s)
            except ValueError:
                return None
        return None

    def get_packs_count(self) -> int:
        """Get total pack entries for this run."""
        table = self._table("packs")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row.get("count", 0)

    def get_all_packs(self) -> List[Dict]:
        """Get all pack entries as list of dicts."""
        table = self._table("packs")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, start_date, end_date, currency, unit_price, ppp_ex_vat, ppp_vat,
                       vat_percent, reimbursable_status, reimbursable_rate, copay_price,
                       copay_percent, deductible, margin_rule, product_group, local_pack_description,
                       active_substance, manufacturer, formulation,
                       strength_size, local_pack_code, reimbursement_message, source_url
                FROM {table}
                WHERE run_id = %s
            """, (self.run_id,))
            rows = cur.fetchall()
            columns = [
                "id", "start_date", "end_date", "currency", "unit_price", "ppp_ex_vat",
                "ppp_vat", "vat_percent", "reimbursable_status", "reimbursable_rate",
                "copay_price", "copay_percent", "deductible", "margin_rule", "product_group",
                "local_pack_description", "active_substance", "manufacturer", "formulation",
                "strength_size", "local_pack_code", "reimbursement_message", "source_url"
            ]
            return [dict(zip(columns, row)) for row in rows]

    # ==================================================================
    # DETAILS (Step 2a) - replaces details.csv
    # ==================================================================

    def insert_details(self, details: List[Dict]) -> int:
        """
        Insert product details from farmacotherapeutischkompas.nl.

        Args:
            details: List of detail records

        Returns:
            Number of rows inserted
        """
        if not details:
            return 0

        table = self._table("details")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, detail_url, product_name, product_type, manufacturer,
             administration_form, strengths_raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, detail_url) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                product_type = EXCLUDED.product_type,
                manufacturer = EXCLUDED.manufacturer,
                administration_form = EXCLUDED.administration_form,
                strengths_raw = EXCLUDED.strengths_raw
        """

        with self.db.cursor() as cur:
            for detail in details:
                cur.execute(sql, (
                    self.run_id,
                    detail.get("detail_url", ""),
                    detail.get("product_name", ""),
                    detail.get("product_type", ""),
                    detail.get("manufacturer", ""),
                    detail.get("administration_form", ""),
                    detail.get("strengths_raw", ""),
                ))
                count += 1

        self.db.commit()
        self._db_log(f"OK | nl_details inserted={count} | run_id={self.run_id}")
        return count

    def get_details_count(self) -> int:
        """Get total detail entries for this run."""
        table = self._table("details")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row.get("count", 0)

    def get_scraped_detail_urls(self) -> Set[str]:
        """Get set of detail URLs that have been scraped."""
        table = self._table("details")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT detail_url FROM {table}
                WHERE run_id = %s
            """, (self.run_id,))
            rows = cur.fetchall()
            return {row[0] for row in rows if row[0]}

    def get_all_details(self) -> List[Dict]:
        """Get all detail entries as list of dicts."""
        table = self._table("details")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, detail_url, product_name, product_type, manufacturer,
                       administration_form, strengths_raw
                FROM {table}
                WHERE run_id = %s
            """, (self.run_id,))
            rows = cur.fetchall()
            columns = [
                "id", "detail_url", "product_name", "product_type", "manufacturer",
                "administration_form", "strengths_raw"
            ]
            return [dict(zip(columns, row)) for row in rows]

    # ==================================================================
    # COSTS (Step 2b) - replaces costs.csv
    # ==================================================================

    def insert_costs(self, costs: List[Dict], detail_url: str = None) -> int:
        """
        Insert cost/pricing data from farmacotherapeutischkompas.nl.

        Args:
            costs: List of cost records
            detail_url: Optional detail URL to associate with all costs

        Returns:
            Number of rows inserted
        """
        if not costs:
            return 0

        table = self._table("costs")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, detail_url, brand_full, brand_name, pack_presentation, ddd_text,
             currency, price_per_day, price_per_week, price_per_month, price_per_six_months,
             reimbursed_per_day, extra_payment_per_day, table_type, unit_type, unit_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self.db.cursor() as cur:
            for cost in costs:
                url = cost.get("detail_url", detail_url) or ""
                cur.execute(sql, (
                    self.run_id,
                    url,
                    cost.get("brand_full", ""),
                    cost.get("brand_name", ""),
                    cost.get("pack_presentation", ""),
                    cost.get("ddd_text", ""),
                    cost.get("currency", "EUR"),
                    self._parse_decimal(cost.get("price_per_day")),
                    self._parse_decimal(cost.get("price_per_week")),
                    self._parse_decimal(cost.get("price_per_month")),
                    self._parse_decimal(cost.get("price_per_six_months")),
                    self._parse_decimal(cost.get("reimbursed_per_day")),
                    self._parse_decimal(cost.get("extra_payment_per_day")),
                    cost.get("table_type", ""),
                    cost.get("unit_type", ""),
                    cost.get("unit_amount", ""),
                ))
                count += 1

        self.db.commit()
        self._db_log(f"OK | nl_costs inserted={count} | run_id={self.run_id}")
        return count

    def get_costs_count(self) -> int:
        """Get total cost entries for this run."""
        table = self._table("costs")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row.get("count", 0)

    def get_all_costs(self) -> List[Dict]:
        """Get all cost entries as list of dicts."""
        table = self._table("costs")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, detail_url, brand_full, brand_name, pack_presentation, ddd_text,
                       currency, price_per_day, price_per_week, price_per_month,
                       price_per_six_months, reimbursed_per_day, extra_payment_per_day,
                       table_type, unit_type, unit_amount
                FROM {table}
                WHERE run_id = %s
            """, (self.run_id,))
            rows = cur.fetchall()
            columns = [
                "id", "detail_url", "brand_full", "brand_name", "pack_presentation",
                "ddd_text", "currency", "price_per_day", "price_per_week", "price_per_month",
                "price_per_six_months", "reimbursed_per_day", "extra_payment_per_day",
                "table_type", "unit_type", "unit_amount"
            ]
            return [dict(zip(columns, row)) for row in rows]

    # ==================================================================
    # CONSOLIDATION (Step 3) - replaces consolidated_products.csv
    # ==================================================================

    def consolidate_data(self) -> int:
        """
        Consolidate details and costs data using DB-side JOIN.
        Inserts merged data into nl_consolidated table.

        Returns:
            Number of rows inserted
        """
        consolidated_table = self._table("consolidated")
        details_table = self._table("details")
        costs_table = self._table("costs")

        # Clear existing consolidated data for this run
        with self.db.cursor() as cur:
            cur.execute(f"DELETE FROM {consolidated_table} WHERE run_id = %s", (self.run_id,))

        # Insert consolidated data via JOIN
        sql = f"""
            INSERT INTO {consolidated_table}
            (run_id, detail_url, product_name, brand_name, manufacturer,
             administration_form, strengths_raw, pack_presentation, currency,
             price_per_day, reimbursed_per_day, extra_payment_per_day, ddd_text,
             table_type, unit_type, unit_amount)
            SELECT
                d.run_id,
                d.detail_url,
                d.product_name,
                c.brand_name,
                d.manufacturer,
                d.administration_form,
                d.strengths_raw,
                c.pack_presentation,
                c.currency,
                c.price_per_day,
                c.reimbursed_per_day,
                c.extra_payment_per_day,
                c.ddd_text,
                c.table_type,
                c.unit_type,
                c.unit_amount
            FROM {details_table} d
            LEFT JOIN {costs_table} c ON d.detail_url = c.detail_url AND d.run_id = c.run_id
            WHERE d.run_id = %s
            ON CONFLICT (run_id, detail_url, brand_name) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                manufacturer = EXCLUDED.manufacturer,
                administration_form = EXCLUDED.administration_form,
                strengths_raw = EXCLUDED.strengths_raw,
                pack_presentation = EXCLUDED.pack_presentation,
                currency = EXCLUDED.currency,
                price_per_day = EXCLUDED.price_per_day,
                reimbursed_per_day = EXCLUDED.reimbursed_per_day,
                extra_payment_per_day = EXCLUDED.extra_payment_per_day,
                ddd_text = EXCLUDED.ddd_text,
                table_type = EXCLUDED.table_type,
                unit_type = EXCLUDED.unit_type,
                unit_amount = EXCLUDED.unit_amount
        """

        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id,))
            count = cur.rowcount

        self.db.commit()
        self._db_log(f"OK | nl_consolidated merged={count} | run_id={self.run_id}")
        return count

    def get_consolidated_count(self) -> int:
        """Get total consolidated entries for this run."""
        table = self._table("consolidated")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row.get("count", 0)

    def get_consolidated_data(self) -> List[Dict]:
        """Get all consolidated entries as list of dicts."""
        table = self._table("consolidated")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, detail_url, product_name, brand_name, manufacturer,
                       administration_form, strengths_raw, pack_presentation, currency,
                       price_per_day, reimbursed_per_day, extra_payment_per_day,
                       ddd_text, table_type, unit_type, unit_amount
                FROM {table}
                WHERE run_id = %s
                ORDER BY id
            """, (self.run_id,))
            rows = cur.fetchall()
            columns = [
                "id", "detail_url", "product_name", "brand_name", "manufacturer",
                "administration_form", "strengths_raw", "pack_presentation", "currency",
                "price_per_day", "reimbursed_per_day", "extra_payment_per_day",
                "ddd_text", "table_type", "unit_type", "unit_amount"
            ]
            return [dict(zip(columns, row)) for row in rows]

    # ==================================================================
    # CHROME INSTANCE TRACKING
    # ==================================================================

    # ==================================================================
    # CHROME INSTANCE TRACKING (Standardized - Shared Table)
    # ==================================================================

    def register_chrome_instance(self, step_number: int, thread_id: int,
                                  pid: int, user_data_dir: str = None,
                                  browser_type: str = "chrome",
                                  parent_pid: int = None) -> int:
        """
        Register a Chrome/browser instance in the SHARED database table.

        Args:
            step_number: Pipeline step (1, 2, etc.)
            thread_id: Worker thread ID
            pid: Process ID of the browser
            user_data_dir: Path to user data directory
            browser_type: 'chrome', 'chromium', or 'firefox'
            parent_pid: Parent process ID (chromedriver)

        Returns:
            Instance ID
        """
        table = "chrome_instances"  # Shared table

        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, scraper_name, step_number, thread_id, browser_type, pid, parent_pid, user_data_dir)
                VALUES (%s, 'Netherlands', %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (self.run_id, step_number, thread_id, browser_type, pid, parent_pid, user_data_dir))
            row = cur.fetchone()
            instance_id = row[0] if row else 0

        self.db.commit()
        self._db_log(f"OK | chrome_instance registered | pid={pid} step={step_number} thread={thread_id}")
        return instance_id

    def mark_chrome_terminated(self, instance_id: int, reason: str = "cleanup") -> None:
        """Mark a Chrome instance as terminated."""
        table = "chrome_instances"  # Shared table
        now = datetime.now()

        with self.db.cursor() as cur:
            cur.execute(f"""
                UPDATE {table}
                SET terminated_at = %s, termination_reason = %s
                WHERE id = %s AND scraper_name = 'Netherlands'
            """, (now, reason, instance_id))

        self.db.commit()

    def mark_chrome_terminated_by_pid(self, pid: int, reason: str = "cleanup") -> None:
        """Mark a Chrome instance as terminated by PID."""
        table = "chrome_instances"  # Shared table
        now = datetime.now()

        with self.db.cursor() as cur:
            cur.execute(f"""
                UPDATE {table}
                SET terminated_at = %s, termination_reason = %s
                WHERE run_id = %s AND scraper_name = 'Netherlands' AND pid = %s AND terminated_at IS NULL
            """, (now, reason, self.run_id, pid))

        self.db.commit()

    def get_active_chrome_instances(self) -> List[Dict]:
        """Get all active (not terminated) Chrome instances for this run."""
        table = "chrome_instances"  # Shared table

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, step_number, thread_id, browser_type, pid, parent_pid,
                       user_data_dir, started_at
                FROM {table}
                WHERE run_id = %s AND scraper_name = 'Netherlands' AND terminated_at IS NULL
            """, (self.run_id,))
            rows = cur.fetchall()
            columns = [
                "id", "step_number", "thread_id", "browser_type", "pid", "parent_pid",
                "user_data_dir", "started_at"
            ]
            return [dict(zip(columns, row)) for row in rows]

    def get_orphaned_chrome_instances(self, max_age_hours: int = 2) -> List[Dict]:
        """
        Get Chrome instances that have been running too long (likely orphaned).

        Args:
            max_age_hours: Maximum age in hours before considering orphaned

        Returns:
            List of orphaned instance records
        """
        table = "chrome_instances"  # Shared table

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, pid, user_data_dir, started_at
                FROM {table}
                WHERE run_id = %s AND scraper_name = 'Netherlands'
                  AND terminated_at IS NULL
                  AND started_at < NOW() - MAKE_INTERVAL(hours => %s)
            """, (self.run_id, max_age_hours))
            rows = cur.fetchall()
            columns = ["id", "pid", "user_data_dir", "started_at"]
            return [dict(zip(columns, row)) for row in rows]

    def terminate_all_chrome_instances(self, reason: str = "pipeline_cleanup") -> int:
        """Mark all active Chrome instances for this run as terminated."""
        table = "chrome_instances"  # Shared table
        now = datetime.now()

        with self.db.cursor() as cur:
            cur.execute(f"""
                UPDATE {table}
                SET terminated_at = %s, termination_reason = %s
                WHERE run_id = %s AND scraper_name = 'Netherlands' AND terminated_at IS NULL
            """, (now, reason, self.run_id))
            count = cur.rowcount

        self.db.commit()
        self._db_log(f"OK | chrome_instances terminated={count} | run_id={self.run_id}")
        return count

    def terminate_orphaned_instances(self, max_age_hours: int = 2) -> int:
        """
        Find and terminate orphaned Chrome instances.

        This method:
        1. Finds Chrome instances that have been running too long
        2. Attempts to kill the actual OS processes
        3. Marks them as terminated in the database

        Args:
            max_age_hours: Maximum age in hours before considering orphaned

        Returns:
            Number of instances terminated
        """
        orphaned = self.get_orphaned_chrome_instances(max_age_hours)
        if not orphaned:
            return 0

        terminated_count = 0
        for instance in orphaned:
            pid = instance.get("pid")
            instance_id = instance.get("id")

            # Try to kill the process
            try:
                import psutil
                if psutil.pid_exists(pid):
                    proc = psutil.Process(pid)
                    proc.terminate()
                    proc.wait(timeout=5)
            except Exception:
                pass  # Process may already be dead

            # Mark as terminated in DB
            self.mark_chrome_terminated(instance_id, "orphan_cleanup")
            terminated_count += 1

        self._db_log(f"OK | orphaned_chrome terminated={terminated_count} | max_age_hours={max_age_hours}")
        return terminated_count

    # ==================================================================
    # ERROR LOGGING
    # ==================================================================

    def log_error(self, error_type: str, message: str,
                  context: Dict = None, step_number: int = None,
                  step_name: str = None, url: str = None,
                  thread_id: int = None, include_stack: bool = False) -> None:
        """
        Log an error to the nl_errors table.

        Args:
            error_type: Type of error ('network', 'parse', 'session', 'validation', 'timeout', 'unknown')
            message: Error message
            context: Additional context as dict (stored as JSONB)
            step_number: Pipeline step number
            step_name: Human-readable step name
            url: URL being processed when error occurred
            thread_id: Worker thread ID
            include_stack: Whether to include stack trace
        """
        table = self._table("errors")

        stack_trace = None
        if include_stack:
            stack_trace = traceback.format_exc()

        context_json = json.dumps(context) if context else None

        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, error_type, error_message, context, step_number, step_name,
                 stack_trace, url, thread_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.run_id,
                error_type,
                (message or "")[:5000],
                context_json,
                step_number,
                step_name,
                stack_trace,
                url,
                thread_id,
            ))

        self.db.commit()

    def get_errors_by_step(self, step_number: int) -> List[Dict]:
        """Get all errors for a specific step."""
        table = self._table("errors")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, error_type, error_message, url, thread_id, created_at
                FROM {table}
                WHERE run_id = %s AND step_number = %s
                ORDER BY created_at DESC
            """, (self.run_id, step_number))
            rows = cur.fetchall()
            columns = ["id", "error_type", "error_message", "url", "thread_id", "created_at"]
            return [dict(zip(columns, row)) for row in rows]

    def get_error_summary(self) -> Dict[str, int]:
        """Get error count by type for this run."""
        table = self._table("errors")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT error_type, COUNT(*) as count
                FROM {table}
                WHERE run_id = %s
                GROUP BY error_type
            """, (self.run_id,))
            rows = cur.fetchall()
            return {row[0]: row[1] for row in rows}

    def get_error_count(self) -> int:
        """Get total error count for this run."""
        table = self._table("errors")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row.get("count", 0)

    # ==================================================================
    # EXPORT FUNCTIONS (Generate CSVs from DB)
    # ==================================================================

    def export_collected_urls_csv(self, output_path: Path) -> int:
        """
        Export collected URLs to CSV file.

        Args:
            output_path: Path to output CSV file

        Returns:
            Number of rows exported
        """
        table = self._table("collected_urls")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT prefix, title, active_substance, manufacturer, document_type,
                       price_text, reimbursement, url, url_with_id, packs_scraped, error_message
                FROM {table}
                WHERE run_id = %s
                ORDER BY id
            """, (self.run_id,))
            rows = cur.fetchall()

        if not rows:
            return 0

        output_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "prefix", "title", "active_substance", "manufacturer", "document_type",
            "price_text", "reimbursement", "url", "url_with_id", "packs_scraped", "error"
        ]

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(zip(fieldnames, row)))

        self.log_export_report('collected_urls', len(rows), 'csv')
        self._db_log(f"EXPORT | collected_urls -> {output_path} | rows={len(rows)}")
        return len(rows)

    def export_packs_csv(self, output_path: Path) -> int:
        """Export packs to CSV file."""
        table = self._table("packs")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT start_date, end_date, currency, unit_price, ppp_ex_vat, ppp_vat,
                       vat_percent, reimbursable_status, reimbursable_rate, copay_price,
                       copay_percent, deductible, margin_rule, local_pack_description,
                       active_substance, manufacturer, formulation,
                       strength_size, local_pack_code, reimbursement_message, source_url
                FROM {table}
                WHERE run_id = %s
                ORDER BY id
            """, (self.run_id,))
            rows = cur.fetchall()

        if not rows:
            return 0

        output_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "Start Date", "End Date", "Currency", "Unit price", "Pharmacy Purchase Price",
            "PPP VAT", "VAT Percent", "Reimbursable Status", "Reimbursable Rate",
            "Co-Pay Price", "Copayment Percent", "Deductible", "Margin Rule",
            "Local Pack Description", "Generic Name", "Company Name", "Formulation",
            "Strength Size", "LOCAL_PACK_CODE", "Customized Column 1", "Source URL"
        ]

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(fieldnames)
            for row in rows:
                # Format start_date
                start_date = row[0]
                if start_date:
                    start_date = start_date.strftime("%d-%m-%Y") if hasattr(start_date, 'strftime') else str(start_date)
                writer.writerow([start_date or ""] + list(row[1:]))

        self.log_export_report('packs', len(rows), 'csv')
        self._db_log(f"EXPORT | packs -> {output_path} | rows={len(rows)}")
        return len(rows)

    def export_details_csv(self, output_path: Path) -> int:
        """Export details to CSV file."""
        table = self._table("details")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT detail_url, product_name, product_type, manufacturer,
                       administration_form, strengths_raw
                FROM {table}
                WHERE run_id = %s
                ORDER BY id
            """, (self.run_id,))
            rows = cur.fetchall()

        if not rows:
            return 0

        output_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "detail_url", "product_name", "product_type", "manufacturer",
            "administration_form", "strengths_raw"
        ]

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(zip(fieldnames, row)))

        self.log_export_report('details', len(rows), 'csv')
        self._db_log(f"EXPORT | details -> {output_path} | rows={len(rows)}")
        return len(rows)

    def export_costs_csv(self, output_path: Path) -> int:
        """Export costs to CSV file."""
        table = self._table("costs")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT detail_url, brand_full, brand_name, pack_presentation, ddd_text,
                       currency, price_per_day, price_per_week, price_per_month,
                       price_per_six_months, reimbursed_per_day, extra_payment_per_day,
                       table_type, unit_type, unit_amount
                FROM {table}
                WHERE run_id = %s
                ORDER BY id
            """, (self.run_id,))
            rows = cur.fetchall()

        if not rows:
            return 0

        output_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "detail_url", "brand_full", "brand_name", "pack_presentation", "ddd_text",
            "currency", "price_per_day", "price_per_week", "price_per_month",
            "price_per_six_months", "reimbursed_per_day", "extra_payment_per_day",
            "table_type", "unit_type", "unit_amount"
        ]

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(zip(fieldnames, row)))

        self.log_export_report('costs', len(rows), 'csv')
        self._db_log(f"EXPORT | costs -> {output_path} | rows={len(rows)}")
        return len(rows)

    def export_consolidated_csv(self, output_path: Path) -> int:
        """Export consolidated data to CSV file."""
        table = self._table("consolidated")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT detail_url, product_name, brand_name, manufacturer,
                       administration_form, strengths_raw, pack_presentation, currency,
                       price_per_day, reimbursed_per_day, extra_payment_per_day,
                       ddd_text, table_type, unit_type, unit_amount
                FROM {table}
                WHERE run_id = %s
                ORDER BY id
            """, (self.run_id,))
            rows = cur.fetchall()

        if not rows:
            return 0

        output_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "detail_url", "product_name", "brand_name", "manufacturer",
            "administration_form", "strengths_raw", "pack_presentation", "currency",
            "price_per_day", "reimbursed_per_day", "extra_payment_per_day",
            "ddd_text", "table_type", "unit_type", "unit_amount"
        ]

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(zip(fieldnames, row)))

        self.log_export_report('consolidated', len(rows), 'csv')
        self._db_log(f"EXPORT | consolidated -> {output_path} | rows={len(rows)}")
        return len(rows)

    def export_final_report(self, output_path: Path) -> int:
        """
        Export final report in the standard format matching the sample.
        
        Columns: PCID, Country, Company, Product Group, Local Product Name, Generic Name,
                 Indication, Pack Size, Start Date, End Date, Currency, Unit Price,
                 Pharmacy Purchase Price, PPP VAT, VAT Percent, Reimbursable Status,
                 Reimbursable Rate, Co-Pay Price, Copayment Percent, Margin Rule,
                 Local Pack Description, Formulation, Strength Size, LOCAL_PACK_CODE,
                 Customized Column 1
        """
        table = self._table("packs")
        
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    product_group,
                    manufacturer,
                    product_group as local_product_name,
                    active_substance,
                    local_pack_description,
                    formulation,
                    strength_size,
                    local_pack_code,
                    unit_price,
                    ppp_ex_vat,
                    ppp_vat,
                    vat_percent,
                    reimbursable_status,
                    reimbursable_rate,
                    copay_price,
                    copay_percent,
                    ri_with_vat,
                    margin_rule,
                    reimbursement_message,
                    start_date,
                    end_date,
                    currency
                FROM {table}
                WHERE run_id = %s
                ORDER BY id
            """, (self.run_id,))
            rows = cur.fetchall()
        
        if not rows:
            return 0
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        fieldnames = [
            "PCID", "Country", "Company", "Product Group", "Local Product Name",
            "Generic Name", "Indication", "Pack Size", "Start Date", "End Date",
            "Currency", "Unit Price", "Pharmacy Purchase Price", "PPP VAT",
            "VAT Percent", "Reimbursable Status", "Reimbursable Rate", "Co-Pay Price",
            "Copayment Percent", "RI WITH VAT", "Margin Rule", "Local Pack Description", "Formulation",
            "Strength Size", "LOCAL_PACK_CODE", "Customized Column 1"
        ]
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in rows:
                (
                    product_group, manufacturer, local_product_name, active_substance,
                    local_pack_description, formulation, strength_size, local_pack_code,
                    unit_price, ppp_ex_vat, ppp_vat, vat_percent, reimbursable_status,
                    reimbursable_rate, copay_price, copay_percent, ri_with_vat,
                    margin_rule, reimbursement_message, start_date, end_date, currency
                ) = row
                
                # Format dates
                start_date_str = start_date.strftime("%d-%m-%Y") if start_date else ""
                end_date_str = end_date.strftime("%d-%m-%Y") if end_date else ""
                
                # Format numeric values
                unit_price_str = f"{float(unit_price):.2f}" if unit_price else ""
                ppp_ex_vat_str = f"{float(ppp_ex_vat):.2f}" if ppp_ex_vat else ""
                ppp_vat_str = f"{float(ppp_vat):.2f}" if ppp_vat else ""
                vat_percent_str = f"{float(vat_percent):.0f}" if vat_percent else "9"
                copay_price_str = f"{float(copay_price):.2f}" if copay_price else ""
                ri_with_vat_str = f"{float(ri_with_vat):.2f}" if ri_with_vat else ""
                
                writer.writerow({
                    "PCID": "",  # To be filled from PCID mapping
                    "Country": "NETHERLANDS",
                    "Company": manufacturer or "",
                    "Product Group": product_group or "",
                    "Local Product Name": local_product_name or "",
                    "Generic Name": active_substance or "",
                    "Indication": "",  # To be filled from PCID mapping if available
                    "Pack Size": "1",  # As confirmed in requirements
                    "Start Date": start_date_str,
                    "End Date": end_date_str,
                    "Currency": currency or "EUR",
                    "Unit Price": unit_price_str,
                    "Pharmacy Purchase Price": ppp_ex_vat_str,
                    "PPP VAT": ppp_vat_str,
                    "VAT Percent": vat_percent_str,
                    "Reimbursable Status": reimbursable_status or "",
                    "Reimbursable Rate": reimbursable_rate or "",
                    "Co-Pay Price": copay_price_str,
                    "Copayment Percent": copay_percent or "",
                    "RI WITH VAT": ri_with_vat_str,
                    "Margin Rule": margin_rule or "632 Medicijnkosten Drugs4",
                    "Local Pack Description": local_pack_description or "",
                    "Formulation": formulation or "",
                    "Strength Size": strength_size or "",
                    "LOCAL_PACK_CODE": local_pack_code or "",
                    "Customized Column 1": reimbursement_message or ""
                })
        
        self.log_export_report('final_report', len(rows), 'csv')
        self._db_log(f"EXPORT | final_report -> {output_path} | rows={len(rows)}")
        return len(rows)

    # ==================================================================
    # STEP PROGRESS STATS
    # ==================================================================

    def get_step_progress_stats(self, step_number: int) -> Dict:
        """Get progress stats for a specific step."""
        table = self._table("step_progress")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT status, COUNT(*) as count
                FROM {table}
                WHERE run_id = %s AND step_number = %s
                GROUP BY status
            """, (self.run_id, step_number))
            rows = cur.fetchall()

            stats = {"pending": 0, "in_progress": 0, "completed": 0, "failed": 0, "skipped": 0}
            for row in rows:
                stats[row[0]] = row[1]

            stats["total"] = sum(stats.values())
            return stats

    # ==================================================================
    # SEARCH COMBINATIONS (vorm/sterkte tracking)
    # ==================================================================

    def insert_combination(self, vorm: str, sterkte: str, search_url: str) -> int:
        """
        Insert a vorm/sterkte combination.
        
        Args:
            vorm: Form value (e.g., "TABLETTEN EN CAPSULES")
            sterkte: Strength value (e.g., "10/80MG")
            search_url: Full search URL with vorm/sterkte parameters
        
        Returns:
            ID of inserted/existing combination
        """
        table = self._table("search_combinations")
        
        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, vorm, sterkte, search_url, status)
                VALUES (%s, %s, %s, %s, 'pending')
                ON CONFLICT (run_id, vorm, sterkte) DO UPDATE SET
                    search_url = EXCLUDED.search_url
                RETURNING id
            """, (self.run_id, vorm, sterkte, search_url))
            row = cur.fetchone()
            combination_id = row[0] if row else None
        
        self.db.commit()
        return combination_id

    def insert_combinations_bulk(self, combinations: List[Dict], batch_size: int = 500) -> int:
        """
        Bulk insert vorm/sterkte combinations.
        
        Args:
            combinations: List of dicts with keys: vorm, sterkte, search_url
            batch_size: Number of records per batch
        
        Returns:
            Number of combinations inserted
        """
        if not combinations:
            return 0
        
        table = self._table("search_combinations")
        count = 0
        
        sql = f"""
            INSERT INTO {table}
            (run_id, vorm, sterkte, search_url, status)
            VALUES (%s, %s, %s, %s, 'pending')
            ON CONFLICT (run_id, vorm, sterkte) DO UPDATE SET
                search_url = EXCLUDED.search_url,
                status = CASE
                    WHEN {table}.status = 'completed' THEN {table}.status
                    ELSE 'pending'
                END
        """
        
        with self.db.cursor() as cur:
            for combo in combinations:
                cur.execute(sql, (
                    self.run_id,
                    combo.get("vorm", ""),
                    combo.get("sterkte", ""),
                    combo.get("search_url", ""),
                ))
                count += 1
                
                if count % batch_size == 0:
                    self.db.commit()
        
        self.db.commit()
        self._db_log(f"OK | nl_search_combinations inserted={count} | run_id={self.run_id}")
        return count

    def get_pending_combinations(self, limit: int = 100) -> List[Dict]:
        """Get pending/collecting combinations to process (resumable after crash)."""
        table = self._table("search_combinations")
        
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, vorm, sterkte, search_url
                FROM {table}
                WHERE run_id = %s AND status IN ('pending', 'collecting')
                ORDER BY id
                LIMIT %s
            """, (self.run_id, limit))
            rows = cur.fetchall()
            
            return [
                {
                    "id": row[0],
                    "vorm": row[1],
                    "sterkte": row[2],
                    "search_url": row[3],
                }
                for row in rows
            ]
    
    def get_search_combinations(self, status: str = 'pending', limit: int = 10000) -> List[Dict]:
        """
        Get combinations by status.
        
        Args:
            status: Filter by status ('pending', 'completed', 'failed', or 'all')
            limit: Maximum number to return
        
        Returns:
            List of combination dicts
        """
        table = self._table("search_combinations")
        
        with self.db.cursor() as cur:
            if status == 'all':
                cur.execute(f"""
                    SELECT id, vorm, sterkte, search_url, status
                    FROM {table}
                    WHERE run_id = %s
                    ORDER BY id
                    LIMIT %s
                """, (self.run_id, limit))
            else:
                cur.execute(f"""
                    SELECT id, vorm, sterkte, search_url, status
                    FROM {table}
                    WHERE run_id = %s AND status = %s
                    ORDER BY id
                    LIMIT %s
                """, (self.run_id, status, limit))
            rows = cur.fetchall()
            
            return [
                {
                    "id": row[0],
                    "vorm": row[1],
                    "sterkte": row[2],
                    "search_url": row[3],
                    "status": row[4],
                }
                for row in rows
            ]

    def mark_combination_started(self, combination_id: int) -> None:
        """Mark a combination as started."""
        table = self._table("search_combinations")
        now = datetime.now()
        
        with self.db.cursor() as cur:
            cur.execute(f"""
                UPDATE {table}
                SET status = 'collecting',
                    started_at = %s
                WHERE id = %s
            """, (now, combination_id))
        self.db.commit()

    def mark_combination_completed(self, combination_id: int, 
                                   products_found: int = 0,
                                   urls_discovered: int = 0,
                                   urls_fetched: int = 0,
                                   urls_inserted: int = 0,
                                   urls_duplicate: int = 0,
                                   urls_collected: int = 0) -> None:
        """Mark a combination as completed."""
        table = self._table("search_combinations")
        now = datetime.now()
        
        with self.db.cursor() as cur:
            cur.execute(f"""
                UPDATE {table}
                SET status = 'completed',
                    products_found = %s,
                    urls_discovered = %s,
                    urls_fetched = %s,
                    urls_inserted = %s,
                    urls_duplicate = %s,
                    urls_collected = %s,
                    completed_at = %s
                WHERE id = %s
            """, (products_found, urls_discovered, urls_fetched, urls_inserted, urls_duplicate, urls_collected, now, combination_id))
        self.db.commit()

    def mark_combination_failed(self, combination_id: int, error: str) -> None:
        """Mark a combination as failed."""
        table = self._table("search_combinations")
        
        with self.db.cursor() as cur:
            cur.execute(f"""
                UPDATE {table}
                SET status = 'failed',
                    error_message = %s,
                    retry_count = retry_count + 1
                WHERE id = %s
            """, (error[:500], combination_id))
        self.db.commit()

    def get_combination_stats(self) -> Dict:
        """Get statistics on combinations."""
        table = self._table("search_combinations")
        
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN status = 'collecting' THEN 1 ELSE 0 END) as collecting,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as skipped,
                    SUM(products_found) as total_products,
                    SUM(urls_discovered) as total_urls_discovered,
                    SUM(urls_fetched) as total_urls_fetched,
                    SUM(urls_inserted) as total_urls_inserted,
                    SUM(urls_duplicate) as total_urls_duplicate,
                    SUM(urls_collected) as total_urls_collected
                FROM {table}
                WHERE run_id = %s
            """, (self.run_id,))
            row = cur.fetchone()
            
            if not row:
                return {
                    "total": 0, "pending": 0, "collecting": 0,
                    "completed": 0, "failed": 0, "skipped": 0,
                    "total_products": 0,
                    "total_urls_discovered": 0, "total_urls_fetched": 0,
                    "total_urls_inserted": 0, "total_urls_collected": 0
                }
            
            return {
                "total": row[0] or 0,
                "pending": row[1] or 0,
                "collecting": row[2] or 0,
                "completed": row[3] or 0,
                "failed": row[4] or 0,
                "skipped": row[5] or 0,
                    "total_products": row[6] or 0,
                    "total_urls_discovered": row[7] or 0,
                    "total_urls_fetched": row[8] or 0,
                    "total_urls_inserted": row[9] or 0,
                    "total_urls_duplicate": row[10] or 0,
                    "total_urls_collected": row[11] or 0,
            }

    def get_all_combinations(self) -> List[Dict]:
        """Get all combinations for this run."""
        table = self._table("search_combinations")
        
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, vorm, sterkte, search_url, status,
                       urls_discovered, urls_fetched, urls_inserted, urls_duplicate, products_found, urls_collected, error_message,
                       created_at, started_at, completed_at
                FROM {table}
                WHERE run_id = %s
                ORDER BY id
            """, (self.run_id,))
            rows = cur.fetchall()
            
            return [
                {
                    "id": row[0],
                    "vorm": row[1],
                    "sterkte": row[2],
                    "search_url": row[3],
                    "status": row[4],
                    "urls_discovered": row[5] or 0,
                    "urls_fetched": row[6] or 0,
                    "urls_inserted": row[7] or 0,
                    "urls_duplicate": row[8] or 0,
                    "products_found": row[9] or 0,
                    "urls_collected": row[10] or 0,
                    "error_message": row[11],
                    "created_at": row[12],
                    "started_at": row[13],
                    "completed_at": row[14],
                }
                for row in rows
            ]
