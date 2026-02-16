
#!/usr/bin/env python3
"""
Italy database repository - all DB access in one place.
"""

import logging
from typing import Dict, List, Optional, Set
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class ItalyRepository:
    """All database operations for Italy scraper (PostgreSQL backend)."""


    def __init__(self, db, run_id: str):
        self.db = db
        self.run_id = run_id

    def _db_log(self, message: str) -> None:
        try:
            print(f"[DB] {message}", flush=True)
        except Exception:
            pass

    def _table(self, name: str) -> str:
        return f"it_{name}"


    def start_run(self, mode: str = "fresh") -> None:
        """Register a new run in run_ledger."""
        try:
            from core.db.models import run_ledger_start
            sql, params = run_ledger_start(self.run_id, "Italy", mode=mode)
            with self.db.cursor() as cur:
                cur.execute(sql, params)
            self._db_log(f"Started run {self.run_id}")
        except Exception as e:
            logger.warning(f"Could not register run in ledger: {e}")

    def ensure_run_in_ledger(self, mode: str = "resume") -> None:
        """Ensure this run_id exists in run_ledger."""
        try:
            from core.db.models import run_ledger_ensure_exists
            sql, params = run_ledger_ensure_exists(self.run_id, "Italy", mode=mode)
            with self.db.cursor() as cur:
                cur.execute(sql, params)
        except Exception as e:
            logger.warning(f"Could not ensure run in ledger: {e}")


    # ------------------------------------------------------------------
    # Step 1: Determinas
    # ------------------------------------------------------------------

    def insert_determinas(self, items: List[Dict]) -> int:
        if not items:
            return 0
        table = self._table("determinas")
        count = 0
        with self.db.cursor() as cur:
            for item in items:
                # Parse date if possible
                pub_date = item.get("dataPubblicazione")
                # Format is usually ISO
                
                cur.execute(f"""
                    INSERT INTO {table}
                    (run_id, determina_id, title, publish_date, typology, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, determina_id) DO NOTHING
                """, (
                    self.run_id,
                    item.get("id"),
                    item.get("titolo"),
                    pub_date,
                    item.get("tipologia"),
                    json.dumps(item)
                ))
                count += cur.rowcount # Only counts actual inserts
        self._db_log(f"Inserted {count} determinas for run_id={self.run_id}")
        return count

    def get_determina_ids(self) -> Set[str]:
        table = self._table("determinas")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT determina_id FROM {table} WHERE run_id = %s", (self.run_id,))
            return {row[0] for row in cur.fetchall()}
            
    def get_determinas(self) -> List[Dict]:
        table = self._table("determinas")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Step 3: Products / Extraction
    # ------------------------------------------------------------------

    def insert_products(self, products: List[Dict]) -> int:
        if not products:
            return 0
        table = self._table("products")
        count = 0
        with self.db.cursor() as cur:
            for p in products:
                cur.execute(f"""
                    INSERT INTO {table}
                    (run_id, determina_id, aic_code, product_name, pack_description, 
                     price_ex_factory, price_public, source_pdf, company)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    self.run_id,
                    p.get("determina_id"),
                    p.get("aic"),
                    p.get("product_name"),
                    p.get("pack_description"),
                    p.get("price_ex_factory"),
                    p.get("price_public"),
                    p.get("source_pdf"),
                    p.get("company")
                ))
                count += 1
        self._db_log(f"Inserted {count} products for run_id={self.run_id}")
        return count

    def get_products_for_export(self) -> List[Dict]:
        table = self._table("products")
        dt_table = self._table("determinas")
        
        with self.db.cursor(dict_cursor=True) as cur:
            # Join with determinas to get dates
            cur.execute(f"""
                SELECT p.*, d.publish_date
                FROM {table} p
                LEFT JOIN {dt_table} d ON p.determina_id = d.determina_id AND d.run_id = p.run_id
                WHERE p.run_id = %s
            """, (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Progress
    # ------------------------------------------------------------------

    def mark_progress(self, step_number: int, step_name: str, progress_key: str, status: str, error_message: str = None) -> None:
        table = self._table("step_progress")
        now = datetime.now().isoformat()
        with self.db.cursor() as cur:
             cur.execute(f"""
                INSERT INTO {table} (run_id, step_number, step_name, progress_key, status, error_message, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    completed_at = CASE WHEN EXCLUDED.status IN ('completed', 'failed') THEN %s ELSE {table}.completed_at END
            """, (self.run_id, step_number, step_name, progress_key, status, error_message, now, now, now))

    def is_progress_completed(self, step_number: int, progress_key: str) -> bool:
        table = self._table("step_progress")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT status FROM {table} WHERE run_id = %s AND step_number = %s AND progress_key = %s", 
            (self.run_id, step_number, progress_key))
            row = cur.fetchone()
            return row and row[0] == 'completed'
            

    def clear_step_data(self, step: int) -> None:
        with self.db.cursor() as cur:
            if step == 1:
                cur.execute(f"DELETE FROM {self._table('determinas')} WHERE run_id = %s", (self.run_id,))
            elif step == 3:
                cur.execute(f"DELETE FROM {self._table('products')} WHERE run_id = %s", (self.run_id,))
            # Also clear progress
            cur.execute(f"DELETE FROM {self._table('step_progress')} WHERE run_id = %s AND step_number = %s", (self.run_id, step))

