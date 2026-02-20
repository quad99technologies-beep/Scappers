
#!/usr/bin/env python3
"""
Italy database repository - all DB access in one place.
"""

import sys
from pathlib import Path

# Add repo root to path for core imports (MUST be before any core imports)
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import logging
from typing import Dict, List, Optional, Set

import json

logger = logging.getLogger(__name__)

from core.db.base_repository import BaseRepository


class ItalyRepository(BaseRepository):
    """All database operations for Italy scraper (PostgreSQL backend)."""

    SCRAPER_NAME = "Italy"
    TABLE_PREFIX = "it"

    def __init__(self, db, run_id: str):
        super().__init__(db, run_id)


    # ------------------------------------------------------------------
    # Step 1: Determinas
    # ------------------------------------------------------------------

    def insert_determinas(self, items: List[Dict], source_keyword: Optional[str] = None) -> int:
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
                    (run_id, determina_id, source_keyword, title, publish_date, typology, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (run_id, determina_id) DO NOTHING
                """, (
                    self.run_id,
                    item.get("id"),
                    source_keyword,
                    item.get("titolo"),
                    pub_date,
                    item.get("tipologia"),
                    json.dumps(item)
                ))
                if cur.rowcount:
                    count += 1  # Only counts actual inserts
                elif source_keyword:
                    # If the row already exists, keep the first non-null keyword we ever saw.
                    cur.execute(
                        f"""
                        UPDATE {table}
                        SET source_keyword = %s
                        WHERE run_id = %s AND determina_id = %s AND source_keyword IS NULL
                        """,
                        (source_keyword, self.run_id, item.get("id")),
                    )
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

    def update_determina_detail(self, determina_id: str, detail: Dict) -> None:
        """Persist Step 2 detail payload in DB (no JSON files)."""
        table = self._table("determinas")
        with self.db.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {table}
                SET detail = %s::jsonb, detail_scraped_at = NOW()
                WHERE run_id = %s AND determina_id = %s
                """,
                (json.dumps(detail) if detail is not None else None, self.run_id, determina_id),
            )

    def get_determina_details_by_typology(self, typology: str) -> List[Dict]:
        """Return determinas with stored detail payload for a given typology."""
        table = self._table("determinas")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(
                f"""
                SELECT determina_id, publish_date, typology, source_keyword, detail
                FROM {table}
                WHERE run_id = %s AND typology = %s AND detail IS NOT NULL
                """,
                (self.run_id, typology),
            )
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
                SELECT p.*, d.publish_date, d.typology, d.source_keyword
                FROM {table} p
                LEFT JOIN {dt_table} d ON p.determina_id = d.determina_id AND d.run_id = p.run_id
                WHERE p.run_id = %s
            """, (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Progress tracking (Step 1: month-window granularity)
    # ------------------------------------------------------------------

    def mark_month_progress(self, step_number: int, step_name: str,
                            progress_key: str, status: str,
                            records_fetched: int = 0,
                            api_total_count: int = 0,
                            error_message: str = None) -> None:
        """
        Mark a month-window as in_progress / completed / failed,
        storing the number of records fetched vs found by API.
        """
        from datetime import datetime
        now = datetime.now()
        table = self._table("step_progress")
        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, step_number, step_name, progress_key, status,
                 records_fetched, api_total_count, error_message, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                    status          = EXCLUDED.status,
                    records_fetched = EXCLUDED.records_fetched,
                    api_total_count = EXCLUDED.api_total_count,
                    error_message   = EXCLUDED.error_message,
                    started_at      = CASE
                        WHEN EXCLUDED.status = 'in_progress' THEN EXCLUDED.started_at
                        WHEN {table}.started_at IS NULL      THEN EXCLUDED.started_at
                        ELSE {table}.started_at
                    END,
                    completed_at    = CASE
                        WHEN EXCLUDED.status IN ('completed', 'failed', 'skipped')
                             THEN EXCLUDED.completed_at
                        WHEN EXCLUDED.status = 'in_progress' THEN NULL
                        ELSE {table}.completed_at
                    END
            """, (
                self.run_id, step_number, step_name, progress_key, status,
                records_fetched, api_total_count, error_message,
                now,
                now if status in ("completed", "failed", "skipped") else None,
            ))
        self.db.commit()

    def get_step1_summary(self) -> dict:
        """
        Return aggregate progress stats for Step 1 in this run.
        """
        table = self._table("step_progress")
        with self.db.cursor() as cur:
            # Overall counts
            cur.execute(f"""
                SELECT status, COUNT(*) as cnt,
                       COALESCE(SUM(records_fetched), 0) as saved,
                       COALESCE(SUM(api_total_count), 0) as found
                FROM {table}
                WHERE run_id = %s AND step_number = 1
                GROUP BY status
            """, (self.run_id,))
            rows = cur.fetchall()

        summary = {"completed": 0, "failed": 0, "in_progress": 0, "pending": 0,
                   "total_saved": 0, "total_found": 0, "by_keyword": {}}

        for row in rows:
            status, cnt, saved, found = row[0], int(row[1]), int(row[2]), int(row[3])
            summary[status] = cnt
            if status == "completed":
                summary["total_saved"] += saved
                summary["total_found"] += found

        # Per-keyword breakdown
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT
                    SPLIT_PART(progress_key, ':', 1) AS keyword,
                    status,
                    COUNT(*) AS cnt,
                    COALESCE(SUM(records_fetched), 0) AS saved,
                    COALESCE(SUM(api_total_count), 0) AS found
                FROM {table}
                WHERE run_id = %s AND step_number = 1
                GROUP BY keyword, status
            """, (self.run_id,))
            kw_rows = cur.fetchall()

        for row in kw_rows:
            kw, status, cnt, saved, found = row[0], row[1], int(row[2]), int(row[3]), int(row[4])
            if kw not in summary["by_keyword"]:
                summary["by_keyword"][kw] = {"completed": 0, "failed": 0,
                                             "in_progress": 0, "total_saved": 0,
                                             "total_found": 0}
            summary["by_keyword"][kw][status] = cnt
            if status == "completed":
                summary["by_keyword"][kw]["total_saved"] += saved
                summary["by_keyword"][kw]["total_found"] += found

        return summary


    # ------------------------------------------------------------------
    # Run Statistics (it_run_stats)
    # ------------------------------------------------------------------

    def upsert_stat(self, keyword: str, step_number: int, metric_name: str, metric_value: int) -> None:
        table = self._table("run_stats")
        with self.db.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table} (run_id, keyword, step_number, metric_name, metric_value, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (run_id, keyword, step_number, metric_name) DO UPDATE SET
                    metric_value = EXCLUDED.metric_value,
                    updated_at = NOW()
                """,
                (self.run_id, keyword, step_number, metric_name, int(metric_value)),
            )

    def refresh_step1_keyword_stats(self, keyword: str) -> None:
        """
        Aggregate month-window stats for Step 1 for a given keyword (DET/Riduzione).
        Stores results in it_run_stats.
        """
        progress_table = self._table("step_progress")
        like_pat = f"{keyword}:%"

        with self.db.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    status,
                    COUNT(*) AS cnt,
                    COALESCE(SUM(records_fetched), 0) AS saved,
                    COALESCE(SUM(api_total_count), 0) AS found
                FROM {progress_table}
                WHERE run_id = %s AND step_number = 1 AND progress_key LIKE %s
                GROUP BY status
                """,
                (self.run_id, like_pat),
            )
            rows = cur.fetchall()

        status_counts = {"pending": 0, "in_progress": 0, "completed": 0, "failed": 0, "skipped": 0}
        total_saved = 0
        total_found = 0
        for status, cnt, saved, found in rows:
            status_counts[str(status)] = int(cnt)
            if str(status) == "completed":
                total_saved += int(saved)
                total_found += int(found)

        self.upsert_stat(keyword, 1, "months_completed", status_counts["completed"])
        self.upsert_stat(keyword, 1, "months_failed", status_counts["failed"])
        self.upsert_stat(keyword, 1, "months_in_progress", status_counts["in_progress"])
        self.upsert_stat(keyword, 1, "months_pending", status_counts["pending"])
        self.upsert_stat(keyword, 1, "records_saved", total_saved)
        self.upsert_stat(keyword, 1, "records_found", total_found)

    def refresh_step2_stats_by_keyword(self) -> None:
        """
        Aggregate Step 2 (Download Sources) status counts per keyword.
        Stores results in it_run_stats.
        """
        sp = self._table("step_progress")
        dt = self._table("determinas")
        with self.db.cursor() as cur:
            cur.execute(
                f"""
                SELECT COALESCE(d.source_keyword, '') AS keyword, sp.status, COUNT(*) AS cnt
                FROM {sp} sp
                JOIN {dt} d
                  ON d.run_id = sp.run_id AND d.determina_id = sp.progress_key
                WHERE sp.run_id = %s AND sp.step_number = 2
                GROUP BY COALESCE(d.source_keyword, ''), sp.status
                """,
                (self.run_id,),
            )
            rows = cur.fetchall()

        per_kw: Dict[str, Dict[str, int]] = {}
        for keyword, status, cnt in rows:
            kw = str(keyword or "").strip() or "UNKNOWN"
            st = str(status)
            per_kw.setdefault(kw, {})
            per_kw[kw][st] = int(cnt)

        for kw, m in per_kw.items():
            self.upsert_stat(kw, 2, "completed", m.get("completed", 0))
            self.upsert_stat(kw, 2, "failed", m.get("failed", 0))
            self.upsert_stat(kw, 2, "in_progress", m.get("in_progress", 0))
            self.upsert_stat(kw, 2, "pending", m.get("pending", 0))
            self.upsert_stat(kw, 2, "skipped", m.get("skipped", 0))

    def refresh_step3_product_counts_by_keyword(self) -> None:
        """
        Aggregate Step 3 extracted products count per keyword.
        Stores results in it_run_stats.
        """
        prod = self._table("products")
        dt = self._table("determinas")
        with self.db.cursor() as cur:
            cur.execute(
                f"""
                SELECT COALESCE(d.source_keyword, '') AS keyword, COUNT(*) AS cnt
                FROM {prod} p
                JOIN {dt} d
                  ON d.run_id = p.run_id AND d.determina_id = p.determina_id
                WHERE p.run_id = %s
                GROUP BY COALESCE(d.source_keyword, '')
                """,
                (self.run_id,),
            )
            rows = cur.fetchall()

        for keyword, cnt in rows:
            kw = str(keyword or "").strip() or "UNKNOWN"
            self.upsert_stat(kw, 3, "products", int(cnt))


    def clear_step_data(self, step: int) -> None:
        with self.db.cursor() as cur:
            if step == 1:
                cur.execute(f"DELETE FROM {self._table('determinas')} WHERE run_id = %s", (self.run_id,))
            elif step == 3:
                cur.execute(f"DELETE FROM {self._table('products')} WHERE run_id = %s", (self.run_id,))
            # Also clear progress
            cur.execute(f"DELETE FROM {self._table('step_progress')} WHERE run_id = %s AND step_number = %s", (self.run_id, step))

