#!/usr/bin/env python3
"""
Canada Quebec database repository - all DB access in one place.

Provides methods for:
- Inserting/querying annexe drug pricing data (IV.1, IV.2, V)
- Sub-step progress tracking
- Run lifecycle management
- Export report tracking
"""

import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

# Add repo root to path for core imports (MUST be before any core imports)
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class CanadaQuebecRepository(BaseRepository):
    """All database operations for Canada Quebec scraper (PostgreSQL backend)."""

    SCRAPER_NAME = "CanadaQuebec"
    TABLE_PREFIX = "cq"

    _STEP_TABLE_MAP = {
        1: ("annexe_data",),
        2: ("annexe_data",),
        3: ("annexe_data",),
        4: ("annexe_data",),
        5: ("annexe_data",),
        6: ("annexe_data",),
    }

    def __init__(self, db, run_id: str):
        super().__init__(db, run_id)

    # clear_step_data, lifecycle, and progress methods inherited from BaseRepository

    # ------------------------------------------------------------------
    # Annexe Data (Steps 1-6)
    # ------------------------------------------------------------------

    def insert_annexe_data(self, data: List[Dict]) -> int:
        """Bulk insert annexe drug pricing data."""
        if not data:
            return 0

        table = self._table("annexe_data")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, annexe_type, generic_name, formulation, strength,
             fill_size, din, brand, manufacturer, price, price_type,
             currency, local_pack_code, local_pack_description, source_page)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, din, annexe_type) DO UPDATE SET
                generic_name = EXCLUDED.generic_name,
                formulation = EXCLUDED.formulation,
                strength = EXCLUDED.strength,
                fill_size = EXCLUDED.fill_size,
                brand = EXCLUDED.brand,
                manufacturer = EXCLUDED.manufacturer,
                price = EXCLUDED.price,
                price_type = EXCLUDED.price_type,
                currency = EXCLUDED.currency,
                local_pack_code = EXCLUDED.local_pack_code,
                local_pack_description = EXCLUDED.local_pack_description,
                source_page = EXCLUDED.source_page,
                scraped_at = CURRENT_TIMESTAMP
        """

        with self.db.cursor() as cur:
            for row in data:
                cur.execute(sql, (
                    self.run_id,
                    row.get("annexe_type"),
                    row.get("generic_name"),
                    row.get("formulation"),
                    row.get("strength"),
                    row.get("fill_size"),
                    row.get("din"),
                    row.get("brand"),
                    row.get("manufacturer"),
                    row.get("price"),
                    row.get("price_type"),
                    row.get("currency", "CAD"),
                    row.get("local_pack_code"),
                    row.get("local_pack_description"),
                    row.get("source_page"),
                ))
                count += 1

        logger.info("Inserted %d annexe data entries", count)
        self._db_log(f"OK | cq_annexe_data inserted={count} | run_id={self.run_id}")
        return count

    def get_annexe_data_count(self) -> int:
        """Get total annexe data entries for this run."""
        table = self._table("annexe_data")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_annexe_data(self) -> List[Dict]:
        """Get all annexe data entries as list of dicts."""
        table = self._table("annexe_data")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_annexe_data_by_type(self, annexe_type: str) -> List[Dict]:
        """Get annexe data filtered by annexe type (e.g. 'IV.1', 'IV.2', 'V')."""
        table = self._table("annexe_data")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s AND annexe_type = %s",
                       (self.run_id, annexe_type))
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
        table = self._table("annexe_data")
        stats = {
            "annexe_data_total": self.get_annexe_data_count(),
        }

        # Break down by annexe type
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT annexe_type, COUNT(*) as cnt
                FROM {table}
                WHERE run_id = %s
                GROUP BY annexe_type
                ORDER BY annexe_type
            """, (self.run_id,))
            rows = cur.fetchall()
            for row in rows:
                atype = row[0] if isinstance(row, tuple) else row["annexe_type"]
                cnt = row[1] if isinstance(row, tuple) else row["cnt"]
                stats[f"annexe_{atype}"] = cnt

        return stats
