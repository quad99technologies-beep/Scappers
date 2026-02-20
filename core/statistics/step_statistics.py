"""
Shared per-step statistics persistence.

Stores a JSON snapshot per (scraper_name, run_id, step_number) in the shared
scraper_step_statistics table.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from core.db.tracking import ensure_run_ledger_row

logger = logging.getLogger(__name__)


_STEP_STATS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS scraper_step_statistics (
    id SERIAL PRIMARY KEY,
    scraper_name TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    step_number INTEGER NOT NULL,
    step_name TEXT,
    status TEXT,
    error_message TEXT,
    stats_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scraper_name, run_id, step_number)
);
CREATE INDEX IF NOT EXISTS idx_sss_scraper ON scraper_step_statistics(scraper_name);
CREATE INDEX IF NOT EXISTS idx_sss_run ON scraper_step_statistics(run_id);
CREATE INDEX IF NOT EXISTS idx_sss_step ON scraper_step_statistics(scraper_name, step_number);
"""

_ddl_ensured = False


def upsert_step_statistics(
    db,
    scraper_name: str,
    run_id: str,
    step_number: int,
    step_name: Optional[str],
    status: Optional[str],
    error_message: Optional[str],
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Upsert a per-step stats snapshot (best-effort).

    stats is stored as JSONB. Callers should pass simple JSON-serializable values.
    """
    if not db or not scraper_name or not run_id:
        return
    stats = stats or {}

    try:
        ensure_run_ledger_row(db, run_id, scraper_name, mode="resume")
    except Exception:
        pass

    try:
        # Defensive: create table if schema wasn't applied yet.
        global _ddl_ensured
        if not _ddl_ensured:
            try:
                db.executescript(_STEP_STATS_TABLE_DDL)
                _ddl_ensured = True
            except Exception:
                pass

        payload = dict(stats)
        payload.setdefault("updated_at", datetime.utcnow().isoformat())

        with db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scraper_step_statistics
                    (scraper_name, run_id, step_number, step_name, status, error_message, stats_json, recorded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (scraper_name, run_id, step_number)
                DO UPDATE SET
                    step_name = COALESCE(EXCLUDED.step_name, scraper_step_statistics.step_name),
                    status = COALESCE(EXCLUDED.status, scraper_step_statistics.status),
                    error_message = EXCLUDED.error_message,
                    stats_json = EXCLUDED.stats_json,
                    recorded_at = CURRENT_TIMESTAMP
                """,
                (
                    scraper_name,
                    run_id,
                    step_number,
                    step_name,
                    status,
                    error_message,
                    json.dumps(payload, default=str),
                ),
            )
    except Exception as e:
        logger.debug("Could not upsert scraper_step_statistics (non-fatal): %s", e)
