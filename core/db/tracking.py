"""
Best-effort tracking helpers for shared tables.

These helpers are intentionally non-blocking: failures should never break scraping.
They also guard FK constraints by ensuring a run_ledger row exists (insert-if-missing),
without mutating existing run_ledger status/mode.
"""

from __future__ import annotations

import logging
from typing import Optional

from core.db.models import run_ledger_insert_if_missing

logger = logging.getLogger(__name__)


def ensure_run_ledger_row(db, run_id: str, scraper_name: str, mode: str = "resume") -> None:
    """Insert a minimal run_ledger row if missing (no-op if already present)."""
    if not run_id or not scraper_name or db is None:
        return
    try:
        sql, params = run_ledger_insert_if_missing(run_id, scraper_name, mode=mode)
        with db.cursor() as cur:
            cur.execute(sql, params)
    except Exception as e:
        logger.debug("run_ledger insert-if-missing failed (non-fatal): %s", e)


def log_http_request(
    db,
    run_id: str,
    scraper_name: str,
    url: str,
    method: str = "GET",
    status_code: Optional[int] = None,
    response_bytes: Optional[int] = None,
    elapsed_ms: Optional[float] = None,
    error: Optional[str] = None,
    ensure_run: bool = True,
    mode_if_missing: str = "resume",
) -> None:
    """Log a request to the shared http_requests table (best-effort)."""
    if not db or not run_id or not url:
        return
    if ensure_run:
        ensure_run_ledger_row(db, run_id, scraper_name, mode=mode_if_missing)
    try:
        with db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO http_requests
                (run_id, url, method, status_code, response_bytes, elapsed_ms, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (run_id, url, method, status_code, response_bytes, elapsed_ms, error),
            )
    except Exception as e:
        logger.debug("http_requests insert failed (non-fatal): %s", e)

