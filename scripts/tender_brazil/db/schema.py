#!/usr/bin/env python3
"""
Tender Brazil-specific database schema (PostgreSQL).
"""

import sys
from pathlib import Path

# Add repo root to path for core imports
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

TENDER_LIST_DDL = """
CREATE TABLE IF NOT EXISTS br_tender_list (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    cn_number TEXT NOT NULL,
    metadata JSONB,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, cn_number)
);
CREATE INDEX IF NOT EXISTS idx_br_list_run ON br_tender_list(run_id);
CREATE INDEX IF NOT EXISTS idx_br_list_cn ON br_tender_list(cn_number);
"""

TENDER_DETAILS_DDL = """
CREATE TABLE IF NOT EXISTS br_tender_details (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    cn_number TEXT NOT NULL,
    source_tender_id TEXT,
    tender_title TEXT,
    province TEXT,
    authority TEXT,
    purchasing_unit TEXT,
    status TEXT,
    publication_date TEXT,
    deadline_date TEXT,
    currency TEXT DEFAULT 'BRL',
    contract_type TEXT,
    legal_basis TEXT,
    budget_source TEXT,
    notice_link TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, cn_number)
);
CREATE INDEX IF NOT EXISTS idx_br_details_run ON br_tender_details(run_id);
CREATE INDEX IF NOT EXISTS idx_br_details_cn ON br_tender_details(cn_number);
"""

TENDER_AWARDS_DDL = """
CREATE TABLE IF NOT EXISTS br_tender_awards (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    cn_number TEXT NOT NULL,
    item_no TEXT,
    lot_number TEXT,
    lot_title TEXT,
    awarded_lot_title TEXT,
    est_lot_value_local REAL,
    ceiling_price_mg_iu REAL,
    ceiling_unit_price REAL,
    meat TEXT,
    price_eval_ratio REAL,
    quality_eval_ratio REAL,
    other_eval_ratio REAL,
    award_date TEXT,
    bidder TEXT,
    bidder_id TEXT,
    bid_status TEXT,
    awarded_qty REAL,
    awarded_unit_price REAL,
    lot_award_value_local REAL,
    ranking_order TEXT,
    price_eval REAL,
    quality_eval REAL,
    other_eval REAL,
    basic_productive_incentive TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_br_awards_run ON br_tender_awards(run_id);
CREATE INDEX IF NOT EXISTS idx_br_awards_cn ON br_tender_awards(cn_number);
"""

STEP_PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS br_step_progress (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    step_number INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    progress_key TEXT NOT NULL,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    UNIQUE(run_id, step_number, progress_key)
);
CREATE INDEX IF NOT EXISTS idx_br_progress_run_step ON br_step_progress(run_id, step_number);
"""

BRAZIL_SCHEMA_DDL = [
    TENDER_LIST_DDL,
    TENDER_DETAILS_DDL,
    TENDER_AWARDS_DDL,
    STEP_PROGRESS_DDL,
]

def apply_brazil_schema(db) -> None:
    from core.db.models import apply_common_schema
    apply_common_schema(db)
    for ddl in BRAZIL_SCHEMA_DDL:
        db.executescript(ddl)
    try:
        db.commit()
    except Exception:
        pass
