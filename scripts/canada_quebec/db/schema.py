#!/usr/bin/env python3
"""
Canada Quebec-specific database schema (PostgreSQL).

Tables:
- cq_annexe_data: Extracted drug pricing data from PDF annexes (Steps 1-6)
- cq_step_progress: Sub-step resume tracking (all steps)
- cq_export_reports: Generated export/report tracking
- cq_errors: Error tracking for pipeline runs
"""

import sys
from pathlib import Path

# Add repo root to path for core imports (MUST be before any core imports)
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# PostgreSQL uses SERIAL instead of AUTOINCREMENT
# PostgreSQL uses CURRENT_TIMESTAMP instead of datetime('now')
# PostgreSQL uses ON CONFLICT DO UPDATE/NOTHING instead of ON CONFLICT REPLACE

ANNEXE_DATA_DDL = """
CREATE TABLE IF NOT EXISTS cq_annexe_data (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    -- Annexe identification
    annexe_type TEXT NOT NULL,
    -- Drug identification
    generic_name TEXT,
    formulation TEXT,
    strength TEXT,
    fill_size TEXT,
    din TEXT,
    brand TEXT,
    manufacturer TEXT,
    -- Pricing (in CAD - Canadian Dollar)
    price REAL,
    price_type TEXT,
    currency TEXT DEFAULT 'CAD',
    -- Pack info
    local_pack_code TEXT,
    local_pack_description TEXT,
    -- Source tracking
    source_page INTEGER,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, din, annexe_type)
);
CREATE INDEX IF NOT EXISTS idx_cq_annexe_run ON cq_annexe_data(run_id);
CREATE INDEX IF NOT EXISTS idx_cq_annexe_type ON cq_annexe_data(annexe_type);
CREATE INDEX IF NOT EXISTS idx_cq_annexe_din ON cq_annexe_data(din);
CREATE INDEX IF NOT EXISTS idx_cq_annexe_generic ON cq_annexe_data(generic_name);
"""

STEP_PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS cq_step_progress (
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
CREATE INDEX IF NOT EXISTS idx_cq_progress_run_step ON cq_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_cq_progress_status ON cq_step_progress(status);
"""

EXPORT_REPORTS_DDL = """
CREATE TABLE IF NOT EXISTS cq_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT,
    row_count INTEGER,
    export_format TEXT DEFAULT 'db',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cq_export_reports_run ON cq_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_cq_export_reports_type ON cq_export_reports(report_type);
"""

ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS cq_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cq_errors_run ON cq_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_cq_errors_step ON cq_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_cq_errors_type ON cq_errors(error_type);
"""

CANADA_QUEBEC_SCHEMA_DDL = [
    ANNEXE_DATA_DDL,
    STEP_PROGRESS_DDL,
    EXPORT_REPORTS_DDL,
    ERRORS_DDL,
]


def apply_canada_quebec_schema(db) -> None:
    """Apply all Canada Quebec-specific DDL to a CountryDB instance."""
    from core.db.models import apply_common_schema
    apply_common_schema(db)
    for ddl in CANADA_QUEBEC_SCHEMA_DDL:
        db.executescript(ddl)
