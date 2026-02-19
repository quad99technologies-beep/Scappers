#!/usr/bin/env python3
"""
Tender Chile-specific database schema (PostgreSQL).

Tables:
- tc_tender_redirects: Redirect URLs from tender list (Step 1)
- tc_tender_details: Tender details from MercadoPublico (Step 2)
- tc_tender_awards: Tender award information (Step 3)
- tc_step_progress: Sub-step resume tracking (all steps)
- tc_export_reports: Generated export/report tracking
- tc_final_output: Final merged tender output (EVERSANA format)
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

TENDER_REDIRECTS_DDL = """
CREATE TABLE IF NOT EXISTS tc_tender_redirects (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    tender_id TEXT NOT NULL,
    redirect_url TEXT,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, tender_id)
);
CREATE INDEX IF NOT EXISTS idx_tc_redirects_run ON tc_tender_redirects(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_redirects_tender ON tc_tender_redirects(tender_id);
"""

TENDER_DETAILS_DDL = """
CREATE TABLE IF NOT EXISTS tc_tender_details (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    tender_id TEXT NOT NULL,
    tender_name TEXT,
    tender_status TEXT,
    publication_date TEXT,
    closing_date TEXT,
    organization TEXT,
    contact_info TEXT,
    description TEXT,
    currency TEXT DEFAULT 'CLP',
    estimated_amount REAL,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, tender_id)
);
CREATE INDEX IF NOT EXISTS idx_tc_details_run ON tc_tender_details(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_details_tender ON tc_tender_details(tender_id);
CREATE INDEX IF NOT EXISTS idx_tc_details_status ON tc_tender_details(tender_status);
"""

TENDER_AWARDS_DDL = """
CREATE TABLE IF NOT EXISTS tc_tender_awards (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    tender_id TEXT NOT NULL,
    lot_number TEXT,
    lot_title TEXT,
    un_classification_code TEXT,
    buyer_specifications TEXT,
    lot_quantity TEXT,
    supplier_name TEXT,
    supplier_rut TEXT,
    supplier_specifications TEXT,
    unit_price_offer REAL,
    awarded_quantity REAL,
    total_net_awarded REAL,
    award_amount REAL,
    award_date TEXT,
    award_status TEXT,
    is_awarded BOOLEAN,
    awarded_unit_price REAL,
    source_url TEXT,
    source_tender_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tc_awards_run ON tc_tender_awards(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_awards_tender ON tc_tender_awards(tender_id);
CREATE INDEX IF NOT EXISTS idx_tc_awards_supplier ON tc_tender_awards(supplier_name);
"""

# Final output table - EVERSANA format, no CSV reference
FINAL_OUTPUT_DDL = """
CREATE TABLE IF NOT EXISTS tc_final_output (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    -- Tender identification
    tender_id TEXT NOT NULL,
    tender_name TEXT,
    tender_status TEXT,
    -- Organization
    organization TEXT,
    contact_info TEXT,
    -- Lot/Award info
    lot_number TEXT,
    lot_title TEXT,
    -- Supplier info
    supplier_name TEXT,
    supplier_rut TEXT,
    -- Financial
    currency TEXT DEFAULT 'CLP',
    estimated_amount REAL,
    award_amount REAL,
    -- Dates
    publication_date TEXT,
    closing_date TEXT,
    award_date TEXT,
    -- Description
    description TEXT,
    -- Source tracking
    source_url TEXT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, tender_id, lot_number, supplier_rut)
);
CREATE INDEX IF NOT EXISTS idx_tc_final_output_run ON tc_final_output(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_final_output_tender ON tc_final_output(tender_id);
CREATE INDEX IF NOT EXISTS idx_tc_final_output_status ON tc_final_output(tender_status);
CREATE INDEX IF NOT EXISTS idx_tc_final_output_supplier ON tc_final_output(supplier_name);
"""

STEP_PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS tc_step_progress (
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
CREATE INDEX IF NOT EXISTS idx_tc_progress_run_step ON tc_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_tc_progress_status ON tc_step_progress(status);
"""

EXPORT_REPORTS_DDL = """
CREATE TABLE IF NOT EXISTS tc_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT,
    row_count INTEGER,
    export_format TEXT DEFAULT 'db',  -- 'db' means database-only, no file
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tc_export_reports_run ON tc_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_export_reports_type ON tc_export_reports(report_type);
"""

ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS tc_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tc_errors_run ON tc_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_errors_step ON tc_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_tc_errors_type ON tc_errors(error_type);
"""

CHILE_SCHEMA_DDL = [
    TENDER_REDIRECTS_DDL,
    TENDER_DETAILS_DDL,
    TENDER_AWARDS_DDL,
    FINAL_OUTPUT_DDL,
    STEP_PROGRESS_DDL,
    EXPORT_REPORTS_DDL,
    ERRORS_DDL,
]


def apply_chile_schema(db) -> None:
    """Apply all Chile-specific DDL to a CountryDB instance.
    Also applies inputs.sql so tc_input_tender_list exists (no CSV input).
    """
    from core.db.models import apply_common_schema
    from pathlib import Path
    from core.db.schema_registry import SchemaRegistry
    
    apply_common_schema(db)
    for ddl in CHILE_SCHEMA_DDL:
        db.executescript(ddl)
    
    # Ensure input table tc_input_tender_list exists (Chile uses input table, not CSV)
    repo_root = Path(__file__).resolve().parents[3]
    inputs_sql = repo_root / "sql" / "schemas" / "postgres" / "inputs.sql"
    if inputs_sql.exists():
        try:
            SchemaRegistry(db).apply_schema(inputs_sql)
        except Exception as e:
            print(f"[WARN] Could not apply inputs.sql schema: {e}")
    
    try:
        db.commit()
    except Exception:
        pass  # May autocommit