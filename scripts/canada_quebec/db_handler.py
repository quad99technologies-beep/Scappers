"""
Database Handler for Canada Quebec Scraper.

Handles schema definitions, connections, and data operations using core/db/postgres_connection.py.
"""
import sys
import logging
from datetime import datetime
from pathlib import Path

# Setup paths
script_path = Path(__file__).resolve().parent
# If running as script, parent is CanadaQuebec. 
# Repository root should be Scrappers.
# D:\quad99\Scrappers\scripts\CanadaQuebec
# parents[0] = scripts
# parents[1] = Scrappers
repo_root = script_path.parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.db.postgres_connection import PostgresDB
from core.db.models import run_ledger_start, run_ledger_finish, run_ledger_ensure_exists
from scripts.canada_quebec.config_loader import DB_ENABLED, SCRAPER_ID_DB

log = logging.getLogger("db_handler")

# Schema Definitions
SCHEMA_SQL = """
-- Annexe III (Price Limit Products)
CREATE TABLE IF NOT EXISTS {prefix}annexe_iii (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    generic_name TEXT,
    formulation TEXT,
    din TEXT,
    brand TEXT,
    manufacturer TEXT,
    format_str TEXT,
    price TEXT,
    unit_price TEXT,
    page_num INTEGER,
    annexe TEXT DEFAULT 'III',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, brand, formulation, manufacturer) 
);

-- Annexe IV (Exceptions - Primarily Text)
CREATE TABLE IF NOT EXISTS {prefix}annexe_iv (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    generic_name TEXT,
    formulation TEXT,
    din TEXT,
    brand TEXT,
    manufacturer TEXT,
    format_str TEXT,
    price TEXT,
    unit_price TEXT,
    page_num INTEGER,
    annexe TEXT DEFAULT 'IV',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Annexe IV.1 (Exception Drugs - Text Heavy)
CREATE TABLE IF NOT EXISTS {prefix}annexe_iv1 (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    generic_name TEXT,
    formulation TEXT,
    din TEXT,
    brand TEXT,
    manufacturer TEXT,
    format_str TEXT,
    price TEXT,
    unit_price TEXT,
    page_num INTEGER,
    annexe TEXT DEFAULT 'IV.1',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, din, format_str, price)
);

-- Annexe IV.2 (Stable Agents - Tabular)
CREATE TABLE IF NOT EXISTS {prefix}annexe_iv2 (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    generic_name TEXT,
    formulation TEXT,
    din TEXT,
    brand TEXT,
    manufacturer TEXT,
    format_str TEXT,
    price TEXT,
    unit_price TEXT,
    page_num INTEGER,
    annexe TEXT DEFAULT 'IV.2',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, din, format_str, price)
);

-- Annexe V (Main Drug List - Tabular)
CREATE TABLE IF NOT EXISTS {prefix}annexe_v (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    generic_name TEXT,
    formulation TEXT,
    din TEXT,
    brand TEXT,
    manufacturer TEXT,
    format_str TEXT,
    price TEXT,
    unit_price TEXT,
    page_num INTEGER,
    annexe TEXT DEFAULT 'V',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, din, format_str, price)
);

-- Pipeline Stats
CREATE TABLE IF NOT EXISTS {prefix}pipeline_stats (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    step_name TEXT,
    status TEXT,
    rows_extracted INTEGER,
    validation_errors INTEGER,
    duration_seconds FLOAT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline Runs
CREATE TABLE IF NOT EXISTS {prefix}pipeline_runs (
    run_id TEXT PRIMARY KEY,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT,
    total_rows INTEGER,
    config_snapshot JSONB
);
"""

class DBHandler:
    def __init__(self):
        if not DB_ENABLED:
            log.warning("DB is disabled in config but DBHandler initialized.")
        self.db = PostgresDB("CanadaQuebec") # Uses ca_qc_ prefix
        self.prefix = self.db.prefix

    def init_schema(self):
        """Create tables if they don't exist."""
        sql = SCHEMA_SQL.format(prefix=self.prefix)
        log.info("Initializing schema...")
        self.db.executescript(sql)
        log.info("Schema initialized.")

    def start_run(self, run_id: str):
        """Log run start."""
        # Local schema update
        with self.db.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self.prefix}pipeline_runs (run_id, status) VALUES (%s, %s) ON CONFLICT (run_id) DO NOTHING",
                (run_id, "RUNNING")
            )
        
        # Shared run_ledger update for GUI visibility
        try:
            # We use "CanadaQuebec" as the official scraper name for the ledger
            sql, params = run_ledger_ensure_exists(run_id, "CanadaQuebec", mode="fresh")
            with self.db.cursor() as cur:
                cur.execute(sql, params)
        except Exception as e:
            log.warning(f"Failed to update shared run_ledger: {e}")

    def log_step(self, run_id: str, step_name: str, status: str, rows: int = 0, duration: float = 0.0, meta: dict = None):
        """Log step execution stats."""
        import json
        meta_json = json.dumps(meta) if meta else None
        with self.db.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self.prefix}pipeline_stats (run_id, step_name, status, rows_extracted, duration_seconds, metadata) VALUES (%s, %s, %s, %s, %s, %s)",
                (run_id, step_name, status, rows, duration, meta_json)
            )

    def save_rows(self, table_suffix: str, rows: list, run_id: str):
        """
        Save extracted rows to the specified table (iv1, iv2, v).
        table_suffix: "annexe_iv1", "annexe_iv2", "annexe_v"
        """
        if not rows:
            return

        table_name = f"{self.prefix}{table_suffix}"
        
        # Determine columns from first row (assuming all rows have same keys or keys are consistent with model)
        # We use strict mapping to DB schema columns
        columns = [
            "run_id", "generic_name", "formulation", "din", "brand", 
            "manufacturer", "format_str", "price", "unit_price", "page_num", "annexe"
        ]
        
        values_list = []
        for r in rows:
            # Map RowData object or dict to tuple
            if hasattr(r, '__dict__'):
                d = r.__dict__
            else:
                d = r # Assume dict
            
            values_list.append((
                run_id,
                d.get("generic_name", ""),
                d.get("formulation", ""),
                d.get("din", ""),
                d.get("brand", ""),
                d.get("manufacturer", ""),
                d.get("format_str", ""),
                d.get("price", ""),
                d.get("unit_price", ""),
                d.get("page_num", 0),
                d.get("annexe", table_suffix.replace("annexe_", "").upper())
            ))

        if table_suffix == "annexe_iii":
            conflict_cols = "(run_id, brand, formulation, manufacturer)"
        else:
            conflict_cols = "(run_id, din, format_str, price)"

        placeholders = ",".join(["%s"] * len(columns))
        # Update/upsert on Conflict
        sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT {conflict_cols} DO UPDATE SET generic_name = EXCLUDED.generic_name, formulation = EXCLUDED.formulation, brand = EXCLUDED.brand, manufacturer = EXCLUDED.manufacturer, unit_price = EXCLUDED.unit_price, page_num = EXCLUDED.page_num"
        
        log.info(f"Inserting {len(values_list)} rows into {table_name}")
        self.db.executemany(sql, values_list)

    def finish_run(self, run_id: str, status: str = "COMPLETED"):
        """Mark run as finished."""
        # Local schema update
        with self.db.cursor() as cur:
            cur.execute(
                f"UPDATE {self.prefix}pipeline_runs SET status = %s, end_time = CURRENT_TIMESTAMP WHERE run_id = %s",
                (status, run_id)
            )
            
        # Update shared run_ledger
        try:
            # Calculate totals for ledger
            stats = self.get_run_stats(run_id)
            total_items = sum(stats.values())
            
            # Update status in run_ledger
            # status needs to be lowercase for check constraint ('running', 'completed', 'failed')
            ledger_status = status.lower()
            if ledger_status not in ('running', 'completed', 'failed', 'cancelled', 'partial', 'resume', 'stopped'):
                ledger_status = 'completed' if status == "COMPLETED" else 'failed'

            sql, params = run_ledger_finish(
                run_id, 
                ledger_status, 
                items_scraped=total_items, 
                items_exported=total_items # Assuming all exported
            )
            with self.db.cursor() as cur:
                cur.execute(sql, params)
                
        except Exception as e:
            log.warning(f"Failed to update shared run_ledger finish: {e}")

    def get_run_stats(self, run_id: str):
        """Get stats for verification."""
        stats = {}
        for suffix in ["annexe_iii", "annexe_iv", "annexe_iv1", "annexe_iv2", "annexe_v"]:
             table = f"{self.prefix}{suffix}"
             with self.db.cursor() as cur:
                 cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (run_id,))
                 count = cur.fetchone()[0]
                 stats[suffix] = count
        return stats

if __name__ == "__main__":
    # Diagnostic / Setup
    logging.basicConfig(level=logging.INFO)
    handler = DBHandler()
    handler.init_schema()
    print("Schema initialized.")
