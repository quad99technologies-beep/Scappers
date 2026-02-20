
import os
import sys
from pathlib import Path
import logging

# Ensure repo root is on path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.postgres_connection import PostgresDB
from config_loader import load_env_file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("debug_failures")

def main():
    load_env_file()
    db = PostgresDB("India")
    db.connect()

    # Get latest run_id
    cur = db.execute(
        "SELECT run_id FROM run_ledger WHERE scraper_name = 'India' ORDER BY started_at DESC LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        logger.error("No runs found")
        return
    run_id = row[0]
    logger.info(f"Analyzing Run ID: {run_id}")

    # Count statuses
    cur = db.execute(
        "SELECT status, COUNT(*) FROM in_formulation_status WHERE run_id = %s GROUP BY status",
        (run_id,)
    )
    logger.info("Status Breakdown:")
    for status, count in cur.fetchall():
        logger.info(f"  {status}: {count}")

    # Fetch sample zero_records
    logger.info("\nSample 'zero_records' formulations:")
    cur = db.execute(
        "SELECT formulation, attempts, error_message FROM in_formulation_status "
        "WHERE run_id = %s AND status = 'zero_records' LIMIT 20",
        (run_id,)
    )
    for row in cur.fetchall():
        logger.info(f"  Formulation: '{row[0]}' | Attempts: {row[1]} | Error: {row[2]}")

    # Fetch sample failed
    logger.info("\nSample 'failed' formulations:")
    cur = db.execute(
        "SELECT formulation, attempts, error_message FROM in_formulation_status "
        "WHERE run_id = %s AND status = 'failed' LIMIT 20",
        (run_id,)
    )
    for row in cur.fetchall():
        logger.info(f"  Formulation: '{row[0]}' | Attempts: {row[1]} | Error: {row[2]}")
        
    db.close()

if __name__ == "__main__":
    main()
