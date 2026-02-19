#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Step 7: Scrape No-Data (Selenium Retry)

Goal
----
Retry products that are present in PCID mapping but still have no scraped data.
Runs Selenium worker for NO_DATA_MAX_ROUNDS (default: 1) to attempt scraping
missing products.

Steps
-----
1) Loads the latest `*_pcid_no_data.csv` (produced by Step 6)
2) Filters to pairs that still have NO rows in `ar_products` for this run_id
3) Re-queues them (status=pending, loop_count=0, total_records=0) in `ar_product_index`
4) Runs Selenium worker (NO_DATA_MAX_ROUNDS times)
5) Marks successfully scraped products with scrape_source='step7'

Note: After this step, run Step 8 (Refresh Export) to regenerate reports with
the newly scraped data.

Usage
-----
This script is designed to be called by `run_pipeline_resume.py`.
It will respect ARGENTINA_RUN_ID from env / output/.current_run_id.
"""

from __future__ import annotations

import sys
import os
from pathlib import Path

# Add project root to sys.path to allow 'core' imports
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Add script dir to sys.path to allow local imports
script_dir = Path(__file__).resolve().parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

import os
import sys
import re
import unicodedata
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Iterable, List, Tuple, Set

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
# Ensure Argentina directory is at the front of sys.path to prioritize local 'db' package
# This fixes conflict with core/db which might be in sys.path
sys.path = [p for p in sys.path if not Path(p).name == 'core']
if str(_SCRIPT_DIR) in sys.path:
    sys.path.remove(str(_SCRIPT_DIR))
sys.path.insert(0, str(_SCRIPT_DIR))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

from config_loader import get_output_dir, PRODUCTS_URL, NO_DATA_MAX_ROUNDS, SKIP_NO_DATA_STEP
from core.db.connection import CountryDB
from db.schema import apply_argentina_schema
from db.repositories import ArgentinaRepository

try:
    from psycopg2.extras import execute_values  # type: ignore
    _HAS_EXECUTE_VALUES = True
except Exception:
    execute_values = None
    _HAS_EXECUTE_VALUES = False


def _get_run_id(output_dir: Path) -> str:
    rid = os.environ.get("ARGENTINA_RUN_ID", "").strip()
    if rid:
        return rid
    run_id_file = output_dir / ".current_run_id"
    if run_id_file.exists():
        try:
            txt = run_id_file.read_text(encoding="utf-8").strip()
            if txt:
                os.environ["ARGENTINA_RUN_ID"] = txt
                return txt
        except Exception:
            pass
    raise RuntimeError("ARGENTINA_RUN_ID not set and .current_run_id missing. Run Step 0 first.")


def _find_latest_no_data_csv(output_dir: Path) -> Path | None:
    exports_dir = output_dir / "exports"
    candidates: List[Path] = []
    if exports_dir.exists():
        candidates.extend(list(exports_dir.glob("*_pcid_no_data.csv")))
    candidates.extend(list(output_dir.glob("*_pcid_no_data.csv")))
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _read_no_data_pairs(csv_path: Path) -> List[Tuple[str, str]]:
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    # Canonical columns produced by 06_GenerateOutput.py
    company_col = "Company" if "Company" in df.columns else None
    product_col = "Local Product Name" if "Local Product Name" in df.columns else None
    if not company_col or not product_col:
        raise ValueError(f"Unexpected no-data columns: {list(df.columns)} (need Company + Local Product Name)")
    pairs: Set[Tuple[str, str]] = set()
    for _, row in df.iterrows():
        c = str(row.get(company_col, "")).strip()
        p = str(row.get(product_col, "")).strip()
        if c and p:
            pairs.add((c, p))
    return sorted(pairs)


def strip_accents(s: str) -> str:
    if not s:
        return ""
    s = s.replace("ß", "ss").replace("ẞ", "SS")
    s = s.replace("æ", "ae").replace("Æ", "AE")
    s = s.replace("œ", "oe").replace("Œ", "OE")
    s = s.replace("ø", "o").replace("Ø", "O")
    s = s.replace("ð", "d").replace("Ð", "D")
    s = s.replace("þ", "th").replace("Þ", "TH")
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def construct_url(product_name: str) -> str:
    if not product_name:
        return ""
    sanitized = strip_accents(product_name)
    sanitized = re.sub(r"\s*\+\s*", " ", sanitized)
    sanitized = re.sub(r"[^a-zA-Z0-9\s-]", "", sanitized)
    sanitized = re.sub(r"\s+", "-", sanitized.strip())
    sanitized = re.sub(r"-{3,}", "--", sanitized)
    sanitized = sanitized.lower().strip("-")
    if not sanitized:
        return ""
    base_url = (PRODUCTS_URL or "https://www.alfabeta.net/precio").rstrip("/")
    return f"{base_url}/{sanitized}.html"


def _missing_pairs_in_db(db: CountryDB, run_id: str, pairs: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    """
    Return only (company, product) pairs that have NO rows in ar_products for this run_id.
    Uses a temp-table join for speed (handles thousands of rows efficiently).
    """
    if not pairs:
        return []
    with db.cursor() as cur:
        cur.execute("CREATE TEMP TABLE _tmp_no_data (company TEXT, product TEXT) ON COMMIT DROP")
        if _HAS_EXECUTE_VALUES:
            execute_values(cur, "INSERT INTO _tmp_no_data (company, product) VALUES %s", pairs, page_size=1000)
        else:
            cur.executemany("INSERT INTO _tmp_no_data (company, product) VALUES (%s, %s)", pairs)

        cur.execute(
            """
            SELECT t.company, t.product
              FROM _tmp_no_data t
              LEFT JOIN ar_products p
                ON p.run_id = %s
               AND p.input_company = t.company
               AND p.input_product_name = t.product
             WHERE p.id IS NULL
            """,
            (run_id,),
        )
        rows = cur.fetchall()
    return [(r[0], r[1]) for r in rows]


def _requeue_pairs(repo: ArgentinaRepository, pairs: List[Tuple[str, str]]) -> int:
    """Upsert pairs into ar_product_index as pending with reset counters."""
    if not pairs:
        return 0
    db = repo.db
    run_id = repo.run_id
    tuples = []
    for company, product in pairs:
        url = construct_url(product)
        tuples.append((run_id, product, company, url))

    sql_ev = """
        INSERT INTO ar_product_index
            (run_id, product, company, url, status, total_records, loop_count,
             scraped_by_selenium, scraped_by_api, updated_at, error_message)
        VALUES %s
        ON CONFLICT (run_id, company, product) DO UPDATE SET
            url = EXCLUDED.url,
            status = 'pending',
            total_records = 0,
            loop_count = 0,
            scraped_by_selenium = FALSE,
            scraped_by_api = FALSE,
            error_message = NULL,
            updated_at = CURRENT_TIMESTAMP
    """
    sql_row = """
        INSERT INTO ar_product_index
            (run_id, product, company, url, status, total_records, loop_count,
             scraped_by_selenium, scraped_by_api, updated_at, error_message)
        VALUES
            (%s, %s, %s, %s, 'pending', 0, 0, FALSE, FALSE, CURRENT_TIMESTAMP, NULL)
        ON CONFLICT (run_id, company, product) DO UPDATE SET
            url = EXCLUDED.url,
            status = 'pending',
            total_records = 0,
            loop_count = 0,
            scraped_by_selenium = FALSE,
            scraped_by_api = FALSE,
            error_message = NULL,
            updated_at = CURRENT_TIMESTAMP
    """

    with db.cursor() as cur:
        if _HAS_EXECUTE_VALUES:
            # IMPORTANT: tuples only contains 4 values (run_id, product, company, url).
            # We must provide a template that supplies literals for the remaining columns.
            execute_values(
                cur,
                sql_ev,
                tuples,
                template="(%s, %s, %s, %s, 'pending', 0, 0, FALSE, FALSE, CURRENT_TIMESTAMP, NULL)",
                page_size=500,
            )
            inserted = len(tuples)
        else:
            inserted = 0
            for t in tuples:
                cur.execute(sql_row, t)
                inserted += 1
    try:
        db.commit()
    except Exception:
        pass
    return inserted


def _mark_scrape_source_step7(db: CountryDB, run_id: str, pairs: List[Tuple[str, str]]) -> None:
    """Mark products that were scraped in step 7 with scrape_source='step7'."""
    if not pairs:
        return
    with db.cursor() as cur:
        for company, product in pairs:
            cur.execute(
                """
                UPDATE ar_product_index
                SET scrape_source = 'step7',
                    updated_at = CURRENT_TIMESTAMP
                WHERE run_id = %s AND company = %s AND product = %s
                  AND total_records > 0
                  AND (scrape_source IS NULL OR scrape_source = '')
                """,
                (run_id, company, product),
            )
    try:
        db.commit()
    except Exception:
        pass


def _run_script(script_name: str, extra_env: dict | None = None) -> None:
    script_path = _SCRIPT_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")
    env = os.environ.copy()
    env["PIPELINE_RUNNER"] = "1"
    env["ARGENTINA_NO_DATA_RETRY"] = "1"
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})
    subprocess.run([sys.executable, "-u", str(script_path)], check=True, env=env)


def _run_selenium_worker(extra_env: dict | None = None) -> None:
    """
    Run the Selenium WORKER once (single pass).

    We intentionally do NOT call `03_alfabeta_selenium_scraper.py` here because that wrapper
    performs many LOOP passes (SELENIUM_MAX_LOOPS) and can look like an infinite loop.
    Step 7 controls the "2 rounds" logic itself by calling the worker twice.
    """
    script_path = _SCRIPT_DIR / "03_alfabeta_selenium_worker.py"
    if not script_path.exists():
        raise FileNotFoundError(f"Selenium worker not found: {script_path}")
    env = os.environ.copy()
    env["PIPELINE_RUNNER"] = "1"
    env["ARGENTINA_NO_DATA_RETRY"] = "1"
    # Force "no API" behavior even if user config enables API steps
    env["USE_API_STEPS"] = "0"
    # Keep eligibility tight per round:
    # - step 7 will requeue missing pairs and set loop_count=0
    # - worker will bump loop_count while attempting
    # - next round, step 7 requeues again (loop_count reset) only if still missing
    env["SELENIUM_MAX_LOOPS"] = "1"
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})
    subprocess.run([sys.executable, "-u", str(script_path)], check=True, env=env)


def main() -> None:
    # Check if step should be skipped via config
    if SKIP_NO_DATA_STEP:
        print("[NO-DATA] SKIP_NO_DATA_STEP is enabled. Skipping this step.", flush=True)
        return

    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = _get_run_id(output_dir)

    db = CountryDB("Argentina")
    apply_argentina_schema(db)
    repo = ArgentinaRepository(db, run_id)

    no_data_csv = _find_latest_no_data_csv(output_dir)
    if not no_data_csv:
        print("[NO-DATA] No *_pcid_no_data.csv found. Nothing to retry.", flush=True)
        return

    all_pairs = _read_no_data_pairs(no_data_csv)
    if not all_pairs:
        print(f"[NO-DATA] {no_data_csv.name} is empty. Nothing to retry.", flush=True)
        return

    print(f"[NO-DATA] Using: {no_data_csv}", flush=True)
    print(f"[NO-DATA] Candidates (unique company+product): {len(all_pairs)}", flush=True)

    for round_idx in range(1, NO_DATA_MAX_ROUNDS + 1):
        missing_now = _missing_pairs_in_db(db, run_id, all_pairs)
        if not missing_now:
            print(f"[NO-DATA] Round {round_idx}: no remaining missing pairs. Stopping.", flush=True)
            break

        print(f"\n[NO-DATA] Round {round_idx}/{NO_DATA_MAX_ROUNDS}", flush=True)
        print(f"[NO-DATA] Still missing in DB: {len(missing_now)}", flush=True)

        queued = _requeue_pairs(repo, missing_now)
        print(f"[NO-DATA] Re-queued: {queued}", flush=True)

        # Selenium worker pass (single pass; Step 7 controls the 2-round logic)
        print("[NO-DATA] Running Selenium worker (single pass) on queued items...", flush=True)
        _run_selenium_worker(extra_env={"NO_DATA_ROUND": round_idx})

        # Progress check (cheap): how many remain missing after scrape
        missing_after = _missing_pairs_in_db(db, run_id, all_pairs)
        print(f"[NO-DATA] Remaining missing after round {round_idx}: {len(missing_after)}", flush=True)

        # Mark products that were scraped in this round with scrape_source='step7'
        scraped_this_round = set(missing_now) - set(missing_after)
        if scraped_this_round:
            _mark_scrape_source_step7(db, run_id, list(scraped_this_round))
            print(f"[NO-DATA] Marked {len(scraped_this_round)} products with scrape_source='step7'", flush=True)

    print(f"\n[NO-DATA] Scraping complete (run_id={run_id}) at {datetime.now().isoformat(timespec='seconds')}", flush=True)
    print("[NO-DATA] Run Step 8 (Refresh Export) to regenerate reports with newly scraped data.", flush=True)


if __name__ == "__main__":
    main()

