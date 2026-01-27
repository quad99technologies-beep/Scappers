#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
India NPPA Scraper - Multi-formulation runner (requests-first)

NPPA is prone to Chromium renderer crashes on Windows when driven via Selenium
for long runs. This runner avoids that by using `requests` for the entire
pipeline after a single warmup GET to establish cookies.

Outputs:
- Per formulation folder: <OUTPUT_DIR>/<slug>/per_row_api/*.json
- Per formulation CSV:     <OUTPUT_DIR>/details/<slug>_final.csv
- Checkpoints:             <OUTPUT_DIR>/.checkpoints/formulation_progress.json
- Report:                  <OUTPUT_DIR>/scraping_report.json
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import random
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

try:
    from config_loader import load_env_file, getenv_float, getenv_int
except Exception:  # pragma: no cover
    def load_env_file() -> None:
        return None

    def getenv_float(key: str, default: float = 0.0) -> float:
        try:
            return float(os.getenv(key, str(default)))
        except Exception:
            return default

    def getenv_int(key: str, default: int = 0) -> int:
        try:
            return int(os.getenv(key, str(default)))
        except Exception:
            return default

try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TaskID,
        TextColumn,
        TimeElapsedColumn,
    )

    _RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None
    Progress = Any
    TaskID = Any
    _RICH_AVAILABLE = False

SEARCH_URL = "https://nppaipdms.gov.in/NPPA/PharmaSahiDaam/searchMedicine"
REST_BASE = "https://nppaipdms.gov.in/NPPA/rest"
API_FORMULATION_LIST = f"{REST_BASE}/formulationListNew"
API_FORMULATION_TABLE = f"{REST_BASE}/formulationDataTableNew"
API_SKU_MRP = f"{REST_BASE}/skuMrpNew"
API_OTHER_BRANDS = f"{REST_BASE}/otherBrandPriceNew"
API_MED_DTLS = f"{REST_BASE}/medDtlsNew"

USER_AGENT_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko)"
    " Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/111.0.5550.0 Safari/537.36",
]

FORMULATION_COLUMN_CANDIDATES = ["formulation", "name", "generic_name", "medicine", "drug"]
DEFAULT_PROGRESS_FIELDS = (
    [SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TimeElapsedColumn()]
    if _RICH_AVAILABLE
    else []
)

def now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def slugify(value: str, max_len: int = 120) -> str:
    text = (value or "").strip()
    text = text.replace("\n", " ").replace("\r", " ")
    text = text.replace("/", " ").replace("\\", " ")
    text = text.replace(":", " ").replace("*", " ").replace("?", " ").strip()
    text = "_".join(text.split())
    return text[:max_len].rstrip("_")


def atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _sanitize_api_value(value: Any) -> str:
    if value is None:
        return ""
    s = str(value)
    s = s.replace(" ", "_").replace("\r\n", "_").replace("\n", "_").replace("\r", "_")
    if s == "undefined":
        return ""
    return s


def compute_fhttf(params: Dict[str, Any]) -> str:
    entries = sorted(
        ((k.lower(), k, _sanitize_api_value(params.get(k))) for k in params if k != "fhttf"),
        key=lambda x: x[0],
    )
    acc = "".join(v for _, __, v in entries)
    return hashlib.md5(acc.encode("utf-8")).hexdigest()


def human_sleep(base: float = 0.5, jitter: float = 0.4) -> None:
    time.sleep(base + random.random() * jitter)


def create_requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": random.choice(USER_AGENT_LIST),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    # Warm-up: establish cookies like a real browser would.
    r = s.get(SEARCH_URL, timeout=60)
    r.raise_for_status()
    s.headers["Referer"] = SEARCH_URL
    return s


def fetch_api_json(session: requests.Session, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
    if params:
        # Most NPPA REST endpoints require this token; without it they respond 404.
        params = dict(params)
        params["fhttf"] = compute_fhttf(params)
    response = session.get(endpoint, params=params, timeout=90)
    response.raise_for_status()
    return response.json()


def normalize_name(value: str) -> str:
    return " ".join((value or "").strip().upper().split())


def load_formulation_map(session: requests.Session, cache_path: Path) -> Dict[str, str]:
    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data:
                return {normalize_name(k): str(v) for k, v in data.items()}
        except Exception:
            pass
    items = fetch_api_json(session, API_FORMULATION_LIST)
    mapping: Dict[str, str] = {}
    if isinstance(items, list):
        for row in items:
            if not isinstance(row, dict):
                continue
            name = normalize_name(row.get("formulationName", ""))
            fid = str(row.get("formulationId", "")).strip()
            if name and fid:
                mapping[name] = fid
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8")
    return mapping


def load_csv_formulations(path: Path, column: Optional[str] = None, limit: Optional[int] = None) -> List[str]:
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise ValueError("Formulations file missing header")
        candidates = [col.strip() for col in reader.fieldnames if col]
        field = column or next((col for col in candidates if col.lower() in FORMULATION_COLUMN_CANDIDATES), candidates[0])
        formulations = []
        for row in reader:
            value = (row.get(field) or "").strip()
            if value:
                formulations.append(value)
            if limit and len(formulations) >= limit:
                break
    return formulations


def build_final_one_brand_one_row_csv(
    table_rows: List[Dict[str, Any]],
    per_row_dir: Path,
    out_csv: Path,
    attach_details: bool = False,
) -> int:
    def sget(record: Dict[str, Any], key: str) -> str:
        value = record.get(key, "")
        if value is None:
            return ""
        return str(value)

    def as_list(value: Any) -> List[Any]:
        if value is None:
            return []
        return value if isinstance(value, list) else [value]

    rows: List[Dict[str, Any]] = []
    total_rows = 0
    for idx, base in enumerate(table_rows, start=1):
        hid = sget(base, "hiddenId").strip()
        if not hid:
            continue

        main_row = {
            "HiddenId": hid,
            "BrandType": "MAIN",
            "BrandName": sget(base, "skuName"),
            "Company": sget(base, "company"),
            "Composition": sget(base, "composition"),
            "PackSize": sget(base, "packSize"),
            "Unit": sget(base, "dosageForm"),
            "Status": sget(base, "scheduleStatus"),
            "CeilingPrice": sget(base, "ceilingPrice"),
            "MRP": sget(base, "mrp"),
            "MRPPerUnit": sget(base, "mrpPerUnit"),
            "YearMonth": sget(base, "yearMonth"),
        }
        rows.append(main_row)
        total_rows += 1

        other_path = per_row_dir / f"{idx:03d}_otherBrandPriceNew.json"
        if other_path.exists():
            other_payload = load_json(other_path)
            for other in as_list(other_payload):
                if not isinstance(other, dict):
                    continue
                rows.append(
                    {
                        "HiddenId": hid,
                        "BrandType": "OTHER",
                        "BrandName": sget(other, "brandName"),
                        "Company": sget(other, "company"),
                        "Composition": sget(base, "composition"),
                        "PackSize": sget(other, "packSize"),
                        "Unit": sget(base, "dosageForm"),
                        "Status": sget(base, "scheduleStatus"),
                        "CeilingPrice": sget(base, "ceilingPrice"),
                        "MRP": sget(other, "brandMrp"),
                        "MRPPerUnit": sget(other, "mrpPerUnit"),
                        "YearMonth": sget(other, "yearMonth") or sget(base, "yearMonth"),
                    }
                )
                total_rows += 1

        if attach_details:
            sku_path = per_row_dir / f"{idx:03d}_skuMrpNew.json"
            med_path = per_row_dir / f"{idx:03d}_medDtlsNew.json"

            sku0 = {}
            med0 = {}
            if sku_path.exists():
                sku0 = extract_first_json(sku_path)
            if med_path.exists():
                med0 = extract_first_json(med_path)

            extra = {}
            if isinstance(sku0, dict):
                for key in ["brandName", "composition", "packSize", "company", "mrp", "mrpPerUnit", "yearMonth"]:
                    value = sku0.get(key)
                    if value is not None:
                        extra[f"SKU_{key}"] = str(value)

            if isinstance(med0, dict):
                for key, value in med0.items():
                    if value is None:
                        continue
                    if isinstance(value, (str, int, float, bool)):
                        extra[f"MED_{key}"] = str(value)

            if extra:
                for row in rows:
                    if row.get("HiddenId") == hid:
                        row.update(extra)

    if rows:
        df = pd.DataFrame(rows)
        df["_k"] = (
            df["HiddenId"].astype(str).fillna("") + "||" +
            df["BrandType"].astype(str).fillna("") + "||" +
            df["BrandName"].astype(str).fillna("") + "||" +
            df["PackSize"].astype(str).fillna("")
        )
        df = df.drop_duplicates(subset=["_k"]).drop(columns=["_k"])
        preferred = [
            "HiddenId", "BrandType", "BrandName", "Company",
            "Composition", "PackSize", "Unit", "Status",
            "CeilingPrice", "MRP", "MRPPerUnit", "YearMonth",
        ]
        columns = [col for col in preferred if col in df.columns] + [col for col in df.columns if col not in preferred]
        df = df[columns]
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    return total_rows


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def extract_first_json(path: Path) -> Dict[str, Any]:
    payload = load_json(path)
    if isinstance(payload, list) and payload:
        value = payload[0]
        return value if isinstance(value, dict) else {}
    if isinstance(payload, dict):
        return payload
    return {}


def update_checkpoint(
    checkpoint_file: Path,
    completed: List[str],
    zero_records: List[str],
    failed: Dict[str, str],
    in_progress: Optional[str],
    stats: Dict[str, Any],
) -> None:
    data = {
        "completed_formulations": completed,
        "zero_record_formulations": zero_records,
        "failed_formulations": failed,
        "in_progress": in_progress,
        "last_updated": now_ts(),
        "stats": stats,
    }
    atomic_write_json(checkpoint_file, data)


@contextmanager
def progress_guard():
    progress = None
    console = Console() if _RICH_AVAILABLE else None
    try:
        if _RICH_AVAILABLE:
            progress = Progress(*DEFAULT_PROGRESS_FIELDS, console=console, refresh_per_second=2)
            progress.start()
        yield progress
    finally:
        if progress:
            progress.stop()


def run_formulation(
    formulation: str,
    session: requests.Session,
    formulation_map: Dict[str, str],
    progress: Optional[Progress],
    worker_output: Path,
    attach_details: bool,
) -> Tuple[int, int]:
    slug = slugify(formulation)
    output_folder = worker_output / slug
    per_row_dir = output_folder / "per_row_api"
    per_row_dir.mkdir(parents=True, exist_ok=True)
    details_dir = worker_output / "details"
    details_dir.mkdir(parents=True, exist_ok=True)

    formulation_id = formulation_map.get(normalize_name(formulation), "")
    if not formulation_id:
        return 0, 0

    params = {"formulationId": formulation_id, "strengthId": "0", "dosageId": "0"}
    table_data = fetch_api_json(session, API_FORMULATION_TABLE, params=params)
    if not isinstance(table_data, list):
        raise RuntimeError("Unexpected response for formulationDataTableNew")

    if not table_data:
        return 0, 0

    max_rows = max(1, int(getenv_int("MAX_MEDICINES_PER_FORMULATION", 5000)))
    if len(table_data) > max_rows:
        logging.warning("Truncating formulation '%s' rows from %s to %s", formulation, len(table_data), max_rows)
        table_data = table_data[:max_rows]

    atomic_write_json(output_folder / "formulationDataTableNew.json", table_data)
    task_id = None
    if progress:
        task_id = progress.add_task(f"[cyan]Rows [{slug}]", total=len(table_data))

    total_other = 0
    hidden_rows = 0
    detail_delay = max(0.0, float(getenv_float("DETAIL_DELAY", 0.6)))
    for idx, row in enumerate(table_data, start=1):
        if not isinstance(row, dict):
            continue
        hidden_id = (row.get("hiddenId") or "").strip()
        if not hidden_id:
            logging.warning("Row %s missing hiddenId, skipping", idx)
            progress_advance(progress, task_id)
            continue

        meta = {
            "index": idx,
            "hiddenId": hidden_id,
            "skuName": row.get("skuName", ""),
            "scheduleStatus": row.get("scheduleStatus", ""),
            "ceilingPrice": row.get("ceilingPrice", ""),
            "dosageForm": row.get("dosageForm", ""),
            "mrp": row.get("mrp", ""),
            "mrpPerUnit": row.get("mrpPerUnit", ""),
            "yearMonth": row.get("yearMonth", ""),
        }
        atomic_write_json(per_row_dir / f"{idx:03d}_row_meta.json", meta)

        sku_payload = fetch_api_json(session, API_SKU_MRP, params={"hiddenId": hidden_id})
        atomic_write_json(per_row_dir / f"{idx:03d}_skuMrpNew.json", sku_payload)

        other_payload = fetch_api_json(session, API_OTHER_BRANDS, params={"hiddenId": hidden_id})
        atomic_write_json(per_row_dir / f"{idx:03d}_otherBrandPriceNew.json", other_payload)
        total_other += len(other_payload) if isinstance(other_payload, list) else 0

        med_payload = fetch_api_json(session, API_MED_DTLS, params={"hiddenId": hidden_id})
        atomic_write_json(per_row_dir / f"{idx:03d}_medDtlsNew.json", med_payload)

        hidden_rows += 1
        progress_advance(progress, task_id)
        if detail_delay:
            time.sleep(detail_delay + random.random() * 0.2)

    if task_id is not None and progress:
        progress.remove_task(task_id)

    final_csv = details_dir / f"{slug}_final.csv"
    build_final_one_brand_one_row_csv(
        table_data,
        per_row_dir,
        final_csv,
        attach_details=attach_details,
    )
    return hidden_rows, total_other


def progress_advance(progress: Optional[Progress], task_id: Optional[TaskID]) -> None:
    if progress and task_id is not None:
        progress.advance(task_id)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="India NPPA Detail Extractor (multi-formulation aware)")
    parser.add_argument("--formulation", type=str, help="Single formulation name")
    parser.add_argument(
        "--formulations-file",
        type=Path,
        help="CSV file containing formulations (headers: formulation, name, etc.)",
    )
    parser.add_argument("--column", type=str, help="Column name for formulation lookup in CSV")
    parser.add_argument("--limit", type=int, help="Limit number of formulations to process from CSV")
    parser.add_argument("--attach-details", action="store_true", help="Attach SKU/MED metadata to final CSV")
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    logger = logging.getLogger("india_scraper")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_env_file()

    formulations: List[str] = []
    if args.formulations_file:
        formulations = load_csv_formulations(args.formulations_file, column=args.column, limit=args.limit)
    elif os.getenv("FORMULATIONS_FILE"):
        path = Path(os.getenv("FORMULATIONS_FILE"))
        formulations = load_csv_formulations(path, column=args.column, limit=args.limit)
    elif args.formulation:
        formulations = [args.formulation]

    if not formulations:
        raise SystemExit("No formulations provided. Use --formulation or --formulations-file.")

    worker_output = Path(os.getenv("OUTPUT_DIR", "./output_nppa")).resolve()
    worker_output.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = worker_output / ".checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_file = checkpoint_dir / "formulation_progress.json"

    start_time = time.time()
    completed: List[str] = []
    zero_records: List[str] = []
    failed: Dict[str, str] = {}
    stats = {"total_medicines": 0, "total_substitutes": 0, "errors": 0}
    cache_dir = worker_output / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    session = create_requests_session()
    formulation_map = load_formulation_map(session, cache_dir / "formulation_map.json")
    search_delay = max(0.0, float(getenv_float("SEARCH_DELAY", 0.8)))
    max_retries = max(1, int(getenv_int("MAX_RETRIES", 3)))

    with progress_guard() as progress:
        for formulation in formulations:
            update_checkpoint(checkpoint_file, completed, zero_records, failed, formulation, stats)
            human_sleep(search_delay, 0.4)
            attempt = 0
            while attempt < max_retries:
                try:
                    hidden_rows, other_rows = run_formulation(
                        formulation,
                        session,
                        formulation_map,
                        progress,
                        worker_output,
                        attach_details=args.attach_details,
                    )
                    if hidden_rows == 0:
                        zero_records.append(formulation)
                    else:
                        stats["total_medicines"] += hidden_rows
                        stats["total_substitutes"] += other_rows
                        completed.append(formulation)
                    logging.info("Formulation '%s' processed (rows=%s other=%s)", formulation, hidden_rows, other_rows)
                    break
                except (requests.RequestException, ValueError) as exc:
                    attempt += 1
                    if attempt >= max_retries:
                        msg = str(exc)
                        failed[formulation] = msg
                        stats["errors"] += 1
                        logging.error("Formulation '%s' failed after retries: %s", formulation, msg)
                        break
                    # Recreate the session on network/JSON issues.
                    time.sleep(2 + random.random() * 2)
                    session = create_requests_session()
                except Exception as exc:  # pylint: disable=broad-except
                    msg = str(exc)
                    failed[formulation] = msg
                    stats["errors"] += 1
                    logging.error("Formulation '%s' failed: %s", formulation, msg)
                    break
            update_checkpoint(checkpoint_file, completed, zero_records, failed, None, stats)

    duration = time.time() - start_time
    report = {
        "scraper": "India",
        "worker": os.getenv("WORKER_ID", "main"),
        "timestamp": now_ts(),
        "duration_seconds": round(duration, 2),
        "formulations_input": len(formulations),
        "formulations_success": len(completed),
        "formulations_zero": len(zero_records),
        "formulations_failed": len(failed),
        "stats": stats,
    }
    atomic_write_json(worker_output / "scraping_report.json", report)


if __name__ == "__main__":
    main()
