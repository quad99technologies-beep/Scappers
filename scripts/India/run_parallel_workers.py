#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
India NPPA Scraper - Parallel Worker Runner

Discovers per-formulation CSV chunks and launches one worker process per file.
Workers can run concurrently via threads and each writes to its own output directory.
"""

import argparse
import csv
import json
import math
import os
import re
import subprocess
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import get_input_dir, get_output_dir, load_env_file, getenv_int


def resolve_input_file(raw_path: str) -> Path:
    if raw_path:
        p = Path(raw_path)
        if not p.is_absolute():
            p = get_input_dir() / p
        return p
    return get_input_dir() / "formulations.csv"


def resolve_parts_dir(raw_path: str, input_file: Optional[Path]) -> Optional[Path]:
    if raw_path:
        p = Path(raw_path)
        if not p.is_absolute():
            p = input_file.parent / p
        return p
    return None


def resolve_output_base(raw_path: str) -> Path:
    if raw_path:
        p = Path(raw_path)
        if not p.is_absolute():
            p = _repo_root / p
        return p
    return get_output_dir("workers")


def read_csv_rows(path: Path) -> List[List[str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.reader(f))


def write_csv_rows(path: Path, rows: List[List[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def combine_part_files(part_files: List[Path], out_path: Path) -> Path:
    """Combine multiple *_part*.csv files into a single CSV (header from first file)."""
    if not part_files:
        raise ValueError("No part files provided")
    rows0 = read_csv_rows(part_files[0])
    if not rows0:
        raise ValueError(f"Empty part file: {part_files[0]}")
    header = rows0[0]
    combined: List[List[str]] = [header]
    combined.extend(rows0[1:])
    for pf in part_files[1:]:
        rows = read_csv_rows(pf)
        if not rows:
            continue
        body = rows[1:] if rows and rows[0] == header else rows
        combined.extend(body)
    write_csv_rows(out_path, combined)
    return out_path


def count_formulations_in_csv(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        _ = next(reader, None)  # header
        for row in reader:
            if any(cell.strip() for cell in row):
                count += 1
    return count


def normalize_stem_for_parts(stem: str) -> str:
    return re.sub(r"_part\\d+$", "", stem, flags=re.IGNORECASE)


def get_parts_dir(input_file: Optional[Path], parts_dir: Optional[Path]) -> Path:
    if parts_dir:
        return parts_dir
    if input_file is not None and input_file.parent:
        return input_file.parent
    return get_input_dir()


def find_existing_parts(input_file: Optional[Path], parts_dir: Optional[Path]) -> List[Path]:
    base_dir = get_parts_dir(input_file, parts_dir)
    base_stem = None
    suffix = ".csv"
    if input_file is not None:
        base_stem = normalize_stem_for_parts(input_file.stem)
        suffix = input_file.suffix or ".csv"
    if base_stem:
        pattern = f"{base_stem}_part*{suffix}"
    else:
        pattern = f"*_part*{suffix}"
    candidates = list(base_dir.glob(pattern))

    def sort_key(p: Path) -> int:
        m = re.search(r"_part(\\d+)$", p.stem, flags=re.IGNORECASE)
        return int(m.group(1)) if m else 0

    return sorted(candidates, key=sort_key)


def detect_formulation_index(header: List[str]) -> int:
    candidates = ["formulation", "name", "generic_name"]
    normalized = [h.strip().lower() for h in header]
    for candidate in candidates:
        if candidate in normalized:
            return normalized.index(candidate)
    return 0


def dedupe_rows_by_formulation(header: List[str], body: List[List[str]]) -> Tuple[List[List[str]], int]:
    idx = detect_formulation_index(header)
    seen = set()
    deduped: List[List[str]] = []
    dup_count = 0
    for row in body:
        if idx >= len(row):
            continue
        key = (row[idx] or "").strip().upper()
        if not key:
            continue
        if key in seen:
            dup_count += 1
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, dup_count


def split_formulations(input_file: Path, parts: int, parts_dir: Path) -> List[Path]:
    rows = read_csv_rows(input_file)
    if not rows:
        raise ValueError("Input CSV is empty.")
    header = rows[0]
    body = rows[1:]
    if not body:
        raise ValueError("Input CSV has no data rows.")

    body, dup_count = dedupe_rows_by_formulation(header, body)
    if dup_count:
        print(f"[INFO] Removed {dup_count} duplicate formulation row(s) before split", flush=True)
    if not body:
        raise ValueError("Input CSV has no usable data rows after de-duplication.")

    chunk_size = max(1, math.ceil(len(body) / parts))
    part_files: List[Path] = []
    base_stem = normalize_stem_for_parts(input_file.stem)
    for i in range(parts):
        start = i * chunk_size
        end = start + chunk_size
        chunk = body[start:end]
        if not chunk:
            break
        part_path = parts_dir / f"{base_stem}_part{i + 1}{input_file.suffix}"
        write_csv_rows(part_path, [header] + chunk)
        part_files.append(part_path)
    return part_files


def detect_prebuilt_parts(base_dir: Path, pattern: str) -> List[Path]:
    candidates = [p for p in base_dir.glob(pattern) if p.is_file()]

    def sort_key(p: Path) -> int:
        m = re.search(r"_part(\\d+)", p.stem, flags=re.IGNORECASE)
        return int(m.group(1)) if m else 0

    return sorted(candidates, key=sort_key)


def run_worker_job(worker_id: int, part_file: Path, worker_output: Path, script_path: Path) -> Tuple[int, int]:
    env = os.environ.copy()
    env["FORMULATIONS_FILE"] = str(part_file)
    env["OUTPUT_DIR"] = str(worker_output)
    env["WORKER_ID"] = str(worker_id)
    env.setdefault("PYTHONUNBUFFERED", "1")

    cmd = [sys.executable, "-u", str(script_path)]
    print(
        f"[START] Worker {worker_id}: {script_path} | "
        f"FORMULATIONS_FILE={part_file} | OUTPUT_DIR={worker_output}",
        flush=True,
    )
    result = subprocess.run(cmd, env=env, check=False)
    return worker_id, result.returncode


def read_worker_checkpoint(worker_output: Path) -> Dict[str, Any]:
    checkpoint_file = worker_output / ".checkpoints" / "formulation_progress.json"
    if not checkpoint_file.exists():
        return {"terminal": 0, "failed": 0, "completed": 0, "zero_records": 0, "stats": {}}
    try:
        with checkpoint_file.open("r", encoding="utf-8") as f:
            data = json.load(f)
        completed = len(data.get("completed_formulations", []))
        zero = len(data.get("zero_record_formulations", []))
        failed = data.get("failed_formulations", {})
        failed_count = len(failed) if isinstance(failed, dict) else 0
        return {
            "terminal": completed + zero,
            "failed": failed_count,
            "completed": completed,
            "zero_records": zero,
            "stats": data.get("stats", {}),
        }
    except Exception:
        return {"terminal": 0, "failed": 0, "completed": 0, "zero_records": 0, "stats": {}}


def emit_parallel_progress(worker_totals: dict, worker_outputs: dict) -> Tuple[int, int]:
    total_terminal = 0
    total_failed = 0
    per_worker = []
    total_completed = 0
    total_zero = 0
    total_rows = 0
    for worker_id in sorted(worker_totals.keys()):
        checkpoint_data = read_worker_checkpoint(worker_outputs[worker_id])
        terminal = checkpoint_data["terminal"]
        failed = checkpoint_data["failed"]
        total_terminal += terminal
        total_failed += failed
        total_completed += checkpoint_data.get("completed", 0)
        total_zero += checkpoint_data.get("zero_records", 0)
        stats = checkpoint_data.get("stats", {}) or {}
        total_rows += int(stats.get("total_substitutes", 0) or 0)
        per_worker.append(f"W{worker_id} {terminal}/{worker_totals[worker_id]}")
    total_all = sum(worker_totals.values())
    percent = (total_terminal / total_all) * 100 if total_all else 0.0
    suffix = " | ".join(per_worker)
    if total_failed:
        suffix = f"{suffix} | failed={total_failed}"
    print(
        f"[PROGRESS] Parallel Formulations: {total_terminal}/{total_all} ({percent:.1f}%) - {suffix}",
        flush=True,
    )
    print(
        f"[STATS] Success: {total_completed} | Zero-records: {total_zero} | Detail rows: {total_rows}",
        flush=True,
    )
    return total_terminal, total_failed


def combine_worker_details(worker_output: Path) -> Optional[Path]:
    details_dir = worker_output / "details"
    if not details_dir.exists():
        return None

    detail_files = sorted(details_dir.glob("*.csv"))
    if not detail_files:
        return None

    combined_path = worker_output / "details_combined.csv"
    header: Optional[List[str]] = None
    with combined_path.open("w", newline="", encoding="utf-8-sig") as out:
        writer = None
        for csv_file in detail_files:
            with csv_file.open("r", newline="", encoding="utf-8-sig") as inp:
                reader = csv.DictReader(inp)
                if not reader.fieldnames:
                    continue
                if header is None:
                    header = list(reader.fieldnames)
                    writer = csv.DictWriter(out, fieldnames=header)
                    writer.writeheader()
                elif reader.fieldnames != header:
                    # Align rows to the original header if columns differ.
                    pass
                for row in reader:
                    if writer is None or header is None:
                        continue
                    if reader.fieldnames != header:
                        row = {k: row.get(k, "") for k in header}
                    writer.writerow(row)
    return combined_path


def write_parallel_marker(output_base: Path, workers: int, input_file: Path, exit_codes: dict, duration_seconds: float) -> Path:
    output_base.mkdir(parents=True, exist_ok=True)
    marker = output_base / ".parallel_complete.json"
    data = {
        "completed_at": datetime.now().isoformat(),
        "workers": workers,
        "input_file": str(input_file),
        "output_base": str(output_base),
        "duration_seconds": round(duration_seconds, 2),
        "exit_codes": exit_codes,
    }
    with marker.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    return marker


def main() -> None:
    load_env_file()
    default_workers = getenv_int("INDIA_WORKERS", 5)
    start_ts = time.time()

    parser = argparse.ArgumentParser(
        description="Run India scraper in parallel with N workers."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=default_workers,
        help="Number of workers to run (default: 5 or INDIA_WORKERS).",
    )
    parser.add_argument(
        "--input",
        default="",
        help="Path to formulations CSV (default: input/India/formulations.csv).",
    )
    parser.add_argument(
        "--input-dir",
        default="",
        help="Directory containing worker-specific formulation files (overrides splitting).",
    )
    parser.add_argument(
        "--pattern",
        default="formulations_part*.csv",
        help="Glob pattern to detect per-worker formulation CSVs.",
    )
    parser.add_argument(
        "--output-base",
        default="",
        help="Base directory for worker outputs (default: output/India/workers).",
    )
    parser.add_argument(
        "--use-existing-parts",
        action="store_true",
        help="Use existing *_part*.csv files instead of re-splitting.",
    )
    parser.add_argument(
        "--parts-dir",
        default="",
        help="Directory containing *_part*.csv files (default: input file directory).",
    )
    parser.add_argument(
        "--no-combine-details",
        action="store_true",
        help="Skip creating details_combined.csv per worker.",
    )
    args = parser.parse_args()

    workers = max(1, int(args.workers))
    input_file = resolve_input_file(args.input)
    parts_dir = resolve_parts_dir(args.parts_dir, input_file)

    output_base = resolve_output_base(args.output_base)
    output_base.mkdir(parents=True, exist_ok=True)

    print(f"[CONFIG] Input file: {input_file}", flush=True)
    print(f"[CONFIG] Output base: {output_base}", flush=True)
    print(f"[CONFIG] Workers: {workers}", flush=True)

    script_path = _script_dir / "02_get_details.py"
    if not script_path.exists():
        print(f"[ERROR] Worker script not found: {script_path}", flush=True)
        sys.exit(1)

    search_dir = Path(args.input_dir) if args.input_dir else (input_file.parent if input_file else get_input_dir())
    auto_parts: List[Path] = []
    if search_dir and search_dir.exists():
        auto_parts = detect_prebuilt_parts(search_dir, args.pattern or "formulations_part*.csv")

    part_files: List[Path] = []
    if auto_parts and len(auto_parts) == workers:
        part_files = auto_parts
        print(f"[INFO] Using {len(part_files)} pre-split part file(s) in {search_dir}", flush=True)
    elif auto_parts:
        print(
            f"[WARN] Found {len(auto_parts)} pre-split part file(s) but workers={workers}; re-splitting input.",
            flush=True,
        )
    elif args.use_existing_parts:
        if not input_file.exists():
            print(f"[ERROR] Input file not found: {input_file}", flush=True)
            sys.exit(1)
        part_files = find_existing_parts(input_file, parts_dir)
        if not part_files:
            print("[ERROR] No existing part files found.", flush=True)
            sys.exit(1)
        print(f"[INFO] Reusing {len(part_files)} existing part file(s).", flush=True)
    else:
        if not input_file.exists():
            print(f"[ERROR] Input file not found: {input_file}", flush=True)
            sys.exit(1)
        target_dir = parts_dir or input_file.parent
        part_files = split_formulations(input_file, workers, target_dir)
        if not part_files:
            print("[ERROR] No part files created; check input file contents.", flush=True)
            sys.exit(1)
        print(f"[INFO] Created {len(part_files)} part file(s).", flush=True)

    # If pre-split parts were present but count mismatched, create fresh parts under output.
    # If the master input file doesn't exist, rebuild it by combining the pre-split parts.
    if not part_files and auto_parts:
        target_dir = output_base / "_parts"
        target_dir.mkdir(parents=True, exist_ok=True)
        master = input_file
        if not master.exists():
            master = combine_part_files(auto_parts, target_dir / "formulations_master_from_parts.csv")
            print(f"[INFO] Rebuilt missing input file from parts: {master}", flush=True)
        part_files = split_formulations(master, workers, target_dir)
        if not part_files:
            print("[ERROR] No part files created; check input file contents.", flush=True)
            sys.exit(1)
        print(f"[INFO] Created {len(part_files)} part file(s) in {target_dir}", flush=True)

    worker_count = len(part_files)
    if worker_count == 0:
        print("[ERROR] No formulation files detected.", flush=True)
        sys.exit(1)

    worker_totals = {idx: count_formulations_in_csv(pf) for idx, pf in enumerate(part_files, start=1)}
    worker_outputs: Dict[int, Path] = {}
    for idx in worker_totals:
        worker_outputs[idx] = output_base / f"worker_{idx}"
        worker_outputs[idx].mkdir(parents=True, exist_ok=True)

    max_workers = min(max(1, workers), worker_count)
    progress_interval = max(3, getenv_int("PARALLEL_PROGRESS_EVERY_SEC", 15))
    exit_codes: Dict[int, int] = {}
    futures = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, part_file in enumerate(part_files, start=1):
            future = executor.submit(run_worker_job, idx, part_file, worker_outputs[idx], script_path)
            futures[future] = idx

        pending = set(futures.keys())
        try:
            while pending:
                done, pending = wait(pending, timeout=progress_interval, return_when=FIRST_COMPLETED)
                emit_parallel_progress(worker_totals, worker_outputs)
                for future in done:
                    worker_id = futures.pop(future)
                    try:
                        idx, code = future.result()
                    except Exception as exc:
                        print(f"[ERROR] Worker {worker_id} crashed: {exc}", flush=True)
                        idx, code = worker_id, 1
                    exit_codes[idx] = code
                    status = "OK" if code == 0 else "FAIL"
                    print(f"[DONE] Worker {idx}: exit={code} ({status})", flush=True)
        except KeyboardInterrupt:
            print("[WARN] Interrupted; cancelling remaining workers", flush=True)
            for future in pending:
                future.cancel()
            sys.exit(1)

    emit_parallel_progress(worker_totals, worker_outputs)

    for idx in worker_totals:
        exit_codes.setdefault(idx, 1)

    if not args.no_combine_details:
        for idx in sorted(worker_outputs.keys()):
            worker_output = worker_outputs[idx]
            combined = combine_worker_details(worker_output)
            if combined:
                print(f"[COMBINE] Worker {idx}: {combined}", flush=True)
            else:
                print(f"[COMBINE] Worker {idx}: no details to combine", flush=True)

    failed = [idx for idx, code in exit_codes.items() if code != 0]
    if failed:
        print(f"[ERROR] Workers failed: {failed}", flush=True)
        sys.exit(1)

    duration = time.time() - start_ts
    marker = write_parallel_marker(output_base, worker_count, input_file, exit_codes, duration)
    print(f"[MARKER] {marker}", flush=True)
    print("[OK] All workers completed successfully.", flush=True)


if __name__ == "__main__":
    main()
