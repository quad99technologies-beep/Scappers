#!/usr/bin/env python3
"""
India scraper health check.

Runs DB connectivity check only (India pipeline is Scrapy + PostgreSQL;
no URL/selector checks needed for the NPPA source in this script).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Tuple

import json
import sys

# Add repo root for core imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from scripts.India.config_loader import load_env_file, get_central_output_dir

load_env_file()


@dataclass
class CheckResult:
    step: str
    check: str
    status: str
    detail: str


def check_db_connection() -> Tuple[bool, str]:
    """Verify PostgreSQL connection and run_ledger for India."""
    try:
        from core.db.postgres_connection import PostgresDB
        db = PostgresDB("India")
        db.connect()
        try:
            cur = db.execute(
                "SELECT 1 FROM run_ledger WHERE scraper_name = %s LIMIT 1",
                ("India",),
            )
            cur.fetchone()
        finally:
            db.close()
        return True, "PostgreSQL connected, run_ledger accessible"
    except Exception as exc:
        return False, f"DB: {exc}"


def run_health_checks() -> List[CheckResult]:
    checks: List[Tuple[str, str, Callable[[], Tuple[bool, str]]]] = [
        ("Config", "PostgreSQL (run_ledger)", check_db_connection),
    ]
    results = []
    for step, label, runner in checks:
        try:
            passed, details = runner()
        except Exception as exc:
            passed, details = False, f"Exception: {exc}"
        results.append(
            CheckResult(
                step=step,
                check=label,
                status="PASS" if passed else "FAIL",
                detail=details,
            )
        )
    return results


def format_table(results: List[CheckResult]) -> List[str]:
    col_widths = [
        max(len(res.step) for res in results + [CheckResult("Step", "", "", "")]),
        max(len(res.check) for res in results + [CheckResult("", "Check", "", "")]),
        max(len(res.status) for res in results + [CheckResult("", "", "Status", "")]),
        0,
    ]
    lines = []
    header_line = (
        f"{'Step'.ljust(col_widths[0])} | "
        f"{'Check'.ljust(col_widths[1])} | "
        f"{'Status'.ljust(col_widths[2])} | Detail"
    )
    sep = "-" * len(header_line)
    lines.append(header_line)
    lines.append(sep)
    for res in results:
        lines.append(
            f"{res.step.ljust(col_widths[0])} | "
            f"{res.check.ljust(col_widths[1])} | "
            f"{res.status.ljust(col_widths[2])} | {res.detail}"
        )
    return lines


def persist_report(results: List[CheckResult]) -> tuple[Path, Path]:
    exports_dir = get_central_output_dir() / "health_check"
    exports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = exports_dir / f"health_check_India_{timestamp}.txt"
    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write("India Scraper Health Check\n")
        fh.write("=" * 60 + "\n")
        fh.write("\n".join(format_table(results)))
        fh.write("\n")
        fh.write("=" * 60 + "\n")
        fh.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    json_path = exports_dir / f"health_check_India_{timestamp}.json"
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump([res.__dict__ for res in results], fh, indent=2, ensure_ascii=False)
    return report_path, json_path


def main() -> None:
    print("[HEALTH CHECK] India scraper health (DB)", flush=True)
    results = run_health_checks()
    for line in format_table(results):
        print(line)
    report_path, json_path = persist_report(results)
    passed = sum(1 for res in results if res.status == "PASS")
    print(f"\n[HEALTH CHECK] {passed}/{len(results)} checks passed", flush=True)
    print(f"[HEALTH CHECK] Detailed report saved: {report_path}", flush=True)
    print(f"[HEALTH CHECK] JSON summary saved: {json_path}", flush=True)


if __name__ == "__main__":
    main()
