#!/usr/bin/env python3
"""
India pipeline wrapper for distributed workers.

This adapter keeps the existing pipeline intact by delegating to
run_pipeline_scrapy.py, while wiring PLATFORM_RUN_ID from WORKER_RUN_ID
when invoked by services/worker.py.
"""

import os
import sys
import subprocess
from pathlib import Path


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    target = script_dir / "run_pipeline_scrapy.py"

    if not target.exists():
        print(f"ERROR: run_pipeline_scrapy.py not found at {target}")
        return 1

    env = os.environ.copy()
    if env.get("WORKER_RUN_ID") and not env.get("PLATFORM_RUN_ID"):
        env["PLATFORM_RUN_ID"] = env["WORKER_RUN_ID"]

    cmd = [sys.executable, str(target)] + sys.argv[1:]
    return subprocess.call(cmd, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
