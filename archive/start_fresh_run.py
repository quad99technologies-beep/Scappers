#!/usr/bin/env python3
"""Start a fresh pipeline run with parallel processing."""

import os
import sys
import subprocess
import time
from pathlib import Path

# Set environment variables
os.environ["TENDER_CHILE_RUN_ID"] = f"run_{time.strftime('%Y%m%d_%H%M%S')}"

print("="*80)
print("STARTING FRESH PIPELINE RUN WITH PARALLEL PROCESSING")
print("="*80)
print(f"Run ID: {os.environ['TENDER_CHILE_RUN_ID']}")
print(f"Max Tenders: 6000")
print(f"Workers: 4 (parallel)")
print()

# Change to script directory
script_dir = Path(__file__).parent / "scripts" / "Tender- Chile"
os.chdir(script_dir)

# Run the pipeline with fresh flag
result = subprocess.run(
    ["python", "run_pipeline_resume.py", "--fresh"],
    capture_output=False,
    text=True
)

sys.exit(result.returncode)
