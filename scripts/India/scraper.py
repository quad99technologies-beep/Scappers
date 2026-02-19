#!/usr/bin/env python3
"""
India Scraper - Enterprise Edition
Wraps Scrapy pipeline in BaseScraper structure for uniformity and observability.
"""

import sys
import os
import subprocess
from pathlib import Path

# ---- Path wiring ----
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline.base_scraper import BaseScraper

class IndiaScraper(BaseScraper):
    def __init__(self):
        super().__init__("India")
        
    def run(self):
        self.logger.info("Starting India Enterprise Pipeline (Scrapy)")
        
        # We forward all arguments to the underlying pipeline script
        script_args = sys.argv[1:]
        
        step_script = "run_pipeline_scrapy.py"
        base_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(base_dir, step_script)
        
        if not os.path.exists(script_path):
             self.logger.error(f"Script not found: {script_path}")
             self.record_error("missing_pipeline_script")
             return

        self.logger.info(f"Delegating to {step_script} with args: {script_args}")
        self.record_request_metric(0, 0, "START_SCRAPY_PIPELINE")
        
        try:
            env = os.environ.copy()
            # If we had a run_id from BaseScraper, we could pass it, but India manages its own
            
            # Use subprocess to run the complex Scrapy pipeline
            ret = subprocess.run(
                [sys.executable, "-u", script_path] + script_args,
                cwd=base_dir,
                env=env,
                check=True
            )
            
            self.logger.info("India Pipeline Delegate Completed successfully")
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"India Pipeline failed with exit code {e.returncode}")
            self.record_error("failed_pipeline")
            raise e

if __name__ == "__main__":
    try:
        scraper = IndiaScraper()
        scraper.run()
    except Exception as e:
        print(f"Pipeline Failed: {e}")
        sys.exit(1)
