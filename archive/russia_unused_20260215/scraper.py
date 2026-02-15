#!/usr/bin/env python3
"""
Russia Scraper - Enterprise Edition
Wraps legacy scripts in BaseScraper structure for uniformity and observability.
"""

import sys
import os
import subprocess

# ---- Path wiring ----
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from core.base_scraper import BaseScraper

class RussiaScraper(BaseScraper):
    def __init__(self):
        super().__init__("Russia")
        
    def run(self):
        self.logger.info("Starting Russia Enterprise Pipeline")
        
        steps = [
            ("00_backup_and_clean.py", "Backup and Clean", True),
            ("01_russia_farmcom_scraper.py", "Scrape VED Registry", True),
            ("02_russia_farmcom_excluded_scraper.py", "Scrape Excluded", True),
            ("03_retry_failed_pages.py", "Retry Failed", True),
            ("04_process_and_translate.py", "Process & Translate", True),
            ("05_format_for_export.py", "Format Export", True),
        ]
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        for script, desc, strict in steps:
            if self._shutdown_requested:
                break
                
            self.logger.info(f"Step: {desc} ({script})")
            
            try:
                env = os.environ.copy()
                env["RUSSIA_RUN_ID"] = self.run_id
                
                script_path = os.path.join(base_dir, script)
                if not os.path.exists(script_path):
                     self.logger.error(f"Script not found: {script_path}")
                     self.record_error(f"missing_script_{script}")
                     continue

                self.record_request_metric(0, 0, f"START_{script}")
                
                ret = subprocess.run(
                    [sys.executable, "-u", script_path],
                    cwd=base_dir,
                    env=env,
                    check=strict
                )
                     
                self.logger.info(f"Step {desc} completed successfully")
                
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Step {desc} failed with exit code {e.returncode}")
                self.record_error(f"failed_step_{script}")
                if strict: raise e

        self.logger.info("Russia Pipeline Completed successfully")

if __name__ == "__main__":
    try:
        scraper = RussiaScraper()
        scraper.run()
    except Exception as e:
        print(f"Pipeline Failed: {e}")
        sys.exit(1)
