#!/usr/bin/env python3
"""
Taiwan Scraper - Enterprise Edition
Wraps legacy scripts in BaseScraper structure for uniformity and observability.
"""

import sys
import os
import subprocess

# ---- Path wiring ----
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from core.pipeline.base_scraper import BaseScraper

class TaiwanScraper(BaseScraper):
    def __init__(self):
        super().__init__("Taiwan")
        
    def run(self):
        self.logger.info("Starting Taiwan Enterprise Pipeline")
        
        steps = [
            ("00_backup_and_clean.py", "Backup and Clean", True),
            ("01_taiwan_collect_drug_code_urls.py.py", "Collect URLs", True),
            ("02_taiwan_extract_drug_code_details.py", "Extract Details", True),
        ]
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        for script, desc, strict in steps:
            if self._shutdown_requested:
                break
                
            self.logger.info(f"Step: {desc} ({script})")
            
            try:
                env = os.environ.copy()
                env["TAIWAN_RUN_ID"] = self.run_id
                
                script_path = os.path.join(base_dir, script)
                if not os.path.exists(script_path):
                     # Handle the possible double .py typo in file system vs code
                     # If the double .py doesn't exist, try single .py
                     if script.endswith(".py.py"):
                         alt_script = script[:-3]
                         alt_path = os.path.join(base_dir, alt_script)
                         if os.path.exists(alt_path):
                             script = alt_script
                             script_path = alt_path
                             
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

        self.logger.info("Taiwan Pipeline Completed successfully")

if __name__ == "__main__":
    try:
        scraper = TaiwanScraper()
        scraper.run()
    except Exception as e:
        print(f"Pipeline Failed: {e}")
        sys.exit(1)
