#!/usr/bin/env python3
"""
Canada Quebec Scraper - Enterprise Edition
Wraps legacy scripts in BaseScraper structure for uniformity and observability.
"""

import sys
import os
import subprocess

# ---- Path wiring ----
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from core.pipeline.base_scraper import BaseScraper

class CanadaQuebecScraper(BaseScraper):
    def __init__(self):
        super().__init__("CanadaQuebec")
        
    def run(self):
        self.logger.info("Starting Canada Quebec Enterprise Pipeline")
        
        steps = [
            ("00_backup_and_clean.py", "Backup and Clean", True),
            ("01_split_pdf_into_annexes.py", "Split PDF", True),
            ("02_validate_pdf_structure.py", "Validate Structure", False), # Optional
            ("03_extract_annexe_iv1.py", "Extract IV.1", True),
            ("04_extract_annexe_iv2.py", "Extract IV.2", True),
            ("05_extract_annexe_v.py", "Extract V", True),
            ("06_merge_all_annexes.py", "Merge Final CSV", True),
        ]
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        for script, desc, strict in steps:
            if self._shutdown_requested:
                break
                
            self.logger.info(f"Step: {desc} ({script})")
            
            try:
                env = os.environ.copy()
                env["CANADA_QUEBEC_RUN_ID"] = self.run_id
                
                script_path = os.path.join(base_dir, script)
                if not os.path.exists(script_path):
                     self.logger.error(f"Script not found: {script_path}")
                     self.record_error(f"missing_script_{script}")
                     if strict: continue

                self.record_request_metric(0, 0, f"START_{script}")
                
                ret = subprocess.run(
                    [sys.executable, "-u", script_path],
                    cwd=base_dir,
                    env=env,
                    check=strict # Raise error if strict is True
                )
                
                # If non-strict step fails, ret.returncode will be != 0 but check=False prevents raise
                if not strict and ret.returncode != 0:
                     self.logger.warning(f"Optional step {desc} failed with code {ret.returncode}")
                else:
                     self.logger.info(f"Step {desc} completed successfully")
                
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Step {desc} failed with exit code {e.returncode}")
                self.record_error(f"failed_step_{script}")
                if strict: raise e

        self.logger.info("Canada Quebec Pipeline Completed successfully")

if __name__ == "__main__":
    try:
        scraper = CanadaQuebecScraper()
        scraper.run()
    except Exception as e:
        print(f"Pipeline Failed: {e}")
        sys.exit(1)
