#!/usr/bin/env python3
"""
Canada Ontario Scraper - Enterprise Edition
Wraps legacy scripts in BaseScraper structure for uniformity and observability.
"""

import sys
import os

# ---- Path wiring ----
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from core.pipeline.base_scraper import BaseScraper
import subprocess

class CanadaOntarioScraper(BaseScraper):
    def __init__(self):
        super().__init__("CanadaOntario")
        
    def run(self):
        self.logger.info("Starting Canada Ontario Enterprise Pipeline")
        
        # Define steps (Legacy scripts)
        steps = [
            ("00_backup_and_clean.py", "Backup and Clean"),
            ("01_extract_product_details.py", "Extract Product Details"),
            ("02_ontario_eap_prices.py", "Extract EAP Prices"),
            ("03_GenerateOutput.py", "Generate Final Output"),
        ]
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        for script, desc in steps:
            if self._shutdown_requested:
                break
                
            self.logger.info(f"Step: {desc} ({script})")
            
            try:
                env = os.environ.copy()
                env["CANADA_ONTARIO_RUN_ID"] = self.run_id
                
                # Check if script exists
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
                    check=True
                )
                
                self.logger.info(f"Step {desc} completed successfully")
                
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Step {desc} failed with exit code {e.returncode}")
                self.record_error(f"failed_step_{script}")
                raise

        self.logger.info("Canada Ontario Pipeline Completed successfully")

if __name__ == "__main__":
    try:
        scraper = CanadaOntarioScraper()
        scraper.run()
    except Exception as e:
        print(f"Pipeline Failed: {e}")
        sys.exit(1)
