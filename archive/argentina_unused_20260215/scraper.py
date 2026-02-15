#!/usr/bin/env python3
"""
Argentina Scraper - Enterprise Edition
Wraps legacy scripts in BaseScraper structure for uniformity and observability.
"""

import sys
import os

# ---- Path wiring ----
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from core.base_scraper import BaseScraper
import subprocess
import sys
import os

class ArgentinaScraper(BaseScraper):
    def __init__(self):
        super().__init__("Argentina")
        
    def run(self):
        self.logger.info("Starting Argentina Enterprise Pipeline")
        
        # Define steps (Legacy scripts)
        steps = [
            ("00_backup_and_clean.py", "Backup and Clean"),
            ("01_getProdList.py", "Get Product List"),
            ("02_prepare_urls.py", "Prepare URLs"),
            ("03_alfabeta_selenium_scraper.py", "Scrape Products (Selenium)"),
            ("04_alfabeta_api_scraper.py", "Scrape Products (API)"),
            ("05_TranslateUsingDictionary.py", "Translate"),
            ("06_GenerateOutput.py", "Generate Output"),
        ]
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        for script, desc in steps:
            if self._shutdown_requested:
                break
                
            self.logger.info(f"Step: {desc} ({script})")
            
            try:
                # Use subprocess to isolate legacy global state
                # Pass run_id via env if needed, but legacy scripts manage their own state mostly
                env = os.environ.copy()
                env["ARGENTINA_RUN_ID"] = self.run_id
                
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
                # For now, we continue or stop?
                # Enterprise standard usually means stop on critical error
                raise

        self.logger.info("Argentina Pipeline Completed successfully")

if __name__ == "__main__":
    try:
        scraper = ArgentinaScraper()
        scraper.run()
    except Exception as e:
        print(f"Pipeline Failed: {e}")
        sys.exit(1)
