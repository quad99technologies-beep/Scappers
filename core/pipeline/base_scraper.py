import sys
import os
import logging
from typing import Optional, List, Dict, Any, Union
import asyncio
import httpx

from core.config.config_manager import ConfigManager
from core.db.postgres_connection import PostgresDB
from core.utils.text_utils import normalize_ws

class BaseScraper:
    """
    Abstract base scraper class enforcing structure and providing utilities.
    """
    def __init__(self, scraper_name: str, run_id: Optional[str] = None):
        """
        Initialize the scraper.
        
        Args:
            scraper_name: Name of the scraper (e.g. "Netherlands")
            run_id: Optional specific run_id. If not provided, loaded from env.
        """
        self.scraper_name = scraper_name
        
        # 1. Load Config
        # Ensure directories exist first
        ConfigManager.ensure_dirs()
        self.config = ConfigManager.load_env(scraper_name)
        
        # 2. Setup Logging
        # Only configure basicConfig if root logger is not configured? 
        # For now, just setting up self.logger is enough, basicConfig might be handled by caller or kept simple.
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[logging.StreamHandler(sys.stdout)]
            )
        self.logger = logging.getLogger(f"scraper.{scraper_name}")
        
        # 3. Determine Run ID
        if not run_id:
            # Try scraper specific env var first, then generic
            run_id = os.environ.get(f"{scraper_name.upper()}_RUN_ID") or os.environ.get("RUN_ID")
        self.run_id = run_id
        
        if not self.run_id:
            self.logger.warning("No RUN_ID provided or found in environment variables.")

        # 4. Setup DB
        self.db = PostgresDB(scraper_name)
        
        self.logger.info(f"Initialized {scraper_name} Scraper (RunID: {self.run_id})")
        
        # Shutdown flag for graceful exit
        self._shutdown_requested = False

    def record_error(self, error_type: str, message: str = None):
        """Record an error metric."""
        # Placeholder for error recording logic (e.g. to stats table)
        # implementation optional for now, can be extended later
        pass

    def record_scraped_item(self, count: int = 1, item_type: str = "item"):
        """Record a scraped item metric."""
        # Placeholder for metric recording
        pass

    async def collect_urls(self):
        """Must override"""
        raise NotImplementedError

    def run(self):
        """Synchronous entry point."""
        raise NotImplementedError
