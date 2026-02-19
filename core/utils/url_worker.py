"""
Distributed URL Worker

Processes URLs from shared work queue across multiple nodes.
Each node runs its own Tor/browser but shares the same run_id.
"""

import os
import sys
import time
import logging
import socket
import uuid
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

# Add repo root to path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.pipeline.url_work_queue import URLWorkQueue
from core.browser.chrome_manager import kill_orphaned_chrome_processes
# CORRECTED IMPORTS
from core.network.proxy_checker import check_tor_running
from core.browser.driver_factory import create_firefox_driver, create_chrome_driver

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DistributedURLWorker:
    """Worker that processes URLs from distributed queue"""
    
    def __init__(self, scraper_name: str, run_id: str, db_config: Dict[str, Any]):
        """
        Initialize distributed worker.
        
        Args:
            scraper_name: Name of the scraper to process
            run_id: Shared run ID across all workers
            db_config: Database configuration
        """
        self.scraper_name = scraper_name
        self.run_id = run_id
        self.worker_id = f"{socket.gethostname()}_{os.getpid()}_{uuid.uuid4().hex[:8]}"
        self.queue = URLWorkQueue(db_config)
        self.driver = None
        self.running = True
        
        logger.info(f"Initialized worker {self.worker_id} for {scraper_name} run {run_id}")
    
    def setup_browser(self, use_tor: bool = True):
        """Setup browser session with optional Tor"""
        try:
            tor_config = {}
            if use_tor:
                # Use the corrected import
                tor_running, tor_port = check_tor_running()
                if not tor_running:
                    logger.warning("Tor not running, attempting to use direct connection")
                    use_tor = False
                else:
                    tor_config = {"enabled": True, "port": tor_port}
            
            if use_tor:
                # Use standardized driver factory
                self.driver = create_firefox_driver(
                    headless=True, 
                    tor_config=tor_config
                )
                logger.info(f"Worker {self.worker_id} initialized with Tor")
            else:
                # Use standardized driver factory
                self.driver = create_chrome_driver(headless=True)
                logger.info(f"Worker {self.worker_id} initialized without Tor")
            
        except Exception as e:
            logger.error(f"Failed to setup browser: {e}")
            raise
    
    def process_url(self, url: str,work_id: int) -> bool:
        """
        Process a single URL.
        
        Override this method in subclasses for scraper-specific logic.
        
        Args:
            url: URL to process
            work_id: Work queue item ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Processing URL: {url}")
            
            # Example: fetch page
            if self.driver:
                self.driver.get(url)
                time.sleep(2)  # Basic delay
                
                # TODO: Scraper-specific extraction logic here
                # This is just a template
                
                return True
            else:
                logger.error("No driver available")
                return False
                
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            return False
    
    def run(self, batch_size: int = 10, lease_seconds: int = 300, poll_interval: int = 5):
        """
        Main worker loop.
        
        Args:
            batch_size: Number of URLs to claim per batch
            lease_seconds: Lease duration for claimed URLs
            poll_interval: Seconds to wait between batch claims
        """
        try:
            self.setup_browser(use_tor=True)
            
            logger.info(f"Worker {self.worker_id} starting main loop")
            
            while self.running:
                # Release any expired leases first
                self.queue.release_expired_leases(lease_seconds)
                
                # Claim batch of URLs
                batch = self.queue.claim_batch(
                    worker_id=self.worker_id,
                    scraper_name=self.scraper_name,
                    run_id=self.run_id,
                    batch_size=batch_size,
                    lease_seconds=lease_seconds
                )
                
                if not batch:
                    # No work available, check if queue is empty
                    stats = self.queue.get_queue_stats(self.run_id, self.scraper_name)
                    
                    if stats['remaining'] == 0:
                        logger.info(f"Queue empty for {self.scraper_name} run {self.run_id}, shutting down")
                        break
                    else:
                        logger.info(f"No URLs available, waiting {poll_interval}s... "
                                  f"(Remaining: {stats['remaining']})")
                        time.sleep(poll_interval)
                        continue
                
                # Process batch
                for item in batch:
                    url = item['url']
                    work_id = item['id']
                    
                    try:
                        success = self.process_url(url, work_id)
                        
                        if success:
                            self.queue.complete_url(work_id, success=True)
                            logger.info(f"✓ Completed: {url}")
                        else:
                            self.queue.complete_url(
                                work_id,
                                success=False,
                                error_message="Processing returned False"
                            )
                            logger.warning(f"✗ Failed: {url}")
                    
                    except Exception as e:
                        logger.error(f"Exception processing {url}: {e}")
                        self.queue.complete_url(
                            work_id,
                            success=False,
                            error_message=str(e)
                        )
                
                # Brief pause between batches
                time.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        except Exception as e:
            logger.error(f"Worker error: {e}", exc_info=True)
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        logger.info(f"Worker {self.worker_id} cleaning up")
        
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.warning(f"Worker {self.worker_id} driver quit failed: {e}")
        
        # Final stats
        try:
            stats = self.queue.get_queue_stats(self.run_id, self.scraper_name)
            logger.info(f"Final stats for {self.scraper_name} run {self.run_id}: {stats}")
        except:
            pass


def _load_platform_env():
    """Load platform.env and .env before reading config (all values from env files)."""
    from dotenv import load_dotenv
    repo_root = Path(__file__).resolve().parents[2]
    platform_env = repo_root / "config" / "platform.env"
    if platform_env.exists():
        load_dotenv(platform_env, override=False)
    root_env = repo_root / ".env"
    if root_env.exists():
        load_dotenv(root_env, override=False)


def main():
    """Main entry point for distributed worker"""
    import argparse

    _load_platform_env()

    def _env(key: str, default: str = "") -> str:
        return os.getenv(key, default)

    def _env_int(key: str, default: int) -> int:
        try:
            return int(os.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default

    parser = argparse.ArgumentParser(description="Distributed URL Worker")
    parser.add_argument("--scraper", required=True, help="Scraper name")
    parser.add_argument("--run-id", required=True, help="Run ID")
    parser.add_argument("--batch-size", type=int,
                       default=_env_int("WORKER_BATCH_SIZE", 10), help="Batch size (env: WORKER_BATCH_SIZE)")
    parser.add_argument("--db-host", default=_env("POSTGRES_HOST") or _env("DB_HOST", "localhost"),
                        help="Database host (env: POSTGRES_HOST or DB_HOST)")
    parser.add_argument("--db-port", type=int,
                       default=_env_int("POSTGRES_PORT", 0) or _env_int("DB_PORT", 5432),
                       help="Database port (env: POSTGRES_PORT or DB_PORT)")
    parser.add_argument("--db-name", default=_env("POSTGRES_DB") or _env("DB_NAME", "scraper_db"),
                        help="Database name (env: POSTGRES_DB or DB_NAME)")
    parser.add_argument("--db-user", default=_env("POSTGRES_USER") or _env("DB_USER", "postgres"),
                        help="Database user (env: POSTGRES_USER or DB_USER)")
    parser.add_argument("--db-password", default=_env("POSTGRES_PASSWORD") or _env("DB_PASSWORD", ""),
                        help="Database password (env: POSTGRES_PASSWORD or DB_PASSWORD)")
    
    args = parser.parse_args()
    
    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_password
    }
    
    worker = DistributedURLWorker(
        scraper_name=args.scraper,
        run_id=args.run_id,
        db_config=db_config
    )
    
    worker.run(batch_size=args.batch_size)


if __name__ == "__main__":
    main()
