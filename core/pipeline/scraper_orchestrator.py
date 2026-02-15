"""
Distributed Scraper Orchestrator

Routes scrapers to either single-node execution or distributed queue
based on per-scraper execution_mode configuration.
"""

import os
import sys
import logging
import uuid
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime

# Add repo root to path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from scripts.common.scraper_registry import (
    get_scraper_config,
    get_execution_mode,
    get_run_id_env_var,
    get_pipeline_script
)
from core.url_work_queue import URLWorkQueue

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ScraperOrchestrator:
    """Routes scrapers to single or distributed execution"""
    
    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """
        Initialize orchestrator.
        
        Args:
            db_config: Database configuration for distributed mode
        """
        self.db_config = db_config or self._get_default_db_config()
        self.queue = URLWorkQueue(self.db_config)
    
    def _get_default_db_config(self) -> Dict[str, Any]:
        """Get default database configuration from environment"""
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', '5432')),
            'database': os.getenv('DB_NAME', 'scraper_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }
    
    def start_scraper(self, scraper_name: str, resume: bool = True, 
                     urls: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Start a scraper based on its execution mode.
        
        Args:
            scraper_name: Name of the scraper
            resume: Whether to resume from checkpoint (single mode)
            urls: URLs to process (distributed mode)
            
        Returns:
            dict with status and run_id
        """
        config = get_scraper_config(scraper_name)
        if not config:
            return {
                'status': 'error',
                'message': f"Scraper '{scraper_name}' not found in registry"
            }
        
        execution_mode = get_execution_mode(scraper_name)
        
        if execution_mode == "distributed":
            return self._start_distributed(scraper_name, urls or [])
        else:
            return self._start_single(scraper_name, resume)
    
    def _start_single(self, scraper_name: str, resume: bool) -> Dict[str, Any]:
        """
        Start scraper in single-node mode (existing behavior).
        
        Args:
            scraper_name: Scraper name
            resume: Whether to resume from checkpoint
            
        Returns:
            dict with status and run_id
        """
        import subprocess
        
        logger.info(f"Starting {scraper_name} in SINGLE mode (resume={resume})")
        
        # Generate run_id
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        env_var = get_run_id_env_var(scraper_name)
        
        # Get pipeline script
        pipeline_script = get_pipeline_script(scraper_name)
        if not pipeline_script or not pipeline_script.exists():
            return {
                'status': 'error',
                'message': f"Pipeline script not found for {scraper_name}"
            }
        
        # Build command
        cmd = [sys.executable, str(pipeline_script)]
        if resume:
            cmd.append("--resume")
        else:
            cmd.append("--fresh")
        
        # Set environment
        env = os.environ.copy()
        env[env_var] = run_id
        
        # Start process (non-blocking)
        try:
            process = subprocess.Popen(
                cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=pipeline_script.parent
            )
            
            logger.info(f"Started {scraper_name} with PID {process.pid}, run_id={run_id}")
            
            return {
                'status': 'started',
                'mode': 'single',
                'run_id': run_id,
                'pid': process.pid
            }
        
        except Exception as e:
            logger.error(f"Failed to start {scraper_name}: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def _start_distributed(self, scraper_name: str, urls: List[str]) -> Dict[str, Any]:
        """
        Start scraper in distributed mode (queue-based).
        
        Args:
            scraper_name: Scraper name
            urls: URLs to distribute across workers
            
        Returns:
            dict with status and run_id
        """
        logger.info(f"Starting {scraper_name} in DISTRIBUTED mode with {len(urls)} URLs")
        
        # Generate shared run_id
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if not urls:
            logger.warning(f"No URLs provided for distributed scraper {scraper_name}")
            return {
                'status': 'error',
                'message': 'No URLs provided for distributed mode'
            }
        
        # Enqueue URLs
        enqueued = self.queue.enqueue_urls(
            run_id=run_id,
            scraper_name=scraper_name,
            urls=urls,
            priority=0
        )
        
        # Get queue stats
        stats = self.queue.get_queue_stats(run_id, scraper_name)
        
        logger.info(f"Enqueued {enqueued} URLs for {scraper_name} run {run_id}")
        
        return {
            'status': 'queued',
            'mode': 'distributed',
            'run_id': run_id,
            'enqueued': enqueued,
            'stats': stats,
            'worker_command': self._get_worker_command(scraper_name, run_id)
        }
    
    def _get_worker_command(self, scraper_name: str, run_id: str) -> str:
        """
        Get command to start workers for distributed run.
        
        Args:
            scraper_name: Scraper name
            run_id: Run ID
            
        Returns:
            Worker start command
        """
        return (
            f"python core/url_worker.py "
            f"--scraper {scraper_name} "
            f"--run-id {run_id} "
            f"--db-host {self.db_config['host']} "
            f"--db-port {self.db_config['port']} "
            f"--db-name {self.db_config['database']} "
            f"--db-user {self.db_config['user']}"
        )
    
    def get_stats(self, scraper_name: str, run_id: str) -> Dict[str, Any]:
        """
        Get statistics for a distributed run.
        
        Args:
            scraper_name: Scraper name
            run_id: Run ID
            
        Returns:
            Queue statistics
        """
        execution_mode = get_execution_mode(scraper_name)
        
        if execution_mode != "distributed":
            return {
                'status': 'error',
                'message': f"{scraper_name} is not a distributed scraper"
            }
        
        stats = self.queue.get_queue_stats(run_id, scraper_name)
        return {
            'status': 'success',
            'scraper': scraper_name,
            'run_id': run_id,
            'stats': stats
        }


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Scraper Orchestrator")
    parser.add_argument("scraper", help="Scraper name")
    parser.add_argument("--mode", choices=['single', 'distributed'], 
                       help="Force execution mode (overrides config)")
    parser.add_argument("--resume", action="store_true", 
                       help="Resume from checkpoint (single mode)")
    parser.add_argument("--fresh", action="store_true", 
                       help="Fresh start (single mode)")
    parser.add_argument("--urls-file", type=Path, 
                       help="File with URLs to process (distributed mode)")
    
    args = parser.parse_args()
    
    orchestrator = ScraperOrchestrator()
    
    # Load URLs if provided
    urls = []
    if args.urls_file and args.urls_file.exists():
        with open(args.urls_file, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(urls)} URLs from {args.urls_file}")
    
    # Start scraper
    result = orchestrator.start_scraper(
        scraper_name=args.scraper,
        resume=args.resume or not args.fresh,
        urls=urls
    )
    
    # Print result
    import json
    print(json.dumps(result, indent=2))
    
    if result['status'] == 'queued':
        print("\nTo start workers, run on each node:")
        print(f"  {result['worker_command']}")


if __name__ == "__main__":
    main()
