#!/usr/bin/env python3
"""
Standardized pipeline runner for India.
Wraps the Scrapy-based run_scrapy_india.py to provide a consistent interface.
"""
import sys
import subprocess
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.config.config_manager import ConfigManager
from core.utils.logger import get_logger

logger = get_logger("india_pipeline")

def main():
    script_dir = Path(__file__).parent
    scrapy_runner = script_dir / "run_scrapy_india.py"
    
    if not scrapy_runner.exists():
        logger.error(f"Scrapy runner not found at: {scrapy_runner}")
        sys.exit(1)
        
    # Map standard args to Scrapy runner args
    cmd = [sys.executable, str(scrapy_runner)]
    
    # Pass through arguments
    # If --fresh is passed, Scrapy runner handles it directly
    if "--fresh" in sys.argv:
        cmd.append("--fresh")
        
    # Pass through other common args if present
    for arg in sys.argv[1:]:
        if arg not in cmd:
            cmd.append(arg)
            
    logger.info(f"Checking configuration for India...")
    try:
        # Verify config exists
        ConfigManager.load_env("India")
    except Exception as e:
        logger.warning(f"Config check warning: {e}")
        # Continue anyway, let the runner handle it
        
    logger.info("Starting India Scrapy pipeline...")
    logger.info(f"Command: {' '.join(cmd)}")
    
    try:
        # Run and stream output
        process = subprocess.Popen(
            cmd,
            stdout=sys.stdout,
            stderr=sys.stderr,
            universal_newlines=True,
            bufsize=1
        )
        return_code = process.wait()
        
        if return_code != 0:
            logger.error(f"India pipeline failed with code {return_code}")
            sys.exit(return_code)
            
        logger.info("India pipeline completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Pipeline execution error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
