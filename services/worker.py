#!/usr/bin/env python3
"""
Distributed Worker for Pipeline Execution.

This worker:
- Polls PostgreSQL for queued jobs
- Claims jobs atomically
- Executes pipeline steps
- Sends heartbeats
- Handles stop/resume commands
- Recovers from crashes

Usage:
    python worker.py [--countries COUNTRY1,COUNTRY2] [--once]
    
Arguments:
    --countries: Comma-separated list of countries to handle (default: all)
    --once: Run one job and exit (for testing)
    --heartbeat-interval: Heartbeat interval in seconds (default: 30)
"""

import os
import sys
import time
import signal
import logging
import argparse
import threading
import traceback
from datetime import datetime
from typing import List, Optional, Callable, Dict, Any

# Add parent directories to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from services.db import (
    generate_worker_id,
    register_worker,
    update_worker_heartbeat,
    unregister_worker,
    claim_next_run,
    update_run_status,
    update_run_step,
    heartbeat as db_heartbeat,
    get_latest_command,
    acknowledge_command,
    ensure_platform_schema,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [worker] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)


# =============================================================================
# Pipeline Runner Registry
# =============================================================================

# Registry of pipeline runners by country
_pipeline_runners: Dict[str, Callable] = {}


def register_pipeline(country: str, runner: Callable) -> None:
    """
    Register a pipeline runner for a country.
    
    Args:
        country: Country name
        runner: Callable that takes (run_id, start_step, check_stop_callback) and runs the pipeline
    """
    _pipeline_runners[country] = runner
    log.info(f"Registered pipeline runner for {country}")


def get_available_countries() -> List[str]:
    """Get list of countries with registered pipeline runners."""
    return list(_pipeline_runners.keys())


# =============================================================================
# Worker Class
# =============================================================================

class Worker:
    """Distributed worker that executes pipeline jobs from PostgreSQL queue."""
    
    def __init__(
        self,
        worker_id: Optional[str] = None,
        countries: Optional[List[str]] = None,
        heartbeat_interval: int = 30,
        poll_interval: int = 5
    ):
        """
        Initialize the worker.
        
        Args:
            worker_id: Unique worker ID (auto-generated if not provided)
            countries: List of countries to handle (None = all registered)
            heartbeat_interval: Heartbeat interval in seconds
            poll_interval: Poll interval when no jobs available
        """
        self.worker_id = worker_id or generate_worker_id()
        self.countries = countries
        self.heartbeat_interval = heartbeat_interval
        self.poll_interval = poll_interval
        
        # State
        self._running = False
        self._current_run_id: Optional[str] = None
        self._stop_requested = False
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop = threading.Event()
        
        # Signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        log.warning(f"Received signal {signum}, initiating graceful shutdown...")
        self._stop_requested = True
        self._heartbeat_stop.set()
    
    def _heartbeat_loop(self):
        """Background thread that sends heartbeats."""
        while not self._heartbeat_stop.wait(self.heartbeat_interval):
            if self._current_run_id:
                try:
                    db_heartbeat(self._current_run_id)
                    update_worker_heartbeat(self.worker_id, "busy", self._current_run_id)
                except Exception as e:
                    log.warning(f"Heartbeat failed: {e}")
    
    def _start_heartbeat(self):
        """Start the heartbeat thread."""
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
    
    def _stop_heartbeat(self):
        """Stop the heartbeat thread."""
        self._heartbeat_stop.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5)
            self._heartbeat_thread = None
    
    def check_stop_requested(self) -> bool:
        """
        Check if stop has been requested (via signal or DB command).
        
        This is passed to pipeline runners so they can check between steps.
        
        Returns:
            True if stop requested, False otherwise
        """
        if self._stop_requested:
            return True
        
        if self._current_run_id:
            try:
                cmd = get_latest_command(self._current_run_id)
                if cmd and cmd['command'] in ('stop', 'cancel'):
                    log.info(f"Stop command received: {cmd['command']}")
                    acknowledge_command(cmd['id'])
                    return True
            except Exception as e:
                log.warning(f"Failed to check commands: {e}")
        
        return False
    
    def run_one_job(self) -> bool:
        """
        Try to claim and run one job.
        
        Returns:
            True if a job was executed, False if no jobs available
        """
        # Determine which countries to handle
        countries = self.countries or get_available_countries()
        if not countries:
            log.warning("No pipeline runners registered and no countries specified")
            return False
        
        # Try to claim a job
        job = claim_next_run(self.worker_id, countries)
        if not job:
            return False
        
        run_id = str(job['run_id'])
        country = job['country']
        current_step = job.get('current_step_num', 0) or 0
        
        log.info(f"Claimed job: run_id={run_id}, country={country}, step={current_step}")
        
        self._current_run_id = run_id
        self._start_heartbeat()
        
        max_retries = 3
        retry_delay = 60  # seconds
        attempt = 0
        
        try:
            while attempt < max_retries:
                try:
                    # Get the pipeline runner
                    runner = _pipeline_runners.get(country)
                    if not runner:
                        raise ValueError(f"No pipeline runner registered for {country}")
                    
                    # Run the pipeline
                    log.info(f"Starting pipeline for {country} from step {current_step} (attempt {attempt + 1}/{max_retries})")
                    runner(run_id, current_step, self.check_stop_requested)
                    
                    # Check if stopped or completed
                    if self.check_stop_requested():
                        log.info(f"Pipeline stopped: run_id={run_id}")
                        update_run_status(run_id, "stopped")
                    else:
                        log.info(f"Pipeline completed: run_id={run_id}")
                        update_run_status(run_id, "completed")
                    
                    # Success - break retry loop
                    break
                    
                except Exception as e:
                    attempt += 1
                    error_msg = f"{type(e).__name__}: {str(e)}"
                    stack = traceback.format_exc()
                    log.error(f"Pipeline failed: run_id={run_id}, error={error_msg} (attempt {attempt}/{max_retries})")
                    log.debug(stack)
                    
                    if attempt < max_retries:
                        # Retry with exponential backoff
                        wait_time = retry_delay * (2 ** (attempt - 1))
                        log.info(f"Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        # Max retries reached - mark as failed
                        update_run_status(run_id, "failed", error_message=error_msg)
                        log.error(f"Pipeline failed after {max_retries} attempts: {error_msg}")
        finally:
            self._stop_heartbeat()
            self._current_run_id = None
            update_worker_heartbeat(self.worker_id, "idle", None)
        
        return True
    
    def run(self, once: bool = False):
        """
        Main worker loop.
        
        Args:
            once: If True, run one job and exit
        """
        log.info(f"Worker starting: id={self.worker_id}")
        
        # Ensure schema exists
        try:
            ensure_platform_schema()
        except Exception as e:
            log.warning(f"Could not ensure schema: {e}")
        
        # Register worker
        try:
            register_worker(self.worker_id, capabilities=self.countries)
        except Exception as e:
            log.warning(f"Could not register worker: {e}")
        
        self._running = True
        jobs_run = 0
        
        try:
            while self._running and not self._stop_requested:
                try:
                    if self.run_one_job():
                        jobs_run += 1
                        if once:
                            log.info("Single job mode, exiting after one job")
                            break
                    else:
                        # No jobs available, wait before polling again
                        update_worker_heartbeat(self.worker_id, "idle", None)
                        log.debug(f"No jobs available, sleeping {self.poll_interval}s")
                        time.sleep(self.poll_interval)
                        
                except KeyboardInterrupt:
                    log.info("Keyboard interrupt, shutting down...")
                    break
                except Exception as e:
                    log.error(f"Error in worker loop: {e}")
                    log.debug(traceback.format_exc())
                    time.sleep(self.poll_interval)
                    
        finally:
            self._running = False
            self._stop_heartbeat()
            
            try:
                unregister_worker(self.worker_id)
            except Exception:
                pass
            
            log.info(f"Worker stopped: id={self.worker_id}, jobs_run={jobs_run}")


# =============================================================================
# Pipeline Runner Factory
# =============================================================================

def create_subprocess_runner(country: str, pipeline_script: Optional[str] = None) -> Callable:
    """
    Create a pipeline runner that executes via subprocess.
    
    This allows running existing run_pipeline_resume.py scripts without modification.
    
    Args:
        country: Country name
        pipeline_script: Path to pipeline script (default: scripts/{country}/run_pipeline_resume.py)
        
    Returns:
        Runner callable
    """
    import subprocess
    
    if pipeline_script is None:
        scripts_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        pipeline_script = os.path.join(scripts_dir, country, "run_pipeline_resume.py")
    
    def runner(run_id: str, start_step: int, check_stop: Callable) -> None:
        """Run pipeline via subprocess."""
        env = os.environ.copy()
        env['WORKER_RUN_ID'] = run_id
        env['WORKER_START_STEP'] = str(start_step)
        
        cmd = [sys.executable, pipeline_script, "--step", str(start_step)]
        
        log.info(f"Running: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Stream output and check for stop
        try:
            while True:
                # Check if stop requested
                if check_stop():
                    log.info("Stop requested, terminating subprocess...")
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    return
                
                # Read output
                line = process.stdout.readline()
                if line:
                    log.info(f"[{country}] {line.rstrip()}")
                
                # Check if process ended
                if process.poll() is not None:
                    break
                    
            # Read remaining output
            remaining = process.stdout.read()
            if remaining:
                for line in remaining.splitlines():
                    log.info(f"[{country}] {line}")
            
            # Check exit code
            if process.returncode != 0:
                raise RuntimeError(f"Pipeline exited with code {process.returncode}")
                
        except Exception:
            process.kill()
            raise
    
    return runner


def auto_register_pipelines():
    """Automatically register pipeline runners for all countries with run_pipeline_resume.py."""
    scripts_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # List of country folders to check
    potential_countries = [
        "Argentina", "Belarus", "Canada Ontario", "CanadaQuebec",
        "India", "Malaysia", "Netherlands", "North Macedonia",
        "Russia", "Taiwan", "Tender- Chile"
    ]
    
    for country in potential_countries:
        country_dir = os.path.join(scripts_dir, country)
        pipeline_script = os.path.join(country_dir, "run_pipeline_resume.py")
        
        if os.path.exists(pipeline_script):
            runner = create_subprocess_runner(country, pipeline_script)
            register_pipeline(country, runner)


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Distributed pipeline worker")
    parser.add_argument(
        "--countries",
        type=str,
        help="Comma-separated list of countries to handle (default: all)"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one job and exit"
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=int,
        default=30,
        help="Heartbeat interval in seconds (default: 30)"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Poll interval when no jobs available (default: 5)"
    )
    parser.add_argument(
        "--worker-id",
        type=str,
        help="Worker ID (auto-generated if not provided)"
    )
    parser.add_argument(
        "--auto-register",
        action="store_true",
        default=True,
        help="Auto-register pipeline runners for all countries"
    )
    
    args = parser.parse_args()
    
    # Parse countries
    countries = None
    if args.countries:
        countries = [c.strip() for c in args.countries.split(",")]
    
    # Auto-register pipelines
    if args.auto_register:
        auto_register_pipelines()
    
    # Create and run worker
    worker = Worker(
        worker_id=args.worker_id,
        countries=countries,
        heartbeat_interval=args.heartbeat_interval,
        poll_interval=args.poll_interval
    )
    
    worker.run(once=args.once)


if __name__ == "__main__":
    main()
