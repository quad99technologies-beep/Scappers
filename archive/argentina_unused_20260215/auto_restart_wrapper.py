#!/usr/bin/env python3
"""
Argentina Scraper Auto-Restart Wrapper

Monitors the scraper and automatically restarts it if:
1. No progress for X minutes (hang detection)
2. Process crashes/exits with error
3. Memory usage exceeds threshold

Usage:
    python auto_restart_wrapper.py [--max-runtime-hours 4] [--no-progress-timeout 600]

Features:
- Resumes from DB automatically (no data loss)
- Logs all restarts
- Limits total runtime to prevent infinite loops
- Can be stopped gracefully with Ctrl+C
"""

import argparse
import os
import sys
import time
import signal
import subprocess
import psutil
from pathlib import Path
from datetime import datetime, timedelta

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent

# Default settings
DEFAULT_MAX_RUNTIME_HOURS = 8
DEFAULT_NO_PROGRESS_TIMEOUT = 600  # 10 minutes
DEFAULT_MEMORY_LIMIT_MB = 2048
CHECK_INTERVAL = 30  # Check every 30 seconds


class ScraperMonitor:
    """Monitors and auto-restarts the Argentina scraper"""
    
    def __init__(self, max_runtime_hours=DEFAULT_MAX_RUNTIME_HOURS,
                 no_progress_timeout=DEFAULT_NO_PROGRESS_TIMEOUT,
                 memory_limit_mb=DEFAULT_MEMORY_LIMIT_MB):
        self.max_runtime_hours = max_runtime_hours
        self.no_progress_timeout = no_progress_timeout
        self.memory_limit_mb = memory_limit_mb
        
        self.start_time = datetime.now()
        self.last_progress_time = datetime.now()
        self.last_progress_count = 0
        self.restart_count = 0
        self.process = None
        self.shutdown_requested = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\n[WRAPPER] Received signal {signum}, shutting down...", flush=True)
        self.shutdown_requested = True
        if self.process:
            self.process.terminate()
        sys.exit(0)
    
    def _get_progress_from_db(self):
        """Get current progress count from DB"""
        try:
            from core.db.connection import CountryDB
            db = CountryDB("Argentina")
            
            # Get latest run_id
            cur = db.execute(
                "SELECT run_id FROM run_ledger WHERE scraper_name = 'Argentina' "
                "ORDER BY started_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return 0
            
            run_id = row[0]
            
            # Count completed products
            cur = db.execute(
                "SELECT COUNT(*) FROM ar_step_progress "
                "WHERE run_id = %s AND step_number = 3 AND status = 'completed'",
                (run_id,)
            )
            count = cur.fetchone()[0] or 0
            db.close()
            return count
        except Exception as e:
            print(f"[WRAPPER] Warning: Could not read progress from DB: {e}", flush=True)
            return self.last_progress_count  # Return last known
    
    def _check_memory(self, pid):
        """Check memory usage of process"""
        try:
            process = psutil.Process(pid)
            mem_mb = process.memory_info().rss / 1024 / 1024
            return mem_mb
        except Exception:
            return 0
    
    def _is_process_running(self):
        """Check if scraper process is still running"""
        if self.process is None:
            return False
        return self.process.poll() is None
    
    def _start_scraper(self):
        """Start the Argentina scraper"""
        self.restart_count += 1
        print(f"\n{'='*80}", flush=True)
        print(f"[WRAPPER] Starting scraper (attempt #{self.restart_count})", flush=True)
        print(f"[WRAPPER] Max runtime: {self.max_runtime_hours}h", flush=True)
        print(f"[WRAPPER] No-progress timeout: {self.no_progress_timeout}s", flush=True)
        print(f"{'='*80}\n", flush=True)
        
        # Start the scraper
        cmd = [sys.executable, "-u", str(_script_dir / "03_alfabeta_selenium_scraper.py")]
        
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        self.last_progress_time = datetime.now()
        self.last_progress_count = self._get_progress_from_db()
    
    def _monitor_output(self):
        """Read and print output from scraper"""
        try:
            # Read line without blocking
            import select
            if self.process.stdout in select.select([self.process.stdout], [], [], 0)[0]:
                line = self.process.stdout.readline()
                if line:
                    print(line, end='', flush=True)
                    return True
        except Exception:
            pass
        return False
    
    def run(self):
        """Main monitoring loop"""
        print("[WRAPPER] Argentina Scraper Auto-Restart Wrapper", flush=True)
        print(f"[WRAPPER] Started at: {datetime.now().isoformat()}", flush=True)
        
        # Start initial scraper
        self._start_scraper()
        
        while not self.shutdown_requested:
            # Check if max runtime exceeded
            runtime = datetime.now() - self.start_time
            if runtime > timedelta(hours=self.max_runtime_hours):
                print(f"\n[WRAPPER] Max runtime ({self.max_runtime_hours}h) exceeded. Stopping.", flush=True)
                if self.process:
                    self.process.terminate()
                break
            
            # Check if process is still running
            if not self._is_process_running():
                exit_code = self.process.poll()
                print(f"\n[WRAPPER] Scraper exited with code {exit_code}", flush=True)
                
                # Wait a bit before restarting
                print("[WRAPPER] Waiting 10 seconds before restart...", flush=True)
                time.sleep(10)
                
                if self.shutdown_requested:
                    break
                
                # Restart
                self._start_scraper()
                continue
            
            # Monitor output
            self._monitor_output()
            
            # Check progress every CHECK_INTERVAL seconds
            time.sleep(CHECK_INTERVAL)
            
            # Get current progress
            current_progress = self._get_progress_from_db()
            
            # Check if progress was made
            if current_progress > self.last_progress_count:
                self.last_progress_count = current_progress
                self.last_progress_time = datetime.now()
                print(f"[WRAPPER] Progress: {current_progress} products completed", flush=True)
            else:
                # Check if no progress timeout
                no_progress_time = (datetime.now() - self.last_progress_time).total_seconds()
                if no_progress_time > self.no_progress_timeout:
                    print(f"\n[WRAPPER] WARNING: No progress for {no_progress_time:.0f}s. Restarting...", flush=True)
                    
                    # Kill the process
                    try:
                        self.process.terminate()
                        time.sleep(2)
                        if self.process.poll() is None:
                            self.process.kill()
                    except Exception:
                        pass
                    
                    # Wait before restart
                    time.sleep(5)
                    
                    if self.shutdown_requested:
                        break
                    
                    # Restart
                    self._start_scraper()
                    continue
            
            # Check memory usage
            mem_mb = self._check_memory(self.process.pid)
            if mem_mb > self.memory_limit_mb:
                print(f"\n[WRAPPER] WARNING: Memory usage {mem_mb:.0f}MB exceeds limit. Restarting...", flush=True)
                
                # Kill the process
                try:
                    self.process.terminate()
                    time.sleep(2)
                    if self.process.poll() is None:
                        self.process.kill()
                except Exception:
                    pass
                
                # Wait before restart
                time.sleep(5)
                
                if self.shutdown_requested:
                    break
                
                # Restart
                self._start_scraper()
                continue
        
        # Cleanup
        print(f"\n[WRAPPER] Shutting down. Total restarts: {self.restart_count}", flush=True)
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except Exception:
                self.process.kill()


def main():
    parser = argparse.ArgumentParser(description="Argentina Scraper Auto-Restart Wrapper")
    parser.add_argument("--max-runtime-hours", type=int, default=DEFAULT_MAX_RUNTIME_HOURS,
                        help=f"Maximum total runtime in hours (default: {DEFAULT_MAX_RUNTIME_HOURS})")
    parser.add_argument("--no-progress-timeout", type=int, default=DEFAULT_NO_PROGRESS_TIMEOUT,
                        help=f"Restart if no progress for N seconds (default: {DEFAULT_NO_PROGRESS_TIMEOUT})")
    parser.add_argument("--memory-limit-mb", type=int, default=DEFAULT_MEMORY_LIMIT_MB,
                        help=f"Restart if memory exceeds N MB (default: {DEFAULT_MEMORY_LIMIT_MB})")
    
    args = parser.parse_args()
    
    monitor = ScraperMonitor(
        max_runtime_hours=args.max_runtime_hours,
        no_progress_timeout=args.no_progress_timeout,
        memory_limit_mb=args.memory_limit_mb
    )
    
    try:
        monitor.run()
    except KeyboardInterrupt:
        print("\n[WRAPPER] Interrupted by user", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()
