import os
import signal
import sys
import atexit
import logging
from typing import Callable, Optional

log = logging.getLogger(__name__)

def register_shutdown_handler(
    cleanup_func: Optional[Callable] = None,
    save_state_func: Optional[Callable] = None
):
    """
    Register graceful shutdown handler for SIGINT and SIGTERM.
    Optionally provide cleanup function (like closing browsers)
    and save_state function (like saving progress to DB).
    """
    def signal_handler(signum, frame):
        log.info(f"\n[SIGNAL] Received signal {signum}, initiating graceful shutdown...")
        
        if save_state_func:
            try:
                save_state_func()
                log.info("[SIGNAL] State saved successfully")
            except Exception as e:
                log.warning(f"[SIGNAL] Failed to save state: {e}")
        
        if cleanup_func:
            try:
                cleanup_func()
                log.info("[SIGNAL] Cleanup completed")
            except Exception as e:
                log.warning(f"[SIGNAL] Cleanup failed: {e}")
                
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if cleanup_func:
        atexit.register(cleanup_func)
