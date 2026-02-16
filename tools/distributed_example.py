"""
Example: Using the Distributed Scraping System

This shows how to use the new distributed architecture.
Run from repo root: python tools/distributed_example.py <command>
"""

import sys
from pathlib import Path

# Ensure repo root is in path when run from tools/
_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parent if _script_dir.name == "tools" else _script_dir
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.pipeline.scraper_orchestrator import ScraperOrchestrator
from core.pipeline.url_work_queue import URLWorkQueue

# ============================================================================
# Example 1: Start a Distributed Scraper
# ============================================================================

def example_distributed_scraper():
    """Start India scraper in distributed mode with 1M URLs"""
    
    # Load URLs from file or generate them
    urls = []
    with open("india_formulations.txt", "r") as f:
        urls = [line.strip() for line in f if line.strip()]
    
    print(f"Loaded {len(urls)} URLs")
    
    # Create orchestrator
    orch = ScraperOrchestrator()
    
    # Start scraper (automatically routes to distributed mode for India)
    result = orch.start_scraper("India", urls=urls)
    
    print(f"Status: {result['status']}")
    print(f"Mode: {result['mode']}")
    print(f"Run ID: {result['run_id']}")
    print(f"Enqueued: {result['enqueued']} URLs")
    
    # Print worker command
    print("\nTo start workers on each node, run:")
    print(result['worker_command'])
    
    return result['run_id']


# ============================================================================
# Example 2: Monitor Progress
# ============================================================================

def monitor_distributed_run(scraper_name: str, run_id: str):
    """Monitor progress of a distributed run"""
    import time
    
    orch = ScraperOrchestrator()
    
    while True:
        stats = orch.get_stats(scraper_name, run_id)
        
        if stats['status'] == 'success':
            s = stats['stats']
            
            print(f"\n{scraper_name} Run {run_id}:")
            print(f"  Pending:   {s['pending']:,}")
            print(f"  Claimed:   {s['claimed']:,}")
            print(f"  Completed: {s['completed']:,}")
            print(f"  Failed:    {s['failed']:,}")
            print(f"  ---")
            print(f"  Total:     {s['total']:,}")
            print(f"  Remaining: {s['remaining']:,}")
            
            # Calculate progress
            if s['total'] > 0:
                progress = (s['completed'] / s['total']) * 100
                print(f"  Progress:  {progress:.2f}%")
            
            # Check if done
            if s['remaining'] == 0:
                print("\n✓ Run complete!")
                break
        
        time.sleep(10)  # Check every 10 seconds


# ============================================================================
# Example 3: Start Single-Mode Scraper (Existing Behavior)
# ============================================================================

def example_single_scraper():
    """Start Russia scraper in single mode (unchanged behavior)"""
    
    orch = ScraperOrchestrator()
    
    # Russia is not distributed, so it runs in single mode automatically
    result = orch.start_scraper("Russia", resume=True)
    
    print(f"Status: {result['status']}")
    print(f"Mode: {result['mode']}")  # Will be 'single'
    print(f"Run ID: {result['run_id']}")
    print(f"PID: {result.get('pid', 'N/A')}")


# ============================================================================
# Example 4: Convert Existing Scraper to Distributed
# ============================================================================

def convert_malaysia_to_distributed():
    """
    To convert Malaysia to distributed mode:
    
    1. Update scraper_registry.py:
       "Malaysia": {
           ...
           "execution_mode": "distributed",  # Add this line
       }
    
    2. Start with URLs:
    """
    
    # Example URLs for Malaysia
    urls = [
        "https://example.com/product/1",
        "https://example.com/product/2",
        # ... more URLs
    ]
    
    orch = ScraperOrchestrator()
    result = orch.start_scraper("Malaysia", urls=urls)
    
    print(f"Malaysia now running in {result['mode']} mode")
    print(f"Run ID: {result['run_id']}")
    print(f"Enqueued: {result['enqueued']} URLs")


# ============================================================================
# Example 5: Direct Queue Operations
# ============================================================================

def example_direct_queue():
    """Direct queue operations for advanced usage"""
    
    from core.pipeline.url_work_queue import URLWorkQueue
    
    # Connect to queue
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'scraper_db',
        'user': 'postgres',
        'password': ''
    }
    
    queue = URLWorkQueue(db_config)
    
    # Enqueue URLs
    urls = ["http://example.com/1", "http://example.com/2"]
    enqueued = queue.enqueue_urls(
        run_id="20260215_120000",
        scraper_name="India",
        urls=urls,
        priority=0
    )
    print(f"Enqueued {enqueued} URLs")
    
    # Claim a batch (simulating a worker)
    batch = queue.claim_batch(
        worker_id="test_worker_1",
        scraper_name="India",
        run_id="20260215_120000",
        batch_size=10
    )
    print(f"Claimed {len(batch)} URLs")
    
    # Mark first URL as completed
    if batch:
        queue.complete_url(batch[0]['id'], success=True)
        print(f"Completed URL: {batch[0]['url']}")
    
    # Get stats
    stats = queue.get_queue_stats("20260215_120000", "India")
    print(f"Queue stats: {stats}")
    
    # Release expired leases
    released = queue.release_expired_leases(lease_seconds=300)
    print(f"Released {released} expired leases")


# ============================================================================
# Example 6: CLI Usage
# ============================================================================

def cli_examples():
    """
    Command-line examples:
    
    # Start distributed scraper
    python core/scraper_orchestrator.py India --urls-file india_urls.txt
    
    # Start worker on node 1 (DB/Tor from config/platform.env)
    python core/utils/url_worker.py --scraper India --run-id 20260215_120000 --batch-size 50
    
    # Start worker on node 2 (same command, different machine)
    python core/utils/url_worker.py --scraper India --run-id 20260215_120000 --batch-size 50
    
    # Start single-mode scraper
    python core/scraper_orchestrator.py Russia --resume
    
    # Get stats
    python -c "from core.scraper_orchestrator import ScraperOrchestrator; \
               orch = ScraperOrchestrator(); \
               print(orch.get_stats('India', '20260215_120000'))"
    """
    pass


# ============================================================================
# Example 7: Docker Deployment
# ============================================================================

def docker_deployment_example():
    """
    Docker deployment steps:
    
    1. Build image:
       docker build -t scraper:v1 .
    
    2. Start orchestrator (once):
       docker run scraper:v1 \
         python core/scraper_orchestrator.py India --urls-file /data/urls.txt
    
    3. Scale workers:
       docker-compose up --scale worker=10
    
    4. Scale up dynamically:
       docker-compose up --scale worker=50
    
    5. Monitor:
       docker-compose logs -f worker
    """
    pass


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python tools/distributed_example.py start-india")
        print("  python tools/distributed_example.py monitor <run_id>")
        print("  python tools/distributed_example.py start-russia")
        print("  python tools/distributed_example.py queue-ops")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "start-india":
        run_id = example_distributed_scraper()
        print(f"\nNow monitor with: python tools/distributed_example.py monitor {run_id}")
    
    elif command == "monitor":
        if len(sys.argv) < 3:
            print("Usage: python tools/distributed_example.py monitor <run_id>")
            sys.exit(1)
        monitor_distributed_run("India", sys.argv[2])
    
    elif command == "start-russia":
        example_single_scraper()
    
    elif command == "queue-ops":
        example_direct_queue()
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
