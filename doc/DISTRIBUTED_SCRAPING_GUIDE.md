# Distributed Scraping - Quick Start Guide

## Overview

The scraper system now supports **hybrid execution**:
- **Single Mode**: Current local execution (default)
- **Distributed Mode**: Queue-based horizontal scaling

## Per-Scraper Configuration

In `scripts/common/scraper_registry.py`:

```python
"India": {
    ...
    "execution_mode": "distributed",  # Enable distributed mode
}
```

**Default**: `"single"` (if not specified)

## How It Works

### Single Mode (Default)
- Runs locally with existing `run_pipeline_resume.py`
- Uses file-based checkpoints and locks
- One process per run

### Distributed Mode
- Creates shared run_id
- Enqueues URLs to Postgres work queue
- Workers on any node claim batches atomically
- Each worker runs its own Tor/browser
- Scales horizontally by adding more workers

## Starting a Distributed Scraper

### 1. Enqueue Work (Orchestrator)

```bash
# Create run and enqueue URLs
python core/scraper_orchestrator.py India --urls-file urls.txt
```

Output:
```json
{
  "status": "queued",
  "mode": "distributed",
  "run_id": "20260215_120000",
  "enqueued": 1000000,
  "worker_command": "python core/url_worker.py --scraper India --run-id 20260215_120000..."
}
```

### 2. Start Workers (On Each Node)

```bash
# Node 1
python core/url_worker.py \
  --scraper India \
  --run-id 20260215_120000 \
  --batch-size 50 \
  --db-host localhost \
  --db-name scraper_db \
  --db-user postgres

# Node 2 (same command, different machine)
python core/url_worker.py \
  --scraper India \
  --run-id 20260215_120000 \
  --batch-size 50 \
  --db-host 192.168.1.100 \
  --db-name scraper_db \
  --db-user postgres

# Node 3, 4, 5... (scale as needed)
```

### 3. Monitor Progress

```python
from core.scraper_orchestrator import ScraperOrchestrator

orch = ScraperOrchestrator()
stats = orch.get_stats("India", "20260215_120000")
print(stats)
# {
#   "pending": 950000,
#   "claimed": 500,
#   "completed": 49000,
#   "failed": 500,
#   "remaining": 950500
# }
```

## Worker Architecture

### Each Worker Node Runs:
1. **Own Tor daemon** (local SOCKS proxy)
2. **Own browser instances** (isolated profiles)
3. **URL claiming loop**:
   - Claims batch (10-100 URLs) atomically
   - Processes each URL
   - Marks as completed/failed
   - Claims next batch

### Shared Across All Workers:
- **run_id**: Single identifier for the entire run
- **Database**: PostgreSQL work queue
- **Results**: Stored with run_id + url_hash

## Atomic URL Claiming

Uses PostgreSQL `FOR UPDATE SKIP LOCKED`:

```sql
-- Worker 1 claims batch
SELECT * FROM url_work_queue
WHERE run_id = '20260215_120000'
  AND status = 'pending'
LIMIT 10
FOR UPDATE SKIP LOCKED;  -- Other workers skip these rows

-- Worker 2 claims different batch (no collision)
SELECT * FROM url_work_queue
WHERE run_id = '20260215_120000'
  AND status = 'pending'
LIMIT 10
FOR UPDATE SKIP LOCKED;  -- Gets next 10 URLs automatically
```

## Auto-Scaling

### Scale Out (Add Workers)
```bash
# Just start more workers on any node
for i in {1..10}; do
  python core/url_worker.py --scraper India --run-id 20260215_120000 &
done
```

### Scale Down (Remove Workers)
- Workers finish current batch and exit
- Uncompleted URLs automatically return to queue after lease expiry

## Failure Handling

### Worker Dies
- Claimed URLs released after 300s (configurable)
- Automatically retried by other workers
- Max 3 retries per URL (configurable)

### Partial Failures
```python
# Check failed URLs
SELECT url, error_message, retry_count
FROM url_work_queue
WHERE run_id = '20260215_120000'
  AND status = 'failed';
```

## Docker Deployment

### Orchestrator (Start once)
```bash
docker run scraper:v1 \
  python core/scraper_orchestrator.py India \
  --urls-file /data/urls.txt
```

### Workers (Scale horizontally)
```bash
# Start 10 workers
docker-compose up --scale worker=10
```

### docker-compose.yml
```yaml
services:
  worker:
    build: .
    command: python core/url_worker.py --scraper India --run-id ${RUN_ID}
    depends_on:
      - postgres
      - tor
    environment:
      DB_HOST: postgres
      TOR_SOCKS_PORT: 9050
    deploy:
      replicas: 10  # Horizontal scaling
```

## Migration Path

### Phase 1: Test with India (Already Configured)
```bash
# India is already marked as distributed
python core/scraper_orchestrator.py India --urls-file india_urls.txt
```

### Phase 2: Convert Other Scrapers As Needed
```python
# In scraper_registry.py
"Malaysia": {
    ...
    "execution_mode": "distributed",  # Add this line
}
```

### Phase 3: Hybrid Operation
- **Malaysia**: Distributed (1M URLs)
- **Russia**: Single Distributed (complex logic)
- **Argentina**: Single (multi-threaded architecture)

## Performance Example

### Single Mode
- 1 node, 1 process
- ~10 URLs/min
- **1M URLs = ~70 days**

### Distributed Mode
- 10 nodes × 5 workers = 50 workers
- ~10 URLs/min per worker
- **1M URLs = ~33 hours**

## CLI Reference

### Orchestrator
```bash
python core/scraper_orchestrator.py <scraper> [--urls-file FILE] [--resume] [--fresh]
```

### Worker
```bash
python core/url_worker.py \
  --scraper <name> \
  --run-id <id> \
  --batch-size 10 \
  --db-host localhost \
  --db-port 5432 \
  --db-name scraper_db \
  --db-user postgres \
  --db-password <password>
```

### Stats
```bash
python -c "
from core.scraper_orchestrator import ScraperOrchestrator
orch = ScraperOrchestrator()
print(orch.get_stats('India', '20260215_120000'))
"
```

## Database Schema

```sql
CREATE TABLE url_work_queue (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100),
    scraper_name VARCHAR(100),
    url TEXT,
    url_hash VARCHAR(64),  -- SHA256 for deduplication
    priority INT DEFAULT 0,
    status VARCHAR(20),     -- pending/claimed/completed/failed
    worker_id VARCHAR(100),
    claimed_at TIMESTAMP,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    UNIQUE(run_id, url_hash)
);

CREATE INDEX idx_work_queue_status 
    ON url_work_queue(run_id, scraper_name, status, priority DESC);
```

## Monitoring & Debugging

### Check Active Workers
```sql
SELECT worker_id, COUNT(*) as claimed_urls
FROM url_work_queue
WHERE status = 'claimed'
GROUP BY worker_id;
```

### Check Queue Depth
```sql
SELECT status, COUNT(*)
FROM url_work_queue
WHERE run_id = '20260215_120000'
GROUP BY status;
```

### Release Stuck URLs
```python
from core.url_work_queue import URLWorkQueue

queue = URLWorkQueue(db_config)
queue.release_expired_leases(lease_seconds=300)
```

---

**Status**: Implemented ✓  
**Tested**: Ready for testing  
**Next**: Try with India scraper
