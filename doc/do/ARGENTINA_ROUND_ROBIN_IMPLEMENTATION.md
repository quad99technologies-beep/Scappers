# Argentina Round-Robin Retry Implementation Plan

## Current Behavior (Immediate Retry)
```
P1: Try 1 → Fail → Try 2 → Fail → Try 3 → Fail → Try 4 → Fail → Try 5 → Fail → Next Product
P2: Try 1 → Success → Next Product
```

## Desired Behavior (Round-Robin Retry)
```
Loop 1: P1(1) → P2(1) → P3(1) → ... → P100(1)
Loop 2: P1(2) → P3(2) → P7(2) → ... (only failed products)
Loop 3: P1(3) → P7(3) → ... (still failing)
...
Loop 5: Final attempt
```

## Implementation Changes Needed

### 1. Config Changes (config_loader.py)
```python
SELENIUM_ROUND_ROBIN_RETRY = True  # New option
SELENIUM_MAX_ATTEMPTS_PER_PRODUCT = 5  # Max attempts across all loops
```

### 2. Track Attempt Count (03_alfabeta_selenium_worker.py)
Add global dictionary to track attempts per product:
```python
_product_attempt_counts = {}  # (company, product) -> attempt_count
_attempt_counts_lock = threading.Lock()
```

### 3. Modify Retry Logic
Instead of immediate retry loop, requeue to end:

```python
# OLD: Immediate retry
for attempt in range(5):
    if try_scrape(product):
        break
    sleep(10)

# NEW: Round-robin retry
success = try_scrape(product)
if not success:
    attempt_count = get_attempt_count(product)
    if attempt_count < MAX_ATTEMPTS:
        increment_attempt_count(product)
        selenium_queue.put(item)  # Requeue to end
    else:
        mark_api_pending(product)  # Max attempts reached
```

### 4. Key Code Locations to Modify

#### Location A: Line ~3319 - Main retry loop
```python
# Change from:
while retry_count <= max_retries and not success:
    # ... immediate retry logic

# To:
if SELENIUM_ROUND_ROBIN_RETRY:
    # Single attempt, then requeue if failed
    success = try_once()
    if not success and can_retry(product):
        requeue(product)
else:
    # Original immediate retry logic
    while retry_count <= max_retries and not success:
        # ...
```

#### Location B: Line ~3349 - Inner retry loop
Same pattern - single attempt in round-robin mode.

#### Location C: Line ~3026, 3218, 3938, 4072, 4162, 4236 - Requeue points
All requeue points should respect round-robin mode and attempt counts.

## Simplified Implementation

Since the code is complex, here's a minimal change approach:

### Step 1: Add attempt tracking
```python
# Near line 285-290 (after other globals)
_product_attempt_counts = {}
_attempt_counts_lock = threading.Lock()

def get_product_attempt_count(company, product):
    key = (nk(company), nk(product))
    with _attempt_counts_lock:
        return _product_attempt_counts.get(key, 0)

def increment_product_attempt_count(company, product):
    key = (nk(company), nk(product))
    with _attempt_counts_lock:
        _product_attempt_counts[key] = _product_attempt_counts.get(key, 0) + 1
        return _product_attempt_counts[key]
```

### Step 2: Modify main processing loop

Around line 3315-3320, change the retry logic:

```python
# OLD:
max_retries = 0 if SELENIUM_SINGLE_ATTEMPT else MAX_RETRIES_TIMEOUT
retry_count = 0
success = False

while retry_count <= max_retries and not success:
    # ... retry logic

# NEW:
if SELENIUM_ROUND_ROBIN_RETRY:
    # Check if we've exceeded max attempts for this product
    current_attempts = get_product_attempt_count(in_company, in_product)
    if current_attempts >= SELENIUM_MAX_ATTEMPTS_PER_PRODUCT:
        log.warning(f"[ROUND_ROBIN] Max attempts ({SELENIUM_MAX_ATTEMPTS_PER_PRODUCT}) reached for {in_company} | {in_product}, moving to API")
        mark_api_pending(in_company, in_product)
        continue
    
    # Single attempt mode
    max_retries = 0  # No immediate retries
else:
    # Original behavior
    max_retries = 0 if SELENIUM_SINGLE_ATTEMPT else MAX_RETRIES_TIMEOUT

retry_count = 0
success = False

while retry_count <= max_retries and not success:
    # ... existing retry logic
```

### Step 3: On failure, requeue instead of immediate retry

Around line 3910-3940 (where requeue happens):

```python
# After failed attempt:
if not success:
    if SELENIUM_ROUND_ROBIN_RETRY:
        attempts = increment_product_attempt_count(in_company, in_product)
        if attempts < SELENIUM_MAX_ATTEMPTS_PER_PRODUCT:
            log.info(f"[ROUND_ROBIN] Requeueing {in_company} | {in_product} for loop {attempts + 1}")
            selenium_queue.put(item)  # Requeue to end
        else:
            log.warning(f"[ROUND_ROBIN] Max attempts reached for {in_company} | {in_product}, moving to API")
            mark_api_pending(in_company, in_product)
    else:
        # Original requeue logic
        ...
```

## Testing

1. Set `SELENIUM_ROUND_ROBIN_RETRY=true` in config
2. Set `SELENIUM_MAX_ATTEMPTS_PER_PRODUCT=5`
3. Run scraper with small batch (10 products)
4. Verify: Each product gets 1 attempt, then loop restarts with failed products

## Migration Path

1. Default: `SELENIUM_ROUND_ROBIN_RETRY=false` (existing behavior)
2. Opt-in: Set to `true` for round-robin mode
3. Monitor logs for "[ROUND_ROBIN]" messages
