# Argentina Round-Robin Retry Mode

## Overview

The Argentina scraper now supports a **round-robin retry mode** that changes how failed products are retried. Instead of immediately retrying the same product up to 5 times, the scraper tries each product once, then loops back to retry failed products in subsequent rounds.

## Behavior Comparison

### Original Mode (Immediate Retry)
```
P1: Try 1 → Fail → Try 2 → Fail → Try 3 → Fail → Try 4 → Fail → Try 5 → Fail → Next Product
P2: Try 1 → Success → Next Product
```

### Round-Robin Mode
```
Loop 1: P1(1) → P2(1) → P3(1) → ... → P100(1)
Loop 2: P1(2) → P3(2) → P7(2) → ... (only failed products)
Loop 3: P1(3) → P7(3) → ... (still failing)
...
Loop 5: Final attempt → Move to API if still failing
```

## Configuration

Add to your `.env` file:

```bash
# Enable round-robin retry mode
SELENIUM_ROUND_ROBIN_RETRY=true

# Maximum attempts per product across all loops (default: 5)
SELENIUM_MAX_ATTEMPTS_PER_PRODUCT=5
```

## Benefits

1. **Better IP Rotation Distribution**: Each product gets a fresh IP on retry (if using VPN/TOR rotation)
2. **Reduced Hammering**: Won't repeatedly hit the same product with the same IP
3. **Fairness**: All products get equal chance before any product gets retried
4. **Resume Support**: Attempt counts are tracked in memory; DB loop_count tracks overall progress

## How It Works

1. **Attempt Tracking**: Each product has an attempt counter stored in `_product_attempt_counts`
2. **Single Attempt Per Loop**: In round-robin mode, each product gets only 1 attempt per loop
3. **Requeue on Failure**: Failed products are requeued to the end of the queue
4. **Max Attempts Check**: Before processing, the scraper checks if max attempts reached
5. **Move to API**: After max attempts, product is moved to API queue (if API steps enabled)

## Log Messages

Look for these messages in logs:

```
[ROUND_ROBIN] Attempt 1/5 for Company | Product
[ROUND_ROBIN] Requeueing Company | Product for attempt 2/5
[ROUND_ROBIN] Max attempts (5) reached for Company | Product
```

## Compatibility

- Works with `SELENIUM_SINGLE_ATTEMPT=false` (normal mode)
- Can be combined with VPN rotation for best results
- Auto-restart wrapper continues to work normally
- DB resume support works normally

## When to Use

**Use Round-Robin when:**
- Using VPN with IP rotation
- Site blocks/throttles based on IP reputation
- Want to maximize success rate across all products

**Use Original Mode when:**
- Using TOR (slower, may want immediate retries)
- Testing small batches
- Site errors are transient (not IP-based)

## Migration from Original Mode

1. Set `SELENIUM_ROUND_ROBIN_RETRY=true` in `.env`
2. Set `SELENIUM_MAX_ATTEMPTS_PER_PRODUCT=5` (or desired value)
3. Ensure VPN rotation is configured for best results
4. Run scraper normally
