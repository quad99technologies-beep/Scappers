# Argentina 3-Round Selenium Retry - Quick Reference Guide

## Overview

The Argentina scraper uses an intelligent 3-round retry mechanism to maximize data coverage and handle temporary scraping failures gracefully.

## How It Works

```
┌─────────────────────────────────────────────────────────────┐
│                     ROUND 1                                 │
│  Process ALL products (Scraped_By_Selenium=no)             │
│  Typical Success Rate: 70-80%                               │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ├─ Success → Mark as complete (Selenium_Attempt=1, Records>0)
                 │
                 └─ Failure → Mark for retry (Selenium_Attempt=1, Records=0)

                              ⏸ Pause 60 seconds

┌─────────────────────────────────────────────────────────────┐
│                     ROUND 2                                 │
│  Retry ONLY products with Records=0 from Round 1           │
│  Recovers: ~15-20% of Round 1 failures                     │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ├─ Success → Mark as complete (Selenium_Attempt=2, Records>0)
                 │
                 └─ Failure → Mark for retry (Selenium_Attempt=2, Records=0)

                              ⏸ Pause 60 seconds

┌─────────────────────────────────────────────────────────────┐
│                     ROUND 3                                 │
│  Final retry for products with Records=0 from Round 2       │
│  Recovers: ~5-10% of Round 2 failures                      │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ├─ Success → Mark as complete (Selenium_Attempt=3, Records>0)
                 │
                 └─ Failure → Mark for API (Selenium_Attempt=3, Records=0, Source=api)

┌─────────────────────────────────────────────────────────────┐
│                     STEP 4: API SCRAPER                     │
│  Process products marked with Source=api                    │
│  (Only if USE_API_STEPS=True)                              │
└─────────────────────────────────────────────────────────────┘
```

## Common Failure Reasons (Why Retries Help)

| Failure Type | Description | Recovery Round |
|-------------|-------------|----------------|
| **Network Timeout** | Temporary connection issues | Round 2 |
| **Rate Limiting** | Too many requests, temporary block | Round 2 |
| **JavaScript Timing** | Dynamic content loaded too slowly | Round 2/3 |
| **Session Expiry** | Login session expired mid-scrape | Round 2 |
| **Server Load** | Website under heavy load | Round 2/3 |
| **True Missing Data** | Product discontinued/not found | → API |

## Configuration

### Basic Settings

```json
{
  "config": {
    "SELENIUM_ROUNDS": 3,              // Number of retry rounds (1-3)
    "ROUND_PAUSE_SECONDS": 60,         // Pause between rounds
    "USE_API_STEPS": true,             // Enable API fallback
    "SELENIUM_THREADS": 4,             // Concurrent browsers per round
    "MAX_ROWS": 0                      // 0 = unlimited
  }
}
```

### When to Adjust Settings

**Hitting Rate Limits?**
```json
"ROUND_PAUSE_SECONDS": 120,          // Increase pause to 2 minutes
"SELENIUM_THREADS": 2                // Reduce concurrent threads
```

**Need Faster Testing?**
```json
"SELENIUM_ROUNDS": 1,                // Single round for testing
"MAX_ROWS": 100,                     // Limit products
"ROUND_PAUSE_SECONDS": 10            // Shorter pause
```

**Maximum Coverage?**
```json
"SELENIUM_ROUNDS": 3,                // All 3 rounds
"ROUND_PAUSE_SECONDS": 90,           // Longer pause for stability
"USE_API_STEPS": true                // Enable API fallback
```

## Running the Scraper

### Option 1: Full Pipeline (Recommended)
```bash
python scripts/Argentina/run_pipeline_resume.py
```
- Runs all 7 steps including 3-round Selenium scraping
- Automatic checkpoint/resume support
- Progress tracking

### Option 2: Direct Wrapper (Testing)
```bash
python scripts/Argentina/03_selenium_3round_wrapper.py

# With limits for testing
python scripts/Argentina/03_selenium_3round_wrapper.py --max-rows 100
```

### Option 3: GUI
```bash
python scraper_gui.py
# Select "Argentina" → Click "Run Pipeline"
```

## Understanding the Output

### During Execution

```
================================================================================
SELENIUM SCRAPING - ROUND 1 OF 3
================================================================================
[ROUND 1] Processing all products marked for Selenium scraping
[ROUND 1] Products to scrape: 1,234
================================================================================

... (scraping progress) ...

================================================================================
[ROUND 1] COMPLETED
================================================================================
[ROUND 1] Duration: 45m 23s
[ROUND 1] Products processed: 1,234
[ROUND 1] Successfully scraped: 987
[ROUND 1] Still need retry: 247
[ROUND 1] Success rate: 80.0%
================================================================================
```

### Between Rounds

```
================================================================================
[PAUSE] Break before Round 2
================================================================================
[PAUSE] 247 products need retry in Round 2
[PAUSE] Waiting 60 seconds to let system stabilize...
[PAUSE] (Helps avoid rate limiting and gives browser time to rest)
[PAUSE] 50 seconds remaining...
[PAUSE] 40 seconds remaining...
...
[PAUSE] Resuming with Round 2
================================================================================
```

### Final Summary

```
================================================================================
3-ROUND SELENIUM SCRAPING - FINAL SUMMARY
================================================================================

[RESULTS] Products Processed: 1,234
[RESULTS] Total Successful: 1,201 (97.3%)
[RESULTS]   ├─ Succeeded in Round 1: 987
[RESULTS]   ├─ Succeeded in Round 2: 198 (recovered from Round 1 failures)
[RESULTS]   └─ Succeeded in Round 3: 16 (recovered from Round 2 failures)
[RESULTS] Total Failed: 33
[RESULTS]   └─ Marked for API scraping: 33

[IMPACT] Retry Effectiveness:
[IMPACT]   214 products recovered through retries
[IMPACT]   Without retries, success rate would have been 80.0%
[IMPACT]   With retries, success rate is 97.3%
[IMPACT]   Improvement: +17.3 percentage points

================================================================================
[SUCCESS] 3-Round Selenium scraping completed successfully!
================================================================================
```

## Tracking Columns

The wrapper adds/updates these columns in `Productlist_with_urls.csv`:

| Column | Values | Description |
|--------|--------|-------------|
| `Selenium_Attempt` | 0-3 | Current attempt number |
| `Last_Attempt_Records` | 0+ | Records found in last attempt |
| `Selenium_Records` | 0+ | Total records from Selenium |
| `Scraped_By_Selenium` | yes/no | Completion status |
| `Source` | selenium/api | Next scraping method |

## Troubleshooting

### Problem: Round 1 success rate < 50%

**Possible Causes:**
- Invalid credentials (check `ALFABETA_USER`/`ALFABETA_PASS`)
- Website structure changed (check selectors)
- Network issues (check internet connection)

**Solutions:**
1. Run health check: `python scripts/Argentina/health_check.py`
2. Verify credentials in config
3. Check if website is accessible manually

### Problem: Many products failing all 3 rounds

**Possible Causes:**
- Rate limiting too aggressive
- Products genuinely don't exist
- Incorrect URL construction

**Solutions:**
1. Increase `ROUND_PAUSE_SECONDS` to 90-120
2. Reduce `SELENIUM_THREADS` to 2
3. Check `alfabeta_errors.csv` for error patterns
4. Manually verify a few failed products on website

### Problem: Scraper stuck between rounds

**Possible Causes:**
- Browser processes not closing properly
- System resource exhaustion

**Solutions:**
1. Kill orphan Firefox processes: `python scripts/Argentina/cleanup_lock.py`
2. Restart the pipeline
3. Reduce `SELENIUM_THREADS` if memory is low

## Best Practices

### ✅ DO

- Let all 3 rounds complete before analyzing results
- Monitor the final summary statistics
- Check `alfabeta_errors.csv` if success rate is unexpectedly low
- Increase pause time if you see rate limiting errors
- Use `MAX_ROWS` for testing before full runs

### ❌ DON'T

- Don't stop the pipeline mid-round (it can resume, but wastes time)
- Don't set `SELENIUM_ROUNDS=1` for production runs (lose retry benefits)
- Don't set `ROUND_PAUSE_SECONDS` too low (<30s) - causes rate limiting
- Don't manually mark products as scraped - let the automation handle it

## Performance Tips

### For Maximum Speed (Testing)
```json
"SELENIUM_ROUNDS": 1,
"MAX_ROWS": 100,
"ROUND_PAUSE_SECONDS": 10,
"SELENIUM_THREADS": 8
```

### For Maximum Coverage (Production)
```json
"SELENIUM_ROUNDS": 3,
"MAX_ROWS": 0,
"ROUND_PAUSE_SECONDS": 90,
"SELENIUM_THREADS": 4,
"USE_API_STEPS": true
```

### For Rate-Limited Websites
```json
"SELENIUM_ROUNDS": 3,
"ROUND_PAUSE_SECONDS": 120,
"SELENIUM_THREADS": 2,
"RATE_LIMIT_SECONDS": 15
```

## Integration with Pipeline

The 3-round wrapper is **automatically used** when you run the full pipeline:

```
Step 0: Backup and Clean
Step 1: Get Product List
Step 2: Prepare URLs
Step 3: 3-Round Selenium Scraping ← YOU ARE HERE
        ├─ Round 1: All products
        ├─ Round 2: Retry failures
        └─ Round 3: Final retry
Step 4: API Scraping (if enabled)
Step 5: Translate to English
Step 6: Generate Final Output
```

## Support

For issues or questions:
1. Check `alfabeta_errors.csv` for error details
2. Run health check for diagnostic info
3. Review this guide for configuration tweaks
4. Check main README.md for detailed documentation
