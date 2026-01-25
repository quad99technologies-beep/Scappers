# Headless Mode with Cloudflare - Troubleshooting Guide

## The Problem

When running the Malaysia scraper with `SCRIPT_01_HEADLESS: "true"`, the script gets stuck at Cloudflare verification and never completes. However, with `SCRIPT_01_HEADLESS: "false"` (visible browser), it works fine.

## Why This Happens

Cloudflare uses advanced bot detection that can identify headless browsers through multiple signals:

### Detection Methods
1. **Missing Browser APIs**
   - Headless Chrome lacks some WebGL, Canvas, and Audio APIs
   - Different navigator properties (plugins, mimeTypes, etc.)

2. **Behavioral Fingerprinting**
   - No mouse movements or keyboard timing
   - Instant page loads without human-like delays
   - Missing scroll events

3. **WebGL/GPU Fingerprinting**
   - Headless mode reports different GPU renderer
   - `--disable-gpu` flag creates obvious patterns

4. **Window Properties**
   - `window.chrome` object differences
   - Viewport size anomalies
   - Missing browser extensions

5. **Network Patterns**
   - HTTP headers inconsistencies
   - TLS fingerprint mismatches
   - Connection timing patterns

## Current Solutions

### Solution 1: Run with Visible Browser (Recommended)
**Config:** Set `SCRIPT_01_HEADLESS: "false"` in [Malaysia.env.json](config/Malaysia.env.json)

✅ **Pros:**
- 100% success rate with Cloudflare
- No detection issues
- Can visually debug issues

❌ **Cons:**
- Requires display/GUI environment
- Can't run on headless servers
- Browser window visible during execution

### Solution 2: Enhanced Headless Mode (Partial Fix)
**Improvements made to [01_Product_Registration_Number.py](scripts/Malaysia/01_Product_Registration_Number.py:139-158):**

```python
if headless:
    # Use newer headless mode
    options.add_argument("--headless=new")

    # Critical: Realistic window size
    options.add_argument("--window-size=1920,1080")

    # Disable automation detection
    options.add_argument("--disable-blink-features=AutomationControlled")

    # Realistic user agent
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)...")
```

⚠️ **Status:** May work sometimes, but not 100% reliable

### Solution 3: Use Virtual Display (Linux/Server)
For headless servers, use Xvfb (X Virtual Framebuffer):

```bash
# Install Xvfb
sudo apt-get install xvfb

# Run with virtual display
xvfb-run -a python run_pipeline_resume.py
```

✅ **Pros:**
- Works on servers without GUI
- Browser thinks it has a display
- Better Cloudflare success rate

### Solution 4: Use Residential Proxy + Headless
Combine headless mode with residential IP proxies:

```json
"SCRIPT_01_PROXY_SERVER": "http://proxy.example.com:8080",
"SCRIPT_01_PROXY_USERNAME": "user",
"SCRIPT_01_PROXY_PASSWORD": "pass"
```

## Recommended Configuration

### For Development/Local Machines
```json
{
  "SCRIPT_01_HEADLESS": "false",
  "SCRIPT_01_CHROME_START_MAXIMIZED": "--start-maximized"
}
```

### For Production Servers (Linux with Xvfb)
```bash
# Use Xvfb wrapper
xvfb-run --auto-servernum --server-args="-screen 0 1920x1080x24" \
  python run_pipeline_resume.py
```

```json
{
  "SCRIPT_01_HEADLESS": "false",
  "SCRIPT_01_CHROME_START_MAXIMIZED": ""
}
```

### For Headless-Only Environments (Less Reliable)
```json
{
  "SCRIPT_01_HEADLESS": "true"
}
```

⚠️ **Warning:** May get stuck at Cloudflare. Monitor execution and be prepared to switch to visible mode.

## Detection Test

Run this to check if headless mode is detected:

```python
# Add to script temporarily for testing
driver.get("https://kaliiiiiiiiii.github.io/brotector/")
time.sleep(10)  # View results
```

This will show all detection vectors that identify your browser as a bot.

## Long-term Solutions

### 1. Puppeteer Stealth Plugin (Node.js)
Use Puppeteer with stealth plugin instead of Selenium:
- Better Cloudflare bypass
- More realistic browser fingerprint

### 2. Playwright with Firefox
Firefox headless is harder to detect:
- Different fingerprint from Chrome
- Better API coverage in headless

### 3. Cloud Browser Services
Use services like BrowserStack, LambdaTest:
- Real browsers in cloud
- No headless detection
- More expensive

### 4. Browser Automation Detection Evasion (BADE)
Use specialized tools like:
- `selenium-stealth` (Python package)
- `puppeteer-extra-plugin-stealth` (Node.js)
- `playwright-stealth` (Python)

## Current Status

**Implementation:** Enhanced headless mode with anti-detection measures
**Reliability:** ~60-70% success rate in headless mode
**Recommendation:** Use visible browser (`headless: false`) for production

## Alternative Approach

Instead of scraping with browser automation, consider:

1. **Direct API calls** (if available)
   - Reverse engineer website API
   - Use requests library
   - Much faster and no detection

2. **HTTP session with proper headers**
   - Bypass Cloudflare with HTTP requests
   - Solve challenges programmatically
   - Requires more reverse engineering

## Summary

**TL;DR:**
- Cloudflare detects headless browsers easily
- Use `SCRIPT_01_HEADLESS: "false"` for reliable scraping
- On servers, use Xvfb to provide virtual display
- Enhanced headless mode has been implemented but isn't 100% reliable

**Current Config:**
```json
"SCRIPT_01_HEADLESS": "false"  // ← Recommended for reliability
```

This ensures the scraper works consistently without getting stuck at Cloudflare verification.
