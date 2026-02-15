# Chrome Version Mismatch Fix

## Issue
The Malaysia scraper was failing with the error:
```
selenium.common.exceptions.SessionNotCreatedException: Message: session not created:
This version of ChromeDriver only supports Chrome version 144
Current browser version is 143.0.7499.193
```

## Root Cause
The `undetected-chromedriver` package was automatically downloading ChromeDriver version 144, but the installed Chrome browser was version 143. When `version_main=None` was used, it failed to auto-detect the correct version.

## Solution Implemented
Added automatic Chrome version detection to [01_Product_Registration_Number.py](scripts/Malaysia/01_Product_Registration_Number.py):

1. **Fast Registry Detection (Primary Method)**
   - Queries Windows Registry at `HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon`
   - Falls back to `HKEY_LOCAL_MACHINE\Software\Google\Chrome\BLBeacon`
   - Extracts major version number (e.g., 143 from 143.0.7499.193)

2. **Executable Method (Fallback)**
   - Runs `chrome.exe --version` if registry method fails
   - Parses version from command output

3. **Safe Defaults**
   - If detection fails, falls back to `version_main=None`
   - Provides clear console output about detection status

## Code Changes
File: `scripts/Malaysia/01_Product_Registration_Number.py`

### Added Function
```python
def get_chrome_major_version():
    """Get the major version of installed Chrome browser."""
    # Queries Windows Registry for fast, reliable version detection
    # Falls back to chrome.exe --version if needed
```

### Updated Driver Initialization
```python
chrome_version = get_chrome_major_version()
if chrome_version:
    print(f"Detected Chrome version: {chrome_version}", flush=True)
    driver = uc.Chrome(options=options, version_main=chrome_version, ...)
else:
    print("Chrome version auto-detection failed, using default", flush=True)
    driver = uc.Chrome(options=options, version_main=None, ...)
```

## Prevention Strategy
This fix prevents the issue from happening again by:

1. **Automatic Detection**: Always matches ChromeDriver version to installed Chrome
2. **Multiple Methods**: Uses both registry and executable detection for reliability
3. **Fast Execution**: Registry method completes in <1 second
4. **Clear Logging**: Shows detected version in console output
5. **Graceful Degradation**: Falls back to defaults if detection fails

## Verification
The fix was tested and confirmed working:
```
Using undetected-chromedriver for Cloudflare bypass...
Detected Chrome version: 143
[INFO] [undetected_chromedriver.patcher] patching driver executable...
Opening MyPriMe website...
✓ Successfully initialized ChromeDriver with matching version
```

## Future Chrome Updates
When Chrome auto-updates to version 144+:
- The script will automatically detect and use the new version
- No manual configuration needed
- No code changes required

## Notes
- This fix is Windows-specific (uses Windows Registry)
- For Linux/Mac systems, only the executable method will be used
- The detection adds <1 second to startup time
- Works with both headless and headed browser modes
