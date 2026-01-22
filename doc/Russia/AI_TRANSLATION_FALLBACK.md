# Russia AI Translation Fallback

## Overview

The Russia pipeline now supports **AI-powered translation fallback** for terms missing from the Dictionary.csv file. When a Russian word is not found in the dictionary, the system automatically attempts to translate it using AI engines.

## How It Works

### Translation Flow

```
Input: Russian text
    ↓
1. Check Dictionary.csv
    ├─ Found → Use dictionary translation
    └─ Not Found ↓
2. Check if already in English
    ├─ Yes → Keep original
    └─ No (Cyrillic detected) ↓
3. AI Translation Fallback (if enabled)
    ├─ Check AI cache
    │   ├─ Found → Use cached translation
    │   └─ Not Found ↓
    ├─ Translate with AI engine
    │   ├─ Argos Translate (offline, priority 1)
    │   └─ Google Translate (online, fallback)
    ├─ Cache result
    └─ Return translation
4. Mark as missing (if AI disabled/failed)
```

### Translation Engines

**Priority 1: Argos Translate (Offline)**
- Runs completely offline
- No API costs or rate limits
- Automatically downloads Russian→English model on first use
- Faster for bulk translations

**Priority 2: Google Translate (Online Fallback)**
- Used if Argos Translate is unavailable
- Requires internet connection
- May have rate limits for large volumes

### Caching

All AI translations are cached in:
```
cache/russia_ai_translation_cache.json
```

Benefits:
- Prevents re-translating the same terms
- Persists between runs
- Significantly speeds up subsequent runs
- Reduces API calls for online services

## Usage

### Option 1: Via Pipeline (Recommended)

The AI fallback is **enabled by default** in the pipeline:

```bash
python scripts/Russia/run_pipeline_resume.py
```

It automatically enables AI fallback for:
- Step 4: Generate Pricing Data Template
- Step 5: Generate Discontinued List Template

### Option 2: Manual Script Execution

Enable AI fallback with the `--enable-ai-fallback` flag:

```bash
# Pricing data with AI fallback
python scripts/Russia/04_generate_pricing_data.py --enable-ai-fallback

# Discontinued list with AI fallback
python scripts/Russia/05_generate_discontinued_list.py --enable-ai-fallback
```

### Option 3: Disable AI Fallback

To use **only** dictionary-based translation (no AI):

1. Edit `run_pipeline_resume.py`
2. Remove `["--enable-ai-fallback"]` from steps 4 and 5
3. Run the pipeline

Or run scripts manually without the flag:
```bash
python scripts/Russia/04_generate_pricing_data.py
```

## Testing

Test the AI fallback functionality:

```bash
python scripts/Russia/test_ai_fallback.py
```

This script:
- Enables AI fallback
- Loads the dictionary
- Tests translation of sample Russian words
- Shows which words use dictionary vs AI
- Reports any remaining untranslated words

## Installation Requirements

### For Argos Translate (Offline)

```bash
pip install argostranslate
```

The Russian→English model will be downloaded automatically on first use.

### For Google Translate (Online Fallback)

```bash
pip install deep-translator
```

Already included in most Python environments.

## Configuration

### Default Cache Location

```
<repo_root>/cache/russia_ai_translation_cache.json
```

### Custom Cache Location

If using the API directly:

```python
from translation_utils import enable_ai_fallback

# Use custom cache path
enable_ai_fallback(cache_path=Path("/custom/path/cache.json"))
```

## Performance

### Translation Speed

- **Dictionary lookup**: Instant (~0.001s per word)
- **AI translation (cached)**: Instant (~0.001s per word)
- **AI translation (Argos, uncached)**: ~0.1-0.5s per word
- **AI translation (Google, uncached)**: ~0.2-1s per word (network dependent)

### Recommendations

1. **First run**: May be slower as AI cache is built
2. **Subsequent runs**: Much faster due to caching
3. **Large datasets**: Argos Translate is faster than Google
4. **Add to Dictionary.csv**: For frequently used terms, add them to the dictionary for instant translation

## Monitoring

### Console Output

When AI fallback is enabled, you'll see:

```
[INFO] AI translation fallback enabled. Cache: cache/russia_ai_translation_cache.json
[INFO] Loaded 0 cached translations
[INFO] AI fallback: Using Argos Translate (offline RU->EN)
[AI] Translated: Таблетки -> Tablets
[AI] Translated: Капсулы -> Capsules
```

### Missing Dictionary Report

Words that **still fail** after AI translation are logged in:
- `russia_pricing_missing_dictionary.csv`
- `russia_discontinued_missing_dictionary.csv`

These reports help identify:
- Translation failures
- Words that need manual review
- Terms to add to Dictionary.csv

## Advantages Over Dictionary-Only

| Feature | Dictionary Only | With AI Fallback |
|---------|----------------|------------------|
| Coverage | Limited to dictionary entries | Handles any Russian text |
| Maintenance | Manual updates required | Automatic translation |
| New terms | Must add to dictionary | Translated automatically |
| Speed (cached) | Instant | Instant |
| Speed (uncached) | N/A (missing) | ~0.1-1s per term |
| Offline support | ✓ Yes | ✓ Yes (with Argos) |

## Best Practices

1. **Keep Dictionary.csv updated**: Add frequently used terms for instant translation
2. **Review AI translations**: Check the cache file periodically for accuracy
3. **Use Argos for bulk**: Install Argos Translate for large datasets
4. **Monitor missing reports**: Review what still fails after AI
5. **Cache maintenance**: The cache grows over time; review and clean periodically

## Troubleshooting

### AI Translation Not Working

**Check 1: Is it enabled?**
```bash
# Look for this message in console output:
[INFO] AI translation fallback enabled
```

**Check 2: Are dependencies installed?**
```bash
pip install argostranslate deep-translator
```

**Check 3: Network connectivity** (for Google Translate)
```bash
ping translate.google.com
```

### Argos Model Download Fails

If automatic download fails:
```bash
# Manual installation
python -c "import argostranslate.package as pkg; pkg.update_package_index(); available = pkg.get_available_packages(); ru_en = next(p for p in available if p.from_code == 'ru' and p.to_code == 'en'); pkg.install_from_path(ru_en.download())"
```

### Translations Not Cached

Check cache file permissions:
```bash
# Should exist after first run
ls -la cache/russia_ai_translation_cache.json
```

## API Reference

### `enable_ai_fallback(cache_path=None)`

Enable AI translation fallback for missing dictionary entries.

**Parameters:**
- `cache_path` (Path, optional): Custom cache file path. Default: `cache/russia_ai_translation_cache.json`

**Example:**
```python
from translation_utils import enable_ai_fallback
enable_ai_fallback()  # Use default cache
```

### `translate_value(value, mapping, english_set, colname, miss_counter, miss_cols)`

Translate a single value using dictionary + AI fallback.

**Translation priority:**
1. Dictionary lookup
2. English detection
3. AI translation (if enabled)
4. Mark as missing

**Returns:** Translated string or original if translation fails

## Future Enhancements

Potential improvements:
- [ ] Support for additional translation engines (DeepL, Azure, etc.)
- [ ] Batch translation optimization for AI engines
- [ ] Translation quality scoring
- [ ] Auto-update Dictionary.csv from AI cache
- [ ] Multi-language support beyond Russian→English

## Support

For issues or questions:
1. Check the missing dictionary reports
2. Review the AI cache file
3. Run the test script: `python scripts/Russia/test_ai_fallback.py`
4. Check console output for error messages
