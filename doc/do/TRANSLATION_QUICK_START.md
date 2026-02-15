# Russia Translation - Quick Start Guide

## TL;DR

**AI translation fallback is now enabled by default!** Missing dictionary words are automatically translated.

## Run the Pipeline

```bash
python scripts/Russia/run_pipeline_resume.py
```

That's it! AI translation is automatically enabled for steps 4 and 5.

## How It Works

```
Russian word → Dictionary lookup → AI translation → English output
```

- **In dictionary**: Instant translation
- **Not in dictionary**: AI translates automatically
- **Already translated**: Uses cache (instant)

## Translation Engines

1. **Argos Translate** (offline, free, recommended)
2. **Google Translate** (online, fallback)

## Installation

```bash
# For offline translation (recommended)
pip install argostranslate

# For online fallback
pip install deep-translator
```

## Test It

```bash
python scripts/Russia/test_ai_fallback.py
```

## Disable AI Fallback

If you only want dictionary-based translation:

```bash
# Edit run_pipeline_resume.py
# Change lines 195 and 200 from:
["--enable-ai-fallback"]
# To:
None
```

## Files

**Input:**
- `input/Russia/Dictionary.csv` - Manual translations

**Cache:**
- `cache/russia_ai_translation_cache.json` - AI translations

**Output:**
- `*_missing_dictionary.csv` - Words that still failed

## Example Output

```
[INFO] AI translation fallback enabled
[INFO] Using Argos Translate (offline RU->EN)
[AI] Translated: Таблетки → Tablets
[AI] Translated: Раствор для инъекций → Solution for injection
```

## Performance

- **First run**: Slower (building cache)
- **Subsequent runs**: Fast (using cache)
- **Cache hits**: Instant (same as dictionary)

## Benefits

✓ Handles new Russian terms automatically
✓ No manual dictionary updates needed
✓ Offline support (with Argos)
✓ Free (no API costs)
✓ Fast with caching
✓ Fallback if dictionary incomplete

## Still Getting Untranslated Words?

Check the missing dictionary reports:
- `russia_pricing_missing_dictionary.csv`
- `russia_discontinued_missing_dictionary.csv`

Add frequently used terms to `Dictionary.csv` for faster translation.

## Need Help?

See full documentation: [AI_TRANSLATION_FALLBACK.md](AI_TRANSLATION_FALLBACK.md)
