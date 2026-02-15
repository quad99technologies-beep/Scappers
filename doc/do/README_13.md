# Russia Pipeline Documentation

## Overview

The Russia pipeline scrapes pharmaceutical pricing data from Russian sources and generates standardized reports. It now includes **AI-powered translation** to automatically translate Russian text to English.

## Features

- ✅ Scrape VED Registry pricing data
- ✅ Scrape excluded drugs list
- ✅ Generate standardized reports
- ✅ **NEW: AI translation fallback** for missing dictionary entries
- ✅ Translation caching for performance
- ✅ Offline translation support (Argos Translate)
- ✅ Automatic checkpoint/resume functionality

## Translation System

### Two-Tier Translation Approach

```
┌─────────────────────────────────────────────────────────────┐
│                    TIER 1: Dictionary                       │
│  ┌────────────────────────────────────────────────────┐    │
│  │  input/Russia/Dictionary.csv                       │    │
│  │  - Manual, curated translations                    │    │
│  │  - Instant lookup                                  │    │
│  │  - Highest priority                                │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↓                                 │
│                   Dictionary miss?                          │
│                           ↓                                 │
│                    TIER 2: AI Fallback                      │
│  ┌────────────────────────────────────────────────────┐    │
│  │  AI Translation Engines                            │    │
│  │  - Argos Translate (offline, priority)            │    │
│  │  - Google Translate (online, fallback)            │    │
│  │  - Automatic caching                               │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Quick Start

**Enable AI translation (default):**
```bash
python scripts/Russia/run_pipeline_resume.py
```

**Test translation:**
```bash
python scripts/Russia/test_ai_fallback.py
```

**See:** [TRANSLATION_QUICK_START.md](TRANSLATION_QUICK_START.md)

## Pipeline Steps

| Step | Script | Description | AI Translation |
|------|--------|-------------|----------------|
| 0 | `00_backup_and_clean.py` | Backup and clean output directory | - |
| 1 | `01_russia_farmcom_scraper.py` | Scrape VED pricing data | - |
| 2 | `02_russia_farmcom_excluded_scraper.py` | Scrape excluded drugs list | - |
| 3 | `03_generate_output.py` | Generate clean reports | - |
| 4 | `04_generate_pricing_data.py` | Generate pricing template | ✅ Enabled |
| 5 | `05_generate_discontinued_list.py` | Generate discontinued template | ✅ Enabled |
| 6 | `06_translate_reports.py` | Translate final reports | ✅ Always |

## Input Files

| File | Purpose | Required |
|------|---------|----------|
| `input/Russia/Dictionary.csv` | Manual translations (Russian→English) | Yes |
| `config/Russia.env.json` | Pipeline configuration | Yes |

## Output Files

| File | Description |
|------|-------------|
| `output/Russia/russia_ved_report.csv` | VED pricing data (Russian) |
| `output/Russia/russia_excluded_report.csv` | Excluded drugs (Russian) |
| `output/Russia/russia_pricing_data.csv` | Pricing template (English) |
| `output/Russia/russia_discontinued_list.csv` | Discontinued template (English) |
| `exports/Russia/Russia_VED_Report.csv` | Final VED report (English) |
| `exports/Russia/Russia_Excluded_Report.csv` | Final excluded report (English) |
| `exports/Russia/Russia_Pricing_Data.csv` | Final pricing data (English) |
| `exports/Russia/Russia_Discontinued_List.csv` | Final discontinued list (English) |

## Cache Files

| File | Purpose |
|------|---------|
| `cache/russia_ai_translation_cache.json` | AI translation cache |
| `cache/russia_translation_cache_en.json` | Final report translation cache |

## Installation

### Basic Requirements

```bash
pip install -r requirements.txt
```

### AI Translation (Recommended)

```bash
# For offline translation
pip install argostranslate

# For online fallback
pip install deep-translator
```

## Usage

### Run Full Pipeline

```bash
python scripts/Russia/run_pipeline_resume.py
```

### Run Specific Step

```bash
# With AI translation
python scripts/Russia/04_generate_pricing_data.py --enable-ai-fallback

# Without AI translation
python scripts/Russia/04_generate_pricing_data.py
```

### Resume from Checkpoint

The pipeline automatically resumes from the last completed step:

```bash
# Resumes automatically
python scripts/Russia/run_pipeline_resume.py
```

## Configuration

### Enable/Disable AI Translation

**Enabled by default** in pipeline. To disable:

Edit [run_pipeline_resume.py](../../scripts/Russia/run_pipeline_resume.py):
```python
# Change from:
(4, "04_generate_pricing_data.py", "...", [...], ["--enable-ai-fallback"]),
# To:
(4, "04_generate_pricing_data.py", "...", [...], None),
```

## Performance

### Translation Speed

| Scenario | Speed | Notes |
|----------|-------|-------|
| Dictionary hit | <1ms | Instant |
| AI cache hit | <1ms | Instant |
| AI translation (Argos) | 100-500ms | First time only |
| AI translation (Google) | 200-1000ms | Network dependent |

### Coverage

- **Dictionary only**: ~70-80% coverage
- **Dictionary + AI**: ~99% coverage

## Troubleshooting

### AI Translation Not Working

1. **Check if enabled:**
   ```bash
   # Look for this in console:
   [INFO] AI translation fallback enabled
   ```

2. **Install dependencies:**
   ```bash
   pip install argostranslate deep-translator
   ```

3. **Test directly:**
   ```bash
   python scripts/Russia/test_ai_fallback.py
   ```

### Dictionary Encoding Issues

The Dictionary.csv file shows `????????` instead of Cyrillic. To fix:

1. Open Dictionary.csv in a text editor
2. Save with UTF-8 encoding
3. Verify Cyrillic characters display correctly

### Missing Translations

Check the missing dictionary reports:
- `russia_pricing_missing_dictionary.csv`
- `russia_discontinued_missing_dictionary.csv`

Add frequently used terms to Dictionary.csv for faster translation.

## Documentation

| Document | Description |
|----------|-------------|
| [AI_TRANSLATION_FALLBACK.md](AI_TRANSLATION_FALLBACK.md) | Comprehensive AI translation guide |
| [TRANSLATION_QUICK_START.md](TRANSLATION_QUICK_START.md) | Quick reference guide |
| [CHANGELOG_AI_FALLBACK.md](CHANGELOG_AI_FALLBACK.md) | Implementation changelog |

## Support

For issues or questions:
1. Run test script: `python scripts/Russia/test_ai_fallback.py`
2. Check console output for error messages
3. Review missing dictionary reports
4. Check AI cache: `cache/russia_ai_translation_cache.json`

## Recent Updates

### 2026-01-20: AI Translation Fallback
- Added AI-powered translation for missing dictionary entries
- Implemented translation caching
- Enabled by default in pipeline
- See: [CHANGELOG_AI_FALLBACK.md](CHANGELOG_AI_FALLBACK.md)
