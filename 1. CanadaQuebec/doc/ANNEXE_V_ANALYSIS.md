# Annexe V Extraction Analysis & Issues

## PDF Structure

Based on `output/split_pdf/index.json`:
- **Annexe V spans pages 248-791 in original PDF** (544 pages total)
- **Annexe V PDF starts at page 1** (it's an extracted section)
- The script incorrectly assumes page 6 is the start

## Critical Data Loss Issues

### 1. **START_PAGE_1IDX = 6** ❌
   - **Problem**: Script starts from page 6, skipping pages 1-5 of annexe_v.pdf
   - **Impact**: **~5 pages of data lost** (could be 50-100+ product records)
   - **Fix**: Should be `START_PAGE_1IDX = 1` (start from beginning of annexe V)

### 2. **MAX_ROWS = 500** ❌
   - **Problem**: Script stops after 500 rows for "testing"
   - **Impact**: **All data after row 500 is lost** (annexe V likely has 1000+ rows)
   - **Fix**: Set to `None` for production, or make configurable via environment variable

### 3. **No Encoding Utilities** ❌
   - **Problem**: Script doesn't use `step_00_utils_encoding.py` for UTF-8/mojibake fixes
   - **Impact**: French characters (é, è, à, etc.) may be corrupted
   - **Fix**: Import and use `clean_extracted_text()` from encoding utilities

### 4. **Main Pipeline Skips Annexe V** ❌
   - **Problem**: `doc/step_04_extract_din_data.py.py` has `mode == "V"` → SKIP
   - **Impact**: Annexe V is completely ignored in main pipeline
   - **Fix**: Either integrate annexe V into main pipeline OR ensure standalone script is production-ready

## Code Quality Issues

### 5. **Hardcoded Paths**
   - Uses environment variables but has confusing fallback logic
   - Should use consistent BASE_DIR pattern like other scripts

### 6. **Poor Error Handling**
   - No try/except blocks around critical operations
   - No logging for debugging
   - Silent failures possible

### 7. **Parsing Logic Issues**
   - `parse_line_format_cost()` may miss multi-line format entries
   - `brand_and_manufacturer_from_after_tokens()` uses gap detection that may fail on tight layouts
   - No validation of extracted data

### 8. **No Integration with Pipeline**
   - Standalone script, not part of main pipeline
   - Output format differs slightly from standard format
   - No database integration (unlike main pipeline)

## Recommended Fixes

1. ✅ Change `START_PAGE_1IDX = 1` (or detect from PDF structure)
2. ✅ Remove `MAX_ROWS` limit or make it configurable via env var
3. ✅ Integrate encoding utilities (`step_00_utils_encoding.py`)
4. ✅ Add proper error handling and logging
5. ✅ Improve parsing logic for edge cases
6. ✅ Add data validation
7. ✅ Consider integrating into main pipeline or creating unified extraction

## Expected Data Volume

- Annexe V: ~544 pages
- Estimated rows: **1000-2000+** (depending on density)
- Current extraction: **~500 rows max** (due to MAX_ROWS limit)
- **Data loss: ~50-70% of records**

