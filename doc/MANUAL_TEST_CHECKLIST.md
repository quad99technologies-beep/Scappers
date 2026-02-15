# Manual Test Checklist

## Pre-Test Setup

```powershell
# Set environment variables
$env:POSTGRES_HOST="localhost"
$env:POSTGRES_PORT="5432"
$env:POSTGRES_DB="scrappers"
$env:POSTGRES_USER="postgres"
$env:POSTGRES_PASSWORD="admin123"
```

---

## Task 1: Directory Migration Verification

### 1.1 Verify Old Directories Removed
- [ ] `scripts\Colombia` - MOVED to `archive\scripts\Colombia`
- [ ] `scripts\Peru` - MOVED to `archive\scripts\Peru`
- [ ] `scripts\South Korea` - MOVED to `archive\scripts\South Korea`
- [ ] `scripts\Italy` - MOVED to `archive\scripts\Italy`

### 1.2 Verify New Directory Names
- [ ] `scripts\canada_ontario` (was `Canada Ontario`)
- [ ] `scripts\canada_quebec` (was `CanadaQuebec`)
- [ ] `scripts\north_macedonia` (was `North Macedonia`)
- [ ] `scripts\tender_chile` (was `Tender- Chile`)
- [ ] `scripts\tender_brazil` (was `Tender - Brazil`)

### 1.3 Verify JSON Cache Files Archived
- [ ] `cache\argentina_translation_cache.json` - MOVED to `archive\cache\`
- [ ] `cache\russia_translation_cache.json` - MOVED to `archive\cache\`
- [ ] `cache\russia_ai_translation_cache.json` - MOVED to `archive\cache\`
- [ ] `cache\russia_translation_cache_en.json` - MOVED to `archive\cache\`
- [ ] `cache\belarus_translation_cache.json` - MOVED to `archive\cache\`

**Verify command:**
```powershell
Get-ChildItem archive\cache -File
Get-ChildItem cache -File  # Should only show run_index.json
```

---

## Task 2: Translation Cache Verification

### 2.1 Unified Cache Module Tests

**Test 2.1.1: Basic Cache Operations**
```powershell
python -c "
from core.translation import TranslationCache

cache = TranslationCache('argentina')

# Test set
result = cache.set('hello', 'hola', 'en', 'es')
print(f'Set: {result}')  # Expected: True

# Test get
result = cache.get('hello', 'en', 'es')
print(f'Get: {result}')  # Expected: hola

# Test stats
stats = cache.get_stats()
print(f'Entries: {stats[\"total_entries\"]}')
print(f'Schema: {stats[\"schema\"]}')  # Expected: unified
"
```
- [ ] Set operation returns True
- [ ] Get operation returns correct value
- [ ] Stats show correct entry count
- [ ] Schema detected correctly

**Test 2.1.2: Multi-Scraper Cache**
```powershell
python -c "
from core.translation import get_cache

# Test each scraper
for scraper in ['argentina', 'russia', 'belarus', 'north_macedonia', 'malaysia']:
    cache = get_cache(scraper)
    cache.set(f'test_{scraper}', f'result_{scraper}', 'src', 'tgt')
    result = cache.get(f'test_{scraper}', 'src', 'tgt')
    print(f'{scraper}: {result == f\"result_{scraper}\"}')"
```
- [ ] Argentina cache works
- [ ] Russia cache works
- [ ] Belarus cache works
- [ ] North Macedonia cache works
- [ ] Malaysia cache works

### 2.2 Repository Integration Tests

**Test 2.2.1: Argentina Repository**
```powershell
python -c "
import sys
sys.path.insert(0, 'scripts/Argentina')
from scripts.Argentina.db.repositories import ArgentinaRepository
from core.db.connection import CountryDB

db = CountryDB('Argentina')
db.connect()
repo = ArgentinaRepository(db, 'test_manual_run')

# Save translation
repo.save_single_translation('medicamento_test', 'medicine_test', 'es', 'en')

# Retrieve translation
result = repo.get_cached_translation('medicamento_test', 'es', 'en')
print(f'Argentina repo: {result == \"medicine_test\"}')"  # Expected: True
```
- [ ] Save via repository works
- [ ] Load via repository works

**Test 2.2.2: Russia Repository**
```powershell
python -c "
import sys
sys.path.insert(0, 'scripts/Russia')
from scripts.Russia.db.repositories import RussiaRepository
from core.db.connection import CountryDB

db = CountryDB('Russia')
db.connect()
repo = RussiaRepository(db, 'test_manual_run')

repo.save_single_translation('lekarstvo_test', 'medicine_test', 'ru', 'en')
result = repo.get_cached_translation('lekarstvo_test', 'ru', 'en')
print(f'Russia repo: {result == \"medicine_test\"}')"  # Expected: True
```
- [ ] Save via repository works
- [ ] Load via repository works

**Test 2.2.3: Belarus Repository**
```powershell
python -c "
import sys
sys.path.insert(0, 'scripts/Belarus')
from scripts.Belarus.db.repositories import BelarusRepository
from core.db.connection import CountryDB

db = CountryDB('Belarus')
db.connect()
repo = BelarusRepository(db, 'test_manual_run')

repo.save_single_translation('lekarstvo_test', 'medicine_test', 'be', 'en')
result = repo.get_cached_translation('lekarstvo_test', 'be', 'en')
print(f'Belarus repo: {result == \"medicine_test\"}')"  # Expected: True
```
- [ ] Save via repository works
- [ ] Load via repository works

**Test 2.2.4: North Macedonia (was in-memory only)**
```powershell
python -c "
import sys
sys.path.insert(0, 'scripts/north_macedonia')
from core.translation import get_cache

# North Macedonia now uses persistent cache
cache = get_cache('north_macedonia')
cache.set('lek_test', 'medicine_test', 'mk', 'en')

# Verify it's in DB (not just memory)
stats = cache.get_stats()
print(f'North Macedonia entries: {stats[\"total_entries\"]}')  # Should be >= 1
print(f'Persistence: {cache.get(\"lek_test\", \"mk\", \"en\") == \"medicine_test\"}')"  # Expected: True
```
- [ ] Cache persists (not lost on restart)
- [ ] Entries stored in DB

### 2.3 Migration Verification

**Test 2.3.1: Verify Migrated Data**
```powershell
python -c "
from core.translation import get_cache

# Russia should have ~5800 entries from migration
cache = get_cache('russia')
stats = cache.get_stats()
print(f'Russia entries: {stats[\"total_entries\"]}')  # Expected: ~5802

# Belarus should have ~5400 entries from migration
cache = get_cache('belarus')
stats = cache.get_stats()
print(f'Belarus entries: {stats[\"total_entries\"]}')  # Expected: ~5467
"
```
- [ ] Russia has migrated entries
- [ ] Belarus has migrated entries

---

## Task 3: Deprecation Warnings

### 3.1 platform_config.py Deprecation
```powershell
python -c "
import warnings
warnings.filterwarnings('always', category=DeprecationWarning)

import platform_config
"
```
- [ ] Deprecation warning printed on import

### 3.2 PathManager Method Deprecation
```powershell
python -c "
import warnings
warnings.filterwarnings('always', category=DeprecationWarning)

from platform_config import PathManager

# These should emit warnings
PathManager.get_platform_root()
PathManager.get_config_dir()
PathManager.get_input_dir('argentina')
"
```
- [ ] Warning on `get_platform_root()`
- [ ] Warning on `get_config_dir()`
- [ ] Warning on `get_input_dir()`

---

## Integration Tests

### Integration Test 1: Full Translation Flow (Argentina)
```powershell
python -c "
import sys
sys.path.insert(0, 'scripts/Argentina')
from scripts.Argentina.db.repositories import ArgentinaRepository
from core.db.connection import CountryDB

db = CountryDB('Argentina')
db.connect()
repo = ArgentinaRepository(db, 'integration_test')

# Simulate translation workflow
terms = [
    ('paracetamol', 'paracetamol', 'es', 'en'),
    ('ibuprofeno', 'ibuprofen', 'es', 'en'),
    ('amoxicilina', 'amoxicillin', 'es', 'en'),
]

# Save all
for source, translated, src_lang, tgt_lang in terms:
    repo.save_single_translation(source, translated, src_lang, tgt_lang)

# Retrieve all
all_found = True
for source, expected, src_lang, tgt_lang in terms:
    result = repo.get_cached_translation(source, src_lang, tgt_lang)
    if result != expected:
        print(f'FAIL: {source} -> {result} (expected {expected})')
        all_found = False

if all_found:
    print('Integration test: PASS')
"
```
- [ ] All terms saved correctly
- [ ] All terms retrieved correctly

### Integration Test 2: Cache Persistence
```powershell
# First run - save something
python -c "
from core.translation import get_cache
cache = get_cache('test_persistence')
cache.set('persistent_key', 'persistent_value', 'src', 'tgt')
print('Saved')
"

# Second run - verify it's still there
python -c "
from core.translation import get_cache
cache = get_cache('test_persistence')
result = cache.get('persistent_key', 'src', 'tgt')
print(f'Persistence test: {result == \"persistent_value\"}')"  # Expected: True
```
- [ ] Data persists across separate Python runs

---

## Final Verification

### Run Automated Test Suite
```powershell
python smoke_test.py
python test_translation_cache.py
```
- [ ] smoke_test.py: ALL PASSED
- [ ] test_translation_cache.py: ALL PASSED

### Check No Regressions
- [ ] No import errors in core modules
- [ ] No DB connection errors
- [ ] All scraper directories accessible

---

## Sign-Off

| Tester | Date | Result |
|--------|------|--------|
|        |      | ☐ PASS / ☐ FAIL |

**Notes:**
