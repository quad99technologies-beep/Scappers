# PCID Mapping Unification Plan

## Current State

| Scraper | PCID Source | Table/Method | Issue |
|---------|-------------|--------------|-------|
| Argentina | Shared `pcid_mapping` table | DB table | ✅ Fixed - now uses unified module |
| Malaysia | Country-specific `my_pcid_reference` | DB table | Uses CSV import in step 5 |
| Belarus | CSV file | Direct CSV read | No DB table |
| Netherlands | CSV file | Direct CSV read | No DB table |
| Canada Ontario | None | N/A | No PCID mapping |
| Russia | None | N/A | No PCID mapping |

## Goal
All scrapers should read PCID mapping from database table (single source of truth), populated via GUI CSV upload.

## Solution

### 1. Shared Module (`core/pcid_mapping.py`)
Created unified module that all scrapers can use:
- `PCIDMapping` class - reads from `pcid_mapping` table
- `reload_pcid_mapping_from_csv()` - function to reload from CSV

### 2. Argentina (✅ Fixed)
**Files modified:**
- `01_getProdList.py` - Uses `PCIDMapping.get_oos()` to exclude OOS products
- `06_GenerateOutput.py` - Uses `PCIDMapping.get_all()` for final mapping

**Usage:**
```python
from core.pcid_mapping import PCIDMapping
pcid = PCIDMapping("Argentina")
oos_products = pcid.get_oos()  # For exclusion
all_mappings = pcid.get_all()  # For final report
```

### 3. Malaysia (TODO)
**Current:** Uses `my_pcid_reference` table + CSV import in step 5
**Required changes:**
1. Modify `step_05_pcid_export.py` to read from shared `pcid_mapping` table
2. Update `MalaysiaRepository.load_pcid_reference()` to use shared table
3. OR - migrate to use `core.pcid_mapping` module

### 4. Belarus (TODO)
**Current:** Reads directly from CSV in `02_belarus_pcid_mapping.py`
**Required changes:**
1. Create Belarus PCID mapping table OR use shared `pcid_mapping`
2. Modify `02_belarus_pcid_mapping.py` to read from DB table

### 5. Netherlands (TODO)
**Current:** Reads directly from CSV in `05_Generate_PCID_Mapped.py`
**Required changes:**
1. Modify to read from shared `pcid_mapping` table
2. Use `core.pcid_mapping` module

## GUI Integration

The GUI already has PCID mapping upload functionality that updates the `pcid_mapping` table:
- Input tab → Select country → PCID Mapping → Import CSV
- This calls `CSVImporter.import_csv()` which populates `pcid_mapping` table

## Migration Steps

1. ✅ Create `core/pcid_mapping.py` unified module
2. ✅ Update Argentina to use unified module
3. Update Malaysia to use unified module (or shared table)
4. Update Belarus to use unified module
5. Update Netherlands to use unified module
6. Test all scrapers with fresh PCID mapping uploads

## Testing Checklist

- [ ] Upload new PCID mapping via GUI
- [ ] Run Argentina step 1 - OOS products excluded correctly
- [ ] Run Argentina step 6 - Latest PCID mapping used
- [ ] Run Malaysia step 5 - Latest PCID mapping used
- [ ] Run Belarus step 2 - Latest PCID mapping used
- [ ] Run Netherlands step 5 - Latest PCID mapping used
