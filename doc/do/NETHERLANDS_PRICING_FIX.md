# Netherlands Scraper - Pricing Logic Fix

## ISSUE: Incorrect PPP Assignment

### Current (WRONG) Logic:
```python
# Line 1792-1794
ppp_vat = deductible_value(driver)  # ❌ This reads "Eigen risico" (Deductible) = €2.38
ppp_vat_float = euro_str_to_float(ppp_vat)
ppp_ex_vat = fmt_float(ppp_vat_float / (1.0 + VAT_RATE) if ppp_vat_float is not None else None)
```

### What's Wrong:
- `ppp_vat` is being set to the **Deductible** (Eigen risico) value
- But PPP should be the **"Gemiddelde prijs per..."** (Average price per package)

### From Screenshot:
- **PPP with VAT** = €4.81 (Gemiddelde prijs per...)
- **Copay with VAT** = €2.43 (You must pay an additional...)
- **RI (Deductible) with VAT** = €2.38 (Eigen risico)

### Correct Logic Should Be:
```python
# Read the package price (PPP with VAT)
pack_price_vat = prices.get("package", "")  # This is €4.81 = PPP with VAT

# Read deductible separately
deductible_vat = deductible_value(driver)  # This is €2.38 = RI with VAT

# Calculate PPP ex VAT
ppp_vat = pack_price_vat  # €4.81
ppp_vat_float = euro_str_to_float(ppp_vat)
ppp_ex_vat = fmt_float(ppp_vat_float / 1.09 if ppp_vat_float is not None else None)  # €4.81 / 1.09 = €4.41

# Store deductible separately (not as PPP!)
ri_vat = deductible_vat  # €2.38
ri_vat_float = euro_str_to_float(ri_vat)
ri_ex_vat = fmt_float(ri_vat_float / 1.09 if ri_vat_float is not None else None)  # €2.38 / 1.09 = €2.18
```

### Summary:
| Field | Current (WRONG) | Should Be (CORRECT) |
|-------|----------------|---------------------|
| `ppp_vat` | €2.38 (Deductible) ❌ | €4.81 (Package Price) ✅ |
| `ppp_ex_vat` | €2.18 ❌ | €4.41 ✅ |
| `deductible/RI` | Not stored ❌ | €2.38 ✅ |

### Fix Required:
1. Change `ppp_vat` to use `pack_price_vat` instead of `deductible_value()`
2. Store deductible separately (add new fields if needed)
3. Update database schema to include deductible fields

### Database Schema Update Needed:
```sql
ALTER TABLE nl_packs ADD COLUMN IF NOT EXISTS deductible_vat NUMERIC(12,4);
ALTER TABLE nl_packs ADD COLUMN IF NOT EXISTS deductible_ex_vat NUMERIC(12,4);
```

### Code Fix Location:
File: `01_get_medicijnkosten_data.py`
Lines: 1792-1794

Replace with:
```python
# PPP VAT is the package price (Gemiddelde prijs per...)
ppp_vat = pack_price_vat  # Already extracted from prices.get("package")
ppp_vat_float = euro_str_to_float(ppp_vat)
ppp_ex_vat = fmt_float(ppp_vat_float / 1.09 if ppp_vat_float is not None else None)

# Deductible (Eigen risico) - store separately
deductible_vat = deductible_value(driver)
deductible_vat_float = euro_str_to_float(deductible_vat)
deductible_ex_vat = fmt_float(deductible_vat_float / 1.09 if deductible_vat_float is not None else None)
```
