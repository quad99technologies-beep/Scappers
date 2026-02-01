-- India NPPA export: produces the same CSV format as the original scraper
-- Replicates build_final_one_brand_one_row_csv logic:
--   1 MAIN row per SKU + N OTHER rows per brand alternative
--   Dedup by (HiddenId, BrandType, BrandName, PackSize)

WITH main_rows AS (
    SELECT
        s.hidden_id AS HiddenId,
        'MAIN' AS BrandType,
        s.sku_name AS BrandName,
        s.company AS Company,
        s.composition AS Composition,
        s.pack_size AS PackSize,
        s.dosage_form AS Unit,
        s.schedule_status AS Status,
        s.ceiling_price AS CeilingPrice,
        s.mrp AS MRP,
        s.mrp_per_unit AS MRPPerUnit,
        s.year_month AS YearMonth
    FROM src.sku_main s
    WHERE s.run_id = '{run_id}'
),
other_rows AS (
    SELECT
        b.hidden_id AS HiddenId,
        'OTHER' AS BrandType,
        b.brand_name AS BrandName,
        b.company AS Company,
        s.composition AS Composition,
        b.pack_size AS PackSize,
        s.dosage_form AS Unit,
        s.schedule_status AS Status,
        s.ceiling_price AS CeilingPrice,
        b.brand_mrp AS MRP,
        b.mrp_per_unit AS MRPPerUnit,
        COALESCE(NULLIF(b.year_month, ''), s.year_month) AS YearMonth
    FROM src.brand_alternatives b
    JOIN src.sku_main s ON b.hidden_id = s.hidden_id AND s.run_id = '{run_id}'
    WHERE b.run_id = '{run_id}'
),
combined AS (
    SELECT * FROM main_rows
    UNION ALL
    SELECT * FROM other_rows
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY HiddenId, BrandType, BrandName, PackSize
            ORDER BY YearMonth DESC NULLS LAST, MRP DESC NULLS LAST
        ) AS rn
    FROM combined
)
SELECT
    HiddenId,
    BrandType,
    BrandName,
    Company,
    Composition,
    PackSize,
    Unit,
    Status,
    CeilingPrice,
    MRP,
    MRPPerUnit,
    YearMonth
FROM deduped
WHERE rn = 1
ORDER BY HiddenId, BrandType DESC;
