-- QC: Check for NULL values in required columns.
-- Usage: run via QualityGate with table_name and column_name parameters.
-- Returns rows where the specified column IS NULL.

SELECT
    '{column_name}' AS column_name,
    COUNT(*) AS null_count,
    ROUND(COUNT(*) * 100.0 / MAX(total.cnt), 2) AS null_pct
FROM src.{table_name},
     (SELECT COUNT(*) AS cnt FROM src.{table_name}) AS total
WHERE {column_name} IS NULL
HAVING COUNT(*) > 0;
