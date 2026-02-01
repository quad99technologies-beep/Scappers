-- QC: Check for duplicate rows on specified key columns.
-- Usage: run via QualityGate with table_name and key_columns parameters.
-- Returns groups that have more than one row.

SELECT
    {key_columns},
    COUNT(*) AS dup_count
FROM src.{table_name}
GROUP BY {key_columns}
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 100;
