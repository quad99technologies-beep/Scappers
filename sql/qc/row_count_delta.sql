-- QC: Compare current row count against previous run.
-- Usage: run via QualityGate with table_name, previous_count, max_drop_pct.
-- Returns a single row with the delta analysis.

SELECT
    COUNT(*) AS current_count,
    {previous_count} AS previous_count,
    COUNT(*) - {previous_count} AS delta,
    CASE
        WHEN {previous_count} = 0 THEN 0.0
        ELSE ROUND((({previous_count} - COUNT(*)) * 100.0 / {previous_count}), 2)
    END AS drop_pct,
    CASE
        WHEN {previous_count} = 0 THEN 'pass'
        WHEN (({previous_count} - COUNT(*)) * 100.0 / {previous_count}) > {max_drop_pct} THEN 'fail'
        ELSE 'pass'
    END AS result
FROM src.{table_name};
