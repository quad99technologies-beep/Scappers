-- QC: Check price values are within sane range.
-- Usage: run via QualityGate with table_name, price_column, min_val, max_val.
-- Returns rows outside the valid range.

SELECT
    'below_min' AS issue,
    COUNT(*) AS count
FROM src.{table_name}
WHERE {price_column} IS NOT NULL AND CAST({price_column} AS REAL) < {min_val}

UNION ALL

SELECT
    'above_max' AS issue,
    COUNT(*) AS count
FROM src.{table_name}
WHERE {price_column} IS NOT NULL AND CAST({price_column} AS REAL) > {max_val}

UNION ALL

SELECT
    'non_numeric' AS issue,
    COUNT(*) AS count
FROM src.{table_name}
WHERE {price_column} IS NOT NULL
  AND typeof({price_column}) NOT IN ('integer', 'real')
  AND CAST({price_column} AS REAL) = 0
  AND {price_column} != '0';
