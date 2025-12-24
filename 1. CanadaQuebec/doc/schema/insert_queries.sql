-- Parameterized INSERT queries for database operations
-- All queries use %s placeholders for safe parameterized execution
-- NEVER use f-strings or string concatenation with these queries

-- ============================================================================
-- RUN TRACKING
-- ============================================================================

-- Insert new run
-- Parameters: run_id (UUID), scraper_id (TEXT), started_at (TIMESTAMP), status (TEXT)
INSERT INTO scraper_runs (run_id, scraper_id, started_at, status)
VALUES (%s, %s, %s, %s);

-- Update run completion
-- Parameters: completed_at (TIMESTAMP), status (TEXT), total_pages (INTEGER), total_records (INTEGER), run_id (UUID)
UPDATE scraper_runs
SET completed_at = %s,
    status = %s,
    total_pages = %s,
    total_records = %s
WHERE run_id = %s;

-- Update run error
-- Parameters: status (TEXT), error_message (TEXT), run_id (UUID)
UPDATE scraper_runs
SET status = %s,
    error_message = %s,
    completed_at = NOW()
WHERE run_id = %s;

-- ============================================================================
-- EXTRACTED DATA (from step_04_extract_din_data.py)
-- ============================================================================

-- Insert single extracted data record
-- Parameters: run_id, scraper_id, generic, flags, form, strength, strength_value, strength_unit, ppb, din, brand, manufacturer, pack, pack_price, unit_price, unit_price_source, page, confidence, confidence_label
INSERT INTO extracted_data (
    run_id, scraper_id, generic, flags, form, strength, strength_value, strength_unit,
    ppb, din, brand, manufacturer, pack, pack_price, unit_price, unit_price_source,
    page, confidence, confidence_label
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);

-- Batch insert extracted data (for execute_batch)
-- Same parameters as single insert, but called with execute_batch
INSERT INTO extracted_data (
    run_id, scraper_id, generic, flags, form, strength, strength_value, strength_unit,
    ppb, din, brand, manufacturer, pack, pack_price, unit_price, unit_price_source,
    page, confidence, confidence_label
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);

-- Upsert extracted data (ON CONFLICT DO UPDATE)
-- Uses din + run_id as unique constraint (if needed, add unique constraint first)
-- Parameters: same as insert, plus all values again for UPDATE clause
INSERT INTO extracted_data (
    run_id, scraper_id, generic, flags, form, strength, strength_value, strength_unit,
    ppb, din, brand, manufacturer, pack, pack_price, unit_price, unit_price_source,
    page, confidence, confidence_label
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (id) DO UPDATE SET
    generic = EXCLUDED.generic,
    flags = EXCLUDED.flags,
    form = EXCLUDED.form,
    strength = EXCLUDED.strength,
    strength_value = EXCLUDED.strength_value,
    strength_unit = EXCLUDED.strength_unit,
    ppb = EXCLUDED.ppb,
    brand = EXCLUDED.brand,
    manufacturer = EXCLUDED.manufacturer,
    pack = EXCLUDED.pack,
    pack_price = EXCLUDED.pack_price,
    unit_price = EXCLUDED.unit_price,
    unit_price_source = EXCLUDED.unit_price_source,
    page = EXCLUDED.page,
    confidence = EXCLUDED.confidence,
    confidence_label = EXCLUDED.confidence_label,
    updated_at = NOW();

-- ============================================================================
-- STANDARD FORMAT DATA (from step_07_transform_to_standard_format.py)
-- ============================================================================

-- Insert single standard format record
-- Parameters: run_id, scraper_id, country, company, local_product_name, generic_name, currency, ex_factory_wholesale_price, region, marketing_authority, local_pack_description, formulation, fill_size, strength, strength_unit, local_pack_code
INSERT INTO standard_format_data (
    run_id, scraper_id, country, company, local_product_name, generic_name, currency,
    ex_factory_wholesale_price, region, marketing_authority, local_pack_description,
    formulation, fill_size, strength, strength_unit, local_pack_code
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);

-- Batch insert standard format data (for execute_batch)
-- Same parameters as single insert
INSERT INTO standard_format_data (
    run_id, scraper_id, country, company, local_product_name, generic_name, currency,
    ex_factory_wholesale_price, region, marketing_authority, local_pack_description,
    formulation, fill_size, strength, strength_unit, local_pack_code
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);

-- Upsert standard format data
-- Parameters: same as insert, plus all values again for UPDATE clause
INSERT INTO standard_format_data (
    run_id, scraper_id, country, company, local_product_name, generic_name, currency,
    ex_factory_wholesale_price, region, marketing_authority, local_pack_description,
    formulation, fill_size, strength, strength_unit, local_pack_code
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (id) DO UPDATE SET
    country = EXCLUDED.country,
    company = EXCLUDED.company,
    local_product_name = EXCLUDED.local_product_name,
    generic_name = EXCLUDED.generic_name,
    currency = EXCLUDED.currency,
    ex_factory_wholesale_price = EXCLUDED.ex_factory_wholesale_price,
    region = EXCLUDED.region,
    marketing_authority = EXCLUDED.marketing_authority,
    local_pack_description = EXCLUDED.local_pack_description,
    formulation = EXCLUDED.formulation,
    fill_size = EXCLUDED.fill_size,
    strength = EXCLUDED.strength,
    strength_unit = EXCLUDED.strength_unit,
    local_pack_code = EXCLUDED.local_pack_code,
    updated_at = NOW();

