-- Input tables schema for PostgreSQL (country-specific inputs)
-- Each country gets prefixed input tables.

-- ============================================================
-- PCID MAPPING (universal - shared across countries)
-- ============================================================

-- Create table with basic columns first (without presentation-dependent constraints)
CREATE TABLE IF NOT EXISTS pcid_mapping (
    id SERIAL PRIMARY KEY,
    pcid TEXT,
    company TEXT,
    local_product_name TEXT,
    generic_name TEXT,
    local_pack_description TEXT,
    local_pack_code TEXT,
    source_country TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add presentation column if it doesn't exist (for existing tables)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pcid_mapping' AND column_name = 'presentation'
    ) THEN
        ALTER TABLE pcid_mapping ADD COLUMN presentation TEXT;
    END IF;
END $$;

-- Add unique constraint on (local_pack_code, presentation, source_country) if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'pcid_mapping_local_pack_code_presentation_source_country_key'
    ) THEN
        ALTER TABLE pcid_mapping 
        ADD CONSTRAINT pcid_mapping_local_pack_code_presentation_source_country_key 
        UNIQUE (local_pack_code, presentation, source_country);
    END IF;
EXCEPTION WHEN OTHERS THEN
    -- Constraint may fail if duplicates exist, that's ok for now
    NULL;
END $$;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_pcid_code ON pcid_mapping(local_pack_code);

-- Create presentation index only if column exists
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pcid_mapping' AND column_name = 'presentation'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_pcid_code_presentation ON pcid_mapping(local_pack_code, presentation);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_pcid_generic ON pcid_mapping(generic_name);
CREATE INDEX IF NOT EXISTS idx_pcid_product ON pcid_mapping(local_product_name);

-- ============================================================
-- ARGENTINA: product list + ignore list + dictionary (ar_ prefix)
-- ============================================================
CREATE TABLE IF NOT EXISTS ar_input_product_list (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    url TEXT,
    company TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ar_input_ignore_list (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ar_input_dictionary (
    id SERIAL PRIMARY KEY,
    source_term TEXT NOT NULL,
    translated_term TEXT,
    language_from TEXT DEFAULT 'es',
    language_to TEXT DEFAULT 'en',
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ar_dict_source ON ar_input_dictionary(source_term);

-- ============================================================
-- BELARUS: generic names + dictionary (by_ prefix)
-- ============================================================
CREATE TABLE IF NOT EXISTS by_input_generic_names (
    id SERIAL PRIMARY KEY,
    generic_name TEXT NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Belarus translation dictionary (source_term -> translated_term for RU->EN)
CREATE TABLE IF NOT EXISTS by_input_dictionary (
    id SERIAL PRIMARY KEY,
    source_term TEXT NOT NULL,
    translated_term TEXT,
    language_from TEXT DEFAULT 'ru',
    language_to TEXT DEFAULT 'en',
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_by_dict_source ON by_input_dictionary(source_term);

-- ============================================================
-- MALAYSIA: product list (my_ prefix)
-- ============================================================
CREATE TABLE IF NOT EXISTS my_input_products (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    registration_no TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_my_input_products_name ON my_input_products(product_name);

-- ============================================================
-- TAIWAN: ATC prefixes (tw_ prefix)
-- ============================================================
CREATE TABLE IF NOT EXISTS tw_input_atc_prefixes (
    id SERIAL PRIMARY KEY,
    atc_code TEXT NOT NULL,
    description TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- TENDER CHILE: tender list (cl_ prefix)
-- ============================================================
-- Tender Chile input table (tc_ prefix to match Chile schema)
CREATE TABLE IF NOT EXISTS tc_input_tender_list (
    id SERIAL PRIMARY KEY,
    tender_id TEXT NOT NULL,
    description TEXT,
    url TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tc_input_tender_list_id ON tc_input_tender_list(tender_id);

-- Legacy cl_ prefix table (kept for migration compatibility)
CREATE TABLE IF NOT EXISTS cl_input_tender_list (
    id SERIAL PRIMARY KEY,
    tender_id TEXT NOT NULL,
    description TEXT,
    url TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- CANADA ONTARIO: (co_ prefix - matches postgres_connection)
-- ============================================================
CREATE TABLE IF NOT EXISTS co_input_products (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    din TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Legacy ca_on_ prefix (kept for migration compatibility)
CREATE TABLE IF NOT EXISTS ca_on_input_products (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    din TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- CANADA QUEBEC: (ca_qc_ prefix)
-- ============================================================
CREATE TABLE IF NOT EXISTS ca_qc_input_products (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    din TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- RUSSIA: (ru_ prefix)
-- ============================================================
CREATE TABLE IF NOT EXISTS ru_input_products (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Russia translation dictionary (source_term -> translated_term for RU->EN)
CREATE TABLE IF NOT EXISTS ru_input_dictionary (
    id SERIAL PRIMARY KEY,
    source_term TEXT NOT NULL,
    translated_term TEXT,
    language_from TEXT DEFAULT 'ru',
    language_to TEXT DEFAULT 'en',
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_term, language_from, language_to)
);
CREATE INDEX IF NOT EXISTS idx_ru_dict_source ON ru_input_dictionary(source_term);

-- ============================================================
-- INDIA: formulations list (in_ prefix)
-- ============================================================
CREATE TABLE IF NOT EXISTS in_input_formulations (
    id SERIAL PRIMARY KEY,
    generic_name TEXT NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_in_form_name ON in_input_formulations(generic_name);

-- ============================================================
-- NORTH MACEDONIA: (mk_ prefix)
-- ============================================================
CREATE TABLE IF NOT EXISTS mk_input_products (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
