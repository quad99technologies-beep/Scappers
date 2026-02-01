-- Input tables schema for PostgreSQL (country-specific inputs)
-- Each country gets prefixed input tables.

-- ============================================================
-- PCID MAPPING (universal - shared across countries)
-- ============================================================
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
CREATE INDEX IF NOT EXISTS idx_pcid_code ON pcid_mapping(local_pack_code);
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
-- NETHERLANDS: search terms (nl_ prefix)
-- ============================================================
CREATE TABLE IF NOT EXISTS nl_input_search_terms (
    id SERIAL PRIMARY KEY,
    search_term TEXT NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- BELARUS: generic names (by_ prefix)
-- ============================================================
CREATE TABLE IF NOT EXISTS by_input_generic_names (
    id SERIAL PRIMARY KEY,
    generic_name TEXT NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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
CREATE TABLE IF NOT EXISTS cl_input_tender_list (
    id SERIAL PRIMARY KEY,
    tender_id TEXT NOT NULL,
    description TEXT,
    url TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- CANADA ONTARIO: (ca_on_ prefix)
-- ============================================================
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

-- ============================================================
-- NORTH MACEDONIA: (mk_ prefix)
-- ============================================================
CREATE TABLE IF NOT EXISTS mk_input_products (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
