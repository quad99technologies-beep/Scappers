-- =============================================================================
-- Platform Schema: Distributed Worker & Generic Data Model
-- =============================================================================
-- This schema supports:
-- 1. Distributed worker execution with job queue
-- 2. Generic entity/attribute model for all countries
-- 3. URL registry and fetch logging
-- 4. File storage tracking
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DISTRIBUTED WORKER TABLES
-- -----------------------------------------------------------------------------

-- Pipeline runs: tracks each pipeline execution
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    country TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK(status IN ('queued', 'running', 'stopped', 'completed', 'failed', 'cancelled')),
    current_step TEXT,
    current_step_num INTEGER DEFAULT 0,
    total_steps INTEGER,
    worker_id TEXT,  -- Which worker claimed this job
    priority INTEGER DEFAULT 0,  -- Higher = higher priority
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    last_heartbeat TIMESTAMP,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT,
    metadata_json JSONB DEFAULT '{}'::jsonb
);

-- Indexes for pipeline_runs
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_country ON pipeline_runs(country);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_worker ON pipeline_runs(worker_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_created ON pipeline_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_heartbeat ON pipeline_runs(last_heartbeat) 
    WHERE status = 'running';

-- Pipeline commands: control channel for workers
CREATE TABLE IF NOT EXISTS pipeline_commands (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    command TEXT NOT NULL CHECK(command IN ('stop', 'resume', 'cancel', 'pause')),
    issued_by TEXT,  -- Who issued the command (user, system, etc.)
    acknowledged_at TIMESTAMP,  -- When worker acknowledged
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_commands_run ON pipeline_commands(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_commands_pending ON pipeline_commands(run_id, created_at DESC) 
    WHERE acknowledged_at IS NULL;

-- Worker registry: tracks active workers
CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    pid INTEGER,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'idle', 'busy', 'offline')),
    current_run_id UUID REFERENCES pipeline_runs(run_id),
    capabilities JSONB DEFAULT '[]'::jsonb,  -- Which countries/tasks this worker can handle
    metadata_json JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status);
CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON workers(last_heartbeat);

-- -----------------------------------------------------------------------------
-- GENERIC DATA MODEL TABLES
-- -----------------------------------------------------------------------------

-- URL registry: tracks all discovered URLs
CREATE TABLE IF NOT EXISTS urls (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    url_hash TEXT GENERATED ALWAYS AS (md5(url)) STORED,
    country TEXT NOT NULL,
    source TEXT,  -- Where this URL was discovered (seed, crawl, sitemap, etc.)
    entity_type TEXT,  -- product, tender, drug, etc.
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'queued', 'fetching', 'fetched', 'failed', 'skipped')),
    priority INTEGER DEFAULT 0,
    depth INTEGER DEFAULT 0,  -- Crawl depth from seed
    last_fetch_at TIMESTAMP,
    next_fetch_at TIMESTAMP,
    fetch_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    content_hash TEXT,  -- Hash of fetched content for change detection
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata_json JSONB DEFAULT '{}'::jsonb,
    UNIQUE(url_hash, country)
);

CREATE INDEX IF NOT EXISTS idx_urls_country ON urls(country);
CREATE INDEX IF NOT EXISTS idx_urls_status ON urls(status);
CREATE INDEX IF NOT EXISTS idx_urls_hash ON urls(url_hash);
CREATE INDEX IF NOT EXISTS idx_urls_entity_type ON urls(entity_type);
CREATE INDEX IF NOT EXISTS idx_urls_pending ON urls(country, priority DESC, created_at) 
    WHERE status = 'pending';

-- Fetch logs: detailed log of every fetch operation
CREATE TABLE IF NOT EXISTS fetch_logs (
    id BIGSERIAL PRIMARY KEY,
    url_id BIGINT REFERENCES urls(id) ON DELETE SET NULL,
    run_id UUID REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    url TEXT NOT NULL,
    method TEXT NOT NULL DEFAULT 'http'
        CHECK(method IN ('http', 'http_stealth', 'selenium', 'playwright', 'api', 'tor', 'scrapy')),
    http_method TEXT DEFAULT 'GET',
    status_code INTEGER,
    success BOOLEAN NOT NULL DEFAULT false,
    response_bytes INTEGER,
    latency_ms INTEGER,
    proxy_used TEXT,
    user_agent TEXT,
    error_type TEXT,  -- timeout, blocked, captcha, connection, parse, etc.
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    fallback_used BOOLEAN DEFAULT false,  -- Did we fallback to browser?
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fetch_logs_url ON fetch_logs(url_id);
CREATE INDEX IF NOT EXISTS idx_fetch_logs_run ON fetch_logs(run_id);
CREATE INDEX IF NOT EXISTS idx_fetch_logs_method ON fetch_logs(method);
CREATE INDEX IF NOT EXISTS idx_fetch_logs_success ON fetch_logs(success);
CREATE INDEX IF NOT EXISTS idx_fetch_logs_date ON fetch_logs(fetched_at DESC);

-- Entities: generic entity storage (products, tenders, drugs, companies, etc.)
CREATE TABLE IF NOT EXISTS entities (
    id BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,  -- product, tender, drug, company, document
    country TEXT NOT NULL,
    source_url_id BIGINT REFERENCES urls(id) ON DELETE SET NULL,
    run_id UUID REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    external_id TEXT,  -- External identifier from source
    entity_hash TEXT,  -- Hash for deduplication
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'archived', 'deleted')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_type, country, entity_hash)
);

CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_country ON entities(country);
CREATE INDEX IF NOT EXISTS idx_entities_hash ON entities(entity_hash);
CREATE INDEX IF NOT EXISTS idx_entities_external ON entities(external_id);
CREATE INDEX IF NOT EXISTS idx_entities_run ON entities(run_id);

-- Entity attributes: flexible key-value storage for entity fields
CREATE TABLE IF NOT EXISTS entity_attributes (
    id BIGSERIAL PRIMARY KEY,
    entity_id BIGINT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    field_value TEXT,
    field_type TEXT DEFAULT 'text' CHECK(field_type IN ('text', 'number', 'date', 'boolean', 'json', 'array')),
    field_order INTEGER DEFAULT 0,  -- For ordering fields in export
    language TEXT DEFAULT 'en',  -- For translated fields
    source TEXT,  -- Where this value came from (scrape, api, manual, etc.)
    confidence REAL,  -- Confidence score for ML-extracted fields
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_entity_attrs_entity ON entity_attributes(entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_attrs_field ON entity_attributes(field_name);
CREATE INDEX IF NOT EXISTS idx_entity_attrs_value ON entity_attributes(field_value) WHERE length(field_value) < 256;

-- Files: tracks downloaded binary files (PDFs, images, etc.)
CREATE TABLE IF NOT EXISTS files (
    id BIGSERIAL PRIMARY KEY,
    url_id BIGINT REFERENCES urls(id) ON DELETE SET NULL,
    entity_id BIGINT REFERENCES entities(id) ON DELETE SET NULL,
    run_id UUID REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    file_type TEXT NOT NULL CHECK(file_type IN ('html', 'pdf', 'xlsx', 'csv', 'image', 'json', 'xml', 'other')),
    file_path TEXT NOT NULL,  -- Relative path from storage root
    file_name TEXT NOT NULL,
    file_size INTEGER,
    checksum TEXT,  -- SHA-256 hash
    mime_type TEXT,
    extraction_status TEXT DEFAULT 'pending'
        CHECK(extraction_status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
    extraction_error TEXT,
    extracted_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_files_url ON files(url_id);
CREATE INDEX IF NOT EXISTS idx_files_entity ON files(entity_id);
CREATE INDEX IF NOT EXISTS idx_files_type ON files(file_type);
CREATE INDEX IF NOT EXISTS idx_files_checksum ON files(checksum);
CREATE INDEX IF NOT EXISTS idx_files_extraction ON files(extraction_status);

-- -----------------------------------------------------------------------------
-- ERROR TRACKING
-- -----------------------------------------------------------------------------

-- Errors: centralized error tracking
CREATE TABLE IF NOT EXISTS errors (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    country TEXT NOT NULL,
    step TEXT,
    url_id BIGINT REFERENCES urls(id) ON DELETE SET NULL,
    error_type TEXT NOT NULL,  -- fetch, parse, validation, export, system
    error_code TEXT,
    error_message TEXT NOT NULL,
    stack_trace TEXT,
    context_json JSONB DEFAULT '{}'::jsonb,
    severity TEXT DEFAULT 'error' CHECK(severity IN ('warning', 'error', 'critical')),
    resolved BOOLEAN DEFAULT false,
    resolved_at TIMESTAMP,
    resolved_by TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_errors_run ON errors(run_id);
CREATE INDEX IF NOT EXISTS idx_errors_country ON errors(country);
CREATE INDEX IF NOT EXISTS idx_errors_type ON errors(error_type);
CREATE INDEX IF NOT EXISTS idx_errors_unresolved ON errors(country, created_at DESC) WHERE NOT resolved;

-- -----------------------------------------------------------------------------
-- HELPER FUNCTIONS
-- -----------------------------------------------------------------------------

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers
DROP TRIGGER IF EXISTS update_pipeline_runs_updated_at ON pipeline_runs;
CREATE TRIGGER update_pipeline_runs_updated_at BEFORE UPDATE ON pipeline_runs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_urls_updated_at ON urls;
CREATE TRIGGER update_urls_updated_at BEFORE UPDATE ON urls
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_entities_updated_at ON entities;
CREATE TRIGGER update_entities_updated_at BEFORE UPDATE ON entities
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_entity_attributes_updated_at ON entity_attributes;
CREATE TRIGGER update_entity_attributes_updated_at BEFORE UPDATE ON entity_attributes
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- -----------------------------------------------------------------------------
-- VIEWS FOR MONITORING
-- -----------------------------------------------------------------------------

-- Active runs view
CREATE OR REPLACE VIEW v_active_runs AS
SELECT 
    pr.run_id,
    pr.country,
    pr.status,
    pr.current_step,
    pr.current_step_num,
    pr.total_steps,
    pr.worker_id,
    pr.started_at,
    pr.last_heartbeat,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - pr.last_heartbeat)) AS heartbeat_age_seconds,
    w.hostname AS worker_hostname,
    w.status AS worker_status
FROM pipeline_runs pr
LEFT JOIN workers w ON pr.worker_id = w.worker_id
WHERE pr.status IN ('queued', 'running');

-- Run statistics view
CREATE OR REPLACE VIEW v_run_stats AS
SELECT 
    country,
    COUNT(*) FILTER (WHERE status = 'completed') AS completed_runs,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
    COUNT(*) FILTER (WHERE status = 'running') AS running_runs,
    COUNT(*) FILTER (WHERE status = 'queued') AS queued_runs,
    AVG(EXTRACT(EPOCH FROM (ended_at - started_at))) FILTER (WHERE ended_at IS NOT NULL) AS avg_duration_seconds,
    MAX(ended_at) AS last_completed_at
FROM pipeline_runs
GROUP BY country;

-- Fetch statistics view
CREATE OR REPLACE VIEW v_fetch_stats AS
SELECT 
    DATE(fetched_at) AS fetch_date,
    method,
    COUNT(*) AS total_fetches,
    COUNT(*) FILTER (WHERE success) AS successful_fetches,
    AVG(latency_ms) AS avg_latency_ms,
    COUNT(*) FILTER (WHERE fallback_used) AS fallback_count,
    SUM(response_bytes) AS total_bytes
FROM fetch_logs
WHERE fetched_at > CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(fetched_at), method
ORDER BY fetch_date DESC, method;

-- Entity counts view
CREATE OR REPLACE VIEW v_entity_counts AS
SELECT 
    country,
    entity_type,
    COUNT(*) AS entity_count,
    MAX(created_at) AS last_created_at
FROM entities
WHERE status = 'active'
GROUP BY country, entity_type
ORDER BY country, entity_type;
