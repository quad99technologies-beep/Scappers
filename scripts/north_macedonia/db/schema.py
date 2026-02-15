#!/usr/bin/env python3
"""
North Macedonia database schema.
Defines all nm_* tables for the scraper.
"""

def apply_schema(db):
    """
    Apply North Macedonia schema to database.
    
    Tables:
    - nm_urls: Collected detail URLs (replaces CSV)
    - nm_drug_register: Drug registration data
    - nm_pcid_mappings: PCID mapping results
    - nm_final_output: EVERSANA format output
    - nm_step_progress: Sub-step tracking
    - nm_export_reports: Export metadata
    - nm_errors: Error tracking
    - nm_validation_results: Data validation results
    - nm_statistics: Run statistics
    """
    
    with db.cursor() as cur:
        # URLs table (replaces north_macedonia_detail_urls.csv)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nm_urls (
                id SERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                detail_url TEXT NOT NULL,
                page_num INTEGER,
                status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'scraped', 'failed', 'skipped')),
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                scraped_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(run_id, detail_url)
            )
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_urls_run ON nm_urls(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_urls_status ON nm_urls(run_id, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_urls_page ON nm_urls(run_id, page_num)")
        
        # Drug register table (replaces north_macedonia_drug_register.csv)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nm_drug_register (
                id SERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                url_id INTEGER REFERENCES nm_urls(id),
                detail_url TEXT,
                
                -- Product identification
                local_product_name TEXT,
                local_pack_code TEXT,
                generic_name TEXT,
                who_atc_code TEXT,
                
                -- Product details
                formulation TEXT,
                strength_size TEXT,
                fill_size TEXT,
                customized_1 TEXT,
                
                -- Company & dates
                marketing_authority_company_name TEXT,
                effective_start_date TEXT,
                effective_end_date TEXT,
                
                -- Pricing
                public_with_vat_price TEXT,
                pharmacy_purchase_price TEXT,
                
                -- Description
                local_pack_description TEXT,
                
                -- Reimbursement (standard values)
                reimbursable_status TEXT DEFAULT 'PARTIALLY REIMBURSABLE',
                reimbursable_rate TEXT DEFAULT '80.00%',
                reimbursable_notes TEXT DEFAULT '',
                copayment_value TEXT DEFAULT '',
                copayment_percent TEXT DEFAULT '20.00%',
                margin_rule TEXT DEFAULT '650 PPP & PPI Listed',
                vat_percent TEXT DEFAULT '5',
                
                -- Metadata
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(run_id, detail_url)
            )
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_drug_reg_run ON nm_drug_register(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_drug_reg_url ON nm_drug_register(url_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_drug_reg_product ON nm_drug_register(local_product_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_drug_reg_atc ON nm_drug_register(who_atc_code)")

        # Drop legacy nm_max_prices table if it exists (removed from pipeline)
        cur.execute("DROP TABLE IF EXISTS nm_max_prices CASCADE")

        # PCID mappings table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nm_pcid_mappings (
                id SERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                drug_register_id INTEGER REFERENCES nm_drug_register(id),
                
                -- PCID mapping
                pcid TEXT,
                match_type TEXT CHECK(match_type IN ('exact', 'fuzzy', 'manual', 'not_found')),
                match_score REAL,
                
                -- Product details
                local_product_name TEXT,
                generic_name TEXT,
                manufacturer TEXT,
                local_pack_code TEXT,
                local_pack_description TEXT,
                
                -- Metadata
                country TEXT DEFAULT 'NORTH MACEDONIA',
                region TEXT DEFAULT 'EUROPE',
                currency TEXT DEFAULT 'MKD',
                source TEXT DEFAULT 'PRICENTRIC',
                mapped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(run_id, drug_register_id)
            )
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_pcid_run ON nm_pcid_mappings(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_pcid_code ON nm_pcid_mappings(pcid)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_pcid_drug ON nm_pcid_mappings(drug_register_id)")
        
        # Final output table (EVERSANA format)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nm_final_output (
                id SERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                drug_register_id INTEGER REFERENCES nm_drug_register(id),
                pcid_mapping_id INTEGER REFERENCES nm_pcid_mappings(id),
                
                -- EVERSANA fields
                pcid TEXT,
                country TEXT DEFAULT 'NORTH MACEDONIA',
                company TEXT,
                local_product_name TEXT,
                generic_name TEXT,
                description TEXT,
                strength TEXT,
                dosage_form TEXT,
                pack_size TEXT,
                
                -- Pricing
                public_price REAL,
                pharmacy_price REAL,
                currency TEXT DEFAULT 'MKD',
                
                -- Dates
                effective_start_date TEXT,
                effective_end_date TEXT,
                
                -- Codes
                local_pack_code TEXT,
                atc_code TEXT,
                
                -- Reimbursement
                reimbursable_status TEXT,
                reimbursable_rate TEXT,
                copayment_percent TEXT,
                margin_rule TEXT,
                vat_percent TEXT,
                
                -- Source
                marketing_authorisation_holder TEXT,
                source_url TEXT,
                source_type TEXT CHECK(source_type IN ('drug_register', 'merged')),
                
                -- Metadata
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(run_id, drug_register_id)
            )
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_final_run ON nm_final_output(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_final_pcid ON nm_final_output(pcid)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_final_product ON nm_final_output(local_product_name)")
        
        # Step progress table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nm_step_progress (
                id SERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                step_number INTEGER NOT NULL,
                step_name TEXT NOT NULL,
                progress_key TEXT NOT NULL,
                status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped')),
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                UNIQUE(run_id, step_number, progress_key)
            )
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_progress_run_step ON nm_step_progress(run_id, step_number)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_progress_status ON nm_step_progress(status)")
        
        # Export reports table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nm_export_reports (
                id SERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                report_type TEXT NOT NULL,
                file_path TEXT,
                row_count INTEGER,
                export_format TEXT DEFAULT 'db',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_export_run ON nm_export_reports(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_export_type ON nm_export_reports(report_type)")
        
        # Errors table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nm_errors (
                id SERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                error_type TEXT,
                error_message TEXT NOT NULL,
                context JSONB,
                step_number INTEGER,
                step_name TEXT,
                url TEXT,
                traceback TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_errors_run ON nm_errors(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_errors_step ON nm_errors(step_number)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_errors_type ON nm_errors(error_type)")
        
        # Validation results table (NEW)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nm_validation_results (
                id SERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                validation_type TEXT NOT NULL,
                table_name TEXT,
                record_id INTEGER,
                field_name TEXT,
                validation_rule TEXT,
                status TEXT CHECK(status IN ('pass', 'fail', 'warning')),
                message TEXT,
                severity TEXT CHECK(severity IN ('critical', 'high', 'medium', 'low', 'info')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_validation_run ON nm_validation_results(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_validation_status ON nm_validation_results(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_validation_type ON nm_validation_results(validation_type)")
        
        # Statistics table (NEW)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nm_statistics (
                id SERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                step_number INTEGER,
                metric_name TEXT NOT NULL,
                metric_value NUMERIC,
                metric_type TEXT CHECK(metric_type IN ('count', 'percentage', 'duration', 'rate', 'size')),
                category TEXT,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_stats_run ON nm_statistics(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_stats_step ON nm_statistics(step_number)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_stats_metric ON nm_statistics(metric_name)")

        # Input dictionary table (MK→EN translation dictionary)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nm_input_dictionary (
                id SERIAL PRIMARY KEY,
                source_term TEXT NOT NULL,
                translated_term TEXT,
                language_from TEXT DEFAULT 'mk',
                language_to TEXT DEFAULT 'en',
                category TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_term, language_from, language_to)
            )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_dict_source ON nm_input_dictionary(source_term)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_dict_lang ON nm_input_dictionary(language_from, language_to)")

        # Translation Cache table (replaces JSON file cache)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nm_translation_cache (
                id SERIAL PRIMARY KEY,
                source_text TEXT NOT NULL UNIQUE,
                translated_text TEXT NOT NULL,
                source_language TEXT DEFAULT 'mk',
                target_language TEXT DEFAULT 'en',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_trans_cache_source ON nm_translation_cache(source_text)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_trans_cache_lookup ON nm_translation_cache(source_language, target_language, source_text)")

    db.commit()
    print("[SCHEMA] North Macedonia schema applied successfully")


# Alias for backward compatibility
def apply_north_macedonia_schema(db):
    """Alias for apply_schema() for backward compatibility."""
    return apply_schema(db)
