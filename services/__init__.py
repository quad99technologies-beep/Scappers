"""
Common utilities for the scraping platform.

This module provides centralized access to:
- Database operations (db.py)
- Unified fetcher (fetcher.py)
- Response validation (validator.py)
- Worker operations (worker.py)
- Watchdog (watchdog.py)
- GUI control panel (gui.py)

Quick Start:
    # Fetch a page with automatic method selection and fallback
    from services import fetch
    result = fetch("https://example.com", country="Malaysia")
    
    # Use the unified database layer
    from services import register_url, insert_entity, insert_attributes
    url_id = register_url("https://example.com/product/1", "Malaysia")
    entity_id = insert_entity("product", "Malaysia", source_url_id=url_id)
    insert_attributes(entity_id, {"name": "Product 1", "price": "10.00"})
    
    # Run a distributed worker
    # python services/worker.py
    
    # Run the GUI
    # streamlit run services/gui.py
"""

# Database operations
from .db import (
    # Connection helpers
    get_platform_db,
    get_country_db,
    get_cursor,
    get_connection,
    ensure_platform_schema,
    # Pipeline runs
    create_pipeline_run,
    claim_next_run,
    update_run_status,
    update_run_step,
    heartbeat,
    get_latest_command,
    acknowledge_command,
    issue_command,
    get_stale_runs,
    requeue_stale_runs,
    # Worker registry
    register_worker,
    update_worker_heartbeat,
    unregister_worker,
    generate_worker_id,
    # URL operations
    register_url,
    upsert_url,
    get_url_id,
    update_url_status,
    get_pending_urls,
    # Entity operations
    insert_entity,
    insert_attributes,
    insert_attribute,
    get_entity,
    compute_entity_hash,
    # Fetch logging
    log_fetch,
    # Error logging
    log_error,
    # File operations
    register_file,
    update_file_extraction,
)

# Unified fetcher
from .fetcher import (
    fetch,
    fetch_html,
    fetch_bytes,
    FetchResult,
    FetchMethod,
    validate_response,
    get_random_user_agent,
    cleanup as cleanup_fetcher,
)

# Validation
from .validator import (
    validate_html,
    quick_validate,
    ValidationResult,
    detect_cloudflare,
    detect_captcha,
    detect_block,
    is_html,
    has_element,
    get_title,
)

# Worker
from .worker import (
    Worker,
    register_pipeline,
    get_available_countries,
    create_subprocess_runner,
    auto_register_pipelines,
)


__all__ = [
    # Database - Connection
    'get_platform_db',
    'get_country_db',
    'get_cursor',
    'get_connection',
    'ensure_platform_schema',
    
    # Database - Pipeline Runs
    'create_pipeline_run',
    'claim_next_run',
    'update_run_status',
    'update_run_step',
    'heartbeat',
    'get_latest_command',
    'acknowledge_command',
    'issue_command',
    'get_stale_runs',
    'requeue_stale_runs',
    
    # Database - Workers
    'register_worker',
    'update_worker_heartbeat',
    'unregister_worker',
    'generate_worker_id',
    
    # Database - URLs
    'register_url',
    'upsert_url',
    'get_url_id',
    'update_url_status',
    'get_pending_urls',
    
    # Database - Entities
    'insert_entity',
    'insert_attributes',
    'insert_attribute',
    'get_entity',
    'compute_entity_hash',
    
    # Database - Logging
    'log_fetch',
    'log_error',
    
    # Database - Files
    'register_file',
    'update_file_extraction',
    
    # Fetcher
    'fetch',
    'fetch_html',
    'fetch_bytes',
    'FetchResult',
    'FetchMethod',
    'validate_response',
    'get_random_user_agent',
    'cleanup_fetcher',
    
    # Validation
    'validate_html',
    'quick_validate',
    'ValidationResult',
    'detect_cloudflare',
    'detect_captcha',
    'detect_block',
    'is_html',
    'has_element',
    'get_title',
    
    # Worker
    'Worker',
    'register_pipeline',
    'get_available_countries',
    'create_subprocess_runner',
    'auto_register_pipelines',
]
