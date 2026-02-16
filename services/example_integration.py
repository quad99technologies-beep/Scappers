#!/usr/bin/env python3
"""
Example Integration Script

This script demonstrates how to integrate the new platform infrastructure
with existing country scrapers.

It shows:
1. Using the unified fetcher
2. Storing data in generic entities
3. Logging fetch operations
4. Running as part of the distributed worker system
"""

import os
import sys
import logging
from typing import Optional, Callable

# Add parent directories to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)


# =============================================================================
# Example 1: Using the Unified Fetcher
# =============================================================================

def example_fetch():
    """Demonstrate using the unified fetcher."""
    from services import fetch, fetch_html, FetchMethod
    
    print("\n" + "="*60)
    print("Example 1: Unified Fetcher")
    print("="*60)
    
    # Simple fetch with automatic method selection
    result = fetch(
        url="https://httpbin.org/html",
        country="Malaysia",  # Method selection based on country
        validate=True,
        min_length=100
    )
    
    print(f"URL: {result.url}")
    print(f"Success: {result.success}")
    print(f"Method used: {result.method_used.value}")
    print(f"Status code: {result.status_code}")
    print(f"Latency: {result.latency_ms}ms")
    print(f"Content length: {len(result.content) if result.content else 0}")
    
    if not result.success:
        print(f"Error: {result.error_type} - {result.error_message}")
    
    # Convenience function
    html = fetch_html("https://httpbin.org/html", country="India")
    print(f"\nConvenience fetch: {len(html) if html else 0} bytes")


# =============================================================================
# Example 2: Using the Generic Data Model
# =============================================================================

def example_data_model():
    """Demonstrate using the generic entity/attribute model."""
    from services import (
        ensure_platform_schema,
        register_url,
        insert_entity,
        insert_attributes,
        get_entity,
    )
    
    print("\n" + "="*60)
    print("Example 2: Generic Data Model")
    print("="*60)
    
    # Ensure schema exists
    try:
        ensure_platform_schema()
        print("Schema initialized")
    except Exception as e:
        print(f"Schema initialization skipped: {e}")
        return
    
    # Register a URL
    url = "https://example.com/product/12345"
    url_id = register_url(
        url=url,
        country="Malaysia",
        source="seed",
        entity_type="product"
    )
    print(f"Registered URL: {url_id}")
    
    # Create an entity
    entity_id = insert_entity(
        entity_type="product",
        country="Malaysia",
        source_url_id=url_id,
        external_id="PROD-12345",
        data={"name": "Paracetamol 500mg"}
    )
    print(f"Created entity: {entity_id}")
    
    # Add attributes
    attributes = {
        "name": "Paracetamol 500mg",
        "brand": "Generic",
        "manufacturer": "PharmaCorp",
        "price": 10.50,
        "pack_size": "100 tablets",
        "registration_number": "REG-12345",
        "status": "Active",
    }
    
    count = insert_attributes(entity_id, attributes, source="scrape")
    print(f"Added {count} attributes")
    
    # Retrieve entity
    entity = get_entity(entity_id)
    print(f"Retrieved entity: {entity['entity_type']} - {entity['external_id']}")
    print(f"Attributes: {entity['attributes']}")


# =============================================================================
# Example 3: Migrating a Scraper Function
# =============================================================================

def example_scraper_migration():
    """
    Demonstrate how to migrate an existing scraper function to use
    the new platform infrastructure.
    
    BEFORE:
        import requests
        response = requests.get(url)
        html = response.text
        # ... parse and save to CSV
    
    AFTER:
        from services import fetch, register_url, insert_entity, insert_attributes
        result = fetch(url, country="Malaysia")
        if result.success:
            # ... parse
            entity_id = insert_entity("product", "Malaysia", ...)
            insert_attributes(entity_id, parsed_data)
    """
    from services import (
        fetch,
        register_url,
        insert_entity,
        insert_attributes,
        log_error,
    )
    from bs4 import BeautifulSoup
    
    print("\n" + "="*60)
    print("Example 3: Scraper Migration Pattern")
    print("="*60)
    
    # Example product URLs
    urls = [
        "https://httpbin.org/html",
        "https://httpbin.org/robots.txt",  # Will fail validation (not HTML)
    ]
    
    country = "Malaysia"
    
    for url in urls:
        print(f"\nProcessing: {url}")
        
        # Step 1: Fetch with automatic method selection
        result = fetch(
            url=url,
            country=country,
            validate=True,
            min_length=100,
            log_to_db=False  # Set to True in production
        )
        
        if not result.success:
            print(f"  FAILED: {result.error_type} - {result.error_message}")
            # In production, log error to DB
            # log_error(country, result.error_type, result.error_message, url_id=url_id)
            continue
        
        print(f"  Fetched: {result.method_used.value}, {len(result.content)} bytes")
        
        # Step 2: Parse (example)
        try:
            soup = BeautifulSoup(result.content, 'html.parser')
            title = soup.find('title')
            title_text = title.get_text(strip=True) if title else "No title"
            print(f"  Title: {title_text}")
        except Exception as e:
            print(f"  Parse error: {e}")
            continue
        
        # Step 3: Store in generic model (commented out to avoid DB dependency)
        # url_id = register_url(url, country, source="example")
        # entity_id = insert_entity("product", country, source_url_id=url_id)
        # insert_attributes(entity_id, {"title": title_text, "url": url})
        print("  Would store entity in DB")


# =============================================================================
# Example 4: Creating a Worker-Compatible Pipeline
# =============================================================================

def example_worker_pipeline():
    """
    Demonstrate how to create a pipeline that works with the distributed worker.
    
    The pipeline must be a function that:
    1. Takes run_id, start_step, and check_stop callback
    2. Executes steps sequentially
    3. Checks for stop commands between steps
    4. Updates step progress
    """
    from services import update_run_step
    
    print("\n" + "="*60)
    print("Example 4: Worker-Compatible Pipeline")
    print("="*60)
    
    def my_pipeline(run_id: str, start_step: int, check_stop: Callable[[], bool]):
        """
        Example pipeline that works with the distributed worker.
        
        Args:
            run_id: Pipeline run ID from database
            start_step: Step to start from (for resume)
            check_stop: Callback that returns True if stop requested
        """
        steps = [
            ("backup", lambda: print("  Backing up previous data...")),
            ("collect_urls", lambda: print("  Collecting product URLs...")),
            ("scrape_details", lambda: print("  Scraping product details...")),
            ("consolidate", lambda: print("  Consolidating results...")),
            ("export", lambda: print("  Exporting to CSV/DB...")),
        ]
        
        for step_num, (step_name, step_func) in enumerate(steps):
            # Skip completed steps (for resume)
            if step_num < start_step:
                print(f"  Skipping step {step_num}: {step_name}")
                continue
            
            # Check for stop command
            if check_stop():
                print(f"  Stop requested at step {step_num}")
                return
            
            # Update progress in database (commented out to avoid DB dependency)
            # update_run_step(run_id, step_num, step_name)
            
            print(f"  Running step {step_num}: {step_name}")
            step_func()
        
        print("  Pipeline completed!")
    
    # Simulate running the pipeline
    print("\nSimulating pipeline execution:")
    my_pipeline(
        run_id="test-run-id",
        start_step=0,
        check_stop=lambda: False  # Never stop in simulation
    )
    
    print("\nSimulating resume from step 2:")
    my_pipeline(
        run_id="test-run-id",
        start_step=2,
        check_stop=lambda: False
    )


# =============================================================================
# Example 5: Country-Specific Fetch Configuration
# =============================================================================

def example_country_config():
    """
    Demonstrate how to configure fetch behavior per country.
    """
    from services.fetcher import (
        get_fetch_order,
        COUNTRY_FETCH_ORDER,
        TOR_COUNTRIES,
        API_COUNTRIES,
    )
    
    print("\n" + "="*60)
    print("Example 5: Country-Specific Configuration")
    print("="*60)
    
    print("\nFetch order by country:")
    for country, methods in COUNTRY_FETCH_ORDER.items():
        method_names = [m.value for m in methods]
        print(f"  {country}: {' -> '.join(method_names)}")
    
    print(f"\nTOR-required countries: {TOR_COUNTRIES}")
    print(f"API-enabled countries: {API_COUNTRIES}")
    
    print("\nTo add a new country, modify fetcher.py:")
    print("""
    # In COUNTRY_FETCH_ORDER:
    "NewCountry": [FetchMethod.HTTP_STEALTH, FetchMethod.PLAYWRIGHT, FetchMethod.SELENIUM],
    """)


# =============================================================================
# Main
# =============================================================================

def main():
    print("="*60)
    print("Platform Integration Examples")
    print("="*60)
    
    # Run examples
    example_fetch()
    # example_data_model()  # Requires database connection
    example_scraper_migration()
    example_worker_pipeline()
    example_country_config()
    
    print("\n" + "="*60)
    print("Examples completed!")
    print("="*60)
    print("""
Next Steps:
1. Set up PostgreSQL and configure environment variables
2. Run schema: psql -f sql/schemas/postgres/platform.sql
3. Start a worker: python services/worker.py
4. Start the GUI: streamlit run services/gui.py
5. Migrate your scrapers one by one using the patterns shown
    """)


if __name__ == "__main__":
    main()
