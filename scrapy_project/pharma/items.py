"""
Base Scrapy item definitions for the pharma scraper platform.

Each country spider defines its own Item subclass with country-specific fields.
All items share common metadata fields for auditing.
"""

import scrapy


class PharmaItem(scrapy.Item):
    """Base item with common audit fields. Country spiders extend this."""

    # Audit metadata (auto-populated by pipeline)
    run_id = scrapy.Field()
    source_url = scrapy.Field()
    item_hash = scrapy.Field()
    scraped_at = scrapy.Field()

    # Override in subclass with country-specific fields


class IndiaDetailItem(PharmaItem):
    """India NPPA medicine detail."""
    formulation = scrapy.Field()
    brand_name = scrapy.Field()
    manufacturer = scrapy.Field()
    pack_size = scrapy.Field()
    mrp = scrapy.Field()
    ceiling_price = scrapy.Field()


class MalaysiaReimbursableItem(PharmaItem):
    """Malaysia fully reimbursable drug."""
    product_name = scrapy.Field()
    category = scrapy.Field()
    generic_name = scrapy.Field()
    dosage_form = scrapy.Field()
    strength = scrapy.Field()


class OntarioProductItem(PharmaItem):
    """Canada Ontario drug product detail."""
    din = scrapy.Field()
    product_name = scrapy.Field()
    manufacturer = scrapy.Field()
    strength = scrapy.Field()
    dosage_form = scrapy.Field()
    route = scrapy.Field()
    local_pack_code = scrapy.Field()


class NetherlandsReimbursementItem(PharmaItem):
    """Netherlands reimbursement detail."""
    product_name = scrapy.Field()
    active_substance = scrapy.Field()
    dosage_form = scrapy.Field()
    strength = scrapy.Field()
    reimbursement_amount = scrapy.Field()
    manufacturer = scrapy.Field()


class ArgentinaProductItem(PharmaItem):
    """Argentina Alfabeta API product."""
    product_name = scrapy.Field()
    laboratory = scrapy.Field()
    presentation = scrapy.Field()
    active_ingredient = scrapy.Field()
    price = scrapy.Field()
