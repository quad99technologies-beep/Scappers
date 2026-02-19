#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PCID Mapping Contract

Standardized interface for PCID mapping (MANDATORY for all countries).
This contract ensures all countries use the shared pcid_mapping table
and prevents drift.

Usage:
    from core.data.pcid_mapping_contract import get_pcid_mapping
    
    # This is the ONLY way to access PCID mappings
    pcid = get_pcid_mapping("Malaysia")
    all_mappings = pcid.get_all()
    oos_products = pcid.get_oos()
    pcid_value = pcid.lookup(company, product, generic, pack_desc)
"""

import logging
from typing import Optional, List, Dict
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# Import existing PCIDMapping class
from core.data.pcid_mapping import PCIDMapping as _PCIDMapping


class PCIDMappingInterface(ABC):
    """Standardized interface for PCID mapping (MANDATORY for all countries)."""
    
    @abstractmethod
    def get_all(self) -> List[Dict]:
        """Get all PCID mappings for this country."""
        pass
    
    @abstractmethod
    def lookup(self, company: str, product: str, 
               generic: str = "", pack_desc: str = "") -> Optional[str]:
        """Lookup PCID by product details."""
        pass
    
    @abstractmethod
    def get_oos(self) -> List[Dict]:
        """Get OOS (Out of Scope) products."""
        pass
    
    @abstractmethod
    def is_oos_product(self, company: str, product: str) -> bool:
        """Check if product is OOS."""
        pass


class SharedPCIDMapping(PCIDMappingInterface):
    """
    Standard implementation using shared pcid_mapping table.
    
    This is the ONLY allowed implementation. All countries must use this.
    """
    
    def __init__(self, country: str, db=None):
        """
        Initialize PCID mapping for a country.
        
        Args:
            country: Country name (e.g., "Malaysia", "Argentina")
            db: Optional database connection (auto-connects if None)
        """
        self.country = country
        self._mapping = _PCIDMapping(country, db)
    
    def get_all(self) -> List[Dict]:
        """Get all PCID mappings for this country from shared table."""
        rows = self._mapping.get_all()
        return [
            {
                "pcid": row.pcid,
                "company": row.company,
                "local_product_name": row.local_product_name,
                "generic_name": row.generic_name,
                "local_pack_description": row.local_pack_description,
                "local_pack_code": row.local_pack_code,
            }
            for row in rows
        ]
    
    def lookup(self, company: str, product: str, 
               generic: str = "", pack_desc: str = "") -> Optional[str]:
        """Lookup PCID using normalized matching."""
        return self._mapping.lookup(company, product, generic, pack_desc)
    
    def get_oos(self) -> List[Dict]:
        """Get OOS products (PCID = 'OOS')."""
        rows = self._mapping.get_oos()
        return [
            {
                "pcid": row.pcid,
                "company": row.company,
                "local_product_name": row.local_product_name,
                "generic_name": row.generic_name,
                "local_pack_description": row.local_pack_description,
                "local_pack_code": row.local_pack_code,
            }
            for row in rows
        ]
    
    def is_oos_product(self, company: str, product: str) -> bool:
        """Check if product is OOS."""
        return self._mapping.is_oos_product(company, product)


# MANDATORY: All countries MUST use this function
def get_pcid_mapping(country: str) -> PCIDMappingInterface:
    """
    Get PCID mapping instance for a country.
    
    This is the ONLY way to access PCID mappings.
    All countries must use this function.
    
    Args:
        country: Country name (e.g., "Malaysia", "Argentina")
        
    Returns:
        PCIDMappingInterface instance
        
    Example:
        pcid = get_pcid_mapping("Malaysia")
        all_mappings = pcid.get_all()
        oos_products = pcid.get_oos()
    """
    return SharedPCIDMapping(country)
