"""
PCID Mapping Module - Single Source of Truth

All scrapers must use this module to read PCID mapping data.
The pcid_mapping database table is the only source of truth.

Usage:
    from core.data.pcid_mapping import PCIDMapping
    
    pcid = PCIDMapping("Argentina")
    mappings = pcid.get_all()  # Get all PCID mappings for country
    oos_products = pcid.get_oos()  # Get OOS products
    pcid_value = pcid.lookup(company, product, generic, pack_desc)  # Lookup PCID
"""

import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class PCIDMappingRow:
    """Represents a single PCID mapping row"""
    pcid: str
    company: str
    local_product_name: str
    generic_name: str
    local_pack_description: str
    local_pack_code: Optional[str] = None
    atc_code: Optional[str] = None
    strength: Optional[str] = None
    fill_size: Optional[str] = None
    
    def is_oos(self) -> bool:
        """Check if this is an OOS (Out of Scope) product"""
        return self.pcid.strip().upper() == "OOS"


class PCIDMapping:
    """
    PCID Mapping manager - reads from pcid_mapping database table only.
    This ensures all scrapers use the same data source.
    """
    
    def __init__(self, country: str, db=None):
        """
        Initialize PCID mapping for a country.
        
        Args:
            country: Country name (e.g., "Argentina", "Malaysia")
            db: Optional database connection (will create one if not provided)
        """
        self.country = country
        self._db = db
        self._cache: Optional[List[PCIDMappingRow]] = None
    
    def _get_db(self):
        """Get database connection"""
        if self._db is None:
            from core.db.connection import CountryDB
            self._db = CountryDB(self.country)
        return self._db
    
    def get_all(self, use_cache: bool = False) -> List[PCIDMappingRow]:
        """
        Get all PCID mappings for this country.
        
        Args:
            use_cache: If True, cache results for subsequent calls
            
        Returns:
            List of PCIDMappingRow objects
        """
        if use_cache and self._cache is not None:
            return self._cache
        
        db = self._get_db()
        rows = []
        
        with db.cursor(dict_cursor=True) as cur:
            cur.execute("""
                SELECT pcid, company, local_product_name, generic_name, 
                       local_pack_description, local_pack_code,
                       atc_code, strength, fill_size
                FROM pcid_mapping
                WHERE source_country = %s
            """, (self.country,))
            
            for row in cur.fetchall():
                rows.append(PCIDMappingRow(
                    pcid=row.get("pcid", ""),
                    company=row.get("company", ""),
                    local_product_name=row.get("local_product_name", ""),
                    generic_name=row.get("generic_name", ""),
                    local_pack_description=row.get("local_pack_description", ""),
                    local_pack_code=row.get("local_pack_code"),
                    atc_code=row.get("atc_code"),
                    strength=row.get("strength"),
                    fill_size=row.get("fill_size"),
                ))
        
        logger.info(f"[PCID] Loaded {len(rows)} mappings for {self.country}")
        
        if use_cache:
            self._cache = rows
        
        return rows
    
    def get_oos(self) -> List[PCIDMappingRow]:
        """Get all OOS (Out of Scope) products"""
        all_mappings = self.get_all()
        return [m for m in all_mappings if m.is_oos()]
    
    def get_valid(self) -> List[PCIDMappingRow]:
        """Get all valid PCID mappings (excluding OOS)"""
        all_mappings = self.get_all()
        return [m for m in all_mappings if not m.is_oos()]
    
    def lookup(self, company: str, product: str, 
               generic: str = "", pack_desc: str = "") -> Optional[str]:
        """
        Lookup PCID by product details.
        
        Args:
            company: Company name
            product: Product name
            generic: Generic name (optional)
            pack_desc: Pack description (optional)
            
        Returns:
            PCID value or None if not found
        """
        all_mappings = self.get_all()
        
        # Normalize inputs
        comp_norm = self._normalize(company)
        prod_norm = self._normalize(product)
        gen_norm = self._normalize(generic)
        pack_norm = self._normalize(pack_desc)
        
        # Try exact match first
        for m in all_mappings:
            if (self._normalize(m.company) == comp_norm and 
                self._normalize(m.local_product_name) == prod_norm):
                # If generic and pack_desc provided, match them too
                if generic and pack_desc:
                    if (self._normalize(m.generic_name) == gen_norm and
                        self._normalize(m.local_pack_description) == pack_norm):
                        return m.pcid
                elif generic:
                    if self._normalize(m.generic_name) == gen_norm:
                        return m.pcid
                elif pack_desc:
                    if self._normalize(m.local_pack_description) == pack_norm:
                        return m.pcid
                else:
                    return m.pcid
        
        return None
    
    def is_oos_product(self, company: str, product: str) -> bool:
        """Check if a product is OOS"""
        pcid = self.lookup(company, product)
        return pcid is not None and pcid.upper() == "OOS"
    
    def get_oos_dict(self) -> Dict[Tuple[str, str], PCIDMappingRow]:
        """
        Get OOS products as a dictionary for quick lookup.
        Key: (product_lower, company_lower)
        """
        oos_products = {}
        for m in self.get_oos():
            key = (m.local_product_name.lower().strip(), m.company.lower().strip())
            oos_products[key] = m
        return oos_products
    
    @staticmethod
    def _normalize(s: str) -> str:
        """Normalize string for comparison"""
        return str(s).strip().lower() if s else ""
    
    def clear_cache(self):
        """Clear the cache if use_cache was True"""
        self._cache = None


def reload_pcid_mapping_from_csv(country: str, csv_path: str, db=None) -> int:
    """
    Reload PCID mapping from CSV file into database.
    This is called by the GUI when user uploads a new CSV.
    
    Args:
        country: Country name
        csv_path: Path to CSV file
        db: Optional database connection
        
    Returns:
        Number of rows loaded
    """
    import csv
    from pathlib import Path
    
    if db is None:
        from core.db.connection import CountryDB
        db = CountryDB(country)
    
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Clear existing mappings for this country
    with db.cursor() as cur:
        cur.execute("DELETE FROM pcid_mapping WHERE source_country = %s", (country,))
    
    rows_loaded = 0
    
    # Read CSV and insert into database
    with open(csv_file, 'r', encoding='utf-8-sig', newline='') as f:
        # 1. Normalize headers (strip whitespace and lower case)
        reader = csv.reader(f)
        try:
            original_headers = next(reader)
        except StopIteration:
            return 0  # Empty file

        # Map lower-case stripped header -> original header
        # This allows us to look up by lower case name
        header_map = {h.strip().lower(): h.strip() for h in original_headers}
        
        # Use content-based headers for the DictReader
        # DictReader uses the *next* row if fieldnames is None, but we already consumed it.
        # So we pass the *original* headers (stripped) as fieldnames so it maps correctly to the subsequent rows.
        clean_headers = [h.strip() for h in original_headers]
        dict_reader = csv.DictReader(f, fieldnames=clean_headers)

        def get_val(row_dict, *keys):
            """Try multiple case-insensitive keys"""
            for key in keys:
                # 1. Try exact match from normalized headers
                if key in row_dict:
                    val = row_dict[key]
                    if val and val.strip():
                        return val.strip()
                
                # 2. Try looking up via lower-case map
                key_lower = key.lower()
                if key_lower in header_map:
                    real_key = header_map[key_lower]
                    if real_key in row_dict:
                        val = row_dict[real_key]
                        if val and val.strip():
                            return val.strip()
            return None

        with db.cursor() as cur:
            for row in dict_reader:
                # Map CSV columns to database columns with fuzzy matching
                pcid = get_val(row, 'PCID', 'pcid') or ""
                company = get_val(row, 'Company', 'company')
                product = get_val(row, 'Local Product Name', 'local product name', 'product', 'trade name') or ""
                generic = get_val(row, 'Generic Name', 'generic name', 'inn') or ""
                
                # Description often varies
                pack_desc = get_val(row, 'Local Pack Description', 'local pack description', 'dosage form', 'presentation') or ""
                
                # New columns for North Macedonia
                pack_code = get_val(row, 'LOCAL_PACK_CODE', 'Local Pack Code', 'local_pack_code')
                atc_code = get_val(row, 'WHO ATC Code', 'ATC Code', 'atc code', 'atc')
                strength = get_val(row, 'Strength', 'Strength Size', 'strength size')
                fill_size = get_val(row, 'Fill Size', 'fill size')
                
                # Check for minimum data required to be useful
                if product or pcid:
                    cur.execute("""
                        INSERT INTO pcid_mapping 
                        (pcid, company, local_product_name, generic_name, 
                         local_pack_description, local_pack_code, source_country,
                         atc_code, strength, fill_size)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        pcid, 
                        company,
                        product, 
                        generic, 
                        pack_desc, 
                        pack_code, 
                        country,
                        atc_code, 
                        strength, 
                        fill_size
                    ))
                    rows_loaded += 1
    
    logger.info(f"[PCID] Reloaded {rows_loaded} mappings for {country} from {csv_path}")
    return rows_loaded
