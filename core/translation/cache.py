#!/usr/bin/env python3
"""
Unified Translation Cache

Provides consistent translation caching across all scrapers using PostgreSQL.
Migrates away from JSON file caches and in-memory-only caches.

Supports two schema variants:
- Legacy: source_text as UNIQUE key (Argentina, Russia, Belarus)
- Unified: source_hash as UNIQUE key with source_text stored

Usage:
    from core.translation import TranslationCache
    
    cache = TranslationCache("argentina")  # or "ar" prefix
    
    # Get cached translation
    result = cache.get("hola", "es", "en")
    
    # Set translation
    cache.set("hola", "hello", "es", "en")
    
    # Bulk operations
    cache.get_many([("hola", "es", "en"), ("mundo", "es", "en")])
    cache.set_many({("hola", "es", "en"): "hello", ("mundo", "es", "en"): "world"})
"""

import hashlib
import logging
from typing import Optional, Dict, List, Tuple, Any
from datetime import datetime

from core.db.connection import CountryDB

logger = logging.getLogger(__name__)

# Scraper name to table prefix mapping
# Supports: lowercase_snake, CamelCase, and Title Case
PREFIX_MAP = {
    # Argentina
    "argentina": "ar",
    "Argentina": "ar",
    # Russia
    "russia": "ru", 
    "Russia": "ru",
    # Belarus
    "belarus": "by",
    "Belarus": "by",
    # North Macedonia
    "north_macedonia": "nm",
    "NorthMacedonia": "nm",
    "North Macedonia": "nm",
    # Malaysia
    "malaysia": "my",
    "Malaysia": "my",
    # Canada Ontario
    "canada_ontario": "ca_on",
    "CanadaOntario": "ca_on",
    "Canada Ontario": "ca_on",
    # Canada Quebec
    "canada_quebec": "ca_qc",
    "CanadaQuebec": "ca_qc",
    "Canada Quebec": "ca_qc",
    # Netherlands
    "netherlands": "nl",
    "Netherlands": "nl",
    # Taiwan
    "taiwan": "tw",
    "Taiwan": "tw",
    # Tender Chile
    "tender_chile": "tc",
    "TenderChile": "tc",
    "Tender_Chile": "tc",
    "Tender-Chile": "tc",
    # Tender Brazil
    "tender_brazil": "tb",
    "TenderBrazil": "tb",
    "Tender_Brazil": "tb",
    "Tender - Brazil": "tb",
    # India
    "india": "in",
    "India": "in",
}

# Scrapers using legacy schema (source_text as unique key)
LEGACY_SCHEMA_SCRAPERS = {"ar", "ru", "by"}


class TranslationCache:
    """
    Unified translation cache using PostgreSQL.
    
    Each scraper gets its own {prefix}_translation_cache table.
    Automatically detects and works with both legacy and unified schemas.
    """
    
    def __init__(self, scraper_name: str):
        """
        Initialize translation cache for a scraper.
        
        Args:
            scraper_name: Name of the scraper (e.g., "argentina", "russia")
                         Can be full name or prefix (e.g., "ar")
        """
        self.scraper_name = scraper_name.lower().replace(" ", "_").replace("-", "_")
        self.prefix = self._get_prefix(self.scraper_name)
        self.table_name = f"{self.prefix}_translation_cache"
        self._is_legacy = self.prefix in LEGACY_SCHEMA_SCRAPERS
        
        # Initialize DB connection (lazy)
        self._db: Optional[CountryDB] = None
        self._schema_checked = False
        
    def _get_prefix(self, name: str) -> str:
        """Get table prefix from scraper name"""
        # Direct match
        if name in PREFIX_MAP:
            return PREFIX_MAP[name]
        
        # Check if it's already a prefix
        if name in PREFIX_MAP.values():
            return name
            
        # Try to find by partial match
        for full_name, prefix in PREFIX_MAP.items():
            if name in full_name or full_name in name:
                return prefix
                
        # Default: use first 2 chars
        logger.warning(f"Unknown scraper '{name}', using '{name[:2]}' as prefix")
        return name[:2]
    
    @property
    def db(self) -> CountryDB:
        """Lazy DB connection"""
        if self._db is None:
            # Use proper country name for CountryDB
            # CountryDB expects TitleCase or specific format from COUNTRY_PREFIX_MAP
            country_name_map = {
                "ar": "Argentina",
                "ru": "Russia",
                "by": "Belarus",
                "nm": "NorthMacedonia",
                "my": "Malaysia",
                "ca_on": "CanadaOntario",
                "ca_qc": "CanadaQuebec",
                "nl": "Netherlands",
                "tw": "Taiwan",
                "tc": "Tender_Chile",
                "tb": "Tender_Brazil",
                "in": "India",
            }
            full_name = country_name_map.get(self.prefix, self.scraper_name.title())
            self._db = CountryDB(full_name)
            self._db.connect()
            if not self._schema_checked:
                self._detect_schema()
                self._schema_checked = True
        return self._db
    
    def _detect_schema(self):
        """Detect if table uses legacy or unified schema"""
        try:
            # Check if table exists and has source_hash column
            sql = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = 'source_hash'
            """
            result = self._db.fetchone(sql, (self.table_name,))
            has_hash = result is not None
            
            # Always update the flag based on actual schema
            if has_hash:
                if self._is_legacy:
                    logger.info(f"{self.table_name}: detected unified schema (was legacy)")
                self._is_legacy = False
            else:
                if not self._is_legacy:
                    logger.info(f"{self.table_name}: detected legacy schema (was unified)")
                self._is_legacy = True
                
        except Exception as e:
            logger.debug(f"Schema detection error (table may not exist): {e}")
    
    def _hash_text(self, text: str) -> str:
        """Create hash of source text for indexing"""
        return hashlib.sha256(text.encode('utf-8')).hexdigest()
    
    def get(self, source_text: str, source_lang: str, target_lang: str) -> Optional[str]:
        """
        Get cached translation.
        
        Args:
            source_text: Text to translate
            source_lang: Source language code (e.g., "es", "ru")
            target_lang: Target language code (e.g., "en")
            
        Returns:
            Translated text or None if not cached
        """
        if not source_text or not source_text.strip():
            return None
        
        source_text = source_text.strip()
        
        # Ensure schema is detected before choosing SQL path
        self._ensure_schema_detected()
        
        try:
            if self._is_legacy:
                # Legacy schema: query by source_text directly
                sql = f"""
                    SELECT translated_text FROM {self.table_name}
                    WHERE source_text = %s 
                      AND source_language = %s 
                      AND target_language = %s
                    LIMIT 1
                """
                result = self.db.fetchone(sql, (source_text, source_lang, target_lang))
            else:
                # Unified schema: query by hash
                text_hash = self._hash_text(source_text)
                sql = f"""
                    SELECT translated_text FROM {self.table_name}
                    WHERE source_hash = %s 
                      AND source_language = %s 
                      AND target_language = %s
                    LIMIT 1
                """
                result = self.db.fetchone(sql, (text_hash, source_lang, target_lang))
            
            if result:
                logger.debug(f"Cache hit: '{source_text[:50]}...' ({source_lang}->{target_lang})")
                return result[0]
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            
        return None
    
    def _ensure_schema_detected(self):
        """Ensure schema is detected before operations"""
        if not self._schema_checked:
            # Force db connection which triggers schema detection
            _ = self.db
    
    def set(self, source_text: str, translated_text: str, 
            source_lang: str, target_lang: str) -> bool:
        """
        Cache a translation.
        
        Args:
            source_text: Original text
            translated_text: Translated text
            source_lang: Source language code
            target_lang: Target language code
            
        Returns:
            True if successful
        """
        if not source_text or not translated_text:
            return False
        
        source_text = source_text.strip()
        translated_text = translated_text.strip()
        
        # Ensure schema is detected before choosing SQL path
        self._ensure_schema_detected()
        
        try:
            if self._is_legacy:
                # Legacy schema: upsert by source_text
                sql = f"""
                    INSERT INTO {self.table_name} 
                        (source_text, translated_text, source_language, target_language)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (source_text) 
                    DO UPDATE SET 
                        translated_text = EXCLUDED.translated_text,
                        updated_at = CURRENT_TIMESTAMP
                """
                self.db.execute(sql, (source_text, translated_text, source_lang, target_lang))
            else:
                # Unified schema: upsert by hash
                text_hash = self._hash_text(source_text)
                sql = f"""
                    INSERT INTO {self.table_name} 
                        (source_text, source_hash, translated_text, source_language, target_language)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (source_hash, source_language, target_language) 
                    DO UPDATE SET 
                        translated_text = EXCLUDED.translated_text,
                        updated_at = CURRENT_TIMESTAMP
                """
                self.db.execute(sql, (source_text, text_hash, translated_text, source_lang, target_lang))
            
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            try:
                self.db.rollback()
            except:
                pass
            return False
    
    def get_many(self, items: List[Tuple[str, str, str]]) -> Dict[Tuple[str, str, str], str]:
        """
        Get multiple translations at once.
        
        Args:
            items: List of (source_text, source_lang, target_lang) tuples
            
        Returns:
            Dict mapping (source, src_lang, tgt_lang) -> translation
        """
        results = {}
        for source_text, src_lang, tgt_lang in items:
            translation = self.get(source_text, src_lang, tgt_lang)
            if translation:
                results[(source_text, src_lang, tgt_lang)] = translation
        return results
    
    def set_many(self, translations: Dict[Tuple[str, str, str], str]) -> int:
        """
        Cache multiple translations at once.
        
        Args:
            translations: Dict mapping (source, src_lang, tgt_lang) -> translation
            
        Returns:
            Number of items cached
        """
        count = 0
        for (source, src_lang, tgt_lang), translation in translations.items():
            if self.set(source, translation, src_lang, tgt_lang):
                count += 1
        return count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            total = self.db.fetchone(f"SELECT COUNT(*) FROM {self.table_name}")[0]
            
            # Language pair distribution
            lang_pairs = self.db.fetchall(
                f"""SELECT source_language, target_language, COUNT(*) 
                    FROM {self.table_name} 
                    GROUP BY source_language, target_language"""
            )
            
            return {
                "total_entries": total,
                "language_pairs": [
                    {"from": r[0], "to": r[1], "count": r[2]} 
                    for r in lang_pairs
                ],
                "table_name": self.table_name,
                "schema": "legacy" if self._is_legacy else "unified"
            }
        except Exception as e:
            logger.error(f"Stats error: {e}")
            return {"error": str(e), "total_entries": 0}
    
    def migrate_from_json(self, json_path: str, source_lang: str, target_lang: str) -> int:
        """
        Migrate translations from old JSON cache file.
        
        Args:
            json_path: Path to JSON cache file
            source_lang: Source language (e.g., "es")
            target_lang: Target language (e.g., "en")
            
        Returns:
            Number of items migrated
        """
        import json
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load JSON: {e}")
            return 0
        
        count = 0
        for source, translated in data.items():
            if self.set(source, translated, source_lang, target_lang):
                count += 1
                
        logger.info(f"Migrated {count} entries from {json_path}")
        return count


# Global cache instances (lazy initialization)
_cache_instances: Dict[str, TranslationCache] = {}


def get_cache(scraper_name: str) -> TranslationCache:
    """Get or create cache instance for a scraper"""
    if scraper_name not in _cache_instances:
        _cache_instances[scraper_name] = TranslationCache(scraper_name)
    return _cache_instances[scraper_name]
