#!/usr/bin/env python3
"""
Unified Translation Service

Provides a centralized service for translating text using various providers (e.g., Google Translate).
This service integrates with the unified translation cache to minimize API calls.

Usage:
    from core.translation.service import TranslationService
    
    service = TranslationService("russia")
    
    # Translate with caching and retries
    result = service.translate("some russian text", source="ru", target="en")
"""

import time
import logging
from typing import Optional, Dict

from core.translation import get_cache

logger = logging.getLogger(__name__)

class TranslationService:
    """
    Unified translation service handling caching and API calls.
    """

    def __init__(self, scraper_name: str):
        """
        Initialize translation service for a specific scraper.
        
        Args:
            scraper_name: Name of the scraper (used for cache prefix)
        """
        self.scraper_name = scraper_name
        self.cache = get_cache(scraper_name)
        self._translator = None

    def _get_translator(self):
        """Lazy initialization of GoogleTranslator."""
        if self._translator is not None:
            return self._translator
        
        try:
            from deep_translator import GoogleTranslator
            self._translator = GoogleTranslator(source="auto", target="en")
            return self._translator
        except ImportError:
            logger.warning("deep_translator not installed. AI translation will be unavailable.")
            return None

    def translate(self, text: str, source: str = "auto", target: str = "en", retries: int = 3) -> Optional[str]:
        """
        Translate text with caching and retries.
        
        1. Check cache first.
        2. If missing, call API (Google Translate).
        3. Save result to cache.
        
        Args:
            text: Text to translate
            source: Source language code (e.g., "ru", "es")
            target: Target language code (default "en")
            retries: Number of API retries
            
        Returns:
            Translated text, or None if failed
        """
        if not text:
            return None
            
        text = text.strip()
        if not text:
            return ""

        # 1. Check cache
        cached = self.cache.get(text, source_lang=source, target_lang=target)
        if cached:
            return cached

        # 2. Call API
        translator = self._get_translator()
        if not translator:
            return None # Or return text? Usually None to indicate failure to translate

        # Update translator source/target if needed (deep_translator instances are usually specific)
        # Note: deep_translator GoogleTranslator objects are slightly different, 
        # usually simpler to re-instantiate or just use one generic if 'source' varies frequently.
        # But here we assume source is consistent per service instance often.
        # Use a new instance per call to be safe with dynamic langs or rely on `translate(..., source=...)` if supported?
        # deep_translator's translate() method supports overrides in some versions, but standard usage is constructor.
        # Let's re-instantiate for correctness if we want to be strict, or assumes 'source' passed to constructor.
        
        # Optimization: Re-use instance if langs match, else generic
        try:
             # deep_translator handling
             translator.source = source
             translator.target = target
        except:
             # Fallback: create specific instance
             from deep_translator import GoogleTranslator
             translator = GoogleTranslator(source=source, target=target)

        translated = None
        for attempt in range(retries):
            try:
                translated = translator.translate(text)
                if translated:
                    translated = translated.strip()
                    break
            except Exception as e:
                if attempt == retries - 1:
                    logger.warning(f"Translation failed for '{text[:20]}...': {e}")
                time.sleep(0.5 * (attempt + 1))

        # 3. Save to cache
        if translated:
            self.cache.set(text, translated, source_lang=source, target_lang=target)
            return translated
        
        return None
