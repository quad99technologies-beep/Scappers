"""
Translation Cache Module

Unified translation caching that works across all scrapers.
Replaces: JSON file caches, in-memory caches, ad-hoc DB caches
"""

from .cache import TranslationCache, get_cache

__all__ = ['TranslationCache', 'get_cache']
