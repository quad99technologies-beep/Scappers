#!/usr/bin/env python3
"""
Selector Healer - Auto-heal broken selectors using Schema Inference

When selectors break, automatically infer new selectors using LLM.
"""

import logging
import os
from typing import Optional, Dict, List, Any
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class SelectorHealer:
    """Auto-heal broken selectors using schema inference."""
    
    def __init__(self):
        self._inference_available = False
        self._connection_checked = False
        
        # Check if schema inference is disabled via environment variable
        if os.getenv("DISABLE_SCHEMA_INFERENCE", "").lower() in ("1", "true", "yes"):
            logger.debug("Schema inference disabled via DISABLE_SCHEMA_INFERENCE")
            self.inference = None
            return
        
        try:
            from core.data.schema_inference import LLMSchemaInference
            ollama_url = os.getenv("OLLAMA_URL", "http://mac-studio:11434")
            self.inference = LLMSchemaInference(ollama_url=ollama_url)
            self._inference_available = True
        except ImportError:
            logger.debug("Schema inference not available (import failed)")
            self.inference = None
        except Exception as e:
            logger.debug(f"Schema inference initialization failed: {e}")
            self.inference = None
    
    def _check_ollama_available(self) -> bool:
        """Check if Ollama service is available."""
        if not self._inference_available or not self.inference:
            return False
        
        if self._connection_checked:
            return self._inference_available
        
        # Test connection by trying a simple health check
        try:
            import requests
            ollama_url = self.inference.ollama_url
            response = requests.get(f"{ollama_url}/api/tags", timeout=2)
            response.raise_for_status()
            self._connection_checked = True
            logger.debug("Ollama service is available")
            return True
        except Exception as e:
            logger.debug(f"Ollama service not available at {ollama_url}: {e}")
            self._inference_available = False
            self._connection_checked = True
            return False
    
    def heal_selector(self, html: str, broken_selector: str, expected_fields: List[str],
                     scraper_name: str = "unknown") -> Optional[Dict[str, Any]]:
        """
        Heal a broken selector by inferring new schema.
        
        Args:
            html: HTML content
            broken_selector: The broken selector
            expected_fields: List of field names we expect to extract
            scraper_name: Scraper name for context
        
        Returns:
            Dict with new selectors and confidence, or None if healing failed
        """
        # Check if Ollama is available before attempting to use it
        if not self._check_ollama_available():
            return None
        
        try:
            # Infer schema from HTML
            schema = self.inference.infer_schema(html, expected_fields)
            
            if not schema or not schema.fields:
                logger.warning(f"Schema inference returned no fields for {scraper_name}")
                return None
            
            # Build result with new selectors
            result = {
                "fields": {},
                "confidence": schema.confidence,
                "original_selector": broken_selector,
            }
            
            for field in schema.fields:
                result["fields"][field.name] = {
                    "selector": field.selector,
                    "confidence": field.confidence,
                    "alternatives": field.alternatives,
                }
            
            logger.info(f"[SELECTOR_HEALER] Healed selector for {scraper_name}: "
                       f"{len(result['fields'])} fields, confidence={schema.confidence:.2f}")
            
            return result
            
        except Exception as e:
            # Mark inference as unavailable if connection fails
            logger.debug(f"Selector healing failed (will disable): {e}")
            self._inference_available = False
            return None
    
    def test_selector(self, html: str, selector: str) -> bool:
        """
        Test if a selector works on given HTML.
        
        Returns:
            True if selector finds elements, False otherwise
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            elements = soup.select(selector)
            return len(elements) > 0
        except Exception:
            return False


def get_selector_healer() -> SelectorHealer:
    """Get singleton selector healer instance."""
    if not hasattr(get_selector_healer, '_instance'):
        get_selector_healer._instance = SelectorHealer()
    return get_selector_healer._instance
