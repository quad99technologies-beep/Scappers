import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

# Add project root to sys.path
root_dir = Path(__file__).resolve().parents[1]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from core.ai.cleaner import ProductCleaner, BaseProductSchema

class AIService:
    """
    High-level service to be used by any scraper in the project.
    Provides a simple interface to use Gemini for data cleaning,
    with built-in support for swappable modes.
    """
    
    def __init__(self):
        self.cleaner = ProductCleaner()
        # Default mode is AI_REFINEMENT if key exists, otherwise HEURISTIC
        default_mode = "AI_REFINEMENT" if self.cleaner.enabled else "HEURISTIC"
        self.mode = os.environ.get("CLEANING_MODE", default_mode).upper()
        self.is_enabled = self.cleaner.enabled

    def clean_pharmaceutical_data(self, 
                                 raw_inputs: List[str], 
                                 country_context: str) -> List[Dict[str, Any]]:
        """
        Generic method for all medicine scrapers.
        Swaps behavior based on CLEANING_MODE.
        """
        # If mode is HEURISTIC or AI is disabled, we return empty list 
        # (The calling script should handle the bypass)
        if self.mode == "HEURISTIC" or not self.is_enabled:
            return []
        
        return self.cleaner.transform(raw_inputs, context=country_context)

    def clean_custom_data(self, 
                         raw_inputs: List[str], 
                         context: str, 
                         schema: BaseModel) -> List[Dict[str, Any]]:
        """
        If you have a special requirement (e.g. non-medicine), 
        you can pass a custom Pydantic schema.
        """
        if not self.is_enabled:
            return []
            
        return self.cleaner.transform(raw_inputs, context=context, custom_schema=schema)

# Singleton for easy access across the project
ai_service = AIService()
