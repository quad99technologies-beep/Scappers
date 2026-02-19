import os
import json
import logging
import time
from typing import List, Dict, Any, Optional, Type
import google.generativeai as genai
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("core.ai.cleaner")

class BaseProductSchema(BaseModel):
    """Universal schema for most pharmaceutical scrapers"""
    generic_name: Optional[str] = Field(None, description="The generic or chemical name")
    brand_name: Optional[str] = Field(None, description="The commercial brand name")
    manufacturer: Optional[str] = Field(None, description="The manufacturing company")
    dosage_form: Optional[str] = Field(None, description="Tablet, Capsule, etc.")
    strength: Optional[str] = Field(None, description="E.g., 500mg, 10ml")
    pack_size: Optional[str] = Field(None, description="E.g., Pack of 30")
    price: Optional[float] = Field(None, description="Numerical price value")
    currency: Optional[str] = Field(None, description="E.g., CAD, ARS, MYR")
    identifier: Optional[str] = Field(None, description="Any code like DIN, CUI, or URL ID")

class ProductCleaner:
    """
    Generic AI Transformation Engine.
    Converts messy text from ANY country into a standardized JSON format.
    """
    
    def __init__(self, api_key: Optional[str] = None, model_name: str = "gemini-1.5-flash"):
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
        if not self.api_key:
            logger.warning("AI Cleaner: GOOGLE_API_KEY not found. AI features will be disabled.")
            self.enabled = False
        else:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel(model_name)
            self.enabled = True
            
    def transform(self, 
                  data: List[str], 
                  context: str = "Pharmaceutical data extraction",
                  custom_schema: Optional[Type[BaseModel]] = None) -> List[Dict[str, Any]]:
        """
        The main generic method to transform messy data.
        
        Args:
            data: List of strings or messy dicts to clean.
            context: Description of the source (e.g. 'Canada Quebec PDF' or 'Argentina HTML')
            custom_schema: Optional Pydantic model to force a specific output format.
        """
        if not self.enabled or not data:
            return []

        # Use custom schema description or the default one
        schema_model = custom_schema or BaseProductSchema
        schema_json = schema_model.model_json_schema()
        
        prompt = f"""
        Role: Expert Data Engineer
        Task: Standardize the following messy strings into clean JSON objects.
        Context: {context}
        
        Return Format Requirements:
        1. Output MUST be a valid JSON array of objects.
        2. Match this schema exactly: {json.dumps(schema_json['properties'], indent=2)}
        3. Do not include markdown code blocks (no ```json).
        4. If data is missing for a field, use null.
        
        Input Data:
        {json.dumps(data, indent=2)}
        """

        for attempt in range(3):
            try:
                response = self.model.generate_content(prompt)
                text = response.text.strip()
                
                # Clean potential markdown garbage
                if text.startswith("```"):
                    text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
                if text.strip().startswith("```json"):
                    text = text.replace("```json", "").replace("```", "")
                
                return json.loads(text)
            except Exception as e:
                logger.error(f"AI Transformation Attempt {attempt+1} failed: {e}")
                time.sleep(2)
        
        return []

    def transform_single(self, raw_data: str, context: str = "") -> Optional[Dict[str, Any]]:
        results = self.transform([raw_data], context)
        return results[0] if results else None

# Example Usage for Canada Quebec:
if __name__ == "__main__":
    # Mock data from a messy PDF line
    messy_lines = [
        "02244521  LIPITOR  PFIZER  10 MG  TAB  0.5432",
        "ABACAVIR (SULFATE D') / LAMIVUDINE 600 mg / 300 mg CP 30 156.0000 5.2000 02396762"
    ]
    
    cleaner = ProductCleaner()
    if cleaner.enabled:
        print("Cleaning Canada Quebec data...")
        results = cleaner.clean_batch(messy_lines, context="Canada Quebec Exception Drugs List")
        print(json.dumps(results, indent=2))
    else:
        print("Please set GOOGLE_API_KEY to test.")
