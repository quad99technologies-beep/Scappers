#!/usr/bin/env python3
"""
Auto Schema Inference - High Value Feature

Uses LLM to automatically infer data schemas from HTML and suggest selectors.
Reduces manual selector maintenance significantly.

Features:
- HTML structure analysis
- Selector suggestion
- Schema extraction from sample data
- Change detection and re-inference
"""

import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import hashlib

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class FieldSchema:
    """Schema for a single field"""
    name: str
    selector: str
    field_type: str  # text, number, date, url, image
    description: str
    example: str
    confidence: float  # 0.0 to 1.0
    alternatives: List[str]  # Alternative selectors


@dataclass
class InferredSchema:
    """Complete inferred schema for a page"""
    source_url: str
    html_hash: str
    inferred_at: datetime
    fields: List[FieldSchema]
    confidence: float
    llm_model: str
    
    def to_dict(self) -> Dict:
        return {
            "source_url": self.source_url,
            "html_hash": self.html_hash,
            "inferred_at": self.inferred_at.isoformat(),
            "confidence": self.confidence,
            "llm_model": self.llm_model,
            "fields": [
                {
                    "name": f.name,
                    "selector": f.selector,
                    "type": f.field_type,
                    "description": f.description,
                    "example": f.example,
                    "confidence": f.confidence,
                    "alternatives": f.alternatives,
                }
                for f in self.fields
            ]
        }


class LLMSchemaInference:
    """
    LLM-powered schema inference using local Ollama.
    """
    
    COMMON_PHARMA_FIELDS = [
        "product_name", "brand_name", "generic_name",
        "registration_number", "manufacturer",
        "price", "currency", "unit_price",
        "dosage_form", "strength", "pack_size",
        "approval_date", "expiry_date",
        "atc_code", "category", "therapeutic_class"
    ]
    
    def __init__(self, ollama_url: Optional[str] = None, model: Optional[str] = None):
        import os
        self.ollama_url = ollama_url or os.getenv("OLLAMA_URL", "http://mac-studio:11434")
        self.model = model or os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
        self.cache: Dict[str, InferredSchema] = {}
    
    def _call_llm(self, prompt: str, format_json: bool = True) -> str:
        """Call local LLM via Ollama API"""
        import requests
        
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
        }
        
        if format_json:
            payload["format"] = "json"
        
        try:
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json=payload,
                timeout=120
            )
            response.raise_for_status()
            return response.json()["response"]
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            raise
    
    def infer_schema(self, html: str, url: str, hint: Optional[str] = None) -> InferredSchema:
        """
        Infer data schema from HTML.
        
        Args:
            html: Raw HTML content
            url: Source URL for context
            hint: Optional hint about what data to extract
        
        Returns:
            InferredSchema with fields and selectors
        """
        # Clean and truncate HTML
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script/style tags
        for tag in soup(['script', 'style', 'nav', 'footer']):
            tag.decompose()
        
        # Get text representation
        text = soup.get_text(separator='\n', strip=True)
        text = text[:4000]  # Limit for LLM context
        
        # Get sample of HTML structure
        sample_html = str(soup)[:3000]
        
        # Build prompt
        prompt = f"""Analyze this HTML and extract a structured data schema.

URL: {url}

Hint: {hint or "Extract pharmaceutical product information"}

HTML Text Sample:
{text}

HTML Structure Sample:
{sample_html}

Identify fields commonly found in pharmaceutical data:
- Product name, brand name, generic name
- Registration number, manufacturer
- Price, currency, unit price
- Dosage form, strength, pack size
- Approval/expiry dates
- ATC codes, categories

Return JSON with this structure:
{{
  "fields": [
    {{
      "name": "field_name",
      "selector": "CSS selector to extract this field",
      "type": "text|number|date|url|image",
      "description": "What this field represents",
      "example": "Sample value",
      "confidence": 0.95,
      "alternatives": ["alternative selector 1", "alternative selector 2"]
    }}
  ],
  "overall_confidence": 0.85
}}"""
        
        try:
            response = self._call_llm(prompt)
            result = json.loads(response)
            
            fields = []
            for f in result.get("fields", []):
                field = FieldSchema(
                    name=f["name"],
                    selector=f["selector"],
                    field_type=f.get("type", "text"),
                    description=f.get("description", ""),
                    example=f.get("example", ""),
                    confidence=f.get("confidence", 0.5),
                    alternatives=f.get("alternatives", [])
                )
                fields.append(field)
            
            html_hash = hashlib.sha256(html.encode()).hexdigest()[:16]
            
            schema = InferredSchema(
                source_url=url,
                html_hash=html_hash,
                inferred_at=datetime.utcnow(),
                fields=fields,
                confidence=result.get("overall_confidence", 0.5),
                llm_model=self.model
            )
            
            # Cache result
            self.cache[url] = schema
            
            logger.info(f"Inferred schema for {url}: {len(fields)} fields, confidence={schema.confidence}")
            return schema
            
        except Exception as e:
            logger.error(f"Schema inference failed: {e}")
            # Return empty schema
            return InferredSchema(
                source_url=url,
                html_hash="",
                inferred_at=datetime.utcnow(),
                fields=[],
                confidence=0.0,
                llm_model=self.model
            )
    
    def heal_selectors(self, html: str, old_schema: Dict, url: str) -> InferredSchema:
        """
        Heal broken selectors when site structure changes.
        
        Args:
            html: Current HTML
            old_schema: Previously working schema
            url: Source URL
        
        Returns:
            Updated schema with new selectors
        """
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text(separator='\n', strip=True)[:3000]
        
        old_fields = old_schema.get("fields", [])
        old_selectors = {f["name"]: f["selector"] for f in old_fields}
        
        prompt = f"""The CSS selectors for this pharmaceutical website have changed.

URL: {url}

Old selectors that no longer work:
{json.dumps(old_selectors, indent=2)}

Current HTML text:
{text}

Find new CSS selectors that extract the same data.
Return JSON with updated selectors and confidence scores.

Format:
{{
  "fields": [
    {{
      "name": "field_name",
      "old_selector": "old css selector",
      "new_selector": "new css selector",
      "confidence": 0.85,
      "change_type": "modified|removed|new"
    }}
  ],
  "overall_confidence": 0.80,
  "major_changes": false
}}"""
        
        try:
            response = self._call_llm(prompt)
            result = json.loads(response)
            
            # Merge with old schema
            new_fields = []
            for f in result.get("fields", []):
                if f.get("change_type") != "removed":
                    field = FieldSchema(
                        name=f["name"],
                        selector=f["new_selector"],
                        field_type="text",  # Preserve from old if possible
                        description=f"Healed from: {f.get('old_selector', 'unknown')}",
                        example="",
                        confidence=f.get("confidence", 0.5),
                        alternatives=[f.get("old_selector", "")]
                    )
                    new_fields.append(field)
            
            html_hash = hashlib.sha256(html.encode()).hexdigest()[:16]
            
            schema = InferredSchema(
                source_url=url,
                html_hash=html_hash,
                inferred_at=datetime.utcnow(),
                fields=new_fields,
                confidence=result.get("overall_confidence", 0.5),
                llm_model=self.model
            )
            
            logger.info(f"Healed schema for {url}: {len(new_fields)} fields, confidence={schema.confidence}")
            return schema
            
        except Exception as e:
            logger.error(f"Selector healing failed: {e}")
            return self.infer_schema(html, url, "Heal broken selectors")
    
    def detect_schema_change(self, html: str, cached_schema: InferredSchema) -> bool:
        """Detect if HTML structure has changed significantly"""
        current_hash = hashlib.sha256(html.encode()).hexdigest()[:16]
        return current_hash != cached_schema.html_hash
    
    def extract_with_schema(self, html: str, schema: InferredSchema) -> Dict[str, Any]:
        """
        Extract data from HTML using inferred schema.
        
        Args:
            html: HTML content
            schema: Inferred schema with selectors
        
        Returns:
            Extracted data dictionary
        """
        soup = BeautifulSoup(html, 'html.parser')
        result = {}
        
        for field in schema.fields:
            try:
                element = soup.select_one(field.selector)
                if element:
                    value = element.get_text(strip=True)
                    result[field.name] = value
                else:
                    # Try alternatives
                    for alt_selector in field.alternatives:
                        alt_element = soup.select_one(alt_selector)
                        if alt_element:
                            result[field.name] = alt_element.get_text(strip=True)
                            break
                    else:
                        result[field.name] = None
            except Exception as e:
                logger.warning(f"Failed to extract field {field.name}: {e}")
                result[field.name] = None
        
        return result


class SchemaRegistry:
    """
    Registry for managing inferred schemas across scrapers.
    """
    
    def __init__(self, db_path: str = ".cache/schema_registry.db"):
        self.db_path = db_path
        self.inference_engine = LLMSchemaInference()
        self._ensure_db()
    
    def _ensure_db(self):
        import sqlite3
        from pathlib import Path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schemas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scraper_name TEXT NOT NULL,
                    url_pattern TEXT NOT NULL,
                    schema_json TEXT NOT NULL,
                    html_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success_count INTEGER DEFAULT 0,
                    fail_count INTEGER DEFAULT 0
                )
            """)
            conn.commit()
    
    def get_schema(self, scraper_name: str, url: str, html: str) -> Optional[InferredSchema]:
        """Get schema for URL, infer if not exists"""
        import sqlite3
        
        # Check cache
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT schema_json, html_hash FROM schemas WHERE scraper_name = ? AND ? LIKE url_pattern",
                (scraper_name, url)
            )
            row = cursor.fetchone()
            
            if row:
                schema_data = json.loads(row[0])
                cached_hash = row[1]
                current_hash = hashlib.sha256(html.encode()).hexdigest()[:16]
                
                # Check if HTML changed
                if cached_hash == current_hash:
                    logger.info(f"Using cached schema for {url}")
                    return InferredSchema(
                        source_url=url,
                        html_hash=cached_hash,
                        inferred_at=datetime.fromisoformat(schema_data["inferred_at"]),
                        fields=[FieldSchema(**f) for f in schema_data["fields"]],
                        confidence=schema_data["confidence"],
                        llm_model=schema_data["llm_model"]
                    )
                else:
                    # HTML changed, heal schema
                    logger.info(f"HTML changed for {url}, healing schema")
                    healed = self.inference_engine.heal_selectors(html, schema_data, url)
                    self.save_schema(scraper_name, url, healed, current_hash)
                    return healed
        
        # No cached schema, infer new
        logger.info(f"Inferring new schema for {url}")
        schema = self.inference_engine.infer_schema(html, url)
        self.save_schema(scraper_name, url, schema, schema.html_hash)
        return schema
    
    def save_schema(self, scraper_name: str, url: str, schema: InferredSchema, html_hash: str):
        """Save schema to registry"""
        import sqlite3
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO schemas 
                   (scraper_name, url_pattern, schema_json, html_hash, updated_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (scraper_name, url, json.dumps(schema.to_dict()), html_hash, datetime.utcnow().isoformat())
            )
            conn.commit()
    
    def report_result(self, scraper_name: str, url: str, success: bool):
        """Report extraction success/failure for statistics"""
        import sqlite3
        
        column = "success_count" if success else "fail_count"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE schemas SET {column} = {column} + 1 WHERE scraper_name = ? AND ? LIKE url_pattern",
                (scraper_name, url)
            )
            conn.commit()


# Convenience functions
_default_registry: Optional[SchemaRegistry] = None

def get_schema_registry() -> SchemaRegistry:
    global _default_registry
    if _default_registry is None:
        _default_registry = SchemaRegistry()
    return _default_registry


def extract_data(html: str, url: str, scraper_name: str) -> Dict[str, Any]:
    """Extract data using auto-inferred schema"""
    registry = get_schema_registry()
    schema = registry.get_schema(scraper_name, url, html)
    
    if not schema.fields:
        logger.warning(f"No schema available for {url}")
        return {}
    
    data = registry.inference_engine.extract_with_schema(html, schema)
    
    # Report success/failure
    success = bool(data)
    registry.report_result(scraper_name, url, success)
    
    return data
