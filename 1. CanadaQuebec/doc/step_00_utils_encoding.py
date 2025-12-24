#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Encoding Utilities

Provides encoding correction utilities for text extraction from PDFs and other sources.
Ensures UTF-8 correctness at extraction time to prevent mojibake corruption.

Author: Enterprise PDF Processing Pipeline
License: Proprietary

Usage:
    from step_00_encoding_utils import clean_extracted_text
    
    text = page.extract_text()
    clean_text = clean_extracted_text(text)
"""

import re
import unicodedata
from typing import Optional

# Common mojibake patterns (UTF-8 decoded as Latin-1/Windows-1252)
MOJIBAKE_FIXES = {
    # French accents
    'Ã‰': 'É', 'Ã©': 'é', 'Ã¨': 'è', 'Ãª': 'ê', 'Ã«': 'ë',
    'Ã€': 'À', 'Ã ': 'à', 'Ã¢': 'â', 'Ã§': 'ç',
    'ÃŽ': 'Î', 'Ã®': 'î', 'Ã¯': 'ï',
    'Ã': 'Ô', 'Ã´': 'ô', 'Ã¶': 'ö',
    'Ã›': 'Û', 'Ã»': 'û', 'Ã¼': 'ü',
    'ÃŸ': 'ß', 'Ã¿': 'ÿ',
    # Percent-encoded patterns
    'Ã%o': 'é', 'Ã%a': 'à', 'Ã%u': 'ù',
    # Other common issues
    'â€™': "'", 'â€œ': '"', 'â€': '"',
    'â€"': '—', 'â€"': '–',
}

# Pattern to detect mojibake sequences
MOJIBAKE_PATTERN = re.compile(r'Ã[^\s]{0,2}')

def detect_mojibake(text: str) -> bool:
    """
    Detect if text contains mojibake patterns.
    Returns True if suspicious sequences are found.
    """
    if not text:
        return False
    return bool(MOJIBAKE_PATTERN.search(text))

def fix_mojibake(text: str) -> str:
    """
    Fix common mojibake patterns by replacing corrupted sequences.
    This handles UTF-8 text that was incorrectly decoded as Latin-1/Windows-1252.
    """
    if not text:
        return text
    
    result = text
    
    # Apply known fixes
    for wrong, correct in MOJIBAKE_FIXES.items():
        result = result.replace(wrong, correct)
    
    # Try to fix percent-encoded patterns (like CÃ%oFOTA -> Céfotaxir)
    # Pattern: Ã% followed by hex digits or letters
    def fix_percent_encoded(match):
        seq = match.group(0)
        # Common mojibake patterns from percent-encoded UTF-8
        # Ã%o often represents é (when %E9 was mis-decoded)
        if 'Ã%o' in seq or seq.startswith('Ã%o'):
            return 'é'
        if 'Ã%a' in seq or seq.startswith('Ã%a'):
            return 'à'
        if 'Ã%u' in seq or seq.startswith('Ã%u'):
            return 'ù'
        if 'Ã%c' in seq or seq.startswith('Ã%c'):
            return 'ç'
        if 'Ã%i' in seq or seq.startswith('Ã%i'):
            return 'î'
        if 'Ã%e' in seq or seq.startswith('Ã%e'):
            return 'è'
        return seq
    
    # Fix percent-encoded mojibake (pattern: Ã% followed by letter/digit)
    result = re.sub(r'Ã%[0-9a-zA-Z]{1,2}', fix_percent_encoded, result)
    
    return result

def normalize_unicode_nfc(text: str) -> str:
    """
    Normalize Unicode to NFC (Canonical Composition).
    This ensures consistent representation of accented characters.
    """
    if not text:
        return text
    return unicodedata.normalize('NFC', text)

def clean_extracted_text(text: Optional[str], enforce_utf8: bool = True) -> str:
    """
    Main function to clean text at extraction point.
    
    This should be called immediately after extracting text from PDF/OCR/data source.
    
    Steps:
    1. Handle None/empty
    2. Detect and fix mojibake
    3. Normalize to Unicode NFC
    4. Ensure valid UTF-8
    
    Args:
        text: Raw extracted text (may contain mojibake)
        enforce_utf8: If True, ensure output is valid UTF-8
        
    Returns:
        Cleaned, normalized text ready for processing
    """
    if text is None:
        return ""
    
    if not isinstance(text, str):
        # If somehow we got bytes, decode as UTF-8
        try:
            if isinstance(text, bytes):
                text = text.decode('utf-8', errors='replace')
            else:
                text = str(text)
        except Exception:
            return ""
    
    # Step 1: Fix mojibake patterns
    cleaned = fix_mojibake(text)
    
    # Step 2: Normalize to NFC
    cleaned = normalize_unicode_nfc(cleaned)
    
    # Step 3: Ensure valid UTF-8 (remove any remaining invalid sequences)
    if enforce_utf8:
        try:
            # Encode/decode cycle to ensure valid UTF-8
            cleaned = cleaned.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
        except Exception:
            # Fallback: remove non-UTF-8 characters
            cleaned = cleaned.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
    
    return cleaned

def clean_word_token(word_dict: dict) -> dict:
    """
    Clean a word token dictionary from pdfplumber.
    Applies encoding fixes to the 'text' field.
    
    Args:
        word_dict: Dictionary with 'text' key (from pdfplumber extract_words)
        
    Returns:
        Dictionary with cleaned 'text' field
    """
    if not isinstance(word_dict, dict):
        return word_dict
    
    if 'text' in word_dict:
        word_dict = word_dict.copy()  # Don't mutate original
        word_dict['text'] = clean_extracted_text(word_dict['text'])
    
    return word_dict

def ensure_utf8_file_write(file_path, mode='w', add_bom=False):
    """
    Context manager helper to ensure UTF-8 file writing.
    Use this instead of open() for text files.

    Args:
        file_path: Path to file
        mode: File mode ('w', 'a', etc.)
        add_bom: If True, writes UTF-8 BOM for Excel compatibility

    Usage:
        with ensure_utf8_file_write('output.csv', add_bom=True) as f:
            f.write("text")
    """
    import io
    encoding = 'utf-8-sig' if add_bom else 'utf-8'
    return io.open(file_path, mode, encoding=encoding, newline='', errors='replace')

def csv_writer_utf8(file_path, add_bom=True):
    """
    Open a CSV file for writing with proper UTF-8 encoding.
    Adds BOM by default for Excel compatibility.

    Args:
        file_path: Path to CSV file
        add_bom: If True, adds UTF-8 BOM for Excel (default: True)

    Returns:
        Open file handle (use with 'with' statement)

    Usage:
        with csv_writer_utf8('output.csv') as f:
            writer = csv.DictWriter(f, fieldnames=['col1', 'col2'])
            writer.writeheader()
    """
    import io
    encoding = 'utf-8-sig' if add_bom else 'utf-8'
    return io.open(file_path, 'w', encoding=encoding, newline='', errors='replace')

def csv_reader_utf8(file_path):
    """
    Open a CSV file for reading with proper UTF-8 encoding.
    Handles UTF-8 with or without BOM.

    Args:
        file_path: Path to CSV file

    Returns:
        Open file handle (use with 'with' statement)

    Usage:
        with csv_reader_utf8('input.csv') as f:
            reader = csv.DictReader(f)
            for row in reader:
                process(row)
    """
    import io
    return io.open(file_path, 'r', encoding='utf-8-sig', newline='', errors='replace')

