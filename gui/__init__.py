"""
GUI Package - Modular Tab Components

Contains individual tab implementations for separating GUI concerns.
"""

__version__ = "1.0.0"

# Only export what we actually have
from .tabs import ConfigTab

__all__ = ['ConfigTab']
