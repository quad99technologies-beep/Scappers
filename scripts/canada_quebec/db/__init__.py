#!/usr/bin/env python3
"""Canada Quebec database module."""

from .schema import apply_canada_quebec_schema, CANADA_QUEBEC_SCHEMA_DDL
from .repositories import CanadaQuebecRepository

__all__ = [
    "apply_canada_quebec_schema",
    "CANADA_QUEBEC_SCHEMA_DDL",
    "CanadaQuebecRepository",
]
