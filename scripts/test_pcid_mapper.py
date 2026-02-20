"""
Tests for PcidMapper and pcid_export utilities.

Run with: python -m pytest scripts/test_pcid_mapper.py -v
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.utils.pcid_mapper import PcidMapper
from core.utils.pcid_export import categorize_products, PcidExportResult


# ---------------------------------------------------------------------------
# Separator collision fix
# ---------------------------------------------------------------------------

def test_different_boundaries_produce_different_keys():
    """("AB","CD") and ("A","BCD") should NOT collide."""
    strategies = [{"Col A": "a", "Col B": "b"}]
    mapper = PcidMapper(strategies)
    ref = [
        {"pcid": "P1", "a": "AB", "b": "CD"},
        {"pcid": "P2", "a": "A", "b": "BCD"},
    ]
    mapper.build_reference_store(ref)

    match1 = mapper.find_match({"Col A": "AB", "Col B": "CD"})
    assert match1 is not None
    assert match1["pcid"] == "P1"

    match2 = mapper.find_match({"Col A": "A", "Col B": "BCD"})
    assert match2 is not None
    assert match2["pcid"] == "P2"


def test_single_column_strategy_still_works():
    """Single-column strategies should work identically to before."""
    mapper = PcidMapper([{"Code": "code"}])
    mapper.build_reference_store([{"pcid": "P1", "code": "ABC-123"}])

    match = mapper.find_match({"Code": "ABC 123"})
    assert match is not None
    assert match["pcid"] == "P1"


# ---------------------------------------------------------------------------
# Fallback strategy ordering
# ---------------------------------------------------------------------------

def test_fallback_strategies():
    """Strategy 1 (pack code) should be tried first, then strategy 2 (composite)."""
    strategies = [
        {"Local Pack Code": "local_pack_code"},
        {"Local Product Name": "local_product_name", "Generic Name": "generic_name"},
    ]
    mapper = PcidMapper(strategies)
    ref = [
        {"pcid": "P1", "local_pack_code": "12345", "local_product_name": "Drug A", "generic_name": "Gen A"},
        {"pcid": "P2", "local_pack_code": "", "local_product_name": "Drug B", "generic_name": "Gen B"},
    ]
    mapper.build_reference_store(ref)

    # Match by pack code (strategy 1)
    m1 = mapper.find_match({"Local Pack Code": "12345", "Local Product Name": "Wrong", "Generic Name": "Wrong"})
    assert m1["pcid"] == "P1"

    # Match by name (strategy 2, pack code empty)
    m2 = mapper.find_match({"Local Pack Code": "", "Local Product Name": "Drug B", "Generic Name": "Gen B"})
    assert m2["pcid"] == "P2"

    # No match
    m3 = mapper.find_match({"Local Pack Code": "99999", "Local Product Name": "Unknown", "Generic Name": "Unknown"})
    assert m3 is None


# ---------------------------------------------------------------------------
# Special character normalization
# ---------------------------------------------------------------------------

def test_special_char_normalization():
    """Spaces, hyphens, and special chars should be stripped for matching."""
    mapper = PcidMapper([{"Name": "name"}])
    mapper.build_reference_store([{"pcid": "P1", "name": "Product B"}])

    match = mapper.find_match({"Name": "Product   B!"})
    assert match is not None
    assert match["pcid"] == "P1"

    match2 = mapper.find_match({"Name": "Product-B"})
    assert match2 is not None
    assert match2["pcid"] == "P1"


# ---------------------------------------------------------------------------
# categorize_match
# ---------------------------------------------------------------------------

def test_categorize_match_mapped():
    mapper = PcidMapper([{"Name": "name"}])
    mapper.build_reference_store([{"pcid": "P1", "name": "Drug A"}])

    match, cat = mapper.categorize_match({"Name": "Drug A"})
    assert cat == "mapped"
    assert match["pcid"] == "P1"


def test_categorize_match_oos():
    mapper = PcidMapper([{"Name": "name"}])
    mapper.build_reference_store([{"pcid": "OOS", "name": "Drug B"}])

    match, cat = mapper.categorize_match({"Name": "Drug B"})
    assert cat == "oos"
    assert match["pcid"] == "OOS"


def test_categorize_match_missing():
    mapper = PcidMapper([{"Name": "name"}])
    mapper.build_reference_store([{"pcid": "P1", "name": "Drug A"}])

    match, cat = mapper.categorize_match({"Name": "Drug C"})
    assert cat == "missing"
    assert match is None


# ---------------------------------------------------------------------------
# get_unmatched_references
# ---------------------------------------------------------------------------

def test_unmatched_references():
    mapper = PcidMapper([{"Name": "name"}])
    ref = [
        {"pcid": "P1", "name": "Drug A"},
        {"pcid": "P2", "name": "Drug X"},  # no product will match this
        {"pcid": "OOS", "name": "Drug Y"},  # OOS should be excluded from no_data
    ]
    mapper.build_reference_store(ref)

    mapper.find_match({"Name": "Drug A"})  # matches P1

    unmatched = mapper.get_unmatched_references()
    assert len(unmatched) == 1
    assert unmatched[0]["pcid"] == "P2"


# ---------------------------------------------------------------------------
# categorize_products (full 4-way split)
# ---------------------------------------------------------------------------

def test_categorize_products_four_way_split():
    mapper = PcidMapper([{"Name": "name"}])
    ref = [
        {"pcid": "P1", "name": "Drug A"},
        {"pcid": "OOS", "name": "Drug B"},
        {"pcid": "P3", "name": "Drug X"},  # no_data — no product matches
    ]
    mapper.build_reference_store(ref)

    products = [
        {"Name": "Drug A"},
        {"Name": "Drug B"},
        {"Name": "Drug C"},
    ]
    result = categorize_products(products, mapper)

    assert isinstance(result, PcidExportResult)
    assert len(result.mapped) == 1
    assert result.mapped[0]["PCID"] == "P1"
    assert len(result.oos) == 1
    assert result.oos[0]["PCID"] == "OOS"
    assert len(result.missing) == 1
    assert result.missing[0]["PCID"] == ""
    assert len(result.no_data) == 1
    assert result.no_data[0]["pcid"] == "P3"


def test_categorize_products_with_enrichment():
    mapper = PcidMapper([{"Code": "code"}])
    ref = [{"pcid": "P1", "code": "ABC", "strength": "10mg"}]
    mapper.build_reference_store(ref)

    products = [{"Code": "ABC", "Strength": ""}]
    result = categorize_products(
        products, mapper,
        enrich_from_match={"Strength": "strength"},
    )

    assert len(result.mapped) == 1
    assert result.mapped[0]["Strength"] == "10mg"


# ---------------------------------------------------------------------------
# Duplicate key warning
# ---------------------------------------------------------------------------

def test_duplicate_key_logs_warning(caplog):
    mapper = PcidMapper([{"Code": "code"}])
    ref = [
        {"pcid": "P1", "code": "ABC"},
        {"pcid": "P2", "code": "ABC"},  # duplicate
    ]
    with caplog.at_level(logging.WARNING, logger="core.utils.pcid_mapper"):
        mapper.build_reference_store(ref)

    assert "Duplicate normalized key" in caplog.text
    assert "P1" in caplog.text
    assert "P2" in caplog.text

    # First one should be kept
    match = mapper.find_match({"Code": "ABC"})
    assert match["pcid"] == "P1"


# ---------------------------------------------------------------------------
# from_env_string
# ---------------------------------------------------------------------------

def test_from_env_string():
    env = "Local Pack Code:pack_code | Local Product Name:product_name,Generic Name:generic_name"
    mapper = PcidMapper.from_env_string(env)

    assert len(mapper.strategies) == 2
    assert mapper.strategies[0] == {"Local Pack Code": "pack_code"}
    assert mapper.strategies[1] == {
        "Local Product Name": "product_name",
        "Generic Name": "generic_name",
    }


def test_from_env_string_empty():
    mapper = PcidMapper.from_env_string("")
    assert len(mapper.strategies) == 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_empty_reference_data():
    mapper = PcidMapper([{"Name": "name"}])
    mapper.build_reference_store([])

    match = mapper.find_match({"Name": "Anything"})
    assert match is None

    unmatched = mapper.get_unmatched_references()
    assert unmatched == []


def test_all_none_values_produce_no_key():
    """If all columns are None/empty, the key should be skipped."""
    mapper = PcidMapper([{"A": "a", "B": "b"}])
    mapper.build_reference_store([{"pcid": "P1", "a": None, "b": ""}])

    # Should have no entries in lookup map
    assert len(mapper.lookup_maps[0]) == 0


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
