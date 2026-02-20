import re
import logging
import unicodedata
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


class PcidMapper:
    """
    Handles uniform PCID mapping logic across different scrapers.

    This class allows defining one or more 'strategies' for mapping product data to PCID reference data.
    Each strategy specifies a set of columns to combine into a lookup key.
    The key generation logic is standardized:
      1. Normalize each column value individually (unicode NFC, strip non-alphanumeric, uppercase).
      2. Join normalized parts with a separator to prevent boundary collisions.
      3. Look up the composite key in the strategy's reference map.

    This class supports multiple fallback strategies. For example:
      - Strategy 1: Match by 'Local Pack Code'
      - Strategy 2: Match by composite key ('Product Name' + 'Generic Name' + ...)
    """

    @staticmethod
    def from_env_string(env_string: str) -> 'PcidMapper':
        """
        Creates a PcidMapper instance from an environment variable string.

        Format: "Strategy1_Key1:Ref1,Strategy1_Key2:Ref2 | Strategy2_Key1:Ref1..."
        - Strategies are separated by '|'
        - Column mappings within a strategy are separated by ','
        - Each mapping is "ScraperColumn:ReferenceColumn"

        Example:
            "Local Pack Code:pack_code | Local Product Name:product_name,Generic Name:generic_name"

        Args:
            env_string: The configuration string from .env

        Returns:
            A configured PcidMapper instance.
        """
        if not env_string:
            return PcidMapper([])

        strategies = []
        # Split by strategy separator
        strategy_strs = [s.strip() for s in env_string.split('|') if s.strip()]

        for s_str in strategy_strs:
            strategy = {}
            # Split by column separator
            pairs = [p.strip() for p in s_str.split(',') if p.strip()]
            for pair in pairs:
                if ':' not in pair:
                    continue
                k, v = pair.split(':', 1)
                strategy[k.strip()] = v.strip()

            if strategy:
                strategies.append(strategy)

        return PcidMapper(strategies)

    def __init__(self, strategies: List[Dict[str, str]]):
        """
        Initialize the PcidMapper with a list of mapping strategies.

        Args:
            strategies: A list of dictionaries. Each dictionary represents a strategy
                        and maps the SCRAPER column name (key) to the REFERENCE data column name (value).

                        Example:
                        [
                            # Strategy 1: Match by Pack Code
                            {"Local Pack Code": "local_pack_code"},

                            # Strategy 2: Match by Composite Key
                            {
                                "Local Product Name": "local_product_name",
                                "Generic Name": "generic_name",
                                "WHO ATC Code": "atc_code"
                            }
                        ]

                        The order of strategies matters. find_match() will try Strategy 1 first, then Strategy 2, etc.
        """
        self.strategies = strategies
        self.lookup_maps: List[Dict[str, Dict[str, Any]]] = [{} for _ in strategies]
        self._matched_keys: List[set] = [set() for _ in strategies]

    def _normalize_part(self, text: str) -> str:
        """
        Normalize a single key part: unicode NFC, strip non-alphanumeric, uppercase.
        """
        if text is None:
            return ""
        text = str(text)
        text = unicodedata.normalize("NFC", text)
        text = re.sub(r"[^A-Za-z0-9]", "", text)
        return text.upper()

    def _build_composite_key(self, parts: List[str]) -> str:
        """
        Build a composite lookup key from multiple values.
        Each part is normalized individually, then joined with NUL separator
        to prevent boundary collisions (e.g. ("AB","CD") vs ("A","BCD")).
        """
        normalized = [self._normalize_part(p) for p in parts]
        return "\x00".join(normalized)

    def build_reference_store(self, reference_data: List[Dict[str, Any]]):
        """
        Builds the internal lookup maps from the provided reference data (PCID mapping table rows).

        Args:
            reference_data: List of dictionaries representing rows from the pcid_mapping table.
                            Each dictionary must contain the keys specified as VALUES in the strategies.
        """
        for m in self.lookup_maps:
            m.clear()
        for s in self._matched_keys:
            s.clear()

        for row in reference_data:
            for i, strategy in enumerate(self.strategies):
                key_parts = []
                for ref_col in strategy.values():
                    val = row.get(ref_col)
                    key_parts.append(str(val) if val is not None else "")

                normalized_key = self._build_composite_key(key_parts)

                # Skip empty keys (all parts normalized to empty)
                if not normalized_key.replace("\x00", ""):
                    continue

                if normalized_key in self.lookup_maps[i]:
                    existing = self.lookup_maps[i][normalized_key]
                    logger.warning(
                        "Duplicate normalized key in strategy %d: "
                        "keeping pcid=%s, dropping pcid=%s",
                        i, existing.get("pcid", "?"), row.get("pcid", "?"),
                    )
                else:
                    self.lookup_maps[i][normalized_key] = row

    def find_match(self, product_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Attempts to find a matching PCID record for the given product row.
        Tries each strategy in order.

        Args:
            product_row: Dictionary representing a scraped product.
                         Must contain keys specified as KEYS in the strategies.

        Returns:
            The matching reference row (Dict) if found, else None.
        """
        for i, strategy in enumerate(self.strategies):
            key_parts = []
            for scraper_col in strategy.keys():
                val = product_row.get(scraper_col)
                key_parts.append(str(val) if val is not None else "")

            normalized_key = self._build_composite_key(key_parts)

            if not normalized_key.replace("\x00", ""):
                continue

            if normalized_key in self.lookup_maps[i]:
                self._matched_keys[i].add(normalized_key)
                return self.lookup_maps[i][normalized_key]

        return None

    def categorize_match(self, product_row: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        Find a matching PCID record and categorize the result.

        Returns:
            Tuple of (match_or_None, category) where category is:
              - "mapped": valid PCID match
              - "oos": matched to Out-of-Scope reference
              - "missing": no match found
        """
        match = self.find_match(product_row)
        if match is None:
            return None, "missing"
        pcid_val = str(match.get("pcid", "")).strip().upper()
        if pcid_val == "OOS":
            return match, "oos"
        if pcid_val == "":
            return match, "missing"
        return match, "mapped"

    def get_unmatched_references(self) -> List[Dict[str, Any]]:
        """
        Get reference rows that were never matched by any product.
        Excludes OOS entries. Call AFTER all find_match()/categorize_match() calls.

        Returns:
            List of unmatched, non-OOS reference row dicts.
        """
        matched_ref_ids = set()
        for i in range(len(self.strategies)):
            for key in self._matched_keys[i]:
                matched_ref_ids.add(id(self.lookup_maps[i][key]))

        seen_ref_ids = set()
        unmatched = []
        for lookup_map in self.lookup_maps:
            for ref in lookup_map.values():
                ref_id = id(ref)
                if ref_id in seen_ref_ids:
                    continue
                seen_ref_ids.add(ref_id)
                if ref_id not in matched_ref_ids:
                    pcid_val = str(ref.get("pcid", "")).strip().upper()
                    if pcid_val != "OOS":
                        unmatched.append(ref)

        return unmatched
