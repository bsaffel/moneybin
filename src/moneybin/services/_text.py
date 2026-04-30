"""Pure text-normalization helpers shared across categorization services.

Lives outside ``categorization_service`` and ``auto_rule_service`` so both
can import from here without a circular dependency.
"""

from __future__ import annotations

import re

# Common POS prefixes: Square, Toast, PayPal, etc.
_POS_PREFIXES = re.compile(
    r"^(SQ\s*\*|TST\s*\*|PP\s*\*|PAYPAL\s*\*|VENMO\s*\*|ZELLE\s*\*|CKE\s*\*)",
    re.IGNORECASE,
)

# Trailing location: city/state/zip patterns
_TRAILING_LOCATION = re.compile(
    r"\s+"
    r"(?:(?:[A-Z][A-Za-z]{3,}\s+)?[A-Z]{2}\s+\d{5}(?:-\d{4})?$"  # [City ]ST 12345 [-6789]
    r"|[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*,?\s+[A-Z]{2}$"  # City, ST
    r"|\d{5}(?:-\d{4})?$"  # bare zip code
    r")"
)

# Trailing numbers: store IDs, reference numbers (3+ digits at end)
_TRAILING_NUMBERS = re.compile(r"\s+#?\d{3,}$")

# Multiple spaces to single
_MULTI_SPACE = re.compile(r"\s+")


def normalize_description(description: str) -> str:
    """Clean a raw transaction description for matching and display.

    Applies deterministic cleanup:
    1. Strip POS prefixes (SQ *, TST*, PP*, etc.)
    2. Strip trailing location info (city, state, zip)
    3. Strip trailing store IDs / reference numbers
    4. Normalize whitespace and trim

    Args:
        description: Raw transaction description.

    Returns:
        Cleaned description string.
    """
    if not description:
        return ""

    result = description.strip()
    result = _POS_PREFIXES.sub("", result)
    result = _TRAILING_LOCATION.sub("", result)
    result = _TRAILING_NUMBERS.sub("", result)
    result = _MULTI_SPACE.sub(" ", result).strip()

    return result
