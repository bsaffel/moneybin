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
    r"(?:[A-Z]{2}\s+\d{5}(?:-\d{4})?$"  # ST 12345 [-6789]
    r"|[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*,?\s+[A-Z]{2}$"  # City, ST
    r"|\d{5}(?:-\d{4})?$"  # bare zip code
    r")"
)

# Trailing numbers: store IDs, reference numbers (3+ digits at end)
_TRAILING_NUMBERS = re.compile(r"\s+#?\d{3,}$")

# Multiple spaces to single
_MULTI_SPACE = re.compile(r"\s+")

# ---------- redact_for_llm helpers ----------

# US state abbreviations used in city+state and bare-state patterns.
_STATES_ALT = (
    r"AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN"
    r"|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT"
    r"|VA|WA|WV|WI|WY|DC"
)

# City+state: one or more all-caps words followed by a 2-letter state code.
# Applied before other passes so the state code provides context for the city.
_CITY_STATE = re.compile(
    r"\b[A-Z][A-Z0-9]+(?:\s+[A-Z][A-Z0-9]+)*\s+(?:" + _STATES_ALT + r")\b"
)

# Bare state code after city+state removal.
_STATE_CODE = re.compile(r"\b(?:" + _STATES_ALT + r")\b")

# P2P recipient names: strip everything after PAYMENT TO/FROM, ZELLE/VENMO/CASHAPP TO/FROM.
_P2P_RECIPIENT = re.compile(
    r"\b((?:PAYMENT|ZELLE|VENMO|CASHAPP)\s+(?:TO|FROM))\s+.*$",
    re.IGNORECASE,
)

# Email addresses.
_EMAIL = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")

# Phone numbers: 10-digit US/CA formats and 7-digit local format (XXX-XXXX).
_PHONE = re.compile(
    r"\(?\b\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
    r"|\b\d{3}[-.\s]\d{4}\b"
)

# Tokens that embed an asterisk (card refs like US*AB1CD2, *4567).
_STAR_TOKEN = re.compile(r"\*\w+|\w+\*\w*")

# Embedded dates like 02/14.
_DATE_MMDD = re.compile(r"\b\d{1,2}/\d{2}\b")

# Hash-prefixed numbers (#1234, #9876) — store IDs, check numbers.
_HASH_NUM = re.compile(r"#\d+")

# URL-like tokens: word.TLD or word.TLD/path.
_URL_TOKEN = re.compile(
    r"\b\w[\w.-]*\.(?:COM|NET|ORG|IO|US|CA)\b(?:/\S*)?",
    re.IGNORECASE,
)

# Bare 3–5 digit numbers (card last-fours, store IDs) not already caught above.
_BARE_DIGITS = re.compile(r"\b\d{3,5}\b")


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


def redact_for_llm(description: str) -> str:
    """Strip likely-PII from a transaction description for LLM consumption.

    Calls normalize_description first, then applies PII-stripping passes.
    Conservative — over-strips rather than under-strips. Each transformation
    is a named regex so the pipeline is auditable and extensible.

    Why each pass: bank descriptions can carry card last-fours (some issuers
    embed them), embedded contact info for billers, full recipient names for
    P2P transactions, and geographic noise (city/state). The LLM gets enough
    signal from the merchant name alone.
    """
    s = normalize_description(description)
    if not s:
        return s
    s = _P2P_RECIPIENT.sub(lambda m: m.group(1), s)
    s = _EMAIL.sub("", s)
    s = _PHONE.sub("", s)
    s = _CITY_STATE.sub("", s)
    s = _STATE_CODE.sub("", s)
    s = _DATE_MMDD.sub("", s)
    s = _HASH_NUM.sub("", s)
    s = _URL_TOKEN.sub("", s)
    s = _STAR_TOKEN.sub("", s)
    s = _BARE_DIGITS.sub("", s)
    s = _MULTI_SPACE.sub(" ", s).strip()
    return s
