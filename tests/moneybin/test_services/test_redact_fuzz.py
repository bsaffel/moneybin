"""Fuzz tests: assert no embedded PII patterns appear in redacted output.

These run as part of CI on every PR. New PII patterns identified in the wild
become new fuzz cases — see private/followups.md "Categorization redaction:
testing, tuning, internationalization".
"""

import pytest

from moneybin.services._text import redact_for_llm

# Test corpus: realistic-looking PII embedded in description-like strings
_FUZZ_CASES = [
    # (description, list of substrings that must NOT appear in redacted output)
    ("MERCHANT *1234", ["1234"]),
    ("PAYMENT TO JOHN SMITH 02/14", ["JOHN", "SMITH"]),
    ("ZELLE FROM SARAH JONES sarah.j@example.com", ["SARAH", "JONES", "sarah.j", "@"]),
    ("BILLER 555-123-4567 ACCOUNT *9876", ["555", "9876"]),
    ("VENMO TO @username PAYMENT", ["@username", "username"]),
    ("CASHAPP TO JANE DOE 50.00", ["JANE", "DOE", "50.00"]),
    ("MERCHANT (415) 555-9999", ["415", "555", "9999"]),
]


@pytest.mark.parametrize("description, forbidden", _FUZZ_CASES)
def test_redact_strips_forbidden_pii(description: str, forbidden: list[str]) -> None:
    redacted = redact_for_llm(description)
    for fragment in forbidden:
        assert fragment not in redacted, (
            f"Forbidden fragment {fragment!r} appears in redacted output {redacted!r} "
            f"(from {description!r})"
        )


def test_redact_handles_empty_description() -> None:
    assert redact_for_llm("") == ""


def test_redact_handles_only_whitespace() -> None:
    assert redact_for_llm("    ") == ""


def test_redact_preserves_useful_merchant_signal() -> None:
    """Even after redaction, common merchants should remain identifiable enough for the LLM."""
    cases = ["STARBUCKS", "AMZN MKTP", "NETFLIX", "COMCAST CABLE"]
    for c in cases:
        assert c.split()[0] in redact_for_llm(c), f"Lost merchant signal in {c!r}"
