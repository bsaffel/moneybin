"""Unit tests for redact_for_llm — strips PII from descriptions before LLM consumption."""

import pytest

from moneybin.services._text import redact_for_llm


@pytest.mark.parametrize(
    "raw, expected",
    [
        # Common merchants — minimal change after normalize
        ("STARBUCKS #1234 SEATTLE WA 02/14 *4567", "STARBUCKS"),
        ("AMZN MKTP US*AB1CD2 AMZN.COM/BILL WA", "AMZN MKTP"),
        ("COMCAST CABLE 800-555-1234 *9876", "COMCAST CABLE"),
        # P2P transfers — strip recipient names
        ("VENMO PAYMENT TO J SMITH 02/14", "VENMO PAYMENT TO"),
        ("ZELLE FROM SARAH JONES sarah@example.com", "ZELLE FROM"),
        # Misc PII
        ("CHECK #2341", "CHECK"),
    ],
)
def test_redact_for_llm_strips_pii(raw: str, expected: str) -> None:
    assert redact_for_llm(raw) == expected


def test_redact_for_llm_strips_card_last_four() -> None:
    assert "*4567" not in redact_for_llm("ACME GROCERY *4567")
    assert "1234" not in redact_for_llm("PURCHASE 1234 PENDING")


def test_redact_for_llm_strips_email_patterns() -> None:
    assert "@" not in redact_for_llm("PAYMENT TO foo@bar.com FOR SERVICES")


def test_redact_for_llm_strips_phone_patterns() -> None:
    assert "555-1234" not in redact_for_llm("MERCHANT 555-1234")
    assert "(555) 123-4567" not in redact_for_llm("MERCHANT (555) 123-4567")


def test_redact_for_llm_collapses_whitespace() -> None:
    result = redact_for_llm("MERCHANT     MULTIPLE    SPACES")
    assert "  " not in result
