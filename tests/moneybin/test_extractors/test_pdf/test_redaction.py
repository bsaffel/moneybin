"""Tests for the narrow Critical-field redactor applied before AI disclosure."""

import pytest

from moneybin.extractors.pdf.identity import (
    is_aba_checksum_valid,
    is_valid_aba_routing_number,
)
from moneybin.extractors.pdf.redaction import redact_critical, redact_tables


@pytest.mark.parametrize(
    "spelling",
    [
        "1234-5678-9012",
        "1234 5678 9012",
        "123456789012",
        "1234.5678.9012",
        "1234/5678/9012",
    ],
)
def test_identified_account_redacted_in_any_separator_form(spelling: str) -> None:
    text = f"Account {spelling} summary"
    out = redact_critical(text, account_identifiers=["123456789012"])
    assert "5678" not in out and out.endswith("9012 summary")
    assert len(out) == len(text)


def test_ssn_strict_format_only() -> None:
    assert redact_critical("SSN 123-45-6789", account_identifiers=[]) == (
        "SSN XXX-XX-6789"
    )
    assert redact_critical("ref 123456789", account_identifiers=[]) == "ref 123456789"


def test_routing_number_requires_aba_checksum() -> None:
    assert redact_critical("RTN 021000021", account_identifiers=[]) == ("RTN XXXXX0021")
    assert redact_critical("RTN 123456789", account_identifiers=[]) == ("RTN 123456789")


def test_amounts_and_descriptions_untouched() -> None:
    line = "03/02 COFFEE SHOP 1234567 -12.50 1,204.33"
    assert redact_critical(line, account_identifiers=["99998888"]) == line


def test_identifier_embedded_in_longer_digit_run_untouched() -> None:
    text = "ref 91234567890123"
    assert redact_critical(text, account_identifiers=["1234567890"]) == text


def test_short_identifier_ignored() -> None:
    assert redact_critical("code 1234", account_identifiers=["1234"]) == "code 1234"


def test_tables_redacted_cellwise() -> None:
    t: list[list[list[str | None]]] = [
        [["Acct", "123456789012"], ["Desc", "-4.00"], [None, "x"]]
    ]
    out = redact_tables(t, account_identifiers=["123456789012"])
    assert out[0][0][1] == "XXXXXXXX9012"
    assert out[0][1] == ["Desc", "-4.00"]
    assert out[0][2][0] is None


def test_routing_checksum_valid_outside_account_prefix_ranges_redacted() -> None:
    assert is_aba_checksum_valid("500000005")
    assert not is_valid_aba_routing_number("500000005")
    assert redact_critical("RTN 500000005", account_identifiers=[]) == "RTN XXXXX0005"


def test_zero_padded_identifier_redacted() -> None:
    out = redact_critical(
        "Acct 000123456789012 x", account_identifiers=["123456789012"]
    )
    assert out == "Acct XXXXXXXXXXX9012 x"
