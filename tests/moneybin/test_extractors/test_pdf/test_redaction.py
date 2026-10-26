"""Tests for the narrow Critical-field redactor applied before AI disclosure."""

import pytest

from moneybin.extractors.pdf.identity import (
    is_aba_checksum_valid,
    is_valid_aba_routing_number,
)
from moneybin.extractors.pdf.redaction import redact_critical


@pytest.mark.parametrize(
    "spelling",
    [
        "1234-5678-9012",
        "1234 5678 9012",
        "123456789012",
        "1234.5678.9012",
        "1234/5678/9012",
        "1234 - 5678 - 9012",
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


def test_routing_checksum_valid_outside_account_prefix_ranges_redacted() -> None:
    assert is_aba_checksum_valid("500000005")
    assert not is_valid_aba_routing_number("500000005")
    assert redact_critical("RTN 500000005", account_identifiers=[]) == "RTN XXXXX0005"


def test_zero_padded_identifier_redacted() -> None:
    out = redact_critical(
        "Acct 000123456789012 x", account_identifiers=["123456789012"]
    )
    assert out == "Acct XXXXXXXXXXX9012 x"


def test_amount_spelling_the_identifier_is_masked_fail_closed() -> None:
    """Deliberate: 123456.78 is the identifier 12345678 written with a dot.

    Shape cannot tell them apart, and a leaked account number costs more than
    a non-reconciling row, so the redactor masks it.
    """
    out = redact_critical("Balance 123456.78 end", account_identifiers=["12345678"])
    assert out == "Balance XXXX56.78 end"


@pytest.mark.parametrize(
    ("spelling", "masked"),
    [
        ("1234567890.12", "XXXXXXXX90.12"),
        ("123456789012", "XXXXXXXX9012"),
        ("1234-5678-9012", "XXXX-XXXX-9012"),
        ("1234 5678 9012", "XXXX XXXX 9012"),
    ],
)
def test_amount_shaped_identifier_still_masked(spelling: str, masked: str) -> None:
    """Fail closed: an amount-shaped spelling of the identifier still masks."""
    out = redact_critical(f"Acct {spelling} x", account_identifiers=["123456789012"])
    assert out == f"Acct {masked} x"


def test_amount_adjacent_to_number_masks_only_the_number() -> None:
    out = redact_critical("Total 1234 56.78 due", account_identifiers=["12345678"])
    assert out == "Total XXXX 56.78 due"


@pytest.mark.parametrize(
    ("spelling", "masked"),
    [
        ("1234.5678", "XXXX.5678"),
        ("1234-5678", "XXXX-5678"),
        ("1234 5678", "XXXX 5678"),
        ("12345678", "XXXX5678"),
    ],
)
def test_short_identifier_with_separators_still_masked(
    spelling: str, masked: str
) -> None:
    out = redact_critical(f"Acct {spelling} x", account_identifiers=["12345678"])
    assert out == f"Acct {masked} x"


@pytest.mark.parametrize(
    ("spelling", "masked"),
    [
        ("AB12CD345678", "XXXXXXXX5678"),
        ("ab12cd345678", "XXXXXXXX5678"),
        ("AB-12CD-3456 78", "XX-XXXX-XX56 78"),
        ("AB12 CD34.5678", "XXXX XXXX.5678"),
    ],
)
def test_alphanumeric_identifier_redacted_letters_included(
    spelling: str, masked: str
) -> None:
    """Letters are part of the account number, so they mask like digits."""
    out = redact_critical(f"Acct {spelling} x", account_identifiers=["AB12CD345678"])
    assert out == f"Acct {masked} x"


def test_alphanumeric_identifier_letters_must_match() -> None:
    text = "Acct ZZ12CD345678 x"
    assert redact_critical(text, account_identifiers=["AB12CD345678"]) == text


def test_alphanumeric_identifier_embedded_in_digit_run_untouched() -> None:
    text = "ref 9AB12CD3456789"
    assert redact_critical(text, account_identifiers=["AB12CD345678"]) == text
