"""Unit tests for build_match_text — the matcher + LLM input string builder."""

from moneybin.services._text import build_match_text


def test_both_fields_present_joined_with_newline():
    result = build_match_text(description="PAYPAL INST XFER", memo="GOOGLE YOUTUBE")
    assert result == "PAYPAL INST XFER\nGOOGLE YOUTUBE"


def test_description_only():
    # normalize_description strips POS prefixes; memo missing → no separator.
    result = build_match_text(description="SQ *STARBUCKS COFFEE", memo=None)
    assert result == "STARBUCKS COFFEE"


def test_memo_only():
    # memo flows through normalize_description; no description → no separator.
    result = build_match_text(description=None, memo="ACH TRANSFER REF")
    assert result == "ACH TRANSFER REF"


def test_empty_strings_treated_as_missing():
    assert build_match_text(description="", memo="") == ""
    assert build_match_text(description="X", memo="") == "X"
    assert build_match_text(description="", memo="Y") == "Y"


def test_whitespace_only_treated_as_missing():
    assert build_match_text(description="   ", memo="\t\n") == ""
    assert build_match_text(description="X", memo="   ") == "X"


def test_normalization_applied_to_each_side_before_concat():
    # normalize_description strips POS prefixes and trailing state+zip
    result = build_match_text(
        description="SQ *STARBUCKS WA 98101",
        memo="ACH PAYMENT",
    )
    assert "STARBUCKS" in result
    assert "ACH PAYMENT" in result
    assert "\n" in result
    assert "98101" not in result
    assert "SQ *" not in result


def test_separator_is_literal_newline_not_space():
    result = build_match_text(description="A", memo="B")
    assert result == "A\nB"


def test_idempotent_same_input_same_output():
    args = {"description": "PAYPAL INST XFER", "memo": "GOOGLE YOUTUBE"}
    assert build_match_text(**args) == build_match_text(**args)
