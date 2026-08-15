"""Tests for PDF account identity derivation."""

from moneybin.services.pdf_account_identity import derive_pdf_account_identity


def test_complete_identifier_uses_opaque_document_key_and_scoped_full_number() -> None:
    identity = derive_pdf_account_identity(
        issuer="Chase Bank",
        identifier="0012 3456-7890",
        document_sha256="A" * 64,
        identifier_is_complete=True,
    )

    assert identity.source_account_key == f"pdf_doc_{'a' * 16}"
    assert identity.last_four == "7890"
    assert identity.scoped_full_number == "chase-bank:001234567890"
    assert identity.legacy_source_account_key == "chase-bank_7890"
    assert "001234567890" not in identity.source_account_key


def test_masked_identifier_never_produces_scoped_full_number() -> None:
    identity = derive_pdf_account_identity(
        issuer="Chase Bank",
        identifier="XXXX XXXX XXXX 7890",
        document_sha256="b" * 64,
        identifier_is_complete=True,
    )

    assert identity.source_account_key == f"pdf_doc_{'b' * 16}"
    assert identity.last_four == "7890"
    assert identity.scoped_full_number is None
    assert identity.legacy_source_account_key == "chase-bank_7890"


def test_last_four_identifier_never_produces_scoped_full_number() -> None:
    identity = derive_pdf_account_identity(
        issuer="Chase Bank",
        identifier="0075",
        document_sha256="c" * 64,
        identifier_is_complete=False,
    )

    assert identity.last_four == "0075"
    assert identity.scoped_full_number is None
    assert identity.legacy_source_account_key == "chase-bank_0075"


def test_missing_identifier_has_no_account_match_keys() -> None:
    identity = derive_pdf_account_identity(
        issuer="Chase Bank",
        identifier=None,
        document_sha256="d" * 64,
        identifier_is_complete=False,
    )

    assert identity.source_account_key == f"pdf_doc_{'d' * 16}"
    assert identity.last_four is None
    assert identity.scoped_full_number is None
    assert identity.legacy_source_account_key is None


def test_unproven_long_suffix_never_produces_scoped_full_number() -> None:
    identity = derive_pdf_account_identity(
        issuer="Chase Bank",
        identifier="123456",
        document_sha256="e" * 64,
        identifier_is_complete=False,
    )

    assert identity.last_four == "3456"
    assert identity.scoped_full_number is None


def test_unknown_issuer_never_scopes_a_complete_identifier_by_unknown() -> None:
    identity = derive_pdf_account_identity(
        issuer="unknown",
        identifier="001234567890",
        document_sha256="f" * 64,
        identifier_is_complete=True,
    )

    assert identity.scoped_full_number is None


def test_routing_number_scopes_complete_identifier_when_issuer_is_unknown() -> None:
    identity = derive_pdf_account_identity(
        issuer="unknown",
        routing_number="021000021",
        identifier="001234567890",
        document_sha256="1" * 64,
        identifier_is_complete=True,
    )

    assert identity.scoped_full_number == "021000021:001234567890"


def test_known_issuer_scope_is_stable_when_routing_capture_varies() -> None:
    without_routing = derive_pdf_account_identity(
        issuer="Chase Bank",
        identifier="001234567890",
        document_sha256="3" * 64,
        identifier_is_complete=True,
    )
    with_routing = derive_pdf_account_identity(
        issuer="Chase Bank",
        routing_number="021000021",
        identifier="001234567890",
        document_sha256="4" * 64,
        identifier_is_complete=True,
    )

    assert without_routing.scoped_full_number == "chase-bank:001234567890"
    assert with_routing.scoped_full_number == without_routing.scoped_full_number


def test_checksum_invalid_routing_number_cannot_create_strong_scope() -> None:
    identity = derive_pdf_account_identity(
        issuer="unknown",
        routing_number="021000022",
        identifier="001234567890",
        document_sha256="2" * 64,
        identifier_is_complete=True,
    )

    assert identity.scoped_full_number is None


def test_checksum_valid_zero_routing_placeholder_cannot_create_strong_scope() -> None:
    identity = derive_pdf_account_identity(
        issuer="unknown",
        routing_number="000000000",
        identifier="001234567890",
        document_sha256="5" * 64,
        identifier_is_complete=True,
    )

    assert identity.scoped_full_number is None


def test_checksum_valid_reserved_routing_prefix_cannot_create_strong_scope() -> None:
    identity = derive_pdf_account_identity(
        issuer="unknown",
        routing_number="990000000",
        identifier="001234567890",
        document_sha256="6" * 64,
        identifier_is_complete=True,
    )

    assert identity.scoped_full_number is None
