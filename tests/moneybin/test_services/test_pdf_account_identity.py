"""Tests for PDF account identity derivation."""

from moneybin.extractors.pdf.metadata import StatementMetadata
from moneybin.extractors.pdf.routing import RouteDecision
from moneybin.services.import_service import (
    _pdf_source_account,  # pyright: ignore[reportPrivateUsage]
)
from moneybin.services.pdf_account_identity import derive_pdf_account_identity


def _decision(*, issuer: str) -> RouteDecision:
    return RouteDecision(
        outcome="transactions",
        recipe=None,
        rows=[],
        metadata=StatementMetadata(
            account_id="001234567890",
            period_start=None,
            period_end=None,
            opening_balance=None,
            closing_balance=None,
            routing_number="021000021",
            account_id_complete=True,
        ),
        confidence=1.0,
        reason="passed",
        fp={"issuer": issuer, "headers": [], "page_bucket": "1"},
    )


def test_complete_identifier_without_routing_is_not_a_strong_ref() -> None:
    identity = derive_pdf_account_identity(
        issuer="Chase Bank",
        identifier="0012 3456-7890",
        document_sha256="A" * 64,
        identifier_is_complete=True,
    )

    assert identity.source_account_key == f"pdf_doc_{'a' * 16}"
    assert identity.source_origin == "document"
    assert identity.last_four == "7890"
    assert identity.scoped_full_number is None
    assert identity.legacy_source_account_key == "chase-bank_7890"
    assert identity.legacy_source_origin == "chase-bank"
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


def test_complete_but_short_identifier_never_produces_scoped_full_number() -> None:
    identity = derive_pdf_account_identity(
        issuer="Chase Bank",
        routing_number="021000021",
        identifier="0075",
        document_sha256="9" * 64,
        identifier_is_complete=True,
    )

    assert identity.scoped_full_number is None


def test_legacy_key_preserves_empty_issuer_slug() -> None:
    identity = derive_pdf_account_identity(
        issuer="!!!",
        identifier="****1234",
        document_sha256="0" * 64,
        identifier_is_complete=False,
    )

    assert identity.legacy_source_account_key == "_1234"
    assert identity.legacy_source_origin == ""


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


def test_valid_routing_scope_is_independent_of_heuristic_issuer() -> None:
    unknown_issuer = derive_pdf_account_identity(
        issuer="unknown",
        routing_number="021000021",
        identifier="001234567890",
        document_sha256="3" * 64,
        identifier_is_complete=True,
    )
    known_issuer = derive_pdf_account_identity(
        issuer="Chase Bank",
        routing_number="021000021",
        identifier="001234567890",
        document_sha256="4" * 64,
        identifier_is_complete=True,
    )

    assert unknown_issuer.scoped_full_number == "021000021:001234567890"
    assert known_issuer.scoped_full_number == unknown_issuer.scoped_full_number


def test_document_origin_is_independent_of_heuristic_issuer() -> None:
    unknown_issuer = derive_pdf_account_identity(
        issuer="unknown",
        identifier="001234567890",
        document_sha256="7" * 64,
        identifier_is_complete=True,
    )
    known_issuer = derive_pdf_account_identity(
        issuer="Chase Bank",
        identifier="001234567890",
        document_sha256="7" * 64,
        identifier_is_complete=True,
    )

    assert unknown_issuer.source_origin == "document"
    assert known_issuer.source_origin == unknown_issuer.source_origin
    assert known_issuer.source_account_key == unknown_issuer.source_account_key


def test_pdf_source_native_tuple_survives_issuer_detector_changes() -> None:
    unknown_issuer = _pdf_source_account(
        _decision(issuer="unknown"),
        resolved_alias="statement",
        account_id_override=None,
        document_sha256="8" * 64,
    ).source
    known_issuer = _pdf_source_account(
        _decision(issuer="Chase Bank"),
        resolved_alias="statement",
        account_id_override=None,
        document_sha256="8" * 64,
    ).source

    assert (
        unknown_issuer.source_type,
        unknown_issuer.source_origin,
        unknown_issuer.source_account_key,
    ) == (
        known_issuer.source_type,
        known_issuer.source_origin,
        known_issuer.source_account_key,
    )
    assert known_issuer.account_number == "021000021:001234567890"


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
