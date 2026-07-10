"""Tests for OFX/QFX file extractor.

This module tests the OFX extractor with sample QFX data to ensure proper
parsing, validation, and data extraction into raw table structures.
"""

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from moneybin.extractors.ofx import OFXExtractor, OFXProviderConfig
from moneybin.extractors.ofx.extractor import (
    OFXTransactionSchema,
    _decode_text_field,  # pyright: ignore[reportPrivateUsage]
    _disambiguate_colliding_fitids,  # pyright: ignore[reportPrivateUsage]
    extract_ofx_file,
)

# Path to test fixtures directory
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"

_IMPORT_ID = "00000000-0000-0000-0000-000000000001"
_SOURCE_ORIGIN = "test_bank"


@pytest.fixture
def sample_ofx_file() -> Path:
    """Path to sample OFX fixture file for testing."""
    fixture_path = FIXTURES_DIR / "sample_statement.qfx"
    if not fixture_path.exists():
        raise FileNotFoundError(
            f"Test fixture not found: {fixture_path}\n"
            f"Expected location: tests/fixtures/sample_statement.qfx"
        )
    return fixture_path


@pytest.fixture
def extractor_config(tmp_path: Path) -> OFXProviderConfig:
    """Create test extraction configuration."""
    return OFXProviderConfig(
        raw_data_path=tmp_path / "raw_ofx",
        preserve_source_files=True,
        validate_balances=True,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Plain text passes through untouched.
        ("Coffee Shop", "Coffee Shop"),
        ("BILL PAY Megabank - Rewards", "BILL PAY Megabank - Rewards"),
        # Already-clean ampersand is left alone (unescape is idempotent here).
        ("AT&T", "AT&T"),
        # Single SGML entity escape → decoded once.
        ("AT&amp;T", "AT&T"),
        # Double escape (notably Wells Fargo) → the repeated-unescape loop must
        # fully decode it; a single html.unescape would leave "AT&amp;T".
        ("AT&amp;amp;T", "AT&T"),
        ("BILL PAY Telco &amp;amp; Cable Internet", "BILL PAY Telco & Cable Internet"),
        # Non-string sentinels.
        (None, None),
        ("", ""),
    ],
)
def test_decode_text_field_unescapes_double_encoded_entities(
    raw: str | None, expected: str | None
) -> None:
    """OFX payee/memo entities are fully decoded, including double escapes.

    Guards the fix for banks that emit already-escaped SGML content which
    ofxparse decodes only one level (`AT&amp;amp;T` → `AT&amp;T`), leaving stale
    entities in the description the matcher and reports read.
    """
    assert _decode_text_field(raw) == expected


@pytest.mark.unit
def test_ofx_transaction_schema_validation() -> None:
    """Test that OFX transaction schema validates and converts data correctly."""
    # Test valid transaction
    tx = OFXTransactionSchema(
        id="TXN001",
        type="DEBIT",
        date=datetime(2025, 1, 15),
        amount=Decimal("-50.00"),
        payee="Test Merchant",
        memo="Test purchase",
        checknum=None,
    )

    assert tx.id == "TXN001"
    assert tx.type == "DEBIT"
    assert tx.amount == Decimal("-50.00")
    assert tx.payee == "Test Merchant"

    # Test amount conversion from float
    tx2 = OFXTransactionSchema(
        id="TXN002",
        type="CREDIT",
        date=datetime(2025, 1, 15),
        amount=100.50,  # type: ignore[arg-type]
        payee="Test Payer",
        memo=None,
        checknum=None,
    )

    assert tx2.amount == Decimal("100.50")


@pytest.mark.unit
def test_extractor_initialization(extractor_config: OFXProviderConfig) -> None:
    """Test that OFX extractor initializes correctly."""
    extractor = OFXExtractor(extractor_config)

    assert extractor.config == extractor_config
    # raw_data_path is resolved into an extractor instance attribute; the
    # frozen config itself stays unmutated. Pre-fix this asserted the
    # mutation on extractor.config.raw_data_path.
    assert extractor.raw_data_path.exists()


@pytest.mark.unit
def test_extract_from_file_creates_dataframes(
    sample_ofx_file: Path, extractor_config: OFXProviderConfig
) -> None:
    """Test that extraction creates all expected DataFrames."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(
        sample_ofx_file, import_id=_IMPORT_ID, source_origin=_SOURCE_ORIGIN
    )

    # Check all expected tables are present
    assert "institutions" in results
    assert "accounts" in results
    assert "transactions" in results
    assert "balances" in results

    # Check that results are DataFrames
    assert isinstance(results["institutions"], pl.DataFrame)
    assert isinstance(results["accounts"], pl.DataFrame)
    assert isinstance(results["transactions"], pl.DataFrame)
    assert isinstance(results["balances"], pl.DataFrame)


@pytest.mark.unit
def test_extract_institutions_data(
    sample_ofx_file: Path, extractor_config: OFXProviderConfig
) -> None:
    """Test that institution data is extracted correctly."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(
        sample_ofx_file, import_id=_IMPORT_ID, source_origin=_SOURCE_ORIGIN
    )

    institutions = results["institutions"]

    # Should have at least one institution
    assert len(institutions) >= 1

    # Check expected columns
    assert "organization" in institutions.columns
    assert "fid" in institutions.columns
    assert "source_file" in institutions.columns
    assert "extracted_at" in institutions.columns

    # Check values
    first_row = institutions.row(0, named=True)
    assert first_row["organization"] == "Test Bank"
    assert first_row["fid"] == "12345"


@pytest.mark.unit
def test_extract_accounts_data(
    sample_ofx_file: Path, extractor_config: OFXProviderConfig
) -> None:
    """Test that account data is extracted correctly."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(
        sample_ofx_file, import_id=_IMPORT_ID, source_origin=_SOURCE_ORIGIN
    )

    accounts = results["accounts"]

    # Should have at least one account
    assert len(accounts) >= 1

    # Check expected columns
    assert "account_id" in accounts.columns
    assert "routing_number" in accounts.columns
    assert "account_type" in accounts.columns
    assert "institution_org" in accounts.columns

    # Check values
    first_row = accounts.row(0, named=True)
    assert first_row["account_id"] == "9876543210"
    assert first_row["routing_number"] == "123456789"
    assert first_row["account_type"] == "CHECKING"


@pytest.mark.unit
def test_extract_transactions_data(
    sample_ofx_file: Path, extractor_config: OFXProviderConfig
) -> None:
    """Test that transaction data is extracted correctly."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(
        sample_ofx_file, import_id=_IMPORT_ID, source_origin=_SOURCE_ORIGIN
    )

    transactions = results["transactions"]

    # Should have 3 transactions from sample data
    assert len(transactions) == 3

    # Check expected columns
    expected_cols = [
        "source_transaction_id",
        "account_id",
        "transaction_type",
        "date_posted",
        "amount",
        "payee",
        "memo",
    ]
    for col in expected_cols:
        assert col in transactions.columns

    # Check first transaction (debit)
    tx1 = transactions.row(0, named=True)
    assert tx1["source_transaction_id"] == "TXN001"
    assert tx1["transaction_type"].upper() == "DEBIT"
    assert tx1["amount"] == Decimal("-50.00")
    assert tx1["payee"] == "Coffee Shop"

    # Check second transaction (credit)
    tx2 = transactions.row(1, named=True)
    assert tx2["source_transaction_id"] == "TXN002"
    assert tx2["transaction_type"].upper() == "CREDIT"
    assert tx2["amount"] == Decimal("1000.00")
    assert tx2["payee"] == "Payroll Deposit"


@pytest.mark.unit
def test_extract_balances_data(
    sample_ofx_file: Path, extractor_config: OFXProviderConfig
) -> None:
    """Test that balance data is extracted correctly."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(
        sample_ofx_file, import_id=_IMPORT_ID, source_origin=_SOURCE_ORIGIN
    )

    balances = results["balances"]

    # Should have at least one balance record
    assert len(balances) >= 1

    # Check expected columns
    assert "account_id" in balances.columns
    assert "ledger_balance" in balances.columns
    assert "available_balance" in balances.columns

    # Check values
    first_row = balances.row(0, named=True)
    assert first_row["account_id"] == "9876543210"
    assert first_row["ledger_balance"] == Decimal("5000.00")
    assert first_row["available_balance"] == Decimal("4800.00")


@pytest.mark.unit
def test_extract_nonexistent_file_raises_error(
    extractor_config: OFXProviderConfig,
) -> None:
    """Test that extracting non-existent file raises FileNotFoundError."""
    extractor = OFXExtractor(extractor_config)

    with pytest.raises(FileNotFoundError):
        extractor.extract_from_file(
            Path("/nonexistent/file.qfx"),
            import_id=_IMPORT_ID,
            source_origin=_SOURCE_ORIGIN,
        )


@pytest.mark.unit
def test_extract_invalid_ofx_raises_error(
    tmp_path: Path, extractor_config: OFXProviderConfig
) -> None:
    """Test that invalid OFX content raises ValueError."""
    # Create file with invalid OFX content
    invalid_file = tmp_path / "invalid.qfx"
    invalid_file.write_text("This is not valid OFX content")

    extractor = OFXExtractor(extractor_config)

    with pytest.raises(ValueError, match="Invalid OFX file format"):
        extractor.extract_from_file(
            invalid_file, import_id=_IMPORT_ID, source_origin=_SOURCE_ORIGIN
        )


@pytest.mark.unit
def test_convenience_function(sample_ofx_file: Path) -> None:
    """Test the convenience function for OFX extraction."""
    results = extract_ofx_file(
        sample_ofx_file, import_id=_IMPORT_ID, source_origin=_SOURCE_ORIGIN
    )

    # Check all expected tables are present
    assert "institutions" in results
    assert "accounts" in results
    assert "transactions" in results
    assert "balances" in results

    # Check transactions were extracted
    assert len(results["transactions"]) == 3


@pytest.mark.unit
def test_extract_preserves_metadata(
    sample_ofx_file: Path, extractor_config: OFXProviderConfig
) -> None:
    """Test that extraction preserves metadata like source file and extraction time."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(
        sample_ofx_file, import_id=_IMPORT_ID, source_origin=_SOURCE_ORIGIN
    )

    # Check transactions have metadata
    transactions = results["transactions"]
    first_tx = transactions.row(0, named=True)

    assert "source_file" in first_tx
    assert "extracted_at" in first_tx
    assert str(sample_ofx_file) in first_tx["source_file"]

    # Verify extraction timestamp is recent (within last minute)
    extracted_at = datetime.fromisoformat(first_tx["extracted_at"])
    time_diff = datetime.now() - extracted_at
    assert time_diff.total_seconds() < 60


@pytest.mark.unit
def test_extracted_transaction_amount_is_decimal(
    sample_ofx_file: Path, extractor_config: OFXProviderConfig
) -> None:
    """Transaction amount column must be pl.Decimal(18,2), not Float64."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(
        sample_ofx_file, import_id=_IMPORT_ID, source_origin=_SOURCE_ORIGIN
    )

    transactions = results["transactions"]
    assert transactions["amount"].dtype == pl.Decimal(precision=18, scale=2)


@pytest.mark.unit
def test_extracted_balance_amounts_are_decimal(
    sample_ofx_file: Path, extractor_config: OFXProviderConfig
) -> None:
    """Balance amount columns must be pl.Decimal(18,2), not Float64."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(
        sample_ofx_file, import_id=_IMPORT_ID, source_origin=_SOURCE_ORIGIN
    )

    balances = results["balances"]
    assert balances["ledger_balance"].dtype == pl.Decimal(precision=18, scale=2)
    assert balances["available_balance"].dtype == pl.Decimal(precision=18, scale=2)


class TestExtractorPopulatesBatchColumns:
    """extract_from_file populates import_id, source_type, and source_origin in DataFrames."""

    def test_transactions_df_has_import_id_and_source_origin(self) -> None:
        fixture = FIXTURES_DIR / "ofx" / "sample_minimal.ofx"
        if not fixture.exists():
            pytest.skip("OFX fixture not present yet")

        extractor = OFXExtractor()
        result = extractor.extract_from_file(
            fixture,
            import_id="11111111-1111-1111-1111-111111111111",
            source_origin="test_bank",
        )

        txns = result["transactions"]
        assert "import_id" in txns.columns
        assert "source_origin" in txns.columns
        assert "source_type" in txns.columns
        assert all(
            v == "11111111-1111-1111-1111-111111111111"
            for v in txns["import_id"].to_list()
        )
        assert all(v == "test_bank" for v in txns["source_origin"].to_list())
        assert all(v == "ofx" for v in txns["source_type"].to_list())

    def test_all_dataframes_have_import_id_and_source_type(self) -> None:
        """All four DataFrames carry import_id and source_type."""
        fixture = FIXTURES_DIR / "ofx" / "sample_minimal.ofx"
        if not fixture.exists():
            pytest.skip("OFX fixture not present yet")

        extractor = OFXExtractor()
        result = extractor.extract_from_file(
            fixture,
            import_id="22222222-2222-2222-2222-222222222222",
            source_origin="minimal_bank",
        )

        for name in ("institutions", "accounts", "transactions", "balances"):
            df = result[name]
            assert "import_id" in df.columns, f"{name} missing import_id"
            assert "source_type" in df.columns, f"{name} missing source_type"

    def test_institution_name_comes_from_file_not_caller(self) -> None:
        """Institution org comes from <FI><ORG> in the file, not from a caller hint."""
        fixture = FIXTURES_DIR / "ofx" / "sample_minimal.ofx"
        if not fixture.exists():
            pytest.skip("OFX fixture not present yet")

        extractor = OFXExtractor()
        result = extractor.extract_from_file(
            fixture,
            import_id=_IMPORT_ID,
            source_origin=_SOURCE_ORIGIN,
        )

        institutions = result["institutions"]
        assert len(institutions) >= 1
        first = institutions.row(0, named=True)
        # The fixture has <ORG>SAMPLE BANK</ORG>
        assert first["organization"] == "SAMPLE BANK"


def _txn_row(
    *,
    source_transaction_id: str,
    account_id: str = "4387",
    transaction_type: str = "DEBIT",
    date_posted: str = "2025-11-19",
    amount: str = "-13.12",
    payee: str = "MERCHANT",
    memo: str | None = None,
    check_number: str | None = None,
) -> dict[str, object]:
    """Minimal transaction row carrying the fields the FITID-collision repair reads."""
    return {
        "source_transaction_id": source_transaction_id,
        "account_id": account_id,
        "transaction_type": transaction_type,
        "date_posted": date_posted,
        "amount": Decimal(amount),
        "payee": payee,
        "memo": memo,
        "check_number": check_number,
    }


@pytest.mark.unit
def test_extract_disambiguates_shared_fitid_within_file(
    extractor_config: OFXProviderConfig,
) -> None:
    """Two distinct transactions sharing one FITID must both survive extraction.

    Reproduces F1: Chase stamps a foreign purchase (-13.12) and its
    foreign-transaction-fee (-0.39) with one shared FITID on the same day. The
    raw PK and every dedup layer key on ``(source_transaction_id, account_id)``,
    so without repair one of the two distinct transactions is silently dropped.
    """
    fixture = FIXTURES_DIR / "ofx" / "duplicate_fitid_sample.ofx"
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(
        fixture, import_id=_IMPORT_ID, source_origin=_SOURCE_ORIGIN
    )

    txns = results["transactions"]
    # All three rows survive as distinct records (two share the raw FITID).
    assert len(txns) == 3
    ids = txns["source_transaction_id"].to_list()
    assert len(set(ids)) == 3, f"ids collided: {ids}"

    # The two repaired rows both derive from the shared FITID; the unique row is
    # left untouched (source-provided id stored as-is).
    shared = sorted(i for i in ids if i.startswith("SHAREDFITID999"))
    assert len(shared) == 2
    assert "UNIQUEFITID001" in ids

    # Neither amount is lost — the -13.12 purchase survives alongside the fee.
    amounts = set(txns["amount"].to_list())
    assert Decimal("-13.12") in amounts
    assert Decimal("-0.39") in amounts


class TestDisambiguateCollidingFitids:
    """Content-based repair of non-unique OFX FITIDs within a single file."""

    def test_differing_content_same_fitid_gets_distinct_ids(self) -> None:
        rows = [
            _txn_row(source_transaction_id="F1", amount="-13.12", payee="PURCHASE"),
            _txn_row(source_transaction_id="F1", amount="-0.39", payee="FEE"),
        ]
        rewritten = _disambiguate_colliding_fitids(rows)

        assert rewritten == 2
        first, second = (r["source_transaction_id"] for r in rows)
        assert first != second
        assert all(str(r["source_transaction_id"]).startswith("F1#") for r in rows)

    def test_delimiter_in_free_text_does_not_collapse_distinct_rows(self) -> None:
        """A delimiter inside payee/memo must not make two distinct rows look alike.

        ``payee="A|B",memo="C"`` and ``payee="A",memo="B|C"`` are different
        transactions; a bare pipe-join would serialize both identically and drop
        one. The signature must keep them distinct so both get suffixed.
        """
        rows = [
            _txn_row(source_transaction_id="F", payee="A|B", memo="C"),
            _txn_row(source_transaction_id="F", payee="A", memo="B|C"),
        ]
        rewritten = _disambiguate_colliding_fitids(rows)

        assert rewritten == 2
        assert rows[0]["source_transaction_id"] != rows[1]["source_transaction_id"]

    def test_identical_content_same_fitid_left_to_collapse(self) -> None:
        """A genuine in-file duplicate keeps one id so raw dedup still collapses it."""
        rows = [
            _txn_row(source_transaction_id="F1", amount="-5.00", payee="X"),
            _txn_row(source_transaction_id="F1", amount="-5.00", payee="X"),
        ]
        rewritten = _disambiguate_colliding_fitids(rows)

        assert rewritten == 0
        assert all(r["source_transaction_id"] == "F1" for r in rows)

    def test_distinct_fitids_are_untouched(self) -> None:
        rows = [
            _txn_row(source_transaction_id="A", amount="-1.00"),
            _txn_row(source_transaction_id="B", amount="-2.00"),
        ]
        rewritten = _disambiguate_colliding_fitids(rows)

        assert rewritten == 0
        assert [r["source_transaction_id"] for r in rows] == ["A", "B"]

    def test_suffix_is_deterministic_across_runs(self) -> None:
        """Same content → same suffix, so re-importing the file stays idempotent."""
        run_a = [
            _txn_row(source_transaction_id="F", amount="-1.00", payee="P"),
            _txn_row(source_transaction_id="F", amount="-2.00", payee="Q"),
        ]
        run_b = [
            _txn_row(source_transaction_id="F", amount="-1.00", payee="P"),
            _txn_row(source_transaction_id="F", amount="-2.00", payee="Q"),
        ]
        _disambiguate_colliding_fitids(run_a)
        _disambiguate_colliding_fitids(run_b)

        assert [r["source_transaction_id"] for r in run_a] == [
            r["source_transaction_id"] for r in run_b
        ]

    def test_same_fitid_different_accounts_untouched(self) -> None:
        """Cross-account FITID reuse (F11) is not an in-account collision.

        ``account_id`` is part of the key at every layer, so the same short
        FITID in two different accounts is already distinct — repairing it would
        be wrong. This is the boundary between F1 (in-account data loss, fixed
        here) and F11 (cross-account matching noise, handled elsewhere).
        """
        rows = [
            _txn_row(source_transaction_id="123", account_id="acctA", amount="-1.00"),
            _txn_row(source_transaction_id="123", account_id="acctB", amount="-2.00"),
        ]
        rewritten = _disambiguate_colliding_fitids(rows)

        assert rewritten == 0
        assert all(r["source_transaction_id"] == "123" for r in rows)
