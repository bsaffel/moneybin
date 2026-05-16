"""Unit tests for RecordingContext."""

from __future__ import annotations

from moneybin.services.auto_rule_service import RecordingContext, TxnRow
from moneybin.services.categorization import Merchant


def _merchant(
    merchant_id: str,
    raw_pattern: str,
    match_type: str,
    canonical: str,
    category: str,
    subcategory: str | None = None,
) -> Merchant:
    return Merchant(
        merchant_id=merchant_id,
        raw_pattern=raw_pattern,
        match_type=match_type,
        canonical_name=canonical,
        category=category,
        subcategory=subcategory,
        exemplars=[],
    )


class TestTxnRowLookup:
    """Tests for txn_row_for and description_for lookup methods."""

    def test_txn_row_for_returns_loaded_row(self) -> None:
        ctx = RecordingContext(
            txn_rows={
                "csv_a": TxnRow(
                    description="STARBUCKS", amount=-5.0, account_id="acct_1"
                )
            },
            active_rules=[],
            merchant_mappings=[],
        )
        row = ctx.txn_row_for("csv_a")
        assert row is not None
        assert row.description == "STARBUCKS"

    def test_description_for_returns_description(self) -> None:
        ctx = RecordingContext(
            txn_rows={
                "csv_a": TxnRow(description="STARBUCKS", amount=-5.0, account_id=None)
            },
            active_rules=[],
            merchant_mappings=[],
        )
        assert ctx.description_for("csv_a") == "STARBUCKS"

    def test_description_for_returns_none_when_missing(self) -> None:
        ctx = RecordingContext(txn_rows={}, active_rules=[], merchant_mappings=[])
        assert ctx.description_for("missing") is None


class TestRegisterNewMerchant:
    """Tests for register_new_merchant insertion ordering invariant."""

    def test_inserts_before_first_regex(self) -> None:
        ctx = RecordingContext(
            txn_rows={},
            active_rules=[],
            merchant_mappings=[
                _merchant("m1", "amzn", "exact", "AMZN", "Shopping"),
                _merchant("m2", "amzn", "contains", "AMZN", "Shopping"),
                _merchant("m3", ".*coffee.*", "regex", "Coffee", "Food"),
            ],
        )
        new = _merchant("m4", "starbucks", "contains", "Starbucks", "Food", "Coffee")
        ctx.register_new_merchant(new)
        assert ctx.merchant_mappings[2] == new
        assert ctx.merchant_mappings[3].merchant_id == "m3"

    def test_appends_when_no_regex(self) -> None:
        ctx = RecordingContext(
            txn_rows={},
            active_rules=[],
            merchant_mappings=[
                _merchant("m1", "amzn", "exact", "AMZN", "Shopping"),
            ],
        )
        new = _merchant("m2", "starbucks", "contains", "Starbucks", "Food")
        ctx.register_new_merchant(new)
        assert ctx.merchant_mappings[-1] == new


class TestMerchantMappingCovers:
    """Tests for merchant_mapping_covers Python-side cover check."""

    def test_returns_true_on_contains_category_match(self) -> None:
        ctx = RecordingContext(
            txn_rows={},
            active_rules=[],
            merchant_mappings=[
                _merchant("m1", "AMZN", "contains", "AMZN", "Shopping", None),
            ],
        )
        assert ctx.merchant_mapping_covers("AMZN MARKETPLACE", "Shopping", None)

    def test_returns_false_on_category_mismatch(self) -> None:
        ctx = RecordingContext(
            txn_rows={},
            active_rules=[],
            merchant_mappings=[
                _merchant("m1", "AMZN", "contains", "AMZN", "Shopping", None),
            ],
        )
        assert not ctx.merchant_mapping_covers("AMZN MARKETPLACE", "Food", None)

    def test_subcategory_mismatch_means_no_cover(self) -> None:
        ctx = RecordingContext(
            txn_rows={},
            active_rules=[],
            merchant_mappings=[
                _merchant("m1", "AMZN", "contains", "AMZN", "Shopping", "Books"),
            ],
        )
        assert not ctx.merchant_mapping_covers("AMZN MARKETPLACE", "Shopping", None)
        assert ctx.merchant_mapping_covers("AMZN MARKETPLACE", "Shopping", "Books")
