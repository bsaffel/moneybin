"""Tests for per-view class derivation (Option C: lineage on the view body)."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.classify import (
    classify_columns,
    derive_view_classes,
)
from moneybin.tables import TableRef

_VIEW = TableRef("reports", "test_summary")


def test_derive_view_classes_from_body(reports_db: Database) -> None:
    # Classes are derived from the view BODY (refs core.fct_transactions), not
    # from reports.* — so account_id resolves CRITICAL, SUM(amount) HIGH,
    # COUNT LOW. Expected values come from the CLASSIFICATION registry.
    classes = derive_view_classes(reports_db, _VIEW)
    assert classes == {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "amount": DataClass.TXN_AMOUNT,
        "txn_count": DataClass.AGGREGATE,
    }


def test_classify_columns_maps_by_name(reports_db: Database) -> None:
    mapped = classify_columns(reports_db, _VIEW, ["txn_count", "amount"])
    assert mapped == {
        "txn_count": DataClass.AGGREGATE,
        "amount": DataClass.TXN_AMOUNT,
    }


def test_classify_columns_fails_closed_on_unknown_column(reports_db: Database) -> None:
    # A result column absent from the view map falls back to the max tier
    # present — CRITICAL here (account_id) — so an unclassifiable column can
    # never leak in the clear.
    mapped = classify_columns(reports_db, _VIEW, ["amount", "mystery"])
    assert mapped["mystery"] == DataClass.ACCOUNT_IDENTIFIER


def test_derive_view_classes_is_cached(reports_db: Database) -> None:
    first = derive_view_classes(reports_db, _VIEW)
    second = derive_view_classes(reports_db, _VIEW)
    assert first == second
