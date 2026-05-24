"""Tests for per-view class derivation (Option C: lineage on the view body)."""

from __future__ import annotations

from unittest.mock import patch

from moneybin.database import Database
from moneybin.privacy.sql_lineage import SqlSchemaError
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


def test_derive_view_classes_fails_closed_on_lineage_error(
    reports_db: Database,
) -> None:
    # A view DuckDB accepts but sqlglot can't parse/resolve must NOT hard-fail
    # the report — derive returns an empty map so classify_columns falls back to
    # _FAIL_CLOSED (mask everything) and the report still returns masked results.
    reports_db.execute(
        "CREATE OR REPLACE VIEW reports.unresolvable AS "
        "SELECT account_id FROM core.fct_transactions"
    )
    view = TableRef("reports", "unresolvable")
    with patch(
        "moneybin.reports._framework.classify.resolve_output_classes",
        side_effect=SqlSchemaError("cannot resolve"),
    ):
        classes = derive_view_classes(reports_db, view)
    assert classes == {}


def test_classify_columns_fails_closed_when_view_map_empty(
    reports_db: Database,
) -> None:
    # When lineage yields nothing (e.g. a view body sqlglot can't parse),
    # every column must fall back to a masking CRITICAL-tier class — not the
    # lowest tier — so nothing leaks in the clear.
    with patch(
        "moneybin.reports._framework.classify.derive_view_classes",
        return_value={},
    ):
        mapped = classify_columns(reports_db, _VIEW, ["account_id", "amount"])
    assert mapped == {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "amount": DataClass.ACCOUNT_IDENTIFIER,
    }
    assert all(c.tier is DataClass.ACCOUNT_IDENTIFIER.tier for c in mapped.values())


def test_derive_view_classes_is_cached(reports_db: Database) -> None:
    first = derive_view_classes(reports_db, _VIEW)
    second = derive_view_classes(reports_db, _VIEW)
    assert first == second


def test_derive_view_classes_invalidates_on_body_change(
    reports_db: Database,
) -> None:
    # CREATE OR REPLACE VIEW does not bump the migration version, so the cache
    # must key on the view body itself or it serves stale classifications until
    # restart — a masking miss if the rebuilt view exposes new sensitive columns.
    first = derive_view_classes(reports_db, _VIEW)
    assert "amount" in first

    reports_db.execute(
        """
        CREATE OR REPLACE VIEW reports.test_summary AS
        SELECT account_id FROM core.fct_transactions GROUP BY account_id
        """
    )
    second = derive_view_classes(reports_db, _VIEW)
    assert second == {"account_id": DataClass.ACCOUNT_IDENTIFIER}
