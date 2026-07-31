"""Tests for declared-map column classification (ADR-013: declared, not derived)."""

from __future__ import annotations

import logging
from collections.abc import Mapping

import pytest

from moneybin.database import Database
from moneybin.privacy.redaction import mask_strength
from moneybin.privacy.sql_lineage import FAIL_CLOSED_CLASS
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.classify import classify_columns
from moneybin.reports._framework.contract import ReportQuery, ReportSpec
from moneybin.tables import TableRef
from tests.moneybin.test_reports._metadata import TEST_SEMANTICS, output_columns


def _stub_runner(db: Database) -> ReportQuery:  # noqa: ARG001 — contract handle, unused
    return ReportQuery("SELECT 1", [])


def _spec(classes: Mapping[str, DataClass]) -> ReportSpec:
    return ReportSpec(
        report_id="test:t",
        name="t",
        description="t",
        view=TableRef("reports", "t"),
        runner=_stub_runner,
        classes=classes,
        columns=output_columns(classes),
        semantics=TEST_SEMANTICS,
    )


def test_the_undeclared_column_warning_names_neither_report_nor_column(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Both interpolations are user-authored on the tier that reaches this path.

    A packaged report's undeclared column is a build-time bug CI catches, so the
    tier that actually arrives here at runtime is the user tier — where the report
    name and the output alias are both text the user typed. ``amazon_spend`` is as
    plausible a merchant name as a column one, and ``SanitizedLogFormatter``
    recognizes neither, so the record keeps the report id and a count.

    The masked columns are already named in the response the caller reads; the log
    is not where the operator learns which alias it was.
    """
    spec = _spec({"amount": DataClass.TXN_AMOUNT})

    with caplog.at_level(logging.WARNING):
        classify_columns(spec, ["amount", "amazon_spend"])

    logged = [record.getMessage() for record in caplog.records]
    assert logged, "expected a record identifying the report"
    assert not any("amazon_spend" in message for message in logged)
    assert not any("'t'" in message for message in logged)
    assert any("test:t" in message for message in logged)


def test_classify_columns_maps_from_declared_classes() -> None:
    spec = _spec({
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "amount": DataClass.TXN_AMOUNT,
        "txn_count": DataClass.AGGREGATE,
    })
    mapped = classify_columns(spec, ["amount", "account_id"])
    assert mapped == {
        "amount": DataClass.TXN_AMOUNT,
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
    }


def test_classify_columns_fails_closed_on_undeclared_column() -> None:
    """An undeclared column takes the shared whole-masking fail-closed class.

    Pinned to ``FAIL_CLOSED_CLASS`` itself, not to a class named here, so this
    site cannot drift back into declaring its own. The mask-strength assertion
    is the substance: a partial-masking CRITICAL class (ACCOUNT_IDENTIFIER,
    which this used to be) would publish ``"****" + value[-4:]`` — four
    characters of a value the report never identified.
    """
    spec = _spec({"amount": DataClass.TXN_AMOUNT})
    mapped = classify_columns(spec, ["amount", "mystery"])
    assert mapped["amount"] is DataClass.TXN_AMOUNT
    assert mapped["mystery"] is FAIL_CLOSED_CLASS
    assert mapped["mystery"].tier is Tier.CRITICAL
    assert mask_strength(mapped["mystery"]) > mask_strength(
        DataClass.ACCOUNT_IDENTIFIER
    )
