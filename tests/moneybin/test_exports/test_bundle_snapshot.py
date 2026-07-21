"""Canonical bundle snapshot contract tests."""

from __future__ import annotations

import json
from datetime import UTC, date
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.exports.service import ExportService
from tests.moneybin.db_helpers import create_core_dim_stub_views, create_core_tables

EXPECTED_TABLES = [
    "accounts",
    "transactions",
    "transaction_lines",
    "transfers",
    "balances",
    "balances_daily",
    "categories",
    "merchants",
    "securities",
    "investment_transactions",
    "investment_lots",
    "realized_gains",
    "holdings",
]


def _seed_bundle_rows(db: Database) -> None:
    create_core_tables(db)
    create_core_dim_stub_views(db)
    db.execute(
        """
        INSERT INTO core.dim_accounts (
            account_id, routing_number, account_type, institution_name,
            source_type, source_file, display_name, currency_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "account-1",
            "021000021",
            "depository",
            "Test Bank",
            "ofx",
            "statement.ofx",
            None,
            "USD",
        ],
    )
    db.execute(
        """
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, description, memo, currency_code, source_type,
            source_count, has_splits
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "transaction-1",
            "account-1",
            date(2026, 7, 20),
            Decimal("-12.34"),
            Decimal("12.34"),
            "Test merchant",
            None,
            "USD",
            "ofx",
            1,
            False,
        ],
    )


def test_prepare_bundle_builds_the_closed_typed_canonical_snapshot(
    db: Database,
) -> None:
    _seed_bundle_rows(db)
    service = ExportService(db)

    first = service.prepare_bundle(profile="test", redaction_mode="unredacted")
    second = service.prepare_bundle(profile="test", redaction_mode="unredacted")

    assert [table.name for table in first.tables] == EXPECTED_TABLES
    assert first.artifact_version == 1
    assert first.profile == "test"
    assert first.created_at.tzinfo is UTC
    assert first.manifest["created_at"] == first.created_at.isoformat()
    assert first.manifest["subject"] == {"kind": "bundle"}
    assert all(table.source.schema == "core" for table in first.tables)
    assert all(
        not table.source.full_name.startswith(("raw.", "app.", "prep.", "meta."))
        for table in first.tables
    )

    transactions = next(table for table in first.tables if table.name == "transactions")
    row = dict(
        zip(
            (column.name for column in transactions.columns),
            transactions.rows[0],
            strict=True,
        )
    )
    assert row["amount"] == Decimal("-12.34")
    assert isinstance(row["amount"], Decimal)
    assert row["transaction_date"] == date(2026, 7, 20)
    assert isinstance(row["transaction_date"], date)
    assert row["memo"] is None

    assert [table.checksum_sha256 for table in first.tables] == [
        table.checksum_sha256 for table in second.tables
    ]
    assert all(
        len(table.checksum_sha256) == 64
        and set(table.checksum_sha256) <= set("0123456789abcdef")
        for table in first.tables
    )
    json.dumps(first.manifest)
    json.dumps(first.data_dictionary)


@pytest.mark.parametrize(
    ("report_id", "report_parameters", "message"),
    [
        ("net-worth", None, "report id"),
        (None, {"month": "2026-07"}, "report parameters"),
    ],
)
def test_prepare_bundle_rejects_report_subject_fields(
    db: Database,
    report_id: str | None,
    report_parameters: dict[str, str] | None,
    message: str,
) -> None:
    create_core_tables(db)
    create_core_dim_stub_views(db)

    with pytest.raises(ValueError, match=message):
        ExportService(db).prepare_bundle(
            profile="test",
            report_id=report_id,
            report_parameters=report_parameters,
        )
