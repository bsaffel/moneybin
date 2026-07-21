"""Canonical bundle snapshot contract tests."""

from __future__ import annotations

import copy
import json
from datetime import UTC, date
from decimal import Decimal
from typing import cast

import pytest

from moneybin.database import Database
from moneybin.exports.catalog import BUNDLE_TABLES
from moneybin.exports.service import ExportService
from moneybin.tables import (
    BRIDGE_TRANSFERS,
    CATEGORIES,
    DIM_ACCOUNTS,
    DIM_HOLDINGS,
    DIM_SECURITIES,
    FCT_BALANCES,
    FCT_BALANCES_DAILY,
    FCT_INVESTMENT_LOTS,
    FCT_INVESTMENT_TRANSACTIONS,
    FCT_REALIZED_GAINS,
    FCT_TRANSACTION_LINES,
    FCT_TRANSACTIONS,
    MERCHANTS,
)
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
        INSERT INTO core.dim_accounts (
            account_id, routing_number, account_type, institution_name,
            source_type, source_file, display_name, currency_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "account-0",
            "026009593",
            "depository",
            "Earlier Bank",
            "ofx",
            "earlier.ofx",
            "Earlier",
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
    db.execute(
        """
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, description, memo, currency_code, source_type,
            source_count, has_splits
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "transaction-0",
            "account-0",
            date(2026, 7, 19),
            Decimal("20.00"),
            Decimal("20.00"),
            "Earlier transaction",
            "seeded second",
            "USD",
            "ofx",
            1,
            False,
        ],
    )


def test_bundle_catalog_is_the_exact_ordered_portability_contract() -> None:
    assert [(table.name, table.source, table.order_by) for table in BUNDLE_TABLES] == [
        ("accounts", DIM_ACCOUNTS, ("account_id",)),
        (
            "transactions",
            FCT_TRANSACTIONS,
            ("transaction_date", "transaction_id"),
        ),
        (
            "transaction_lines",
            FCT_TRANSACTION_LINES,
            ("transaction_date", "transaction_id", "line_id"),
        ),
        ("transfers", BRIDGE_TRANSFERS, ("transfer_id",)),
        (
            "balances",
            FCT_BALANCES,
            ("balance_date", "account_id", "source_type", "source_ref"),
        ),
        (
            "balances_daily",
            FCT_BALANCES_DAILY,
            ("balance_date", "account_id"),
        ),
        ("categories", CATEGORIES, ("category_id",)),
        ("merchants", MERCHANTS, ("merchant_id",)),
        ("securities", DIM_SECURITIES, ("security_id",)),
        (
            "investment_transactions",
            FCT_INVESTMENT_TRANSACTIONS,
            ("trade_date", "investment_transaction_id"),
        ),
        (
            "investment_lots",
            FCT_INVESTMENT_LOTS,
            ("acquisition_date", "lot_id"),
        ),
        (
            "realized_gains",
            FCT_REALIZED_GAINS,
            ("disposal_date", "realized_gain_id"),
        ),
        ("holdings", DIM_HOLDINGS, ("account_id", "security_id")),
    ]


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
    accounts = next(table for table in first.tables if table.name == "accounts")
    account_id_index = next(
        index
        for index, column in enumerate(accounts.columns)
        if column.name == "account_id"
    )
    transaction_id_index = next(
        index
        for index, column in enumerate(transactions.columns)
        if column.name == "transaction_id"
    )
    assert [row[account_id_index] for row in accounts.rows] == [
        "account-0",
        "account-1",
    ]
    assert [row[transaction_id_index] for row in transactions.rows] == [
        "transaction-0",
        "transaction-1",
    ]
    row = dict(
        zip(
            (column.name for column in transactions.columns),
            transactions.rows[1],
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
    json.dumps(first.manifest["data_dictionary"])


def test_data_dictionary_and_manifest_are_json_safe_isolated_receipts(
    db: Database,
) -> None:
    _seed_bundle_rows(db)
    snapshot = ExportService(db).prepare_bundle(
        profile="test", redaction_mode="unredacted"
    )
    original_manifest = copy.deepcopy(snapshot.manifest)

    exposed_data_dictionary = snapshot.data_dictionary
    original_data_dictionary = json.loads(json.dumps(exposed_data_dictionary))
    dictionary_tables = cast(list[dict[str, object]], exposed_data_dictionary["tables"])
    dictionary_columns = cast(list[dict[str, object]], dictionary_tables[0]["columns"])
    dictionary_tables[0]["name"] = "corrupted"
    dictionary_columns[0]["name"] = "corrupted"

    exposed_manifest = snapshot.manifest
    exposed_dictionary = cast(dict[str, object], exposed_manifest["data_dictionary"])
    exposed_tables = cast(list[dict[str, object]], exposed_dictionary["tables"])
    exposed_columns = cast(list[dict[str, object]], exposed_tables[0]["columns"])
    exposed_tables[0]["name"] = "corrupted"
    exposed_columns[0]["name"] = "corrupted"

    assert snapshot.data_dictionary == original_data_dictionary
    assert snapshot.data_dictionary is not exposed_data_dictionary
    assert snapshot.manifest == original_manifest


@pytest.mark.parametrize(
    ("report_id", "report_parameters", "message"),
    [
        ("net-worth", None, "report id"),
        (None, {}, "report parameters"),
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
