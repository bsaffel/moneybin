"""Prepared export redaction boundary tests."""

from __future__ import annotations

from pytest_mock import MockerFixture

import moneybin.exports.redaction as export_redaction
from moneybin.database import Database
from moneybin.exports.redaction import apply_export_redaction
from moneybin.exports.service import ExportService
from moneybin.exports.snapshot import PreparedExport
from moneybin.privacy.taxonomy import DataClass
from tests.moneybin.db_helpers import create_core_dim_stub_views, create_core_tables


def _seed_critical_account(db: Database) -> ExportService:
    create_core_tables(db)
    create_core_dim_stub_views(db)
    db.execute(
        """
        INSERT INTO core.dim_accounts (
            account_id, routing_number, account_type, source_type, source_file
        ) VALUES (?, ?, ?, ?, ?)
        """,
        ["account-1", "021000021", "depository", "ofx", "statement.ofx"],
    )
    return ExportService(db)


def _account_value(snapshot: PreparedExport, column_name: str) -> object:
    accounts = next(table for table in snapshot.tables if table.name == "accounts")
    values = dict(
        zip(
            (column.name for column in accounts.columns),
            accounts.rows[0],
            strict=True,
        )
    )
    return values[column_name]


def test_prepare_bundle_masks_critical_columns_by_default(db: Database) -> None:
    service = _seed_critical_account(db)

    snapshot = service.prepare_bundle(profile="test")

    assert snapshot.redaction_mode == "redacted"
    assert _account_value(snapshot, "routing_number") == "*****"
    accounts = next(table for table in snapshot.tables if table.name == "accounts")
    routing = next(
        column for column in accounts.columns if column.name == "routing_number"
    )
    assert routing.data_class is DataClass.ROUTING_NUMBER
    assert snapshot.manifest["redaction_mode"] == "redacted"


def test_prepare_bundle_exposes_critical_columns_only_when_explicit(
    db: Database,
) -> None:
    service = _seed_critical_account(db)

    snapshot = service.prepare_bundle(profile="test", redaction_mode="unredacted")

    assert _account_value(snapshot, "routing_number") == "021000021"
    assert snapshot.redaction_mode == "unredacted"


def test_unredacted_request_does_not_change_the_next_request(db: Database) -> None:
    service = _seed_critical_account(db)

    unredacted = service.prepare_bundle(profile="test", redaction_mode="unredacted")
    next_default = service.prepare_bundle(profile="test")

    assert _account_value(unredacted, "routing_number") == "021000021"
    assert _account_value(next_default, "routing_number") == "*****"


def test_apply_export_redaction_returns_a_copy_without_mutating_input(
    db: Database,
) -> None:
    service = _seed_critical_account(db)
    original = service.prepare_bundle(profile="test", redaction_mode="unredacted")

    redacted = apply_export_redaction(original, "redacted")

    assert redacted is not original
    assert _account_value(redacted, "routing_number") == "*****"
    assert _account_value(original, "routing_number") == "021000021"


def test_redacted_policy_calls_the_shared_engine_once_per_table(
    db: Database, mocker: MockerFixture
) -> None:
    service = _seed_critical_account(db)
    redact_spy = mocker.spy(export_redaction, "redact_records")

    snapshot = service.prepare_bundle(profile="test")

    assert redact_spy.call_count == len(snapshot.tables)
