"""Tests for V036: rename app.account_settings.iso_currency_code -> currency_code."""

from __future__ import annotations

import json
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V036__rename_iso_currency_code_to_currency_code import (
    migrate,
)
from tests.moneybin.migration_helpers import column_exists, insert_rows, run_migration

pytestmark = pytest.mark.fresh_db

_ROWS: list[tuple[str, str | None]] = [
    ("acct_checking01", "USD"),
    ("acct_savings002", "EUR"),
    ("acct_creditcrd3", None),
]

# Full-row capture shape (Invariant 10) for a pre-migration account_settings.set
# audit event — every column app.account_settings carries, including a
# Decimal-as-string credit_limit (matches BaseRepo._serialize_for_audit).
_ACCOUNT_SETTINGS_BEFORE: dict[str, Any] = {
    "account_id": "acct_creditcrd3",
    "display_name": "Rewards Visa",
    "official_name": "Chase Sapphire Reserve",
    "last_four": "4321",
    "account_subtype": "credit card",
    "holder_category": "personal",
    "iso_currency_code": "USD",
    "credit_limit": "5000.00",
    "archived": False,
    "include_in_net_worth": True,
    "default_cost_basis_method": None,
    "updated_at": "2026-01-15T10:30:00",
}

_ACCOUNT_SETTINGS_AFTER: dict[str, Any] = {
    **_ACCOUNT_SETTINGS_BEFORE,
    "display_name": "Primary Rewards Visa",
    "updated_at": "2026-01-15T11:05:00",
}


def _reset_to_pre_v036_state(db: Database) -> None:
    """Reverse the V036 end-state so the migration has work to do."""
    db.execute(
        "ALTER TABLE app.account_settings RENAME COLUMN currency_code TO iso_currency_code"
    )


def _populate(db: Database) -> None:
    insert_rows(
        db, "app", "account_settings", ("account_id", "iso_currency_code"), _ROWS
    )


def _insert_audit_row(
    db: Database,
    *,
    audit_id: str,
    target_table: str,
    before_value: dict[str, Any] | None,
    after_value: dict[str, Any] | None,
) -> None:
    """Insert a raw app.audit_log row, bypassing repos.

    Simulates a pre-V036 ``account_settings.set`` capture — AccountSettingsRepo
    already writes the post-rename ``currency_code`` key, so it can't produce
    the stale-key payload this migration needs to fix.
    """
    db.execute(
        """
        INSERT INTO app.audit_log (
            audit_id, actor, action, target_schema, target_table, target_id,
            before_value, after_value, operation_id
        ) VALUES (?, 'cli', 'account_settings.set', 'app', ?, ?, ?, ?, ?)
        """,
        [
            audit_id,
            target_table,
            audit_id,
            json.dumps(before_value) if before_value is not None else None,
            json.dumps(after_value) if after_value is not None else None,
            f"op_{audit_id}",
        ],
    )


def _audit_json(
    db: Database, audit_id: str
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    row = db.execute(
        "SELECT before_value, after_value FROM app.audit_log WHERE audit_id = ?",
        [audit_id],
    ).fetchone()
    assert row is not None
    before_raw, after_raw = row
    before = json.loads(before_raw) if before_raw is not None else None
    after = json.loads(after_raw) if after_raw is not None else None
    return before, after


class TestV036Migration:
    """V036 renames app.account_settings.iso_currency_code, idempotently."""

    def test_v036_renames_column(self, db: Database) -> None:
        _reset_to_pre_v036_state(db)
        _populate(db)
        assert column_exists(db, "app", "account_settings", "iso_currency_code")
        assert not column_exists(db, "app", "account_settings", "currency_code")

        run_migration(db, migrate)

        assert not column_exists(db, "app", "account_settings", "iso_currency_code")
        assert column_exists(db, "app", "account_settings", "currency_code")

    def test_v036_preserves_data(self, db: Database) -> None:
        _reset_to_pre_v036_state(db)
        _populate(db)

        run_migration(db, migrate)

        rows = db.execute(
            "SELECT account_id, currency_code FROM app.account_settings "
            "ORDER BY account_id"
        ).fetchall()
        assert rows == sorted(_ROWS)

    def test_v036_idempotent_on_second_run(self, db: Database) -> None:
        _reset_to_pre_v036_state(db)
        _populate(db)
        run_migration(db, migrate)
        run_migration(db, migrate)
        assert column_exists(db, "app", "account_settings", "currency_code")

    def test_v036_idempotent_on_fresh_install(self, db: Database) -> None:
        # No reset — db comes from init_schemas with the final shape already.
        run_migration(db, migrate)
        assert column_exists(db, "app", "account_settings", "currency_code")


class TestV036AuditLogRewrite:
    """V036 rewrites iso_currency_code -> currency_code inside historical audit rows.

    app.audit_log.before_value/after_value capture the full row state at
    mutation time (Invariant 10), so every account_settings.set audit event
    written before this migration has iso_currency_code as a JSON key —
    regardless of which field the user actually changed. Without this
    rewrite, BaseRepo._restore_row (undo) would build
    ``UPDATE app.account_settings SET iso_currency_code = ? ...`` against a
    column the rename above just removed.
    """

    def test_rewrites_account_settings_payload_preserving_other_keys(
        self, db: Database
    ) -> None:
        _reset_to_pre_v036_state(db)
        _populate(db)
        _insert_audit_row(
            db,
            audit_id="audit_test_001",
            target_table="account_settings",
            before_value=_ACCOUNT_SETTINGS_BEFORE,
            after_value=_ACCOUNT_SETTINGS_AFTER,
        )

        run_migration(db, migrate)

        before, after = _audit_json(db, "audit_test_001")
        assert before is not None
        assert after is not None
        assert "iso_currency_code" not in before
        assert "iso_currency_code" not in after
        assert before["currency_code"] == "USD"
        assert after["currency_code"] == "USD"
        # Every other key/value survives untouched — including the
        # Decimal-as-string credit_limit.
        expected_before = {
            ("currency_code" if k == "iso_currency_code" else k): v
            for k, v in _ACCOUNT_SETTINGS_BEFORE.items()
        }
        expected_after = {
            ("currency_code" if k == "iso_currency_code" else k): v
            for k, v in _ACCOUNT_SETTINGS_AFTER.items()
        }
        assert before == expected_before
        assert after == expected_after

    def test_leaves_other_target_tables_untouched(self, db: Database) -> None:
        """Proves the rewrite is scoped by target_table, not by key presence.

        A different target_table's payload survives even when it happens to
        contain the literal key ``iso_currency_code``.
        """
        _reset_to_pre_v036_state(db)
        _populate(db)
        other_before = {
            "transaction_id": "txn_1",
            "category": "Groceries",
            "iso_currency_code": "USD",
        }
        other_after = {
            "transaction_id": "txn_1",
            "category": "Dining",
            "iso_currency_code": "USD",
        }
        _insert_audit_row(
            db,
            audit_id="audit_test_002",
            target_table="transaction_categories",
            before_value=other_before,
            after_value=other_after,
        )

        run_migration(db, migrate)

        before, after = _audit_json(db, "audit_test_002")
        assert before == other_before
        assert after == other_after
        assert before is not None
        assert "iso_currency_code" in before

    def test_handles_null_before_value_on_insert_event(self, db: Database) -> None:
        _reset_to_pre_v036_state(db)
        _populate(db)
        _insert_audit_row(
            db,
            audit_id="audit_test_003",
            target_table="account_settings",
            before_value=None,
            after_value=_ACCOUNT_SETTINGS_AFTER,
        )

        run_migration(db, migrate)

        before, after = _audit_json(db, "audit_test_003")
        assert before is None
        assert after is not None
        assert "iso_currency_code" not in after
        assert after["currency_code"] == "USD"

    def test_handles_null_after_value_on_delete_event(self, db: Database) -> None:
        _reset_to_pre_v036_state(db)
        _populate(db)
        _insert_audit_row(
            db,
            audit_id="audit_test_005",
            target_table="account_settings",
            before_value=_ACCOUNT_SETTINGS_BEFORE,
            after_value=None,
        )

        run_migration(db, migrate)

        before, after = _audit_json(db, "audit_test_005")
        assert after is None
        assert before is not None
        assert "iso_currency_code" not in before
        assert before["currency_code"] == "USD"

    def test_idempotent_on_second_run(self, db: Database) -> None:
        _reset_to_pre_v036_state(db)
        _populate(db)
        _insert_audit_row(
            db,
            audit_id="audit_test_004",
            target_table="account_settings",
            before_value=_ACCOUNT_SETTINGS_BEFORE,
            after_value=_ACCOUNT_SETTINGS_AFTER,
        )

        run_migration(db, migrate)
        before_first, after_first = _audit_json(db, "audit_test_004")

        run_migration(db, migrate)
        before_second, after_second = _audit_json(db, "audit_test_004")

        assert before_second == before_first
        assert after_second == after_first
        assert before_second is not None
        assert before_second["currency_code"] == "USD"
        assert "iso_currency_code" not in before_second
