"""Tests for V034: create the investment tables + cost-basis default column.

V034 lays the schema foundation for the investments data model (M1J.1):
app.securities, raw.manual_investment_transactions, app.lot_selections, and
app.account_settings.default_cost_basis_method. Fresh installs get all four
from the schema DDL; existing installs get them via this migration. Pure
additive DDL — no backfill, no reshape.

Per `.claude/rules/database.md`, the test drives ``migrate()`` through the
shared ``run_migration()`` helper to reproduce the runner's enclosing
BEGIN/COMMIT transaction. The fixture reverses the V034 end-state first
(the ``db`` fixture initializes the current schema, which already includes
the investment DDL) and seeds ``app.account_settings`` rows so the ALTER
runs against populated data.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V034__add_investment_tables import migrate
from tests.moneybin.migration_helpers import column_exists, run_migration

pytestmark = pytest.mark.fresh_db


def _table_exists(db: Database, schema: str, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return row is not None


def _recreate_pre_v034_state(db: Database) -> None:
    """Reverse the V034 end-state: drop the tables and the settings column.

    Also reverses V036's rename of the currency column. V034's rebuild-with-
    CHECK idiom recreates ``app.account_settings`` wholesale and hardcodes the
    column name that existed when V034 was written (``iso_currency_code`` --
    V034 predates V036). A DB genuinely reaching V034 in migration order has
    not run V036 yet, so this fixture must match that real chronology for
    V034's frozen SQL to bind.
    """
    db.execute("DROP TABLE IF EXISTS app.securities")
    db.execute("DROP TABLE IF EXISTS raw.manual_investment_transactions")
    db.execute("DROP TABLE IF EXISTS app.lot_selections")
    db.execute(
        "ALTER TABLE app.account_settings DROP COLUMN IF EXISTS default_cost_basis_method"
    )
    db.execute(
        "ALTER TABLE app.account_settings RENAME COLUMN currency_code TO iso_currency_code"
    )
    # Realistic populated rows (>=3) with NON-TRIVIAL values in every nullable
    # column the V034 rebuild preserves — so a future edit that drops a column
    # from the INSERT...SELECT is caught (a minimal 2-column stub would leave
    # the rest at NULL/default and hide the data loss). See database.md
    # "Migration test data realism".
    db.execute(
        """
        INSERT INTO app.account_settings (
            account_id, display_name, official_name, last_four,
            account_subtype, holder_category, iso_currency_code,
            credit_limit, archived, include_in_net_worth, updated_at
        ) VALUES
        ('acct_checking1', 'Everyday Checking', 'Everyday Checking 1234', '1234',
         'checking', 'personal', 'USD', NULL, FALSE, TRUE,
         TIMESTAMP '2024-01-02 03:04:05'),
        ('acct_broker001', 'Fidelity Brokerage', 'Fidelity Individual', '9876',
         'brokerage', 'personal', 'USD', NULL, FALSE, TRUE,
         TIMESTAMP '2024-03-04 05:06:07'),
        ('acct_credit001', 'Chase Sapphire', 'Chase Sapphire Reserve', '4321',
         'credit card', 'business', 'USD', 15000.00, TRUE, FALSE,
         TIMESTAMP '2024-05-06 07:08:09')
        """
    )


class TestV034Migration:
    """V034 creates the investment tables and settings column, idempotently."""

    def test_creates_tables_and_column(self, db: Database) -> None:
        _recreate_pre_v034_state(db)
        assert not _table_exists(db, "app", "securities")

        run_migration(db, migrate)

        assert _table_exists(db, "app", "securities")
        assert _table_exists(db, "raw", "manual_investment_transactions")
        assert _table_exists(db, "app", "lot_selections")
        assert column_exists(db, "app", "account_settings", "default_cost_basis_method")
        # Existing rows survive with NULL default.
        rows = db.execute(
            "SELECT count(*) FROM app.account_settings "
            "WHERE default_cost_basis_method IS NULL"
        ).fetchone()
        assert rows is not None and rows[0] >= 3

    def test_preserves_all_settings_columns(self, db: Database) -> None:
        # V034 rebuilds app.account_settings (DROP + CREATE + INSERT...SELECT).
        # A future edit that drops a column from that INSERT...SELECT would
        # silently reset it on every existing DB. Assert every preserved column
        # survives with its seeded value so such a regression fails here.
        _recreate_pre_v034_state(db)
        run_migration(db, migrate)

        row = db.execute(
            """
            SELECT display_name, official_name, last_four, account_subtype,
                   holder_category, iso_currency_code, credit_limit, archived,
                   include_in_net_worth, updated_at, default_cost_basis_method
              FROM app.account_settings
             WHERE account_id = 'acct_credit001'
            """
        ).fetchone()
        assert row == (
            "Chase Sapphire",
            "Chase Sapphire Reserve",
            "4321",
            "credit card",
            "business",
            "USD",
            Decimal("15000.00"),
            True,
            False,
            datetime(2024, 5, 6, 7, 8, 9),
            None,  # newly-added column defaults NULL
        )

    def test_check_constraint_enforced_after_migration(self, db: Database) -> None:
        _recreate_pre_v034_state(db)
        run_migration(db, migrate)

        db.execute(
            "UPDATE app.account_settings SET default_cost_basis_method = 'hifo' "
            "WHERE account_id = 'acct_broker001'"
        )
        with pytest.raises(duckdb.ConstraintException):
            db.execute(
                "UPDATE app.account_settings SET default_cost_basis_method = 'lifo' "
                "WHERE account_id = 'acct_checking1'"
            )

    def test_idempotent_on_fresh_install(self, db: Database) -> None:
        """A fresh install already has the end-state; re-running is a no-op."""
        run_migration(db, migrate)
        run_migration(db, migrate)

        assert _table_exists(db, "app", "securities")
        assert column_exists(db, "app", "account_settings", "default_cost_basis_method")
