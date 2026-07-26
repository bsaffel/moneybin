"""Tests for audited profile-level settings (multi-currency Requirement 4)."""

from __future__ import annotations

from typing import Any

import duckdb
import pytest

from moneybin.database import Database
from moneybin.repositories.profile_settings_repo import ProfileSettingsRepo
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from moneybin.sql.migrations.V044__create_app_profile_settings import migrate
from tests.moneybin.migration_helpers import run_migration


@pytest.fixture()
def repo(db: Database) -> ProfileSettingsRepo:
    run_migration(db, migrate)
    return ProfileSettingsRepo(db)


def _audit_rows(db: Database) -> list[tuple[Any, ...]]:
    return db.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor
          FROM app.audit_log
         WHERE target_table = 'profile_settings'
         ORDER BY occurred_at ASC, audit_id ASC
        """
    ).fetchall()


def test_home_currency_is_unset_on_a_fresh_profile(repo: ProfileSettingsRepo) -> None:
    """A profile that never chose a home currency reports none — never 'USD'.

    This is the bug class M1K.1 exists to kill: a blind USD default relabels a
    EUR-only user's money. Absence must stay representable.
    """
    assert repo.get_home_currency() is None


def test_set_home_currency_persists_and_emits_one_audit_row(
    db: Database, repo: ProfileSettingsRepo
) -> None:
    """Setting the home currency stores it and pairs exactly one audit row."""
    repo.set_home_currency("EUR", actor="cli")

    assert repo.get_home_currency() == "EUR"

    audit = _audit_rows(db)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor = audit[0]
    assert (schema, table) == ("app", "profile_settings")
    assert action == "profile_settings.set"
    assert target_id == "profile"
    assert before is None
    assert '"home_currency":"EUR"' in after.replace(" ", "")
    assert actor == "cli"


def test_second_set_replaces_the_row_and_audits_the_prior_value(
    db: Database, repo: ProfileSettingsRepo
) -> None:
    """The table is a singleton: a second set updates in place, and is audited.

    Catches an INSERT-only implementation that accumulates rows, which would
    leave SQLMesh guards reading an arbitrary one of several home currencies.
    """
    repo.set_home_currency("EUR", actor="cli")
    repo.set_home_currency("GBP", actor="mcp")

    assert repo.get_home_currency() == "GBP"

    row_count = db.execute("SELECT COUNT(*) FROM app.profile_settings").fetchone()
    assert row_count is not None and row_count[0] == 1

    audit = _audit_rows(db)
    assert len(audit) == 2
    assert '"home_currency":"EUR"' in audit[1][4].replace(" ", "")
    assert '"home_currency":"GBP"' in audit[1][5].replace(" ", "")
    assert audit[1][6] == "mcp"


@pytest.mark.parametrize("bad", ["eur", "EU", "EURO", "", "E1R", "US$"])
def test_rejects_codes_that_are_not_iso_4217_shaped(
    repo: ProfileSettingsRepo, bad: str
) -> None:
    """Only three uppercase letters are accepted — matching `accounts set --currency`."""
    with pytest.raises(ValueError):
        repo.set_home_currency(bad, actor="cli")


def test_a_rejected_code_leaves_the_stored_value_untouched(
    repo: ProfileSettingsRepo,
) -> None:
    """Validation runs before the write, so a bad code cannot clobber a good one."""
    repo.set_home_currency("EUR", actor="cli")

    with pytest.raises(ValueError):
        repo.set_home_currency("dollars", actor="cli")

    assert repo.get_home_currency() == "EUR"


def test_the_table_admits_only_one_settings_row(
    db: Database, repo: ProfileSettingsRepo
) -> None:
    """A second scope value is refused, so `home_currency` is unambiguous in SQL.

    SQLMesh report guards read this table with a bare SELECT; two rows would
    make the home currency depend on scan order. Asserting on the CHECK
    constraint by name keeps a typo'd INSERT from passing this test for the
    wrong reason.
    """
    repo.set_home_currency("EUR", actor="cli")

    with pytest.raises(duckdb.ConstraintException, match="CHECK constraint"):
        db.execute(
            "INSERT INTO app.profile_settings (scope, home_currency) "
            "VALUES ('other', 'GBP')"
        )

    remaining = db.execute(
        "SELECT scope, home_currency FROM app.profile_settings"
    ).fetchall()
    assert remaining == [("profile", "EUR")]


def test_setting_the_home_currency_is_undoable(
    db: Database, repo: ProfileSettingsRepo
) -> None:
    """`system_audit_undo` reverses a home-currency change, including to unset.

    The generic BaseRepo inverse is what makes this work, but it is not free
    here: `app.profile_settings` is a singleton guarded by a CHECK on `scope`,
    so undoing the *first* write has to remove the only row rather than restore
    a previous one. Both directions are asserted because they take different
    branches — UPDATE restores a before-image, INSERT deletes.
    """
    with operation() as first_write:
        repo.set_home_currency("EUR", actor="cli")
    with operation() as second_write:
        repo.set_home_currency("GBP", actor="cli")

    UndoService(db).undo(second_write, actor="cli")
    assert repo.get_home_currency() == "EUR"

    UndoService(db).undo(first_write, actor="cli")
    assert repo.get_home_currency() is None
