"""Tests for audited profile-level settings (multi-currency Requirement 4)."""

from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo

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


def test_display_currency_targets_start_empty_and_round_trip_normalized(
    repo: ProfileSettingsRepo,
) -> None:
    """Declared display targets are an empty set until explicitly chosen."""
    assert repo.get_display_currency_targets() == ()

    repo.set_display_currency_targets(("eur", "GBP", "EUR"), actor="cli")

    assert repo.get_display_currency_targets() == ("EUR", "GBP")


def test_pre_v062_schema_keeps_home_currency_readable_and_targets_empty(
    db: Database, repo: ProfileSettingsRepo
) -> None:
    """Read-only callers can inspect a pre-V062 profile before migration runs."""
    repo.set_home_currency("EUR", actor="cli")
    db.execute("ALTER TABLE app.profile_settings DROP COLUMN display_currency_targets")

    assert repo.get_home_currency() == "EUR"
    assert repo.get_display_currency_targets() == ()


def test_display_currency_target_change_is_undoable(
    db: Database, repo: ProfileSettingsRepo
) -> None:
    """Undo restores the prior target collection rather than only a scalar field."""
    with operation() as first_write:
        repo.set_display_currency_targets(("EUR",), actor="cli")
    with operation() as second_write:
        repo.set_display_currency_targets(("GBP",), actor="cli")

    UndoService(db).undo(second_write, actor="cli")
    assert repo.get_display_currency_targets() == ("EUR",)

    UndoService(db).undo(first_write, actor="cli")
    assert repo.get_display_currency_targets() == ()


def test_invalid_display_currency_target_leaves_prior_targets_untouched(
    repo: ProfileSettingsRepo,
) -> None:
    """Validation precedes the audited write, so bad target input cannot clobber it."""
    repo.set_display_currency_targets(("EUR",), actor="cli")

    with pytest.raises(ValueError):
        repo.set_display_currency_targets(("not-a-currency",), actor="cli")

    assert repo.get_display_currency_targets() == ("EUR",)


def test_oversized_display_currency_target_set_is_rejected(
    repo: ProfileSettingsRepo,
) -> None:
    """A target count past the bound is refused before the Cartesian blowup.

    ``plan_rate_backfill`` builds held-currency x target pairs, so an unbounded
    set becomes unbounded provider calls each refresh (MB-148 CONSIDER).
    """
    from moneybin.limits import DISPLAY_CURRENCY_TARGETS_MAX_COUNT

    codes = [
        f"{chr(65 + i % 26)}{chr(65 + (i // 26) % 26)}{chr(65 + i // 676)}"
        for i in range(DISPLAY_CURRENCY_TARGETS_MAX_COUNT + 1)
    ]

    with pytest.raises(ValueError, match="exceeds"):
        repo.set_display_currency_targets(codes, actor="cli")

    assert repo.get_display_currency_targets() == ()


def test_home_currency_reads_as_unset_before_the_table_exists(
    db: Database, repo: ProfileSettingsRepo
) -> None:
    """A database that predates V044 reads as "no home currency", not a crash.

    Read-only opens deliberately skip `init_schemas` and the migration runner
    (`Database.__init__` returns at the read-only branch), so the first command
    an upgrading user runs — `moneybin profile show`, `profile` over MCP — hits
    this table before any write-mode open has created it. A raw
    `CatalogException` there is unclassified by `classify_user_error`, so the
    CLI re-raises it as a traceback. "Never chosen" is the honest answer.
    """
    db.execute("DROP TABLE app.profile_settings")

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
    assert '"reporting_timezone":"UTC"' in before.replace(" ", "")
    assert '"home_currency":null' in before.replace(" ", "")
    assert '"home_currency":"EUR"' in after.replace(" ", "")
    assert actor == "cli"


def test_home_currency_is_normalized_before_it_is_persisted(
    repo: ProfileSettingsRepo,
) -> None:
    """Home and display settings use the same canonical ISO spelling."""
    repo.set_home_currency(" eur ", actor="cli")

    assert repo.get_home_currency() == "EUR"


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


@pytest.mark.parametrize("bad", ["EU", "EURO", "", "E1R", "US$"])
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


def test_profile_timezone_survives_host_change(
    db: Database, repo: ProfileSettingsRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import UTC, datetime

    from moneybin.reports.definitions._shared import default_window

    repo.set_reporting_timezone("America/New_York", actor="cli")
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    reopened = ProfileSettingsRepo(db)
    assert reopened.get_reporting_timezone() == "America/New_York"
    assert default_window(
        reporting_timezone=reopened.get_reporting_timezone(),
        now=datetime(2026, 3, 1, 0, 30, tzinfo=UTC),
    ) == ("2025-03", "2026-02")
    before = _audit_rows(db)
    with pytest.raises(ValueError):
        reopened.set_reporting_timezone("Invalid/Timezone", actor="cli")
    assert reopened.get_reporting_timezone() == "America/New_York"
    assert _audit_rows(db) == before


def test_profile_timezone_audit_undo(db: Database, repo: ProfileSettingsRepo) -> None:
    repo.set_reporting_timezone("America/New_York", actor="cli")
    with operation() as change:
        repo.set_reporting_timezone("Asia/Tokyo", actor="cli")
    UndoService(db).undo(change, actor="cli")
    assert repo.get_reporting_timezone() == "America/New_York"


def test_legacy_currency_undo_cannot_remove_reporting_policy(
    db: Database, repo: ProfileSettingsRepo
) -> None:
    from moneybin.errors import UserError
    from moneybin.services.audit_service import AuditService

    repo.set_reporting_timezone("America/New_York", actor="cli")
    repo.set_home_currency("EUR", actor="cli")
    event = AuditService(db).record_audit_event(
        action="profile_settings.set",
        target=("app", "profile_settings", "profile"),
        before=None,
        after={"scope": "profile", "home_currency": "EUR"},
        actor="legacy",
    )
    before = _audit_rows(db)
    with pytest.raises(UserError, match="reporting timezone"):
        repo.undo_event(event, actor="cli")
    assert repo.get_reporting_timezone() == "America/New_York"
    assert repo.get_home_currency() == "EUR"
    assert _audit_rows(db) == before


@pytest.fixture
def host_special_timezone_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    """Model a host whose TZPATH resolves special keys, independently of macOS."""

    def resolve(value: str) -> ZoneInfo:
        if value in {"localtime", "posixrules"} or value.startswith((
            "posix/",
            "right/",
        )):
            return ZoneInfo("UTC")
        return ZoneInfo(value)

    monkeypatch.setattr("moneybin.repositories.profile_settings_repo.ZoneInfo", resolve)


@pytest.mark.parametrize(
    "zone", ["localtime", "posixrules", "posix/America/New_York", "right/UTC"]
)
def test_host_special_timezone_rejected_without_mutation(
    db: Database,
    repo: ProfileSettingsRepo,
    host_special_timezone_resolver: None,
    zone: str,
) -> None:
    repo.set_reporting_timezone("America/New_York", actor="cli")
    before = _audit_rows(db)
    with pytest.raises(ValueError, match="IANA"):
        repo.set_reporting_timezone(zone, actor="cli")
    assert repo.get_reporting_timezone() == "America/New_York"
    assert _audit_rows(db) == before


@pytest.mark.parametrize(
    "zone", ["localtime", "posixrules", "posix/America/New_York", "right/UTC"]
)
def test_existing_host_special_timezone_requires_upgrade(
    db: Database,
    repo: ProfileSettingsRepo,
    host_special_timezone_resolver: None,
    zone: str,
) -> None:
    from moneybin.temporal_contract import (
        UTCUpgradeRequiredError,
        inspect_temporal_contract,
    )

    db.execute("UPDATE app.profile_settings SET reporting_timezone = ?", [zone])
    before = _audit_rows(db)
    with pytest.raises(UTCUpgradeRequiredError):
        repo.get_reporting_timezone()
    assert inspect_temporal_contract(db.conn) == "incomplete"
    assert _audit_rows(db) == before


@pytest.mark.parametrize("zone", ["UTC", "America/New_York", "Etc/UTC", "US/Eastern"])
def test_portable_iana_timezone_keys_remain_available(
    repo: ProfileSettingsRepo, zone: str
) -> None:
    repo.set_reporting_timezone(zone, actor="cli")
    assert repo.get_reporting_timezone() == zone
