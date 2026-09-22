"""account_archive_intent_ambiguous: the acceptance cases.

See docs/specs/reports-net-worth-sql-surface.md §Implementation Plan → Tests.
"""

from __future__ import annotations

from datetime import date

import pytest

from moneybin.database import Database
from moneybin.services.account_service import AccountService
from moneybin.services.audit_service import AuditService
from moneybin.services.doctor_service import DoctorService, InvariantResult
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.db_helpers import create_core_tables

_NAME = "account_archive_intent_ambiguous"
_TARGET = ("app", "account_settings")


def _row(*, archived: bool, include: bool) -> dict[str, object]:
    # Only the keys the invariant reads; legacy audit images are fixtures.
    return {"archived": archived, "include_in_net_worth": include}


@pytest.fixture()
def intent_db(db: Database) -> Database:
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id) VALUES ('acct_a'), ('acct_b')"
    )  # test fixture
    return db


def _legacy_settings_row(db: Database, account_id: str, *, archived: bool) -> None:
    """The stored row a pre-V063 cascade (or a standalone exclude) left behind."""
    db.execute(
        """
        INSERT INTO app.account_settings
            (account_id, archived, archived_at, include_in_net_worth)
        VALUES (?, ?, ?, FALSE)
        """,  # test fixture
        [account_id, archived, date(2026, 1, 15) if archived else None],
    )


def _legacy_audit(
    db: Database,
    account_id: str,
    before: dict[str, object] | None,
    after: dict[str, object],
) -> None:
    """A pre-marker account_settings.set row: no context_json."""
    AuditService(db).record_audit_event(
        action="account_settings.set",
        target=(*_TARGET, account_id),
        before=before,
        after=after,
        actor="cli",
    )


def _cascade_archive(db: Database, account_id: str) -> None:
    """V063-backfilled ambiguous state: one write archived AND forced exclude."""
    _legacy_settings_row(db, account_id, archived=True)
    _legacy_audit(
        db,
        account_id,
        _row(archived=False, include=True),
        _row(archived=True, include=False),
    )


def _result(db: Database) -> InvariantResult:
    return DoctorService(db)._run_account_archive_intent_ambiguous()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.unit
def test_backfilled_cascade_account_warns(intent_db: Database) -> None:
    _cascade_archive(intent_db, "acct_a")
    result = _result(intent_db)
    assert result.name == _NAME
    assert result.status == "warn"
    assert result.affected_ids == ["account:acct_a"]


@pytest.mark.unit
def test_still_warns_after_unarchive(intent_db: Database) -> None:
    _cascade_archive(intent_db, "acct_a")
    AccountService(intent_db).unarchive("acct_a", actor="cli")
    result = _result(intent_db)
    assert result.status == "warn"
    assert result.affected_ids == ["account:acct_a"]


@pytest.mark.unit
@pytest.mark.parametrize("include", [True, False])
def test_explicit_include_or_exclude_clears_it(
    intent_db: Database, include: bool
) -> None:
    # include=False is the idempotent --exclude: the flag stays FALSE, and the
    # marker alone is what settles the account.
    _cascade_archive(intent_db, "acct_a")
    AccountService(intent_db).settings_update(
        "acct_a", include_in_net_worth=include, actor="cli"
    )
    assert _result(intent_db).status == "pass"


def _noop_restate(db: Database, *, account_currency_changed: bool = False) -> None:
    """Stand in for ``restate_fx_accounting`` — see the caller for why."""


@pytest.mark.unit
def test_unrelated_settings_write_does_not_clear_it(
    intent_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # currency_code triggers an FX restate (settings_update); intent_db's
    # create_core_tables fixture makes model_presence.never_built False, so
    # the real restate would try to reach a SQLMesh 'prod' environment that
    # doesn't exist in a unit test. Stub it out like
    # test_unrelated_account_setting_does_not_restate_fx_accounting does.
    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        _noop_restate,
    )
    _cascade_archive(intent_db, "acct_a")
    service = AccountService(intent_db)
    service.settings_update("acct_a", display_name="Renamed", actor="cli")
    service.settings_update("acct_a", currency_code="EUR", actor="cli")
    assert _result(intent_db).affected_ids == ["account:acct_a"]


@pytest.mark.unit
def test_exclude_without_any_archive_row_never_warns(intent_db: Database) -> None:
    _legacy_settings_row(intent_db, "acct_a", archived=False)
    _legacy_audit(
        intent_db,
        "acct_a",
        _row(archived=False, include=True),
        _row(archived=False, include=False),
    )
    assert _result(intent_db).status == "pass"


@pytest.mark.unit
def test_pre_marker_standalone_exclude_then_archive_never_warns(
    intent_db: Database,
) -> None:
    _legacy_settings_row(intent_db, "acct_a", archived=True)
    _legacy_audit(
        intent_db,
        "acct_a",
        _row(archived=False, include=True),
        _row(archived=False, include=False),
    )
    _legacy_audit(
        intent_db,
        "acct_a",
        _row(archived=False, include=False),
        _row(archived=True, include=False),
    )
    assert _result(intent_db).status == "pass"


@pytest.mark.unit
def test_first_write_exclude_with_null_before_then_archive_never_warns(
    intent_db: Database,
) -> None:
    _legacy_settings_row(intent_db, "acct_a", archived=True)
    _legacy_audit(intent_db, "acct_a", None, _row(archived=False, include=False))
    _legacy_audit(
        intent_db,
        "acct_a",
        _row(archived=False, include=False),
        _row(archived=True, include=False),
    )
    assert _result(intent_db).status == "pass"


@pytest.mark.unit
def test_first_write_that_also_archives_is_the_cascade(intent_db: Database) -> None:
    # A NULL before_value alone does not settle: archived=TRUE in the same row
    # is the cascade's signature, not a standalone exclusion.
    _legacy_settings_row(intent_db, "acct_a", archived=True)
    _legacy_audit(intent_db, "acct_a", None, _row(archived=True, include=False))
    assert _result(intent_db).affected_ids == ["account:acct_a"]


@pytest.mark.unit
def test_pre_marker_exclude_superseded_by_later_cascade_warns(
    intent_db: Database,
) -> None:
    # exclude -> include -> archive: the stored FALSE is the cascade's, so the
    # earlier standalone exclusion no longer settles the account.
    _legacy_settings_row(intent_db, "acct_a", archived=True)
    _legacy_audit(
        intent_db,
        "acct_a",
        _row(archived=False, include=True),
        _row(archived=False, include=False),
    )
    _legacy_audit(
        intent_db,
        "acct_a",
        _row(archived=False, include=False),
        _row(archived=False, include=True),
    )
    _legacy_audit(
        intent_db,
        "acct_a",
        _row(archived=False, include=True),
        _row(archived=True, include=False),
    )
    assert _result(intent_db).affected_ids == ["account:acct_a"]


@pytest.mark.unit
def test_pre_marker_exclude_after_cascade_settles(intent_db: Database) -> None:
    # cascade -> unarchive and include -> standalone exclude: the row shape
    # postdates the evidence, so it settles the account without a marker.
    _cascade_archive(intent_db, "acct_a")
    _legacy_audit(
        intent_db,
        "acct_a",
        _row(archived=True, include=False),
        _row(archived=False, include=True),
    )
    _legacy_audit(
        intent_db,
        "acct_a",
        _row(archived=False, include=True),
        _row(archived=False, include=False),
    )
    assert _result(intent_db).status == "pass"


@pytest.mark.unit
def test_pre_v063_archive_of_already_excluded_account_never_warns(
    intent_db: Database,
) -> None:
    # The before image already had include FALSE: the cascade did not write it.
    _legacy_settings_row(intent_db, "acct_a", archived=True)
    _legacy_audit(
        intent_db,
        "acct_a",
        _row(archived=False, include=False),
        _row(archived=True, include=False),
    )
    assert _result(intent_db).status == "pass"


@pytest.mark.unit
def test_only_ambiguous_accounts_are_named(intent_db: Database) -> None:
    _cascade_archive(intent_db, "acct_a")
    _cascade_archive(intent_db, "acct_b")
    AccountService(intent_db).settings_update(
        "acct_b", include_in_net_worth=False, actor="cli"
    )
    assert _result(intent_db).affected_ids == ["account:acct_a"]


@pytest.mark.unit
def test_post_v063_archive_of_legacy_exclusion_never_warns(
    intent_db: Database,
) -> None:
    # An exclusion that predates the audit log, archived today: the retired
    # cascade cannot have written it, so the post-V063 image is no evidence.
    _legacy_settings_row(intent_db, "acct_a", archived=False)
    AccountService(intent_db).archive("acct_a", actor="cli")
    assert _result(intent_db).status == "pass"


@pytest.mark.unit
def test_undone_include_decision_warns_again(intent_db: Database) -> None:
    _cascade_archive(intent_db, "acct_a")
    with operation() as op:
        AccountService(intent_db).settings_update(
            "acct_a", include_in_net_worth=True, actor="cli"
        )
    assert _result(intent_db).status == "pass"
    UndoService(intent_db).undo(op, actor="cli")
    result = _result(intent_db)
    assert result.status == "warn"
    assert result.affected_ids == ["account:acct_a"]


@pytest.mark.unit
def test_undo_row_never_settles(intent_db: Database) -> None:
    # An undo row shaped like a standalone exclusion must not settle the account.
    _cascade_archive(intent_db, "acct_a")
    AuditService(intent_db).record_audit_event(
        action="account_settings.set.undo",
        target=(*_TARGET, "acct_a"),
        before=_row(archived=False, include=True),
        after=_row(archived=False, include=False),
        actor="cli",
    )
    assert _result(intent_db).affected_ids == ["account:acct_a"]


@pytest.mark.unit
def test_account_number_shaped_id_is_masked(intent_db: Database) -> None:
    raw = "9876543210"  # synthetic source-native account number
    intent_db.execute(
        "INSERT INTO core.dim_accounts (account_id) VALUES (?)", [raw]
    )  # test fixture
    _cascade_archive(intent_db, raw)
    (affected,) = _result(intent_db).affected_ids
    assert affected.startswith("account:")
    assert raw not in affected
