"""Tests for ``AccountSettingsRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, and that ``before_value``
captures the FULL prior row (Req 4).
"""

from __future__ import annotations

import json
from collections.abc import Generator
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest

from moneybin.database import Database
from moneybin.repositories.account_settings_repo import AccountSettingsRepo
from moneybin.services.audit_service import AuditEvent
from tests.moneybin.test_repositories.conftest import audit_rows_for as _audit_rows_for
from tests.moneybin.test_repositories.conftest import metric_for

_metric = metric_for("account_settings")


def _set(repo: AccountSettingsRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "account_id": "acct_a",
        "display_name": "Checking",
        "official_name": None,
        "last_four": "1234",
        "account_subtype": "checking",
        "holder_category": "personal",
        "currency_code": "USD",
        "credit_limit": None,
        "archived": False,
        "archived_at": None,
        "include_in_net_worth": True,
        "default_cost_basis_method": None,
        "actor": "cli",
    }
    kwargs.update(overrides)
    return repo.set(**kwargs)


def test_set_inserts_row_and_audit(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    before_metric = _metric("account_settings.set")

    event = _set(repo)
    assert event.target_id == "acct_a"

    row = db.conn.execute(
        "SELECT display_name, last_four, archived FROM app.account_settings "
        "WHERE account_id = ?",
        ["acct_a"],
    ).fetchone()
    assert row == ("Checking", "1234", False)

    audit = _audit_rows_for(db, "acct_a")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "account_settings.set"
    assert (schema, table, target_id) == ("app", "account_settings", "acct_a")
    assert before is None
    assert json.loads(after)["display_name"] == "Checking"
    assert actor == "cli"

    assert _metric("account_settings.set") - before_metric == 1.0


def test_set_update_captures_full_prior_row(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    _set(repo, display_name="Old", credit_limit=Decimal("500.00"))
    _set(repo, display_name="New", credit_limit=Decimal("750.00"))

    row = db.conn.execute(
        "SELECT display_name FROM app.account_settings WHERE account_id = ?",
        ["acct_a"],
    ).fetchone()
    assert row == ("New",)

    # The second set's audit row captures the FULL prior row in before.
    update_audit = _audit_rows_for(db, "acct_a")[1]
    before = json.loads(update_audit[4])
    after = json.loads(update_audit[5])
    assert before["display_name"] == "Old"
    assert before["credit_limit"] == "500.00"  # Decimal serialized losslessly as str
    assert after["display_name"] == "New"
    assert after["credit_limit"] == "750.00"


def test_set_records_parent_audit_id(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    event = _set(repo, parent_audit_id="p1")
    assert _audit_rows_for(db, event.target_id or "")[0][7] == "p1"


def test_set_persists_default_cost_basis_method(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    _set(repo, default_cost_basis_method="average")

    row = db.conn.execute(
        "SELECT default_cost_basis_method FROM app.account_settings "
        "WHERE account_id = ?",
        ["acct_a"],
    ).fetchone()
    assert row == ("average",)


def test_set_invalid_default_cost_basis_method_raises_constraint_exception(
    db: Database,
) -> None:
    repo = AccountSettingsRepo(db)
    with pytest.raises(duckdb.ConstraintException):
        _set(repo, default_cost_basis_method="lifo")


def test_delete_captures_before_and_returns_event(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    _set(repo, display_name="ToDelete")

    event = repo.delete("acct_a", actor="cli")
    assert event is not None

    assert (
        db.conn.execute(
            "SELECT 1 FROM app.account_settings WHERE account_id = ?", ["acct_a"]
        ).fetchone()
        is None
    )

    delete_audit = next(
        r for r in _audit_rows_for(db, "acct_a") if r[0] == "account_settings.delete"
    )
    assert json.loads(delete_audit[4])["display_name"] == "ToDelete"
    assert delete_audit[5] is None  # after is None for DELETE


def test_delete_returns_none_for_missing_row(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    assert repo.delete("nope", actor="cli") is None
    assert _audit_rows_for(db, "nope") == []


def test_set_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = AccountSettingsRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _set(repo, account_id="ghost", display_name="Ghost")

    rows = db.conn.execute(
        "SELECT 1 FROM app.account_settings WHERE account_id = ?", ["ghost"]
    ).fetchall()
    assert rows == []


def _legacy_row(*, account_id: str, archived: bool) -> dict[str, Any]:
    """A full-row capture shaped like a pre-V062 audit image (no archived_at key)."""
    return {
        "account_id": account_id,
        "display_name": "Legacy Account",
        "official_name": None,
        "last_four": None,
        "account_subtype": None,
        "holder_category": None,
        "currency_code": None,
        "credit_limit": None,
        "archived": archived,
        "include_in_net_worth": True,
        "default_cost_basis_method": None,
        "updated_at": "2025-06-01T00:00:00",
    }


def _legacy_undo_event(
    *, account_id: str, before_archived: bool, after_archived: bool
) -> AuditEvent:
    return AuditEvent(
        audit_id="aud-legacy1",
        occurred_at="2025-06-01T00:00:00",
        actor="cli",
        action="account_settings.set",
        target_schema="app",
        target_table="account_settings",
        target_id=account_id,
        before_value=_legacy_row(account_id=account_id, archived=before_archived),
        after_value=_legacy_row(account_id=account_id, archived=after_archived),
        parent_audit_id=None,
        operation_id="op-legacy1",
    )


def test_undo_of_legacy_archive_row_derives_archived_at_from_today(
    db: Database,
) -> None:
    """Undo of a legacy 'archive' row must clear a stale archived_at, not skip it."""
    repo = AccountSettingsRepo(db)
    # Current DB state matches the event's "after" image: archived=True with
    # a stale date, standing in for whatever archived_at happens to hold —
    # proving the clear fires, not just that None was already there.
    _set(repo, account_id="acct_legacy", archived=True, archived_at=date(2025, 1, 1))

    # Forward mutation this reverses was the original archive: False -> True.
    event = _legacy_undo_event(
        account_id="acct_legacy", before_archived=False, after_archived=True
    )
    repo.undo_event(event, actor="cli")

    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_legacy"],
    ).fetchone()
    assert row == (False, None)


def test_undo_of_legacy_unarchive_row_derives_archived_at_today_on_rearchive(
    db: Database,
) -> None:
    """Undo of a legacy 'unarchive' row re-archives; archived_at must become today."""
    repo = AccountSettingsRepo(db)
    _set(repo, account_id="acct_legacy2", archived=False, archived_at=None)

    # Forward mutation this reverses was an unarchive: True -> False.
    event = _legacy_undo_event(
        account_id="acct_legacy2", before_archived=True, after_archived=False
    )
    repo.undo_event(event, actor="cli")

    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_legacy2"],
    ).fetchone()
    assert row == (True, date.today())


def test_undo_of_undo_of_legacy_unarchive_row_leaves_active_account_null(
    db: Database,
) -> None:
    """Redoing a legacy unarchive-undo must not leave a date on an active account.

    Undoing a legacy 'unarchive' row re-archives and stamps today's date
    (the prior test). Undoing THAT undo (a redo, back to active) must NOT
    reuse the same derived date for both images: ``locate`` (the state
    right before the first undo -- ``archived=False``) is not a live
    transition and has no recoverable real date, so it must normalize to
    NULL, never today's. Deriving one shared date for both images stamps
    today's date onto that NULL-while-active image too; when THIS undo's
    own emitted audit row is later replayed, its ``before_value`` already
    carries ``archived_at`` (Req 4 complete) and takes the base-class
    literal-restore path, landing the live row on ``archived=False`` with
    a non-NULL ``archived_at`` -- violating the column's own documented
    contract.
    """
    repo = AccountSettingsRepo(db)
    _set(repo, account_id="acct_legacy8", archived=False, archived_at=None)

    # Forward mutation this reverses was an unarchive: True -> False.
    event = _legacy_undo_event(
        account_id="acct_legacy8", before_archived=True, after_archived=False
    )
    undo_result = repo.undo_event(event, actor="cli")
    assert undo_result is not None
    # Sanity: the first undo re-archives and stamps today, as above.
    mid_row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_legacy8"],
    ).fetchone()
    assert mid_row == (True, date.today())

    # Undo the undo (redo): back to active. Must clear archived_at, not
    # reuse today's date on an archived=False row.
    redo_result = repo.undo_event(undo_result, actor="cli")
    assert redo_result is not None

    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_legacy8"],
    ).fetchone()
    assert row == (False, None)

    # The redo's own emitted audit row must also carry NULL, or a further
    # undo-of-this-redo reproduces the bug via the literal-restore path.
    assert redo_result.after_value is not None
    assert redo_result.after_value["archived_at"] is None


def test_undo_of_current_capture_leaves_archived_at_override_untouched(
    db: Database,
) -> None:
    """A post-V062 capture already carries archived_at; the override must not clobber it."""
    repo = AccountSettingsRepo(db)
    _set(repo, account_id="acct_current", archived=True, archived_at=date(2026, 1, 10))
    set_event = _set(repo, account_id="acct_current", archived=False, archived_at=None)

    repo.undo_event(set_event, actor="cli")

    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_current"],
    ).fetchone()
    # Restored to the captured before-image exactly: archived=True with the
    # real historical date, not today's.
    assert row == (True, date(2026, 1, 10))


def test_undo_of_legacy_row_with_unchanged_archived_leaves_archived_at_alone(
    db: Database,
) -> None:
    """A legacy row whose `archived` didn't change must not touch archived_at.

    The forward mutation this reverses changed `display_name` only --
    `archived=True` in both its before and after image -- so this is NOT an
    archive/unarchive transition. Undoing it must leave the live
    `archived_at` at its real historical value rather than deriving today's
    date from `before.get("archived")` alone. (Not built via
    `_legacy_undo_event`: identical before/after images there would make
    `BaseRepo.undo_event` treat the whole event as a no-op and never reach
    `_restore_row` at all -- this needs a before/after pair that differs on
    a non-`archived` field.)
    """
    repo = AccountSettingsRepo(db)
    _set(repo, account_id="acct_legacy3", archived=True, archived_at=date(2024, 3, 15))

    before_image = _legacy_row(account_id="acct_legacy3", archived=True)
    after_image = dict(before_image)
    after_image["display_name"] = "Legacy Account Renamed"
    event = AuditEvent(
        audit_id="aud-legacy3",
        occurred_at="2025-06-01T00:00:00",
        actor="cli",
        action="account_settings.set",
        target_schema="app",
        target_table="account_settings",
        target_id="acct_legacy3",
        before_value=before_image,
        after_value=after_image,
        parent_audit_id=None,
        operation_id="op-legacy3",
    )
    repo.undo_event(event, actor="cli")

    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_legacy3"],
    ).fetchone()
    assert row == (True, date(2024, 3, 15))


def test_undo_of_legacy_row_with_unchanged_archived_captures_live_value_in_own_audit(
    db: Database,
) -> None:
    """A non-transition legacy undo's own audit row must still carry archived_at.

    Codex PR #596 P2 round 2 (account_settings_repo.py:293): when the forward
    event changed a field OTHER than ``archived``, this repo previously
    returned without ever adding ``archived_at`` to either image -- leaving
    the audit event ``undo_event`` emits from these same dicts (``before=
    after``, ``after=before``) with an incomplete row capture even on a
    migrated catalog that fully supports the column, so ``system_audit``
    could not tell a persisted ``NULL`` from a genuinely missing legacy
    field. The live column is never touched by the base class's ``UPDATE``
    here (``archived_at`` isn't a key in ``before``), so the fix reads the
    live value once and records it, unchanged, on both images instead of
    leaving the key out.
    """
    repo = AccountSettingsRepo(db)
    _set(repo, account_id="acct_legacy11", archived=True, archived_at=date(2024, 3, 15))

    before_image = _legacy_row(account_id="acct_legacy11", archived=True)
    after_image = dict(before_image)
    after_image["display_name"] = "Legacy Account Renamed"
    event = AuditEvent(
        audit_id="aud-legacy11",
        occurred_at="2025-06-01T00:00:00",
        actor="cli",
        action="account_settings.set",
        target_schema="app",
        target_table="account_settings",
        target_id="acct_legacy11",
        before_value=before_image,
        after_value=after_image,
        parent_audit_id=None,
        operation_id="op-legacy11",
    )
    undo_result = repo.undo_event(event, actor="cli")

    assert undo_result is not None
    assert undo_result.before_value is not None
    assert undo_result.after_value is not None
    # undo_event emits before=after, after=before (swapped) -- both original
    # images now carry the same live, unchanged archived_at instead of
    # neither carrying the key.
    assert undo_result.before_value["archived_at"] == "2024-03-15"
    assert undo_result.after_value["archived_at"] == "2024-03-15"


def test_undo_of_legacy_row_normalizes_archived_at_into_its_own_audit(
    db: Database,
) -> None:
    """The undo's own emitted audit row must carry the archived_at it derived.

    Otherwise undoing THIS undo re-derives from whatever "today" is at redo
    time instead of restoring the date this undo just wrote.
    """
    repo = AccountSettingsRepo(db)
    _set(repo, account_id="acct_legacy4", archived=False, archived_at=None)

    event = _legacy_undo_event(
        account_id="acct_legacy4", before_archived=True, after_archived=False
    )
    undo_result = repo.undo_event(event, actor="cli")

    assert undo_result is not None
    assert undo_result.after_value is not None
    assert undo_result.after_value["archived_at"] == date.today().isoformat()


def test_undo_of_legacy_archive_row_preserves_live_archived_at_in_capture(
    db: Database,
) -> None:
    """A migrated catalog's real archived_at must survive into the undo's capture.

    Codex PR #596 P2 (thread ``PRRT_kwDOPjlNiM6h1uiX``, anchored
    ``account_settings_repo.py:248``). Mirror-image fixture, isolated to a
    single undo hop: the LIVE ``app.account_settings`` catalog HAS
    ``archived_at`` (migrated), and the row already carries a real date --
    standing in for a V062 backfill or any other post-V062 write -- even
    though the AUDIT ROW being undone is a legacy (pre-V062) capture missing
    the key from both its images. Per ``_archived_at_supported``'s invariant,
    the guard must be invisible on a migrated catalog: undoing this legacy
    event must not clobber that real, recoverable value with a guessed NULL.
    """
    repo = AccountSettingsRepo(db)
    _set(repo, account_id="acct_legacy9", archived=True, archived_at=date(2023, 11, 2))

    # Forward mutation this reverses was the original archive: False -> True.
    event = _legacy_undo_event(
        account_id="acct_legacy9", before_archived=False, after_archived=True
    )
    undo_result = repo.undo_event(event, actor="cli")

    assert undo_result is not None
    # The live row is correctly unarchived with archived_at cleared going
    # forward -- clearing on unarchive is right regardless of catalog state.
    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_legacy9"],
    ).fetchone()
    assert row == (False, None)
    # But the real pre-undo date must survive in the undo's own before_value
    # capture -- not be reported as an unrecoverable NULL, which is what a
    # later redo (undo-of-this-undo) would restore from.
    assert undo_result.before_value is not None
    assert undo_result.before_value["archived_at"] == "2023-11-02"


def test_undo_of_undo_of_legacy_archive_row_succeeds(db: Database) -> None:
    """Undoing a legacy archive-undo a second time must not lose its date.

    A redo must not raise, and must not destroy a real, recoverable archive
    date along the way.

    Codex PR #596 P2 (thread ``PRRT_kwDOPjlNiM6h1uiX``, anchored
    ``account_settings_repo.py:248``). The live row here already carries a
    real ``archived_at`` (``2024-03-15``, standing in for a V062 backfill or
    any other post-V062 write) BEFORE the undo below ever runs -- i.e. the
    catalog is migrated, even though the audit row being undone is a legacy
    (pre-V062) capture missing the key entirely. Per ``_archived_at_supported``'s
    invariant, the guard must be invisible on a migrated catalog: it must not
    destroy a value the live row actually holds.

    ``_restore_row`` must also normalize BOTH the ``before`` and ``locate``
    images it derives ``archived_at`` into, not just ``before``:
    ``BaseRepo.undo_event`` emits its own audit row as
    ``before=locate, after=before`` (Req 4), so a row missing ``archived_at``
    on only one side makes the newly emitted undo row asymmetric --
    ``after_value`` carries the key while ``before_value`` doesn't. Redoing
    that undo then hits ``_require_capture`` and is wrongly rejected as "not
    reversible", even though it is a brand-new, fully generated audit row and
    not a legacy one.
    """
    repo = AccountSettingsRepo(db)
    _set(repo, account_id="acct_legacy5", archived=True, archived_at=date(2024, 3, 15))

    # Forward mutation this reverses was the original archive: False -> True.
    event = _legacy_undo_event(
        account_id="acct_legacy5", before_archived=False, after_archived=True
    )
    undo_result = repo.undo_event(event, actor="cli")
    assert undo_result is not None
    # The first undo (unarchive) correctly clears archived_at going forward,
    # but must capture the real 2024-03-15 it just overwrote in its own
    # before_value -- the only place that date survives past this call.
    assert undo_result.before_value is not None
    assert undo_result.before_value["archived_at"] == "2024-03-15"

    # Undo the undo (redo the archive). Must succeed, not raise UserError.
    redo_result = repo.undo_event(undo_result, actor="cli")

    assert redo_result is not None
    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_legacy5"],
    ).fetchone()
    # By the time this redo runs, its own audit row carries an explicit
    # archived_at on both images -- a full capture per Req 4 -- so it takes
    # BaseRepo's literal-restore path, not the legacy-derivation branch
    # (which only fires when the key is absent). That literal value is the
    # real 2024-03-15 captured above, not a guessed NULL -- the account's
    # real archive date predates archived_at tracking on the AUDIT ROW, but
    # was never actually lost from the LIVE catalog, so recovering it here is
    # correct, not a guess.
    assert row == (True, date(2024, 3, 15))


def test_undo_of_undo_of_legacy_first_write_archive_normalizes_archived_at(
    db: Database,
) -> None:
    """Undo-of-undo of a pre-V062 FIRST-write archive must preserve archived_at.

    When archiving was a pre-V062 account's first settings write, its
    ``before_value`` is NULL, so undoing that event takes ``undo_event``'s
    INSERT-shaped path (``before is None`` -> ``_delete_by_pk``), never
    ``_restore_row``. The live row already carries a real, V062-backfilled
    ``archived_at`` (2024-06-01) at undo time, even though the legacy
    captured ``after`` image has no ``archived_at`` key at all --
    ``_delete_by_pk`` must copy that live value into the capture before the
    DELETE erases it. Undoing THAT generated undo (a redo) then reinserts the
    captured row through ``_insert_row``: with the date now present in the
    capture, it must restore the real 2024-06-01, not synthesize today's date.

    Codex PR #596 P2 (comment 3998603776): before this fix, ``_delete_by_pk``
    dropped the live date on the floor and ``_insert_row`` guessed
    ``date.today()`` on redo -- this test used to assert exactly that guess,
    pinning the bug in place.
    """
    repo = AccountSettingsRepo(db)
    # The live row mirrors what the first-write archive produced, with
    # archived_at already backfilled by V062.
    _set(repo, account_id="acct_legacy6", archived=True, archived_at=date(2024, 6, 1))

    # The forward event this reverses was the account's FIRST settings write
    # (before=NULL) -- a pre-V062 capture, so its after-image has no
    # archived_at key.
    first_write_event = AuditEvent(
        audit_id="aud-legacy6",
        occurred_at="2024-06-01T00:00:00",
        actor="cli",
        action="account_settings.set",
        target_schema="app",
        target_table="account_settings",
        target_id="acct_legacy6",
        before_value=None,
        after_value=_legacy_row(account_id="acct_legacy6", archived=True),
        parent_audit_id=None,
        operation_id="op-legacy6",
    )

    # Undo #1: before is None -> deletes the row via _delete_by_pk.
    undo_result = repo.undo_event(first_write_event, actor="cli")
    assert undo_result is not None
    assert (
        db.conn.execute(
            "SELECT 1 FROM app.account_settings WHERE account_id = ?",
            ["acct_legacy6"],
        ).fetchone()
        is None
    )
    # The real live date must survive into the undo's own before_value
    # capture -- not be reported as an unrecoverable NULL/missing key, which
    # is what a later redo would restore from.
    assert undo_result.before_value is not None
    assert undo_result.before_value["archived_at"] == "2024-06-01"

    # Undo #2 (redo): after is None -> reinserts via _insert_row, not
    # _restore_row.
    redo_result = repo.undo_event(undo_result, actor="cli")
    assert redo_result is not None

    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_legacy6"],
    ).fetchone()
    assert row == (True, date(2024, 6, 1))

    # The redo's own emitted audit row must carry the preserved date, not a
    # freshly guessed one -- otherwise undoing THIS redo repeats the loss.
    assert redo_result.after_value is not None
    assert redo_result.after_value["archived_at"] == "2024-06-01"


def test_undo_of_undo_of_legacy_first_write_unarchive_leaves_archived_at_null(
    db: Database,
) -> None:
    """The insert-path normalization only fires for a reinstated archive=True row.

    A legacy first-write whose captured value was ``archived=False`` must
    reinsert with ``archived_at`` left NULL -- the natural state for an
    active account -- not stamped with today's date.
    """
    repo = AccountSettingsRepo(db)
    _set(repo, account_id="acct_legacy7", archived=False, archived_at=None)

    first_write_event = AuditEvent(
        audit_id="aud-legacy7",
        occurred_at="2024-06-01T00:00:00",
        actor="cli",
        action="account_settings.set",
        target_schema="app",
        target_table="account_settings",
        target_id="acct_legacy7",
        before_value=None,
        after_value=_legacy_row(account_id="acct_legacy7", archived=False),
        parent_audit_id=None,
        operation_id="op-legacy7",
    )

    undo_result = repo.undo_event(first_write_event, actor="cli")
    assert undo_result is not None
    redo_result = repo.undo_event(undo_result, actor="cli")
    assert redo_result is not None

    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_legacy7"],
    ).fetchone()
    assert row == (False, None)


class TestPreV062SchemaToleranceOnAccountSettingsWrite:
    """A write-mode open must tolerate account_settings predating archived_at.

    Codex PR #596 P2 (thread ``PRRT_kwDOPjlNiM6h1iuP``) plus the wider grid
    found alongside it. ``Database.__init__`` calls ``init_schemas()`` (``CREATE TABLE IF NOT
    EXISTS``, a no-op on an existing table) unconditionally in EVERY open --
    read or write -- before the explicit ``no_auto_upgrade`` gate decides
    whether pending migrations run at all. So a profile opened with
    ``no_auto_upgrade=True`` never gets V062 applied, not just transiently.
    Unlike the legacy-audit-capture tests above (which simulate a pre-V062
    *audit row* on an already-migrated live table), every fixture here drops
    the column from the LIVE table and never adds it back.
    """

    @pytest.fixture()
    def pre_v062_rw_db(
        self, db: Database, mock_secret_store: MagicMock
    ) -> Generator[Database, None, None]:
        """A real write-mode Database reopened over account_settings missing archived_at."""
        db.execute("ALTER TABLE app.account_settings DROP COLUMN archived_at")
        db_path = db.path
        db.close()
        rw_db = Database(
            db_path,
            secret_store=mock_secret_store,
            no_auto_upgrade=True,
            read_only=False,
        )
        yield rw_db
        rw_db.close()

    def test_set_insert_succeeds(self, pre_v062_rw_db: Database) -> None:
        """Cell 2: the INSERT column list must drop archived_at when absent."""
        repo = AccountSettingsRepo(pre_v062_rw_db)
        event = _set(repo, account_id="acct_pre_v062")
        assert event.target_id == "acct_pre_v062"

        row = pre_v062_rw_db.conn.execute(
            "SELECT display_name, archived FROM app.account_settings "
            "WHERE account_id = ?",
            ["acct_pre_v062"],
        ).fetchone()
        assert row == ("Checking", False)

    def test_set_on_conflict_update_succeeds(self, pre_v062_rw_db: Database) -> None:
        """Cell 2: the ON CONFLICT DO UPDATE SET list must also drop archived_at."""
        repo = AccountSettingsRepo(pre_v062_rw_db)
        _set(repo, account_id="acct_pre_v062", display_name="Old")
        _set(repo, account_id="acct_pre_v062", display_name="New")

        row = pre_v062_rw_db.conn.execute(
            "SELECT display_name FROM app.account_settings WHERE account_id = ?",
            ["acct_pre_v062"],
        ).fetchone()
        assert row == ("New",)

    def test_delete_before_capture_succeeds(self, pre_v062_rw_db: Database) -> None:
        """Cell 3: _fetch_row's projection must not crash delete()'s before-capture."""
        repo = AccountSettingsRepo(pre_v062_rw_db)
        _set(repo, account_id="acct_pre_v062")

        event = repo.delete("acct_pre_v062", actor="cli")
        assert event is not None
        assert event.before_value is not None
        assert event.before_value["display_name"] == "Checking"
        assert "archived_at" not in event.before_value

    def test_undo_of_archive_transition_succeeds(
        self, pre_v062_rw_db: Database
    ) -> None:
        """Cell 4: _restore_row's derived UPDATE must not crash when absent.

        It must skip the derivation entirely when the column is absent.
        """
        repo = AccountSettingsRepo(pre_v062_rw_db)
        _set(repo, account_id="acct_pre_v062", archived=False)
        archive_event = _set(repo, account_id="acct_pre_v062", archived=True)
        assert "archived_at" not in (archive_event.before_value or {})
        assert "archived_at" not in (archive_event.after_value or {})

        undo_result = repo.undo_event(archive_event, actor="cli")
        assert undo_result is not None

        row = pre_v062_rw_db.conn.execute(
            "SELECT archived FROM app.account_settings WHERE account_id = ?",
            ["acct_pre_v062"],
        ).fetchone()
        assert row == (False,)

    def test_undo_of_delete_of_archived_account_succeeds(
        self, pre_v062_rw_db: Database
    ) -> None:
        """Discovered alongside cell 4: _insert_row's own backfill must skip too.

        The undo-of-DELETE path must skip deriving a value when the live
        catalog has no column to write it into -- the same failure mode as
        cell 4, one hop over in BaseRepo.undo_event's dispatch.
        """
        repo = AccountSettingsRepo(pre_v062_rw_db)
        _set(repo, account_id="acct_pre_v062", archived=True)
        delete_event = repo.delete("acct_pre_v062", actor="cli")
        assert delete_event is not None

        undo_result = repo.undo_event(delete_event, actor="cli")
        assert undo_result is not None

        row = pre_v062_rw_db.conn.execute(
            "SELECT archived FROM app.account_settings WHERE account_id = ?",
            ["acct_pre_v062"],
        ).fetchone()
        assert row == (True,)

    def test_undo_of_first_write_archive_succeeds(
        self, pre_v062_rw_db: Database
    ) -> None:
        """_delete_by_pk's live-date read must skip too when the column is absent.

        The undo-of-INSERT path (BaseRepo.undo_event's ``before is None``
        branch, undoing an account's first-ever settings write) must not
        attempt to SELECT a column the live catalog does not have -- the same
        failure mode cells 4 and the DELETE-path test above guard against,
        one more hop over in BaseRepo.undo_event's dispatch.
        """
        repo = AccountSettingsRepo(pre_v062_rw_db)
        first_write_event = _set(repo, account_id="acct_pre_v062", archived=True)
        assert first_write_event.before_value is None
        assert "archived_at" not in (first_write_event.after_value or {})

        undo_result = repo.undo_event(first_write_event, actor="cli")
        assert undo_result is not None

        assert (
            pre_v062_rw_db.conn.execute(
                "SELECT 1 FROM app.account_settings WHERE account_id = ?",
                ["acct_pre_v062"],
            ).fetchone()
            is None
        )
