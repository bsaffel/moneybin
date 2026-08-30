"""Tests for ``ExchangeRateOverridesRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, that ``before_value`` captures
the FULL prior row (Req 4), and that the composite ``(from_currency,
to_currency, rate_date)`` primary key maps to a single composite ``target_id``.

``delete`` is not CRUD symmetry. An override outranks the cached provider rate
for its pair and date, and ``set`` can only replace the value — so without
``delete`` a correction is unreachable once written and that date can never
return to the provider's published rate.
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.exchange_rate_repo import ExchangeRateOverridesRepo
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_repositories.conftest import audit_rows_for as _audit_rows_for
from tests.moneybin.test_repositories.conftest import metric_for

_metric = metric_for("exchange_rate_overrides")


_DAY = date(2026, 3, 16)
_TARGET = "USD|EUR|2026-03-16"


def test_set_inserts_and_emits_paired_audit(db: Database) -> None:
    repo = ExchangeRateOverridesRepo(db)
    before_metric = _metric("exchange_rate_override.set")

    event = repo.set(
        "USD",
        "EUR",
        _DAY,
        rate=Decimal("0.93000000"),
        note="bank rate, not ECB mid",
        actor="cli",
    )

    row = db.execute(
        "SELECT rate, note FROM app.exchange_rate_overrides "
        "WHERE from_currency = ? AND to_currency = ? AND rate_date = ?",
        ["USD", "EUR", _DAY],
    ).fetchone()
    assert row == (Decimal("0.93000000"), "bank rate, not ECB mid")

    audits = _audit_rows_for(db, _TARGET)
    assert len(audits) == 1
    action, schema, table, target_id, before, after, actor, parent = audits[0]
    assert action == "exchange_rate_override.set"
    assert (schema, table, target_id) == ("app", "exchange_rate_overrides", _TARGET)
    assert before is None, "a first write has no before state"
    # The audit serializes DECIMAL(18,8) as its full-scale string, so compare
    # the numeric value rather than an incidental text form.
    assert Decimal(json.loads(after)["rate"]) == Decimal("0.93")
    assert (actor, parent) == ("cli", None)
    assert event.audit_id
    assert _metric("exchange_rate_override.set") == before_metric + 1


def test_set_twice_updates_and_captures_full_prior_row(db: Database) -> None:
    """The correction's before_value must carry the whole prior row, not a diff."""
    repo = ExchangeRateOverridesRepo(db)
    repo.set("USD", "EUR", _DAY, rate=Decimal("0.93"), note="first", actor="cli")
    repo.set("USD", "EUR", _DAY, rate=Decimal("0.94"), note="corrected", actor="cli")

    row = db.execute(
        "SELECT rate, note FROM app.exchange_rate_overrides "
        "WHERE from_currency = ? AND to_currency = ?",
        ["USD", "EUR"],
    ).fetchone()
    assert row == (Decimal("0.94000000"), "corrected")

    audits = _audit_rows_for(db, _TARGET)
    assert len(audits) == 2
    before = json.loads(audits[1][4])
    assert Decimal(before["rate"]) == Decimal("0.93")
    assert before["note"] == "first"
    assert before["from_currency"] == "USD"


def test_set_preserves_created_at_on_update(db: Database) -> None:
    repo = ExchangeRateOverridesRepo(db)
    repo.set("USD", "EUR", _DAY, rate=Decimal("0.93"), note=None, actor="cli")
    created = db.execute(
        "SELECT created_at FROM app.exchange_rate_overrides WHERE from_currency = ?",
        ["USD"],
    ).fetchone()
    assert created is not None

    repo.set("USD", "EUR", _DAY, rate=Decimal("0.94"), note=None, actor="cli")
    after = db.execute(
        "SELECT created_at FROM app.exchange_rate_overrides WHERE from_currency = ?",
        ["USD"],
    ).fetchone()
    assert after is not None and after[0] == created[0]


def test_set_scopes_the_correction_to_one_direction(db: Database) -> None:
    """USD→EUR and EUR→USD are different rates, not two spellings of one.

    Both directions of a pair are legitimate rows for the same date, and their
    values are reciprocals rather than equal — a repo that keyed on an unordered
    pair would answer one direction's conversion with the other's rate.
    """
    repo = ExchangeRateOverridesRepo(db)
    repo.set("USD", "EUR", _DAY, rate=Decimal("0.93"), note=None, actor="cli")
    repo.set("EUR", "USD", _DAY, rate=Decimal("1.07"), note=None, actor="cli")

    rows = db.execute(
        "SELECT from_currency, to_currency, rate FROM app.exchange_rate_overrides "
        "WHERE rate_date = ? ORDER BY from_currency",
        [_DAY],
    ).fetchall()
    assert rows == [
        ("EUR", "USD", Decimal("1.07000000")),
        ("USD", "EUR", Decimal("0.93000000")),
    ]
    assert len(_audit_rows_for(db, _TARGET)) == 1, "USD→EUR must see one write"


def test_delete_removes_the_override_and_emits_audit(db: Database) -> None:
    repo = ExchangeRateOverridesRepo(db)
    repo.set("USD", "EUR", _DAY, rate=Decimal("0.93"), note="typo", actor="cli")

    event = repo.delete("USD", "EUR", _DAY, actor="cli")

    row = db.execute(
        "SELECT COUNT(*) FROM app.exchange_rate_overrides WHERE from_currency = ?",
        ["USD"],
    ).fetchone()
    assert row is not None and row[0] == 0

    audits = _audit_rows_for(db, _TARGET)
    assert len(audits) == 2
    action, _, _, _, before, after, _, _ = audits[1]
    assert action == "exchange_rate_override.delete"
    assert Decimal(json.loads(before)["rate"]) == Decimal("0.93")
    assert after is None
    assert event is not None and event.audit_id


def test_delete_of_a_missing_override_is_a_silent_no_op(db: Database) -> None:
    repo = ExchangeRateOverridesRepo(db)
    before_metric = _metric("exchange_rate_override.delete")

    assert repo.delete("USD", "EUR", _DAY, actor="cli") is None

    assert _audit_rows_for(db, _TARGET) == []
    assert _metric("exchange_rate_override.delete") == before_metric


def test_parent_audit_id_threads_through(db: Database) -> None:
    """A cascading user action shares one audit chain."""
    repo = ExchangeRateOverridesRepo(db)
    first = repo.set("USD", "EUR", _DAY, rate=Decimal("0.93"), note=None, actor="mcp")
    repo.set(
        "USD",
        "EUR",
        date(2026, 3, 17),
        rate=Decimal("0.94"),
        note=None,
        actor="mcp",
        parent_audit_id=first.audit_id,
    )
    row = db.execute(
        "SELECT parent_audit_id FROM app.audit_log WHERE target_id = ?",
        ["USD|EUR|2026-03-17"],
    ).fetchone()
    assert row is not None and row[0] == first.audit_id


def test_undoing_an_override_scopes_its_audit_row_to_the_composite_key(
    db: Database,
) -> None:
    """An undo must record the same target_id its forward mutation did.

    ``BaseRepo._row_target_id`` defaults to the FIRST pk column —
    ``from_currency`` alone, a value every override out of USD shares — while
    ``set``/``delete`` both emit the composite
    ``"{from_currency}|{to_currency}|{rate_date}"``. Undo rows landing under a
    different id than the mutations they reverse is what defeats the cascade
    guard in the next test; this asserts the id directly so the cause is
    distinguishable from the effect.
    """
    repo = ExchangeRateOverridesRepo(db)
    event = repo.set("USD", "EUR", _DAY, rate=Decimal("0.93"), note=None, actor="cli")

    repo.undo_event(event, actor="cli")

    actions = [row[0] for row in _audit_rows_for(db, _TARGET)]
    assert actions == [
        "exchange_rate_override.set",
        "exchange_rate_override.set.undo",
    ]


def test_undoing_an_undo_will_not_clobber_a_later_correction(db: Database) -> None:
    """Block-don't-cascade has to see the later write in order to refuse.

    The user overrides a rate, undoes it, then re-overrides the same date at a
    corrected value. Undoing the undo would re-insert the ORIGINAL rate over that
    correction. ``UndoService`` refuses exactly this by looking for later
    operations against the same ``(target_schema, target_table, target_id)`` — so
    an undo row keyed on ``from_currency`` alone matches nothing, finds no
    blocker, and performs the silent clobber the guard exists to prevent.
    """
    repo = ExchangeRateOverridesRepo(db)
    first = repo.set("USD", "EUR", _DAY, rate=Decimal("0.93"), note=None, actor="cli")
    undo = repo.undo_event(first, actor="cli")
    assert undo is not None
    repo.set("USD", "EUR", _DAY, rate=Decimal("0.95"), note=None, actor="cli")

    with pytest.raises(UserError) as caught:
        UndoService(db).undo(undo.operation_id, actor="cli")

    assert caught.value.code == error_codes.UNDO_CASCADE_BLOCKED
    row = db.execute(
        "SELECT rate FROM app.exchange_rate_overrides "
        "WHERE from_currency = ? AND to_currency = ? AND rate_date = ?",
        ["USD", "EUR", _DAY],
    ).fetchone()
    assert row is not None and row[0] == Decimal("0.95000000")
