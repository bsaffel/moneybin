"""Tests for ``SecurityPriceRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, that ``before_value`` captures
the FULL prior row (Req 4), and that the composite ``(security_id, price_date,
quote_currency)`` primary key maps to a single composite ``target_id``.

``delete`` carries its own weight beyond CRUD symmetry: source precedence makes
an override beat every provider row for its date, and ``set`` can only replace
the value while keeping ``source = 'override'`` provenance. Without ``delete``
a mark is unreachable once written — the date can never return to
provider-derived valuation.
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any

from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.security_price_repo import SecurityPriceRepo


def _audit_rows_for(db: Database, target_id: str) -> list[tuple[Any, ...]]:
    return db.conn.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor, parent_audit_id
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [target_id],
    ).fetchall()


def _metric(action: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_app_mutation_audit_emitted_total",
            {"repository": "security_price_overrides", "action": action},
        )
        or 0.0
    )


_SEC = "abc123def456"
_DAY = date(2026, 7, 12)
_TARGET = "abc123def456|2026-07-12|USD"


def test_set_inserts_and_emits_paired_audit(db: Database) -> None:
    repo = SecurityPriceRepo(db)
    before_metric = _metric("security_price_override.set")

    event = repo.set(
        _SEC, _DAY, "USD", close=Decimal("214.55"), note="broker statement", actor="cli"
    )

    row = db.execute(
        "SELECT close, note FROM app.security_price_overrides "
        "WHERE security_id = ? AND price_date = ? AND quote_currency = ?",
        [_SEC, _DAY, "USD"],
    ).fetchone()
    assert row == (Decimal("214.55"), "broker statement")

    audits = _audit_rows_for(db, _TARGET)
    assert len(audits) == 1
    action, schema, table, target_id, before, after, actor, parent = audits[0]
    assert action == "security_price_override.set"
    assert (schema, table, target_id) == ("app", "security_price_overrides", _TARGET)
    assert before is None
    # The audit serializes DECIMAL(28,10) as its full-scale string, so compare
    # the numeric value rather than an incidental text form.
    assert Decimal(json.loads(after)["close"]) == Decimal("214.55")
    assert (actor, parent) == ("cli", None)
    assert event.audit_id
    assert _metric("security_price_override.set") == before_metric + 1


def test_set_twice_updates_and_captures_full_prior_row(db: Database) -> None:
    """The second mark's before_value must carry the whole prior row, not a diff."""
    repo = SecurityPriceRepo(db)
    repo.set(_SEC, _DAY, "USD", close=Decimal("214.55"), note="first", actor="cli")
    repo.set(_SEC, _DAY, "USD", close=Decimal("219.01"), note="corrected", actor="cli")

    row = db.execute(
        "SELECT close, note FROM app.security_price_overrides "
        "WHERE security_id = ? AND price_date = ?",
        [_SEC, _DAY],
    ).fetchone()
    assert row == (Decimal("219.01"), "corrected")

    audits = _audit_rows_for(db, _TARGET)
    assert len(audits) == 2
    before = json.loads(audits[1][4])
    assert Decimal(before["close"]) == Decimal("214.55")
    assert before["note"] == "first"
    assert before["security_id"] == _SEC


def test_set_preserves_created_at_on_update(db: Database) -> None:
    repo = SecurityPriceRepo(db)
    repo.set(_SEC, _DAY, "USD", close=Decimal("214.55"), note=None, actor="cli")
    created = db.execute(
        "SELECT created_at FROM app.security_price_overrides WHERE security_id = ?",
        [_SEC],
    ).fetchone()
    assert created is not None

    repo.set(_SEC, _DAY, "USD", close=Decimal("219.01"), note=None, actor="cli")
    after = db.execute(
        "SELECT created_at FROM app.security_price_overrides WHERE security_id = ?",
        [_SEC],
    ).fetchone()
    assert after is not None and after[0] == created[0]


def test_set_scopes_the_mark_to_one_quote_currency(db: Database) -> None:
    """A mark in USD must not disturb the same security-date's GBP mark."""
    repo = SecurityPriceRepo(db)
    repo.set(_SEC, _DAY, "USD", close=Decimal("214.55"), note=None, actor="cli")
    repo.set(_SEC, _DAY, "GBP", close=Decimal("169.20"), note=None, actor="cli")

    rows = db.execute(
        "SELECT quote_currency, close FROM app.security_price_overrides "
        "WHERE security_id = ? AND price_date = ? ORDER BY quote_currency",
        [_SEC, _DAY],
    ).fetchall()
    assert rows == [("GBP", Decimal("169.20")), ("USD", Decimal("214.55"))]
    assert len(_audit_rows_for(db, _TARGET)) == 1, "the USD target must see one write"


def test_delete_removes_the_mark_and_emits_audit(db: Database) -> None:
    repo = SecurityPriceRepo(db)
    repo.set(_SEC, _DAY, "USD", close=Decimal("214.55"), note="typo", actor="cli")

    event = repo.delete(_SEC, _DAY, "USD", actor="cli")

    row = db.execute(
        "SELECT COUNT(*) FROM app.security_price_overrides WHERE security_id = ?",
        [_SEC],
    ).fetchone()
    assert row is not None and row[0] == 0

    audits = _audit_rows_for(db, _TARGET)
    assert len(audits) == 2
    action, _, _, _, before, after, _, _ = audits[1]
    assert action == "security_price_override.delete"
    assert Decimal(json.loads(before)["close"]) == Decimal("214.55")
    assert after is None
    assert event is not None and event.audit_id


def test_delete_of_a_missing_mark_is_a_silent_no_op(db: Database) -> None:
    repo = SecurityPriceRepo(db)
    before_metric = _metric("security_price_override.delete")

    assert repo.delete(_SEC, _DAY, "USD", actor="cli") is None

    assert _audit_rows_for(db, _TARGET) == []
    assert _metric("security_price_override.delete") == before_metric


def test_parent_audit_id_threads_through(db: Database) -> None:
    """A cascading user action shares one audit chain."""
    repo = SecurityPriceRepo(db)
    first = repo.set(_SEC, _DAY, "USD", close=Decimal("1.00"), note=None, actor="mcp")
    repo.set(
        _SEC,
        date(2026, 7, 13),
        "USD",
        close=Decimal("2.00"),
        note=None,
        actor="mcp",
        parent_audit_id=first.audit_id,
    )
    row = db.execute(
        "SELECT parent_audit_id FROM app.audit_log WHERE target_id = ?",
        ["abc123def456|2026-07-13|USD"],
    ).fetchone()
    assert row is not None and row[0] == first.audit_id
