"""Tests for ``SecuritiesRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, and that ``before_value``
captures the FULL prior row (Req 4). ``upsert`` mints a 12-hex
``security_id`` on insert (Strategy 3, identifiers.md) and does a full-row
``ON CONFLICT`` update when a caller-supplied id already exists. It returns
the :class:`AuditEvent`; the resulting id is its ``target_id`` (coherent with
sibling mint-on-insert repos like ``UserMerchantsRepo.insert``).
"""

from __future__ import annotations

import json
import time
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.services.audit_service import AuditEvent


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
            {"repository": "securities", "action": action},
        )
        or 0.0
    )


def _upsert(repo: SecuritiesRepo, **overrides: Any) -> AuditEvent:
    kwargs: dict[str, Any] = {
        "security_id": None,
        "name": "Apple Inc.",
        "security_type": "equity",
        "ticker": "AAPL",
        "exchange": "NASDAQ",
        "actor": "cli",
    }
    kwargs.update(overrides)
    return repo.upsert(**kwargs)


def test_upsert_inserts_row_and_mints_12_hex_id(db: Database) -> None:
    repo = SecuritiesRepo(db)
    before_metric = _metric("securities.upsert")

    event = _upsert(
        repo,
        cusip="037833100",
        isin="US0378331005",
        figi="BBG000B9XRY4",
        coingecko_id=None,
        is_cash_equivalent=False,
        cost_basis_method="fifo",
    )
    security_id = event.target_id
    assert security_id is not None
    assert len(security_id) == 12
    assert all(c in "0123456789abcdef" for c in security_id)

    # All columns round-trip on the INSERT path (guards a params/columns swap).
    row = db.conn.execute(
        "SELECT name, security_type, ticker, exchange, cusip, isin, figi, "
        "coingecko_id, is_cash_equivalent, cost_basis_method, currency_code "
        "FROM app.securities WHERE security_id = ?",
        [security_id],
    ).fetchone()
    assert row == (
        "Apple Inc.",
        "equity",
        "AAPL",
        "NASDAQ",
        "037833100",
        "US0378331005",
        "BBG000B9XRY4",
        None,
        False,
        "fifo",
        "USD",
    )

    audit = _audit_rows_for(db, security_id)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "securities.upsert"
    assert (schema, table, target_id) == ("app", "securities", security_id)
    assert before is None
    assert json.loads(after)["name"] == "Apple Inc."
    assert actor == "cli"

    assert _metric("securities.upsert") - before_metric == 1.0


def test_upsert_with_explicit_id_inserts_that_row(db: Database) -> None:
    repo = SecuritiesRepo(db)
    event = _upsert(repo, security_id="sec_aapl_001")
    assert event.target_id == "sec_aapl_001"

    row = db.conn.execute(
        "SELECT 1 FROM app.securities WHERE security_id = ?", ["sec_aapl_001"]
    ).fetchone()
    assert row is not None


def test_upsert_records_parent_audit_id_when_supplied(db: Database) -> None:
    repo = SecuritiesRepo(db)
    parent = _upsert(repo, security_id="sec_parent")
    event = _upsert(repo, security_id="sec_child", parent_audit_id=parent.audit_id)
    audit = _audit_rows_for(db, event.target_id or "")
    assert audit[0][7] == parent.audit_id  # parent_audit_id column


def test_upsert_update_by_id_changes_all_fields_and_bumps_updated_at(
    db: Database,
) -> None:
    repo = SecuritiesRepo(db)
    security_id = _upsert(
        repo,
        security_id="sec_aapl_001",
        name="Apple Inc.",
        cusip="037833100",
        isin="US0378331005",
        figi="BBG000B9XRY4",
        coingecko_id=None,
        is_cash_equivalent=False,
        cost_basis_method="fifo",
    ).target_id

    before_row = db.conn.execute(
        "SELECT updated_at FROM app.securities WHERE security_id = ?",
        [security_id],
    ).fetchone()
    assert before_row is not None

    # Ensure timestamp resolution actually advances between insert and update.
    time.sleep(0.01)
    returned = _upsert(
        repo,
        security_id=security_id,
        name="Apple Inc. (renamed)",
        ticker="AAPL2",
        exchange="NYSE",
        cusip="CUSIP2",
        isin="ISIN2",
        figi="FIGI2",
        coingecko_id="apple-token",
        is_cash_equivalent=True,
        cost_basis_method="hifo",
    )
    assert returned.target_id == security_id

    # Every column reflects the update (guards a params/columns swap on UPDATE).
    after_row = db.conn.execute(
        "SELECT name, ticker, exchange, cusip, isin, figi, coingecko_id, "
        "is_cash_equivalent, cost_basis_method, updated_at "
        "FROM app.securities WHERE security_id = ?",
        [security_id],
    ).fetchone()
    assert after_row is not None
    assert after_row[:9] == (
        "Apple Inc. (renamed)",
        "AAPL2",
        "NYSE",
        "CUSIP2",
        "ISIN2",
        "FIGI2",
        "apple-token",
        True,
        "hifo",
    )
    assert after_row[9] > before_row[0]


def test_upsert_update_captures_full_prior_row(db: Database) -> None:
    repo = SecuritiesRepo(db)
    security_id = _upsert(
        repo, security_id="sec_aapl_001", name="Old Name", ticker="OLD"
    ).target_id
    _upsert(repo, security_id=security_id, name="New Name", ticker="NEW")

    update_audit = _audit_rows_for(db, security_id or "")[1]
    before = json.loads(update_audit[4])
    after = json.loads(update_audit[5])
    assert before["name"] == "Old Name"
    assert before["ticker"] == "OLD"
    assert after["name"] == "New Name"
    assert after["ticker"] == "NEW"


def test_upsert_rejects_invalid_security_type(db: Database) -> None:
    repo = SecuritiesRepo(db)
    with pytest.raises(duckdb.ConstraintException):
        _upsert(repo, security_type="not_a_real_type")


def test_upsert_rejects_invalid_cost_basis_method(db: Database) -> None:
    repo = SecuritiesRepo(db)
    with pytest.raises(duckdb.ConstraintException):
        _upsert(repo, cost_basis_method="not_a_real_method")


def test_upsert_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = SecuritiesRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _upsert(repo, security_id="ghost", name="Ghost Corp")

    rows = db.conn.execute(
        "SELECT 1 FROM app.securities WHERE security_id = ?", ["ghost"]
    ).fetchall()
    assert rows == []
