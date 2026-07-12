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


def test_upsert_defaults_created_by_to_user(db: Database) -> None:
    repo = SecuritiesRepo(db)
    event = _upsert(repo)
    row = db.conn.execute(
        "SELECT created_by FROM app.securities WHERE security_id = ?",
        [event.target_id],
    ).fetchone()
    assert row == ("user",)


def test_upsert_records_created_by(db: Database) -> None:
    repo = SecuritiesRepo(db)
    event = _upsert(repo, created_by="plaid", actor="system")
    row = db.conn.execute(
        "SELECT created_by FROM app.securities WHERE security_id = ?",
        [event.target_id],
    ).fetchone()
    assert row == ("plaid",)


def test_upsert_conflict_does_not_flip_provenance(db: Database) -> None:
    repo = SecuritiesRepo(db)
    event = _upsert(repo, name="Apple", actor="user")
    security_id = event.target_id
    assert security_id is not None
    _upsert(
        repo,
        security_id=security_id,
        name="Apple Inc.",
        created_by="plaid",
        actor="system",
    )
    row = db.conn.execute(
        "SELECT created_by, name FROM app.securities WHERE security_id = ?",
        [security_id],
    ).fetchone()
    assert row == ("user", "Apple Inc.")


def test_refresh_updates_plaid_minted_row_and_audits(db: Database) -> None:
    repo = SecuritiesRepo(db)
    before_metric = _metric("securities.refresh")
    minted = _upsert(
        repo,
        name="Vangard Total",
        security_type="other",
        ticker=None,
        exchange=None,
        created_by="plaid",
        actor="system",
    ).target_id
    assert minted is not None

    event = repo.refresh_provider_attributes(
        minted,
        name="Vanguard Total Stock Market ETF",
        security_type="etf",
        ticker="VTI",
        actor="system",
    )
    assert event is not None

    row = db.conn.execute(
        "SELECT name, security_type, ticker FROM app.securities WHERE security_id = ?",
        [minted],
    ).fetchone()
    assert row == ("Vanguard Total Stock Market ETF", "etf", "VTI")

    audit = _audit_rows_for(db, minted)
    refresh_rows = [r for r in audit if r[0] == "securities.refresh"]
    assert len(refresh_rows) == 1
    _action, schema, table, target_id, before, after, actor, _parent = refresh_rows[0]
    assert (schema, table, target_id) == ("app", "securities", minted)
    assert json.loads(before)["name"] == "Vangard Total"
    assert json.loads(after)["name"] == "Vanguard Total Stock Market ETF"
    assert actor == "system"

    assert _metric("securities.refresh") - before_metric == 1.0


def test_refresh_is_noop_when_values_already_match(db: Database) -> None:
    """A sync that resubmits identical provider attributes must not write or audit.

    Guards against the omitted-field trap in reverse: the UPDATE always bumps
    ``updated_at``, so without an explicit unchanged-values check every daily
    resolver sync would accrue a no-op ``securities.refresh`` audit row per
    security, forever.
    """
    repo = SecuritiesRepo(db)
    before_metric = _metric("securities.refresh")
    minted = _upsert(
        repo,
        name="Vanguard Total Stock Market ETF",
        security_type="etf",
        ticker="VTI",
        exchange=None,
        created_by="plaid",
        actor="system",
    ).target_id
    assert minted is not None

    before_row = db.conn.execute(
        "SELECT updated_at FROM app.securities WHERE security_id = ?",
        [minted],
    ).fetchone()
    assert before_row is not None
    before_audit_count = len(_audit_rows_for(db, minted))

    time.sleep(0.01)
    event = repo.refresh_provider_attributes(
        minted,
        name="Vanguard Total Stock Market ETF",
        security_type="etf",
        ticker="VTI",
        actor="system",
    )
    assert event is None

    after_row = db.conn.execute(
        "SELECT updated_at FROM app.securities WHERE security_id = ?",
        [minted],
    ).fetchone()
    assert after_row == before_row

    assert len(_audit_rows_for(db, minted)) == before_audit_count
    assert _metric("securities.refresh") - before_metric == 0.0


def test_refresh_writes_when_one_field_genuinely_differs(db: Database) -> None:
    """A single-field diff (not all three) must still write and audit."""
    repo = SecuritiesRepo(db)
    before_metric = _metric("securities.refresh")
    minted = _upsert(
        repo,
        name="Vanguard Total Stock Market ETF",
        security_type="etf",
        ticker="VTI",
        exchange=None,
        created_by="plaid",
        actor="system",
    ).target_id
    assert minted is not None

    event = repo.refresh_provider_attributes(
        minted,
        name="Vanguard Total Stock Market ETF",
        security_type="etf",
        ticker="VTI2",
        actor="system",
    )
    assert event is not None

    row = db.conn.execute(
        "SELECT ticker FROM app.securities WHERE security_id = ?",
        [minted],
    ).fetchone()
    assert row == ("VTI2",)
    assert _metric("securities.refresh") - before_metric == 1.0


def test_refresh_leaves_other_columns_untouched(db: Database) -> None:
    """Guards the omitted-column trap: refresh must not NULL provider-untouched fields."""
    repo = SecuritiesRepo(db)
    minted = _upsert(
        repo,
        name="Vangard Total",
        security_type="other",
        ticker=None,
        exchange="NASDAQ",
        cusip="922908769",
        isin="US9229087690",
        figi="BBG000BDTBL9",
        coingecko_id=None,
        is_cash_equivalent=False,
        cost_basis_method="fifo",
        created_by="plaid",
        actor="system",
    ).target_id
    assert minted is not None

    created_at = db.conn.execute(
        "SELECT created_at FROM app.securities WHERE security_id = ?",
        [minted],
    ).fetchone()
    assert created_at is not None

    repo.refresh_provider_attributes(
        minted,
        name="Vanguard Total Stock Market ETF",
        security_type="etf",
        ticker="VTI",
        actor="system",
    )

    row = db.conn.execute(
        "SELECT exchange, cusip, isin, figi, coingecko_id, is_cash_equivalent, "
        "cost_basis_method, currency_code, created_by, created_at "
        "FROM app.securities WHERE security_id = ?",
        [minted],
    ).fetchone()
    assert row == (
        "NASDAQ",
        "922908769",
        "US9229087690",
        "BBG000BDTBL9",
        None,
        False,
        "fifo",
        "USD",
        "plaid",
        created_at[0],
    )


def test_refresh_touches_only_plaid_minted(db: Database) -> None:
    repo = SecuritiesRepo(db)
    minted = _upsert(
        repo,
        name="Vangard Total",
        security_type="other",
        ticker=None,
        exchange=None,
        created_by="plaid",
        actor="system",
    ).target_id
    authored = _upsert(
        repo, name="My Fund", security_type="etf", ticker=None, exchange=None
    ).target_id
    assert minted is not None
    assert authored is not None

    assert (
        repo.refresh_provider_attributes(
            minted,
            name="Vanguard Total Stock Market ETF",
            security_type="etf",
            ticker="VTI",
            actor="system",
        )
        is not None
    )
    assert (
        repo.refresh_provider_attributes(
            authored,
            name="HIJACKED",
            security_type="other",
            ticker=None,
            actor="system",
        )
        is None
    )

    rows = db.conn.execute(
        "SELECT security_id, name FROM app.securities ORDER BY name"
    ).fetchall()
    assert (authored, "My Fund") in rows
    assert (minted, "Vanguard Total Stock Market ETF") in rows


def test_refresh_preserves_a_user_overridden_field(db: Database) -> None:
    """A field the user edited is theirs — the provider must not revert it.

    The securities-set surface upserts an EXISTING id, which by design does not
    flip ``created_by``, so the row stays ``created_by='plaid'`` and the next
    sync's refresh still matches it. Overwriting here would revert the user's
    rename on every sync with no warning and no way to take ownership.
    """
    repo = SecuritiesRepo(db)
    minted = _upsert(
        repo,
        name="VANGUARD TOTAL STK MKT IDX",
        security_type="etf",
        ticker="VTI",
        exchange=None,
        created_by="plaid",
        actor="system",
    ).target_id
    assert minted is not None

    _upsert(
        repo,
        security_id=minted,
        name="Vanguard Total Stock Market ETF",
        security_type="etf",
        ticker="VTI",
        exchange=None,
        actor="cli",
    )

    assert (
        repo.refresh_provider_attributes(
            minted,
            name="VANGUARD TOTAL STK MKT IDX",
            security_type="etf",
            ticker="VTI",
            actor="system",
        )
        is None
    )

    row = db.conn.execute(
        "SELECT name FROM app.securities WHERE security_id = ?", [minted]
    ).fetchone()
    assert row == ("Vanguard Total Stock Market ETF",)


def test_refresh_still_updates_fields_the_user_did_not_override(
    db: Database,
) -> None:
    """Ownership is per-field: a name edit must not freeze the ticker.

    A ticker frozen at a stale value is the durable form of the stale-mirror
    hazard — a later provider security carrying the recycled ticker would find a
    unique exact-ticker hit on this row and silently merge into it.
    """
    repo = SecuritiesRepo(db)
    minted = _upsert(
        repo,
        name="Facebook Inc",
        security_type="equity",
        ticker="FB",
        exchange=None,
        created_by="plaid",
        actor="system",
    ).target_id
    assert minted is not None

    _upsert(
        repo,
        security_id=minted,
        name="Meta (my label)",
        security_type="equity",
        ticker="FB",
        exchange=None,
        actor="cli",
    )

    event = repo.refresh_provider_attributes(
        minted,
        name="Meta Platforms Inc",
        security_type="equity",
        ticker="META",
        actor="system",
    )
    assert event is not None

    row = db.conn.execute(
        "SELECT name, ticker FROM app.securities WHERE security_id = ?", [minted]
    ).fetchone()
    assert row == ("Meta (my label)", "META")


def test_refresh_returns_none_for_missing_row(db: Database) -> None:
    repo = SecuritiesRepo(db)
    assert (
        repo.refresh_provider_attributes(
            "does-not-exist",
            name="Whatever",
            security_type="other",
            ticker=None,
            actor="system",
        )
        is None
    )


def test_delete_removes_plaid_minted_row_and_audits(db: Database) -> None:
    repo = SecuritiesRepo(db)
    before_metric = _metric("securities.delete")
    minted = _upsert(
        repo, name="Dup", security_type="equity", created_by="plaid", actor="system"
    ).target_id
    assert minted is not None

    event = repo.delete(minted, actor="user")

    rows = db.conn.execute(
        "SELECT 1 FROM app.securities WHERE security_id = ?", [minted]
    ).fetchall()
    assert rows == []

    audit = _audit_rows_for(db, minted)
    delete_rows = [r for r in audit if r[0] == "securities.delete"]
    assert len(delete_rows) == 1
    _action, schema, table, target_id, before, after, actor, _parent = delete_rows[0]
    assert (schema, table, target_id) == ("app", "securities", minted)
    assert json.loads(before)["name"] == "Dup"
    assert after is None
    assert actor == "user"
    assert event.target_id == minted

    assert _metric("securities.delete") - before_metric == 1.0


def test_delete_refuses_user_authored(db: Database) -> None:
    repo = SecuritiesRepo(db)
    minted = _upsert(
        repo, name="Dup", security_type="equity", created_by="plaid", actor="system"
    ).target_id
    authored = _upsert(
        repo, name="Mine", security_type="equity", actor="user"
    ).target_id
    assert minted is not None
    assert authored is not None

    repo.delete(minted, actor="user")
    with pytest.raises(ValueError, match="user-authored"):
        repo.delete(authored, actor="user")

    row = db.conn.execute(
        "SELECT 1 FROM app.securities WHERE security_id = ?", [authored]
    ).fetchall()
    assert row != []


def test_delete_raises_for_missing_row(db: Database) -> None:
    repo = SecuritiesRepo(db)
    with pytest.raises(ValueError, match="security_id"):
        repo.delete("does-not-exist", actor="user")
