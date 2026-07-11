"""Staging views for Plaid investments: resolution, normalization, taxonomy."""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo

pytestmark = pytest.mark.integration


def _insert(db: Database, table: str, row: dict[str, object]) -> None:
    cols = ", ".join(row)
    marks = ", ".join("?" for _ in row)
    db.execute(
        f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({marks})",  # noqa: S608  # fixed tables, test input
        list(row.values()),
    )


def _raw_security(db: Database, **overrides: object) -> None:
    row: dict[str, object] = {
        "security_id": "sec_1",
        "ticker_symbol": "AAPL",
        "market_identifier_code": "XNAS",
        "security_name": "Apple Inc.",
        "security_type": "equity",
        "iso_currency_code": "USD",
        "unofficial_currency_code": None,
        "source_file": "sync_j1",
        "source_origin": "item_1",
    }
    row.update(overrides)
    _insert(db, "raw.plaid_securities", row)


def _raw_holding(db: Database, **overrides: object) -> None:
    row: dict[str, object] = {
        "account_id": "acc_1",
        "security_id": "sec_1",
        "holdings_date": "2026-07-08",
        "quantity": "10.0",
        "cost_basis": "1980.00",
        "iso_currency_code": "USD",
        "transactions_window_start": "2024-07-08",
        "source_file": "sync_j1",
        "source_origin": "item_1",
        "extracted_at": "2026-07-08 12:00:00",
    }
    row.update(overrides)
    _insert(db, "raw.plaid_investment_holdings", row)


def _link_security(
    db: Database, ref: str, canonical: str, *, status: str = "accepted"
) -> None:
    """Seed an app.security_links row via the repo (not raw SQL).

    Uses SecurityLinksRepo directly rather than driving the full
    SecurityResolver ladder — the resolver's own matching logic has dedicated
    coverage elsewhere (Task 9); these tests only need a binding to already
    exist so the staging view's JOIN can be exercised deterministically.
    """
    SecurityLinksRepo(db).insert(
        security_id=canonical,
        ref_kind="plaid_security_id",
        ref_value=ref,
        source_type="plaid",
        decided_by="auto",
        actor="system",
        status=status,
    )


def _link_account(
    db: Database, ref: str, canonical: str, origin: str = "item_1"
) -> None:
    """Seed an app.account_links row via the repo (not raw SQL). See _link_security."""
    AccountLinksRepo(db).insert(
        link_id=uuid.uuid4().hex[:12],
        account_id=canonical,
        ref_kind="source_native",
        ref_value=ref,
        source_type="plaid",
        source_origin=origin,
        decided_by="auto",
        actor="system",
    )


@pytest.mark.slow
def test_stg_securities_resolves_and_normalizes(db: Database) -> None:
    _raw_security(
        db,
        security_type="fixed income",
        iso_currency_code=None,
        unofficial_currency_code="BTC",
    )
    _link_security(db, "sec_1", "cat000000001")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        """
        SELECT security_id, source_security_key, exchange, security_type, currency_code
        FROM prep.stg_plaid__securities
        """
    ).fetchone()
    assert row == ("cat000000001", "sec_1", "XNAS", "bond", "BTC")


@pytest.mark.slow
def test_stg_securities_unresolved_yields_null_security_id(db: Database) -> None:
    """A security with no app.security_links binding resolves to NULL, never sec_1.

    Unlike accounts (which fall back to their source-native id when unresolved),
    securities have no such fallback: a provider id leaking into the canonical
    security_id column would be silently treated as a real catalog entry
    downstream. source_security_key still carries the provider id for audit.
    """
    _raw_security(db)
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        "SELECT security_id, source_security_key, security_type FROM prep.stg_plaid__securities"
    ).fetchone()
    assert row == (None, "sec_1", "equity")


@pytest.mark.slow
def test_stg_securities_reversed_link_does_not_resolve(db: Database) -> None:
    """A reversed (undone) link must not resolve, and must not leak the join into a duplicate row."""
    _raw_security(db)
    _link_security(db, "sec_1", "cat000000001", status="reversed")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        "SELECT security_id, source_security_key FROM prep.stg_plaid__securities"
    ).fetchall()
    assert rows == [(None, "sec_1")]


@pytest.mark.slow
def test_stg_holdings_resolves_both_ids(db: Database) -> None:
    _raw_holding(db)
    _link_security(db, "sec_1", "cat000000001")
    _link_account(db, "acc_1", "canonical_acc")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        """
        SELECT account_id, source_account_key, security_id, source_security_key,
               currency_code, transactions_window_start
        FROM prep.stg_plaid__investment_holdings
        """
    ).fetchone()
    assert row is not None
    assert row[0] == "canonical_acc"
    assert row[1] == "acc_1"
    assert row[2] == "cat000000001"
    assert row[3] == "sec_1"
    assert row[4] == "USD"
    assert str(row[5]) == "2024-07-08"


@pytest.mark.slow
def test_stg_holdings_unresolved_security_yields_null_but_account_resolves(
    db: Database,
) -> None:
    """Only the account is bound; security_id must stay NULL, not fall back to sec_1."""
    _raw_holding(db)
    _link_account(db, "acc_1", "canonical_acc")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        """
        SELECT account_id, source_account_key, security_id, source_security_key
        FROM prep.stg_plaid__investment_holdings
        """
    ).fetchone()
    assert row == ("canonical_acc", "acc_1", None, "sec_1")


@pytest.mark.slow
def test_stg_holdings_unresolved_account_falls_back_to_source_native(
    db: Database,
) -> None:
    """Accounts keep the accounts precedent: unresolved falls back to the native id."""
    _raw_holding(db)
    _link_security(db, "sec_1", "cat000000001")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        "SELECT account_id, source_account_key FROM prep.stg_plaid__investment_holdings"
    ).fetchone()
    assert row == ("acc_1", "acc_1")


@pytest.mark.slow
def test_stg_holdings_same_source_file_repull_does_not_duplicate(db: Database) -> None:
    """Re-pulling the same snapshot (same source_file) upserts in place, not duplicates.

    raw.plaid_investment_holdings' PK is (account_id, security_id, source_origin,
    source_file); the view must trust that PK rather than re-deduping on top of it.
    """
    _raw_holding(db, quantity="10.0")
    _raw_holding(db, quantity="12.0")  # same PK -> upsert, not a second row
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        "SELECT quantity FROM prep.stg_plaid__investment_holdings"
    ).fetchall()
    assert rows == [(Decimal("12.0"),)]


@pytest.mark.slow
def test_stg_holdings_distinct_snapshots_both_preserved(db: Database) -> None:
    """Two distinct snapshots (different source_file) must both survive, never collapsed to latest."""
    _raw_holding(db, source_file="sync_j1", quantity="10.0")
    _raw_holding(
        db, source_file="sync_j2", quantity="11.0", extracted_at="2026-07-09 12:00:00"
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        "SELECT quantity FROM prep.stg_plaid__investment_holdings ORDER BY quantity"
    ).fetchall()
    assert rows == [(Decimal("10.0"),), (Decimal("11.0"),)]
