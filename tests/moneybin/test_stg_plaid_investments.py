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


def _raw_lot(
    db: Database, security_id: str, lot_index: int, **overrides: object
) -> None:
    row: dict[str, object] = {
        "account_id": "acc_1",
        "security_id": security_id,
        "lot_index": lot_index,
        "institution_lot_id": None,
        "original_purchase_datetime": None,
        "quantity": None,
        "purchase_price": None,
        "cost_basis": None,
        "current_value": None,
        "position_type": "long",
        "source_file": "sync_j1",
        "source_origin": "item_1",
    }
    row.update(overrides)
    _insert(db, "raw.plaid_investment_holding_lots", row)


def _raw_investment_txn(db: Database, **overrides: object) -> None:
    row: dict[str, object] = {
        "investment_transaction_id": "itx_1",
        "account_id": "acc_1",
        "security_id": "sec_1",
        "transaction_date": "2026-07-06",
        "transaction_datetime": None,
        "transaction_name": " AAPL BUY ",
        "quantity": "10.0",
        "amount": "2145.50",
        "price": "214.55",
        "fees": "0.00",
        "iso_currency_code": "USD",
        "unofficial_currency_code": None,
        "investment_transaction_type": "buy",
        "investment_transaction_subtype": "buy",
        "source_file": "sync_j1",
        "source_origin": "item_1",
        "extracted_at": "2026-07-08 12:00:00",
    }
    row.update(overrides)
    _insert(db, "raw.plaid_investment_transactions", row)


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
    """Coexisting accepted and reversed links: only accepted row resolves canonical_id.

    app.security_links is append-only. When a link is reversed, both the original
    accepted row and the new reversed row coexist. The view must join ONLY on
    status = 'accepted' to avoid fan-out (one raw security × two links = two rows).
    """
    _raw_security(db)
    # Insert both accepted and reversed links for the same (ref, source_type, ref_kind)
    _link_security(db, "sec_1", "cat000000001", status="accepted")
    _link_security(db, "sec_1", "cat000000999", status="reversed")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        "SELECT security_id, source_security_key FROM prep.stg_plaid__securities"
    ).fetchall()
    # Must emit exactly one row, bound to the accepted link's canonical_id
    assert rows == [("cat000000001", "sec_1")]


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


@pytest.mark.slow
def test_stg_holdings_reversed_security_link_does_not_resolve(db: Database) -> None:
    """Coexisting accepted and reversed security links: only accepted resolves canonical_id.

    Holdings join app.security_links on status = 'accepted' to resolve security_id.
    When both accepted and reversed rows coexist for the same ref, the join must
    emit only one row (not fan out).
    """
    _raw_holding(db)
    _link_account(db, "acc_1", "canonical_acc")
    # Insert both accepted and reversed links for the same (ref, source_type, ref_kind)
    _link_security(db, "sec_1", "cat000000001", status="accepted")
    _link_security(db, "sec_1", "cat000000999", status="reversed")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        "SELECT account_id, security_id, source_account_key, source_security_key "
        "FROM prep.stg_plaid__investment_holdings"
    ).fetchall()
    # Must emit exactly one row: accepted link resolves security_id, account resolves
    assert rows == [("canonical_acc", "cat000000001", "acc_1", "sec_1")]


# ── prep.stg_plaid__investment_transactions ─────────────────────────────────


@pytest.mark.slow
def test_stg_investment_txns_flips_amount_not_quantity(db: Database) -> None:
    """The sign flip lives HERE and nowhere else: Plaid + = cash out → ledger −.

    quantity is NEVER flipped at any layer — Plaid already signs it per the
    ledger convention (+ acquire, − dispose).
    """
    _raw_investment_txn(db)
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        """
        SELECT amount, quantity, type, provider_type, provider_subtype, ledger_include
        FROM prep.stg_plaid__investment_transactions
        """
    ).fetchone()
    assert row == (Decimal("-2145.50"), Decimal("10.0"), "buy", "buy", "buy", True)


@pytest.mark.slow
def test_stg_investment_txns_cash_deposit_lands_positive(db: Database) -> None:
    """The other half of the flip: Plaid sends cash IN as a negative amount.

    A deposit must land positive in the ledger. Together with the buy case above
    this pins the flip to exactly one application — a double flip would make every
    buy look like income and every deposit like a withdrawal.
    """
    _raw_investment_txn(
        db,
        security_id=None,
        quantity=None,
        price=None,
        amount="-500.00",
        investment_transaction_type="cash",
        investment_transaction_subtype="deposit",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        "SELECT type, amount, quantity, security_id "
        "FROM prep.stg_plaid__investment_transactions"
    ).fetchone()
    assert row == ("deposit", Decimal("500.00"), None, None)


@pytest.mark.slow
def test_stg_investment_txns_security_id_is_null_passthrough(db: Database) -> None:
    """An unbound security yields NULL security_id — never the provider id.

    Mirrors stg_plaid__securities / __investment_holdings: a provider id in the
    canonical column would sail past cost_basis.py's `if security_id is None:
    continue` guard and corrupt basis. The account keeps its source-native
    fallback (the accounts precedent); the security does not.
    """
    _raw_investment_txn(db)
    _link_account(db, "acc_1", "canonical_acc")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        """
        SELECT account_id, source_account_key, security_id, source_security_key
        FROM prep.stg_plaid__investment_transactions
        """
    ).fetchone()
    assert row == ("canonical_acc", "acc_1", None, "sec_1")


@pytest.mark.slow
def test_stg_investment_txns_resolves_both_ids(db: Database) -> None:
    """With both links accepted, canonical ids resolve and provider keys persist."""
    _raw_investment_txn(db)
    _link_account(db, "acc_1", "canonical_acc")
    _link_security(db, "sec_1", "cat000000001")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        """
        SELECT account_id, security_id, currency_code, description, event_group_id,
               original_acquisition_date, fees, price
        FROM prep.stg_plaid__investment_transactions
        """
    ).fetchone()
    assert row == (
        "canonical_acc",
        "cat000000001",
        "USD",
        "AAPL BUY",  # TRIM()ed
        None,  # GOLDEN-GATED: pairing disabled until Sandbox goldens land
        None,  # Plaid transactions carry no original-acquisition date
        Decimal("0.00"),
        Decimal("214.55"),
    )


@pytest.mark.slow
def test_trade_date_prefers_transaction_datetime(db: Database) -> None:
    _raw_investment_txn(db, transaction_datetime="2026-07-05 14:30:00")
    _raw_investment_txn(
        db, investment_transaction_id="itx_2", transaction_datetime=None
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = dict(
        db.execute(
            "SELECT investment_transaction_id, trade_date "
            "FROM prep.stg_plaid__investment_transactions"
        ).fetchall()
    )
    assert str(rows["itx_1"]) == "2026-07-05"  # datetime::DATE preferred
    assert str(rows["itx_2"]) == "2026-07-06"  # falls back to posting date


@pytest.mark.slow
def test_settlement_date_is_the_plaid_posting_date(db: Database) -> None:
    """Plaid's `date` is the POSTING date; it becomes settlement_date, not trade_date."""
    _raw_investment_txn(db, transaction_datetime="2026-07-05 14:30:00")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        "SELECT trade_date, settlement_date FROM prep.stg_plaid__investment_transactions"
    ).fetchone()
    assert row is not None
    assert str(row[0]) == "2026-07-05"
    assert str(row[1]) == "2026-07-06"


@pytest.mark.slow
def test_basis_unknown_transfer_maps_amount_to_null(db: Database) -> None:
    _raw_investment_txn(
        db,
        amount="0",
        quantity="6.0",
        investment_transaction_type="transfer",
        investment_transaction_subtype="stock distribution",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        "SELECT type, amount FROM prep.stg_plaid__investment_transactions"
    ).fetchone()
    assert row == ("transfer_in", None)  # NULL, never a false zero-basis lot


@pytest.mark.slow
def test_cash_only_transfer_direction_from_amount_sign(db: Database) -> None:
    """Cash-only transfer/{send, transfer} legs have no security to key on.

    Direction must come from the (already-flipped-elsewhere) amount sign, not
    quantity -- Plaid ships `quantity = 0` (never NULL) for a cash-only leg, so
    a bare `COALESCE(quantity, ...)` would read that 0 as "non-NULL, so inbound"
    and silently record every cash-out transfer as an inflow (the shipped bug).
    Both quantity representations -- Plaid's real "0" and a hypothetical NULL --
    must land identically, since the view keys the cash-only branch on
    `security_id IS NULL`, never on quantity.
    """
    cases = [
        ("itx_0", "send", "0", "100.00"),  # Plaid cash OUT -> withdrawal
        ("itx_1", "transfer", "0", "-100.00"),  # Plaid cash IN -> deposit
        ("itx_2", "send", None, "100.00"),  # same direction, quantity NULL
        ("itx_3", "transfer", None, "-100.00"),  # same direction, quantity NULL
    ]
    for txn_id, psub, qty, amount in cases:
        _raw_investment_txn(
            db,
            investment_transaction_id=txn_id,
            security_id=None,
            quantity=qty,
            amount=amount,
            investment_transaction_type="transfer",
            investment_transaction_subtype=psub,
        )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = {
        r[0]: (r[1], r[2], r[3], r[4])
        for r in db.execute(
            "SELECT investment_transaction_id, type, amount, quantity, ledger_include "
            "FROM prep.stg_plaid__investment_transactions"
        ).fetchall()
    }
    assert rows["itx_0"] == ("withdrawal", Decimal("-100.00"), Decimal("0"), True)
    assert rows["itx_1"] == ("deposit", Decimal("100.00"), Decimal("0"), True)
    assert rows["itx_2"] == ("withdrawal", Decimal("-100.00"), None, True)
    assert rows["itx_3"] == ("deposit", Decimal("100.00"), None, True)


@pytest.mark.slow
def test_security_bearing_transfer_zero_quantity_falls_back_to_amount_sign(
    db: Database,
) -> None:
    """A zero-quantity security transfer is meaningless as a share count.

    The NULLIF guard must treat Plaid's `quantity = 0` the same as NULL and
    fall back to the amount sign -- never silently default to `transfer_in`
    via a bare `COALESCE(quantity, ...)` reading the literal 0 as "non-NULL,
    so inbound" (the exact bug this view shipped with).
    """
    _raw_investment_txn(
        db,
        quantity="0",
        amount="100.00",  # Plaid cash OUT
        investment_transaction_type="transfer",
        investment_transaction_subtype="transfer",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        "SELECT type, amount FROM prep.stg_plaid__investment_transactions"
    ).fetchone()
    # Falls back to the correctly-flipped amount sign -- must NOT be transfer_in.
    assert row == ("transfer_out", Decimal("-100.00"))


@pytest.mark.slow
def test_short_leg_quantity_is_nulled(db: Database) -> None:
    """`other` rows never carry a ledger quantity -- MoneyBin models no shorts.

    buy/buy to cover ships a real Plaid quantity (e.g. +1.0), but the engine
    has no short-position model; a nonzero quantity on an `other` row would
    masquerade as a lot-affecting one.
    """
    _raw_investment_txn(
        db,
        quantity="1.0",
        investment_transaction_type="buy",
        investment_transaction_subtype="buy to cover",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        "SELECT type, quantity, provider_subtype "
        "FROM prep.stg_plaid__investment_transactions"
    ).fetchone()
    assert row == ("other", None, "buy to cover")


@pytest.mark.slow
def test_lifecycle_rows_excluded_entirely(db: Database) -> None:
    _raw_investment_txn(
        db, investment_transaction_type="cancel", investment_transaction_subtype="buy"
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_2",
        investment_transaction_type="cash",
        investment_transaction_subtype="pending credit",
        security_id=None,
        quantity=None,
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_3",
        investment_transaction_type="cash",
        investment_transaction_subtype="pending debit",
        security_id=None,
        quantity=None,
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_4",
        investment_transaction_type="transfer",
        investment_transaction_subtype="request",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        "SELECT COUNT(*) FROM prep.stg_plaid__investment_transactions"
    ).fetchone()
    assert row is not None and row[0] == 0


@pytest.mark.slow
def test_every_split_routes_to_review(db: Database) -> None:
    """GOLDEN-GATED: EVERY split routes to review — no shape is exempt.

    Plaid reports a share DELTA; the engine reads a MULTIPLIER. Whether the
    multiplier is derivable at all is exactly what the Sandbox goldens must
    settle, so v1 computes nothing. A wrong multiplier silently destroys the
    basis of every open lot; a surfaced gap does not.
    """
    shapes = [
        ("100.0", "0"),  # forward split, in-kind
        ("-50.0", "0"),  # reverse split
        ("0.0", "0"),  # zero delta
        (None, "0"),  # no quantity at all
        ("100.0", "12.34"),  # cash-in-lieu accompanying the split
    ]
    for i, (qty, amt) in enumerate(shapes):
        _raw_investment_txn(
            db,
            investment_transaction_id=f"itx_{i}",
            investment_transaction_type="transfer",
            investment_transaction_subtype="split",
            quantity=qty,
            amount=amt,
        )
    _link_security(db, "sec_1", "cat000000001")  # even a fully-bound security
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        "SELECT type, ledger_include, review_reason "
        "FROM prep.stg_plaid__investment_transactions"
    ).fetchall()
    assert len(rows) == len(shapes)
    assert all(r == ("split", False, "split_underivable") for r in rows), rows


@pytest.mark.slow
def test_unmapped_security_bearing_subtype_routes_to_review(db: Database) -> None:
    _raw_investment_txn(
        db,
        investment_transaction_type="buy",
        investment_transaction_subtype="quantum entanglement",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        "SELECT ledger_include, review_reason, type, subtype, provider_subtype "
        "FROM prep.stg_plaid__investment_transactions"
    ).fetchone()
    # type/subtype stay inside the closed vocabulary — the raw Plaid string is
    # preserved only in provider_subtype, never leaked into `type`/`subtype`.
    assert row == (False, "unmapped_subtype", "other", None, "quantum entanglement")


@pytest.mark.slow
def test_unmapped_cash_only_subtype_defaults_to_other(db: Database) -> None:
    _raw_investment_txn(
        db,
        security_id=None,
        quantity=None,
        investment_transaction_type="cash",
        investment_transaction_subtype="mystery credit",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        "SELECT ledger_include, review_reason, type, subtype "
        "FROM prep.stg_plaid__investment_transactions"
    ).fetchone()
    assert row == (True, None, "other", None)


@pytest.mark.slow
def test_taxonomy_mapping_full_table(db: Database) -> None:
    """Parametrized over the spec's taxonomy table — every branch, one plan."""
    # (plaid_type, plaid_subtype, qty) -> (type, subtype, ledger_include)
    expectations = {
        ("buy", "buy", "1"): ("buy", None, True),
        ("buy", "contribution", "1"): ("buy", None, True),
        ("buy", "dividend reinvestment", "1"): ("reinvest", "dividend", True),
        ("buy", "interest reinvestment", "1"): ("reinvest", "interest", True),
        ("buy", "long-term capital gain reinvestment", "1"): (
            "reinvest",
            "capital_gain",
            True,
        ),
        ("buy", "short-term capital gain reinvestment", "1"): (
            "reinvest",
            "capital_gain",
            True,
        ),
        ("buy", "assignment", "1"): ("other", None, True),
        ("buy", "buy to cover", "1"): ("other", None, True),
        ("sell", "sell", "-1"): ("sell", None, True),
        ("sell", "sell short", "-1"): ("other", None, True),
        ("sell", "exercise", "-1"): ("other", None, True),
        ("sell", "distribution", "-1"): ("transfer_out", None, True),
        ("transfer", "assignment", "1"): ("other", None, True),
        ("transfer", "exercise", "1"): ("other", None, True),
        ("transfer", "expire", "-1"): ("other", None, True),
        ("transfer", "stock distribution", "6"): ("transfer_in", None, True),
        ("transfer", "transfer", "5"): ("transfer_in", None, True),
        ("transfer", "transfer", "-5"): ("transfer_out", None, True),
        ("transfer", "send", "-5"): ("transfer_out", None, True),
        ("transfer", "merger", "5"): ("transfer_in", None, True),
        ("transfer", "spin off", "5"): ("transfer_in", None, True),
        ("transfer", "trade", "-5"): ("transfer_out", None, True),
        ("transfer", "adjustment", "1"): ("other", None, True),
        ("cash", "contribution", None): ("deposit", None, True),
        ("cash", "deposit", None): ("deposit", None, True),
        ("cash", "withdrawal", None): ("withdrawal", None, True),
        ("cash", "dividend", None): ("dividend", None, True),
        ("cash", "qualified dividend", None): ("dividend", "qualified", True),
        ("cash", "non-qualified dividend", None): ("dividend", "non_qualified", True),
        ("cash", "interest", None): ("interest", None, True),
        ("cash", "interest receivable", None): ("interest", None, True),
        ("cash", "long-term capital gain", None): (
            "capital_gain_distribution",
            "long_term",
            True,
        ),
        ("cash", "short-term capital gain", None): (
            "capital_gain_distribution",
            "short_term",
            True,
        ),
        ("cash", "unqualified gain", None): ("capital_gain_distribution", None, True),
        ("fee", "account fee", None): ("fee", None, True),
        ("fee", "legal fee", None): ("fee", None, True),
        ("fee", "management fee", None): ("fee", None, True),
        ("fee", "transfer fee", None): ("fee", None, True),
        ("fee", "trust fee", None): ("fee", None, True),
        ("fee", "fund fee", None): ("fee", None, True),
        ("fee", "miscellaneous fee", None): ("fee", None, True),
        ("fee", "margin expense", None): ("fee", None, True),
        ("fee", "tax", None): ("fee", "tax_withheld", True),
        ("fee", "tax withheld", None): ("fee", "tax_withheld", True),
        ("fee", "non-resident tax", None): ("fee", "tax_withheld", True),
        ("fee", "return of principal", None): ("return_of_capital", None, True),
        ("fee", "adjustment", None): ("other", None, True),
        ("cash", "loan payment", None): ("other", None, True),
        ("cash", "rebalance", None): ("other", None, True),
    }
    for i, (ptype, psub, qty) in enumerate(expectations):
        cash_only = qty is None
        _raw_investment_txn(
            db,
            investment_transaction_id=f"itx_{i}",
            security_id=None if cash_only else "sec_1",
            quantity=qty,
            amount="10.00",
            investment_transaction_type=ptype,
            investment_transaction_subtype=psub,
        )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = {
        r[0]: (r[1], r[2], r[3])
        for r in db.execute(
            "SELECT investment_transaction_id, type, subtype, ledger_include "
            "FROM prep.stg_plaid__investment_transactions"
        ).fetchall()
    }
    for i, (key, expected) in enumerate(expectations.items()):
        assert rows[f"itx_{i}"] == expected, f"taxonomy mismatch for {key}"


@pytest.mark.slow
def test_taxonomy_emits_only_closed_vocabulary(db: Database) -> None:
    """No Plaid string may ever leak into the closed `type` / `subtype` columns.

    Adversarial inputs: casing, whitespace-free garbage, and NULL type/subtype.
    """
    garbage = [
        ("BUY", "BUY"),  # upper-case provider strings still map
        ("wormhole", "quantum entanglement"),  # both unknown, security-bearing
        (None, None),  # both NULL
        ("cash", None),  # NULL subtype
    ]
    for i, (ptype, psub) in enumerate(garbage):
        _raw_investment_txn(
            db,
            investment_transaction_id=f"itx_{i}",
            investment_transaction_type=ptype,
            investment_transaction_subtype=psub,
        )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        "SELECT investment_transaction_id, type, subtype "
        "FROM prep.stg_plaid__investment_transactions ORDER BY 1"
    ).fetchall()
    valid_types = {
        "buy",
        "sell",
        "reinvest",
        "dividend",
        "interest",
        "capital_gain_distribution",
        "transfer_in",
        "transfer_out",
        "deposit",
        "withdrawal",
        "split",
        "fee",
        "return_of_capital",
        "other",
    }
    valid_subtypes = {
        None,
        "qualified",
        "non_qualified",
        "short_term",
        "long_term",
        "tax_withheld",
        "dividend",
        "interest",
        "capital_gain",
    }
    assert len(rows) == len(garbage)
    for txn_id, ttype, subtype in rows:
        assert ttype in valid_types, f"{txn_id}: leaked type {ttype!r}"
        assert subtype in valid_subtypes, f"{txn_id}: leaked subtype {subtype!r}"
    # itx_0 proves case-insensitivity; the rest fall to the closed fallback.
    assert rows[0][1] == "buy"


# ── prep.stg_plaid__opening_lots / __opening_lot_review ─────────────────────
#
# W (transactions_window_start) = 2024-07-08 throughout; the first (and only)
# snapshot is source_file 'sync_j1', extracted_at 2026-07-08 12:00:00. Every
# case gets its own security_id so ONE sqlmesh plan covers all of them.


@pytest.fixture
def bootstrap_cases(db: Database) -> Database:
    """The spec's reconciliation cases A–H plus the no-gap and guard cases."""
    # A: pre-window buy, untouched. 100 shares @ 2021-03-11, basis 1000.
    _raw_holding(db, security_id="sec_a", quantity="100", cost_basis="1000.00")
    _raw_lot(
        db,
        "sec_a",
        0,
        institution_lot_id="lot_a",
        original_purchase_datetime="2021-03-11 00:00:00",
        quantity="100",
        cost_basis="1000.00",
    )
    # B: pre-window 100 @ basis 800 + in-window buy 50 (150 held).
    _raw_holding(db, security_id="sec_b", quantity="150", cost_basis="1400.00")
    _raw_lot(
        db,
        "sec_b",
        0,
        original_purchase_datetime="2020-05-01 00:00:00",
        quantity="100",
        cost_basis="800.00",
    )
    _raw_lot(
        db,
        "sec_b",
        1,
        original_purchase_datetime="2025-01-10 00:00:00",
        quantity="50",
        cost_basis="600.00",
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_b_buy",
        security_id="sec_b",
        transaction_date="2025-01-10",
        quantity="50",
        amount="600.00",
    )
    # C: pre-window buy 100, in-window sell 60, survivor lot 40 @ basis 400.
    _raw_holding(db, security_id="sec_c", quantity="40", cost_basis="400.00")
    _raw_lot(
        db,
        "sec_c",
        0,
        original_purchase_datetime="2021-03-11 00:00:00",
        quantity="40",
        cost_basis="400.00",
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_c_sell",
        security_id="sec_c",
        transaction_date="2025-02-01",
        quantity="-60",
        amount="-6000.00",
        investment_transaction_type="sell",
        investment_transaction_subtype="sell",
    )
    # D: sell-then-rebuy — the snapshot holds only the in-window replacement lot.
    _raw_holding(db, security_id="sec_d", quantity="50", cost_basis="2500.00")
    _raw_lot(
        db,
        "sec_d",
        0,
        original_purchase_datetime="2025-02-01 00:00:00",
        quantity="50",
        cost_basis="2500.00",
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_d_sell",
        security_id="sec_d",
        transaction_date="2025-01-20",
        quantity="-80",
        amount="-8000.00",
        investment_transaction_type="sell",
        investment_transaction_subtype="sell",
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_d_buy",
        security_id="sec_d",
        transaction_date="2025-02-01",
        quantity="50",
        amount="2500.00",
    )
    # E: in-window split → review.
    _raw_holding(db, security_id="sec_e", quantity="200", cost_basis="1000.00")
    _raw_lot(
        db,
        "sec_e",
        0,
        original_purchase_datetime="2021-01-01 00:00:00",
        quantity="200",
        cost_basis="1000.00",
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_e_split",
        security_id="sec_e",
        transaction_date="2025-03-01",
        quantity="100",
        amount="0",
        investment_transaction_type="transfer",
        investment_transaction_subtype="split",
    )
    # F1: empty tax_lots, no in-window activity (G = H).
    _raw_holding(db, security_id="sec_f1", quantity="30", cost_basis="900.00")
    # F2: empty tax_lots, in-window buy 10 (G < H).
    _raw_holding(db, security_id="sec_f2", quantity="30", cost_basis="900.00")
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_f2_buy",
        security_id="sec_f2",
        transaction_date="2025-04-01",
        quantity="10",
        amount="300.00",
    )
    # G: lot with a real basis but a NULL acquisition date.
    _raw_holding(db, security_id="sec_g", quantity="25", cost_basis="500.00")
    _raw_lot(db, "sec_g", 0, quantity="25", cost_basis="500.00")
    # No-gap: the in-window buy fully explains the position.
    _raw_holding(db, security_id="sec_ng", quantity="10", cost_basis="300.00")
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_ng_buy",
        security_id="sec_ng",
        transaction_date="2025-05-01",
        quantity="10",
        amount="300.00",
    )
    # Negative gap: the ledger shows MORE than is held → review.
    _raw_holding(db, security_id="sec_neg", quantity="5", cost_basis="150.00")
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_neg_buy",
        security_id="sec_neg",
        transaction_date="2025-05-01",
        quantity="10",
        amount="300.00",
    )
    # Short: position_type='short' → review.
    _raw_holding(db, security_id="sec_sh", quantity="10", cost_basis="100.00")
    _raw_lot(
        db,
        "sec_sh",
        0,
        original_purchase_datetime="2021-01-01 00:00:00",
        quantity="10",
        cost_basis="100.00",
        position_type="short",
    )
    # NULL held quantity: unknowable position → review, never a silent drop.
    _raw_holding(db, security_id="sec_nq", quantity=None, cost_basis="100.00")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    return db


def _opening(db: Database, security_id: str) -> list[tuple[object, ...]]:
    return db.execute(
        """
        SELECT quantity, amount, original_acquisition_date::VARCHAR,
               trade_date::VARCHAR, investment_transaction_id
        FROM prep.stg_plaid__opening_lots
        WHERE source_security_key = ?
        ORDER BY original_acquisition_date
        """,
        [security_id],
    ).fetchall()


@pytest.mark.slow
def test_case_a_untouched_prewindow_lot(bootstrap_cases: Database) -> None:
    rows = _opening(bootstrap_cases, "sec_a")
    assert len(rows) == 1
    qty, amount, acq, trade, txn_id = rows[0]
    assert (qty, amount, acq, trade) == (
        Decimal("100"),
        Decimal("-1000.00"),
        "2021-03-11",
        "2024-07-07",
    )
    assert isinstance(txn_id, str)
    assert txn_id.startswith("plaid_opening_")
    assert len(txn_id) == len("plaid_opening_") + 16


@pytest.mark.slow
def test_case_b_inwindow_lot_excluded(bootstrap_cases: Database) -> None:
    """The 2025 lot belongs to the window — drawing it would double-count the buy."""
    rows = _opening(bootstrap_cases, "sec_b")
    assert len(rows) == 1
    assert rows[0][:2] == (Decimal("100"), Decimal("-800.00"))


@pytest.mark.slow
def test_case_c_survivors_plus_oldest_residual(bootstrap_cases: Database) -> None:
    rows = _opening(bootstrap_cases, "sec_c")  # gap = 40 - (-60) = 100
    assert len(rows) == 2
    residual, drawn = rows[0], rows[1]  # the residual is dated OLDEST
    assert residual[:3] == (Decimal("60"), None, "2021-03-10")
    assert drawn[:3] == (Decimal("40"), Decimal("-400.00"), "2021-03-11")


@pytest.mark.slow
def test_case_d_residual_only_never_wrong_lot_basis(bootstrap_cases: Database) -> None:
    """The in-window replacement lot is excluded; the sold sliver is flagged, not guessed."""
    rows = _opening(bootstrap_cases, "sec_d")  # gap = 50 - (-30) = 80
    assert len(rows) == 1
    assert rows[0][:3] == (Decimal("80"), None, "2024-07-07")


@pytest.mark.slow
def test_case_e_split_and_guards_route_to_review(bootstrap_cases: Database) -> None:
    reasons = dict(
        bootstrap_cases.execute(
            "SELECT source_security_key, reason FROM prep.stg_plaid__opening_lot_review"
        ).fetchall()
    )
    assert reasons["sec_e"] == "in_window_split"
    assert reasons["sec_neg"] == "negative_gap"
    assert reasons["sec_sh"] == "short_or_nonpositive"
    assert reasons["sec_nq"] == "short_or_nonpositive"
    for sec in ("sec_e", "sec_neg", "sec_sh", "sec_nq"):
        assert _opening(bootstrap_cases, sec) == [], sec
    # Bootstrappable and no-gap positions never appear in review.
    assert not {"sec_a", "sec_b", "sec_c", "sec_d", "sec_ng"} & set(reasons)


@pytest.mark.slow
def test_case_f_empty_lots_position_fallback(bootstrap_cases: Database) -> None:
    f1 = _opening(bootstrap_cases, "sec_f1")  # G = H → whole-position basis
    assert len(f1) == 1
    assert f1[0][:2] == (Decimal("30"), Decimal("-900.00"))
    f2 = _opening(bootstrap_cases, "sec_f2")  # G < H → basis not attributable
    assert len(f2) == 1
    assert f2[0][:2] == (Decimal("20"), None)


@pytest.mark.slow
def test_case_g_null_date_keeps_basis_dated_at_window(
    bootstrap_cases: Database,
) -> None:
    rows = _opening(bootstrap_cases, "sec_g")
    assert len(rows) == 1
    assert rows[0][:3] == (Decimal("25"), Decimal("-500.00"), "2024-07-08")


@pytest.mark.slow
def test_no_gap_no_row(bootstrap_cases: Database) -> None:
    assert _opening(bootstrap_cases, "sec_ng") == []


@pytest.mark.slow
def test_bootstrap_row_carries_the_ledger_shape(bootstrap_cases: Database) -> None:
    """A bootstrap row is a plain transfer_in the unchanged engine can consume."""
    row = bootstrap_cases.execute(
        """
        SELECT type, subtype, settlement_date, event_group_id, price, fees,
               provider_type, provider_subtype, currency_code, source_type,
               source_origin, description, created_at::VARCHAR
        FROM prep.stg_plaid__opening_lots
        WHERE source_security_key = 'sec_a'
        """
    ).fetchone()
    assert row == (
        "transfer_in",
        "opening_bootstrap",
        None,
        None,
        None,
        None,
        None,
        None,
        "USD",
        "plaid",
        "item_1",
        "Opening lot bootstrap (pre-window position)",
        "2026-07-08 12:00:00",  # created_at = the FIRST snapshot's extracted_at
    )


@pytest.mark.slow
def test_bootstrap_ids_stable_across_rebuild_and_later_snapshots(
    bootstrap_cases: Database,
) -> None:
    """A later snapshot must never rewrite (or duplicate) a frozen bootstrap lot."""
    before = _opening(bootstrap_cases, "sec_a")
    _raw_holding(
        bootstrap_cases,
        security_id="sec_a",
        quantity="0",  # the position was sold off after first connect
        cost_basis="0",
        source_file="sync_j2",
        extracted_at="2026-08-01 12:00:00",
    )
    with sqlmesh_context(bootstrap_cases) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    after = _opening(bootstrap_cases, "sec_a")
    assert len(after) == 1  # re-sync seeds no second opening lot
    assert before == after  # id, quantity, basis and both dates all frozen


@pytest.mark.slow
def test_lot_dated_exactly_on_the_window_start_is_not_drawn(db: Database) -> None:
    """The pre-window test is STRICT (< W), and that boundary is load-bearing.

    Plaid's transaction window is inclusive of W, so a lot dated exactly on W belongs
    to the window — its acquiring transaction is Plaid's to supply. Drawing it would
    claim its basis a second time the moment that transaction shows up. The gap opens
    as basis_incomplete instead: conservative, and visible.
    """
    _raw_holding(db, security_id="sec_w", quantity="50", cost_basis="500.00")
    _raw_lot(
        db,
        "sec_w",
        0,
        original_purchase_datetime="2024-07-08 00:00:00",  # exactly W
        quantity="50",
        cost_basis="500.00",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = _opening(db, "sec_w")
    assert len(rows) == 1
    # A `<=` boundary would emit (50, -500.00, "2024-07-08") — the double-count.
    assert rows[0][:3] == (Decimal("50"), None, "2024-07-07")


@pytest.mark.slow
def test_boundary_lot_prorates_basis_and_full_draws_stay_exact(db: Database) -> None:
    """Spec case H: several pre-window lots, non-uniform basis, drawn oldest-first.

    The gap falls short of the eligible lots' total (an in-window ACATS transfer_in
    whose shares Plaid folds into a lot carrying its ORIGINAL, pre-window purchase
    date), so the boundary lot is only partially drawn and prorates its basis. The
    fully-drawn lot must keep its basis to the cent — DuckDB has no decimal division,
    so `basis * drawn / qty` detours through DOUBLE; only the boundary row may pay
    that, never a full draw.
    """
    _raw_holding(db, security_id="sec_h", quantity="130", cost_basis="3333.33")
    _raw_lot(
        db,
        "sec_h",
        0,
        original_purchase_datetime="2019-01-01 00:00:00",
        quantity="30",
        cost_basis="333.33",  # an odd basis: float noise would show up here
    )
    _raw_lot(
        db,
        "sec_h",
        1,
        original_purchase_datetime="2020-01-01 00:00:00",
        quantity="100",
        cost_basis="3000.00",
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_h_xfer",
        security_id="sec_h",
        transaction_date="2025-01-05",
        quantity="60",
        amount="0",
        investment_transaction_type="transfer",
        investment_transaction_subtype="transfer",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = _opening(db, "sec_h")  # gap = 130 - 60 = 70
    assert len(rows) == 2  # 30 drawn in full, 40 of 100 drawn from the boundary lot
    assert rows[0][:3] == (Decimal("30"), Decimal("-333.33"), "2019-01-01")
    assert rows[1][:3] == (Decimal("40"), Decimal("-1200.00"), "2020-01-01")


@pytest.mark.slow
def test_bootstrap_id_cannot_collide_across_securities_in_one_account(
    db: Database,
) -> None:
    """Two securities, one account, identical lot shape — ids MUST still differ.

    lot_id in the cost-basis engine hashes (account, security, acquisition_date,
    source_transaction_id). An account-scoped synthetic id would give these two
    positions the same investment_transaction_id, and once a security merge
    collapses them onto one canonical security_id the derived lot_ids collide
    into a PRIMARY KEY violation. The id must be (origin, account, security)-scoped.
    """
    for sec in ("sec_x", "sec_y"):
        _raw_holding(db, security_id=sec, quantity="100", cost_basis="1000.00")
        _raw_lot(
            db,
            sec,
            0,  # same lot_index → same positional lot_key fallback
            original_purchase_datetime="2021-03-11 00:00:00",
            quantity="100",
            cost_basis="1000.00",  # same basis, same date, same account
        )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    ids = db.execute(
        "SELECT source_security_key, investment_transaction_id "
        "FROM prep.stg_plaid__opening_lots ORDER BY 1"
    ).fetchall()
    assert len(ids) == 2
    assert ids[0][1] != ids[1][1]


@pytest.mark.slow
def test_same_security_at_two_institutions_does_not_fan_out(db: Database) -> None:
    """One canonical security bound from two items must not double the rows."""
    _raw_holding(
        db,
        account_id="acc_1",
        security_id="sec_p",
        source_origin="item_1",
        quantity="100",
        cost_basis="1000.00",
    )
    _raw_lot(
        db,
        "sec_p",
        0,
        account_id="acc_1",
        source_origin="item_1",
        original_purchase_datetime="2021-03-11 00:00:00",
        quantity="100",
        cost_basis="1000.00",
    )
    _raw_holding(
        db,
        account_id="acc_2",
        security_id="sec_q",
        source_origin="item_2",
        quantity="40",
        cost_basis="500.00",
    )
    _raw_lot(
        db,
        "sec_q",
        0,
        account_id="acc_2",
        source_origin="item_2",
        original_purchase_datetime="2022-06-01 00:00:00",
        quantity="40",
        cost_basis="500.00",
    )
    # Both provider refs resolve to the SAME canonical security.
    _link_security(db, "sec_p", "cat000000001")
    _link_security(db, "sec_q", "cat000000001")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        "SELECT source_security_key, security_id, quantity, investment_transaction_id "
        "FROM prep.stg_plaid__opening_lots ORDER BY 1"
    ).fetchall()
    assert len(rows) == 2  # exactly one row per (account, security) — no fan-out
    assert rows[0][:3] == ("sec_p", "cat000000001", Decimal("100"))
    assert rows[1][:3] == ("sec_q", "cat000000001", Decimal("40"))
    assert rows[0][3] != rows[1][3]


@pytest.mark.slow
def test_bootstrap_resolves_canonical_ids(db: Database) -> None:
    """Canonical account/security ids resolve; the security has NO provider fallback."""
    _raw_holding(db, security_id="sec_a", quantity="100", cost_basis="1000.00")
    _raw_lot(
        db,
        "sec_a",
        0,
        original_purchase_datetime="2021-03-11 00:00:00",
        quantity="100",
        cost_basis="1000.00",
    )
    _raw_holding(db, security_id="sec_u", quantity="10", cost_basis="100.00")
    _raw_lot(
        db,
        "sec_u",
        0,
        original_purchase_datetime="2021-03-11 00:00:00",
        quantity="10",
        cost_basis="100.00",
    )
    _link_account(db, "acc_1", "canonical_acc")
    _link_security(db, "sec_a", "cat000000001")  # sec_u is deliberately unbound
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        "SELECT source_security_key, account_id, source_account_key, security_id "
        "FROM prep.stg_plaid__opening_lots ORDER BY 1"
    ).fetchall()
    assert rows == [
        ("sec_a", "canonical_acc", "acc_1", "cat000000001"),
        ("sec_u", "canonical_acc", "acc_1", None),  # NULL, never 'sec_u'
    ]
