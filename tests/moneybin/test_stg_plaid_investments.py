"""Staging views for Plaid investments: resolution, normalization, taxonomy.

Also covers the core boundary those views feed: the three-branch union into
``core.fct_investment_transactions`` and the non-authoritative
``provider_reported_*`` reconciliation columns on ``core.dim_holdings``.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.doctor_service import DoctorService
from moneybin.services.investment_service import (
    _PIPELINE_EMITTED_SUBTYPES,  # pyright: ignore[reportPrivateUsage]
    _SUBTYPE_VOCAB,  # pyright: ignore[reportPrivateUsage]
    TAXONOMY,
)

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
    # The loader never writes a holdings row without a receipt for its pull
    # (raw_plaid_investment_holdings_snapshots.sql invariant), and every
    # newest-snapshot join keys on the receipt — so the fixture must not
    # either, or the snapshot this row belongs to is invisible. The window is
    # threaded through rather than defaulted: the loader mirrors it from the
    # same item metadata, so a fixture that overrides it here must not leave the
    # receipt claiming a different one.
    _raw_holdings_receipt(
        db,
        source_origin=str(row["source_origin"]),
        source_file=str(row["source_file"]),
        extracted_at=str(row["extracted_at"]),
        transactions_window_start=str(row["transactions_window_start"]),
    )


def _raw_holdings_receipt(
    db: Database,
    *,
    source_origin: str = "item_1",
    source_file: str = "sync_j1",
    extracted_at: str = "2026-07-08 12:00:00",
    transactions_window_start: str = "2024-07-08",
    holdings_count: int | None = None,
) -> None:
    """Record that an item's holdings were fetched in one pull, as the loader does.

    ``holdings_count`` defaults to however many holdings rows the fixture has
    landed for this (item, pull) so far. Pass ``0`` explicitly to model the pull
    the receipt exists for: an item that reported and holds NOTHING, which
    writes no holdings rows at all.

    The receipt carries the item's window because it is the only row that exists
    when that pull returned no holdings — matching _raw_holding's default so a
    fixture can mix held and liquidated accounts under one item.
    """
    db.execute(
        """
        INSERT OR REPLACE INTO raw.plaid_investment_holdings_snapshots
            (source_origin, source_file, holdings_date, holdings_count,
             transactions_window_start, extracted_at)
        SELECT ?, ?, CAST(? AS TIMESTAMP)::DATE,
               COALESCE(?, (
                   SELECT COUNT(*) FROM raw.plaid_investment_holdings
                   WHERE source_origin = ? AND source_file = ?
               )),
               CAST(? AS DATE),
               CAST(? AS TIMESTAMP)
        """,
        [
            source_origin,
            source_file,
            extracted_at,
            holdings_count,
            source_origin,
            source_file,
            transactions_window_start,
            extracted_at,
        ],
    )


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


def _manual_investment_txn(db: Database, **overrides: object) -> None:
    """Seed one raw.manual_investment_transactions row (the ledger's other branch)."""
    row: dict[str, object] = {
        "source_transaction_id": "manual_1",
        "import_id": "test_import",
        "account_id": "acc_m",
        "security_id": "sec_m",
        "security_ref": None,
        "type": "buy",
        "subtype": None,
        "event_group_id": None,
        "trade_date": "2026-01-05",
        "settlement_date": None,
        "original_acquisition_date": None,
        "quantity": "1",
        "price": "100",
        "amount": "-100.00",
        "fees": None,
        "currency_code": "USD",
        "description": "Manual buy",
        "created_at": "2026-01-05 00:00:00",
        "created_by": "cli",
        "investment_transaction_id": "manual_1",
    }
    row.update(overrides)
    _insert(db, "raw.manual_investment_transactions", row)


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
    `security_id IS NULL`, never on quantity. Both also land with a NULL ledger
    quantity: deposit/withdrawal are cash-only types, and Plaid's literal 0 is
    not a share movement.
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
    assert rows["itx_0"] == ("withdrawal", Decimal("-100.00"), None, True)
    assert rows["itx_1"] == ("deposit", Decimal("100.00"), None, True)
    assert rows["itx_2"] == ("withdrawal", Decimal("-100.00"), None, True)
    assert rows["itx_3"] == ("deposit", Decimal("100.00"), None, True)


@pytest.mark.slow
def test_security_bearing_zero_quantity_transfer_routes_to_review(
    db: Database,
) -> None:
    """A security-bearing transfer with no share delta has NO derivable direction.

    The amount sign is not a proxy for the share direction: cash coming IN
    accompanies shares going OUT, so inferring `transfer_in` from a credit is
    backwards -- and either guess is a fabrication, because the row carries no
    share movement to begin with. The concrete shape is a merger / spin-off
    cash-in-lieu leg (security_id set, quantity 0, amount negative because cash
    was credited): guessed as `transfer_in` it is an _ACQUISITION_TYPE, so
    cost_basis.py opens a lot with original_quantity 0 carrying the cash-in-lieu
    as its basis -- a phantom zero-share lot, and the proceeds never realize.

    So the model refuses, exactly as it refuses on splits: no ledger event, a
    review_reason instead.
    """
    for txn_id, psub, amount in (
        ("itx_lieu", "merger", "-100.00"),  # Plaid credit: cash IN
        ("itx_xfer", "transfer", "100.00"),  # Plaid debit: cash OUT
    ):
        _raw_investment_txn(
            db,
            investment_transaction_id=txn_id,
            quantity="0",  # Plaid ships 0, never NULL
            amount=amount,
            investment_transaction_type="transfer",
            investment_transaction_subtype=psub,
        )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = {
        r[0]: (r[1], r[2], r[3])
        for r in db.execute(
            "SELECT investment_transaction_id, type, ledger_include, review_reason "
            "FROM prep.stg_plaid__investment_transactions"
        ).fetchall()
    }
    assert rows["itx_lieu"] == ("other", False, "transfer_direction_underivable")
    assert rows["itx_xfer"] == ("other", False, "transfer_direction_underivable")


@pytest.mark.slow
def test_cash_only_legs_carry_no_ledger_quantity(db: Database) -> None:
    """Plaid ships `quantity = 0` (not NULL) on a cash-only leg; the ledger wants NULL.

    core.fct_investment_transactions.quantity is contracted as "Signed units:
    + acquire, - dispose, NULL cash-only", and InvestmentService._validate rejects
    a non-NULL quantity on exactly these types (_QTY_NULL). A literal 0 reaching
    core makes every dividend and fee read as a share-moving event to any consumer
    that keys on `quantity IS NULL` (prep.int_plaid__opening_positions does), and
    renders "0.00000000 shares" on a dividend where the schema promised NULL.
    """
    cases = [
        ("itx_div", "cash", "dividend"),
        ("itx_int", "cash", "interest"),
        ("itx_cgd", "cash", "long-term capital gain"),
        ("itx_dep", "cash", "deposit"),
        ("itx_wdr", "cash", "withdrawal"),
        ("itx_fee", "fee", "management fee"),
        ("itx_roc", "fee", "return of principal"),
    ]
    for txn_id, ptype, psub in cases:
        _raw_investment_txn(
            db,
            investment_transaction_id=txn_id,
            security_id=None,
            quantity="0",  # Plaid's cash-only representation
            price=None,
            amount="-25.00",
            investment_transaction_type=ptype,
            investment_transaction_subtype=psub,
        )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        "SELECT investment_transaction_id, type, quantity "
        "FROM prep.stg_plaid__investment_transactions ORDER BY 1"
    ).fetchall()
    assert len(rows) == len(cases)
    for txn_id, type_, quantity in rows:
        assert quantity is None, f"{txn_id} ({type_}) carries quantity {quantity!r}"


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
def test_sold_out_prewindow_position_routes_to_review(db: Database) -> None:
    """A fully-sold pre-window position has no holdings row in any snapshot.

    Plaid never reports closed positions. Without an explicit check it reaches
    neither stg_plaid__opening_lots (nothing to draw from -- it never enters
    int_plaid__opening_positions) nor stg_plaid__opening_lot_review, and becomes
    a silent zero-basis oversold disposal instead of a visible gap.
    """
    # Establishes transactions_window_start=2024-07-08 for this account/item via an
    # unrelated held security; sec_gone itself has NO holdings row anywhere.
    _raw_holding(db, security_id="sec_other", quantity="5", cost_basis="50.00")
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_gone_sell",
        security_id="sec_gone",
        transaction_date="2025-01-15",  # in-window
        quantity="-10",
        amount="-1000.00",
        investment_transaction_type="sell",
        investment_transaction_subtype="sell",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    assert _opening(db, "sec_gone") == []  # nothing to bootstrap -- no basis data
    reasons = dict(
        db.execute(
            "SELECT source_security_key, reason FROM prep.stg_plaid__opening_lot_review"
        ).fetchall()
    )
    assert reasons["sec_gone"] == "sold_out_prewindow"
    assert "sec_other" not in reasons  # the held position bootstraps normally


def test_account_liquidated_before_first_snapshot_routes_to_review(
    db: Database,
) -> None:
    """An account whose FIRST snapshot is EMPTY still flags its disposals.

    The test above has to plant an unrelated HELD security to establish the
    account's window — which is the whole problem. A fully-liquidated broker
    reports zero positions, so raw.plaid_investment_holdings has no row for the
    account at all, and a window derived from holdings ROWS has nothing to key
    on: every disposal the account made silently leaves the review queue and
    lands in the ledger with zero basis. That is the largest version of exactly
    the gap this view exists to surface, and it is the one case holdings rows
    can never cover. The receipt is the only evidence the item reported, so the
    window is read from there.
    """
    _raw_holdings_receipt(db, holdings_count=0)  # item reported; holds NOTHING
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_liquidated_sell",
        security_id="sec_gone",
        transaction_date="2026-07-06",  # in-window (window opens 2024-07-08)
        quantity="-10",
        amount="-1000.00",
        investment_transaction_type="sell",
        investment_transaction_subtype="sell",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    reasons = dict(
        db.execute(
            "SELECT source_security_key, reason FROM prep.stg_plaid__opening_lot_review"
        ).fetchall()
    )
    assert reasons["sec_gone"] == "sold_out_prewindow"


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
def test_boundary_lot_prorates_a_half_cent_tie_up(db: Database) -> None:
    """DECIMAL / DECIMAL in DuckDB returns DOUBLE.

    A cast straight from that DOUBLE to DECIMAL(18,2) truncates a half-cent tie
    instead of rounding it. 300.03 x 1/2 = 150.015 exactly; DECIMAL(18,2)'s HALF_UP convention rounds that
    UP to 150.02. The prior boundary-lot test (sec_h, above) rounds identically
    under a naive direct cast OR the DECIMAL(28,10) round-trip -- it asserts
    precision without ever exercising a tie. This case does discriminate: a naive
    ``(lot_basis * drawn_qty / lot_qty)::DECIMAL(18,2)`` cast emits -150.01 (the
    DOUBLE holds 150.01499999999999...); the DECIMAL(28,10) round-trip emits the
    correct -150.02. Verified by reverting the round-trip cast locally: this test
    goes RED (-150.01 != -150.02) without it, GREEN with it restored.
    """
    _raw_holding(db, security_id="sec_tie", quantity="1", cost_basis="150.02")
    _raw_lot(
        db,
        "sec_tie",
        0,
        original_purchase_datetime="2021-01-01 00:00:00",
        quantity="2",
        cost_basis="300.03",
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = _opening(db, "sec_tie")  # gap = 1 - 0 = 1; boundary draws 1 of 2
    assert len(rows) == 1
    assert rows[0][:3] == (Decimal("1"), Decimal("-150.02"), "2021-01-01")


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
def test_shared_institution_lot_id_still_yields_distinct_ids(db: Database) -> None:
    """Two lots sharing a broker institution_lot_id still get distinct ids.

    Same date, same basis, only the institution_lot_id shared. Pre-fix, lot_key
    = COALESCE(institution_lot_id, 'idx_N') drops lot_index the
    moment institution_lot_id is present -- two lots sharing one broker id then
    hash identically, producing two ledger rows with the SAME
    investment_transaction_id (violates core.fct_investment_transactions' declared
    grain) and collapsing both onto one engine lot_id (cost_basis.py's
    by_lot_id dict keeps only one; app.lot_selections becomes ambiguous).
    """
    _raw_holding(db, security_id="sec_shared", quantity="20", cost_basis="200.00")
    for idx in (0, 1):
        _raw_lot(
            db,
            "sec_shared",
            idx,
            institution_lot_id="LOT_SHARED",  # SAME broker id on both lots
            original_purchase_datetime="2021-01-01 00:00:00",  # SAME date
            quantity="10",
            cost_basis="100.00",  # SAME basis
        )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        "SELECT quantity, amount, investment_transaction_id "
        "FROM prep.stg_plaid__opening_lots WHERE source_security_key = 'sec_shared' "
        "ORDER BY investment_transaction_id"
    ).fetchall()
    assert len(rows) == 2  # both lots drawn in full; gap = 20, two lots of 10 each
    assert rows[0][:2] == (Decimal("10"), Decimal("-100.00"))
    assert rows[1][:2] == (Decimal("10"), Decimal("-100.00"))
    assert rows[0][2] != rows[1][2]  # distinct ids despite identical date/basis


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


# ── core.fct_investment_transactions / core.dim_holdings ────────────────────
#
# The core boundary. Three staging branches union into the ledger (manual,
# Plaid transactions, Plaid opening lots); dim_holdings carries the broker's
# NON-AUTHORITATIVE claim beside MoneyBin's own lot-derived position.
#
# Every core test binds its securities: an unbound security stays NULL through
# staging (deliberate — see test_stg_investment_txns_security_id_is_null_
# passthrough), and the cost-basis engine skips NULL-security events, so no lot
# (and therefore no dim_holdings row) would exist to assert on.


@pytest.mark.slow
def test_plaid_rows_flow_to_lots_and_gains_through_unchanged_engine(
    db: Database,
) -> None:
    """Both Plaid branches reach the engine: bootstrap opens lots, the sell consumes them.

    Also pins the sign end-to-end: Plaid's sell (amount -6000 = cash IN) must
    land as POSITIVE proceeds. A second flip anywhere in core would make the
    proceeds negative and the buy-side basis an income.
    """
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
    _link_security(db, "sec_c", "cat0000000c1")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    # bootstrap seeded (case C): the held position is anchored to the snapshot
    holding = db.execute(
        "SELECT quantity, cost_basis FROM core.dim_holdings "
        "WHERE security_id = 'cat0000000c1'"
    ).fetchall()
    assert holding == [(Decimal("40"), Decimal("400.00"))]
    # the in-window sell consumed the residual (unknown-basis) shares first
    gain = db.execute(
        "SELECT quantity, proceeds, basis_incomplete FROM core.fct_realized_gains "
        "WHERE security_id = 'cat0000000c1'"
    ).fetchall()
    assert gain == [(Decimal("60"), Decimal("6000.00"), True)]


@pytest.mark.slow
def test_provider_columns_carried_and_manual_rows_null(db: Database) -> None:
    """Provider fidelity reaches core; the manual branch carries NULLs, never blanks."""
    _raw_investment_txn(db)  # plaid buy
    _manual_investment_txn(db)
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = {
        r[0]: (r[1], r[2], r[3], r[4])
        for r in db.execute(
            "SELECT investment_transaction_id, provider_type, provider_subtype, "
            "source_type, amount FROM core.fct_investment_transactions"
        ).fetchall()
    }
    assert rows["itx_1"] == ("buy", "buy", "plaid", Decimal("-2145.50"))
    assert rows["manual_1"] == (None, None, "manual", Decimal("-100.00"))


@pytest.mark.slow
def test_dim_holdings_reconciles_against_newest_snapshot_only(db: Database) -> None:
    _raw_investment_txn(db, transaction_date="2025-01-10")  # in-window buy of 10
    _raw_holding(db, quantity="10", cost_basis="2000.00")  # first snapshot
    _raw_holding(
        db,
        quantity="10",
        cost_basis="1980.00",  # NEWEST snapshot
        source_file="sync_j2",
        extracted_at="2026-08-01 12:00:00",
    )
    _link_security(db, "sec_1", "cat000000001")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        """
        SELECT quantity, cost_basis, provider_reported_quantity,
               provider_reported_cost_basis, provider_reported_as_of::VARCHAR
        FROM core.dim_holdings WHERE security_id = 'cat000000001'
        """
    ).fetchone()
    # Ledger-derived figures come from the engine, never from the broker's claim.
    assert row == (
        Decimal("10"),
        Decimal("2145.50"),
        Decimal("10"),
        Decimal("1980.00"),  # newest, not the first snapshot's 2000.00
        "2026-08-01 12:00:00",
    )


@pytest.mark.slow
def test_same_day_second_pull_wins_on_extracted_at_not_holdings_date(
    db: Database,
) -> None:
    """Two pulls on one UTC day TIE on holdings_date — extracted_at breaks the tie.

    holdings_date is extracted_at::DATE, so it cannot order same-day snapshots.
    The newer snapshot is deliberately named EARLIER alphabetically, so a
    `holdings_date DESC, source_file DESC` ordering would pick the stale one.
    """
    _raw_investment_txn(db, transaction_date="2025-01-10")
    _raw_holding(db, quantity="10", cost_basis="2000.00", source_file="sync_b")
    _raw_holding(
        db,
        quantity="10",
        cost_basis="1975.00",  # same calendar day, six hours later
        source_file="sync_a",
        extracted_at="2026-07-08 18:00:00",
    )
    _link_security(db, "sec_1", "cat000000001")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        "SELECT provider_reported_cost_basis, provider_reported_as_of::VARCHAR "
        "FROM core.dim_holdings WHERE security_id = 'cat000000001'"
    ).fetchone()
    assert row == (Decimal("1975.00"), "2026-07-08 18:00:00")


@pytest.mark.slow
def test_position_absent_from_newest_snapshot_shows_null(db: Database) -> None:
    _raw_investment_txn(db, transaction_date="2025-01-10")
    _raw_holding(db, quantity="10", cost_basis="1980.00")
    # newest snapshot omits the position entirely (sold elsewhere / feed stopped)
    _raw_holding(
        db,
        security_id="sec_other",
        quantity="1",
        cost_basis="1.00",
        source_file="sync_j2",
        extracted_at="2026-08-01 12:00:00",
    )
    _link_security(db, "sec_1", "cat000000001")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    row = db.execute(
        """
        SELECT quantity, provider_reported_quantity, provider_reported_cost_basis,
               provider_reported_value, provider_reported_as_of
        FROM core.dim_holdings WHERE security_id = 'cat000000001'
        """
    ).fetchone()
    # NULL is itself the reconciliation signal — never a stale survivor.
    assert row == (Decimal("10"), None, None, None, None)


@pytest.mark.slow
def test_item_reporting_zero_holdings_nulls_the_claim_and_surfaces_the_phantom(
    db: Database,
) -> None:
    """An item whose newest pull returns ZERO holdings: the claim goes NULL, doctor warns.

    The fully-liquidated broker. Plaid returns no holding entries for an item
    holding nothing, so the pull writes no holdings ROWS — only a receipt. A
    newest-snapshot join keyed on the presence of holdings rows therefore never
    sees the pull: it silently keeps sync_j1, and ``provider_reported_quantity``
    comes back as the STALE 10 the broker no longer claims. dim_holdings then
    tells the user MoneyBin's 10 shares are broker-confirmed, and the doctor's
    phantom check (which reads that NULL as its signal) reports `pass` on the
    largest possible net-worth overstatement — MoneyBin claims every position,
    the broker claims none.

    Keyed on the receipt, the empty pull IS the newest snapshot: no holdings row
    joins to it, the claim is NULL, and the lot the ledger never closed surfaces.
    """
    _raw_investment_txn(db, transaction_date="2025-01-10")  # in-window buy of 10
    _raw_holding(db, quantity="10", cost_basis="1980.00")  # sync_j1 confirms it
    _link_security(db, "sec_1", "cat000000001")
    # sync_j2: the account is liquidated. The item reported — and reported
    # nothing. Zero holdings rows; the receipt is the only evidence of the pull.
    _raw_holdings_receipt(
        db,
        source_file="sync_j2",
        extracted_at="2026-08-01 12:00:00",
        holdings_count=0,
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT quantity, provider_reported_quantity, provider_reported_as_of
        FROM core.dim_holdings WHERE security_id = 'cat000000001'
        """
    ).fetchone()
    # NULL, not the stale 10 from the superseded sync_j1 snapshot.
    assert row == (Decimal("10"), None, None)

    result = DoctorService(db)._run_investment_phantom_holdings()  # pyright: ignore[reportPrivateUsage]
    assert result.status == "warn"
    assert result.affected_ids == ["acc_1:cat000000001"]


@pytest.mark.slow
def test_two_institutions_holding_one_security_do_not_fan_out(db: Database) -> None:
    """One canonical security at two items: two positions, each with its own claim.

    prep.stg_plaid__securities emits one row per (security_id, source_origin), so
    a join keyed on the canonical security_id alone would double every row. The
    reconciliation join must key on (account_id, security_id) and aggregate to
    exactly one provider-reported row per position.
    """
    for account, security, origin, qty, basis in (
        ("acc_1", "sec_p", "item_1", "10", "1000.00"),
        ("acc_2", "sec_q", "item_2", "40", "4000.00"),
    ):
        _raw_investment_txn(
            db,
            investment_transaction_id=f"itx_{security}",
            account_id=account,
            security_id=security,
            source_origin=origin,
            transaction_date="2025-01-10",
            quantity=qty,
            amount=basis,  # Plaid positive = cash out
        )
        _raw_holding(
            db,
            account_id=account,
            security_id=security,
            source_origin=origin,
            quantity=qty,
            cost_basis=basis,
        )
        # Both provider refs resolve to the SAME canonical security.
        _link_security(db, security, "cat000000001")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    rows = db.execute(
        """
        SELECT account_id, quantity, provider_reported_quantity,
               provider_reported_cost_basis
        FROM core.dim_holdings WHERE security_id = 'cat000000001' ORDER BY account_id
        """
    ).fetchall()
    assert rows == [
        ("acc_1", Decimal("10"), Decimal("10"), Decimal("1000.00")),
        ("acc_2", Decimal("40"), Decimal("40"), Decimal("4000.00")),
    ]


@pytest.fixture
def core_ledger(db: Database) -> Database:
    """One plan over every branch that reaches (or is kept out of) the ledger."""
    _raw_investment_txn(db, investment_transaction_id="itx_buy")
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_div",
        security_id=None,
        quantity=None,
        amount="-50.00",
        investment_transaction_type="cash",
        investment_transaction_subtype="qualified dividend",
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_reinvest",
        quantity="1",
        investment_transaction_type="buy",
        investment_transaction_subtype="dividend reinvestment",
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_tax",
        security_id=None,
        quantity=None,
        investment_transaction_type="fee",
        investment_transaction_subtype="tax withheld",
    )
    # A real (in-window) ACATS transfer_in — the row a bootstrap must not be
    # confused with.
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_acats",
        security_id="sec_t",
        transaction_date="2025-06-01",
        quantity="5",
        amount="0",
        investment_transaction_type="transfer",
        investment_transaction_subtype="transfer",
    )
    # Review-routed: neither may reach the ledger.
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_split",
        investment_transaction_type="transfer",
        investment_transaction_subtype="split",
    )
    _raw_investment_txn(
        db,
        investment_transaction_id="itx_unmapped",
        investment_transaction_subtype="quantum entanglement",
    )
    # A pre-window position with no acquiring transaction → bootstrap transfer_in.
    _raw_holding(db, security_id="sec_boot", quantity="100", cost_basis="1000.00")
    _raw_lot(
        db,
        "sec_boot",
        0,
        original_purchase_datetime="2021-03-11 00:00:00",
        quantity="100",
        cost_basis="1000.00",
    )
    _manual_investment_txn(db)
    _link_security(db, "sec_1", "cat000000001")
    _link_security(db, "sec_t", "cat000000002")
    _link_security(db, "sec_boot", "cat000000003")
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    return db


@pytest.mark.slow
def test_core_excludes_review_routed_rows(core_ledger: Database) -> None:
    """`WHERE ledger_include` is load-bearing: review rows stay visible in staging only."""
    ids = {
        r[0]
        for r in core_ledger.execute(
            "SELECT investment_transaction_id FROM core.fct_investment_transactions"
        ).fetchall()
    }
    assert {"itx_buy", "itx_div", "itx_reinvest", "itx_tax", "itx_acats"} <= ids
    assert "manual_1" in ids
    assert not {"itx_split", "itx_unmapped"} & ids
    # ...but they are still staged for the doctor to review.
    staged = core_ledger.execute(
        "SELECT COUNT(*) FROM prep.stg_plaid__investment_transactions "
        "WHERE NOT ledger_include"
    ).fetchone()
    assert staged is not None and staged[0] == 2


@pytest.mark.slow
def test_core_ledger_vocabulary_is_closed(core_ledger: Database) -> None:
    """The ledger vocabulary at the CORE boundary is user-authorable ∪ pipeline-emitted.

    Staging pins its own output (test_taxonomy_emits_only_closed_vocabulary); this
    pins the union of every branch that reaches the public table, so a new value
    cannot slip in through the bootstrap or the manual branch unnoticed.
    """
    closed = {
        type_: _SUBTYPE_VOCAB.get(type_, frozenset())
        | _PIPELINE_EMITTED_SUBTYPES.get(type_, frozenset())
        for type_ in TAXONOMY
    }
    pairs = core_ledger.execute(
        "SELECT DISTINCT type, subtype FROM core.fct_investment_transactions"
    ).fetchall()
    assert pairs, "fixture produced no ledger rows"
    for type_, subtype in pairs:
        assert type_ in TAXONOMY, f"leaked type {type_!r}"
        assert subtype is None or subtype in closed[type_], (
            f"leaked subtype {subtype!r} for type {type_!r}"
        )
    # Both halves of the superset are actually exercised by the fixture.
    assert ("reinvest", "dividend") in pairs  # user-authorable
    assert ("transfer_in", "opening_bootstrap") in pairs  # pipeline-emitted only


@pytest.mark.slow
def test_bootstrap_row_is_distinguishable_from_a_real_transfer_in(
    core_ledger: Database,
) -> None:
    """A reconstruction must never read as an observation.

    Both are type='transfer_in'; only the bootstrap carries
    subtype='opening_bootstrap' (impossible to hand-author — it is not in the
    user-authorable vocabulary), so the doctor and any consumer can tell the
    reconstructed pre-window lot from a broker-reported ACATS transfer.
    """
    rows = {
        r[0]: (r[1], r[2])
        for r in core_ledger.execute(
            "SELECT security_id, subtype, investment_transaction_id "
            "FROM core.fct_investment_transactions WHERE type = 'transfer_in'"
        ).fetchall()
    }
    acats_subtype, acats_id = rows["cat000000002"]
    boot_subtype, boot_id = rows["cat000000003"]
    assert acats_subtype is None
    assert acats_id == "itx_acats"
    assert boot_subtype == "opening_bootstrap"
    assert boot_id.startswith("plaid_opening_")
