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
