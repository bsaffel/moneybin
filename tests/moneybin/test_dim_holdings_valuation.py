"""core.dim_holdings valuation: market value, staleness, and honest NULLs."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.services.investment_service import InvestmentService

pytestmark = pytest.mark.integration

_DEFAULT_SECURITY_ID = "canonvti0000001"


def _provider_key(security_id: str) -> str:
    """The provider's own id for a canonical security — derived, never hardcoded.

    ``_seed_price`` writes both the raw price row and the ``app.security_links``
    binding that resolves it. A hardcoded key/link_id pair collides on the link
    primary key the second time a test seeds a *different* security:
    ``ON CONFLICT DO NOTHING`` discards the second binding, the second security's
    price is dropped by ``prep.stg_security_prices``' INNER JOIN, and its position
    reads ``unpriced`` for a reason having nothing to do with the model under test.
    Deriving the key from the security_id makes the helper honor its own signature.
    """
    return f"prov_{security_id}"


_DEFAULT_PROVIDER_KEY = _provider_key(_DEFAULT_SECURITY_ID)


def _case_account(case_id: str) -> str:
    return f"mb21_valuation_{case_id}_account"


def _case_security(case_id: str) -> str:
    return f"mb21_valuation_{case_id}_security"


def _case_origin(case_id: str) -> str:
    return f"mb21_valuation_{case_id}"


def _db_today(db: Database) -> date:
    """The database's own CURRENT_DATE — the clock ``core.dim_holdings`` reads.

    Python's ``date.today()`` is evaluated before ``ctx.plan()``, a multi-second
    operation, while the model re-evaluates SQL ``CURRENT_DATE`` *during* the plan.
    A shard seeding at 23:59:58 and materializing at 00:00:03 sees the two disagree,
    flipping ``days_since_observed`` from 0 to 1 and ``valuation_status`` from
    ``valued`` to ``carried_forward``. Every date-sensitive assertion in this module
    is therefore anchored to this function's value read before seeding, and compared
    against it re-read after the plan — never against the wall clock.
    """
    row = db.execute("SELECT CURRENT_DATE").fetchone()
    assert row is not None
    return row[0]


def _expected_status(elapsed_days: int) -> str:
    """The status a priced, non-withheld position carries ``elapsed_days`` on.

    ``valued`` iff the close is today's. Expressing it as a function of the elapsed
    days — rather than pinning the literal ``valued`` — keeps the assertion exact in
    the ordinary case and correct rather than flaky across a midnight boundary.
    """
    return "valued" if elapsed_days == 0 else "carried_forward"


def _seed_security(db: Database, *, security_id: str = _DEFAULT_SECURITY_ID) -> None:
    """The canonical catalog row — needed for any position, manual or broker-derived."""
    db.execute(
        """
        INSERT INTO app.securities (security_id, name, security_type, ticker)
        VALUES (?, 'Vanguard Total Stock Market ETF', 'etf', 'VTI')
        ON CONFLICT DO NOTHING
        """,  # noqa: S608  # test fixture, not executing user SQL
        [security_id],
    )


_POSITION_TRADE_DATE = date(2026, 1, 5)

# The two fixture timestamps behind the overlap-watermark case. Spelled here as
# constants because the assertion is an equality against them, not a comparison
# against whatever the model happened to produce.
_OVERLAP_HELD_LOT_AT = "2026-01-06 09:00:00"
_OVERLAP_SECOND_SOURCE_AT = "2026-02-10 09:00:00"


def _seed_position(
    db: Database,
    *,
    security_id: str = _DEFAULT_SECURITY_ID,
    account_id: str = "acc_1",
    currency_code: str | None = "USD",
    price: str | None = "100.00",
    transaction_id: str = "buy_1",
    trade_date: date = _POSITION_TRADE_DATE,
    quantity: str = "10",
    amount: str | None = "-1000.00",
    transaction_type: str = "buy",
    created_at: str | None = None,
) -> None:
    """A MANUAL position: 10 units at 100.00, cost basis 1000.00, in account acc_1.

    ``price=None`` records the total without a per-unit price — a real shape for a
    broker CSV or a transfer-in, and the only way to seed a position that carries NO
    price at all. Since C.2, a per-unit price on a ledger event becomes a
    ``trade_implied`` observation in ``core.fct_security_prices`` on its trade date,
    so the default fixture is priced from ``_POSITION_TRADE_DATE`` onward whether or
    not a provider close is seeded. Tests isolating a *provider* pricing rule pass
    ``price=None`` so the trade-implied branch does not supply a competing close.
    """
    _seed_security(db, security_id=security_id)
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees, created_by,
            investment_transaction_id, currency_code, created_at
        ) VALUES (?, ?, ?, ?, 'VTI', ?,
                  ?::DATE, ?, ?, ?, 0.00, 'test', ?, ?,
                  COALESCE(?::TIMESTAMP, CURRENT_TIMESTAMP))
        """,  # noqa: S608  # test fixture, not executing user SQL
        [
            transaction_id,
            f"imp_{transaction_id}",
            account_id,
            security_id,
            transaction_type,
            trade_date,
            quantity,
            price,
            amount,
            transaction_id,
            currency_code,
            created_at,
        ],
    )


def _seed_price(
    db: Database,
    *,
    price_date: date,
    close: str,
    security_id: str = _DEFAULT_SECURITY_ID,
    quote_currency: str = "USD",
    extracted_at: str | None = None,
) -> None:
    """One raw close plus the accepted binding that resolves it to ``security_id``.

    Both the provider key and the link id are derived from ``security_id`` (see
    ``_provider_key``), so seeding two securities produces two distinct, both-accepted
    bindings rather than one accepted and one silently swallowed by ON CONFLICT.

    ``extracted_at`` overrides the observation's provider-served timestamp (default
    ``CURRENT_TIMESTAMP``) — the freshness ``core.fct_security_prices`` carries as
    ``updated_at`` and ``core.dim_holdings`` folds into its own row watermark.
    """
    provider_key = _provider_key(security_id)
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source_type,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES (?, ?, ?, 'plaid', 'item_1', ?, 'raw',
                COALESCE(?::TIMESTAMP, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [provider_key, price_date, quote_currency, close, extracted_at],
    )
    db.execute(
        """
        INSERT INTO app.security_links
            (link_id, security_id, ref_kind, ref_value, source_type,
             status, decided_by, decided_at)
        VALUES (?, ?, 'plaid_security_id', ?, 'plaid',
                'accepted', 'auto', CURRENT_TIMESTAMP)
        ON CONFLICT DO NOTHING
        """,  # noqa: S608  # test fixture, not executing user SQL
        [f"link_{provider_key}", security_id, provider_key],
    )


def _seed_broker_snapshot(
    db: Database,
    *,
    account_id: str,
    quantity: str,
    security_id: str = _DEFAULT_PROVIDER_KEY,
    source_file: str = "sync_job_1",
    source_origin: str = "item_1",
    extracted_at: str | None = None,
) -> None:
    """One broker snapshot: the receipt plus the holding row it accounts for.

    Both rows are required. core.dim_holdings derives "the newest snapshot" from the
    RECEIPT table, never from the holdings rows, so a holdings row written without a
    matching receipt never joins and the provider claim silently reads NULL — the
    divergence under test would not fire and the assertion would pass for the wrong
    reason.

    ``account_id`` is written as the canonical id directly:
    prep.stg_plaid__investment_holdings COALESCEs to the source-native id when no
    account_link resolves, so this needs no account binding. ``security_id`` is the
    PROVIDER id; the default resolves to canonvti0000001 through the
    'plaid_security_id' link ``_seed_price`` writes, and any other value stays
    unresolved (canonical NULL) — which is what a phantom-position fixture wants: the
    account is broker-covered, but the position under test is absent from the claim.
    """
    db.execute(
        """
        INSERT INTO raw.plaid_investment_holdings_snapshots (
            source_origin, source_file, holdings_date, holdings_count,
            transactions_window_start, source_type, extracted_at, loaded_at
        ) VALUES (?, ?, CURRENT_DATE, 1, DATE '2026-01-01', 'plaid',
                  COALESCE(?::TIMESTAMP, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
        ON CONFLICT DO NOTHING
        """,  # noqa: S608  # test fixture, not executing user SQL
        [source_origin, source_file, extracted_at],
    )
    db.execute(
        """
        INSERT INTO raw.plaid_investment_holdings (
            account_id, security_id, holdings_date, institution_price,
            institution_price_as_of, institution_value, cost_basis, quantity,
            iso_currency_code, transactions_window_start, source_file,
            source_type, source_origin, extracted_at, loaded_at
        ) VALUES (?, ?, CURRENT_DATE, 120.00, CURRENT_DATE, NULL, NULL, ?,
                  'USD', DATE '2026-01-01', ?, 'plaid', ?,
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [account_id, security_id, quantity, source_file, source_origin],
    )


def _seed_plaid_buy(
    db: Database,
    *,
    account_id: str,
    security_id: str = _DEFAULT_SECURITY_ID,
    source_origin: str = "item_1",
    trade_date: date = _POSITION_TRADE_DATE,
    quantity: str = "10",
    loaded_at: str | None = None,
) -> None:
    """A Plaid buy that REACHES the ledger, on the canonical account id.

    Distinct from ``_seed_split_reject``, whose split subtype is routed to review
    and never lands in ``core.fct_investment_transactions`` — a reject cannot
    double-count anything, so it is not a source overlap. This one carries
    ``buy``/``buy``, which staging maps and includes.

    ``account_id`` is written as the canonical id directly:
    prep.stg_plaid__investment_transactions COALESCEs to the source-native id
    when no account_link resolves, so this needs no binding. ``amount`` is
    POSITIVE, Plaid's own convention for cash out; staging owns the one sign
    flip and the ledger sees -1000.00. ``security_id`` is passed as the provider
    key so ``_seed_price``'s ``plaid_security_id`` link resolves it onto the
    canonical security — the position then double-counts on one grain, which is
    the failure under test rather than two unrelated rows.
    """
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transactions (
            investment_transaction_id, account_id, security_id,
            investment_transaction_type, investment_transaction_subtype,
            transaction_date, quantity, price, amount, fees, iso_currency_code,
            source_file, source_type, source_origin, extracted_at, loaded_at
        ) VALUES (?, ?, ?, 'buy', 'buy', ?,
                  ?, 100.00, 1000.00, 0.00, 'USD', 'sync_test', 'plaid', ?,
                  CURRENT_TIMESTAMP, COALESCE(?::TIMESTAMP, CURRENT_TIMESTAMP))
        """,  # noqa: S608  # test fixture, not executing user SQL
        [
            f"itx_buy_{account_id}",
            account_id,
            _provider_key(security_id),
            trade_date,
            quantity,
            source_origin,
            loaded_at,
        ],
    )


def _seed_liquidated_snapshot(db: Database, *, source_origin: str) -> None:
    """A snapshot receipt reporting ZERO holdings — the pull where the broker holds nothing.

    Receipt only, deliberately: Plaid returns no holding entries for an item that
    holds nothing, so the liquidated pull writes ``holdings_count = 0`` and not a
    single holdings row. Writing one here would destroy the case under test — the
    account would regain coverage through the holdings leg of the union and the
    narrower, holdings-only scope would look correct.
    """
    db.execute(
        """
        INSERT INTO raw.plaid_investment_holdings_snapshots (
            source_origin, source_file, holdings_date, holdings_count,
            transactions_window_start, source_type, extracted_at, loaded_at
        ) VALUES (?, 'sync_job_liquidated', CURRENT_DATE, 0,
                  DATE '2026-01-01', 'plaid', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [source_origin],
    )


def _seed_split_reject(
    db: Database,
    *,
    account_id: str,
    provider_security_key: str = _DEFAULT_PROVIDER_KEY,
    source_origin: str = "item_1",
    trade_date: date = date(2026, 6, 1),
) -> None:
    """A Plaid split routed to review: held out of the ledger, quantity not restated.

    Every Plaid split is routed to review as ``split_underivable``
    (prep.stg_plaid__investment_transactions, GOLDEN-GATED 1 of 3), so no fixture
    plumbing is needed beyond the raw row — the subtype alone produces the reject.
    ``amount`` is 0.00, not NULL: raw.plaid_investment_transactions.amount is NOT
    NULL, and 0 is what Plaid sends on a split.
    """
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transactions (
            investment_transaction_id, account_id, security_id,
            investment_transaction_type, investment_transaction_subtype,
            transaction_date, quantity, price, amount, fees, iso_currency_code,
            source_file, source_type, source_origin, extracted_at, loaded_at
        ) VALUES (?, ?, ?, 'transfer', 'split', ?,
                  4, NULL, 0.00, NULL, 'USD', 'sync_test', 'plaid', ?,
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [
            f"itx_split_{account_id}",
            account_id,
            provider_security_key,
            trade_date,
            source_origin,
        ],
    )


def _holding(db: Database, account_id: str) -> tuple[object, ...]:
    """Fetch the one dim_holdings row for acc_1's position and assert it IS one.

    ``fetchall()`` (not ``fetchone()``) plus an explicit count check: a bug that
    fans a position out to two rows (e.g. a price join missing the currency
    predicate, matching every quote currency instead of the position's own) would
    otherwise pass or fail depending on DuckDB's arbitrary row-return order rather
    than deterministically failing — grain (account_id, security_id) uniqueness is
    this model's own contract, not an incidental assumption of the test.
    """
    rows = db.execute(
        """
        SELECT market_value, unrealized_gain, price_date, price_source,
               days_since_observed, valuation_status
        FROM core.dim_holdings
        WHERE account_id = ?
        """,
        [account_id],
    ).fetchall()
    assert len(rows) == 1, (
        f"expected exactly one dim_holdings row for {account_id} (grain violation): {rows}"
    )
    return rows[0]


def _assert_withheld_publishes_nothing(row: tuple[object, ...]) -> None:
    """A withheld row carries no figure AND no pricing metadata.

    Blanking market_value while still reporting ``price_date``/``price_source``/
    ``days_since_observed`` let the CLI print ``market_value=- status=withheld
    as_of=<today> (0d)`` — a zero-day-old price beside blank money, which reads as
    "pricing is current, something unrelated is missing" rather than "the share count
    is disputed". All five are NULL together.
    """
    market_value, gain, price_date, source, days, status = row
    assert status == "withheld"
    assert market_value is None
    assert gain is None
    assert price_date is None, "a withheld row must not advertise a price date"
    assert source is None, "a withheld row must not advertise a price source"
    assert days is None, "a withheld row must not advertise price freshness"


def _resolved_close(db: Database, security_id: str, price_date: date) -> object:
    """The close ``core.fct_security_prices`` holds for the default security.

    The withhold assertions above need to prove a close actually RESOLVED — otherwise
    they would pass identically against a model that priced nothing at all. Since
    ``core.dim_holdings`` deliberately no longer republishes it on a withheld row, the
    proof moves one model over. That relocation is the point: the diagnostic still
    exists for a support path, just not on the row that must not make a claim.
    """
    rows = db.execute(
        """
        SELECT close FROM core.fct_security_prices
        WHERE security_id = ? AND price_date = ? AND quote_currency = 'USD'
        """,
        [security_id, price_date],
    ).fetchall()
    assert len(rows) == 1, f"expected exactly one resolved close: {rows}"
    return rows[0][0]


def _account_quantities(db: Database, account_id: str) -> tuple[object, object]:
    """(ledger quantity, provider claim) for acc_1 — the pair a withhold rests on."""
    rows = db.execute(
        """
        SELECT quantity, provider_reported_quantity
        FROM core.dim_holdings
        WHERE account_id = ?
        """,
        [account_id],
    ).fetchall()
    assert len(rows) == 1, (
        f"expected exactly one dim_holdings row for {account_id}: {rows}"
    )
    return rows[0][0], rows[0][1]


@dataclass(frozen=True)
class _ValuationCases:
    db: Database
    anchor: date


@dataclass(frozen=True)
class _ValuationTemplate:
    path: Path
    anchor: date


@pytest.fixture(scope="module")
def valuation_cases_template(
    tmp_path_factory: pytest.TempPathFactory,
    request: pytest.FixtureRequest,
) -> _ValuationTemplate:
    """One real plan over the module's independently namespaced valuation cases."""
    secret_store = MagicMock()
    secret_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    db = Database(
        tmp_path_factory.mktemp("dim_holdings_valuation") / "test.duckdb",
        secret_store=secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    request.addfinalizer(db.close)
    anchor = _db_today(db)

    def account(case_id: str) -> str:
        return _case_account(case_id)

    def security(case_id: str) -> str:
        return _case_security(case_id)

    def origin(case_id: str) -> str:
        return _case_origin(case_id)

    # Straightforward price-resolution and currency cases.
    for case_id, price_date, close, price in (
        ("same_day", anchor, "120.00", "100.00"),
        ("older", anchor - timedelta(days=3), "120.00", "100.00"),
        ("future", anchor + timedelta(days=5), "500.00", "100.00"),
        ("unpriced", None, None, None),
        ("casing", anchor, "120.00", "100.00"),
    ):
        _seed_position(
            db,
            account_id=account(case_id),
            security_id=security(case_id),
            price=price,
            currency_code="usd" if case_id == "casing" else "USD",
            transaction_id=f"{origin(case_id)}_buy",
        )
        if price_date is not None and close is not None:
            _seed_price(
                db,
                security_id=security(case_id),
                price_date=price_date,
                close=close,
                quote_currency="usd" if case_id == "casing" else "USD",
            )

    _seed_position(
        db,
        account_id=account("recent"),
        security_id=security("recent"),
        transaction_id=f"{origin('recent')}_buy",
    )
    _seed_price(
        db,
        security_id=security("recent"),
        price_date=anchor - timedelta(days=10),
        close="50.00",
    )
    _seed_price(
        db,
        security_id=security("recent"),
        price_date=anchor - timedelta(days=2),
        close="120.00",
    )

    _seed_position(
        db,
        account_id=account("other_currency"),
        security_id=security("other_currency"),
        price=None,
        transaction_id=f"{origin('other_currency')}_buy",
    )
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source_type,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES (?, CURRENT_DATE, 'GBP', 'plaid', ?, 95.00, 'raw',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [_provider_key(security("other_currency")), origin("other_currency")],
    )
    _seed_price(
        db,
        security_id=security("other_currency"),
        price_date=anchor - timedelta(days=400),
        close="1.00",
    )

    # Split rejection, reconciliation, and price-after-split cases.
    for case_id, split_date, close_date, close in (
        ("split_withhold", date(2026, 6, 1), anchor, "120.00"),
        ("split_recorded", date(2026, 6, 1), anchor, "120.00"),
        ("split_exdate", date(2026, 6, 1), anchor, "120.00"),
        ("split_pre_price", date(2026, 6, 1), date(2026, 5, 1), "120.00"),
        ("split_post_price", date(2026, 6, 1), date(2026, 6, 15), "120.00"),
    ):
        _seed_position(
            db,
            account_id=account(case_id),
            security_id=security(case_id),
            transaction_id=f"{origin(case_id)}_buy",
        )
        _seed_price(
            db, security_id=security(case_id), price_date=close_date, close=close
        )
        if case_id == "split_withhold":
            _seed_split_reject(
                db,
                account_id=account(case_id),
                provider_security_key=_provider_key(security(case_id)),
                source_origin=origin(case_id),
                trade_date=split_date,
            )
        else:
            _seed_position(
                db,
                account_id=account(case_id),
                security_id=security(case_id),
                transaction_id=f"{origin(case_id)}_split",
                transaction_type="split",
                trade_date=date(2026, 5, 31)
                if case_id == "split_exdate"
                else split_date,
                quantity="4",
                price=None,
                amount=None,
            )
            if case_id in {"split_recorded", "split_exdate"}:
                _seed_split_reject(
                    db,
                    account_id=account(case_id),
                    provider_security_key=_provider_key(security(case_id)),
                    source_origin=origin(case_id),
                    trade_date=split_date,
                )

    sibling_security = security("sibling")
    _seed_position(
        db,
        account_id=account("sibling_a"),
        security_id=sibling_security,
        transaction_id=f"{origin('sibling')}_buy_a",
    )
    _seed_position(
        db,
        account_id=account("sibling_b"),
        security_id=sibling_security,
        transaction_id=f"{origin('sibling')}_buy_b",
        quantity="5",
        amount="-500.00",
    )
    _seed_price(db, security_id=sibling_security, price_date=anchor, close="120.00")
    _seed_split_reject(
        db,
        account_id=account("sibling_a"),
        provider_security_key=_provider_key(sibling_security),
        source_origin=origin("sibling"),
    )

    _seed_security(db, security_id=security("incomplete_basis"))
    _seed_price(
        db, security_id=security("incomplete_basis"), price_date=anchor, close="120.00"
    )
    _seed_position(
        db,
        account_id=account("incomplete_basis"),
        security_id=security("incomplete_basis"),
        transaction_id=f"{origin('incomplete_basis')}_transfer",
        transaction_type="transfer_in",
        price=None,
        amount=None,
    )

    _seed_position(
        db,
        account_id=account("divergence"),
        security_id=security("divergence"),
        transaction_id=f"{origin('divergence')}_buy",
    )
    _seed_price(
        db, security_id=security("divergence"), price_date=anchor, close="120.00"
    )
    _seed_broker_snapshot(
        db,
        account_id=account("divergence"),
        quantity="40",
        security_id=_provider_key(security("divergence")),
        source_origin=origin("divergence"),
    )

    for case_id, broker_first, broker_second in (
        ("phantom", True, True),
        ("manual_covered", False, True),
        ("liquidated", True, False),
        ("broker_matching", True, False),
        ("updated_omitting", True, True),
    ):
        case_security = security(case_id)
        case_account = account(case_id)
        _seed_security(db, security_id=case_security)
        _seed_price(
            db,
            security_id=case_security,
            price_date=date(2026, 7, 15) if case_id == "updated_omitting" else anchor,
            close="120.00",
        )
        if broker_first:
            _seed_broker_snapshot(
                db,
                account_id=case_account,
                quantity="10",
                security_id=_provider_key(case_security),
                source_origin=origin(case_id),
            )
        else:
            _seed_position(
                db,
                account_id=case_account,
                security_id=case_security,
                transaction_id=f"{origin(case_id)}_buy",
            )
        if case_id == "liquidated":
            _seed_liquidated_snapshot(db, source_origin=origin(case_id))
        elif broker_second:
            _seed_broker_snapshot(
                db,
                account_id=case_account,
                quantity="7",
                security_id=f"{origin(case_id)}_unbound",
                source_file="sync_job_2",
                source_origin=origin(case_id),
                extracted_at="2099-01-01 00:00:00"
                if case_id == "updated_omitting"
                else None,
            )

    _seed_position(
        db,
        account_id=account("manual_only"),
        security_id=security("manual_only"),
        transaction_id=f"{origin('manual_only')}_buy",
    )
    _seed_price(
        db, security_id=security("manual_only"), price_date=anchor, close="120.00"
    )

    _seed_security(db, security_id=security("post_reject"))
    _seed_price(
        db, security_id=security("post_reject"), price_date=anchor, close="120.00"
    )
    _seed_split_reject(
        db,
        account_id=account("post_reject"),
        provider_security_key=_provider_key(security("post_reject")),
        source_origin=origin("post_reject"),
    )
    _seed_position(
        db,
        account_id=account("post_reject"),
        security_id=security("post_reject"),
        transaction_id=f"{origin('post_reject')}_buy",
        trade_date=date(2026, 6, 15),
    )

    _seed_position(
        db,
        account_id=account("updated_close"),
        security_id=security("updated_close"),
        transaction_id=f"{origin('updated_close')}_buy",
    )
    _seed_price(
        db,
        security_id=security("updated_close"),
        price_date=anchor,
        close="120.00",
        extracted_at="2099-01-01 00:00:00",
    )

    unknown_security = security("unknown_currency_mix")
    unknown_account = account("unknown_currency_mix")
    _seed_position(
        db,
        account_id=unknown_account,
        security_id=unknown_security,
        transaction_id=f"{origin('unknown_currency_mix')}_eur",
        currency_code="EUR",
    )
    _seed_position(
        db,
        account_id=unknown_account,
        security_id=unknown_security,
        transaction_id=f"{origin('unknown_currency_mix')}_unknown",
        currency_code=None,
        trade_date=date(2026, 1, 6),
        quantity="5",
        price="90.00",
        amount="-450.00",
    )
    _seed_price(db, security_id=unknown_security, price_date=anchor, close="120.00")

    # Source overlap: one account whose investment ledger arrives from BOTH a
    # manual import and a Plaid sync, on one security. Both feeds price cleanly
    # and the close resolves, so a blank figure here can only come from the
    # withhold — not from a missing price.
    for case_id in ("source_overlap", "overlap_beats_withheld"):
        _seed_position(
            db,
            account_id=account(case_id),
            security_id=security(case_id),
            transaction_id=f"{origin(case_id)}_buy",
        )
        _seed_price(
            db, security_id=security(case_id), price_date=anchor, close="120.00"
        )
        _seed_plaid_buy(
            db,
            account_id=account(case_id),
            security_id=security(case_id),
            source_origin=origin(case_id),
        )
    # ...and a broker snapshot contradicting the (now doubled) share count, so
    # both withhold reasons hold at once and the precedence is observable.
    _seed_broker_snapshot(
        db,
        account_id=account("overlap_beats_withheld"),
        quantity="7",
        security_id=_provider_key(security("overlap_beats_withheld")),
        source_file="sync_job_overlap",
        source_origin=origin("overlap_beats_withheld"),
    )

    # A manual ledger beside a broker SNAPSHOT (no Plaid transaction). The snapshot
    # makes the opening-lot bootstrap synthesize a plaid-sourced transfer_in, so the
    # ledger carries two source_types — but the bootstrap reconstructs a pre-window
    # position rather than re-reporting an event, so this is not an overlap.
    _seed_security(db, security_id=security("bootstrap_only"))
    _seed_price(
        db, security_id=security("bootstrap_only"), price_date=anchor, close="120.00"
    )
    _seed_broker_snapshot(
        db,
        account_id=account("bootstrap_only"),
        quantity="10",
        security_id=_provider_key(security("bootstrap_only")),
        source_origin=origin("bootstrap_only"),
    )
    _seed_position(
        db,
        account_id=account("bootstrap_only"),
        security_id=security("bootstrap_only"),
        transaction_id=f"{origin('bootstrap_only')}_buy",
    )

    # The control: a Plaid-only account is ONE source and values normally. The
    # withhold keys on a second source, not on the presence of a connector.
    _seed_security(db, security_id=security("plaid_only"))
    _seed_price(
        db, security_id=security("plaid_only"), price_date=anchor, close="120.00"
    )
    _seed_plaid_buy(
        db,
        account_id=account("plaid_only"),
        security_id=security("plaid_only"),
        source_origin=origin("plaid_only"),
    )

    # The watermark case: ONE account, TWO securities. The held security's own
    # inputs (its lot; it has no price and no snapshot) are older than the
    # SECOND security's plaid event, and that event is what flips the held
    # security's status. A watermark folding only position-scoped timestamps
    # would leave the held row reading as unchanged.
    _seed_position(
        db,
        account_id=account("overlap_watermark"),
        security_id=security("overlap_watermark"),
        transaction_id=f"{origin('overlap_watermark')}_buy",
        price=None,
        created_at=_OVERLAP_HELD_LOT_AT,
    )
    _seed_security(db, security_id=security("overlap_watermark_b"))
    _seed_price(
        db,
        security_id=security("overlap_watermark_b"),
        price_date=anchor,
        close="120.00",
    )
    _seed_plaid_buy(
        db,
        account_id=account("overlap_watermark"),
        security_id=security("overlap_watermark_b"),
        source_origin=origin("overlap_watermark"),
        loaded_at=_OVERLAP_SECOND_SOURCE_AT,
    )

    # Its unaffected twin: one account, one source, one pinned input and no
    # price or snapshot — so its watermark is exactly its lot's, and inheriting
    # the overlap account's newer timestamp would be visible immediately.
    _seed_position(
        db,
        account_id=account("clean_watermark"),
        security_id=security("clean_watermark"),
        transaction_id=f"{origin('clean_watermark')}_buy",
        price=None,
        created_at=_OVERLAP_HELD_LOT_AT,
    )

    mixed_security = security("mixed_currency")
    mixed_account = account("mixed_currency")
    _seed_position(
        db,
        account_id=mixed_account,
        security_id=mixed_security,
        transaction_id=f"{origin('mixed_currency')}_usd",
    )
    _seed_position(
        db,
        account_id=mixed_account,
        security_id=mixed_security,
        transaction_id=f"{origin('mixed_currency')}_eur",
        currency_code="EUR",
        trade_date=date(2026, 1, 6),
        quantity="5",
        price="90.00",
        amount="-450.00",
    )
    _seed_price(db, security_id=mixed_security, price_date=anchor, close="120.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    db.close()
    return _ValuationTemplate(path=db.path, anchor=anchor)


@pytest.fixture()
def valuation_cases(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    valuation_cases_template: _ValuationTemplate,
) -> _ValuationCases:
    """An isolated planned snapshot for one valuation assertion case."""
    path = tmp_path / "test.duckdb"
    shutil.copy(valuation_cases_template.path, path)
    secret_store = MagicMock()
    secret_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    db = Database(
        path,
        secret_store=secret_store,
        no_auto_upgrade=True,
        assume_initialized=True,
        read_only=False,
    )
    request.addfinalizer(db.close)
    return _ValuationCases(db=db, anchor=valuation_cases_template.anchor)


@pytest.mark.slow
def test_same_day_price_values_the_position(valuation_cases: _ValuationCases) -> None:
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    elapsed = (_db_today(db) - anchor).days
    market_value, gain, _pd, source, days, status = _holding(
        db, _case_account("same_day")
    )
    assert market_value == Decimal("1200.00")
    assert gain == Decimal("200.00"), "market value less cost basis"
    assert source == "plaid"
    assert days == elapsed
    assert status == _expected_status(elapsed)


@pytest.mark.slow
def test_older_price_carries_forward_with_rising_staleness(
    valuation_cases: _ValuationCases,
) -> None:
    """Markets close ~114 days a year; as-of resolution is what makes a series possible."""
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _source, days, status = _holding(
        db, _case_account("older")
    )
    assert market_value == Decimal("1200.00")
    assert days == elapsed + 3
    assert status == "carried_forward"


@pytest.mark.slow
def test_most_recent_of_two_past_prices_wins(valuation_cases: _ValuationCases) -> None:
    """The as-of pick is 'most recent on or before today', not merely 'any eligible row'.

    None of the other fixtures in this module ever insert two same-security,
    same-currency observations, so `QUALIFY ROW_NUMBER() ... ORDER BY price_date
    DESC` is otherwise never exercised — a model that picked ANY eligible row
    (e.g. DuckDB's scan order, or `ORDER BY price_date ASC`) would pass every
    other test here unnoticed. The older, wrong-answer row is inserted FIRST so
    a table-scan-order bug produces the stale close (50.00) instead of the
    correct one (120.00) — inserting the winner first would let that exact bug
    pass by coincidence.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, price_date, _source, days, status = _holding(
        db, _case_account("recent")
    )
    assert market_value == Decimal("1200.00"), "the newer close (120.00) must win"
    assert price_date == anchor - timedelta(days=2)
    assert days == elapsed + 2
    assert status == "carried_forward"


@pytest.mark.slow
def test_future_price_never_values_an_earlier_date(
    valuation_cases: _ValuationCases,
) -> None:
    """A close dated ahead of today is not a candidate — the trade price is.

    Before C.2 this position had no usable price at all and the assertion was simply
    'unpriced'. The trade-implied branch now supplies a legitimate past observation on
    the trade date, which makes the test strictly stronger: the future close must lose
    to that older close rather than merely fail to resolve. A model that admitted
    future dates would publish 5000.00 here instead of 1000.00.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    market_value, gain, price_date, source, days, status = _holding(
        db, _case_account("future")
    )
    assert market_value == Decimal("1000.00"), (
        "10 units at the 100.00 trade price — not 5000.00 from the future close"
    )
    assert gain == Decimal("0.00")
    assert price_date == _POSITION_TRADE_DATE
    assert source == "trade_implied"
    assert days == (anchor - _POSITION_TRADE_DATE).days
    assert status == "carried_forward"


@pytest.mark.slow
def test_unpriced_holding_is_null_never_zero(valuation_cases: _ValuationCases) -> None:
    """Zero is indistinguishable from a worthless position and understates every total.

    ``price=None`` is what makes this position genuinely unpriced since C.2: a ledger
    event carrying a per-unit price becomes a trade_implied observation, so the priced
    default fixture no longer reaches the unpriced path at all. The total (-1000.00)
    still funds cost basis, which is what keeps this a position rather than a no-op.
    """
    db = valuation_cases.db
    market_value, gain, price_date, source, days, status = _holding(
        db, _case_account("unpriced")
    )
    assert market_value is None
    assert gain is None
    assert price_date is None
    assert source is None
    assert days is None
    assert status == "unpriced"


@pytest.mark.slow
def test_price_in_another_currency_does_not_value_the_position(
    valuation_cases: _ValuationCases,
) -> None:
    """Valuing a USD position at a GBP close would be silently wrong; M1K.2 converts.

    ``price=None`` keeps the trade-implied branch out of this fixture so the currency
    predicate is the only rule under test. With a trade price the position would carry
    a USD observation newer than the 400-day-old seeded close, and the assertion below
    would pass for a reason having nothing to do with currency.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    _mv, _gain, price_date, _source, _days, _status = _holding(
        db, _case_account("other_currency")
    )
    assert price_date == anchor - timedelta(days=400), (
        "the GBP close must not win over an older USD one"
    )


@pytest.mark.slow
def test_currency_casing_mismatch_still_values_the_position(
    valuation_cases: _ValuationCases,
) -> None:
    """The price side is normalized upstream; the lot side is stored verbatim.

    ``prep.stg_security_prices`` UPPER()s ``quote_currency`` because
    ``core.fct_security_prices``' grain depends on the normalized value, but a lot's
    ``currency_code`` is stored exactly as the source supplied it — Plaid's
    ``COALESCE(iso_currency_code, unofficial_currency_code)`` passes through
    unnormalized, and ``unofficial_currency_code`` (crypto, non-ISO instruments)
    guarantees no casing at all. The two sides also read *different* provider
    objects — the price comes from the security, the lot from the transaction — so
    they are not guaranteed to agree. A case-sensitive join here reports the
    position ``unpriced`` while the close that values it sits in
    ``core.fct_security_prices``: the system has the price and denies it.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    elapsed = (_db_today(db) - anchor).days
    market_value, gain, _pd, source, _days, status = _holding(
        db, _case_account("casing")
    )
    assert status == _expected_status(elapsed), (
        "a casing difference must not unvalue a priced position"
    )
    assert market_value == Decimal("1200.00")
    assert gain == Decimal("200.00")
    assert source == "plaid"


@pytest.mark.slow
def test_split_reject_withholds_the_value(valuation_cases: _ValuationCases) -> None:
    """Publishing quantity × price here yields a number wrong by the split factor."""
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    case_security = _case_security("split_withhold")
    _assert_withheld_publishes_nothing(_holding(db, _case_account("split_withhold")))
    # A same-day close DID resolve; the NULLs above are the withhold, not an absent
    # price. Without this the test would pass identically against a model that priced
    # nothing at all.
    assert _resolved_close(db, case_security, anchor) == Decimal("120.0000000000")


@pytest.mark.slow
def test_withhold_reaches_a_sibling_position_in_another_account(
    valuation_cases: _ValuationCases,
) -> None:
    """One reject implicates every position in the security, not just its own account.

    A split is a corporate action on the SECURITY, so scoping detection to the
    rejecting account would leave siblings valued at a quantity wrong by the split
    factor. The sibling (acc_2) is the row a per-account implementation would leave
    `valued`, and it is deliberately NOT the account carrying the reject.
    """
    db = valuation_cases.db
    rows = {
        account_id: (status, market_value)
        for account_id, status, market_value in db.execute(
            """
            SELECT account_id, valuation_status, market_value
            FROM core.dim_holdings
            WHERE security_id = ? AND account_id IN (?, ?)
            """,
            [
                _case_security("sibling"),
                _case_account("sibling_a"),
                _case_account("sibling_b"),
            ],
        ).fetchall()
    }
    assert len(rows) == 2, f"both positions must reach dim_holdings: {rows}"
    assert rows[_case_account("sibling_a")] == ("withheld", None)
    assert rows[_case_account("sibling_b")] == ("withheld", None), (
        "the sibling position is implicated too"
    )


@pytest.mark.slow
def test_position_that_recorded_the_split_still_values(
    valuation_cases: _ValuationCases,
) -> None:
    """Resolved per position: a ledger carrying the split on that date is restated.

    The manual split is a 4:1 multiplier applied to the 10-unit position, so the
    restated quantity is 40 and the published value is 40 × 120.00. Asserting the
    number — not merely `status != 'withheld'` — is what proves the model published a
    figure rather than merely declining to withhold one.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(
        db, _case_account("split_recorded")
    )
    assert status == _expected_status(elapsed), (
        "the split reached this position's ledger; withholding would suppress a right answer"
    )
    assert market_value == Decimal("4800.00"), "40 restated units × the 120.00 close"


@pytest.mark.slow
def test_split_recorded_on_the_ex_date_clears_a_settlement_dated_reject(
    valuation_cases: _ValuationCases,
) -> None:
    """The two suppliers date one corporate action differently; the match is windowed.

    Plaid routes the split to review dated 2026-06-01 — whatever its feed reported,
    commonly the settlement date. The user reconciles it by hand on the ex-date,
    2026-05-31, one day earlier. The quantity is now correct, so withholding would be
    wrong; and because the design carries no resolved-flag, an exact-date match would
    make that withhold PERMANENT rather than merely late — no later event can ever
    clear it.

    The offset is deliberately 1 day rather than 0: at 0 this test passes against the
    exact-equality predicate it exists to reject, and would discriminate nothing.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(
        db, _case_account("split_exdate")
    )
    assert status == _expected_status(elapsed), (
        "the ledger carries the split one day off the reject's date; the quantity is "
        "restated and withholding it would be permanent"
    )
    assert market_value == Decimal("4800.00"), "40 restated units × the 120.00 close"


@pytest.mark.slow
def test_pre_split_price_falls_back_to_unpriced(
    valuation_cases: _ValuationCases,
) -> None:
    """A recorded split newer than the only close makes that close unusable.

    The ledger carries a 4:1 split dated 2026-06-01, restating the position to 40 post-
    split units. The only available close is dated 2026-05-01 — PRE-split. Multiplying
    the post-split quantity by a pre-split price would overstate market_value by the
    split factor and publish it as carried_forward ("a bit old") rather than wrong-by-4x,
    so the price is dropped and the position falls back to unpriced until a post-split
    close lands. Distinct from the split-reject withhold: here the split IS recorded
    (quantity restated), so nothing withholds — it is the PRICE, not the share count,
    that is stale.
    """
    db = valuation_cases.db
    market_value, _gain, price_date, source, days, status = _holding(
        db, _case_account("split_pre_price")
    )
    assert status == "unpriced", "the only close predates the recorded split"
    assert market_value is None, (
        "a pre-split price must not value a post-split quantity"
    )
    assert price_date is None
    assert source is None
    assert days is None


@pytest.mark.slow
def test_post_split_price_values_the_position(valuation_cases: _ValuationCases) -> None:
    """A close dated after the recorded split values the restated quantity normally.

    Adversarial partner to test_pre_split_price_falls_back_to_unpriced: identical 4:1
    split dated 2026-06-01, but the close is dated 2026-06-15 — AFTER the split. The
    price is usable, so the position values at 40 restated units × the close. Proves the
    split-staleness exclusion does not over-withhold a valid post-split price; the only
    difference from the pre-split case is which side of the split the close falls on.
    """
    db = valuation_cases.db
    market_value, _gain, _pd, _src, _days, status = _holding(
        db, _case_account("split_post_price")
    )
    assert status != "unpriced", "a post-split close is usable"
    assert market_value == Decimal("4800.00"), "40 restated units × the 120.00 close"


@pytest.mark.slow
def test_incomplete_basis_nulls_unrealized_gain_but_keeps_market_value(
    valuation_cases: _ValuationCases,
) -> None:
    """A transfer_in with unknown basis publishes market_value but not an overstated gain.

    An ACATS-style transfer_in with no supplied basis opens a lot the engine flags
    basis_incomplete, storing a 0.00 cost that is not a real zero. market_value
    (quantity × close) is unaffected and stays published, but unrealized_gain =
    market_value - cost_basis would overstate the gain by the entire missing basis, so it
    is nulled. The complete-basis positions elsewhere in this module (e.g.
    test_same_day_price_values_the_position) are the adversarial partner: their gain is
    published because their basis is real.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    elapsed = (_db_today(db) - anchor).days
    market_value, gain, _pd, _src, _days, status = _holding(
        db, _case_account("incomplete_basis")
    )
    assert market_value == Decimal("1200.00"), (
        "10 units × 120.00 — the value is knowable"
    )
    assert gain is None, "cost basis is incomplete; a computed gain would be overstated"
    assert status == _expected_status(elapsed), (
        "the position is priced, just gain-blind"
    )


@pytest.mark.slow
def test_quantity_divergence_withholds(valuation_cases: _ValuationCases) -> None:
    """The broker's newest snapshot contradicts the ledger's own share count.

    The 40-unit claim also bootstraps a 40-unit pre-window opening lot (the account's
    only prior activity is manual, so the whole claim reads as a gap), leaving the
    ledger at 50 against a claim of 40. The exact figures are incidental; what the
    test pins is that the two disagree and no value is published.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    case_account = _case_account("divergence")
    case_security = _case_security("divergence")
    _assert_withheld_publishes_nothing(_holding(db, case_account))
    assert _resolved_close(db, case_security, anchor) == Decimal("120.0000000000"), (
        "a close resolved; the NULLs above are the withhold"
    )

    quantity, claimed = _account_quantities(db, case_account)
    assert claimed == Decimal("40.0000000000"), "the provider claim must have joined"
    assert quantity != claimed, "the withhold must rest on a real disagreement"


@pytest.mark.slow
def test_phantom_position_withholds(valuation_cases: _ValuationCases) -> None:
    """The broker reported this position, then a newer snapshot dropped it.

    The position is broker-derived: the first snapshot's holdings seed a 10-unit opening
    lot (sync-plaid-investments.md § Opening-lot bootstrap), so the ledger carries shares
    the broker once reported. A newer snapshot then omits VTI, so
    provider_reported_quantity is NULL and clause 1 cannot catch it —
    `quantity <> provider_reported_quantity` is UNKNOWN rather than true, so the position
    would slip through and publish a market value for shares the broker says are gone. The
    PRIOR snapshot that carried VTI is what makes this a genuine phantom rather than a
    manual holding (contrast test_manual_position_in_covered_account_still_values, whose
    only difference is that VTI was never reported).
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    case_account = _case_account("phantom")
    case_security = _case_security("phantom")
    _assert_withheld_publishes_nothing(_holding(db, case_account))
    assert _resolved_close(db, case_security, anchor) == Decimal("120.0000000000"), (
        "a close resolved; the NULLs above are the withhold"
    )

    quantity, claimed = _account_quantities(db, case_account)
    assert quantity == Decimal("10.0000000000"), (
        "the ledger carries the broker's opening lot the newest snapshot now omits"
    )
    assert claimed is None, (
        "the newest snapshot omits this position — that NULL is the signal"
    )


@pytest.mark.slow
def test_manual_position_in_covered_account_still_values(
    valuation_cases: _ValuationCases,
) -> None:
    """A hand-tracked position in a broker-linked account values — the broker never had it.

    The account is broker-covered (a snapshot reports a different, unbound security) and
    VTI is absent from that snapshot exactly as in the phantom case above — but here the
    broker has NEVER reported VTI in any snapshot, so it is a manual holding, not a
    phantom, and withholding it would falsely claim the share count is wrong. This is the
    adversarial partner to test_phantom_position_withholds: identical coverage and an
    identical missing claim, the ONLY difference being the prior VTI snapshot that case
    has and this one does not — so it isolates the ever_reported_positions gate.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(
        db, _case_account("manual_covered")
    )
    assert status == _expected_status(elapsed), (
        "the broker never reported this position — it is manual, not a phantom"
    )
    assert market_value == Decimal("1200.00")


@pytest.mark.slow
def test_liquidated_position_absent_from_newest_snapshot_withholds(
    valuation_cases: _ValuationCases,
) -> None:
    """A genuine liquidation: the broker reported VTI, then a pull reports nothing.

    The position is broker-derived: the first snapshot's holdings seed a 10-unit opening
    lot. The pull that liquidates the item then writes a receipt with
    ``holdings_count = 0`` and no holdings rows at all. newest_snapshot must pick that
    EMPTY receipt (it reads receipts, not rows) so the provider claim reads NULL; a
    row-derived newest snapshot would miss the liquidating pull, keep the prior non-empty
    one, and value shares the broker says are gone. The prior snapshot that reported VTI
    is what makes this a phantom rather than a manual holding.

    Ledger quantity is 10 against a close of 120.00, so a regression publishes $1,200.00
    of shares the broker no longer reports.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    case_account = _case_account("liquidated")
    case_security = _case_security("liquidated")
    _assert_withheld_publishes_nothing(_holding(db, case_account))
    assert _resolved_close(db, case_security, anchor) == Decimal("120.0000000000"), (
        "a close resolved; the NULLs above are the withhold"
    )

    quantity, claimed = _account_quantities(db, case_account)
    assert quantity == Decimal("10.0000000000"), (
        "the ledger still carries the position the broker no longer reports"
    )
    assert claimed is None, (
        "the liquidated snapshot reports nothing — that NULL is the signal"
    )


@pytest.mark.slow
def test_manual_account_without_a_snapshot_still_values(
    valuation_cases: _ValuationCases,
) -> None:
    """A manual-only position stays valued: no broker snapshot, nothing to diverge from.

    Divergence detection is inert without a snapshot, and the phantom clause must not
    read a missing claim as an omitted position — dropping the ever_reported_positions
    gate silently unvalues every manually-tracked position in the database.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(
        db, _case_account("manual_only")
    )
    assert status == _expected_status(elapsed)
    assert market_value == Decimal("1200.00")


@pytest.mark.slow
def test_broker_position_matching_the_snapshot_claim_values(
    valuation_cases: _ValuationCases,
) -> None:
    """A broker position whose quantity AGREES with the newest snapshot stays valued.

    The most common real case — a correctly-reconciled brokerage position — and the
    adversarial partner to test_quantity_divergence_withholds: identical broker coverage
    and a snapshot for the SAME bound security, the only difference being that the claim
    AGREES with the ledger. The single snapshot both bootstraps the 10-unit opening lot
    (sync-plaid-investments.md § Opening-lot bootstrap) and is the newest claim (also 10),
    so provider_reported_quantity == quantity and clause 1's
    `quantity <> provider_reported_quantity` is false. A model that withheld on the mere
    PRESENCE of a provider claim, or inverted that comparison, passes every other test in
    this module and fails only here.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    case_account = _case_account("broker_matching")
    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(db, case_account)
    assert status == _expected_status(elapsed), (
        "a broker position whose claim matches the ledger must value, not withhold"
    )
    assert market_value == Decimal("1200.00"), "10 units × the 120.00 close"

    quantity, claimed = _account_quantities(db, case_account)
    assert quantity == claimed == Decimal("10.0000000000"), (
        "the withhold must NOT rest here — ledger and claim agree"
    )


@pytest.mark.slow
def test_position_opened_after_a_reject_split_is_not_withheld(
    valuation_cases: _ValuationCases,
) -> None:
    """A split reject withholds only positions HELD ACROSS the split, not later ones.

    A corporate action can only misstate a quantity that existed at the split. This
    position's single lot opens 2026-06-15, AFTER the 2026-06-01 reject, so its quantity is
    correct from inception and it carries no split event of its own. Scoping the withhold
    by security_id alone (the pre-fix behavior) would withhold it FOREVER — no later event
    can restate a split it never experienced. The adversarial partner is
    test_split_reject_withholds_the_value, whose only difference is a lot opened
    2026-01-05, BEFORE the same reject.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(
        db, _case_account("post_reject")
    )
    assert status == _expected_status(elapsed), (
        "a position opened after the split was never exposed to it — withholding it would "
        "be permanent, never clearing"
    )
    assert market_value == Decimal("1200.00"), "10 units × the 120.00 close"


@pytest.mark.slow
def test_updated_at_reflects_the_resolved_close_freshness(
    valuation_cases: _ValuationCases,
) -> None:
    """A newer close changing market_value must advance the row's updated_at watermark.

    market_value is quantity × the resolved close, so the close's freshness is a real
    input to this row. Pre-fix, updated_at was MAX over the open lots only, so a new close
    could change market_value while updated_at stayed pinned to an old trade timestamp —
    breaking the documented core.*.updated_at incremental-freshness contract. The close is
    stamped with a far-future extracted_at that no lot timestamp can reach, so a folded
    watermark must surface it and an unfolded one (the pre-fix behavior) cannot.
    """
    db = valuation_cases.db
    row = db.execute(
        "SELECT updated_at FROM core.dim_holdings WHERE account_id = ?",
        [_case_account("updated_close")],
    ).fetchone()
    assert row is not None
    assert row[0] == datetime(2099, 1, 1), (
        "the resolved close's freshness must fold into the row watermark"
    )


@pytest.mark.slow
def test_updated_at_reflects_an_omitting_snapshot(
    valuation_cases: _ValuationCases,
) -> None:
    """A pull that DROPS a reported position advances its watermark to that pull's time.

    When a newer snapshot omits a previously reported position it flips to `withheld` — a
    real input change — but the per-position provider claim is NULL (the omitted position
    has no holdings row in that pull), so the watermark must come from the snapshot RECEIPT.
    The omitting pull is stamped far in the future; a fold that read only the per-position
    claim, or the open lots, would miss it and pin updated_at to the old lot time — the
    asymmetry the account-blind per-position claim leaves. Adversarial partner to
    test_updated_at_reflects_the_resolved_close_freshness (a manual position, no snapshot).
    """
    db = valuation_cases.db
    row = db.execute(
        "SELECT valuation_status, updated_at FROM core.dim_holdings "
        "WHERE account_id = ?",
        [_case_account("updated_omitting")],
    ).fetchone()
    assert row is not None
    assert row[0] == "withheld", (
        "the newest snapshot omits the position — it is a phantom"
    )
    assert row[1] == datetime(2099, 1, 1), (
        "the omitting pull's receipt freshness must fold into the row watermark"
    )


@pytest.mark.slow
def test_an_unknown_currency_lot_withholds_the_value_too(
    valuation_cases: _ValuationCases,
) -> None:
    """A lot with no currency beside a EUR lot is a mixed unit the count cannot see.

    ``COUNT(DISTINCT currency_code)`` ignores NULL, so a NULL lot plus a EUR lot counts
    as one currency and slips past the ``currency_count > 1`` guard — the position then
    values the combined quantity at the EUR close. The guard exists for lots that do not
    agree on a unit, and an unknown unit does not agree with a known one. A EUR close DID
    resolve, so a model missing this half of the guard publishes a figure; every
    single-currency position in this module is the adversarial partner that must keep
    valuing normally.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    case_security = _case_security("unknown_currency_mix")
    _assert_withheld_publishes_nothing(
        _holding(db, _case_account("unknown_currency_mix"))
    )
    assert _resolved_close(db, case_security, anchor) == Decimal("120.0000000000"), (
        "a close resolved; the NULLs above are the currency withhold, not an absent price"
    )


@pytest.mark.slow
def test_mixed_currency_lots_withhold_the_value(
    valuation_cases: _ValuationCases,
) -> None:
    """Open lots in two currencies have no single close to value the combined quantity.

    The manual event API takes --currency per event, so one (account, security) position
    can carry a USD lot and a EUR lot. quantity × price would multiply the summed 15 units
    by whichever currency MAX(currency_code) happens to pick — a mixed-unit product, not a
    stale price. The value is withheld until the lots agree. A USD close DID resolve, so a
    model missing the currency guard would publish a figure; the adversarial partner is
    every single-currency position in this module, which values normally.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    case_security = _case_security("mixed_currency")
    _assert_withheld_publishes_nothing(_holding(db, _case_account("mixed_currency")))
    assert _resolved_close(db, case_security, anchor) == Decimal("120.0000000000"), (
        "a close resolved; the NULLs above are the currency withhold, not an absent price"
    )


@pytest.mark.slow
def test_two_source_ledgers_withhold_every_figure(
    valuation_cases: _ValuationCases,
) -> None:
    """A position whose account carries two source ledgers publishes nothing.

    The double-count reaches quantity, cost basis, market value and gain alike,
    so the row makes no valuation claim at all — and says ``source_overlap``
    rather than ``withheld``, because the remedy is to remove one of the two
    feeds, not to reconcile a share count.
    """
    db = valuation_cases.db
    market_value, gain, price_date, source, days, status = _holding(
        db, _case_account("source_overlap")
    )

    assert status == "source_overlap"
    assert market_value is None
    assert gain is None
    assert price_date is None, "a held-back row must not advertise a price date"
    assert source is None, "a held-back row must not advertise a price source"
    assert days is None, "a held-back row must not advertise price freshness"
    # Proves the blank is the withhold and not an absent price: the close is
    # sitting in core.fct_security_prices, resolved, one model over.
    assert (
        _resolved_close(db, _case_security("source_overlap"), valuation_cases.anchor)
        is not None
    )


@pytest.mark.slow
def test_source_overlap_outranks_a_quantity_divergence(
    valuation_cases: _ValuationCases,
) -> None:
    """When both hold, the account-level fault is the one reported.

    A quantity that disagrees with the broker is a *symptom* here — the ledger
    counted every buy twice, so of course the count is wrong. Reporting
    ``withheld`` would send the user to reconcile a share count that reconciling
    cannot fix.
    """
    db = valuation_cases.db
    *_, status = _holding(db, _case_account("overlap_beats_withheld"))

    assert status == "source_overlap"


@pytest.mark.slow
def test_a_single_source_ledger_still_values(
    valuation_cases: _ValuationCases,
) -> None:
    """One source is one ledger, whichever source it is.

    The withhold keys on a SECOND source, not on the presence of a connector: a
    Plaid-only account has nothing to interleave with and values like any other.
    """
    db = valuation_cases.db
    anchor = valuation_cases.anchor
    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _source, _days, status = _holding(
        db, _case_account("plaid_only")
    )

    assert status == _expected_status(elapsed)
    assert market_value == Decimal("1200.00")


@pytest.mark.slow
def test_an_opening_bootstrap_is_not_a_second_source_ledger(
    valuation_cases: _ValuationCases,
) -> None:
    """A reconstructed pre-window lot must not read as a second ledger.

    The opening-lot bootstrap synthesizes a plaid-sourced ``transfer_in`` from the
    broker's first snapshot precisely BECAUSE no transaction covers that position
    — it fills the gap the in-window transactions leave, so it re-reports nothing
    and double-counts nothing. Counting its ``source_type`` would withhold every
    broker-covered account that also holds a manual entry.

    It would also put this model at odds with the check that reports the state:
    ``investment_source_overlap`` joins the two raw TRANSACTION tables, so a
    holdings snapshot alone is not an overlap there. A user would be left holding
    a withheld portfolio with a passing doctor and no remedy named anywhere.
    """
    db = valuation_cases.db
    account_id = _case_account("bootstrap_only")

    rows = db.execute(
        """
        SELECT COUNT(DISTINCT source_type),
               COUNT(*) FILTER (WHERE subtype = 'opening_bootstrap')
        FROM core.fct_investment_transactions
        WHERE account_id = ?
        """,
        [account_id],
    ).fetchall()
    distinct_sources, bootstrap_rows = rows[0]
    # The precondition the test would silently pass without: the ledger really
    # does carry two source_types, and the second one is only the bootstrap.
    assert distinct_sources == 2, (
        "fixture no longer reproduces the two-source ledger this guards"
    )
    assert bootstrap_rows >= 1, "no opening-bootstrap row was synthesized"

    *_, status = _holding(db, account_id)
    assert status != "source_overlap"


@pytest.mark.slow
def test_an_overlap_flip_advances_the_row_watermark(
    valuation_cases: _ValuationCases,
) -> None:
    """A status change caused by a sibling position still moves updated_at.

    The overlap flag is ACCOUNT-scoped, so a second-source event recorded
    against security B flips security A from a valued row to ``source_overlap``
    while none of A's own lots, price, or snapshot timestamps move. A watermark
    folding only position-scoped inputs would report A as unchanged, and an
    incremental consumer querying by the documented row watermark — the
    contract ``core-updated-at-convention.md`` sets and this model's own column
    comment repeats — would keep serving the pre-flip figure.

    The held security here has no price and no broker snapshot, so the only
    other candidate timestamps are its own lot's and the account-level overlap
    input's. The equality below can therefore only hold if the overlap input is
    folded in.
    """
    db = valuation_cases.db
    account_id = _case_account("overlap_watermark")
    held = _case_security("overlap_watermark")

    row = db.execute(
        """
        SELECT updated_at, valuation_status
        FROM core.dim_holdings
        WHERE account_id = ? AND security_id = ?
        """,
        [account_id, held],
    ).fetchone()
    assert row is not None, "the held position produced no dim_holdings row"
    updated_at, status = row

    lot_row = db.execute(
        """
        SELECT MAX(updated_at)
        FROM core.fct_investment_lots
        WHERE account_id = ? AND security_id = ?
        """,
        [account_id, held],
    ).fetchone()
    assert lot_row is not None

    # Preconditions: the scenario really is "the row's own inputs did not move".
    assert status == "source_overlap", "the sibling event did not flip this row"
    assert lot_row[0] == datetime.fromisoformat(_OVERLAP_HELD_LOT_AT), (
        "fixture no longer pins the held position's own freshness"
    )
    assert datetime.fromisoformat(_OVERLAP_HELD_LOT_AT) < datetime.fromisoformat(
        _OVERLAP_SECOND_SOURCE_AT
    ), "the second-source event must be the newer input for this to prove anything"

    assert updated_at == datetime.fromisoformat(_OVERLAP_SECOND_SOURCE_AT)


@pytest.mark.slow
def test_a_single_source_row_keeps_its_own_watermark(
    valuation_cases: _ValuationCases,
) -> None:
    """The overlap term contributes only where the overlap exists.

    ``source_overlap_accounts`` holds only overlapping accounts, so a
    single-source position LEFT JOINs to nothing and falls back to its own
    inputs. Without that scoping every row in the database would inherit
    whatever the freshest overlapping ledger row happened to be — a watermark
    that advances on an unrelated account's fault is as useless as one that
    misses its own.

    This position has no price and no snapshot, so its lot's pinned timestamp
    is the whole answer.
    """
    db = valuation_cases.db

    row = db.execute(
        "SELECT updated_at FROM core.dim_holdings WHERE account_id = ?",
        [_case_account("clean_watermark")],
    ).fetchone()
    assert row is not None
    assert row[0] == datetime.fromisoformat(_OVERLAP_HELD_LOT_AT)


@pytest.mark.slow
def test_both_overlap_implementations_agree_on_the_same_ledger(
    valuation_cases: _ValuationCases,
) -> None:
    """One overlap predicate, two implementations, the same verdict.

    ``core.dim_holdings`` decides the withhold in SQL. ``InvestmentService``
    re-derives the same account set in Python for the ``lots`` and ``gains``
    reads, because those two models carry no status column of their own — a
    row-level trust marker across the three derived investment models is a
    separate design decision. Two implementations of one predicate drift, and
    the load-bearing half is the exclusion nobody would guess: an
    ``opening_bootstrap`` row is MoneyBin's own reconstruction of a pre-window
    position, not a second observation, and must not count on either side.

    Compared over the accounts ``dim_holdings`` can speak for. Its grain is
    OPEN positions, so the Python set is legitimately the wider of the two —
    that is why the gains surface reads the ledger rather than this view.

    Reaches for the private helper deliberately: the claim under test is that
    two *implementations* agree, and the fixture accounts carry no
    ``core.dim_accounts`` rows, so the public reads cannot resolve them.
    """
    db = valuation_cases.db

    holdings_rows = db.execute(
        "SELECT account_id, valuation_status FROM core.dim_holdings"
    ).fetchall()
    sql_side = {str(r[0]) for r in holdings_rows if r[1] == "source_overlap"}
    open_accounts = {str(r[0]) for r in holdings_rows}
    python_side = InvestmentService(db)._source_overlap_accounts()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]  # the claim under test is that two implementations agree

    # Preconditions: both sides really evaluated something, and the fixture
    # holds accounts of each verdict — a green comparison of two empty sets
    # would prove nothing.
    assert _case_account("source_overlap") in sql_side
    assert _case_account("bootstrap_only") in open_accounts
    assert _case_account("plaid_only") in open_accounts

    assert open_accounts & python_side == sql_side
