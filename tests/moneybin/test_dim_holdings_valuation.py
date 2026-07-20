"""core.dim_holdings valuation: market value, staleness, and honest NULLs."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context

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


def _seed_position(
    db: Database,
    *,
    security_id: str = _DEFAULT_SECURITY_ID,
    currency_code: str = "USD",
) -> None:
    """10 units at 100.00, cost basis 1000.00, in account acc_1."""
    db.execute(
        """
        INSERT INTO app.securities (security_id, name, security_type, ticker)
        VALUES (?, 'Vanguard Total Stock Market ETF', 'etf', 'VTI')
        """,  # noqa: S608  # test fixture, not executing user SQL
        [security_id],
    )
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees, created_by,
            investment_transaction_id, currency_code
        ) VALUES ('buy_1', 'imp_1', 'acc_1', ?, 'VTI', 'buy',
                  DATE '2026-01-05', 10, 100.00, -1000.00, 0.00, 'test', 'buy_1', ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [security_id, currency_code],
    )


def _seed_price(
    db: Database,
    *,
    price_date: date,
    close: str,
    security_id: str = _DEFAULT_SECURITY_ID,
    quote_currency: str = "USD",
) -> None:
    """One raw close plus the accepted binding that resolves it to ``security_id``.

    Both the provider key and the link id are derived from ``security_id`` (see
    ``_provider_key``), so seeding two securities produces two distinct, both-accepted
    bindings rather than one accepted and one silently swallowed by ON CONFLICT.
    """
    provider_key = _provider_key(security_id)
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES (?, ?, ?, 'plaid', 'item_1', ?, 'raw',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [provider_key, price_date, quote_currency, close],
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
        ) VALUES ('item_1', ?, CURRENT_DATE, 1, DATE '2026-01-01', 'plaid',
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT DO NOTHING
        """,  # noqa: S608  # test fixture, not executing user SQL
        [source_file],
    )
    db.execute(
        """
        INSERT INTO raw.plaid_investment_holdings (
            account_id, security_id, holdings_date, institution_price,
            institution_price_as_of, institution_value, cost_basis, quantity,
            iso_currency_code, transactions_window_start, source_file,
            source_type, source_origin, extracted_at, loaded_at
        ) VALUES (?, ?, CURRENT_DATE, 120.00, CURRENT_DATE, NULL, NULL, ?,
                  'USD', DATE '2026-01-01', ?, 'plaid', 'item_1',
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [account_id, security_id, quantity, source_file],
    )


def _seed_liquidated_snapshot(db: Database) -> None:
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
        ) VALUES ('item_1', 'sync_job_liquidated', CURRENT_DATE, 0,
                  DATE '2026-01-01', 'plaid', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
    )


def _seed_plaid_buy(db: Database, *, account_id: str) -> None:
    """One ordinary Plaid buy: the row that keeps a liquidated account known to its item.

    A buy, NOT a split: ``split_underivable`` would trip the split clause too, and a
    fixture that satisfies two guards isolates neither — the test would stay green with
    the phantom clause removed entirely.
    """
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transactions (
            investment_transaction_id, account_id, security_id,
            investment_transaction_type, investment_transaction_subtype,
            transaction_date, quantity, price, amount, fees, iso_currency_code,
            source_file, source_type, source_origin, extracted_at, loaded_at
        ) VALUES (?, ?, ?, 'buy', 'buy', DATE '2026-01-10',
                  1, 100.00, -100.00, 0.00, 'USD', 'sync_job_liquidated', 'plaid',
                  'item_1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [f"itx_buy_{account_id}", account_id, _DEFAULT_PROVIDER_KEY],
    )


def _seed_split_reject(
    db: Database, *, account_id: str, trade_date: date = date(2026, 6, 1)
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
                  4, NULL, 0.00, NULL, 'USD', 'sync_test', 'plaid', 'item_1',
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [
            f"itx_split_{account_id}",
            account_id,
            _DEFAULT_PROVIDER_KEY,
            trade_date,
        ],
    )


def _holding(db: Database) -> tuple[object, ...]:
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
        WHERE account_id = 'acc_1'
        """
    ).fetchall()
    assert len(rows) == 1, (
        f"expected exactly one dim_holdings row for acc_1 (grain violation): {rows}"
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


def _resolved_close(db: Database, price_date: date) -> object:
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
        [_DEFAULT_SECURITY_ID, price_date],
    ).fetchall()
    assert len(rows) == 1, f"expected exactly one resolved close: {rows}"
    return rows[0][0]


def _acc_1_quantities(db: Database) -> tuple[object, object]:
    """(ledger quantity, provider claim) for acc_1 — the pair a withhold rests on."""
    rows = db.execute(
        """
        SELECT quantity, provider_reported_quantity
        FROM core.dim_holdings
        WHERE account_id = 'acc_1'
        """
    ).fetchall()
    assert len(rows) == 1, f"expected exactly one dim_holdings row for acc_1: {rows}"
    return rows[0][0], rows[0][1]


@pytest.mark.slow
def test_same_day_price_values_the_position(db: Database) -> None:
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(db, price_date=anchor, close="120.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    elapsed = (_db_today(db) - anchor).days
    market_value, gain, _pd, source, days, status = _holding(db)
    assert market_value == Decimal("1200.00")
    assert gain == Decimal("200.00"), "market value less cost basis"
    assert source == "plaid"
    assert days == elapsed
    assert status == _expected_status(elapsed)


@pytest.mark.slow
def test_older_price_carries_forward_with_rising_staleness(db: Database) -> None:
    """Markets close ~114 days a year; as-of resolution is what makes a series possible."""
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(db, price_date=anchor - timedelta(days=3), close="120.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _source, days, status = _holding(db)
    assert market_value == Decimal("1200.00")
    assert days == elapsed + 3
    assert status == "carried_forward"


@pytest.mark.slow
def test_most_recent_of_two_past_prices_wins(db: Database) -> None:
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
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(db, price_date=anchor - timedelta(days=10), close="50.00")
    _seed_price(db, price_date=anchor - timedelta(days=2), close="120.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, price_date, _source, days, status = _holding(db)
    assert market_value == Decimal("1200.00"), "the newer close (120.00) must win"
    assert price_date == anchor - timedelta(days=2)
    assert days == elapsed + 2
    assert status == "carried_forward"


@pytest.mark.slow
def test_future_price_never_values_an_earlier_date(db: Database) -> None:
    _seed_position(db)
    # +5 days, so the row stays in the future even if the plan crosses midnight.
    _seed_price(db, price_date=_db_today(db) + timedelta(days=5), close="500.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    market_value, _gain, _pd, _source, _days, status = _holding(db)
    assert market_value is None
    assert status == "unpriced"


@pytest.mark.slow
def test_unpriced_holding_is_null_never_zero(db: Database) -> None:
    """Zero is indistinguishable from a worthless position and understates every total."""
    _seed_position(db)

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    market_value, gain, price_date, source, days, status = _holding(db)
    assert market_value is None
    assert gain is None
    assert price_date is None
    assert source is None
    assert days is None
    assert status == "unpriced"


@pytest.mark.slow
def test_price_in_another_currency_does_not_value_the_position(db: Database) -> None:
    """Valuing a USD position at a GBP close would be silently wrong; M1K.2 converts."""
    anchor = _db_today(db)
    _seed_position(db)
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES (?, CURRENT_DATE, 'GBP', 'plaid', 'item_1', 95.00, 'raw',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [_DEFAULT_PROVIDER_KEY],
    )
    _seed_price(db, price_date=anchor - timedelta(days=400), close="1.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    _mv, _gain, price_date, _source, _days, _status = _holding(db)
    assert price_date == anchor - timedelta(days=400), (
        "the GBP close must not win over an older USD one"
    )


@pytest.mark.slow
def test_currency_casing_mismatch_still_values_the_position(db: Database) -> None:
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
    anchor = _db_today(db)
    _seed_position(db, currency_code="usd")
    _seed_price(db, price_date=anchor, close="120.00", quote_currency="usd")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    elapsed = (_db_today(db) - anchor).days
    market_value, gain, _pd, source, _days, status = _holding(db)
    assert status == _expected_status(elapsed), (
        "a casing difference must not unvalue a priced position"
    )
    assert market_value == Decimal("1200.00")
    assert gain == Decimal("200.00")
    assert source == "plaid"


@pytest.mark.slow
def test_split_reject_withholds_the_value(db: Database) -> None:
    """Publishing quantity × price here yields a number wrong by the split factor."""
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(db, price_date=anchor, close="120.00")
    _seed_split_reject(db, account_id="acc_1")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    _assert_withheld_publishes_nothing(_holding(db))
    # A same-day close DID resolve; the NULLs above are the withhold, not an absent
    # price. Without this the test would pass identically against a model that priced
    # nothing at all.
    assert _resolved_close(db, anchor) == Decimal("120.0000000000")


@pytest.mark.slow
def test_withhold_reaches_a_sibling_position_in_another_account(db: Database) -> None:
    """One reject implicates every position in the security, not just its own account.

    A split is a corporate action on the SECURITY, so scoping detection to the
    rejecting account would leave siblings valued at a quantity wrong by the split
    factor. The sibling (acc_2) is the row a per-account implementation would leave
    `valued`, and it is deliberately NOT the account carrying the reject.
    """
    _seed_position(db)
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees, created_by,
            investment_transaction_id
        ) VALUES ('buy_2', 'imp_1', 'acc_2', 'canonvti0000001', 'VTI', 'buy',
                  DATE '2026-01-05', 5, 100.00, -500.00, 0.00, 'test', 'buy_2')
        """  # noqa: S608  # test fixture, not executing user SQL
    )
    _seed_price(db, price_date=_db_today(db), close="120.00")
    _seed_split_reject(db, account_id="acc_1")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = {
        account_id: (status, market_value)
        for account_id, status, market_value in db.execute(
            """
            SELECT account_id, valuation_status, market_value
            FROM core.dim_holdings
            """
        ).fetchall()
    }
    assert len(rows) == 2, f"both positions must reach dim_holdings: {rows}"
    assert rows["acc_1"] == ("withheld", None)
    assert rows["acc_2"] == ("withheld", None), "the sibling position is implicated too"


@pytest.mark.slow
def test_position_that_recorded_the_split_still_values(db: Database) -> None:
    """Resolved per position: a ledger carrying the split on that date is restated.

    The manual split is a 4:1 multiplier applied to the 10-unit position, so the
    restated quantity is 40 and the published value is 40 × 120.00. Asserting the
    number — not merely `status != 'withheld'` — is what proves the model published a
    figure rather than merely declining to withhold one.
    """
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(db, price_date=anchor, close="120.00")
    _seed_split_reject(db, account_id="acc_1")
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees, created_by,
            investment_transaction_id
        ) VALUES ('split_1', 'imp_1', 'acc_1', 'canonvti0000001', 'VTI', 'split',
                  DATE '2026-06-01', 4, NULL, NULL, NULL, 'test', 'split_1')
        """  # noqa: S608  # test fixture, not executing user SQL
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(db)
    assert status == _expected_status(elapsed), (
        "the split reached this position's ledger; withholding would suppress a right answer"
    )
    assert market_value == Decimal("4800.00"), "40 restated units × the 120.00 close"


@pytest.mark.slow
def test_split_recorded_on_the_ex_date_clears_a_settlement_dated_reject(
    db: Database,
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
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(db, price_date=anchor, close="120.00")
    _seed_split_reject(db, account_id="acc_1", trade_date=date(2026, 6, 1))
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees, created_by,
            investment_transaction_id
        ) VALUES ('split_exdate', 'imp_1', 'acc_1', 'canonvti0000001', 'VTI', 'split',
                  DATE '2026-05-31', 4, NULL, NULL, NULL, 'test', 'split_exdate')
        """  # noqa: S608  # test fixture, not executing user SQL
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(db)
    assert status == _expected_status(elapsed), (
        "the ledger carries the split one day off the reject's date; the quantity is "
        "restated and withholding it would be permanent"
    )
    assert market_value == Decimal("4800.00"), "40 restated units × the 120.00 close"


@pytest.mark.slow
def test_quantity_divergence_withholds(db: Database) -> None:
    """The broker's newest snapshot contradicts the ledger's own share count.

    The 40-unit claim also bootstraps a 40-unit pre-window opening lot (the account's
    only prior activity is manual, so the whole claim reads as a gap), leaving the
    ledger at 50 against a claim of 40. The exact figures are incidental; what the
    test pins is that the two disagree and no value is published.
    """
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(db, price_date=anchor, close="120.00")
    _seed_broker_snapshot(db, account_id="acc_1", quantity="40")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    _assert_withheld_publishes_nothing(_holding(db))
    assert _resolved_close(db, anchor) == Decimal("120.0000000000"), (
        "a close resolved; the NULLs above are the withhold"
    )

    quantity, claimed = _acc_1_quantities(db)
    assert claimed == Decimal("40.0000000000"), "the provider claim must have joined"
    assert quantity != claimed, "the withhold must rest on a real disagreement"


@pytest.mark.slow
def test_phantom_position_withholds(db: Database) -> None:
    """A fresh snapshot omits a position the ledger still carries.

    Clause 1 cannot catch this: provider_reported_quantity is NULL, so
    `quantity <> provider_reported_quantity` is UNKNOWN rather than true and the
    position slips through — publishing a market value for shares the broker says are
    gone. The snapshot covers acc_1 (so the account is broker-covered) but reports a
    different, unbound security, leaving the VTI position absent from the claim.
    """
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(db, price_date=anchor, close="120.00")
    _seed_broker_snapshot(
        db, account_id="acc_1", quantity="7", security_id="sec_unbound"
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    _assert_withheld_publishes_nothing(_holding(db))
    assert _resolved_close(db, anchor) == Decimal("120.0000000000"), (
        "a close resolved; the NULLs above are the withhold"
    )

    _quantity, claimed = _acc_1_quantities(db)
    assert claimed is None, "the snapshot omits this position — that NULL is the signal"


@pytest.mark.slow
def test_liquidated_account_absent_from_holdings_still_withholds(db: Database) -> None:
    """The maximal-harm phantom: the broker reports nothing and the ledger reports 11 units.

    The pull that liquidates an item writes a receipt with ``holdings_count = 0`` and no
    holdings rows at all, so an account whose coverage is derived from HOLDINGS rows
    drops out of broker_covered_accounts entirely — and the 100%-overstated account is
    exactly the one that narrower scope filters out. The account stays known to its item
    through the transactions staging view, which is the union leg that supplies coverage
    here; ``core.dim_holdings`` withholds only because it reads both.

    Ledger quantity is 11 (10 manual + 1 Plaid buy) against a close of 120.00, so a
    regression publishes $1,320.00 of shares the broker says are gone.
    """
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(db, price_date=anchor, close="120.00")
    _seed_liquidated_snapshot(db)
    _seed_plaid_buy(db, account_id="acc_1")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    _assert_withheld_publishes_nothing(_holding(db))
    assert _resolved_close(db, anchor) == Decimal("120.0000000000"), (
        "a close resolved; the NULLs above are the withhold"
    )

    quantity, claimed = _acc_1_quantities(db)
    assert quantity == Decimal("11.0000000000"), (
        "the ledger still carries the position the broker no longer reports"
    )
    assert claimed is None, (
        "the liquidated snapshot reports nothing — that NULL is the signal"
    )


@pytest.mark.slow
def test_manual_account_without_a_snapshot_still_values(db: Database) -> None:
    """A manual-only position stays valued: no broker snapshot, nothing to diverge from.

    Divergence detection is inert without a snapshot, and the phantom clause must not
    read a missing claim as an omitted position — dropping the broker_covered_accounts
    scope silently unvalues every manually-tracked position in the database.
    """
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(db, price_date=anchor, close="120.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(db)
    assert status == _expected_status(elapsed)
    assert market_value == Decimal("1200.00")
