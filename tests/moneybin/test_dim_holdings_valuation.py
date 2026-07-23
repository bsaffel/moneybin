"""core.dim_holdings valuation: market value, staleness, and honest NULLs."""

from __future__ import annotations

from datetime import date, datetime, timedelta
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


def _seed_security(db: Database, *, security_id: str = _DEFAULT_SECURITY_ID) -> None:
    """The canonical catalog row — needed for any position, manual or broker-derived."""
    db.execute(
        """
        INSERT INTO app.securities (security_id, name, security_type, ticker)
        VALUES (?, 'Vanguard Total Stock Market ETF', 'etf', 'VTI')
        """,  # noqa: S608  # test fixture, not executing user SQL
        [security_id],
    )


def _seed_position(
    db: Database,
    *,
    security_id: str = _DEFAULT_SECURITY_ID,
    currency_code: str = "USD",
) -> None:
    """A MANUAL position: 10 units at 100.00, cost basis 1000.00, in account acc_1."""
    _seed_security(db, security_id=security_id)
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
        ) VALUES ('item_1', ?, CURRENT_DATE, 1, DATE '2026-01-01', 'plaid',
                  COALESCE(?::TIMESTAMP, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
        ON CONFLICT DO NOTHING
        """,  # noqa: S608  # test fixture, not executing user SQL
        [source_file, extracted_at],
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
            (provider_security_key, price_date, quote_currency, source_type,
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
def test_pre_split_price_falls_back_to_unpriced(db: Database) -> None:
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
    _seed_position(db)
    _seed_price(db, price_date=date(2026, 5, 1), close="120.00")
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

    market_value, _gain, price_date, source, days, status = _holding(db)
    assert status == "unpriced", "the only close predates the recorded split"
    assert market_value is None, (
        "a pre-split price must not value a post-split quantity"
    )
    assert price_date is None
    assert source is None
    assert days is None


@pytest.mark.slow
def test_post_split_price_values_the_position(db: Database) -> None:
    """A close dated after the recorded split values the restated quantity normally.

    Adversarial partner to test_pre_split_price_falls_back_to_unpriced: identical 4:1
    split dated 2026-06-01, but the close is dated 2026-06-15 — AFTER the split. The
    price is usable, so the position values at 40 restated units × the close. Proves the
    split-staleness exclusion does not over-withhold a valid post-split price; the only
    difference from the pre-split case is which side of the split the close falls on.
    """
    _seed_position(db)
    _seed_price(db, price_date=date(2026, 6, 15), close="120.00")
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

    market_value, _gain, _pd, _src, _days, status = _holding(db)
    assert status != "unpriced", "a post-split close is usable"
    assert market_value == Decimal("4800.00"), "40 restated units × the 120.00 close"


@pytest.mark.slow
def test_incomplete_basis_nulls_unrealized_gain_but_keeps_market_value(
    db: Database,
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
    anchor = _db_today(db)
    _seed_security(db)
    _seed_price(db, price_date=anchor, close="120.00")
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees, created_by,
            investment_transaction_id, currency_code
        ) VALUES ('xfer_1', 'imp_1', 'acc_1', 'canonvti0000001', 'VTI', 'transfer_in',
                  DATE '2026-01-05', 10, NULL, NULL, 0.00, 'test', 'xfer_1', 'USD')
        """  # noqa: S608  # test fixture, not executing user SQL
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    elapsed = (_db_today(db) - anchor).days
    market_value, gain, _pd, _src, _days, status = _holding(db)
    assert market_value == Decimal("1200.00"), (
        "10 units × 120.00 — the value is knowable"
    )
    assert gain is None, "cost basis is incomplete; a computed gain would be overstated"
    assert status == _expected_status(elapsed), (
        "the position is priced, just gain-blind"
    )


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
    anchor = _db_today(db)
    _seed_security(db)
    _seed_price(db, price_date=anchor, close="120.00")
    # First snapshot: the broker reports VTI. This both seeds the opening-lot position AND
    # is the prior evidence that makes the drop below a phantom, not a manual holding.
    _seed_broker_snapshot(
        db, account_id="acc_1", quantity="10", source_file="sync_job_1"
    )
    # Newer snapshot: VTI is gone, replaced by a different unbound security.
    _seed_broker_snapshot(
        db,
        account_id="acc_1",
        quantity="7",
        security_id="sec_unbound",
        source_file="sync_job_2",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    _assert_withheld_publishes_nothing(_holding(db))
    assert _resolved_close(db, anchor) == Decimal("120.0000000000"), (
        "a close resolved; the NULLs above are the withhold"
    )

    quantity, claimed = _acc_1_quantities(db)
    assert quantity == Decimal("10.0000000000"), (
        "the ledger carries the broker's opening lot the newest snapshot now omits"
    )
    assert claimed is None, (
        "the newest snapshot omits this position — that NULL is the signal"
    )


@pytest.mark.slow
def test_manual_position_in_covered_account_still_values(db: Database) -> None:
    """A hand-tracked position in a broker-linked account values — the broker never had it.

    The account is broker-covered (a snapshot reports a different, unbound security) and
    VTI is absent from that snapshot exactly as in the phantom case above — but here the
    broker has NEVER reported VTI in any snapshot, so it is a manual holding, not a
    phantom, and withholding it would falsely claim the share count is wrong. This is the
    adversarial partner to test_phantom_position_withholds: identical coverage and an
    identical missing claim, the ONLY difference being the prior VTI snapshot that case
    has and this one does not — so it isolates the ever_reported_positions gate.
    """
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(db, price_date=anchor, close="120.00")
    _seed_broker_snapshot(
        db, account_id="acc_1", quantity="7", security_id="sec_unbound"
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(db)
    assert status == _expected_status(elapsed), (
        "the broker never reported this position — it is manual, not a phantom"
    )
    assert market_value == Decimal("1200.00")


@pytest.mark.slow
def test_liquidated_position_absent_from_newest_snapshot_withholds(
    db: Database,
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
    anchor = _db_today(db)
    _seed_security(db)
    _seed_price(db, price_date=anchor, close="120.00")
    # First snapshot: the broker reports VTI. Seeds the opening-lot position AND is the
    # prior evidence that makes the liquidation below a phantom, not a manual holding.
    _seed_broker_snapshot(
        db, account_id="acc_1", quantity="10", source_file="sync_job_1"
    )
    # Liquidating pull: a receipt reporting zero holdings and not a single holdings row.
    _seed_liquidated_snapshot(db)

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    _assert_withheld_publishes_nothing(_holding(db))
    assert _resolved_close(db, anchor) == Decimal("120.0000000000"), (
        "a close resolved; the NULLs above are the withhold"
    )

    quantity, claimed = _acc_1_quantities(db)
    assert quantity == Decimal("10.0000000000"), (
        "the ledger still carries the position the broker no longer reports"
    )
    assert claimed is None, (
        "the liquidated snapshot reports nothing — that NULL is the signal"
    )


@pytest.mark.slow
def test_manual_account_without_a_snapshot_still_values(db: Database) -> None:
    """A manual-only position stays valued: no broker snapshot, nothing to diverge from.

    Divergence detection is inert without a snapshot, and the phantom clause must not
    read a missing claim as an omitted position — dropping the ever_reported_positions
    gate silently unvalues every manually-tracked position in the database.
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


@pytest.mark.slow
def test_broker_position_matching_the_snapshot_claim_values(db: Database) -> None:
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
    anchor = _db_today(db)
    _seed_security(db)
    _seed_price(db, price_date=anchor, close="120.00")
    _seed_broker_snapshot(db, account_id="acc_1", quantity="10")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(db)
    assert status == _expected_status(elapsed), (
        "a broker position whose claim matches the ledger must value, not withhold"
    )
    assert market_value == Decimal("1200.00"), "10 units × the 120.00 close"

    quantity, claimed = _acc_1_quantities(db)
    assert quantity == claimed == Decimal("10.0000000000"), (
        "the withhold must NOT rest here — ledger and claim agree"
    )


@pytest.mark.slow
def test_position_opened_after_a_reject_split_is_not_withheld(db: Database) -> None:
    """A split reject withholds only positions HELD ACROSS the split, not later ones.

    A corporate action can only misstate a quantity that existed at the split. This
    position's single lot opens 2026-06-15, AFTER the 2026-06-01 reject, so its quantity is
    correct from inception and it carries no split event of its own. Scoping the withhold
    by security_id alone (the pre-fix behavior) would withhold it FOREVER — no later event
    can restate a split it never experienced. The adversarial partner is
    test_split_reject_withholds_the_value, whose only difference is a lot opened
    2026-01-05, BEFORE the same reject.
    """
    anchor = _db_today(db)
    _seed_security(db)
    _seed_price(db, price_date=anchor, close="120.00")
    _seed_split_reject(db, account_id="acc_1", trade_date=date(2026, 6, 1))
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees, created_by,
            investment_transaction_id
        ) VALUES ('buy_after', 'imp_1', 'acc_1', 'canonvti0000001', 'VTI', 'buy',
                  DATE '2026-06-15', 10, 100.00, -1000.00, 0.00, 'test', 'buy_after')
        """  # noqa: S608  # test fixture, not executing user SQL
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    elapsed = (_db_today(db) - anchor).days
    market_value, _gain, _pd, _src, _days, status = _holding(db)
    assert status == _expected_status(elapsed), (
        "a position opened after the split was never exposed to it — withholding it would "
        "be permanent, never clearing"
    )
    assert market_value == Decimal("1200.00"), "10 units × the 120.00 close"


@pytest.mark.slow
def test_updated_at_reflects_the_resolved_close_freshness(db: Database) -> None:
    """A newer close changing market_value must advance the row's updated_at watermark.

    market_value is quantity × the resolved close, so the close's freshness is a real
    input to this row. Pre-fix, updated_at was MAX over the open lots only, so a new close
    could change market_value while updated_at stayed pinned to an old trade timestamp —
    breaking the documented core.*.updated_at incremental-freshness contract. The close is
    stamped with a far-future extracted_at that no lot timestamp can reach, so a folded
    watermark must surface it and an unfolded one (the pre-fix behavior) cannot.
    """
    anchor = _db_today(db)
    _seed_position(db)
    _seed_price(
        db, price_date=anchor, close="120.00", extracted_at="2099-01-01 00:00:00"
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT updated_at FROM core.dim_holdings WHERE account_id = 'acc_1'"
    ).fetchone()
    assert row is not None
    assert row[0] == datetime(2099, 1, 1), (
        "the resolved close's freshness must fold into the row watermark"
    )


@pytest.mark.slow
def test_updated_at_reflects_an_omitting_snapshot(db: Database) -> None:
    """A pull that DROPS a reported position advances its watermark to that pull's time.

    When a newer snapshot omits a previously reported position it flips to `withheld` — a
    real input change — but the per-position provider claim is NULL (the omitted position
    has no holdings row in that pull), so the watermark must come from the snapshot RECEIPT.
    The omitting pull is stamped far in the future; a fold that read only the per-position
    claim, or the open lots, would miss it and pin updated_at to the old lot time — the
    asymmetry the account-blind per-position claim leaves. Adversarial partner to
    test_updated_at_reflects_the_resolved_close_freshness (a manual position, no snapshot).
    """
    _seed_security(db)
    # The price binding resolves the provider key to canonvti0000001; without it the
    # broker holding never binds and no opening lot (hence no position) is created. Its
    # own freshness is ~now, well below the 2099 receipt below, so the snapshot term is
    # what the assertion isolates.
    _seed_price(db, price_date=date(2026, 7, 15), close="120.00")
    # First snapshot reports VTI: seeds the opening lot and is the prior evidence that
    # makes the omission below a phantom rather than a manual holding. Stamped ~now.
    _seed_broker_snapshot(
        db, account_id="acc_1", quantity="10", source_file="sync_job_1"
    )
    # Newer snapshot omits VTI (a different unbound security), stamped far in the future so
    # it is unambiguously the newest pull and no lot timestamp can reach it.
    _seed_broker_snapshot(
        db,
        account_id="acc_1",
        quantity="7",
        security_id="sec_unbound",
        source_file="sync_job_2",
        extracted_at="2099-01-01 00:00:00",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT valuation_status, updated_at FROM core.dim_holdings "
        "WHERE account_id = 'acc_1'"
    ).fetchone()
    assert row is not None
    assert row[0] == "withheld", (
        "the newest snapshot omits the position — it is a phantom"
    )
    assert row[1] == datetime(2099, 1, 1), (
        "the omitting pull's receipt freshness must fold into the row watermark"
    )


@pytest.mark.slow
def test_mixed_currency_lots_withhold_the_value(db: Database) -> None:
    """Open lots in two currencies have no single close to value the combined quantity.

    The manual event API takes --currency per event, so one (account, security) position
    can carry a USD lot and a EUR lot. quantity × price would multiply the summed 15 units
    by whichever currency MAX(currency_code) happens to pick — a mixed-unit product, not a
    stale price. The value is withheld until the lots agree. A USD close DID resolve, so a
    model missing the currency guard would publish a figure; the adversarial partner is
    every single-currency position in this module, which values normally.
    """
    anchor = _db_today(db)
    _seed_position(db, currency_code="USD")
    _seed_price(db, price_date=anchor, close="120.00")
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees, created_by,
            investment_transaction_id, currency_code
        ) VALUES ('buy_eur', 'imp_1', 'acc_1', 'canonvti0000001', 'VTI', 'buy',
                  DATE '2026-01-06', 5, 90.00, -450.00, 0.00, 'test', 'buy_eur', 'EUR')
        """  # noqa: S608  # test fixture, not executing user SQL
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    _assert_withheld_publishes_nothing(_holding(db))
    assert _resolved_close(db, anchor) == Decimal("120.0000000000"), (
        "a close resolved; the NULLs above are the currency withhold, not an absent price"
    )
