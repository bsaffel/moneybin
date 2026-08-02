"""core.fct_security_prices picks one winner per security-date-currency."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration

# source_rank is now mutation-tested across sources, but only via the two DERIVED ones:
# 'override' (rank 1) and 'trade_implied' (rank 5) reach this model without a
# provider binding, because neither is a row in raw.security_prices. The three
# PROVIDER ranks still cannot be tested against each other — only 'plaid' resolves to
# a canonical security_id through prep.stg_security_prices' 'plaid_security_id' link,
# so tiingo and coingecko have no binding to reach this model. That provider-vs-provider
# ordering test lands with C.2's adapters and their ref_kind bindings.


def _insert_price(
    db: Database,
    *,
    key: str,
    close: str,
    basis: str = "raw",
    source: str = "plaid",
    origin: str = "item_1",
    price_date: str = "2026-07-15",
    quote_currency: str = "USD",
    extracted_at: str | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source_type,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES (?, ?::DATE, ?, ?, ?, ?, ?,
                COALESCE(?::TIMESTAMP, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [key, price_date, quote_currency, source, origin, close, basis, extracted_at],
    )


def _accept_link(db: Database, *, key: str, canonical_id: str) -> None:
    db.execute(
        """
        INSERT INTO app.security_links
            (link_id, security_id, ref_kind, ref_value, source_type,
             status, decided_by, decided_at)
        VALUES (?, ?, 'plaid_security_id', ?, 'plaid', 'accepted', 'auto',
                CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [f"link_{key}", canonical_id, key],
    )


def _seed_security(db: Database, *, security_id: str) -> None:
    """The canonical catalog row a derived price is authored against."""
    db.execute(
        """
        INSERT INTO app.securities (security_id, name, security_type, ticker)
        VALUES (?, 'Vanguard Total Stock Market ETF', 'etf', 'VTI')
        ON CONFLICT DO NOTHING
        """,  # noqa: S608  # test fixture, not executing user SQL
        [security_id],
    )


def _insert_override(
    db: Database,
    *,
    security_id: str,
    close: str,
    price_date: str = "2026-07-15",
    quote_currency: str = "USD",
    updated_at: str | None = None,
) -> None:
    """A user price mark, written straight to app.security_price_overrides.

    Deliberately not routed through ``SecurityPriceRepo``: the mechanism under test
    here is the model's resolution, not the repo's Invariant-10 audit pairing, which
    has its own tests. This mirrors ``_insert_price`` writing raw directly.
    """
    db.execute(
        """
        INSERT INTO app.security_price_overrides
            (security_id, price_date, quote_currency, close, updated_at)
        VALUES (?, ?::DATE, ?, ?, COALESCE(?::TIMESTAMP, CURRENT_TIMESTAMP))
        """,  # noqa: S608  # test fixture, not executing user SQL
        [security_id, price_date, quote_currency, close, updated_at],
    )


def _insert_manual_trade(
    db: Database,
    *,
    txn_id: str,
    security_id: str | None,
    price: str | None,
    trade_date: str = "2026-07-15",
    quantity: str | None = "10",
    currency_code: str = "USD",
    txn_type: str = "buy",
    created_at: str | None = None,
) -> None:
    """A manual ledger event — the trade-implied price's real upstream mechanism.

    ``created_at`` is settable so two fills can be made to tie on the freshness key
    the way one sync's rows do, which is what forces the observation_key tiebreak.
    """
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees,
            created_by, investment_transaction_id, currency_code, created_at
        ) VALUES (?, 'imp_1', 'acc_1', ?, 'VTI', ?, ?::DATE, ?, ?, -1000.00, 0.00,
                  'test', ?, ?, COALESCE(?::TIMESTAMP, CURRENT_TIMESTAMP))
        """,  # noqa: S608  # test fixture, not executing user SQL
        [
            txn_id,
            security_id,
            txn_type,
            trade_date,
            quantity,
            price,
            txn_id,
            currency_code,
            created_at,
        ],
    )


@pytest.mark.slow
def test_one_row_per_security_date_currency(db: Database) -> None:
    """Two Plaid connections reporting the same security-date collapse to one row."""
    _insert_price(db, key="sec_vti", close="214.55", origin="item_a")
    _insert_price(db, key="sec_vti", close="214.60", origin="item_b")
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT security_id, quote_currency, source_type, price_basis, updated_at "
        "FROM core.fct_security_prices"
    ).fetchone()
    # Full-row shape check — the four given tests otherwise only ever assert on
    # `close`/COUNT, which would miss a bug that swapped or dropped one of the
    # model's other declared output columns.
    assert row is not None
    security_id, quote_currency, source, price_basis, updated_at = row
    assert security_id == "canonvti0000001"
    assert quote_currency == "USD"
    assert source == "plaid"
    assert price_basis == "raw"
    assert updated_at is not None

    rows = db.execute("SELECT COUNT(*) FROM core.fct_security_prices").fetchall()
    assert rows[0][0] == 1


@pytest.mark.slow
def test_winner_is_stable_across_rebuilds(db: Database) -> None:
    """The pick is deterministic — source_origin breaks the tie extracted_at leaves.

    Without that key a rebuild can return a different close from identical inputs,
    which fails the deterministic-resolution requirement.

    The two rows share one extracted_at, so every key ahead of source_origin
    (source_rank, source, extracted_at) is tied and source_origin alone decides.
    item_b is inserted FIRST and is the cheaper close, so both plausible mutants
    land on it rather than on the correct answer: dropping source_origin falls
    through provider_security_key (also tied) to `close` ascending, which picks
    item_b's 214.55. Only source_origin sorting item_a ahead of item_b yields
    214.60. Inserting the winner first, or making it the cheaper row, would let
    those mutants pass by coincidence.
    """
    _insert_price(
        db,
        key="sec_vti",
        close="214.55",
        origin="item_b",
        extracted_at="2026-07-15 09:00:00",
    )
    _insert_price(
        db,
        key="sec_vti",
        close="214.60",
        origin="item_a",
        extracted_at="2026-07-15 09:00:00",
    )
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")

    seen: list[Decimal] = []
    for _ in range(2):
        with sqlmesh_context(db) as ctx:
            ctx.plan(auto_apply=True, no_prompts=True)
        row = db.execute("SELECT close FROM core.fct_security_prices").fetchone()
        assert row is not None
        seen.append(row[0])

    assert seen[0] == seen[1] == Decimal("214.6000000000"), (
        "item_a sorts first on source_origin, the only key not tied between the two"
    )


@pytest.mark.slow
def test_split_day_key_churn_resolves_by_freshness_not_key_sort(db: Database) -> None:
    """A retired provider ref must not outrank its successor on the changeover day.

    app.security_links is N:1: Plaid retires a security_id on a corporate action and
    binds the successor to the SAME canonical security. On a 10:1 split day both refs
    report a close for one price_date and quote currency — the retired ref at the
    pre-split 2000.00, the successor at the post-split 200.00 — and they reach this
    model tied on security_id, source_rank, and source. Only extracted_at distinguishes
    them, and it must be consulted BEFORE provider_security_key.

    The fixture is oriented adversarially: 'sec_a' is the row that must LOSE, it is
    inserted FIRST, and it sorts first alphabetically on provider_security_key. So
    ordering by provider_security_key ahead of extracted_at picks the pre-split 2000.00,
    which core.dim_holdings would then multiply by the POST-split quantity — publishing
    a market_value overstated by the split factor with valuation_status 'valued'. A
    fixture whose correct answer coincided with insertion or key order could not
    discriminate that at all.
    """
    _insert_price(
        db,
        key="sec_a",
        close="2000.00",
        origin="item_1",
        extracted_at="2026-07-15 09:00:00",
    )
    _insert_price(
        db,
        key="sec_b",
        close="200.00",
        origin="item_1",
        extracted_at="2026-07-15 10:00:00",
    )
    _accept_link(db, key="sec_a", canonical_id="canonvti0000001")
    _accept_link(db, key="sec_b", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    staged = db.execute("SELECT COUNT(*) FROM prep.stg_security_prices").fetchone()
    assert staged is not None and staged[0] == 2, (
        "both provider refs must resolve to the one canonical security for this to "
        "exercise the core-layer tie-break rather than an upstream filter"
    )

    rows = db.execute("SELECT close FROM core.fct_security_prices").fetchall()
    assert rows == [(Decimal("200.0000000000"),)], (
        "the successor ref carries the fresher observation and the post-split close; "
        "the retired ref's pre-split 2000.00 must not win on key sort"
    )


@pytest.mark.slow
def test_split_day_key_churn_in_one_pull_withholds_the_grain(db: Database) -> None:
    """When both refs arrive in one sync, freshness cannot decide — so withhold the grain.

    test_split_day_key_churn_resolves_by_freshness_not_key_sort only resolves because its
    two refs carry DIFFERENT extracted_at. In production the extractor stamps one
    batch-level extracted_at per pull, so a retired ref and its successor reported in the
    SAME sync tie on it too. With freshness exhausted the ORDER BY would fall through to
    provider_security_key and settle a 10:1 split by ASCII sort — publishing the pre-split
    2000.00, which dim_holdings would multiply by the POST-split quantity. This grain must
    instead emit NO row, so dim_holdings falls back to an earlier close under its own
    split-staleness guards rather than a confidently wrong one.

    Adversarial orientation: the losing pre-split ref ('sec_a', 2000.00) is inserted first
    and sorts first on provider_security_key, so a model that dropped the withhold and let
    key sort decide publishes 2000.00 — the exact wrong answer. A single-row result of ANY
    close means the guard is gone.
    """
    _insert_price(
        db,
        key="sec_a",
        close="2000.00",
        origin="item_1",
        extracted_at="2026-07-15 09:00:00",
    )
    _insert_price(
        db,
        key="sec_b",
        close="200.00",
        origin="item_1",
        extracted_at="2026-07-15 09:00:00",
    )
    _accept_link(db, key="sec_a", canonical_id="canonvti0000001")
    _accept_link(db, key="sec_b", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    staged = db.execute("SELECT COUNT(*) FROM prep.stg_security_prices").fetchone()
    assert staged is not None and staged[0] == 2, (
        "both refs must resolve to the one canonical security for this to exercise the "
        "core-layer withhold rather than an upstream filter"
    )

    resolved = db.execute("SELECT COUNT(*) FROM core.fct_security_prices").fetchone()
    assert resolved is not None and resolved[0] == 0, (
        "a freshness-tied conflict between two provider refs is unresolvable — the grain "
        "must withhold, not settle the split by key sort"
    )


@pytest.mark.slow
def test_same_pull_casing_duplicate_of_one_ref_still_resolves(db: Database) -> None:
    """The same-pull withhold is scoped to DIFFERENT refs — one ref's casing dup is not a churn.

    Adversarial partner to test_split_day_key_churn_in_one_pull_withholds_the_grain: two
    rows share one extracted_at and conflict on close, but they carry the SAME
    provider_security_key ('sec_vti', differing only in the 'usd'/'USD' casing staging
    folds away). That is a raw duplicate of one instrument, not a retired/successor pair,
    so `close` legitimately breaks the tie and a row must still resolve. A withhold guard
    that keyed on any close conflict — rather than a conflict spanning distinct provider
    refs — would wrongly blank this grain.
    """
    _insert_price(
        db,
        key="sec_vti",
        close="220.00",
        quote_currency="USD",
        extracted_at="2026-07-15 09:00:00",
    )
    _insert_price(
        db,
        key="sec_vti",
        close="205.00",
        quote_currency="usd",
        extracted_at="2026-07-15 09:00:00",
    )
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        "SELECT quote_currency, close FROM core.fct_security_prices"
    ).fetchall()
    assert rows == [("USD", Decimal("205.0000000000"))], (
        "one ref's casing duplicate must resolve by close, not withhold as a churn"
    )


@pytest.mark.slow
def test_adjusted_rows_are_excluded_from_the_resolved_series(db: Database) -> None:
    """An adjusted close stops being correct after the next corporate action.

    It stays visible in raw and staging; it is not eligible to value a holding.
    """
    _insert_price(db, key="sec_vti", close="107.25", basis="split_adjusted")
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    resolved = db.execute("SELECT COUNT(*) FROM core.fct_security_prices").fetchone()
    assert resolved is not None and resolved[0] == 0
    staged = db.execute("SELECT COUNT(*) FROM prep.stg_security_prices").fetchone()
    assert staged is not None and staged[0] == 1, "adjusted rows stay visible upstream"


@pytest.mark.slow
def test_distinct_dates_are_distinct_rows(db: Database) -> None:
    _insert_price(db, key="sec_vti", close="214.55", price_date="2026-07-15")
    _insert_price(db, key="sec_vti", close="215.10", price_date="2026-07-16")
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        "SELECT price_date, close FROM core.fct_security_prices ORDER BY price_date"
    ).fetchall()
    # Strengthened beyond the brief: a bare COUNT == 2 would also pass for a mutant
    # that returned two rows for the SAME date (e.g. a QUALIFY partition missing
    # price_date) — asserting the exact (date, close) pairs actually proves the
    # two dates stayed distinct.
    assert rows == [
        (date(2026, 7, 15), Decimal("214.5500000000")),
        (date(2026, 7, 16), Decimal("215.1000000000")),
    ]


@pytest.mark.slow
def test_quote_currency_case_variants_resolve_deterministically(db: Database) -> None:
    """A raw casing duplicate must not leave two winners or an unstable pick.

    raw.security_prices stores quote_currency exactly as the provider sent it, but
    prep.stg_security_prices normalizes it with UPPER(). Two raw rows differing
    only in that casing ('usd' vs 'USD') carry distinct PKs and both survive to
    staging, then collapse into one QUALIFY partition here — with identical
    source, source_origin, and provider_security_key, so those keys alone leave
    them fully tied. extracted_at DESC (freshest wins) is what breaks it.

    The fixture makes the fresher row (USD @ 215.00, inserted second) the more
    expensive one and the older row (usd @ 210.00, inserted first) the cheaper
    one, so a correct model must pick 215.00 despite it being the higher close.
    That orientation discriminates two distinct mutants, both of which a
    fresher-is-cheaper fixture would miss:

    - Dropping `DESC` from `extracted_at DESC` (oldest wins instead of freshest)
      picks the older row — 210.00 — instead of 215.00.
    - Dropping `extracted_at` from the ORDER BY entirely falls through to `close`
      ascending, which also picks the lower close — 210.00 — instead of 215.00.

    Either mutant surfaces here as the wrong winner. A fresher-and-cheaper
    fixture (the prior orientation) only caught the first: with extracted_at
    removed, `close` ascending coincidentally lands on the same value the
    correct model produces, so the mutant would pass silently.
    """
    _insert_price(
        db,
        key="sec_vti",
        close="210.00",
        quote_currency="usd",
        extracted_at="2026-07-15 09:00:00",
    )
    _insert_price(
        db,
        key="sec_vti",
        close="215.00",
        quote_currency="USD",
        extracted_at="2026-07-15 10:00:00",
    )
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")

    seen: list[tuple[str, Decimal]] = []
    for _ in range(2):
        with sqlmesh_context(db) as ctx:
            ctx.plan(auto_apply=True, no_prompts=True)

        staged = db.execute("SELECT COUNT(*) FROM prep.stg_security_prices").fetchone()
        assert staged is not None and staged[0] == 2, (
            "both raw casing variants must reach staging for this to be a real "
            "test of the core-layer tie-break, not a shortcut around it"
        )

        rows = db.execute(
            "SELECT quote_currency, close FROM core.fct_security_prices"
        ).fetchall()
        assert len(rows) == 1, "the casing duplicate must collapse to one winner"
        seen.append(rows[0])

    assert seen[0] == seen[1] == ("USD", Decimal("215.0000000000"))


@pytest.mark.slow
def test_quote_currency_case_variant_close_is_the_final_tiebreak(db: Database) -> None:
    """A same-sync casing duplicate makes `close` the deciding ORDER BY key.

    Two casing duplicates that arrive in the *same* sync share an identical
    extracted_at, source, source_origin, and provider_security_key — every key
    ahead of `close` in the ORDER BY is tied, so `close` alone must produce a
    single, deterministic winner. This isolates that key: in
    test_quote_currency_case_variants_resolve_deterministically the fixtures
    differ in extracted_at, so close never has to act as more than a no-op
    tail key there.

    The higher-close row (USD @ 220.00) is inserted first and the correct
    winner (usd @ 205.00, the lower close) second. A model that dropped
    `close` from the ORDER BY would leave every remaining key tied and fall
    back to whatever order the query plan happens to produce — which this
    fixture shows lands on the first-inserted row, 220.00, not the correct
    205.00. Inserting the winner first would let that same mutant pass by
    coincidence, the same trap the sibling test's fresher-and-cheaper
    orientation fell into for extracted_at.
    """
    _insert_price(
        db,
        key="sec_vti",
        close="220.00",
        quote_currency="USD",
        extracted_at="2026-07-15 09:00:00",
    )
    _insert_price(
        db,
        key="sec_vti",
        close="205.00",
        quote_currency="usd",
        extracted_at="2026-07-15 09:00:00",
    )
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    staged = db.execute("SELECT COUNT(*) FROM prep.stg_security_prices").fetchone()
    assert staged is not None and staged[0] == 2, (
        "both raw casing variants must reach staging for this to be a real "
        "test of the core-layer tie-break, not a shortcut around it"
    )

    rows = db.execute(
        "SELECT quote_currency, close FROM core.fct_security_prices"
    ).fetchall()
    assert rows == [("USD", Decimal("205.0000000000"))]


@pytest.mark.slow
def test_an_override_outranks_a_provider_close_on_the_same_date(db: Database) -> None:
    """A user mark beats every provider for its own (security, date, currency).

    Adversarial orientation: the provider row is inserted FIRST and carries the
    FRESHER extracted_at, so both realistic mutants land on it rather than on the
    correct answer. Dropping 'override' from the rank CASE sends it to the ELSE 99
    bucket and plaid's rank 2 wins; consulting freshness before rank also picks
    plaid. Either publishes 214.55 instead of the user's 300.00.
    """
    _seed_security(db, security_id="canonvti0000001")
    _insert_price(db, key="sec_vti", close="214.55", extracted_at="2026-07-20 10:00:00")
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")
    _insert_override(
        db,
        security_id="canonvti0000001",
        close="300.00",
        updated_at="2026-07-15 08:00:00",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        "SELECT source_type, close FROM core.fct_security_prices"
    ).fetchall()
    assert rows == [("override", Decimal("300.0000000000"))], (
        "the mark must win its own date despite the provider row being fresher"
    )


@pytest.mark.slow
def test_an_override_does_not_suppress_a_close_on_another_date(db: Database) -> None:
    """A mark is scoped to one date — it must not blank the rest of the series.

    This is the per-date half of "a later provider refresh never silently
    overwrites it". At this model's observation grain both dates keep their own
    row; which one *values a holding* is dim_holdings' as-of pick. A model that let
    an override win its whole security rather than its own date emits one row here
    instead of two.
    """
    _seed_security(db, security_id="canonvti0000001")
    _insert_override(
        db, security_id="canonvti0000001", close="300.00", price_date="2026-07-15"
    )
    _insert_price(db, key="sec_vti", close="214.55", price_date="2026-07-16")
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        "SELECT price_date, source_type, close FROM core.fct_security_prices "
        "ORDER BY price_date"
    ).fetchall()
    assert rows == [
        (date(2026, 7, 15), "override", Decimal("300.0000000000")),
        (date(2026, 7, 16), "plaid", Decimal("214.5500000000")),
    ]


@pytest.mark.slow
def test_an_override_resolves_for_a_security_no_feed_covers(db: Database) -> None:
    """The feedless case the override path exists to serve.

    A restricted grant or private fund has no raw.security_prices row and no accepted
    app.security_links binding, so it never passes prep.stg_security_prices' INNER
    JOIN. The override branch must reach this model without one. A union that gated
    marks behind the provider join — or that applied a provider-derived floor to them
    — emits nothing here, taking manual valuation for feedless securities with it.
    """
    _seed_security(db, security_id="privateco00001")
    _insert_override(db, security_id="privateco00001", close="42.50")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    staged = db.execute("SELECT COUNT(*) FROM prep.stg_security_prices").fetchone()
    assert staged is not None and staged[0] == 0, (
        "no provider observation exists — this must exercise the override branch alone"
    )

    rows = db.execute(
        "SELECT security_id, source_type, price_basis, close "
        "FROM core.fct_security_prices"
    ).fetchall()
    assert rows == [("privateco00001", "override", "raw", Decimal("42.5000000000"))]


@pytest.mark.slow
def test_a_trade_price_becomes_an_observation_on_its_trade_date(db: Database) -> None:
    """An executed trade is a raw observation by construction.

    Without this branch a restricted grant, pre-IPO holding, or private placement
    values at nothing forever, and the user is asked to re-enter by hand a number
    already recorded on the transaction.
    """
    _seed_security(db, security_id="privateco00001")
    _insert_manual_trade(
        db, txn_id="buy_1", security_id="privateco00001", price="137.25"
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        "SELECT security_id, price_date, source_type, price_basis, close "
        "FROM core.fct_security_prices"
    ).fetchall()
    assert rows == [
        (
            "privateco00001",
            date(2026, 7, 15),
            "trade_implied",
            "raw",
            Decimal("137.2500000000"),
        )
    ]


@pytest.mark.slow
def test_a_provider_close_outranks_a_trade_implied_price(db: Database) -> None:
    """An execution reflects one order's size and spread; the day's close beats it.

    Adversarial orientation: the trade is BOTH fresher and cheaper than the provider
    close, so a freshness-before-rank mutant and an ORDER BY falling through to
    `close` ascending both publish 137.25 instead of the correct 214.55.
    """
    _seed_security(db, security_id="canonvti0000001")
    _insert_price(db, key="sec_vti", close="214.55", extracted_at="2026-07-15 09:00:00")
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")
    _insert_manual_trade(
        db,
        txn_id="buy_1",
        security_id="canonvti0000001",
        price="137.25",
        created_at="2026-07-20 09:00:00",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        "SELECT source_type, close FROM core.fct_security_prices"
    ).fetchall()
    assert rows == [("plaid", Decimal("214.5500000000"))]


@pytest.mark.slow
def test_two_same_day_fills_at_different_prices_still_resolve(db: Database) -> None:
    """The same-pull withhold is a PROVIDER guard — partial fills must not trip it.

    Two fills of one security on one day share source_type, source_origin, and — set
    explicitly here, as one sync's rows do in production — extracted_at, while
    carrying different transaction ids and different prices. That is every condition
    same_pull_key_conflict tests for, so a withhold scoped by rank range or left
    unscoped blanks this grain entirely and the position reads unpriced. Partial
    fills are routine, so the failure would be common and silent.

    The correct winner is the LOWER-sorting transaction id, and it is deliberately
    the MORE expensive fill: a model that dropped observation_key from the ORDER BY
    falls through to `close` ascending and publishes fill_b's 240.00 instead.
    """
    _seed_security(db, security_id="canonvti0000001")
    _insert_manual_trade(
        db,
        txn_id="fill_a",
        security_id="canonvti0000001",
        price="250.00",
        created_at="2026-07-15 09:00:00",
    )
    _insert_manual_trade(
        db,
        txn_id="fill_b",
        security_id="canonvti0000001",
        price="240.00",
        created_at="2026-07-15 09:00:00",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    ledger = db.execute(
        "SELECT COUNT(*) FROM core.fct_investment_transactions"
    ).fetchone()
    assert ledger is not None and ledger[0] == 2, (
        "both fills must reach the ledger for this to exercise the core-layer "
        "withhold scoping rather than an upstream filter"
    )

    rows = db.execute(
        "SELECT source_type, close FROM core.fct_security_prices"
    ).fetchall()
    assert rows == [("trade_implied", Decimal("250.0000000000"))], (
        "two fills are not a provider key churn — the grain must resolve, not withhold"
    )


@pytest.mark.slow
def test_a_zero_priced_ledger_event_never_becomes_a_close(db: Database) -> None:
    """Zero is the value 'an unpriced holding is NULL, never zero' exists to refuse.

    raw.security_prices and app.security_price_overrides both CHECK (close > 0), but
    core.fct_investment_transactions.price carries no such constraint — a stock
    dividend legitimately records price 0. Unioned unfiltered, that zero becomes the
    resolved close and values the whole position at nothing while reporting
    valuation_status 'valued'. The ledger event itself is legitimate and must
    survive; only the price observation is refused.

    The type must be one `trade_implied` already admits. A transfer or a dividend is
    refused by `type IN ('buy', 'sell', 'reinvest')` whatever its price, so it would
    leave `AND t.price > 0` free to be deleted with this test still green.
    """
    _seed_security(db, security_id="privateco00001")
    _insert_manual_trade(
        db,
        txn_id="stock_dividend_1",
        security_id="privateco00001",
        price="0",
        txn_type="reinvest",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    ledger = db.execute(
        "SELECT COUNT(*) FROM core.fct_investment_transactions"
    ).fetchone()
    assert ledger is not None and ledger[0] == 1, "the ledger event must survive"

    resolved = db.execute("SELECT COUNT(*) FROM core.fct_security_prices").fetchone()
    assert resolved is not None and resolved[0] == 0


@pytest.mark.slow
def test_a_dividend_rate_never_becomes_the_resolved_close(db: Database) -> None:
    """The headline fix, asserted on rows rather than on the SQL's vocabulary.

    A dividend carries a security AND a price — a per-share DISTRIBUTION RATE,
    not a traded price. Admitting it publishes a $0.91 dividend as a $290 ETF's
    newest close, and `dim_holdings.latest_price` orders by price_date DESC with
    no source filter, so that $0.91 becomes the position's whole valuation.

    The sibling test derives the admissible type set from
    `investment_service._AMOUNT_REQUIRED` and asserts the filter text matches it.
    That catches the vocabulary drifting; it cannot catch the filter being
    dropped, reordered, or applied to the wrong CTE. This seeds both events and
    reads the resolved series.
    """
    _seed_security(db, security_id="vti00000000001")
    _insert_manual_trade(
        db,
        txn_id="buy_1",
        security_id="vti00000000001",
        price="290.00",
        trade_date="2026-07-15",
        txn_type="buy",
    )
    # Later than the buy, so if it were admitted it would win the date ordering
    # and become the position's newest close — the exact failure being refused.
    _insert_manual_trade(
        db,
        txn_id="div_1",
        security_id="vti00000000001",
        price="0.91",
        trade_date="2026-07-16",
        txn_type="dividend",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        "SELECT price_date, close FROM core.fct_security_prices "
        "WHERE security_id = 'vti00000000001' ORDER BY price_date"
    ).fetchall()
    assert rows == [(date(2026, 7, 15), Decimal("290.0000000000"))], (
        "the dividend's per-share rate is not a market close"
    )


@pytest.mark.slow
def test_an_unbound_security_contributes_no_price(db: Database) -> None:
    """A priced trade whose security never bound must not emit a NULL-keyed row.

    core.fct_investment_transactions.security_id is NULL in two situations, and only
    one of them is caught by the positivity filter. A cash-only event (deposit,
    withdrawal, fee) carries a NULL price, which already fails `price > 0` — so a
    cash-only fixture trips BOTH guards and would prove nothing about either.

    The case that isolates the NULL guard is the other one the ledger documents: a
    synced security with no accepted binding. That row carries a real, positive
    price and a NULL security_id, so the positivity filter passes it through. Without
    the NULL guard it lands in a table whose grain leads with security_id, and every
    downstream join against that grain either drops it or fans out.

    The fixture reaches the state through the manual branch because this model cannot
    tell which staging branch a ledger row came from; the production shape is a Plaid
    buy whose SecurityResolver binding was never accepted.
    """
    _insert_manual_trade(db, txn_id="unbound_1", security_id=None, price="137.25")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    ledger = db.execute(
        "SELECT COUNT(*) FROM core.fct_investment_transactions "
        "WHERE security_id IS NULL AND price > 0"
    ).fetchone()
    assert ledger is not None and ledger[0] == 1, (
        "the priced-but-unbound row must reach the ledger for this to isolate the "
        "NULL guard rather than the positivity filter"
    )

    resolved = db.execute("SELECT COUNT(*) FROM core.fct_security_prices").fetchone()
    assert resolved is not None and resolved[0] == 0
