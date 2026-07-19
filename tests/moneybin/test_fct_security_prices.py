"""core.fct_security_prices picks one winner per security-date-currency."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration


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
            (provider_security_key, price_date, quote_currency, source,
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


@pytest.mark.slow
def test_one_row_per_security_date_currency(db: Database) -> None:
    """Two Plaid connections reporting the same security-date collapse to one row."""
    _insert_price(db, key="sec_vti", close="214.55", origin="item_a")
    _insert_price(db, key="sec_vti", close="214.60", origin="item_b")
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT security_id, quote_currency, source, price_basis, updated_at "
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
    """The pick is deterministic — source_origin breaks the same-rank tie.

    Without that key a rebuild can return a different close from identical inputs,
    which fails the deterministic-resolution requirement.

    item_b is both the fresher observation (later extracted_at) and the lower
    close, so if source_origin's ORDER BY key were dropped, the tiebreak would
    fall through to extracted_at DESC and then close — and item_b would win
    instead. Only source_origin picking item_a first (alphabetically) makes
    item_a the winner despite losing on both of the keys after it. That is what
    proves this test actually exercises source_origin, not one of its neighbors.
    """
    _insert_price(
        db,
        key="sec_vti",
        close="214.60",
        origin="item_a",
        extracted_at="2026-07-15 09:00:00",
    )
    _insert_price(
        db,
        key="sec_vti",
        close="214.55",
        origin="item_b",
        extracted_at="2026-07-15 10:00:00",
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
        "item_a sorts first on source_origin, despite item_b being both fresher "
        "and cheaper"
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
