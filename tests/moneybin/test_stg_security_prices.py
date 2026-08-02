"""prep.stg_security_prices resolves the provider key and rejects unusable closes."""

from __future__ import annotations

from decimal import Decimal

import duckdb
import pytest

from moneybin.database import Database, sqlmesh_context
from tests.moneybin.price_model_helpers import ref_kind_mapping as _ref_kind_mapping

pytestmark = pytest.mark.integration


def _insert_price(
    db: Database,
    *,
    key: str,
    close: str,
    source: str = "plaid",
    origin: str = "item_1",
    price_date: str = "2026-07-15",
) -> None:
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source_type,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES (?, ?::DATE, 'USD', ?, ?, ?, 'raw',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [key, price_date, source, origin, close],
    )


def _accept_link(
    db: Database,
    *,
    key: str,
    canonical_id: str,
    ref_kind: str = "plaid_security_id",
    source_type: str = "plaid",
    link_id: str | None = None,
    decided_on: str | None = None,
) -> None:
    """Accept one binding. `link_id` and `decided_on` separate successive owners.

    A recycled key carries more than one link, so the two must be addressable
    apart; `decided_on` is what orders them, because the model hands each link
    the interval starting at the retirement that preceded its own decision.
    """
    db.execute(
        """
        INSERT INTO app.security_links
            (link_id, security_id, ref_kind, ref_value, source_type,
             status, decided_by, decided_at)
        VALUES (?, ?, ?, ?, ?, 'accepted', 'auto',
                COALESCE(?::TIMESTAMP, CURRENT_TIMESTAMP))
        """,  # noqa: S608  # test fixture, not executing user SQL
        [
            link_id or f"link_{key}",
            canonical_id,
            ref_kind,
            key,
            source_type,
            f"{decided_on} 00:00:00" if decided_on else None,
        ],
    )


@pytest.mark.slow
def test_bound_key_resolves_to_the_canonical_security(db: Database) -> None:
    _insert_price(db, key="sec_vti", close="214.55")
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT security_id, close FROM prep.stg_security_prices"
    ).fetchone()
    assert row == ("canonvti0000001", Decimal("214.5500000000"))


@pytest.mark.slow
def test_unresolved_key_stays_in_raw_and_is_absent_from_staging(
    db: Database,
) -> None:
    """The observation is not dropped — it appears once its security resolves."""
    _insert_price(db, key="sec_unbound", close="10.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    staged = db.execute("SELECT COUNT(*) FROM prep.stg_security_prices").fetchone()
    assert staged is not None and staged[0] == 0
    stored = db.execute("SELECT COUNT(*) FROM raw.security_prices").fetchone()
    assert stored is not None and stored[0] == 1


@pytest.mark.slow
def test_reversed_link_does_not_resolve(db: Database) -> None:
    _insert_price(db, key="sec_vti", close="214.55")
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")
    db.execute("UPDATE app.security_links SET status = 'reversed'")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute("SELECT COUNT(*) FROM prep.stg_security_prices").fetchone()
    assert row is not None and row[0] == 0


@pytest.mark.slow
def test_every_mapped_source_resolves_end_to_end(db: Database) -> None:
    """Every source the ref_kind CASE maps must actually reach staging.

    The mapped set is read from the model file, so this test grows itself: adding
    `WHEN 'x' THEN 'x_key'` to the CASE makes this test start seeding an `x` row and an
    `x_key` binding for it, and fail immediately unless app.security_links.ref_kind is
    CHECK-constrained to admit `x_key` too. Extending the CASE alone does not make a
    source resolve; the constraint must be widened in the same change. (That is what
    V042 did for tiingo_ticker and coingecko_slug.) Pinning the mapping's *shipped* set
    as a literal here instead would drift the moment someone edited the model, which is
    exactly when the check needs to fire.

    This direction only sees mappings that exist. A writer shipping ahead of its mapping
    leaves the CASE untouched and this test unchanged — see
    test_price_service.py::test_every_source_the_service_writes_resolves_in_staging.
    """
    mapping = _ref_kind_mapping()
    for index, (source, ref_kind) in enumerate(sorted(mapping.items())):
        key = f"sec_{source}"
        _insert_price(db, key=key, close="100.00", source=source)
        _accept_link(
            db,
            key=key,
            canonical_id=f"canon{index:011d}",
            ref_kind=ref_kind,
            source_type=source,
        )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    resolved = {
        row[0]
        for row in db.execute(
            "SELECT source_type FROM prep.stg_security_prices"
        ).fetchall()
    }
    assert resolved == set(mapping), (
        f"every source mapped in the ref_kind CASE must resolve; mapped={set(mapping)} "
        f"resolved={resolved}. A source in the CASE but absent here is dropped by the "
        f"INNER JOIN with no error and no doctor coverage."
    )


@pytest.mark.slow
def test_an_unmapped_source_is_dropped_permanently_not_deferred(db: Database) -> None:
    """A source the ref_kind CASE does not map is discarded silently and forever.

    This is the finding the COVERAGE block in the model documents, pinned as behavior.
    The binding here is *accepted* and its ref_value matches, so the row fails for one
    reason only: the CASE returns NULL for an unmapped source, making
    `links.ref_kind = NULL` UNKNOWN and the INNER JOIN drop the row. That is unlike the
    unresolved-binding case, where the observation waits in raw and reappears once its
    security binds — no number of accepted bindings will ever surface this one.

    Originally written against 'tiingo' as a tripwire that would fire when the tiingo
    adapter landed. It did not fire, because it watches the CASE and the adapter shipped
    a *writer* one commit ahead of its mapping — every tiingo row written in between was
    dropped here. Two guards replaced that role, in the directions this test cannot see:
    `test_price_service.py` asserts every source PriceService writes is mapped (build
    time), and `investment_unmapped_price_source` reports any unmapped source_type
    already sitting in raw (run time). This test now pins only the drop semantics, using
    a source with no adapter behind it.
    """
    unmapped = "yahoo"
    assert unmapped not in _ref_kind_mapping(), (
        f"{unmapped!r} now has a ref_kind mapping; pick a source with no adapter behind "
        f"it, or this test silently stops covering the drop it names"
    )
    _insert_price(db, key="yahoo_vti", close="214.55", source=unmapped)
    _accept_link(db, key="yahoo_vti", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    staged = db.execute("SELECT COUNT(*) FROM prep.stg_security_prices").fetchone()
    assert staged is not None and staged[0] == 0, "an unmapped source must not resolve"
    stored = db.execute("SELECT COUNT(*) FROM raw.security_prices").fetchone()
    assert stored is not None and stored[0] == 1, (
        "the row survives in raw — but unlike an unbound security it will never "
        "reappear downstream, because the failure is in the mapping, not the binding"
    )


@pytest.mark.slow
def test_non_positive_close_is_rejected(db: Database) -> None:
    """A zero or negative close is rejected at the raw write boundary by CHECK (close > 0).

    The guard lives on the append-only raw table, not as a downstream staging filter: a
    non-positive close is never a real price, and blocking it at write keeps a bad row from
    squatting on the primary key where — the table being append-only — it could never be
    corrected. A valid positive close still inserts.
    """
    _insert_price(db, key="sec_vti", close="214.55", price_date="2026-07-15")
    with pytest.raises(duckdb.ConstraintException):
        _insert_price(db, key="sec_vti", close="0.0", price_date="2026-07-16")
    with pytest.raises(duckdb.ConstraintException):
        _insert_price(db, key="sec_vti", close="-5.00", price_date="2026-07-17")


def _retire_link(
    db: Database, *, key: str, on: str, by: str = "auto", link_id: str | None = None
) -> None:
    """Reverse one accepted link the way a retirement or a rejection leaves it.

    Addressed by `link_id`, not `ref_value`: a recycled key carries several
    links, and matching on the key would retire the successor along with the
    predecessor.
    """
    db.execute(
        """
        UPDATE app.security_links
        SET status = 'reversed', reversed_at = ?::TIMESTAMP, reversed_by = ?
        WHERE link_id = ?
        """,  # noqa: S608  # test fixture, not executing user SQL
        [f"{on} 00:00:00", by, link_id or f"link_{key}"],
    )


@pytest.mark.slow
def test_an_auto_retired_link_still_resolves_its_earlier_observations(
    db: Database,
) -> None:
    """A renamed ticker must not erase the series stored under the old symbol.

    `_retire_stale_binding` reverses an auto-derived link when the catalog value
    it came from moves — FB becomes META. Those FB closes were still this
    security's prices, so an INNER JOIN that admits only `accepted` drops the
    entire pre-rename history out of prep and therefore out of core, on an
    ordinary corporate action and with no error.
    """
    _insert_price(
        db, key="FB", close="180.00", source="tiingo", price_date="2026-07-10"
    )
    _accept_link(
        db,
        key="FB",
        canonical_id="canonmeta000001",
        ref_kind="tiingo_ticker",
        source_type="tiingo",
    )
    _retire_link(db, key="FB", on="2026-07-15")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT security_id, close FROM prep.stg_security_prices"
    ).fetchone()
    assert row == ("canonmeta000001", Decimal("180.0000000000"))


@pytest.mark.slow
def test_an_auto_retired_link_does_not_claim_later_observations(db: Database) -> None:
    """The retired key stops resolving the moment it is retired.

    Tickers get recycled — the rename that freed FB also frees it for whoever
    lists under it next. Without the date bound the retired link would keep
    claiming every future FB close, quietly valuing this security from a
    different company's series: the exact failure retiring the binding existed
    to prevent.
    """
    _insert_price(
        db, key="FB", close="180.00", source="tiingo", price_date="2026-07-10"
    )
    _insert_price(db, key="FB", close="9.99", source="tiingo", price_date="2026-07-20")
    _accept_link(
        db,
        key="FB",
        canonical_id="canonmeta000001",
        ref_kind="tiingo_ticker",
        source_type="tiingo",
    )
    _retire_link(db, key="FB", on="2026-07-15")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    dates = [
        str(row[0])
        for row in db.execute(
            "SELECT price_date FROM prep.stg_security_prices ORDER BY price_date"
        ).fetchall()
    ]
    assert dates == ["2026-07-10"]


def _owners(db: Database) -> list[tuple[str, str]]:
    """Every (security, date) pair staging resolves, so a double claim shows up."""
    return [
        (str(row[0]), str(row[1]))
        for row in db.execute(
            "SELECT security_id, price_date FROM prep.stg_security_prices "
            "ORDER BY price_date, security_id"
        ).fetchall()
    ]


@pytest.mark.slow
def test_a_recycled_key_does_not_claim_the_previous_owners_history(
    db: Database,
) -> None:
    """The new owner of a freed ticker owns it from the handover, not from birth.

    The retired link keeps resolving its own pre-rename closes, so an accepted
    arm with no lower bound does not merely misfile those rows — it resolves
    them a SECOND time, and one raw observation becomes two securities' price
    history. Asserting the whole (security, date) set rather than a count is
    what makes the duplicate visible.
    """
    _insert_price(
        db, key="FB", close="180.00", source="tiingo", price_date="2026-07-10"
    )
    _insert_price(db, key="FB", close="9.99", source="tiingo", price_date="2026-07-20")
    _accept_link(
        db,
        key="FB",
        canonical_id="canonmeta000001",
        ref_kind="tiingo_ticker",
        source_type="tiingo",
        link_id="link_fb_old",
        decided_on="2026-07-01",
    )
    _retire_link(db, key="FB", on="2026-07-15", link_id="link_fb_old")
    _accept_link(
        db,
        key="FB",
        canonical_id="canonnewco00001",
        ref_kind="tiingo_ticker",
        source_type="tiingo",
        link_id="link_fb_new",
        decided_on="2026-07-16",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    assert _owners(db) == [
        ("canonmeta000001", "2026-07-10"),
        ("canonnewco00001", "2026-07-20"),
    ]


@pytest.mark.slow
def test_a_key_retired_twice_resolves_each_interval_once(db: Database) -> None:
    """Two retirements on one key partition its history; they must not overlap.

    A ticker can move away and come back, leaving one security holding two
    retired links on the same key. Each resolves everything before its own
    retirement, so without a lower bound the later link re-claims the earlier
    link's rows and the security's own history doubles under it — invisible in
    a count of distinct securities, and wrong in every valuation.
    """
    _insert_price(
        db, key="FB", close="180.00", source="tiingo", price_date="2026-07-10"
    )
    _insert_price(db, key="FB", close="9.99", source="tiingo", price_date="2026-07-20")
    _accept_link(
        db,
        key="FB",
        canonical_id="canonmeta000001",
        ref_kind="tiingo_ticker",
        source_type="tiingo",
        link_id="link_fb_first",
        decided_on="2026-07-01",
    )
    _retire_link(db, key="FB", on="2026-07-15", link_id="link_fb_first")
    _accept_link(
        db,
        key="FB",
        canonical_id="canonmeta000001",
        ref_kind="tiingo_ticker",
        source_type="tiingo",
        link_id="link_fb_second",
        decided_on="2026-07-16",
    )
    _retire_link(db, key="FB", on="2026-07-25", link_id="link_fb_second")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    assert _owners(db) == [
        ("canonmeta000001", "2026-07-10"),
        ("canonmeta000001", "2026-07-20"),
    ]


@pytest.mark.slow
def test_a_user_reversal_hands_the_next_owner_the_whole_series(db: Database) -> None:
    """A rejection transfers nothing, so it must not bound the next owner below.

    The user's reversal says the pairing was never real. The closes stored under
    that key were therefore always the next holder's, not split at a boundary
    that only describes someone's mistake — so the handover CTE filters on
    `reversed_by`, not on `status` alone. This is the case that separates the
    two: an arm keyed on `status = 'reversed'` passes every other test here.
    """
    _insert_price(
        db, key="FB", close="180.00", source="tiingo", price_date="2026-07-10"
    )
    _insert_price(db, key="FB", close="9.99", source="tiingo", price_date="2026-07-20")
    _accept_link(
        db,
        key="FB",
        canonical_id="canonmeta000001",
        ref_kind="tiingo_ticker",
        source_type="tiingo",
        link_id="link_fb_rejected",
        decided_on="2026-07-01",
    )
    _retire_link(db, key="FB", on="2026-07-15", by="user", link_id="link_fb_rejected")
    _accept_link(
        db,
        key="FB",
        canonical_id="canonnewco00001",
        ref_kind="tiingo_ticker",
        source_type="tiingo",
        link_id="link_fb_rightful",
        decided_on="2026-07-16",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    assert _owners(db) == [
        ("canonnewco00001", "2026-07-10"),
        ("canonnewco00001", "2026-07-20"),
    ]


@pytest.mark.slow
def test_a_user_reversed_link_resolves_nothing(db: Database) -> None:
    """A rejection is a judgement, not bookkeeping, so its prices must drop.

    Paired with the auto-retirement tests deliberately: an arm written to admit
    every `reversed` row passes those and fails this one, and it would restore
    exactly the valuation the user reversed the binding to reject.
    """
    _insert_price(
        db, key="FB", close="180.00", source="tiingo", price_date="2026-07-10"
    )
    _accept_link(
        db,
        key="FB",
        canonical_id="canonmeta000001",
        ref_kind="tiingo_ticker",
        source_type="tiingo",
    )
    _retire_link(db, key="FB", on="2026-07-15", by="user")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    staged = db.execute("SELECT COUNT(*) FROM prep.stg_security_prices").fetchone()
    assert staged is not None and staged[0] == 0
