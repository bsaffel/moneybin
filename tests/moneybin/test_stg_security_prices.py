"""prep.stg_security_prices resolves the provider key and rejects unusable closes."""

from __future__ import annotations

import shutil
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.tables import SEED_PRICE_SOURCE_MAP
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


@pytest.fixture(scope="module")
def security_price_cases_template(
    tmp_path_factory: pytest.TempPathFactory,
    request: pytest.FixtureRequest,
) -> Path:
    """One encrypted, planned baseline over independent staging-price cases."""
    secret_store = MagicMock()
    secret_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    db = Database(
        tmp_path_factory.mktemp("stg_security_prices") / "test.duckdb",
        secret_store=secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    request.addfinalizer(db.close)

    try:
        _insert_price(
            db,
            key="mb21_bound_key",
            close="214.55",
            origin="mb21_bound",
        )
        _accept_link(
            db,
            key="mb21_bound_key",
            canonical_id="mb21_bound_security",
            link_id="mb21_bound_link",
        )

        _insert_price(
            db,
            key="mb21_unbound_key",
            close="10.00",
            origin="mb21_unbound",
        )

        _insert_price(
            db,
            key="mb21_reversed_key",
            close="214.55",
            origin="mb21_reversed",
        )
        _accept_link(
            db,
            key="mb21_reversed_key",
            canonical_id="mb21_reversed_security",
            link_id="mb21_reversed_link",
        )
        db.execute(
            "UPDATE app.security_links SET status = 'reversed' "
            "WHERE link_id = 'mb21_reversed_link'"
        )

        mapping = _ref_kind_mapping()
        for index, (source, ref_kind) in enumerate(sorted(mapping.items())):
            key = f"mb21_mapped_{source}"
            _insert_price(
                db,
                key=key,
                close="100.00",
                source=source,
                origin=f"mb21_mapped_{source}",
            )
            _accept_link(
                db,
                key=key,
                canonical_id=f"mb21_mapped_{index:011d}",
                ref_kind=ref_kind,
                source_type=source,
                link_id=f"mb21_mapped_link_{source}",
            )

        _insert_price(
            db,
            key="mb21_unmapped_key",
            close="214.55",
            source="yahoo",
            origin="mb21_unmapped",
        )
        _accept_link(
            db,
            key="mb21_unmapped_key",
            canonical_id="mb21_unmapped_security",
            source_type="yahoo",
            link_id="mb21_unmapped_link",
        )

        _insert_price(
            db,
            key="mb21_positive_key",
            close="214.55",
            origin="mb21_non_positive",
        )

        _insert_price(
            db,
            key="mb21_auto_earlier_key",
            close="180.00",
            source="tiingo",
            origin="mb21_auto_earlier",
            price_date="2026-07-10",
        )
        _accept_link(
            db,
            key="mb21_auto_earlier_key",
            canonical_id="mb21_auto_earlier_security",
            ref_kind="tiingo_ticker",
            source_type="tiingo",
            link_id="mb21_auto_earlier_link",
        )
        _retire_link(
            db,
            key="mb21_auto_earlier_key",
            on="2026-07-15",
            link_id="mb21_auto_earlier_link",
        )

        _insert_price(
            db,
            key="mb21_auto_later_key",
            close="180.00",
            source="tiingo",
            origin="mb21_auto_later",
            price_date="2026-07-10",
        )
        _insert_price(
            db,
            key="mb21_auto_later_key",
            close="9.99",
            source="tiingo",
            origin="mb21_auto_later",
            price_date="2026-07-20",
        )
        _accept_link(
            db,
            key="mb21_auto_later_key",
            canonical_id="mb21_auto_later_security",
            ref_kind="tiingo_ticker",
            source_type="tiingo",
            link_id="mb21_auto_later_link",
        )
        _retire_link(
            db,
            key="mb21_auto_later_key",
            on="2026-07-15",
            link_id="mb21_auto_later_link",
        )

        _insert_price(
            db,
            key="mb21_recycled_key",
            close="180.00",
            source="tiingo",
            origin="mb21_recycled",
            price_date="2026-07-10",
        )
        _insert_price(
            db,
            key="mb21_recycled_key",
            close="9.99",
            source="tiingo",
            origin="mb21_recycled",
            price_date="2026-07-20",
        )
        _accept_link(
            db,
            key="mb21_recycled_key",
            canonical_id="mb21_recycled_old_security",
            ref_kind="tiingo_ticker",
            source_type="tiingo",
            link_id="mb21_recycled_old_link",
            decided_on="2026-07-01",
        )
        _retire_link(
            db,
            key="mb21_recycled_key",
            on="2026-07-15",
            link_id="mb21_recycled_old_link",
        )
        _accept_link(
            db,
            key="mb21_recycled_key",
            canonical_id="mb21_recycled_new_security",
            ref_kind="tiingo_ticker",
            source_type="tiingo",
            link_id="mb21_recycled_new_link",
            decided_on="2026-07-16",
        )

        _insert_price(
            db,
            key="mb21_twice_key",
            close="180.00",
            source="tiingo",
            origin="mb21_twice",
            price_date="2026-07-10",
        )
        _insert_price(
            db,
            key="mb21_twice_key",
            close="9.99",
            source="tiingo",
            origin="mb21_twice",
            price_date="2026-07-20",
        )
        _accept_link(
            db,
            key="mb21_twice_key",
            canonical_id="mb21_twice_security",
            ref_kind="tiingo_ticker",
            source_type="tiingo",
            link_id="mb21_twice_first_link",
            decided_on="2026-07-01",
        )
        _retire_link(
            db, key="mb21_twice_key", on="2026-07-15", link_id="mb21_twice_first_link"
        )
        _accept_link(
            db,
            key="mb21_twice_key",
            canonical_id="mb21_twice_security",
            ref_kind="tiingo_ticker",
            source_type="tiingo",
            link_id="mb21_twice_second_link",
            decided_on="2026-07-16",
        )
        _retire_link(
            db, key="mb21_twice_key", on="2026-07-25", link_id="mb21_twice_second_link"
        )

        _insert_price(
            db,
            key="mb21_user_handover_key",
            close="180.00",
            source="tiingo",
            origin="mb21_user_handover",
            price_date="2026-07-10",
        )
        _insert_price(
            db,
            key="mb21_user_handover_key",
            close="9.99",
            source="tiingo",
            origin="mb21_user_handover",
            price_date="2026-07-20",
        )
        _accept_link(
            db,
            key="mb21_user_handover_key",
            canonical_id="mb21_user_handover_rejected",
            ref_kind="tiingo_ticker",
            source_type="tiingo",
            link_id="mb21_user_handover_rejected_link",
            decided_on="2026-07-01",
        )
        _retire_link(
            db,
            key="mb21_user_handover_key",
            on="2026-07-15",
            by="user",
            link_id="mb21_user_handover_rejected_link",
        )
        _accept_link(
            db,
            key="mb21_user_handover_key",
            canonical_id="mb21_user_handover_rightful",
            ref_kind="tiingo_ticker",
            source_type="tiingo",
            link_id="mb21_user_handover_rightful_link",
            decided_on="2026-07-16",
        )

        _insert_price(
            db,
            key="mb21_user_reversed_key",
            close="180.00",
            source="tiingo",
            origin="mb21_user_reversed",
            price_date="2026-07-10",
        )
        _accept_link(
            db,
            key="mb21_user_reversed_key",
            canonical_id="mb21_user_reversed_security",
            ref_kind="tiingo_ticker",
            source_type="tiingo",
            link_id="mb21_user_reversed_link",
        )
        _retire_link(
            db,
            key="mb21_user_reversed_key",
            on="2026-07-15",
            by="user",
            link_id="mb21_user_reversed_link",
        )

        with sqlmesh_context(db) as ctx:
            ctx.plan(auto_apply=True, no_prompts=True)
        return db.path
    finally:
        db.close()


@pytest.fixture()
def security_price_cases(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    security_price_cases_template: Path,
) -> Database:
    """An isolated, writable copy of the shared planned baseline."""
    path = tmp_path / "test.duckdb"
    shutil.copy(security_price_cases_template, path)
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
    return db


def test_bound_key_resolves_to_the_canonical_security(
    security_price_cases: Database,
) -> None:
    db = security_price_cases

    row = db.execute(
        "SELECT security_id, close FROM prep.stg_security_prices "
        "WHERE security_id = 'mb21_bound_security'"
    ).fetchone()
    assert row == ("mb21_bound_security", Decimal("214.5500000000"))


def test_unresolved_key_stays_in_raw_and_is_absent_from_staging(
    security_price_cases: Database,
) -> None:
    """The observation is not dropped — it appears once its security resolves."""
    db = security_price_cases

    staged = db.execute(
        "SELECT COUNT(*) FROM prep.stg_security_prices "
        "WHERE source_origin = 'mb21_unbound'"
    ).fetchone()
    assert staged is not None and staged[0] == 0
    stored = db.execute(
        "SELECT COUNT(*) FROM raw.security_prices WHERE source_origin = 'mb21_unbound'"
    ).fetchone()
    assert stored is not None and stored[0] == 1


def test_reversed_link_does_not_resolve(security_price_cases: Database) -> None:
    db = security_price_cases

    row = db.execute(
        "SELECT COUNT(*) FROM prep.stg_security_prices "
        "WHERE source_origin = 'mb21_reversed'"
    ).fetchone()
    assert row is not None and row[0] == 0


def test_every_mapped_source_resolves_end_to_end(
    security_price_cases: Database,
) -> None:
    """Every source seeds.price_source_map maps must actually reach staging.

    The mapped set is read from the registry, so this test grows itself: adding a row
    with a ref_kind makes this test start seeding an `x` row and an `x_key` binding for
    it, and fail immediately unless app.security_links.ref_kind is CHECK-constrained to
    admit `x_key` too. Declaring the mapping alone does not make a source resolve; the
    constraint must be widened in the same change. (That is what V042 did for
    tiingo_ticker and coingecko_slug.) Pinning the mapping's *shipped* set as a literal
    here instead would drift the moment someone edited the registry, which is exactly
    when the check needs to fire — tests/moneybin/test_price_sources.py holds that pin,
    where changing it is the point rather than the accident.

    This direction only sees mappings that exist. A registry row someone deletes merely
    shrinks the set iterated here; that direction is the rank-order pin in
    test_price_sources.py and the run-time investment_unmapped_price_source check.
    """
    db = security_price_cases
    mapping = _ref_kind_mapping()

    resolved = {
        row[0]
        for row in db.execute(
            "SELECT source_type FROM prep.stg_security_prices "
            "WHERE source_origin LIKE 'mb21_mapped_%'"
        ).fetchall()
    }
    assert resolved == set(mapping), (
        f"every source the registry maps must resolve; mapped={set(mapping)} "
        f"resolved={resolved}. A source in the registry but absent here is dropped by "
        f"the INNER JOIN with no error and no doctor coverage."
    )


def test_the_seeded_registry_nulls_the_ref_kind_of_a_derived_source(
    security_price_cases: Database,
) -> None:
    """A blank CSV cell must seed as NULL, not as the empty string.

    ``core.fct_security_prices`` scopes its same-pull withhold to the sources
    that HAVE a ref_kind, so an empty string seeded in place of NULL would pull
    ``override`` and ``trade_implied`` into a churn check meant only for
    provider keys — and would blank a grain whose two rows are a routine pair of
    partial fills. Nothing else in the suite reads that distinction directly.
    """
    db = security_price_cases

    unmapped = {
        row[0]
        for row in db.execute(
            f"SELECT source_type FROM {SEED_PRICE_SOURCE_MAP.full_name} "  # noqa: S608  # TableRef constant, not user input
            "WHERE ref_kind IS NULL"
        ).fetchall()
    }
    assert unmapped == {"override", "trade_implied"}


def test_an_unmapped_source_is_dropped_permanently_not_deferred(
    security_price_cases: Database,
) -> None:
    """A source the registry does not map is discarded silently and forever.

    This is the finding the COVERAGE block in the model documents, pinned as behavior.
    The binding here is *accepted* and its ref_value matches, so the row fails for one
    reason only: seeds.price_source_map holds no row for the source, so the join to it
    matches nothing and the INNER JOIN drops the row. That is unlike the
    unresolved-binding case, where the observation waits in raw and reappears once its
    security binds — no number of accepted bindings will ever surface this one.

    Originally written against 'tiingo' as a tripwire that would fire when the tiingo
    adapter landed. It did not fire, because it watched the mapping while the adapter
    shipped a *writer* one commit ahead of it — every tiingo row written in between was
    dropped here. One registry row now declares both halves, so that split is
    structural rather than guarded; `investment_unmapped_price_source` still reports any
    unmapped source_type already sitting in raw (run time). This test now pins only the
    drop semantics, using a source with no adapter behind it.
    """
    unmapped = "yahoo"
    assert unmapped not in _ref_kind_mapping(), (
        f"{unmapped!r} now has a ref_kind mapping; pick a source with no adapter behind "
        f"it, or this test silently stops covering the drop it names"
    )
    db = security_price_cases

    staged = db.execute(
        "SELECT COUNT(*) FROM prep.stg_security_prices "
        "WHERE source_origin = 'mb21_unmapped'"
    ).fetchone()
    assert staged is not None and staged[0] == 0, "an unmapped source must not resolve"
    stored = db.execute(
        "SELECT COUNT(*) FROM raw.security_prices WHERE source_origin = 'mb21_unmapped'"
    ).fetchone()
    assert stored is not None and stored[0] == 1, (
        "the row survives in raw — but unlike an unbound security it will never "
        "reappear downstream, because the failure is in the mapping, not the binding"
    )


def test_non_positive_close_is_rejected(security_price_cases: Database) -> None:
    """A zero or negative close is rejected at the raw write boundary by CHECK (close > 0).

    The guard lives on the append-only raw table, not as a downstream staging filter: a
    non-positive close is never a real price, and blocking it at write keeps a bad row from
    squatting on the primary key where — the table being append-only — it could never be
    corrected. A valid positive close still inserts.
    """
    db = security_price_cases
    with pytest.raises(duckdb.ConstraintException):
        _insert_price(
            db,
            key="mb21_positive_key",
            close="0.0",
            origin="mb21_non_positive",
            price_date="2026-07-16",
        )
    with pytest.raises(duckdb.ConstraintException):
        _insert_price(
            db,
            key="mb21_positive_key",
            close="-5.00",
            origin="mb21_non_positive",
            price_date="2026-07-17",
        )


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


def test_an_auto_retired_link_still_resolves_its_earlier_observations(
    security_price_cases: Database,
) -> None:
    """A renamed ticker must not erase the series stored under the old symbol.

    `_retire_stale_binding` reverses an auto-derived link when the catalog value
    it came from moves — FB becomes META. Those FB closes were still this
    security's prices, so an INNER JOIN that admits only `accepted` drops the
    entire pre-rename history out of prep and therefore out of core, on an
    ordinary corporate action and with no error.
    """
    db = security_price_cases

    row = db.execute(
        "SELECT security_id, close FROM prep.stg_security_prices "
        "WHERE security_id = 'mb21_auto_earlier_security'"
    ).fetchone()
    assert row == ("mb21_auto_earlier_security", Decimal("180.0000000000"))


def test_an_auto_retired_link_does_not_claim_later_observations(
    security_price_cases: Database,
) -> None:
    """The retired key stops resolving the moment it is retired.

    Tickers get recycled — the rename that freed FB also frees it for whoever
    lists under it next. Without the date bound the retired link would keep
    claiming every future FB close, quietly valuing this security from a
    different company's series: the exact failure retiring the binding existed
    to prevent.
    """
    db = security_price_cases

    dates = [
        str(row[0])
        for row in db.execute(
            "SELECT price_date FROM prep.stg_security_prices "
            "WHERE security_id = 'mb21_auto_later_security' ORDER BY price_date"
        ).fetchall()
    ]
    assert dates == ["2026-07-10"]


def _owners(db: Database, origin: str) -> list[tuple[str, str]]:
    """Every (security, date) pair staging resolves, so a double claim shows up."""
    return [
        (str(row[0]), str(row[1]))
        for row in db.execute(
            "SELECT security_id, price_date FROM prep.stg_security_prices "
            "WHERE source_origin = ? ORDER BY price_date, security_id",
            [origin],
        ).fetchall()
    ]


def test_a_recycled_key_does_not_claim_the_previous_owners_history(
    security_price_cases: Database,
) -> None:
    """The new owner of a freed ticker owns it from the handover, not from birth.

    The retired link keeps resolving its own pre-rename closes, so an accepted
    arm with no lower bound does not merely misfile those rows — it resolves
    them a SECOND time, and one raw observation becomes two securities' price
    history. Asserting the whole (security, date) set rather than a count is
    what makes the duplicate visible.
    """
    db = security_price_cases

    assert _owners(db, "mb21_recycled") == [
        ("mb21_recycled_old_security", "2026-07-10"),
        ("mb21_recycled_new_security", "2026-07-20"),
    ]


def test_a_key_retired_twice_resolves_each_interval_once(
    security_price_cases: Database,
) -> None:
    """Two retirements on one key partition its history; they must not overlap.

    A ticker can move away and come back, leaving one security holding two
    retired links on the same key. Each resolves everything before its own
    retirement, so without a lower bound the later link re-claims the earlier
    link's rows and the security's own history doubles under it — invisible in
    a count of distinct securities, and wrong in every valuation.
    """
    db = security_price_cases

    assert _owners(db, "mb21_twice") == [
        ("mb21_twice_security", "2026-07-10"),
        ("mb21_twice_security", "2026-07-20"),
    ]


def test_a_user_reversal_hands_the_next_owner_the_whole_series(
    security_price_cases: Database,
) -> None:
    """A rejection transfers nothing, so it must not bound the next owner below.

    The user's reversal says the pairing was never real. The closes stored under
    that key were therefore always the next holder's, not split at a boundary
    that only describes someone's mistake — so the handover CTE filters on
    `reversed_by`, not on `status` alone. This is the case that separates the
    two: an arm keyed on `status = 'reversed'` passes every other test here.
    """
    db = security_price_cases

    assert _owners(db, "mb21_user_handover") == [
        ("mb21_user_handover_rightful", "2026-07-10"),
        ("mb21_user_handover_rightful", "2026-07-20"),
    ]


def test_a_user_reversed_link_resolves_nothing(
    security_price_cases: Database,
) -> None:
    """A rejection is a judgement, not bookkeeping, so its prices must drop.

    Paired with the auto-retirement tests deliberately: an arm written to admit
    every `reversed` row passes those and fails this one, and it would restore
    exactly the valuation the user reversed the binding to reject.
    """
    db = security_price_cases

    staged = db.execute(
        "SELECT COUNT(*) FROM prep.stg_security_prices "
        "WHERE source_origin = 'mb21_user_reversed'"
    ).fetchone()
    assert staged is not None and staged[0] == 0
