"""prep.stg_security_prices resolves the provider key and rejects unusable closes."""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context

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
            (provider_security_key, price_date, quote_currency, source,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES (?, ?::DATE, 'USD', ?, ?, ?, 'raw',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [key, price_date, source, origin, close],
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
def test_non_positive_close_is_rejected(db: Database) -> None:
    _insert_price(db, key="sec_vti", close="214.55", price_date="2026-07-15")
    _insert_price(db, key="sec_vti", close="0.0", price_date="2026-07-16")
    _accept_link(db, key="sec_vti", canonical_id="canonvti0000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute("SELECT price_date FROM prep.stg_security_prices").fetchall()
    assert len(rows) == 1, "a zero close is not a price"
