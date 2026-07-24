"""prep.stg_security_prices resolves the provider key and rejects unusable closes."""

from __future__ import annotations

import re
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

import moneybin
from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration

_MODEL_PATH = (
    Path(moneybin.__file__).parent
    / "sqlmesh"
    / "models"
    / "prep"
    / "stg_security_prices.sql"
)


def _ref_kind_mapping() -> dict[str, str]:
    """The (source -> ref_kind) pairs the model's CASE actually maps, read from it.

    Derived from the model file rather than restated here on purpose. A hardcoded copy
    would drift silently: the whole point of the coverage test below is that extending
    the CASE automatically extends what the test exercises, so an adapter author cannot
    add a mapping without also being told what else the mapping needs (a widened
    app.security_links.ref_kind CHECK).
    """
    sql = _MODEL_PATH.read_text()
    case_blocks = re.findall(r"CASE\s+p\.source_type(.*?)\bEND\b", sql, re.DOTALL)
    assert len(case_blocks) == 1, (
        f"expected exactly one `CASE p.source_type` in {_MODEL_PATH.name}; a second one "
        f"means ref_kind resolution forked and this test no longer covers it: "
        f"{case_blocks}"
    )
    mapping = dict(re.findall(r"WHEN\s+'([^']+)'\s+THEN\s+'([^']+)'", case_blocks[0]))
    assert mapping, "no WHEN ... THEN pairs parsed out of the ref_kind CASE"
    return mapping


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
) -> None:
    db.execute(
        """
        INSERT INTO app.security_links
            (link_id, security_id, ref_kind, ref_value, source_type,
             status, decided_by, decided_at)
        VALUES (?, ?, ?, ?, ?, 'accepted', 'auto',
                CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [f"link_{key}", canonical_id, ref_kind, key, source_type],
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

    The mapped set is read from the model file, so this test grows itself: the day
    someone adds `WHEN 'stooq' THEN 'stooq_ticker'` to the CASE, this test starts
    seeding a stooq row and a stooq_ticker binding for it — and fails immediately,
    because app.security_links.ref_kind is CHECK-constrained to
    ('plaid_security_id', 'institution_security_id'). Extending the CASE alone does not
    make a source resolve; the constraint must be widened in the same change. Pinning
    the mapping's *shipped* set as a literal here instead would drift the moment
    someone edited the model, which is exactly when the check needs to fire.
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
    reason only: `CASE p.source_type WHEN 'plaid' ... END` returns NULL for 'stooq', making
    `links.ref_kind = NULL` UNKNOWN and the INNER JOIN drop the row. That is unlike the
    unresolved-binding case, where the observation waits in raw and reappears once its
    security binds — no number of accepted bindings will ever surface this one.

    Deliberately a tripwire: when a stooq adapter lands and extends the CASE, this test
    goes red and forces whoever wrote it to confront the drop rather than discover it
    in production. Adjust it then; do not weaken it now.
    """
    assert "stooq" not in _ref_kind_mapping(), (
        "stooq now has a ref_kind mapping — this tripwire has done its job. Replace it "
        "with a positive resolution test and move stooq into the covered set."
    )
    _insert_price(db, key="stooq_vti", close="214.55", source="stooq")
    _accept_link(db, key="stooq_vti", canonical_id="canonvti0000001")

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
