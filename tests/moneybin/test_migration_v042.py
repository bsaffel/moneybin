"""V042: widen app.security_links.ref_kind for market-feed provider keys.

Phase C.2 of ``investments-price-feeds.md`` binds Tiingo tickers and CoinGecko
slugs through the existing provider-neutral ``app.security_links`` rather than a
text ``ticker`` join, so every provider resolves by one audited path. DuckDB
cannot alter a CHECK constraint in place, so the migration rebuilds the table
(the V034/V035 idiom): copy -> drop -> recreate with the widened CHECK ->
restore -> drop copy.

Per ``.claude/rules/database.md`` "Migration test data realism", adding a CHECK
constraint touches existing data, so the fixture seeds >=3 rows with realistic
shapes across the nullable reversal columns the rebuild's INSERT...SELECT must
preserve, and every mutation test drives ``migrate()`` through
``run_migration()`` to reproduce the runner's enclosing BEGIN/COMMIT.

The rebuild re-creates four CHECKs. A future edit that drops one while keeping
the ``ref_kind`` widening would leave the table silently unconstrained, so each
sibling CHECK gets its own discriminating test — the V035 precedent for this
same pattern.
"""

from __future__ import annotations

from datetime import datetime

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V042__widen_security_link_ref_kinds import migrate
from tests.moneybin.migration_helpers import run_migration

pytestmark = pytest.mark.fresh_db

_OLD_SHAPE = """
CREATE TABLE app.security_links (
    link_id VARCHAR NOT NULL,
    security_id VARCHAR NOT NULL,
    ref_kind VARCHAR NOT NULL
        CHECK (ref_kind IN ('plaid_security_id', 'institution_security_id')),
    ref_value VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,
    status VARCHAR NOT NULL
        CHECK (status IN ('accepted', 'reversed')),
    decided_by VARCHAR NOT NULL
        CHECK (decided_by IN ('auto', 'user', 'system')),
    decided_at TIMESTAMP NOT NULL,
    reversed_at TIMESTAMP,
    reversed_by VARCHAR
        CHECK (reversed_by IS NULL OR reversed_by IN ('auto', 'user', 'system')),
    PRIMARY KEY (link_id)
)
"""


@pytest.fixture
def old_shape_db(db: Database) -> Database:
    """The pre-V042 app.security_links shape, populated with >=3 realistic rows.

    The third row is reversed so the rebuild has to carry non-NULL values
    through ``reversed_at`` / ``reversed_by`` — the two nullable columns an
    INSERT...SELECT is most likely to drop.
    """
    db.execute("DROP TABLE app.security_links")
    db.execute(_OLD_SHAPE)
    db.execute(
        """
        INSERT INTO app.security_links (
            link_id, security_id, ref_kind, ref_value, source_type, status,
            decided_by, decided_at, reversed_at, reversed_by
        ) VALUES
        ('link0000aaa1', 'abc123def456', 'plaid_security_id', 'eq_plaid_sec_1',
         'plaid', 'accepted', 'auto', TIMESTAMP '2024-01-02 03:04:05',
         NULL, NULL),
        ('link0000bbb2', 'bitcoin000001', 'plaid_security_id', 'crypto_plaid_1',
         'plaid', 'accepted', 'user', TIMESTAMP '2024-02-03 04:05:06',
         NULL, NULL),
        ('link0000ccc3', 'moneymkt000001', 'institution_security_id',
         'ins_109508:mm_sec_9', 'plaid', 'reversed', 'system',
         TIMESTAMP '2024-03-04 05:06:07', TIMESTAMP '2024-03-05 06:07:08',
         'user')
        """
    )
    return db


def _insert_link(db: Database, link_id: str, ref_kind: str, ref_value: str) -> None:
    db.execute(
        """
        INSERT INTO app.security_links (
            link_id, security_id, ref_kind, ref_value, source_type, status,
            decided_by, decided_at
        ) VALUES (?, 'abc123def456', ?, ?, 'plaid', 'accepted', 'auto',
                  TIMESTAMP '2024-06-01 00:00:00')
        """,
        [link_id, ref_kind, ref_value],
    )


def test_v042_admits_tiingo_ticker(old_shape_db: Database) -> None:
    """The equity feed's key must bind through the audited link path."""
    run_migration(old_shape_db, migrate)
    _insert_link(old_shape_db, "linktiingo01", "tiingo_ticker", "VTI")
    row = old_shape_db.execute(
        "SELECT ref_value FROM app.security_links WHERE link_id = 'linktiingo01'"
    ).fetchone()
    assert row is not None and row[0] == "VTI"


def test_v042_admits_coingecko_slug(old_shape_db: Database) -> None:
    """The crypto feed's key binds through the same path, not a second one."""
    run_migration(old_shape_db, migrate)
    _insert_link(old_shape_db, "linkgecko001", "coingecko_slug", "bitcoin")
    row = old_shape_db.execute(
        "SELECT ref_value FROM app.security_links WHERE link_id = 'linkgecko001'"
    ).fetchone()
    assert row is not None and row[0] == "bitcoin"


def test_v042_preserves_existing_rows(old_shape_db: Database) -> None:
    """The rebuild's INSERT...SELECT must not drop a row or a column value."""
    run_migration(old_shape_db, migrate)
    row = old_shape_db.execute(
        """
        SELECT security_id, ref_kind, ref_value, source_type, status,
               decided_by, decided_at, reversed_at, reversed_by
          FROM app.security_links
         WHERE link_id = 'link0000ccc3'
        """
    ).fetchone()
    assert row == (
        "moneymkt000001",
        "institution_security_id",
        "ins_109508:mm_sec_9",
        "plaid",
        "reversed",
        "system",
        datetime(2024, 3, 4, 5, 6, 7),
        datetime(2024, 3, 5, 6, 7, 8),
        "user",
    )


def test_v042_preserves_row_count(old_shape_db: Database) -> None:
    run_migration(old_shape_db, migrate)
    row = old_shape_db.execute("SELECT COUNT(*) FROM app.security_links").fetchone()
    assert row is not None and row[0] == 3


def test_v042_is_idempotent(old_shape_db: Database) -> None:
    run_migration(old_shape_db, migrate)
    run_migration(old_shape_db, migrate)
    row = old_shape_db.execute("SELECT COUNT(*) FROM app.security_links").fetchone()
    assert row is not None and row[0] == 3


def test_v042_still_rejects_unknown_ref_kind(old_shape_db: Database) -> None:
    """Widening is additive — the CHECK must not become a rubber stamp.

    The discriminating value is a plausible-but-unadmitted provider key, not
    obvious garbage: an adapter that ships before its migration would try
    exactly this.
    """
    run_migration(old_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        _insert_link(old_shape_db, "linkbogus001", "yahoo_ticker", "VTI")


def test_v042_status_check_survives_rebuild(old_shape_db: Database) -> None:
    """A future edit dropping the status CHECK from the rebuild must fail here."""
    run_migration(old_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        old_shape_db.execute(
            """
            INSERT INTO app.security_links (
                link_id, security_id, ref_kind, ref_value, source_type, status,
                decided_by, decided_at
            ) VALUES ('linkstatbad1', 'abc123def456', 'tiingo_ticker', 'VOO',
                      'plaid', 'pending', 'auto', TIMESTAMP '2024-06-01 00:00:00')
            """
        )


def test_v042_decided_by_check_survives_rebuild(old_shape_db: Database) -> None:
    """A future edit dropping the decided_by CHECK from the rebuild must fail here."""
    run_migration(old_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        old_shape_db.execute(
            """
            INSERT INTO app.security_links (
                link_id, security_id, ref_kind, ref_value, source_type, status,
                decided_by, decided_at
            ) VALUES ('linkdecbybad', 'abc123def456', 'tiingo_ticker', 'VOO',
                      'plaid', 'accepted', 'admin', TIMESTAMP '2024-06-01 00:00:00')
            """
        )


def test_v042_reversed_by_check_survives_rebuild(old_shape_db: Database) -> None:
    """A future edit dropping the reversed_by CHECK from the rebuild must fail here."""
    run_migration(old_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        old_shape_db.execute(
            "UPDATE app.security_links SET reversed_by = 'admin' "
            "WHERE link_id = 'link0000aaa1'"
        )


def test_v042_leaves_the_decisions_queue_narrow(old_shape_db: Database) -> None:
    """DECISION REVERSED 2026-07-24 — this test is scheduled for replacement.

    It pins what V042 *currently* does, not what the design now calls for. Do
    not read it as settled intent.

    The original reasoning was that ``app.security_link_decisions`` is the
    provider-initiated identity queue (Plaid sends a security MoneyBin does not
    recognize; the resolver proposes a merge candidate for review), while a
    market-feed key travels the opposite direction — minted from a security
    already in the catalog — so nothing would ever write a decision row for one.

    That missed a real property of investing data: a ticker is not a unique
    identifier. The same symbol names different securities across exchanges
    (BHP on NYSE vs ASX), share classes collide (GOOG vs GOOGL), and symbols get
    recycled. So deriving a feed key from the catalog *can* be an ambiguous
    inference, and ``design-principles.md`` ("Magic stays visible") requires a
    weak inference to surface a confirm rather than act silently.

    The replacement work, per the resolved design: widen this CHECK to admit
    ``tiingo_ticker`` / ``coingecko_slug``, and bind silently ONLY on a
    near-certain signal — a previously confirmed binding, a CUSIP/ISIN match, or
    an exact ticker+exchange match resolving to exactly one security. Ambiguous
    derivations route here; unmatched keys stay unbound and surface in the new
    held-but-unpriced doctor check. The user requirement that constrains this:
    a review entry per held position is unacceptable — no comparable tool asks
    that, and near-certain bindings must never reach this queue.
    """
    run_migration(old_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        old_shape_db.execute(
            """
            INSERT INTO app.security_link_decisions (
                decision_id, ref_kind, ref_value, source_type,
                candidate_security_id, status, decided_by, decided_at
            ) VALUES ('dectiingo001', 'tiingo_ticker', 'VTI', 'tiingo',
                      'abc123def456', 'pending', 'auto',
                      TIMESTAMP '2024-06-01 00:00:00')
            """
        )


def test_fresh_schema_admits_the_new_ref_kinds(db: Database) -> None:
    """The dual-path rule: a fresh install gets the widened CHECK from the DDL.

    Without this, ``app_security_links.sql`` and the migration could drift —
    existing databases would accept a Tiingo binding and new ones would not.
    """
    _insert_link(db, "freshtiingo1", "tiingo_ticker", "VTI")
    _insert_link(db, "freshgecko01", "coingecko_slug", "ethereum")
    row = db.execute(
        "SELECT COUNT(*) FROM app.security_links "
        "WHERE ref_kind IN ('tiingo_ticker', 'coingecko_slug')"
    ).fetchone()
    assert row is not None and row[0] == 2
