"""V042: widen the ref_kind CHECK on both security-link tables for feed keys.

Phase C.2 of ``investments-price-feeds.md`` binds Tiingo tickers and CoinGecko
slugs through the existing provider-neutral ``app.security_links`` rather than a
text ``ticker`` join, so every provider resolves by one audited path. Because a
ticker is not a unique identifier (BHP on NYSE vs ASX; GOOG vs GOOGL; recycled
symbols), deriving that feed key from the catalog can be ambiguous, so the same
two ``ref_kind`` values are also admitted on the review queue
``app.security_link_decisions`` — an uncertain derivation queues there rather
than binding silently ("Magic stays visible"). Both tables are rebuilt.

DuckDB cannot alter a CHECK constraint in place, so each rebuild follows the
V034/V035 idiom: copy -> drop -> recreate with the widened CHECK -> restore ->
drop copy.

Per ``.claude/rules/database.md`` "Migration test data realism", adding a CHECK
constraint touches existing data, so each fixture seeds >=3 rows with realistic
shapes across the nullable columns the rebuild's INSERT...SELECT must preserve
(including the decisions queue's JSON and DECIMAL columns), and every mutation
test drives ``migrate()`` through ``run_migration()`` to reproduce the runner's
enclosing BEGIN/COMMIT.

Each rebuild re-creates several sibling CHECKs. A future edit that drops one
while keeping the ``ref_kind`` widening would leave the table silently
unconstrained, so each sibling CHECK gets its own discriminating test — the
V035 precedent for this same pattern.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V042__widen_security_link_ref_kinds import migrate
from tests.moneybin.migration_helpers import run_migration

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


_OLD_DECISIONS_SHAPE = """
CREATE TABLE app.security_link_decisions (
    decision_id VARCHAR NOT NULL,
    ref_kind VARCHAR NOT NULL
        CHECK (ref_kind IN ('plaid_security_id', 'institution_security_id')),
    ref_value VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,
    provider_ticker VARCHAR,
    provider_name VARCHAR,
    candidate_security_id VARCHAR NOT NULL,
    confidence_score DECIMAL(5, 4),
    match_signals JSON,
    status VARCHAR NOT NULL
        CHECK (status IN ('pending', 'accepted', 'rejected', 'reversed')),
    decided_by VARCHAR NOT NULL
        CHECK (decided_by IN ('auto', 'user')),
    match_reason VARCHAR,
    decided_at TIMESTAMP NOT NULL,
    reversed_at TIMESTAMP,
    reversed_by VARCHAR
        CHECK (reversed_by IS NULL OR reversed_by IN ('auto', 'user')),
    PRIMARY KEY (decision_id)
)
"""


@pytest.fixture
def old_decisions_shape_db(db: Database) -> Database:
    """The pre-V042 app.security_link_decisions shape, with >=3 realistic rows.

    The third row is reversed and carries non-NULL ``confidence_score`` (DECIMAL)
    and ``match_signals`` (JSON) values — the columns an INSERT...SELECT rebuild
    is most likely to drop or mis-type.
    """
    db.execute("DROP TABLE app.security_link_decisions")
    db.execute(_OLD_DECISIONS_SHAPE)
    db.execute(
        """
        INSERT INTO app.security_link_decisions (
            decision_id, ref_kind, ref_value, source_type, provider_ticker,
            provider_name, candidate_security_id, confidence_score, match_signals,
            status, decided_by, match_reason, decided_at, reversed_at, reversed_by
        ) VALUES
        ('dec00000pnd1', 'plaid_security_id', 'eq_plaid_sec_1', 'plaid', NULL,
         NULL, 'abc123def456', NULL, NULL, 'pending', 'auto', NULL,
         TIMESTAMP '2024-01-02 03:04:05', NULL, NULL),
        ('dec00000acc2', 'institution_security_id', 'ins_109508:mm_sec_2',
         'plaid', 'VOO', 'Vanguard 500', 'sec000000002', 0.9900,
         '{"signal": "cusip_exact"}', 'accepted', 'user', 'cusip_exact',
         TIMESTAMP '2024-02-03 04:05:06', NULL, NULL),
        ('dec00000rev3', 'institution_security_id', 'ins_109508:mm_sec_9',
         'plaid', 'MMDA', 'Money Market Fund', 'moneymkt000001', 0.8700,
         '{"signal": "name_similarity", "score": 0.87}', 'reversed', 'user',
         'fuzzy_name', TIMESTAMP '2024-03-04 05:06:07',
         TIMESTAMP '2024-03-05 06:07:08', 'user')
        """
    )
    return db


def _decision_row(decision_id: str, **overrides: object) -> dict[str, object]:
    """A valid app.security_link_decisions row; override the column under test."""
    row: dict[str, object] = {
        "decision_id": decision_id,
        "ref_kind": "tiingo_ticker",
        "ref_value": "VTI",
        "source_type": "tiingo",
        "candidate_security_id": "abc123def456",
        "status": "pending",
        "decided_by": "auto",
        "decided_at": datetime(2024, 6, 1, 0, 0, 0),
        "reversed_at": None,
        "reversed_by": None,
    }
    row.update(overrides)
    return row


def _insert_decision(db: Database, decision_id: str, **overrides: object) -> None:
    row = _decision_row(decision_id, **overrides)
    columns = list(row)
    placeholders = ", ".join("?" * len(columns))
    db.execute(
        "INSERT INTO app.security_link_decisions "  # noqa: S608  # fixed column list, not user input
        f"({', '.join(columns)}) VALUES ({placeholders})",
        list(row.values()),
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


def test_v042_admits_tiingo_ticker_decision(old_decisions_shape_db: Database) -> None:
    """An ambiguous Tiingo-ticker derivation must be queueable for review.

    A ticker is not a unique identifier, so binding a feed key from the catalog
    can be a guess. ``design-principles.md`` ("Magic stays visible") requires
    the uncertain case to surface here rather than bind silently.
    """
    run_migration(old_decisions_shape_db, migrate)
    _insert_decision(old_decisions_shape_db, "dectiingo001", ref_value="VTI")
    row = old_decisions_shape_db.execute(
        "SELECT ref_value FROM app.security_link_decisions "
        "WHERE decision_id = 'dectiingo001'"
    ).fetchone()
    assert row is not None and row[0] == "VTI"


def test_v042_admits_coingecko_slug_decision(old_decisions_shape_db: Database) -> None:
    """The crypto feed's ambiguous derivation routes to the same review queue."""
    run_migration(old_decisions_shape_db, migrate)
    _insert_decision(
        old_decisions_shape_db,
        "decgecko0001",
        ref_kind="coingecko_slug",
        ref_value="bitcoin",
        source_type="coingecko",
    )
    row = old_decisions_shape_db.execute(
        "SELECT ref_value FROM app.security_link_decisions "
        "WHERE decision_id = 'decgecko0001'"
    ).fetchone()
    assert row is not None and row[0] == "bitcoin"


def test_v042_preserves_existing_decisions(old_decisions_shape_db: Database) -> None:
    """The rebuild's INSERT...SELECT must carry every column, JSON and DECIMAL included."""
    run_migration(old_decisions_shape_db, migrate)
    row = old_decisions_shape_db.execute(
        """
        SELECT ref_kind, ref_value, source_type, provider_ticker, provider_name,
               candidate_security_id, confidence_score,
               json_extract_string(match_signals, '$.signal') AS signal,
               status, decided_by, match_reason, decided_at, reversed_at,
               reversed_by
          FROM app.security_link_decisions
         WHERE decision_id = 'dec00000rev3'
        """
    ).fetchone()
    assert row == (
        "institution_security_id",
        "ins_109508:mm_sec_9",
        "plaid",
        "MMDA",
        "Money Market Fund",
        "moneymkt000001",
        Decimal("0.8700"),
        "name_similarity",
        "reversed",
        "user",
        "fuzzy_name",
        datetime(2024, 3, 4, 5, 6, 7),
        datetime(2024, 3, 5, 6, 7, 8),
        "user",
    )


def test_v042_preserves_decision_row_count(old_decisions_shape_db: Database) -> None:
    run_migration(old_decisions_shape_db, migrate)
    row = old_decisions_shape_db.execute(
        "SELECT COUNT(*) FROM app.security_link_decisions"
    ).fetchone()
    assert row is not None and row[0] == 3


def test_v042_decisions_is_idempotent(old_decisions_shape_db: Database) -> None:
    run_migration(old_decisions_shape_db, migrate)
    run_migration(old_decisions_shape_db, migrate)
    row = old_decisions_shape_db.execute(
        "SELECT COUNT(*) FROM app.security_link_decisions"
    ).fetchone()
    assert row is not None and row[0] == 3


def test_v042_decisions_still_rejects_unknown_ref_kind(
    old_decisions_shape_db: Database,
) -> None:
    """Widening is additive — the CHECK must not become a rubber stamp."""
    run_migration(old_decisions_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        _insert_decision(
            old_decisions_shape_db, "decbogus0001", ref_kind="yahoo_ticker"
        )


def test_v042_decisions_status_check_survives_rebuild(
    old_decisions_shape_db: Database,
) -> None:
    """A future edit dropping the status CHECK from the rebuild must fail here."""
    run_migration(old_decisions_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        _insert_decision(old_decisions_shape_db, "decstatbad1", status="merged")


def test_v042_decisions_decided_by_check_survives_rebuild(
    old_decisions_shape_db: Database,
) -> None:
    """A future edit dropping the decided_by CHECK from the rebuild must fail here.

    ``system`` is a valid ``decided_by`` on the sibling ``security_links`` table
    but not here — the discriminating value proves the CHECK is scoped, not
    copy-pasted from the wider sibling.
    """
    run_migration(old_decisions_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        _insert_decision(old_decisions_shape_db, "decdecbybad", decided_by="system")


def test_v042_decisions_reversed_by_check_survives_rebuild(
    old_decisions_shape_db: Database,
) -> None:
    """A future edit dropping the reversed_by CHECK from the rebuild must fail here."""
    run_migration(old_decisions_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        old_decisions_shape_db.execute(
            "UPDATE app.security_link_decisions SET reversed_by = 'system' "
            "WHERE decision_id = 'dec00000rev3'"
        )


def test_fresh_schema_admits_the_new_ref_kinds_decisions(db: Database) -> None:
    """The dual-path rule: a fresh install's decisions queue gets the widened CHECK.

    Without this, ``app_security_link_decisions.sql`` and the migration could
    drift — a migrated database would queue a feed-key derivation and a fresh
    one would reject it.
    """
    _insert_decision(db, "freshtiingo1", ref_value="VTI")
    _insert_decision(
        db,
        "freshgecko01",
        ref_kind="coingecko_slug",
        ref_value="ethereum",
        source_type="coingecko",
    )
    row = db.execute(
        "SELECT COUNT(*) FROM app.security_link_decisions "
        "WHERE ref_kind IN ('tiingo_ticker', 'coingecko_slug')"
    ).fetchone()
    assert row is not None and row[0] == 2


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
