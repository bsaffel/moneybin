"""Tests for int_transactions__matched SQL model — live execution against a stub DB.

Executes the model SQL directly (no SQLMesh runtime) by:
  1. Creating ``prep`` schema + a stub ``prep.int_transactions__unioned`` table.
  2. Seeding ``app.match_decisions`` rows.
  3. Stripping the MODEL() header and creating a view from the SQL body.
  4. Querying ``prep.int_transactions__matched``.

This approach exercises the real SQL without requiring the full SQLMesh pipeline.
The model SQL uses only DuckDB-native SQL features (recursive CTEs, SHA256,
LISTAGG), so it runs identically inside a plain DuckDB connection.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Generator
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[3]
_MODEL_FILE = (
    _REPO_ROOT / "sqlmesh" / "models" / "prep" / "int_transactions__matched.sql"
)

# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

_PREP_SCHEMA_DDL = "CREATE SCHEMA IF NOT EXISTS prep;"

# Minimal stub — only the columns referenced by int_transactions__matched.
_UNIONED_STUB_DDL = """\
CREATE TABLE IF NOT EXISTS prep.int_transactions__unioned (
    source_transaction_id VARCHAR NOT NULL,
    account_id            VARCHAR NOT NULL,
    source_account_key    VARCHAR,
    transaction_date      DATE,
    authorized_date       DATE,
    amount                DECIMAL(18, 2),
    description           VARCHAR,
    original_description  VARCHAR,
    merchant_name         VARCHAR,
    memo                  VARCHAR,
    category              VARCHAR,
    subcategory           VARCHAR,
    payment_channel       VARCHAR,
    transaction_type      VARCHAR,
    check_number          VARCHAR,
    is_pending            BOOLEAN,
    pending_transaction_id VARCHAR,
    location_address      VARCHAR,
    location_city         VARCHAR,
    location_region       VARCHAR,
    location_postal_code  VARCHAR,
    location_country      VARCHAR,
    location_latitude     DOUBLE,
    location_longitude    DOUBLE,
    currency_code         VARCHAR,
    source_type           VARCHAR,
    source_origin         VARCHAR,
    source_file           VARCHAR,
    source_extracted_at   TIMESTAMP,
    loaded_at             TIMESTAMP
);
"""


def _load_model_view_sql() -> str:
    """Read the model file and return the CTE+SELECT body as a CREATE VIEW."""
    raw = _MODEL_FILE.read_text()
    # Strip the MODEL(...) block at the top (everything up to and including
    # the closing parenthesis on its own line).
    body = re.sub(r"^MODEL\s*\(.*?\);\s*", "", raw, flags=re.DOTALL).strip()
    return f"CREATE OR REPLACE VIEW prep.int_transactions__matched AS\n{body}"


def _insert_match(
    db: Database,
    *,
    match_id: str,
    stid_a: str,
    st_a: str,
    stid_b: str,
    st_b: str,
    account_id: str,
    confidence: float = 0.95,
) -> None:
    """Insert an accepted dedup match decision."""
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test input, not executing user SQL
        [
            match_id,
            stid_a,
            st_a,
            "bank",
            stid_b,
            st_b,
            "bank",
            account_id,
            confidence,
            "{}",
            "dedup",
            "3",
            None,
            "accepted",
            "test match",
            "auto",
        ],
    )


def _insert_unioned_row(
    db: Database,
    *,
    source_transaction_id: str,
    source_type: str,
    account_id: str,
    source_account_key: str | None = None,
    source_origin: str = "bank",
    txn_date: date = date(2024, 3, 15),
    amount: str = "-52.30",
) -> None:
    """Insert a minimal row into the prep.int_transactions__unioned stub.

    ``source_account_key`` is the immutable source-native account key that the
    RD-2 transaction_id hash is built from; it defaults to ``account_id`` so each
    distinct account gets a distinct key (the realistic case), but tests probing
    account_id-independence override it to hold the source identity fixed.
    """
    db.execute(
        """
        INSERT INTO prep.int_transactions__unioned (
            source_transaction_id, account_id, source_account_key,
            transaction_date, amount, description, currency_code,
            source_type, source_origin, is_pending
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,  # noqa: S608  # test input, not executing user SQL
        [
            source_transaction_id,
            account_id,
            source_account_key if source_account_key is not None else account_id,
            txn_date,
            amount,
            "Test transaction",
            "USD",
            source_type,
            source_origin,
            False,
        ],
    )


def _source_identity_hash(
    *,
    source_type: str,
    source_origin: str,
    source_account_key: str,
    source_transaction_id: str,
) -> str:
    """Independently derive the RD-2 transaction_id from the immutable source tuple.

    Mirrors the model's ``SUBSTRING(SHA256(source_type || '|' || source_origin ||
    '|' || source_account_key || '|' || source_transaction_id), 1, 16)``. Derived
    here from first principles (hashlib over the documented input), never observed
    from the model's output, per testing.md.
    """
    raw = f"{source_type}|{source_origin}|{source_account_key}|{source_transaction_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def matched_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    """Database with ``prep`` + ``app.match_decisions`` ready for model tests."""
    database = Database(
        tmp_path / "test_matched.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    database.execute(_PREP_SCHEMA_DDL)
    database.execute(_UNIONED_STUB_DDL)
    # Create the view from the actual model SQL.
    database.execute(_load_model_view_sql())
    yield database
    database.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestIntTransactionsMatchedModel:
    """Live-execution tests against int_transactions__matched."""

    def test_no_match_decisions_yields_no_groups(self, matched_db: Database) -> None:
        """Without any accepted dedup decisions the group columns are NULL."""
        _insert_unioned_row(
            matched_db,
            source_transaction_id="csv_aabbccddee001122",
            source_type="csv",
            account_id="acct1",
        )
        row = matched_db.execute(
            "SELECT match_group_id, match_confidence "
            "FROM prep.int_transactions__matched"
        ).fetchone()
        assert row is not None
        assert row[0] is None  # no group
        assert row[1] is None  # no confidence

    def test_pipe_in_source_transaction_id_does_not_truncate(
        self, matched_db: Database
    ) -> None:
        """A source_transaction_id containing '|' must still group correctly.

        Regression: the fold previously packed nodes as 'st|stid' and recovered
        the id with SPLIT_PART(node, '|', 2), which truncated ids containing '|'
        (possible for tabular source-provided ids) so the row failed to rejoin
        its group and silently did not dedup.
        """
        acct = "acct_pipe"
        stid_a = "TBL|2024-03-15|AMZN"  # tabular source id with delimiters
        stid_b = "ofx_112233445566aabb"
        _insert_unioned_row(
            matched_db, source_transaction_id=stid_a, source_type="csv", account_id=acct
        )
        _insert_unioned_row(
            matched_db, source_transaction_id=stid_b, source_type="ofx", account_id=acct
        )
        _insert_match(
            matched_db,
            match_id="match_pipe_0001",
            stid_a=stid_a,
            st_a="csv",
            stid_b=stid_b,
            st_b="ofx",
            account_id=acct,
        )

        rows = matched_db.execute(
            "SELECT source_transaction_id, match_group_id, transaction_id "
            "FROM prep.int_transactions__matched "
            "WHERE account_id = ? ORDER BY source_transaction_id",
            [acct],
        ).fetchall()
        assert len(rows) == 2
        assert all(r[1] is not None for r in rows)  # both got a match_group_id
        assert len({r[1] for r in rows}) == 1  # one component
        assert len({r[2] for r in rows}) == 1  # one gold record
        assert stid_a in {r[0] for r in rows}  # '|' id preserved verbatim

    def test_same_source_ids_in_two_accounts_do_not_conflate(
        self, matched_db: Database
    ) -> None:
        """Two accounts reusing the same source_transaction_ids stay separate.

        Regression: group_id was the MIN packed member, unique only within an
        account; source-provided ids can repeat across accounts, so two accounts'
        components would share a group_id and conflate into one gold key. The
        account-prefixed group_id keeps them distinct.
        """
        for acct in ("acctA", "acctB"):
            _insert_unioned_row(
                matched_db,
                source_transaction_id="s1",
                source_type="csv",
                account_id=acct,
            )
            _insert_unioned_row(
                matched_db,
                source_transaction_id="s2",
                source_type="ofx",
                account_id=acct,
            )
            _insert_match(
                matched_db,
                match_id=f"m_{acct}",
                stid_a="s1",
                st_a="csv",
                stid_b="s2",
                st_b="ofx",
                account_id=acct,
            )

        rows = matched_db.execute(
            "SELECT account_id, match_group_id, transaction_id "
            "FROM prep.int_transactions__matched WHERE match_group_id IS NOT NULL"
        ).fetchall()
        # 4 member rows, but two distinct components and two distinct gold keys.
        assert len(rows) == 4
        assert len({r[1] for r in rows}) == 2  # distinct match_group_id per account
        assert len({r[2] for r in rows}) == 2  # distinct gold record per account

    def test_pair_collapses_to_one_group(self, matched_db: Database) -> None:
        """Two matched transactions share a single match_group_id."""
        acct = "acct2"
        stid_a = "csv_aabbccddee001122"
        stid_b = "ofx_112233445566aabb"
        _insert_unioned_row(
            matched_db,
            source_transaction_id=stid_a,
            source_type="csv",
            account_id=acct,
        )
        _insert_unioned_row(
            matched_db,
            source_transaction_id=stid_b,
            source_type="ofx",
            account_id=acct,
        )
        _insert_match(
            matched_db,
            match_id="match_pair_0001",
            stid_a=stid_a,
            st_a="csv",
            stid_b=stid_b,
            st_b="ofx",
            account_id=acct,
        )

        rows = matched_db.execute(
            "SELECT DISTINCT match_group_id, transaction_id "
            "FROM prep.int_transactions__matched "
            "WHERE match_group_id IS NOT NULL "
            "ORDER BY 1"
        ).fetchall()
        # Both rows share one group_id and one synthetic transaction_id.
        assert len(rows) == 1

    def test_four_node_chain_folds_to_one_group(self, matched_db: Database) -> None:
        """A chain A–B, B–C, C–D must collapse to a single match component.

        The two-pass algorithm used before this change incorrectly split this
        chain into two groups (A–B–C and B–C–D). The recursive-CTE transitive
        closure must return exactly 1 distinct match_group_id and 1 distinct
        synthetic transaction_id for the four matched rows.

        Derivation: four nodes linked by three accepted edges form one connected
        component — hand-verified before implementation.
        """
        acct = "acct_chain"
        # Node IDs — hex-only content hashes (no '|' chars, delimiter-safe).
        node_a = "csv_aaaa000000000001"
        node_b = "csv_bbbb000000000002"
        node_c = "csv_cccc000000000003"
        node_d = "csv_dddd000000000004"

        for stid in (node_a, node_b, node_c, node_d):
            _insert_unioned_row(
                matched_db,
                source_transaction_id=stid,
                source_type="csv",
                account_id=acct,
            )

        # Three edges forming a chain: A–B, B–C, C–D.
        # Use lexicographically ordered match_ids so that the 2-pass algorithm
        # deterministically fails: node D only sees edge m3 (the largest),
        # so it never inherits the m1 component and lands in a separate group.
        _insert_match(
            matched_db,
            match_id="match_chain_0001",
            stid_a=node_a,
            st_a="csv",
            stid_b=node_b,
            st_b="csv",
            account_id=acct,
        )
        _insert_match(
            matched_db,
            match_id="match_chain_0002",
            stid_a=node_b,
            st_a="csv",
            stid_b=node_c,
            st_b="csv",
            account_id=acct,
        )
        _insert_match(
            matched_db,
            match_id="match_chain_0003",
            stid_a=node_c,
            st_a="csv",
            stid_b=node_d,
            st_b="csv",
            account_id=acct,
        )

        result = matched_db.execute(
            "SELECT COUNT(DISTINCT match_group_id), COUNT(DISTINCT transaction_id) "
            "FROM prep.int_transactions__matched "
            "WHERE match_group_id IS NOT NULL"
        ).fetchone()
        assert result is not None
        distinct_groups, distinct_tids = result
        # The two-pass algorithm returns (2, 2); the recursive closure must
        # return (1, 1).
        assert distinct_groups == 1, (
            f"Expected 1 match group for 4-node chain, got {distinct_groups}"
        )
        assert distinct_tids == 1, (
            f"Expected 1 synthetic transaction_id for 4-node chain, got {distinct_tids}"
        )

    def test_group_confidence_is_weakest_link(self, matched_db: Database) -> None:
        """A group's match_confidence is the MIN (weakest-link) over its edges.

        Three nodes A–B–C form one connected component with two accepted edges:
          A–B at confidence 0.96
          B–C at confidence 0.80
        The group is only as trustworthy as its shakiest edge, so every member
        must report match_confidence = 0.8000, not 0.9600.
        """
        from decimal import Decimal

        acct = "acct_weakest"
        node_a = "csv_aaaa000000000011"
        node_b = "csv_bbbb000000000022"
        node_c = "csv_cccc000000000033"

        for stid in (node_a, node_b, node_c):
            _insert_unioned_row(
                matched_db,
                source_transaction_id=stid,
                source_type="csv",
                account_id=acct,
            )

        _insert_match(
            matched_db,
            match_id="match_weak_0001",
            stid_a=node_a,
            st_a="csv",
            stid_b=node_b,
            st_b="csv",
            account_id=acct,
            confidence=0.96,
        )
        _insert_match(
            matched_db,
            match_id="match_weak_0002",
            stid_a=node_b,
            st_a="csv",
            stid_b=node_c,
            st_b="csv",
            account_id=acct,
            confidence=0.80,
        )

        rows = matched_db.execute(
            """
            SELECT DISTINCT match_confidence
            FROM prep.int_transactions__matched
            WHERE match_group_id IS NOT NULL
            ORDER BY 1
            """  # noqa: S608  # test input, not executing user SQL
        ).fetchall()
        # Exactly one confidence value in the group — the weakest edge.
        assert len(rows) == 1, f"Expected 1 distinct confidence value, got {rows}"
        assert rows[0][0] == Decimal("0.8000"), (
            f"Expected weakest-link confidence 0.8000, got {rows[0][0]}"
        )

    def test_unmatched_transaction_id_is_source_identity_hash_not_account_id(
        self, matched_db: Database
    ) -> None:
        """RD-2: an unmatched row's transaction_id is independent of account_id.

        The id hashes the immutable source identity
        (source_type|source_origin|source_account_key|source_transaction_id) only.
        Two rows share one source identity but carry different account_ids — the
        artificial case that isolates the hash inputs. They must produce the SAME
        transaction_id, equal to the independently-derived source-identity hash.
        Re-minting account_id (M1S) therefore never re-keys a transaction.
        """
        sak = "src-acct-key-77"
        origin = "first_bank"
        stid = "ofx_fitid_00112233"
        expected = _source_identity_hash(
            source_type="ofx",
            source_origin=origin,
            source_account_key=sak,
            source_transaction_id=stid,
        )

        # Same source identity, two different canonical account_ids.
        for acct in ("acct_before_remint", "acct_after_remint"):
            _insert_unioned_row(
                matched_db,
                source_transaction_id=stid,
                source_type="ofx",
                account_id=acct,
                source_account_key=sak,
                source_origin=origin,
            )

        rows = matched_db.execute(
            "SELECT account_id, transaction_id "
            "FROM prep.int_transactions__matched "
            "WHERE source_transaction_id = ? ORDER BY account_id",
            [stid],
        ).fetchall()

        assert len(rows) == 2
        ids = {r[1] for r in rows}
        assert ids == {expected}, (
            f"Expected both rows to hash to {expected!r} regardless of account_id, "
            f"got {rows!r}"
        )

    def test_matched_group_transaction_id_anchors_on_native_source(
        self, matched_db: Database
    ) -> None:
        """RD-2: a merged group's transaction_id is the hash of its ANCHOR member.

        Anchor = argmin over (stability_rank, loaded_at, source_type, ...). An OFX
        member (stability_rank 0, native) outranks a CSV member (rank 2, content
        hash), so the group key derives from the OFX member's source identity —
        not the CSV member's, and not a whole-set hash including account_id.
        """
        acct = "acct_anchor"
        sak = "anchor-src-key-01"
        origin = "anchor_bank"
        ofx_stid = "ofx_zzzzzzzzzzzzzzzz"  # lexically large: would lose a tiebreak
        csv_stid = "csv_aaaaaaaaaaaaaaaa"  # lexically small: wins only on rank
        _insert_unioned_row(
            matched_db,
            source_transaction_id=ofx_stid,
            source_type="ofx",
            account_id=acct,
            source_account_key=sak,
            source_origin=origin,
        )
        _insert_unioned_row(
            matched_db,
            source_transaction_id=csv_stid,
            source_type="csv",
            account_id=acct,
            source_account_key=sak,
            source_origin=origin,
        )
        _insert_match(
            matched_db,
            match_id="match_anchor_0001",
            stid_a=csv_stid,
            st_a="csv",
            stid_b=ofx_stid,
            st_b="ofx",
            account_id=acct,
        )

        ofx_hash = _source_identity_hash(
            source_type="ofx",
            source_origin=origin,
            source_account_key=sak,
            source_transaction_id=ofx_stid,
        )
        csv_hash = _source_identity_hash(
            source_type="csv",
            source_origin=origin,
            source_account_key=sak,
            source_transaction_id=csv_stid,
        )

        rows = matched_db.execute(
            "SELECT DISTINCT transaction_id "
            "FROM prep.int_transactions__matched "
            "WHERE account_id = ? AND match_group_id IS NOT NULL",
            [acct],
        ).fetchall()

        assert len(rows) == 1, f"Expected one gold key for the group, got {rows!r}"
        assert rows[0][0] == ofx_hash, (
            f"Group key should anchor on the OFX member ({ofx_hash!r}), got {rows[0][0]!r}"
        )
        assert rows[0][0] != csv_hash, "Group key must not derive from the CSV member"
