"""Tests for OFX staging model Tier 2a dedup, new columns, and FITID supersession.

The supersession tests execute the real model SQL against a stub DB: create the
`raw`/`prep` schemas, seed `raw.ofx_transactions`, strip the MODEL() header, and
create a view from the body. `stg_ofx__transactions` uses only DuckDB-native SQL,
so it runs identically inside a plain connection.
"""

from __future__ import annotations

import re
from decimal import Decimal
from pathlib import Path

import pytest

from moneybin.database import SQLMESH_ROOT, Database
from moneybin.extractors.ofx.extractor import (
    _FITID_COLLISION_MARKER,  # pyright: ignore[reportPrivateUsage]  # cross-language constant, pinned below
)

_MODEL_FILE = SQLMESH_ROOT / "models" / "prep" / "stg_ofx__transactions.sql"
_RAW_DDL_FILE = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "moneybin"
    / "extractors"
    / "ofx"
    / "schema"
    / "raw_ofx_transactions.sql"
)


class TestStgOfxTransactionsModel:
    """Tests for OFX staging model Tier 2a dedup and new columns."""

    def test_model_has_row_number_dedup(self) -> None:
        model_path = SQLMESH_ROOT / "models" / "prep" / "stg_ofx__transactions.sql"
        content = model_path.read_text()
        assert "ROW_NUMBER()" in content
        assert "PARTITION BY" in content
        assert "source_transaction_id" in content
        assert "_row_num = 1" in content

    def test_model_has_source_columns(self) -> None:
        model_path = SQLMESH_ROOT / "models" / "prep" / "stg_ofx__transactions.sql"
        content = model_path.read_text()
        assert "'ofx' AS source_type" in content
        assert "source_origin" in content
        assert "source_transaction_id" in content


# ---------------------------------------------------------------------------
# FITID supersession — a bare id orphaned by `_disambiguate_colliding_fitids`
# ---------------------------------------------------------------------------


def _build_staging(db: Database) -> None:
    """Create `raw.ofx_transactions` from its real DDL and the staging view."""
    db.execute("CREATE SCHEMA IF NOT EXISTS raw")
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(_RAW_DDL_FILE.read_text())
    body = re.sub(
        r"^MODEL\s*\(.*?\);\s*", "", _MODEL_FILE.read_text(), flags=re.DOTALL
    ).strip()
    db.execute(f"CREATE OR REPLACE VIEW prep.stg_ofx__transactions AS\n{body}")  # noqa: S608 — model body read from the repo, not user input


def _insert_ofx_row(
    db: Database,
    *,
    fitid: str,
    source_file: str,
    amount: str = "-13.12",
    payee: str = "FOREIGN TRANSACTION FEE",
    memo: str | None = None,
    account_id: str = "ACC1",
    source_origin: str = "chase",
    fitid_repaired: bool = False,
) -> None:
    """Seed one `raw.ofx_transactions` row.

    Defaults carry the shape that produced the live failure: a foreign-transaction
    fee sharing its FITID with the purchase that incurred it.

    ``fitid_repaired`` defaults False — the institution's own id, untouched. Pass
    True only where the extractor would have rewritten it, since that flag is
    what licenses staging to suppress the id this one superseded.
    """
    db.execute(
        """
        INSERT INTO raw.ofx_transactions (
            source_transaction_id, account_id, transaction_type, date_posted,
            amount, payee, memo, check_number, source_file, extracted_at,
            loaded_at, source_type, source_origin, currency_code, fitid_repaired
        ) VALUES (?, ?, 'DEBIT', TIMESTAMP '2026-01-15 00:00:00', ?, ?, ?, NULL,
                  ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'ofx', ?, 'USD', ?)
        """,  # noqa: S608 — test input, not user data
        [
            fitid,
            account_id,
            Decimal(amount),
            payee,
            memo,
            source_file,
            source_origin,
            fitid_repaired,
        ],
    )


def _staged_ids(db: Database) -> list[str]:
    rows = db.execute(
        "SELECT source_transaction_id FROM prep.stg_ofx__transactions ORDER BY 1"
    ).fetchall()
    return [str(r[0]) for r in rows]


@pytest.mark.unit
def test_staging_uses_the_extractors_collision_marker() -> None:
    """The marker is a constant shared across Python and SQL — pin them together.

    `_disambiguate_colliding_fitids` writes the suffix; the staging model has to
    recognize it. SQL cannot import the Python constant, so this fails if either
    side changes it alone and staging silently stops suppressing.
    """
    assert f"'{_FITID_COLLISION_MARKER}'" in _MODEL_FILE.read_text()


@pytest.mark.unit
def test_bare_fitid_is_dropped_when_a_suffixed_twin_matches_its_content(
    db: Database,
) -> None:
    """The live double-count: a file imported before disambiguation, then after.

    The first import wrote FITID `X` bare. The second saw `X` twice with
    differing content and suffixed *every* member, so the bare row is orphaned —
    its PK no longer matches, and staging partitions on the id, so both survive
    into core. One real transaction, counted twice.
    """
    _build_staging(db)
    _insert_ofx_row(db, fitid="X", source_file="before.qfx")
    _insert_ofx_row(
        db, fitid="X#aaaa1111", source_file="after.qfx", fitid_repaired=True
    )
    _insert_ofx_row(
        db,
        fitid="X#bbbb2222",
        source_file="after.qfx",
        amount="-0.39",
        payee="FOREIGN PURCHASE",
        fitid_repaired=True,
    )

    assert _staged_ids(db) == ["X#aaaa1111", "X#bbbb2222"]


@pytest.mark.unit
def test_bare_fitid_survives_when_no_suffixed_twin_matches_its_content(
    db: Database,
) -> None:
    """A bare row the suffixed group does not contain is a third transaction.

    Isolates the content predicate: the marker prefix matches both suffixed rows,
    so only the content comparison can keep this row alive.
    """
    _build_staging(db)
    _insert_ofx_row(db, fitid="X", source_file="before.qfx", amount="-13.12")
    _insert_ofx_row(
        db,
        fitid="X#aaaa1111",
        source_file="after.qfx",
        amount="-0.39",
        fitid_repaired=True,
    )
    _insert_ofx_row(
        db,
        fitid="X#bbbb2222",
        source_file="after.qfx",
        amount="-99.00",
        fitid_repaired=True,
    )

    assert _staged_ids(db) == ["X", "X#aaaa1111", "X#bbbb2222"]


@pytest.mark.unit
def test_a_native_marker_in_a_fitid_does_not_delete_its_bare_twin(
    db: Database,
) -> None:
    """The marker is evidence of nothing; only the repair flag licenses a delete.

    The OFX spec does not reserve `#`, so an institution may legitimately mint
    both `X` and `X#reference` for two distinct transactions. Content equality
    cannot separate that from a repair — `identifiers.md` is explicit that two
    genuinely distinct transactions can carry identical content, which is why the
    occurrence-suffix rule exists at all. Inferring provenance from the marker
    therefore deletes a real transaction, silently and with no review entry.

    Isolation: the ids, account, origin, and all six hashed fields are exactly
    the shape that *does* suppress in
    `test_bare_fitid_is_dropped_when_a_suffixed_twin_matches_its_content`. Only
    the absent repair flag can keep this row alive.
    """
    _build_staging(db)
    _insert_ofx_row(db, fitid="X", source_file="statement.qfx")
    _insert_ofx_row(db, fitid="X#reference", source_file="statement.qfx")

    assert _staged_ids(db) == ["X", "X#reference"]


@pytest.mark.unit
def test_bare_fitid_with_no_suffixed_sibling_survives(db: Database) -> None:
    """The overwhelmingly common case: no collision ever happened."""
    _build_staging(db)
    _insert_ofx_row(db, fitid="X", source_file="only.qfx")

    assert _staged_ids(db) == ["X"]


@pytest.mark.unit
def test_supersession_does_not_cross_accounts(db: Database) -> None:
    """FITID uniqueness is per account, so two accounts may reuse one id."""
    _build_staging(db)
    _insert_ofx_row(db, fitid="X", source_file="a.qfx", account_id="ACC1")
    _insert_ofx_row(
        db,
        fitid="X#aaaa1111",
        source_file="b.qfx",
        account_id="ACC2",
        fitid_repaired=True,
    )

    assert _staged_ids(db) == ["X", "X#aaaa1111"]


@pytest.mark.unit
def test_supersession_does_not_cross_institutions(db: Database) -> None:
    """The account key here is source-native, so two banks can both mint `ACC1`.

    Nothing scopes an OFX ACCTID globally. Two institutions issuing the same
    account string, one of them hitting a FITID collision whose superseded
    prefix is the other's bare id, would otherwise let one bank's row delete the
    other's — silently, and only when all six hashed fields happen to agree.
    Requiring the same origin costs a legitimate suppression only if one
    institution's exports arrive under two origin slugs, and that failure
    double-counts a row where this one deletes it.
    """
    _build_staging(db)
    _insert_ofx_row(db, fitid="X", source_file="a.qfx", source_origin="chase")
    _insert_ofx_row(
        db,
        fitid="X#aaaa1111",
        source_file="b.qfx",
        source_origin="wells_fargo",
        fitid_repaired=True,
    )

    assert _staged_ids(db) == ["X", "X#aaaa1111"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("bare_fitid", "unrelated_base"),
    [("AB_D", "ABCD"), ("AB%D", "ABxyzD")],
)
def test_a_wildcard_in_a_fitid_does_not_borrow_another_ids_suffix(
    db: Database, bare_fitid: str, unrelated_base: str
) -> None:
    """`_` and `%` are LIKE wildcards — matching by pattern would delete real data.

    The hand-run remediation used `LIKE t.source_transaction_id || '#%'`. A FITID
    containing `_` or `%` makes that pattern match an entirely different id's
    suffixed row, silently dropping a transaction that was never superseded.
    Content is identical here (two same-day charges of one amount at one payee do
    collide), so the content predicate cannot save the row — only matching the id
    literally can.
    """
    _build_staging(db)
    _insert_ofx_row(db, fitid=bare_fitid, source_file="a.qfx")
    _insert_ofx_row(db, fitid=f"{unrelated_base}#deadbeef", source_file="b.qfx")

    assert _staged_ids(db) == sorted([bare_fitid, f"{unrelated_base}#deadbeef"])


@pytest.mark.unit
def test_same_id_in_two_files_keeps_only_the_latest_load(db: Database) -> None:
    """Tier 2a dedup, the behaviour the supersession predicate sits beside."""
    _build_staging(db)
    _insert_ofx_row(db, fitid="X", source_file="old.qfx", payee="PENDING")
    db.execute(
        """
        INSERT INTO raw.ofx_transactions (
            source_transaction_id, account_id, transaction_type, date_posted,
            amount, payee, memo, check_number, source_file, extracted_at,
            loaded_at, source_type, source_origin, currency_code
        ) VALUES ('X', 'ACC1', 'DEBIT', TIMESTAMP '2026-01-15 00:00:00',
                  -13.12, 'POSTED', NULL, NULL, 'new.qfx', CURRENT_TIMESTAMP,
                  CURRENT_TIMESTAMP + INTERVAL 1 HOUR, 'ofx', 'chase', 'USD')
        """  # noqa: S608 — test input, not user data
    )

    rows = db.execute(
        "SELECT source_transaction_id, payee FROM prep.stg_ofx__transactions"
    ).fetchall()
    assert rows == [("X", "POSTED")]
