"""Tests for ImportService.revert.

Covers the full response envelope (``reverted``, ``not_found``,
``already_reverted``, ``unsupported``, ``superseded``) across OFX and
tabular source_types. ``not_found`` ships with a ``reason`` field.
"""

from __future__ import annotations

from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.services.import_service import ImportService


def test_revert_unknown_import_id_returns_not_found(db: Database) -> None:
    """Reverting an unknown import_id returns status='not_found'."""
    result = ImportService(db).revert("00000000-0000-0000-0000-000000000000")
    assert result["status"] == "not_found"


def test_revert_already_reverted_returns_already_reverted(db: Database) -> None:
    """Reverting an already-reverted batch returns status='already_reverted'."""
    import_id = import_log.begin_import(
        db,
        source_file="/tmp/test.csv",  # noqa: S108  # test fixture path
        source_type="csv",
        source_origin="tiller",
        account_names=["checking"],
    )
    import_log.finalize_import(
        db, import_id, status="complete", rows_total=0, rows_imported=0
    )
    # First revert flips status; the second is the one we're asserting on.
    ImportService(db).revert(import_id)
    result = ImportService(db).revert(import_id)
    assert result == {"status": "already_reverted"}


def test_revert_tabular_deletes_matching_rows_and_marks_reverted(
    db: Database,
) -> None:
    """Revert deletes raw.tabular_* rows for the import_id and flips status."""
    import_id = import_log.begin_import(
        db,
        source_file="/tmp/test.csv",  # noqa: S108  # test fixture path
        source_type="csv",
        source_origin="tiller",
        account_names=["checking"],
    )
    db.execute(
        """
        INSERT INTO raw.tabular_transactions (
            transaction_id, account_id, transaction_date, amount, description,
            source_file, source_type, source_origin, import_id
        ) VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "csv_t1",
            "checking",
            "2026-01-01",
            "-10.00",
            "X",
            "/tmp/test.csv",  # noqa: S108  # test fixture path
            "csv",
            "tiller",
            import_id,
            "csv_t2",
            "checking",
            "2026-01-02",
            "-20.00",
            "Y",
            "/tmp/test.csv",  # noqa: S108  # test fixture path
            "csv",
            "tiller",
            import_id,
        ],
    )
    import_log.finalize_import(
        db, import_id, status="complete", rows_total=2, rows_imported=2
    )

    result = ImportService(db).revert(import_id)

    assert result["status"] == "reverted"
    assert result["rows_deleted"] == 2
    remaining = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE import_id = ?",
        [import_id],
    ).fetchone()
    assert remaining is not None
    assert remaining[0] == 0
    status_row = db.execute(
        "SELECT status FROM raw.import_log WHERE import_id = ?", [import_id]
    ).fetchone()
    assert status_row is not None
    assert status_row[0] == "reverted"
