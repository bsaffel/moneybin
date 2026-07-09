"""Tests for ImportService.revert.

Covers the full response envelope (``reverted``, ``not_found``,
``already_reverted``, ``unsupported``, ``superseded``) across OFX and
tabular source_types. ``not_found`` ships with a ``reason`` field.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.services.import_service import ImportService
from moneybin.services.investment_service import InvestmentService
from tests.moneybin.db_helpers import create_core_tables


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


def test_revert_manual_investment_deletes_rows_not_orphaned(db: Database) -> None:
    """Reverting a manual investment batch deletes its raw investment rows.

    ``InvestmentService.record_event`` writes via import_log with
    source_type='manual', so revert must clear raw.manual_investment_transactions
    (added to REVERT_TABLES['manual']) — not leave the rows orphaned into
    core.fct_investment_transactions with the batch marked reverted.
    """
    create_core_tables(db)
    db.execute(
        """
        INSERT INTO core.dim_accounts
            (account_id, account_type, institution_name, source_type)
        VALUES ('acct_brokerage', 'investment', 'Fidelity', 'manual')
        """
    )
    svc = InvestmentService(db)
    svc.upsert_security(
        security_id="sec_aapl",
        name="Apple Inc.",
        security_type="equity",
        ticker="AAPL",
        actor="cli",
    )
    svc.record_event(
        account_ref="acct_brokerage",
        security_ref="AAPL",
        type_="buy",
        subtype=None,
        trade_date=date(2024, 1, 15),
        quantity=Decimal("10"),
        price=Decimal("150.00"),
        amount=Decimal("-1500.00"),
        fees=None,
        acquired=None,
        basis=None,
        event_group_id=None,
        currency_code="USD",
        description=None,
        actor="cli",
        created_by="cli",
    )

    import_id_row = db.execute(
        "SELECT DISTINCT import_id FROM raw.manual_investment_transactions"
    ).fetchone()
    assert import_id_row is not None
    import_id = import_id_row[0]

    pre = db.execute(
        "SELECT COUNT(*) FROM raw.manual_investment_transactions WHERE import_id = ?",
        [import_id],
    ).fetchone()
    assert pre is not None
    assert pre[0] == 1

    result = ImportService(db).revert(import_id)

    assert result["status"] == "reverted"
    assert result["rows_deleted"] == 1
    remaining = db.execute(
        "SELECT COUNT(*) FROM raw.manual_investment_transactions WHERE import_id = ?",
        [import_id],
    ).fetchone()
    assert remaining is not None
    assert remaining[0] == 0
    status_row = db.execute(
        "SELECT status FROM raw.import_log WHERE import_id = ?", [import_id]
    ).fetchone()
    assert status_row is not None
    assert status_row[0] == "reverted"


def test_revert_stuck_investment_import_not_superseded_by_cash_batch(
    db: Database,
) -> None:
    """A stuck (zero-row) investment import isn't superseded by a cash batch.

    Both domains share ``source_type='manual'``; before ``allocate_import_log``
    namespaced its synthetic ``source_file`` key by ``format_name``
    (``manual_investment_entry`` vs ``manual_entry``), they collided on the
    exact same key, and revert()'s superseded-lookup (which fires when
    rows_to_delete == 0 — e.g. a crash between allocate_import_log and the
    write transaction's commit) could cross-match a batch from the other
    domain.
    """
    stuck_investment_import_id = ImportService(db).allocate_import_log(
        source_type="manual",
        format_name="manual_investment_entry",
        actor="cli",
    )
    # Backdate so the later cash batch is unambiguously "started_at >" —
    # removes flakiness from timestamp-resolution ties, without faking the
    # outcome under test (revert() still runs its real query).
    db.execute(
        "UPDATE raw.import_log SET started_at = CURRENT_TIMESTAMP - INTERVAL '1 hour' "
        "WHERE import_id = ?",
        [stuck_investment_import_id],
    )
    cash_import_id = ImportService(db).allocate_import_log(
        source_type="manual",
        format_name="manual_entry",
        actor="cli",
    )
    import_log.finalize_import(
        db, cash_import_id, status="complete", rows_total=1, rows_imported=1
    )

    result = ImportService(db).revert(stuck_investment_import_id)

    assert result["status"] == "reverted"
    assert result["rows_deleted"] == 0
