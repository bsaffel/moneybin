"""Unit tests for DoctorService — pipeline invariant checks."""

from __future__ import annotations

import dataclasses
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.services.doctor_service import (
    DoctorReport,
    DoctorService,
    InvariantResult,
)
from tests.moneybin.db_helpers import create_core_tables


@pytest.mark.unit
def test_invariant_result_pass_has_no_detail() -> None:
    result = InvariantResult(
        name="test_audit",
        status="pass",
        detail=None,
        affected_ids=[],
    )
    assert result.status == "pass"
    assert result.detail is None
    assert result.affected_ids == []


@pytest.mark.unit
def test_invariant_result_fail_has_detail() -> None:
    result = InvariantResult(
        name="test_audit",
        status="fail",
        detail="2 violations found",
        affected_ids=["abc123"],
    )
    assert result.status == "fail"
    assert result.detail == "2 violations found"
    assert result.affected_ids == ["abc123"]


@pytest.mark.unit
def test_invariant_result_is_frozen() -> None:
    result = InvariantResult(name="x", status="pass", detail=None, affected_ids=[])
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.name = "y"  # type: ignore[misc]


@pytest.mark.unit
def test_doctor_report_holds_invariants() -> None:
    r = InvariantResult(name="a", status="pass", detail=None, affected_ids=[])
    report = DoctorReport(invariants=[r], transaction_count=42)
    assert len(report.invariants) == 1
    assert report.transaction_count == 42


@pytest.mark.unit
def test_doctor_report_is_frozen() -> None:
    report = DoctorReport(invariants=[], transaction_count=0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        report.transaction_count = 1  # type: ignore[misc]


# ---------------------------------------------------------------------------
# DoctorService tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def doctor_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Minimal DB with core tables for DoctorService tests."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "doctor.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    create_core_tables(database)
    # Seed one valid account and two transactions (both resolve)
    database.execute("""
        INSERT INTO core.dim_accounts (
            account_id, routing_number, account_type, institution_name,
            institution_fid, source_type, source_file, extracted_at, loaded_at,
            updated_at, display_name, iso_currency_code,
            archived, include_in_net_worth
        ) VALUES ('ACC1', '111', 'CHECKING', 'Bank', 'fid', 'ofx',
                  'a.qfx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                  CURRENT_TIMESTAMP, 'Bank CHECKING', 'USD', FALSE, TRUE)
    """)  # noqa: S608 — test input, not user data
    database.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month,
            transaction_year_quarter
        ) VALUES
        ('T1', 'ACC1', '2026-01-01', -50.00, 50.00, 'expense', 'Coffee',
         'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         2026, 1, 1, 3, '2026-01', '2026-Q1'),
        ('T2', 'ACC1', '2026-01-02', 1000.00, 1000.00, 'income', 'Paycheck',
         'CREDIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         2026, 1, 2, 4, '2026-01', '2026-Q1')
    """)  # noqa: S608 — test input, not user data
    db_module._database_instance = database  # type: ignore[attr-defined]
    yield database
    db_module._database_instance = None  # type: ignore[attr-defined]
    database.close()


def _make_mock_ctx(audits: dict[str, tuple[str, str]]) -> Any:
    """Build a mock SQLMesh Context where each audit renders to given SQL."""
    mock_ctx = MagicMock()
    audit_mocks = {}
    for name, (sql, _dialect) in audits.items():
        audit = MagicMock()
        audit.name = name
        audit.render_audit_query.return_value.sql.return_value = sql
        audit_mocks[name] = audit
    mock_ctx.standalone_audits = audit_mocks
    return mock_ctx


_FK_SQL = """
    SELECT t.transaction_id
    FROM core.fct_transactions AS t
    LEFT JOIN core.dim_accounts AS a ON t.account_id = a.account_id
    WHERE a.account_id IS NULL
    ORDER BY t.transaction_id
"""  # noqa: S608 — test SQL

_SIGN_SQL = """
    SELECT transaction_id
    FROM core.fct_transactions
    WHERE amount = 0 OR amount IS NULL
    ORDER BY transaction_id
"""  # noqa: S608 — test SQL

_TRANSFER_SQL = """
    SELECT bt.debit_transaction_id
    FROM core.bridge_transfers AS bt
    JOIN core.fct_transactions AS d ON bt.debit_transaction_id = d.transaction_id
    JOIN core.fct_transactions AS c ON bt.credit_transaction_id = c.transaction_id
    WHERE ABS(d.amount + c.amount) > 0.01
    ORDER BY bt.debit_transaction_id
"""  # noqa: S608 — test SQL

_CLEAN_AUDITS = {
    "fct_transactions_fk_integrity": (_FK_SQL, "duckdb"),
    "fct_transactions_sign_convention": (_SIGN_SQL, "duckdb"),
    "bridge_transfers_balanced": (_TRANSFER_SQL, "duckdb"),
}


@pytest.mark.unit
def test_transaction_count_returns_correct_count(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all(verbose=False)
    assert report.transaction_count == 2


@pytest.mark.unit
def test_fk_integrity_passes_clean_data(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all()
    fk = next(r for r in report.invariants if r.name == "fct_transactions_fk_integrity")
    assert fk.status == "pass"
    assert fk.detail is None
    assert fk.affected_ids == []


@pytest.mark.unit
def test_fk_integrity_fails_orphaned_account(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Insert a transaction with an account_id not in dim_accounts
    doctor_db.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month, transaction_year_quarter
        ) VALUES
        ('ORPHAN', 'GHOST_ACC', '2026-02-01', -10.00, 10.00, 'expense', 'Ghost',
         'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         2026, 2, 1, 6, '2026-02', '2026-Q1')
    """)  # noqa: S608 — test input, not user data
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all(verbose=True)
    fk = next(r for r in report.invariants if r.name == "fct_transactions_fk_integrity")
    assert fk.status == "fail"
    assert "1 transaction" in (fk.detail or "") or "violation" in (fk.detail or "")
    assert "ORPHAN" in fk.affected_ids


@pytest.mark.unit
def test_sign_convention_fails_zero_amount(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    doctor_db.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month, transaction_year_quarter
        ) VALUES
        ('ZERO', 'ACC1', '2026-03-01', 0.00, 0.00, 'expense', 'Zero',
         'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         2026, 3, 1, 6, '2026-03', '2026-Q1')
    """)  # noqa: S608 — test input, not user data
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all(verbose=True)
    sign = next(r for r in report.invariants if r.name == "fct_transactions_sign_convention")
    assert sign.status == "fail"
    assert "ZERO" in sign.affected_ids


@pytest.mark.unit
def test_verbose_false_returns_empty_affected_ids(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Insert orphaned transaction to cause a failure
    doctor_db.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month, transaction_year_quarter
        ) VALUES
        ('ORPHAN2', 'NO_ACC', '2026-04-01', -5.00, 5.00, 'expense', 'Ghost',
         'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         2026, 4, 1, 2, '2026-04', '2026-Q2')
    """)  # noqa: S608 — test input, not user data
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all(verbose=False)
    fk = next(r for r in report.invariants if r.name == "fct_transactions_fk_integrity")
    assert fk.status == "fail"
    assert fk.affected_ids == []  # verbose=False → no IDs


@pytest.mark.unit
def test_staging_coverage_is_always_skipped(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all()
    staging = next(r for r in report.invariants if r.name == "staging_coverage")
    assert staging.status == "skipped"
    assert staging.detail is not None


@pytest.mark.unit
def test_categorization_coverage_passes_when_all_categorized(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Set category on all non-transfer transactions
    doctor_db.execute("""
        UPDATE core.fct_transactions
        SET category = 'Food & Drink'
        WHERE transaction_id IN ('T1', 'T2')
    """)  # noqa: S608 — test input, not user data
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all()
    cat = next(r for r in report.invariants if r.name == "categorization_coverage")
    assert cat.status == "pass"


@pytest.mark.unit
def test_categorization_coverage_warns_when_below_50pct(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # T1 and T2 have no category (default NULL) — 0% categorized → warn
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all()
    cat = next(r for r in report.invariants if r.name == "categorization_coverage")
    assert cat.status == "warn"
    assert "uncategorized" in (cat.detail or "").lower()
