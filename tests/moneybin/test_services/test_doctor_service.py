"""Unit tests for DoctorService — pipeline invariant checks."""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin.database import SQLMESH_ROOT, Database
from moneybin.services.doctor_service import (
    DoctorReport,
    DoctorService,
    InvariantResult,
)
from moneybin.services.transform_service import TransformService
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
def doctor_db(db: Database) -> Database:
    """Minimal DB with core tables for DoctorService tests."""
    create_core_tables(db)
    # Seed one valid account and two transactions (both resolve)
    db.execute("""
        INSERT INTO core.dim_accounts (
            account_id, routing_number, account_type, institution_name,
            institution_fid, source_type, source_file, extracted_at, loaded_at,
            updated_at, display_name, iso_currency_code,
            archived, include_in_net_worth
        ) VALUES ('ACC1', '111', 'CHECKING', 'Bank', 'fid', 'ofx',
                  'a.qfx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                  CURRENT_TIMESTAMP, 'Bank CHECKING', 'USD', FALSE, TRUE)
    """)  # noqa: S608 — test input, not user data
    db.execute("""
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
    return db


def _seed_prep_unioned(db: Database, row_count: int) -> None:
    """Create prep schema with the matched view and insert ``row_count`` rows.

    Creates the full prep layer (unioned table + matched view from the real
    model SQL) so that the dedup_reconciliation formula — which reads from
    prep.int_transactions__matched — works correctly. prep.* is SQLMesh-managed
    in production and absent from the unit-test DB, so tests exercising the
    active check create it here.

    Seeded rows use IDs ``u0``, ``u1``, … with ``source_type='ofx'`` so that
    the ``_insert_match_decision`` helper (which pairs ``u0`` + ``u1``) resolves
    to real rows in the matched view.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(_UNIONED_FULL_DDL)
    raw = _MATCHED_MODEL_FILE.read_text()
    body = re.sub(r"^MODEL\s*\(.*?\);\s*", "", raw, flags=re.DOTALL).strip()
    db.execute(f"CREATE OR REPLACE VIEW prep.int_transactions__matched AS\n{body}")  # noqa: S608 — model body from repo file, not user input
    for i in range(row_count):
        db.execute(
            """
            INSERT INTO prep.int_transactions__unioned (
                source_transaction_id, account_id, source_account_key,
                transaction_date, amount, description, currency_code,
                source_type, source_origin, is_pending
            ) VALUES (?, 'ACC1', 'ACC1', '2026-01-01', -50.00, 'Test', 'USD', 'ofx', 'bank', false)
            """,  # noqa: S608 — test input, not user data
            [f"u{i}"],
        )


def _insert_match_decision(
    db: Database,
    *,
    match_id: str,
    match_type: str = "dedup",
    match_status: str = "accepted",
    reversed_at: str | None = None,
) -> None:
    """Insert one app.match_decisions row pairing u0 and u1 (both source_type='ofx').

    IDs must correspond to real rows in prep.int_transactions__unioned so that
    the matched view can form a group. All dedup tests that call this helper
    seed at least 2 rows via _seed_prep_unioned, so u0 and u1 always exist.
    """
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at, reversed_at
        ) VALUES (?, 'u0', 'ofx', 'bank', 'u1', 'ofx', 'bank', 'ACC1',
                  0.95, '{}', ?, '3', NULL, ?, NULL, 'auto', CURRENT_TIMESTAMP, ?)
        """,  # noqa: S608 — test input, not user data
        [match_id, match_type, match_status, reversed_at],
    )


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
    WHERE amount IS NULL
    ORDER BY transaction_id
"""  # noqa: S608 — test SQL; mirrors fct_transactions_sign_convention.sql (zero is a modeled direction, not a violation)

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
def test_sign_convention_fails_null_amount(
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
        ('NULL_AMT', 'ACC1', '2026-03-01', NULL, NULL, 'expense', 'Unresolved',
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
    sign = next(
        r for r in report.invariants if r.name == "fct_transactions_sign_convention"
    )
    assert sign.status == "fail"
    assert "NULL_AMT" in sign.affected_ids


@pytest.mark.unit
def test_sign_convention_passes_zero_amount(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A $0.00 transaction is a modeled 'zero' direction, not a defect.

    Regression pin for the audit-revival fix: core.fct_transactions models
    zero as a legitimate third transaction_direction (a waived fee, a $0
    authorization), so the sign-convention audit must not flag it.
    """
    doctor_db.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month, transaction_year_quarter
        ) VALUES
        ('ZERO', 'ACC1', '2026-03-01', 0.00, 0.00, 'zero', 'Waived fee',
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
    sign = next(
        r for r in report.invariants if r.name == "fct_transactions_sign_convention"
    )
    assert sign.status == "pass"
    assert sign.affected_ids == []


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


def _dedup_result(db: Database, monkeypatch: pytest.MonkeyPatch) -> InvariantResult:
    """Run the full doctor report (SQLMesh mocked) and return the dedup invariant.

    Goes through the public ``run_all()`` like every other test in this file, so
    the dedup_reconciliation wiring is exercised end-to-end.
    """
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    report = DoctorService(db).run_all()
    return next(r for r in report.invariants if r.name == "dedup_reconciliation")


@pytest.mark.unit
def test_dedup_reconciliation_passes_when_collapse_matches_decisions(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 3 imported rows, 2 core rows (T1, T2 from fixture), 1 accepted dedup
    # decision → exactly 1 row absorbed → 3 - 2 == 1. PASS.
    _seed_prep_unioned(doctor_db, row_count=3)
    _insert_match_decision(doctor_db, match_id="m1")
    result = _dedup_result(doctor_db, monkeypatch)
    assert result.status == "pass"
    assert result.detail is None


@pytest.mark.unit
def test_dedup_reconciliation_fails_when_rows_collapse_without_decision(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 3 imported rows collapse to 2 core rows, but no dedup decision explains
    # it → a leak (rows vanished without a recorded reason). FAIL.
    _seed_prep_unioned(doctor_db, row_count=3)
    result = _dedup_result(doctor_db, monkeypatch)
    assert result.status == "fail"
    assert result.detail is not None


@pytest.mark.unit
def test_dedup_reconciliation_fails_when_decision_did_not_collapse(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 2 imported rows, 2 core rows (nothing collapsed), but a dedup decision
    # says one pair should have merged → an un-applied match. FAIL.
    _seed_prep_unioned(doctor_db, row_count=2)
    _insert_match_decision(doctor_db, match_id="m1")
    result = _dedup_result(doctor_db, monkeypatch)
    assert result.status == "fail"


@pytest.mark.unit
def test_dedup_reconciliation_skipped_when_prep_layer_absent(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No prep.int_transactions__unioned (transform not yet run) → skipped.
    result = _dedup_result(doctor_db, monkeypatch)
    assert result.status == "skipped"
    assert result.detail is not None


@pytest.mark.unit
def test_dedup_reconciliation_excludes_inactive_and_transfer_decisions(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 2 imported rows, 2 core rows (no collapse). A rejected dedup, a reversed
    # dedup, and an accepted transfer must all be excluded from the expected
    # absorbed count → expected 0 → 2 - 2 == 0. PASS.
    _seed_prep_unioned(doctor_db, row_count=2)
    _insert_match_decision(doctor_db, match_id="rej", match_status="rejected")
    _insert_match_decision(
        doctor_db,
        match_id="rev",
        match_status="accepted",
        reversed_at="2026-01-01 00:00:00",
    )
    _insert_match_decision(doctor_db, match_id="xfr", match_type="transfer")
    result = _dedup_result(doctor_db, monkeypatch)
    assert result.status == "pass"


@pytest.mark.unit
def test_dedup_reconciliation_fails_clearly_when_core_exceeds_staging(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # core has 2 rows (T1, T2) but staging has 0 → a row reached core without
    # passing through staging. observed_absorbed would be negative; the detail
    # must name that impossible direction, never report a nonsensical "-2".
    _seed_prep_unioned(doctor_db, row_count=0)
    result = _dedup_result(doctor_db, monkeypatch)
    assert result.status == "fail"
    assert "more rows than staging" in (result.detail or "")
    assert "-2" not in (result.detail or "")


_MATCHED_MODEL_FILE = SQLMESH_ROOT / "models" / "prep" / "int_transactions__matched.sql"

_UNIONED_FULL_DDL = """\
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
    merchant_entity_id    VARCHAR,
    memo                  VARCHAR,
    category              VARCHAR,
    subcategory           VARCHAR,
    category_detailed     VARCHAR,
    plaid_category        VARCHAR,
    category_confidence   VARCHAR,
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


def _create_matched_view(db: Database) -> None:
    """Create prep.int_transactions__unioned + the matched view from the model SQL."""
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(_UNIONED_FULL_DDL)
    raw = _MATCHED_MODEL_FILE.read_text()
    body = re.sub(r"^MODEL\s*\(.*?\);\s*", "", raw, flags=re.DOTALL).strip()
    db.execute(f"CREATE OR REPLACE VIEW prep.int_transactions__matched AS\n{body}")  # noqa: S608 — model body from repo file, not user input


def _insert_unioned_row_for_matched(
    db: Database,
    *,
    source_transaction_id: str,
    source_type: str,
    account_id: str,
) -> None:
    """Insert a minimal row into prep.int_transactions__unioned."""
    db.execute(
        """
        INSERT INTO prep.int_transactions__unioned (
            source_transaction_id, account_id, source_account_key,
            transaction_date, amount, description, currency_code,
            source_type, source_origin, is_pending
        ) VALUES (?, ?, ?, '2026-01-01', -50.00, 'Test', 'USD', ?, 'bank', false)
        """,  # noqa: S608 — test input, not user data
        [source_transaction_id, account_id, account_id, source_type],
    )


def _insert_cycle_match_decision(
    db: Database,
    *,
    match_id: str,
    stid_a: str,
    st_a: str,
    stid_b: str,
    st_b: str,
    account_id: str,
) -> None:
    """Insert an accepted dedup match decision for the cycle test."""
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES (?, ?, ?, 'bank', ?, ?, 'bank', ?, 0.95, '{}',
                  'dedup', '3', NULL, 'accepted', 'test', 'auto', CURRENT_TIMESTAMP)
        """,  # noqa: S608 — test input, not user data
        [match_id, stid_a, st_a, stid_b, st_b, account_id],
    )


@pytest.mark.unit
def test_dedup_reconciliation_counts_group_size_minus_one(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cyclic accepted edges: 3 edges over a 3-node group must report absorbed=2.

    Hand-derived counts:
      - prep.int_transactions__unioned: 5 rows (2 for T1/T2, 3 for the cycle group)
      - core.fct_transactions: 3 rows (T1, T2 from fixture + 1 merged for cycle group)
      - observed_absorbed = raw_total - core_count = 5 - 3 = 2
      - prep.int_transactions__matched: 3 rows in the cycle group share 1 match_group_id
      - dedup_absorbed (new formula) = COUNT(*) - COUNT(DISTINCT match_group_id) = 3 - 1 = 2
      - 2 == 2 → PASS

    Under the OLD formula COUNT(decisions) = 3 ≠ 2 → FAIL. The cycle (A-B, B-C, A-C)
    has 3 edges but absorbs only 2 rows. The new Σ(group_size-1) formula is exact.
    """
    # Build prep schema with the real matched view so match_group_id is populated.
    _create_matched_view(doctor_db)

    # Seed 2 "background" unioned rows for T1/T2 (they have no match decisions,
    # so match_group_id stays NULL — they don't affect dedup_absorbed).
    _insert_unioned_row_for_matched(
        doctor_db, source_transaction_id="ofx_t1", source_type="ofx", account_id="ACC1"
    )
    _insert_unioned_row_for_matched(
        doctor_db, source_transaction_id="ofx_t2", source_type="ofx", account_id="ACC1"
    )

    # Seed 3 unioned rows for the cycle group (A, B, C — same account).
    # source_transaction_ids chosen to avoid collisions with T1/T2 stubs above.
    _insert_unioned_row_for_matched(
        doctor_db, source_transaction_id="csv_aaa", source_type="csv", account_id="ACC1"
    )
    _insert_unioned_row_for_matched(
        doctor_db, source_transaction_id="csv_bbb", source_type="csv", account_id="ACC1"
    )
    _insert_unioned_row_for_matched(
        doctor_db, source_transaction_id="csv_ccc", source_type="csv", account_id="ACC1"
    )

    # 3 accepted dedup decisions forming a triangle: A-B, B-C, A-C.
    _insert_cycle_match_decision(
        doctor_db,
        match_id="m_ab",
        stid_a="csv_aaa",
        st_a="csv",
        stid_b="csv_bbb",
        st_b="csv",
        account_id="ACC1",
    )
    _insert_cycle_match_decision(
        doctor_db,
        match_id="m_bc",
        stid_a="csv_bbb",
        st_a="csv",
        stid_b="csv_ccc",
        st_b="csv",
        account_id="ACC1",
    )
    _insert_cycle_match_decision(
        doctor_db,
        match_id="m_ac",
        stid_a="csv_aaa",
        st_a="csv",
        stid_b="csv_ccc",
        st_b="csv",
        account_id="ACC1",
    )

    # Add 1 merged core transaction for the 3-node group (3 prep rows → 1 core row).
    # raw_total=5, core_count=3 (T1, T2, merged) → observed_absorbed=2.
    doctor_db.execute(
        """
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month, transaction_year_quarter
        ) VALUES ('MERGED', 'ACC1', '2026-01-01', -50.00, 50.00, 'expense',
                  'Merged', 'DEBIT', false, 'USD', 'csv',
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                  2026, 1, 1, 3, '2026-01', '2026-Q1')
        """  # noqa: S608 — test input, not user data
    )

    result = _dedup_result(doctor_db, monkeypatch)
    assert result.status == "pass"


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
    # The recipe registry populates recovery_actions for failing/warning
    # invariants — categorization_coverage emits a single suggested
    # transactions_categorize_run action that an agent can dispatch.
    assert cat.recovery_actions is not None
    assert len(cat.recovery_actions) == 1
    assert cat.recovery_actions[0].tool == "transactions_categorize_run"
    assert cat.recovery_actions[0].confidence == "suggested"


@pytest.mark.unit
def test_run_all_returns_expected_invariants(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all()
    # 3 sqlmesh audits + dedup_reconciliation + categorization + 27 app.* integrity
    # checks (audit coverage for user_categories / category_overrides /
    # gsheet_connections / user_merchants / categorization_rules / proposed_rules /
    # transaction_categories / account_settings / balance_assertions / budgets /
    # tabular_formats / match_decisions / imports / pdf_formats / securities /
    # lot_selections + user_categories uniqueness + user_merchants orphans +
    # proposed_rules->rule FK + transaction_categories->fct FK +
    # account_settings->dim_accounts FK + balance_assertions->dim_accounts FK +
    # budgets->dim_categories FK + match_decisions->dim_accounts FK +
    # pdf_formats recipe-validity / bounds / fingerprint-shape) +
    # orphan_app_state (PR4: scans transaction_notes / transaction_tags vs
    # core) + account_links / account_link_decisions / transaction_id_aliases
    # audit coverage (M1S) + 8 investment reconciliation checks (T17: staging
    # rejects, opening-lot review, unmodeled legs, holdings divergence,
    # source overlap, unresolved securities, unreported holdings, phantom
    # holdings).
    assert len(report.invariants) == 44
    names = [r.name for r in report.invariants]
    assert "fct_transactions_fk_integrity" in names
    assert "fct_transactions_sign_convention" in names
    assert "bridge_transfers_balanced" in names
    assert "dedup_reconciliation" in names
    assert "categorization_coverage" in names
    assert "app_audit_coverage_user_categories" in names
    assert "app_audit_coverage_category_overrides" in names
    assert "app_audit_coverage_gsheet_connections" in names
    assert "app_audit_coverage_account_settings" in names
    assert "app_audit_coverage_balance_assertions" in names
    assert "app_audit_coverage_budgets" in names
    assert "app_audit_coverage_tabular_formats" in names
    assert "app_audit_coverage_match_decisions" in names
    assert "app_audit_coverage_imports" in names
    assert "app_audit_coverage_securities" in names
    assert "app_audit_coverage_lot_selections" in names
    assert "app_user_categories_uniqueness" in names
    assert "app_account_settings_account_fk" in names
    assert "app_balance_assertions_account_fk" in names
    assert "app_budgets_category_fk" in names
    assert "app_match_decisions_account_fk" in names
    assert "orphan_app_state" in names


@pytest.mark.unit
def test_fk_detail_message_contains_count(
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
        ('BAD1', 'NONE', '2026-05-01', -1.00, 1.00, 'expense', 'Bad',
         'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         2026, 5, 1, 4, '2026-05', '2026-Q2'),
        ('BAD2', 'NONE', '2026-05-02', -2.00, 2.00, 'expense', 'Bad2',
         'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         2026, 5, 2, 5, '2026-05', '2026-Q2')
    """)  # noqa: S608 — test input, not user data
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all()
    fk = next(r for r in report.invariants if r.name == "fct_transactions_fk_integrity")
    assert fk.status == "fail"
    assert "2" in (fk.detail or "")


@pytest.mark.unit
def test_bridge_transfers_balanced_fails_unbalanced_pair(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Insert a debit+credit pair where |debit.amount + credit.amount| > 0.01.
    # Debit: -100.00, Credit: +99.00 → net = -1.00 → imbalanced.
    doctor_db.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month, transaction_year_quarter
        ) VALUES
        ('DEBIT1', 'ACC1', '2026-04-01', -100.00, 100.00, 'expense', 'Transfer out',
         'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         2026, 4, 1, 2, '2026-04', '2026-Q2'),
        ('CREDIT1', 'ACC1', '2026-04-01', 99.00, 99.00, 'income', 'Transfer in',
         'CREDIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         2026, 4, 1, 2, '2026-04', '2026-Q2')
    """)  # noqa: S608 — test input, not user data
    doctor_db.execute("""
        INSERT INTO core.bridge_transfers (
            transfer_id, debit_transaction_id, credit_transaction_id,
            date_offset_days, amount
        ) VALUES ('XFR1', 'DEBIT1', 'CREDIT1', 0, 100.00)
    """)  # noqa: S608 — test input, not user data
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all(verbose=True)
    xfr = next(r for r in report.invariants if r.name == "bridge_transfers_balanced")
    assert xfr.status == "fail"
    assert "DEBIT1" in xfr.affected_ids


@pytest.mark.unit
def test_sqlmesh_discovery_failure_emits_skipped_invariant(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    @contextmanager
    def _failing_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        msg = "SQLMesh config not found"
        raise RuntimeError(msg)
        yield  # unreachable; satisfies the generator type @contextmanager requires

    monkeypatch.setattr(
        "moneybin.services.doctor_service.sqlmesh_context", _failing_ctx
    )
    svc = DoctorService(doctor_db)
    report = svc.run_all()
    skipped = next(
        (r for r in report.invariants if r.name == "sqlmesh_audits_unavailable"), None
    )
    assert skipped is not None
    assert skipped.status == "skipped"
    assert "SQLMesh" in (skipped.detail or "")


# ---------------------------------------------------------------------------
# Investment reconciliation checks (T17) — each surfaces a deliberate
# upstream gap (split_underivable/unmapped_subtype staging rejects, declined
# opening-lot bootstraps, unmodeled short/option/catch-all legs,
# holdings-snapshot divergence in both directions (broker-unreported and
# MoneyBin-phantom), manual+Plaid source overlap, and unresolved provider
# securities) rather than letting the pipeline silently drop them.
#
# Exercised through the public run_all() — like every other check in this
# file (_dedup_result precedent above) — never by calling a private _run_*
# method directly: pyright's strict reportPrivateUsage forbids it.
# ---------------------------------------------------------------------------


def _investment_result(
    db: Database, monkeypatch: pytest.MonkeyPatch, name: str
) -> InvariantResult:
    """Run the full doctor report (SQLMesh mocked) and return the named investment invariant."""
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    report = DoctorService(db).run_all()
    return next(r for r in report.invariants if r.name == name)


@pytest.mark.unit
def test_staging_rejects_warn(db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_transactions "
        "(investment_transaction_id VARCHAR, review_reason VARCHAR)"
    )
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_transactions VALUES "
        "('itx_1', 'split_underivable'), ('itx_2', NULL)"
    )
    result = _investment_result(db, monkeypatch, "investment_staging_rejects")
    assert result.status == "warn"
    assert result.affected_ids == ["itx_1"]


@pytest.mark.unit
def test_staging_rejects_pass_when_no_review_reasons(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_transactions "
        "(investment_transaction_id VARCHAR, review_reason VARCHAR)"
    )
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_transactions VALUES ('itx_1', NULL)"
    )
    result = _investment_result(db, monkeypatch, "investment_staging_rejects")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_opening_lot_review_warn(db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__opening_lot_review "
        "(account_id VARCHAR, security_id VARCHAR, source_security_key VARCHAR, "
        "reason VARCHAR)"
    )
    db.execute(
        "INSERT INTO prep.stg_plaid__opening_lot_review VALUES "
        "('acc1', 'sec1', 'plaid_sec1', 'short_or_nonpositive')"
    )
    result = _investment_result(db, monkeypatch, "investment_opening_lot_review")
    assert result.status == "warn"
    # A bound security_id wins over the provider key.
    assert result.affected_ids == ["acc1:sec1 (short_or_nonpositive)"]


@pytest.mark.unit
def test_opening_lot_review_unbound_security_shows_provider_key(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unbound security (security_id NULL) must render its provider key, not 'None'.

    The view carries source_security_key precisely so the raw provider row
    stays addressable when the canonical id never resolved — the same
    fallback ``_run_investment_unreported_holdings`` already applies.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__opening_lot_review "
        "(account_id VARCHAR, security_id VARCHAR, source_security_key VARCHAR, "
        "reason VARCHAR)"
    )
    db.execute(
        "INSERT INTO prep.stg_plaid__opening_lot_review VALUES "
        "('acc1', NULL, 'plaid_sec_unbound', 'short_or_nonpositive')"
    )
    result = _investment_result(db, monkeypatch, "investment_opening_lot_review")
    assert result.status == "warn"
    assert result.affected_ids == ["acc1:plaid_sec_unbound (short_or_nonpositive)"]


@pytest.mark.unit
def test_unmodeled_legs_surface_short_option_and_catchall(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every provider_subtype the staging CASE maps to NULL-quantity 'other' must surface.

    Not just short legs (buy to cover/sell short) — option legs
    (assignment/exercise/expire) and other catch-all events (adjustment/loan
    payment/rebalance) get IDENTICAL treatment in
    stg_plaid__investment_transactions.sql's CASE, and this check is the
    only place any of them surface (ledger_include = TRUE, review_reason =
    NULL). A plain 'buy' must never be flagged.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.fct_investment_transactions "
        "(investment_transaction_id VARCHAR, provider_subtype VARCHAR)"
    )
    db.execute(
        "INSERT INTO core.fct_investment_transactions VALUES "
        "('itx_cover', 'buy to cover'), ('itx_short', 'sell short'), "
        "('itx_assign', 'assignment'), ('itx_exercise', 'exercise'), "
        "('itx_expire', 'expire'), ('itx_adjust', 'adjustment'), "
        "('itx_loan', 'loan payment'), ('itx_rebalance', 'rebalance'), "
        "('itx_buy', 'buy')"
    )
    result = _investment_result(db, monkeypatch, "investment_unmodeled_legs")
    assert result.status == "warn"
    assert result.affected_ids == [
        "itx_adjust",
        "itx_assign",
        "itx_cover",
        "itx_exercise",
        "itx_expire",
        "itx_loan",
        "itx_rebalance",
        "itx_short",
    ]


@pytest.mark.unit
def test_unmodeled_legs_match_subtype_case_insensitively(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The check must normalize provider_subtype the way staging does (LOWER()).

    ``stg_plaid__investment_transactions`` classifies on
    ``LOWER(COALESCE(subtype, ''))`` but preserves the raw string verbatim in
    ``provider_subtype``. A case-sensitive IN-list here misses exactly the rows
    the check exists to surface: an 'Assignment' still maps through the
    LOWER-based branch to NULL-quantity 'other' with no review_reason, so this
    check is its only surface — and it would report `pass`.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.fct_investment_transactions "
        "(investment_transaction_id VARCHAR, provider_subtype VARCHAR)"
    )
    db.execute(
        "INSERT INTO core.fct_investment_transactions VALUES "
        "('itx_assign', 'Assignment'), ('itx_short', 'SELL SHORT'), "
        "('itx_buy', 'Buy')"
    )
    result = _investment_result(db, monkeypatch, "investment_unmodeled_legs")
    assert result.status == "warn"
    assert result.affected_ids == ["itx_assign", "itx_short"]


@pytest.mark.unit
def test_holdings_divergence_warn(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.dim_holdings (account_id VARCHAR, security_id VARCHAR, "
        "quantity DECIMAL(28,10), cost_basis DECIMAL(18,2), "
        "provider_reported_quantity DECIMAL(28,10), provider_reported_cost_basis DECIMAL(18,2))"
    )
    db.execute(
        "INSERT INTO core.dim_holdings VALUES "
        "('a', 's_ok', 10, 100.00, 10, 100.00), "
        "('a', 's_bad', 10, 100.00, 8, 100.00)"
    )
    result = _investment_result(db, monkeypatch, "investment_holdings_divergence")
    assert result.status == "warn"
    assert result.affected_ids == ["a:s_bad"]


@pytest.mark.unit
def test_holdings_divergence_ignores_rows_broker_never_reported(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A position the broker's newest snapshot doesn't report is not a divergence.

    NULL ``provider_reported_quantity`` means the snapshot omits the position
    entirely — not a mismatch to flag (see ``dim_holdings.sql`` header).
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.dim_holdings (account_id VARCHAR, security_id VARCHAR, "
        "quantity DECIMAL(28,10), cost_basis DECIMAL(18,2), "
        "provider_reported_quantity DECIMAL(28,10), provider_reported_cost_basis DECIMAL(18,2))"
    )
    db.execute(
        "INSERT INTO core.dim_holdings VALUES "
        "('a', 's_unreported', 10, 100.00, NULL, NULL)"
    )
    result = _investment_result(db, monkeypatch, "investment_holdings_divergence")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_holdings_divergence_ignores_null_broker_cost_basis(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A broker snapshot that omits cost_basis (NULL) must not read as $0.

    ``COALESCE(provider_reported_cost_basis, 0)`` would turn "the broker
    didn't say" into "the broker says $0" and fire on every quantity-matched
    position whose connection doesn't report basis — the raw DDL declares
    cost_basis nullable and brokers routinely omit it. Quantity matches
    exactly here, so the only thing that could fire is the cost-basis leg.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.dim_holdings (account_id VARCHAR, security_id VARCHAR, "
        "quantity DECIMAL(28,10), cost_basis DECIMAL(18,2), "
        "provider_reported_quantity DECIMAL(28,10), provider_reported_cost_basis DECIMAL(18,2))"
    )
    db.execute(
        "INSERT INTO core.dim_holdings VALUES ('a', 's_ok', 10, 400.00, 10, NULL)"
    )
    result = _investment_result(db, monkeypatch, "investment_holdings_divergence")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_holdings_divergence_relative_tolerance_ignores_rounding_on_large_positions(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A sub-cent-relative mismatch on a large position must not fire.

    Many small DRIP/reinvest lots on a $4,000 position can accumulate a few
    cents of rounding drift against the broker's own rounding — a flat
    $0.01 absolute tolerance would false-positive on healthy large
    positions. The tolerance floor is GREATEST(0.01, 1bp of reported basis);
    here 1bp of $4,000.00 is $0.40, well above the $0.02 gap.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.dim_holdings (account_id VARCHAR, security_id VARCHAR, "
        "quantity DECIMAL(28,10), cost_basis DECIMAL(18,2), "
        "provider_reported_quantity DECIMAL(28,10), provider_reported_cost_basis DECIMAL(18,2))"
    )
    db.execute(
        "INSERT INTO core.dim_holdings VALUES ('a', 's_ok', 10, 4000.00, 10, 4000.02)"
    )
    result = _investment_result(db, monkeypatch, "investment_holdings_divergence")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_holdings_divergence_still_fires_beyond_relative_tolerance(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A genuine cost-basis mismatch beyond the relative floor must still fire.

    Same $4,000 position as the rounding-tolerance test above, but the gap
    ($0.50) exceeds the 1bp floor ($0.40) — the relative tolerance must not
    neuter real divergence detection.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.dim_holdings (account_id VARCHAR, security_id VARCHAR, "
        "quantity DECIMAL(28,10), cost_basis DECIMAL(18,2), "
        "provider_reported_quantity DECIMAL(28,10), provider_reported_cost_basis DECIMAL(18,2))"
    )
    db.execute(
        "INSERT INTO core.dim_holdings VALUES ('a', 's_bad', 10, 4000.00, 10, 4000.50)"
    )
    result = _investment_result(db, monkeypatch, "investment_holdings_divergence")
    assert result.status == "warn"
    assert result.affected_ids == ["a:s_bad"]


@pytest.mark.unit
def test_source_overlap_warn(db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transactions (
            investment_transaction_id, account_id, transaction_date, amount,
            source_file, source_origin
        ) VALUES ('p1', 'plaid_acc1', '2026-01-01', 100.00, 'sync_1', 'item1')
        """  # noqa: S608 — test input, not user data
    )
    db.execute(
        """
        INSERT INTO app.account_links (
            link_id, account_id, ref_kind, ref_value, source_type, source_origin,
            status, decided_by, decided_at
        ) VALUES ('lnk1', 'ACC1', 'source_native', 'plaid_acc1', 'plaid', 'item1',
                   'accepted', 'auto', CURRENT_TIMESTAMP)
        """  # noqa: S608 — test input, not user data
    )
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, type, trade_date, created_by
        ) VALUES ('manual_1', 'imp1', 'ACC1', 'buy', '2026-01-02', 'cli')
        """  # noqa: S608 — test input, not user data
    )
    result = _investment_result(db, monkeypatch, "investment_source_overlap")
    assert result.status == "warn"
    assert result.affected_ids == ["ACC1"]


@pytest.mark.unit
def test_source_overlap_pass_when_only_one_source(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transactions (
            investment_transaction_id, account_id, transaction_date, amount,
            source_file, source_origin
        ) VALUES ('p1', 'plaid_acc1', '2026-01-01', 100.00, 'sync_1', 'item1')
        """  # noqa: S608 — test input, not user data
    )
    result = _investment_result(db, monkeypatch, "investment_source_overlap")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_unresolved_securities_warn(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_transactions "
        "(investment_transaction_id VARCHAR, source_security_key VARCHAR)"
    )
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_transactions VALUES "
        "('itx_1', 'plaid_sec_abc'), ('itx_2', NULL)"
    )
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.fct_investment_transactions "
        "(investment_transaction_id VARCHAR, security_id VARCHAR)"
    )
    db.execute(
        "INSERT INTO core.fct_investment_transactions VALUES "
        "('itx_1', NULL), ('itx_2', NULL)"
    )
    result = _investment_result(db, monkeypatch, "investment_unresolved_securities")
    assert result.status == "warn"
    # itx_2 has no provider security key at all (a legitimate cash-only row,
    # e.g. deposit/withdrawal) — a NULL security_id there is not a gap.
    assert result.affected_ids == ["itx_1"]


@pytest.mark.unit
def test_unresolved_securities_pass_when_resolved(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_transactions "
        "(investment_transaction_id VARCHAR, source_security_key VARCHAR)"
    )
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_transactions VALUES "
        "('itx_1', 'plaid_sec_abc')"
    )
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.fct_investment_transactions "
        "(investment_transaction_id VARCHAR, security_id VARCHAR)"
    )
    db.execute(
        "INSERT INTO core.fct_investment_transactions VALUES ('itx_1', 'sec_canonical')"
    )
    result = _investment_result(db, monkeypatch, "investment_unresolved_securities")
    assert result.status == "pass"
    assert result.affected_ids == []


def _create_snapshot_receipts_table(db: Database) -> None:
    """Create the per-item, per-pull holdings-snapshot receipts staging view.

    Both holdings checks read "the newest snapshot for this item" from HERE,
    not from the presence of holdings rows: an item whose pull returned zero
    positions writes no holdings rows at all, so a row-derived newest snapshot
    silently stays the last NON-EMPTY one.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_holdings_snapshots "
        "(source_origin VARCHAR, source_file VARCHAR, holdings_date DATE, "
        "holdings_count INTEGER, extracted_at TIMESTAMP)"
    )


def _receipt(
    db: Database,
    source_origin: str,
    source_file: str,
    extracted_at: str,
    holdings_count: int,
) -> None:
    """Record that ``source_origin`` reported its holdings in pull ``source_file``."""
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings_snapshots "
        "(source_origin, source_file, holdings_date, holdings_count, extracted_at) "
        "VALUES (?, ?, CAST(? AS TIMESTAMP)::DATE, ?, CAST(? AS TIMESTAMP))",
        [source_origin, source_file, extracted_at, holdings_count, extracted_at],
    )


@pytest.mark.unit
def test_unreported_holdings_warn(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A broker-reported position with no ``core.dim_holdings`` row must surface.

    ``dim_holdings`` is ``positions LEFT JOIN provider_reported`` — a position
    MoneyBin has no lot for (here: an unbound security) produces no row there
    at all, so this direction can only be checked against the staging view
    directly (see ``dim_holdings.sql`` header).
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_holdings "
        "(account_id VARCHAR, security_id VARCHAR, source_security_key VARCHAR, "
        "quantity DECIMAL(28,10), "
        "source_origin VARCHAR, source_file VARCHAR, extracted_at TIMESTAMP)"
    )
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES "
        "('acc1', 'sec_known', 'plaid_sec_known', 5, 'item1', 'sync_1', "
        "'2026-01-01 00:00:00'), "
        "('acc1', NULL, 'plaid_sec_unbound', 3, 'item1', 'sync_1', "
        "'2026-01-01 00:00:00')"
    )
    _create_snapshot_receipts_table(db)
    _receipt(db, "item1", "sync_1", "2026-01-01 00:00:00", 2)
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.dim_holdings (account_id VARCHAR, security_id VARCHAR, "
        "provider_reported_quantity DECIMAL(28,10))"
    )
    db.execute("INSERT INTO core.dim_holdings VALUES ('acc1', 'sec_known', 5)")
    result = _investment_result(db, monkeypatch, "investment_unreported_holdings")
    assert result.status == "warn"
    # sec_known has a matching dim_holdings row — not flagged. The unbound
    # security (NULL canonical id) has none — flagged, displayed by its
    # provider key since it has no canonical id to show.
    assert result.affected_ids == ["acc1:plaid_sec_unbound"]


@pytest.mark.unit
def test_unreported_holdings_ignores_closed_position_at_broker(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A broker row reporting quantity 0 (a closed position) must not surface.

    ``is_short_or_nonpositive`` (the opening-lot bootstrap) already treats
    ``held_qty <= 0`` as expected data, not a gap — a closed position at the
    broker is not a position MoneyBin might secretly be holding.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_holdings "
        "(account_id VARCHAR, security_id VARCHAR, source_security_key VARCHAR, "
        "quantity DECIMAL(28,10), "
        "source_origin VARCHAR, source_file VARCHAR, extracted_at TIMESTAMP)"
    )
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES "
        "('acc1', NULL, 'plaid_sec_closed', 0, 'item1', 'sync_1', "
        "'2026-01-01 00:00:00')"
    )
    _create_snapshot_receipts_table(db)
    _receipt(db, "item1", "sync_1", "2026-01-01 00:00:00", 1)
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.dim_holdings (account_id VARCHAR, security_id VARCHAR, "
        "provider_reported_quantity DECIMAL(28,10))"
    )
    result = _investment_result(db, monkeypatch, "investment_unreported_holdings")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_unreported_holdings_only_considers_newest_snapshot(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A position from a superseded (non-newest) snapshot must not be flagged.

    The broker no longer claiming a position in its newest pull means it's
    gone (sold, disconnected) — not a currently-unknown holding.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_holdings "
        "(account_id VARCHAR, security_id VARCHAR, source_security_key VARCHAR, "
        "quantity DECIMAL(28,10), "
        "source_origin VARCHAR, source_file VARCHAR, extracted_at TIMESTAMP)"
    )
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES "
        "('acc1', 'sec_stale', 'plaid_sec_stale', 5, 'item1', 'sync_1', "
        "'2026-01-01 00:00:00'), "
        "('acc1', 'sec_current', 'plaid_sec_current', 5, 'item1', 'sync_2', "
        "'2026-02-01 00:00:00')"
    )
    _create_snapshot_receipts_table(db)
    _receipt(db, "item1", "sync_1", "2026-01-01 00:00:00", 1)
    _receipt(db, "item1", "sync_2", "2026-02-01 00:00:00", 1)
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.dim_holdings (account_id VARCHAR, security_id VARCHAR, "
        "provider_reported_quantity DECIMAL(28,10))"
    )
    db.execute("INSERT INTO core.dim_holdings VALUES ('acc1', 'sec_current', 5)")
    result = _investment_result(db, monkeypatch, "investment_unreported_holdings")
    assert result.status == "pass"
    assert result.affected_ids == []


def _create_phantom_holdings_tables(db: Database) -> None:
    """Create the staging + core tables ``investment_phantom_holdings`` reads.

    The check keys its stale-item guard on the ITEM (``source_origin``), so it
    reads BOTH Plaid investment staging views: an account whose item reported
    but which holds nothing is absent from the newest holdings snapshot and is
    only discoverable through the transactions view. "Which snapshot is newest"
    comes from the snapshot RECEIPTS view, never from the presence of holdings
    rows — see ``_create_snapshot_receipts_table``.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_holdings "
        "(account_id VARCHAR, security_id VARCHAR, source_security_key VARCHAR, "
        "quantity DECIMAL(28,10), "
        "source_origin VARCHAR, source_file VARCHAR, extracted_at TIMESTAMP)"
    )
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_transactions "
        "(investment_transaction_id VARCHAR, account_id VARCHAR, "
        "source_origin VARCHAR)"
    )
    _create_snapshot_receipts_table(db)
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.dim_holdings (account_id VARCHAR, security_id VARCHAR, "
        "provider_reported_quantity DECIMAL(28,10))"
    )


@pytest.mark.unit
def test_phantom_holdings_warn_when_account_in_snapshot_but_position_isnt(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lot MoneyBin holds that the account's live snapshot omits must surface.

    Reproduces the phantom-position gap: an option assignment (or any
    unmodeled leg) disposes of shares with no ledger quantity to close the
    lot, so ``core.dim_holdings`` still shows it open while the broker's
    fresh snapshot — which DOES report other positions for this account —
    no longer reports this one.
    """
    _create_phantom_holdings_tables(db)
    # acc1's newest snapshot reports sec_other — the account IS live and
    # synced — but says nothing about sec_phantom.
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES "
        "('acc1', 'sec_other', 'plaid_sec_other', 5, 'item1', 'sync_1', "
        "'2026-01-01 00:00:00')"
    )
    _receipt(db, "item1", "sync_1", "2026-01-01 00:00:00", 1)
    db.execute(
        "INSERT INTO core.dim_holdings VALUES "
        "('acc1', 'sec_phantom', NULL), "
        "('acc1', 'sec_other', 5)"
    )
    result = _investment_result(db, monkeypatch, "investment_phantom_holdings")
    assert result.status == "warn"
    # sec_other's provider_reported_quantity is populated (reported) — not
    # flagged. sec_phantom's is NULL while the account is live — flagged.
    assert result.affected_ids == ["acc1:sec_phantom"]


@pytest.mark.unit
def test_phantom_holdings_warn_when_account_fully_liquidated(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The worst phantom — an account the broker now reports as holding NOTHING.

    Plaid returns no holding entries for an account with no positions, so a
    fully-liquidated account has ZERO rows in the item's newest snapshot while
    MoneyBin still shows every lot open (the sells were option
    assignment/exercise rows staging maps to NULL-quantity 'other'). An
    account-keyed stale-item guard filters that account out entirely and
    reports `pass` on a 100%-overstated account. The guard must key on whether
    the ITEM reported, not whether the ACCOUNT did.
    """
    _create_phantom_holdings_tables(db)
    # item1 covers acc1 and acc2. Its newest snapshot (sync_2) reports acc2
    # only — acc1 was liquidated, so the broker returns nothing for it. acc1's
    # positions appear only in the superseded sync_1 snapshot.
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES "
        "('acc1', 'sec_gone', 'plaid_sec_gone', 5, 'item1', 'sync_1', "
        "'2026-01-01 00:00:00'), "
        "('acc2', 'sec_live', 'plaid_sec_live', 7, 'item1', 'sync_2', "
        "'2026-02-01 00:00:00')"
    )
    _receipt(db, "item1", "sync_1", "2026-01-01 00:00:00", 1)
    _receipt(db, "item1", "sync_2", "2026-02-01 00:00:00", 1)
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_transactions VALUES "
        "('itx_1', 'acc1', 'item1'), ('itx_2', 'acc2', 'item1')"
    )
    # MoneyBin never closed acc1's lot: dim_holdings still carries it, with a
    # NULL provider_reported_quantity (the broker's newest snapshot omits it).
    db.execute(
        "INSERT INTO core.dim_holdings VALUES "
        "('acc1', 'sec_gone', NULL), "
        "('acc2', 'sec_live', 7)"
    )
    result = _investment_result(db, monkeypatch, "investment_phantom_holdings")
    assert result.status == "warn"
    assert result.affected_ids == ["acc1:sec_gone"]


@pytest.mark.unit
def test_phantom_holdings_warn_when_account_never_in_any_snapshot_but_item_reported(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An account known only from the transactions view still counts as reported.

    The item delivered a live snapshot (for a sibling account), so its
    accounts are covered by a live pull. An account that has never appeared in
    ANY holdings snapshot while MoneyBin holds open lots for it is a phantom,
    not a stale item.
    """
    _create_phantom_holdings_tables(db)
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES "
        "('acc2', 'sec_live', 'plaid_sec_live', 7, 'item1', 'sync_1', "
        "'2026-01-01 00:00:00')"
    )
    _receipt(db, "item1", "sync_1", "2026-01-01 00:00:00", 1)
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_transactions VALUES "
        "('itx_1', 'acc1', 'item1')"
    )
    db.execute("INSERT INTO core.dim_holdings VALUES ('acc1', 'sec_never', NULL)")
    result = _investment_result(db, monkeypatch, "investment_phantom_holdings")
    assert result.status == "warn"
    assert result.affected_ids == ["acc1:sec_never"]


@pytest.mark.unit
def test_phantom_holdings_warn_when_item_reports_zero_holdings(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The residual blind spot: an item whose pull returns NO holdings at all.

    Every account at the broker is liquidated, so Plaid returns an empty
    holdings array and the loader writes ZERO holdings rows for that pull.
    A newest-snapshot derived from the presence of holdings ROWS therefore
    cannot see this pull at all — the item either drops out of the guard
    entirely (no rows ever) or silently keeps an earlier NON-EMPTY snapshot.
    Either way the largest possible net-worth overstatement — a broker where
    MoneyBin claims every position and the broker claims none — reads as
    `pass`.

    The snapshot RECEIPT is the missing evidence: item1 reported (receipt for
    sync_1), and what it reported was nothing (``holdings_count = 0``). Its
    accounts are in scope, and every lot MoneyBin still holds for them is a
    phantom.
    """
    _create_phantom_holdings_tables(db)
    # No holdings rows for item1 in ANY snapshot — the pull came back empty.
    # The accounts are known only through the transactions view.
    _receipt(db, "item1", "sync_1", "2026-01-01 00:00:00", 0)
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_transactions VALUES "
        "('itx_1', 'acc1', 'item1'), ('itx_2', 'acc2', 'item1')"
    )
    db.execute(
        "INSERT INTO core.dim_holdings VALUES "
        "('acc1', 'sec_gone', NULL), "
        "('acc2', 'sec_also_gone', NULL)"
    )
    result = _investment_result(db, monkeypatch, "investment_phantom_holdings")
    assert result.status == "warn"
    assert result.affected_ids == ["acc1:sec_gone", "acc2:sec_also_gone"]


@pytest.mark.unit
def test_phantom_holdings_pass_when_item_has_no_receipt(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No receipt = the item never reported. Absence of evidence, not evidence of absence.

    The twin of ``test_phantom_holdings_warn_when_item_reports_zero_holdings``,
    and the reason the receipt exists rather than a bare "treat every missing
    snapshot as empty" rule. item1 has holdings rows from an earlier pull but
    NO receipt for any pull — it never reported under the receipt regime (a
    disconnected item, or rows that predate the receipt). Trading the false
    negative for a false positive here would flag every position at a
    disconnected broker as sold.
    """
    _create_phantom_holdings_tables(db)
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES "
        "('acc1', 'sec_held', 'plaid_sec_held', 5, 'item1', 'sync_1', "
        "'2026-01-01 00:00:00')"
    )
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_transactions VALUES "
        "('itx_1', 'acc1', 'item1')"
    )
    db.execute("INSERT INTO core.dim_holdings VALUES ('acc1', 'sec_phantom', NULL)")
    result = _investment_result(db, monkeypatch, "investment_phantom_holdings")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_phantom_holdings_pass_when_item_stale_or_absent(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A held lot must NOT be flagged when its ITEM never delivered a snapshot.

    A stale/disconnected item whose snapshot never arrived would otherwise
    make every position on the account look like a phantom — the item-liveness
    guard exists precisely to rule this out.
    """
    _create_phantom_holdings_tables(db)
    # Only item2 ever delivered a snapshot. acc1 belongs to item1, which has
    # investment transactions but no holdings snapshot at all.
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES "
        "('acc2', 'sec_other', 'plaid_sec_other', 5, 'item2', 'sync_1', "
        "'2026-01-01 00:00:00')"
    )
    _receipt(db, "item2", "sync_1", "2026-01-01 00:00:00", 1)
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_transactions VALUES "
        "('itx_1', 'acc1', 'item1')"
    )
    db.execute("INSERT INTO core.dim_holdings VALUES ('acc1', 'sec_x', NULL)")
    result = _investment_result(db, monkeypatch, "investment_phantom_holdings")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_phantom_holdings_pass_when_account_is_manual_only(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A manual-only account has no Plaid item at all — never a phantom.

    ``core.dim_holdings.provider_reported_quantity`` is NULL for every
    manually-recorded position (no broker ever claimed it), so an unguarded
    check would flag the entire manual ledger.
    """
    _create_phantom_holdings_tables(db)
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES "
        "('acc_plaid', 'sec_live', 'plaid_sec_live', 5, 'item1', 'sync_1', "
        "'2026-01-01 00:00:00')"
    )
    _receipt(db, "item1", "sync_1", "2026-01-01 00:00:00", 1)
    db.execute("INSERT INTO core.dim_holdings VALUES ('acc_manual', 'sec_m', NULL)")
    result = _investment_result(db, monkeypatch, "investment_phantom_holdings")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_phantom_holdings_pass_when_freshly_bootstrapped(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A freshly bootstrapped position (quantity matches the broker) must not fire.

    ``provider_reported_quantity`` populated (not NULL) means the broker's
    newest snapshot DOES report this exact position — the ordinary,
    healthy case.
    """
    _create_phantom_holdings_tables(db)
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES "
        "('acc1', 'sec_ok', 'plaid_sec_ok', 10, 'item1', 'sync_1', "
        "'2026-01-01 00:00:00')"
    )
    _receipt(db, "item1", "sync_1", "2026-01-01 00:00:00", 1)
    db.execute("INSERT INTO core.dim_holdings VALUES ('acc1', 'sec_ok', 10)")
    result = _investment_result(db, monkeypatch, "investment_phantom_holdings")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_investment_checks_skip_when_views_absent(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in (
        "investment_staging_rejects",
        "investment_opening_lot_review",
        "investment_unmodeled_legs",
        "investment_holdings_divergence",
        "investment_unresolved_securities",
        "investment_unreported_holdings",
        "investment_phantom_holdings",
    ):
        assert _investment_result(db, monkeypatch, name).status == "skipped"


@pytest.mark.unit
def test_run_all_includes_investment_checks(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every investment check appears in the aggregated report.

    Holds even when the underlying prep/core views don't exist yet — the
    checks report ``skipped``, they don't vanish from the report.
    """
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.services.doctor_service.sqlmesh_context", _fake_ctx)
    report = DoctorService(doctor_db).run_all()
    names = [r.name for r in report.invariants]
    assert "investment_staging_rejects" in names
    assert "investment_opening_lot_review" in names
    assert "investment_unmodeled_legs" in names
    assert "investment_holdings_divergence" in names
    assert "investment_source_overlap" in names
    assert "investment_unresolved_securities" in names
    assert "investment_unreported_holdings" in names
    assert "investment_phantom_holdings" in names


@pytest.mark.integration
@pytest.mark.slow
def test_investment_checks_bind_to_real_transform_output(db: Database) -> None:
    """The investment checks must run against a REAL transform, not a mock.

    Every ``_run_investment_*`` check fails open to ``skipped`` on any
    exception — correct behavior before a first transform, but it also means
    a renamed column in ``prep.stg_plaid__*`` / ``core.dim_holdings`` /
    ``core.fct_investment_transactions`` would silently degrade every
    investment check to ``skipped`` while the rest of the doctor report
    stayed green — the exact failure mode this task exists to prevent.
    Every other test in this module mocks ``sqlmesh_context`` and fabricates
    the underlying tables by hand; this is the one test that runs a real
    ``TransformService.apply()`` (materializing the actual SQLMesh views)
    and a real (unmocked) ``DoctorService.run_all()`` against them, proving
    the check SQL is wired to the real column names.
    """
    db.execute(
        """
        INSERT INTO app.securities (security_id, name, security_type, currency_code)
        VALUES ('sec_real', 'Real Test Security', 'equity', 'USD')
        """  # noqa: S608  # test fixture, not executing user SQL
    )
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions
            (source_transaction_id, import_id, account_id, security_id, type,
             trade_date, quantity, amount, fees, currency_code,
             created_at, created_by, investment_transaction_id)
        VALUES ('manual_buy_real', 'imp_real', 'acc_real', 'sec_real', 'buy',
                '2026-01-01'::DATE, 10::DECIMAL(28,10), -1000.00::DECIMAL(18,2),
                0::DECIMAL(18,2), 'USD', '2026-01-01 09:00:00'::TIMESTAMP,
                'cli', 'inv_buy_real')
        """  # noqa: S608  # test fixture, not executing user SQL
    )

    result = TransformService(db).apply()
    assert result.applied, f"transform apply failed: {result.error}"

    report = DoctorService(db).run_all()
    investment_names = {
        "investment_staging_rejects",
        "investment_opening_lot_review",
        "investment_unmodeled_legs",
        "investment_holdings_divergence",
        "investment_source_overlap",
        "investment_unresolved_securities",
        "investment_unreported_holdings",
        "investment_phantom_holdings",
    }
    by_name = {r.name: r for r in report.invariants}
    missing = investment_names - by_name.keys()
    assert not missing, f"investment checks missing from the report: {missing}"
    skipped = {
        n: by_name[n].detail for n in investment_names if by_name[n].status == "skipped"
    }
    assert not skipped, (
        f"investment check(s) skipped against a real transform — the SQL is "
        f"not actually bound to the real schema: {skipped}"
    )
