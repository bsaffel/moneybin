"""Unit tests for DoctorService — pipeline invariant checks."""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

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
    yield database
    database.close()


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
                source_transaction_id, account_id, transaction_date, amount,
                description, currency_code, source_type, source_origin, is_pending
            ) VALUES (?, 'ACC1', '2026-01-01', -50.00, 'Test', 'USD', 'ofx', 'bank', false)
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
    sign = next(
        r for r in report.invariants if r.name == "fct_transactions_sign_convention"
    )
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


_MATCHED_MODEL_FILE = (
    Path(__file__).resolve().parents[3]
    / "sqlmesh"
    / "models"
    / "prep"
    / "int_transactions__matched.sql"
)

_UNIONED_FULL_DDL = """\
CREATE TABLE IF NOT EXISTS prep.int_transactions__unioned (
    source_transaction_id VARCHAR NOT NULL,
    account_id            VARCHAR NOT NULL,
    transaction_date      DATE,
    authorized_date       DATE,
    amount                DECIMAL(18, 2),
    description           VARCHAR,
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
            source_transaction_id, account_id, transaction_date, amount,
            description, currency_code, source_type, source_origin, is_pending
        ) VALUES (?, ?, '2026-01-01', -50.00, 'Test', 'USD', ?, 'bank', false)
        """,  # noqa: S608 — test input, not user data
        [source_transaction_id, account_id, source_type],
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
    # 3 sqlmesh audits + dedup_reconciliation + categorization + 21 app.* integrity
    # checks (audit coverage for user_categories / category_overrides /
    # gsheet_connections / user_merchants / categorization_rules / proposed_rules /
    # transaction_categories / account_settings / balance_assertions / budgets /
    # tabular_formats / match_decisions / imports + user_categories uniqueness +
    # user_merchants orphans + proposed_rules->rule FK + transaction_categories->fct
    # FK + account_settings->dim_accounts FK + balance_assertions->dim_accounts FK +
    # budgets->dim_categories FK + match_decisions->dim_accounts FK).
    assert len(report.invariants) == 26
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
    assert "app_user_categories_uniqueness" in names
    assert "app_account_settings_account_fk" in names
    assert "app_balance_assertions_account_fk" in names
    assert "app_budgets_category_fk" in names
    assert "app_match_decisions_account_fk" in names


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
