"""Unit tests for DoctorService — pipeline invariant checks."""

from __future__ import annotations

import dataclasses
import math
import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Final
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from moneybin.config import get_settings
from moneybin.database import SQLMESH_ROOT, Database
from moneybin.metrics.registry import (
    DUPLICATE_ACCOUNT_PAIRS,
    PROFILE_CURRENCIES,
    UNKNOWN_CURRENCY_ROWS,
)
from moneybin.repositories import concrete_repo_classes
from moneybin.services.doctor_service import (
    DoctorReport,
    DoctorService,
    InvariantResult,
)
from moneybin.services.transform_service import TransformService
from tests.moneybin.db_helpers import create_core_tables

_COVERAGE_PREFIX: Final = "app_audit_coverage_"
"""Name prefix `_run_app_audit_coverage` builds from a table's bare name."""

_PAIR_GAUGE_SAMPLE: Final = "moneybin_duplicate_account_pairs"
"""Sample name of `DUPLICATE_ACCOUNT_PAIRS` in the default registry."""

_UNSET_PAIR_GAUGE: Final = -1.0
"""Sentinel no run can produce, so "untouched" is distinguishable from zero."""


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
            updated_at, display_name, currency_code,
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all()
    # 3 sqlmesh audits + dedup_reconciliation + categorization + 31 app.* integrity
    # checks (audit coverage for user_categories / category_overrides /
    # gsheet_connections / user_merchants / categorization_rules / proposed_rules /
    # transaction_categories / account_settings / balance_assertions / budgets /
    # tabular_formats / match_decisions / imports / import_previews / pdf_formats /
    # securities / security_price_overrides (M1J.3 C.2) /
    # exchange_rate_overrides (M1K.2, composite pk_expr) /
    # lot_selections + user_categories uniqueness + user_merchants orphans +
    # proposed_rules->rule FK + transaction_categories->fct FK +
    # transaction_splits->fct FK +
    # account_settings->dim_accounts FK + balance_assertions->dim_accounts FK +
    # budgets->dim_categories FK + match_decisions->dim_accounts FK +
    # pdf_formats recipe-validity / bounds / fingerprint-shape) +
    # orphan_app_state (PR4: scans transaction_notes / transaction_tags vs
    # core) + account_links / account_link_decisions / transaction_id_aliases
    # audit coverage (M1S) + 9 investment reconciliation checks (T17: staging
    # rejects, opening-lot review, unmodeled legs, holdings divergence,
    # source overlap, unresolved securities, conflicting security refs,
    # unreported holdings, phantom holdings) + 4 investment price checks
    # (M1J.3 C.2: price disagreement, unpriced holdings, stale prices,
    # unmapped price source)
    # + transform_model_presence (registered-but-unbuilt models) +
    # currency_integrity (M1K.1 Req 6: unknown-currency rows, then merely-mixed
    # currency) + profile_settings audit coverage (M1K.1 Req 4) + user_reports
    # audit coverage (M2P.2) + duplicate_account_overlap (one account imported
    # under two identities — invisible to the matcher, which blocks candidate
    # pairs on account_id) + unproposed_cross_source_duplicates (the same two
    # sources *after* the link is accepted, which is where the overlap check
    # stops applying and dedup_reconciliation never applied).
    assert len(report.invariants) == 59
    names = [r.name for r in report.invariants]
    assert "fct_transactions_fk_integrity" in names
    assert "fct_transactions_sign_convention" in names
    assert "bridge_transfers_balanced" in names
    assert "transform_model_presence" in names
    assert "dedup_reconciliation" in names
    assert "duplicate_account_overlap" in names
    assert "unproposed_cross_source_duplicates" in names
    assert "categorization_coverage" in names
    assert "currency_integrity" in names
    assert "app_audit_coverage_user_categories" in names
    assert "app_audit_coverage_category_overrides" in names
    assert "app_audit_coverage_gsheet_connections" in names
    assert "app_audit_coverage_account_settings" in names
    assert "app_audit_coverage_balance_assertions" in names
    assert "app_audit_coverage_budgets" in names
    assert "app_audit_coverage_tabular_formats" in names
    assert "app_audit_coverage_match_decisions" in names
    assert "app_audit_coverage_imports" in names
    assert "app_audit_coverage_import_previews" in names
    assert "app_audit_coverage_securities" in names
    assert "app_audit_coverage_lot_selections" in names
    assert "app_audit_coverage_user_reports" in names
    assert "app_user_categories_uniqueness" in names
    assert "app_account_settings_account_fk" in names
    assert "app_balance_assertions_account_fk" in names
    assert "app_budgets_category_fk" in names
    assert "app_match_decisions_account_fk" in names
    assert "app_transaction_splits_fk" in names
    assert "orphan_app_state" in names


_UNCOVERED_REPO_TABLES: Final = {
    "app.ai_consent_grants": "watermark is GREATEST(granted_at, revoked_at)",
    "app.categorization_decisions": (
        "watermark spans proposed_at / decided_at / reversed_at"
    ),
    "app.category_source_map": "has updated_at; composite PK needs a pk_expr",
    "app.export_destinations": "has updated_at",
    "app.merchant_link_decisions": "watermark is GREATEST(decided_at, reversed_at)",
    "app.merchant_links": "watermark is GREATEST(decided_at, reversed_at)",
    "app.security_link_decisions": "watermark is GREATEST(decided_at, reversed_at)",
    "app.security_links": "watermark is GREATEST(decided_at, reversed_at)",
    "app.transaction_notes": "insert-shaped: created_at is the only timestamp",
    "app.transaction_splits": "insert-shaped: created_at is the only timestamp",
    "app.transaction_tags": "applied_at watermark; composite PK needs a pk_expr",
    "raw.manual_investment_transactions": (
        "raw.*, so the app_audit_coverage_* check name does not fit"
    ),
}
"""Repos with no ``app_audit_coverage_*`` invariant — known gaps, not exemptions.

Each reason names what closing it needs: 10 of the 12 lack the default
``updated_at`` watermark, 5 need a new ``_ALLOWED_UPDATED_EXPRS`` entry, and the
composite-PK ones need a ``pk_expr`` reconstructing exactly the ``target_id``
their repo emits. That is a dozen independent correctness decisions, tracked as
a follow-up rather than smuggled into an unrelated PR.
"""


@pytest.mark.unit
def test_doctor_audit_coverage_matches_the_repo_registry(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every repo either has an audit-coverage invariant or an explicit reason.

    The ``len(report.invariants)`` assertion above is one-sided: it fires
    when you *add* an invariant and never when you *forget* one, so a new repo
    could ship with its writes unverified and the suite would stay green. This
    compares the live discovered repo set against the live invariant names.

    Asserting set *equality* (not a subset) is what keeps
    ``_UNCOVERED_REPO_TABLES`` from decaying into a permanent allowlist: wiring
    a doctor call without deleting its entry fails here, and so does an entry
    naming a repo that was renamed or removed.
    """
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
    report = DoctorService(doctor_db).run_all()

    covered = {r.name for r in report.invariants if r.name.startswith(_COVERAGE_PREFIX)}
    uncovered = {
        cls.table_ref.full_name
        for cls in concrete_repo_classes()
        if f"{_COVERAGE_PREFIX}{cls.table_ref.name}" not in covered
    }

    expected = set(_UNCOVERED_REPO_TABLES)
    assert uncovered == expected, (
        f"repos newly missing audit coverage: {sorted(uncovered - expected)}; "
        f"stale _UNCOVERED_REPO_TABLES entries to delete: {sorted(expected - uncovered)}"
    )


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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _failing_ctx)
    svc = DoctorService(doctor_db)
    report = svc.run_all()
    skipped = next(
        (r for r in report.invariants if r.name == "transform_audits_unavailable"), None
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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
def test_conflicting_security_refs_warn(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two securities claiming one provider ref = one instrument held as two.

    The resolver will not repoint either binding (a repoint is a reviewed merge,
    never a sync-time side effect), so it logs and moves on. Nobody reads server
    logs; without this check the split is invisible while both securities quietly
    understate their lots.
    """
    db.execute(
        "INSERT INTO raw.plaid_securities "
        "(security_id, institution_id, institution_security_id, source_file, "
        "source_origin) VALUES ('p_vti', 'ins_1', 'ALPHA-VTI', 'f', 'item_1')"
    )
    # The row's OWN two refs disagree: its plaid id says sec_a, its institution
    # ref says sec_b. Each binding is individually unique and legal — only
    # grouping them back to the provider row exposes the split.
    db.execute(
        "INSERT INTO app.security_links "
        "(link_id, security_id, ref_kind, ref_value, source_type, status, decided_by, "
        "decided_at) VALUES "
        "('l1', 'sec_a', 'plaid_security_id', 'p_vti', 'plaid', 'accepted', 'auto', NOW()), "
        "('l2', 'sec_b', 'institution_security_id', 'ins_1:ALPHA-VTI', 'plaid', "
        "'accepted', 'auto', NOW())"
    )
    result = _investment_result(db, monkeypatch, "investment_conflicting_security_refs")
    assert result.status == "warn"
    assert result.affected_ids == ["p_vti"]


@pytest.mark.unit
def test_conflicting_security_refs_pass_when_each_ref_binds_once(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The steady state, including the case most likely to be a false positive.

    One fund held at TWO brokerages is one security_id with two institution refs
    — three refs, all correctly bound to the same security. That is healthy and
    routine, and a check that flagged it would warn on every multi-brokerage
    database. Only a row whose refs land on DIFFERENT securities is a conflict.
    """
    db.execute(
        "INSERT INTO raw.plaid_securities "
        "(security_id, institution_id, institution_security_id, source_file, "
        "source_origin) VALUES "
        "('p_vti', 'ins_1', 'ALPHA-VTI', 'f', 'item_1'), "
        "('p_vti', 'ins_2', 'BETA-VTI', 'f', 'item_2')"
    )
    db.execute(
        "INSERT INTO app.security_links "
        "(link_id, security_id, ref_kind, ref_value, source_type, status, decided_by, "
        "decided_at) VALUES "
        "('l1', 'sec_a', 'plaid_security_id', 'p_vti', 'plaid', 'accepted', 'auto', NOW()), "
        "('l2', 'sec_a', 'institution_security_id', 'ins_1:ALPHA-VTI', 'plaid', "
        "'accepted', 'auto', NOW()), "
        "('l3', 'sec_a', 'institution_security_id', 'ins_2:BETA-VTI', 'plaid', "
        "'accepted', 'auto', NOW())"
    )
    result = _investment_result(db, monkeypatch, "investment_conflicting_security_refs")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_opening_lot_review_pass_when_nothing_needs_review(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty review queue must read `pass` — not warn on zero rows.

    The check exists to surface positions the bootstrap REFUSED to synthesize;
    a healthy database has none, and that is the state it spends most of its
    life in. Without this, a query that silently matched everything would look
    identical to one that correctly matched nothing.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__opening_lot_review "
        "(account_id VARCHAR, security_id VARCHAR, source_security_key VARCHAR, "
        "reason VARCHAR)"
    )
    result = _investment_result(db, monkeypatch, "investment_opening_lot_review")
    assert result.status == "pass"
    assert result.affected_ids == []


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
def test_unmodeled_legs_pass_when_every_leg_is_modeled(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ordinary legs must read `pass` — the check's IN-list has to be selective.

    The sibling case-insensitivity test proves the check CATCHES what it should;
    this proves it does not catch what it shouldn't. A predicate broadened until
    it matched every subtype would still satisfy every warn test on this check
    while making the doctor cry wolf on a perfectly healthy ledger.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.fct_investment_transactions "
        "(investment_transaction_id VARCHAR, provider_subtype VARCHAR)"
    )
    db.execute(
        "INSERT INTO core.fct_investment_transactions VALUES "
        "('itx_buy', 'buy'), ('itx_sell', 'sell'), ('itx_div', 'dividend')"
    )
    result = _investment_result(db, monkeypatch, "investment_unmodeled_legs")
    assert result.status == "pass"
    assert result.affected_ids == []


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

    The check reads ``prep.stg_plaid__investment_holdings`` (its position-level
    ``ever_reported_positions`` gate) and ``core.dim_holdings``
    (``provider_reported_quantity``). The receipts and transactions tables are created
    here too — the check no longer reads them (decision 1 replaced the account-level
    coverage scope with the position-level gate), but several tests still seed them to
    document the coverage shapes the gate now correctly ignores.
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
def test_phantom_holdings_pass_when_position_never_reported_in_live_account(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A hand-tracked position in a broker-linked account is not a phantom.

    The account is live and synced (its newest snapshot reports sec_other), and
    sec_manual is an open lot the snapshot omits (provider_reported_quantity NULL) —
    the exact shape ``test_phantom_holdings_warn_when_account_fully_liquidated`` flags.
    The one difference: the broker has NEVER carried sec_manual in any holdings
    snapshot, so it is a manual holding, not a phantom, and flagging it would tell the
    user their share count is wrong when it is not. This is the adversarial partner to
    that test, isolating the ``ever_reported_positions`` gate.
    """
    _create_phantom_holdings_tables(db)
    # acc1's newest snapshot reports sec_other — the account IS live and synced — but
    # says nothing about sec_manual, which never appears in any holdings snapshot.
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES "
        "('acc1', 'sec_other', 'plaid_sec_other', 5, 'item1', 'sync_1', "
        "'2026-01-01 00:00:00')"
    )
    _receipt(db, "item1", "sync_1", "2026-01-01 00:00:00", 1)
    db.execute(
        "INSERT INTO core.dim_holdings VALUES "
        "('acc1', 'sec_manual', NULL), "
        "('acc1', 'sec_other', 5)"
    )
    result = _investment_result(db, monkeypatch, "investment_phantom_holdings")
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_phantom_holdings_warn_when_once_reported_position_dropped(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A genuine phantom: the broker reported the position, then a newer pull dropped it.

    The prior snapshot (sync_1) carried sec_gone; the newest (sync_2) reports acc2 only,
    so sec_gone is absent from the current claim (provider_reported_quantity NULL) while
    MoneyBin still holds the lot open — the sells were option assignment/exercise rows
    staging maps to NULL-quantity 'other'. Because the broker ONCE reported sec_gone
    (``ever_reported_positions``), its disappearance is a real overstatement, not a
    manual holding, and it surfaces. Contrast
    ``test_phantom_holdings_pass_when_position_never_reported_in_live_account``.
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
def test_phantom_holdings_pass_when_account_only_in_transactions(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An account known only from the transactions view is not a broker-reported holding.

    The item delivered a live snapshot for a sibling account (acc2), but acc1 appears
    only in the transactions view — the broker has never carried acc1's sec_never in a
    holdings snapshot. Under the position-level gate that is a never-reported holding,
    not a phantom. The old account-level coverage that treated any transactions-known
    account as reported is exactly what decision 1 (``dim_holdings.sql``'s
    ``ever_reported_positions``) removed, because it flagged manual positions.
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
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_phantom_holdings_pass_when_item_only_ever_reported_zero_holdings(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A position the broker never once carried in a holdings snapshot is not flagged.

    item1's only pull came back empty (``holdings_count = 0``, no holdings rows), so
    sec_gone / sec_also_gone never appear in ``prep.stg_plaid__investment_holdings`` —
    the accounts are known only through the transactions view. Under the position-level
    gate these are never-reported holdings, not phantoms, so they do not surface.

    This is the accepted cost of the narrow gate (decision 1): a position bought and
    disposed via an unmodeled leg entirely between holdings pulls — never once in a
    snapshot — is no longer flagged here, matching ``dim_holdings.sql`` (which values it
    rather than withholding). The far more common shape — the broker reported the
    position, then dropped it — still surfaces
    (``test_phantom_holdings_warn_when_once_reported_position_dropped``).
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
    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_phantom_holdings_pass_when_only_a_sibling_position_is_reported(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A never-reported position is not flagged even when the broker reports OTHER holdings.

    The broker carried sec_held for acc1, so the account is covered and that position is
    ever_reported — but the open lot MoneyBin holds is sec_phantom, which never appeared
    in any holdings snapshot. The gate is position-specific: a sibling reported position
    does not make an unreported one a phantom.
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
def test_phantom_holdings_pass_when_position_item_never_reported_holdings(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A held lot whose item never delivered a holdings snapshot is not a phantom.

    Only item2 ever reported holdings (for acc2); acc1's item1 has transactions but
    never a holdings snapshot, so acc1's sec_x has never been broker-reported. Under the
    position-level gate that is a never-reported holding, not a phantom — the item-
    liveness distinction the old account-level scope drew no longer changes the outcome.
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
        "investment_price_disagreement",
        "investment_unpriced_holdings",
        "investment_unmapped_price_source",
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

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
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
    assert "investment_price_disagreement" in names
    assert "investment_unpriced_holdings" in names
    assert "investment_stale_prices" in names
    assert "investment_unmapped_price_source" in names


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
        "investment_price_disagreement",
        "investment_unpriced_holdings",
        "investment_unmapped_price_source",
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


# --- C.2 price checks -------------------------------------------------------


def _price_staging_ddl(db: Database) -> None:
    """The provider-observation view the disagreement check compares across."""
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_security_prices ("
        "security_id VARCHAR, provider_security_key VARCHAR, price_date DATE, "
        "quote_currency VARCHAR, source_type VARCHAR, source_origin VARCHAR, "
        "close DECIMAL(28, 10), price_basis VARCHAR)"
    )


def _stage_price(
    db: Database,
    *,
    security_id: str,
    close: str,
    source: str,
    price_date: str = "2026-07-23",
    currency: str = "USD",
    key: str | None = None,
) -> None:
    db.execute(
        "INSERT INTO prep.stg_security_prices VALUES (?, ?, ?::DATE, ?, ?, '', ?, 'raw')",
        [
            security_id,
            key or f"{source}_{security_id}",
            price_date,
            currency,
            source,
            close,
        ],
    )


@pytest.mark.unit
def test_price_disagreement_warns_when_two_feeds_diverge(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two providers pricing one security on one date must agree; one is wrong.

    The failure this catches is a feed key bound to the wrong security, which
    produces a difference far beyond any timing or venue effect.
    """
    _price_staging_ddl(db)
    _stage_price(db, security_id="sec_1", close="212.55", source="tiingo")
    _stage_price(db, security_id="sec_1", close="19.40", source="plaid")

    result = _investment_result(db, monkeypatch, "investment_price_disagreement")

    assert result.status == "warn"
    assert result.affected_ids == ["sec_1"]


@pytest.mark.unit
def test_price_disagreement_passes_within_tolerance(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two feeds agreeing to within the tolerance is the ordinary case, not a finding."""
    _price_staging_ddl(db)
    _stage_price(db, security_id="sec_1", close="100.00", source="tiingo")
    _stage_price(db, security_id="sec_1", close="100.90", source="plaid")

    result = _investment_result(db, monkeypatch, "investment_price_disagreement")

    assert result.status == "pass"
    assert result.affected_ids == []


def _mark(
    db: Database,
    *,
    security_id: str,
    close: str = "212.55",
    price_date: str = "2026-07-23",
    currency: str = "USD",
) -> None:
    """The user's own price for one grain — what this check tells them to record."""
    db.execute(
        "INSERT INTO app.security_price_overrides "
        "(security_id, price_date, quote_currency, close) VALUES (?, ?::DATE, ?, ?)",
        [security_id, price_date, currency, close],
    )


@pytest.mark.unit
def test_price_disagreement_clears_once_the_user_records_a_mark(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The remediation this check prints must actually clear it.

    A mark fixes the resolved winner in `core`, but the competing provider rows
    it was recorded to settle stay in `prep` forever — so a check reading staging
    unconditionally goes on reporting a disagreement the user has already
    adjudicated, on every run, with no remaining action. A finding that cannot
    be cleared teaches the reader to ignore the whole report.
    """
    _price_staging_ddl(db)
    _stage_price(db, security_id="sec_1", close="212.55", source="tiingo")
    _stage_price(db, security_id="sec_1", close="19.40", source="plaid")
    _mark(db, security_id="sec_1")

    result = _investment_result(db, monkeypatch, "investment_price_disagreement")

    assert result.status == "pass"
    assert result.affected_ids == []


@pytest.mark.unit
def test_price_disagreement_still_warns_on_a_date_the_mark_does_not_cover(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mark settles its own date, not the security.

    Paired with the test above deliberately: an exclusion keyed on `security_id`
    alone passes that one and fails this one, and it would silence every future
    disagreement for a security the moment one date was corrected — including a
    feed key bound to the wrong security, which is what this check exists to
    catch.
    """
    _price_staging_ddl(db)
    _stage_price(db, security_id="sec_1", close="212.55", source="tiingo")
    _stage_price(db, security_id="sec_1", close="19.40", source="plaid")
    _mark(db, security_id="sec_1", price_date="2026-07-22")

    result = _investment_result(db, monkeypatch, "investment_price_disagreement")

    assert result.status == "warn"
    assert result.affected_ids == ["sec_1"]


@pytest.mark.unit
def test_price_disagreement_ignores_a_different_quote_currency(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ADR and its ordinary listing are two prices for two currencies, not a conflict.

    quote_currency is in the key precisely so both survive; comparing across it
    would report every dual-currency security as permanently disagreeing.
    """
    _price_staging_ddl(db)
    _stage_price(db, security_id="sec_1", close="212.55", source="tiingo")
    _stage_price(db, security_id="sec_1", close="19.40", source="plaid", currency="HKD")

    result = _investment_result(db, monkeypatch, "investment_price_disagreement")

    assert result.status == "pass"


@pytest.mark.unit
def test_price_disagreement_ignores_a_different_date(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A security that moved between two dates is not two feeds disagreeing."""
    _price_staging_ddl(db)
    _stage_price(db, security_id="sec_1", close="212.55", source="tiingo")
    _stage_price(
        db, security_id="sec_1", close="19.40", source="plaid", price_date="2026-07-22"
    )

    result = _investment_result(db, monkeypatch, "investment_price_disagreement")

    assert result.status == "pass"


@pytest.mark.unit
def test_price_disagreement_ignores_one_source_reporting_twice(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One feed is not in disagreement with itself.

    A single provider can hold two rows for one security and date through
    distinct source_origins — two Plaid items both reporting the same security.
    That is a duplicate, which the source-overlap check owns; comparing them
    here would report a feed against itself.
    """
    _price_staging_ddl(db)
    _stage_price(db, security_id="sec_1", close="212.55", source="plaid", key="a")
    _stage_price(db, security_id="sec_1", close="19.40", source="plaid", key="b")

    result = _investment_result(db, monkeypatch, "investment_price_disagreement")

    assert result.status == "pass"


@pytest.mark.unit
def test_price_disagreement_respects_the_configured_tolerance(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The threshold is a config field, not a literal buried in the SQL."""
    monkeypatch.setenv("MONEYBIN_INVESTMENTS__PRICE_DISAGREEMENT_TOLERANCE_PCT", "0.5")
    _price_staging_ddl(db)
    _stage_price(db, security_id="sec_1", close="100.00", source="tiingo")
    _stage_price(db, security_id="sec_1", close="100.90", source="plaid")

    result = _investment_result(db, monkeypatch, "investment_price_disagreement")

    assert result.status == "warn", (
        "a 0.9% spread must fire once the tolerance is tightened to 0.5% — the "
        "same fixture passes at the 2.0% default"
    )


def _unpriced_holdings_ddl(db: Database) -> None:
    """Only the columns the check reads, per the phantom-holdings precedent."""
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.dim_holdings (account_id VARCHAR, security_id VARCHAR, "
        "quantity DECIMAL(28,10), valuation_status VARCHAR)"
    )


@pytest.mark.unit
def test_unpriced_holdings_warns_on_a_position_with_no_usable_price(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unpriced position reads blank forever and nothing else surfaces it."""
    _unpriced_holdings_ddl(db)
    db.execute(
        "INSERT INTO core.dim_holdings VALUES ('acc_1', 'sec_1', 10, 'unpriced'), "
        "('acc_1', 'sec_2', 5, 'valued')"
    )

    result = _investment_result(db, monkeypatch, "investment_unpriced_holdings")

    assert result.status == "warn"
    assert result.affected_ids == ["sec_1"]


@pytest.mark.unit
def test_unpriced_holdings_does_not_claim_a_withheld_position(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """'withheld' also publishes no value, but wants a share count reconciled.

    Routing it here would send the user to add a price feed for a position whose
    price was never the problem.
    """
    _unpriced_holdings_ddl(db)
    db.execute(
        "INSERT INTO core.dim_holdings VALUES ('acc_1', 'sec_1', 10, 'withheld')"
    )

    result = _investment_result(db, monkeypatch, "investment_unpriced_holdings")

    assert result.status == "pass"


@pytest.mark.unit
def test_unpriced_holdings_does_not_claim_a_carried_forward_position(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A carried-forward price is a usable price; staleness is the surface for its age."""
    _unpriced_holdings_ddl(db)
    db.execute(
        "INSERT INTO core.dim_holdings VALUES ('acc_1', 'sec_1', 10, 'carried_forward')"
    )

    result = _investment_result(db, monkeypatch, "investment_unpriced_holdings")

    assert result.status == "pass"


@pytest.mark.unit
def test_unpriced_holdings_reports_one_security_held_in_two_accounts_once(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The remedy is per security — bind a feed key or record a mark — not per position."""
    _unpriced_holdings_ddl(db)
    db.execute(
        "INSERT INTO core.dim_holdings VALUES ('acc_1', 'sec_1', 10, 'unpriced'), "
        "('acc_2', 'sec_1', 3, 'unpriced')"
    )

    result = _investment_result(db, monkeypatch, "investment_unpriced_holdings")

    assert result.affected_ids == ["sec_1"]


def _stale_prices_ddl(db: Database) -> None:
    """Only the columns the check reads, per the phantom-holdings precedent."""
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        "CREATE TABLE core.dim_holdings (account_id VARCHAR, security_id VARCHAR, "
        "quantity DECIMAL(28,10), valuation_status VARCHAR, "
        "days_since_observed INTEGER)"
    )
    db.execute(
        "CREATE TABLE core.dim_securities (security_id VARCHAR, security_type VARCHAR)"
    )


def _hold_at_age(
    db: Database,
    *,
    security_id: str,
    days: int | None,
    security_type: str,
    status: str = "carried_forward",
) -> None:
    db.execute(
        "INSERT INTO core.dim_holdings VALUES ('acc_1', ?, 10, ?, ?)",
        [security_id, status, days],
    )
    db.execute(
        "INSERT INTO core.dim_securities VALUES (?, ?)", [security_id, security_type]
    )


@pytest.mark.unit
def test_stale_prices_warns_when_a_close_outlives_its_type_threshold(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A feedless position keeps its last close forever and says nothing.

    ``carried_forward`` publishes a market_value the reader treats as current,
    and the unpriced check deliberately skips it because "its age the staleness
    surface carries" — this is that surface.
    """
    _stale_prices_ddl(db)
    _hold_at_age(db, security_id="sec_1", days=400, security_type="equity")

    result = _investment_result(db, monkeypatch, "investment_stale_prices")

    assert result.status == "warn"
    assert result.affected_ids == ["sec_1"]


@pytest.mark.unit
def test_stale_prices_absorbs_an_ordinary_weekend(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Markets close ~114 days a year; firing on that trains the reader to ignore it."""
    _stale_prices_ddl(db)
    _hold_at_age(db, security_id="sec_1", days=3, security_type="equity")

    result = _investment_result(db, monkeypatch, "investment_stale_prices")

    assert result.status == "pass"


@pytest.mark.unit
def test_stale_prices_holds_crypto_to_its_own_tighter_threshold(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One global threshold cannot be right for both.

    Both positions are the same 2 days old, so only a per-type threshold can
    separate them: crypto trades continuously and yesterday's close is already
    the stalest thing worth having, while 2 days on an equity is a weekend.
    """
    _stale_prices_ddl(db)
    _hold_at_age(db, security_id="sec_coin", days=2, security_type="crypto")
    _hold_at_age(db, security_id="sec_stock", days=2, security_type="equity")

    result = _investment_result(db, monkeypatch, "investment_stale_prices")

    assert result.status == "warn"
    assert result.affected_ids == ["sec_coin"]


@pytest.mark.unit
def test_stale_prices_falls_back_to_the_configured_default(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`cash` and `other` are unnamed on purpose and take the global default."""
    monkeypatch.setenv("MONEYBIN_INVESTMENTS__PRICE_STALENESS_DEFAULT_DAYS", "10")
    _stale_prices_ddl(db)
    _hold_at_age(db, security_id="sec_1", days=11, security_type="other")
    _hold_at_age(db, security_id="sec_2", days=9, security_type="other")

    result = _investment_result(db, monkeypatch, "investment_stale_prices")

    assert result.status == "warn"
    assert result.affected_ids == ["sec_1"]


@pytest.mark.unit
def test_stale_prices_does_not_claim_an_unpriced_position(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nothing was ever observed, so there is no age — that is the other check's."""
    _stale_prices_ddl(db)
    _hold_at_age(
        db, security_id="sec_1", days=None, security_type="equity", status="unpriced"
    )

    result = _investment_result(db, monkeypatch, "investment_stale_prices")

    assert result.status == "pass"


@pytest.mark.unit
def test_stale_prices_reports_one_security_held_in_two_accounts_once(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The remedy is per security — add a feed or record a mark — not per position."""
    _stale_prices_ddl(db)
    _hold_at_age(db, security_id="sec_1", days=400, security_type="equity")
    db.execute(
        "INSERT INTO core.dim_holdings VALUES ('acc_2', 'sec_1', 3, "
        "'carried_forward', 400)"
    )

    result = _investment_result(db, monkeypatch, "investment_stale_prices")

    assert result.affected_ids == ["sec_1"]


def _unmapped_source_fixture(db: Database, *, source: str, staged: bool) -> None:
    """A raw price row with an accepted, matching binding — staged or not."""
    _price_staging_ddl(db)
    db.execute(
        "INSERT INTO raw.security_prices (provider_security_key, price_date, "
        "quote_currency, source_type, source_origin, close, price_basis) "
        "VALUES ('VTI', DATE '2026-07-23', 'USD', ?, '', 214.55, 'raw')",
        [source],
    )
    db.execute(
        "INSERT INTO app.security_links (link_id, security_id, ref_kind, ref_value, "
        "source_type, status, decided_by, decided_at) VALUES "
        "(?, 'sec_1', 'plaid_security_id', 'VTI', ?, 'accepted', 'auto', CURRENT_TIMESTAMP)",
        [f"link_{source}", source],
    )
    if staged:
        _stage_price(db, security_id="sec_1", close="214.55", source=source, key="VTI")


@pytest.mark.unit
def test_unmapped_price_source_warns_when_a_bound_row_never_stages(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The binding is accepted and matches, so only the registry can be dropping it.

    This is the exact defect that made C.2 inert: PriceService wrote tiingo rows
    for a commit while prep.stg_security_prices mapped only 'plaid', and the
    INNER JOIN discarded every one of them with no error and no counter.
    """
    _unmapped_source_fixture(db, source="yahoo", staged=False)

    result = _investment_result(db, monkeypatch, "investment_unmapped_price_source")

    assert result.status == "warn"
    assert result.affected_ids == ["yahoo"]


@pytest.mark.unit
def test_unmapped_price_source_passes_when_the_row_stages(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mapped source resolving normally is the ordinary case."""
    _unmapped_source_fixture(db, source="plaid", staged=True)

    result = _investment_result(db, monkeypatch, "investment_unmapped_price_source")

    assert result.status == "pass"


@pytest.mark.unit
def test_unmapped_price_source_ignores_a_row_the_ownership_interval_excluded(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Absent from staging is not the same question as absent from the registry.

    `prep.stg_security_prices` bounds each key to the interval its current link
    owns, so a close predating a handover — the previous owner of a recycled
    ticker — never stages, by design. Its source is mapped and staging other
    rows normally; a row-wise correlation read that one gap as a broken CASE and
    told the user to add a mapping that already exists.
    """
    _unmapped_source_fixture(db, source="plaid", staged=True)
    db.execute(
        "INSERT INTO raw.security_prices (provider_security_key, price_date, "
        "quote_currency, source_type, source_origin, close, price_basis) "
        "VALUES ('FB', DATE '2019-01-02', 'USD', 'plaid', '', 131.09, 'raw')"
    )
    db.execute(
        "INSERT INTO app.security_links (link_id, security_id, ref_kind, ref_value, "
        "source_type, status, decided_by, decided_at) VALUES "
        "('link_fb', 'sec_2', 'plaid_security_id', 'FB', 'plaid', 'accepted', "
        "'auto', CURRENT_TIMESTAMP)"
    )

    result = _investment_result(db, monkeypatch, "investment_unmapped_price_source")

    assert result.status == "pass"


@pytest.mark.unit
def test_unmapped_price_source_ignores_a_row_with_no_accepted_binding(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unbound observation waits in raw and reappears once its security binds.

    That is a different condition with a different remedy, and it is routine
    during ingestion — the extractor writes prices before the resolver runs. The
    accepted-binding clause is what separates a broken mapping from a pending
    one; without it this check would fire on every ordinary first pull.
    """
    _price_staging_ddl(db)
    db.execute(
        "INSERT INTO raw.security_prices (provider_security_key, price_date, "
        "quote_currency, source_type, source_origin, close, price_basis) "
        "VALUES ('VTI', DATE '2026-07-23', 'USD', 'plaid', '', 214.55, 'raw')"
    )

    result = _investment_result(db, monkeypatch, "investment_unmapped_price_source")

    assert result.status == "pass"


@pytest.mark.unit
def test_missing_registered_model_fails_an_invariant(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A model the project declares but never built must fail the doctor.

    Every other health signal is derived from what IS built, so a model that
    was never materialised leaves nothing to notice. Breaks if the invariant
    compares against the catalog instead of the registered set.
    """
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
    # `doctor_db` already builds `core.fct_transactions`, which no `db init`
    # creates — that alone marks the warehouse as built, staging layer or not.
    svc = DoctorService(doctor_db)

    report = svc.run_all()
    result = next(r for r in report.invariants if r.name == "transform_model_presence")

    assert result.status == "fail"
    # The fixture DB builds only a handful of core tables, so most of the
    # registered set is absent — the point is that it says *which*.
    assert result.affected_ids
    assert all("." in name for name in result.affected_ids)
    assert result.detail is not None


@pytest.mark.unit
def test_unreadable_catalog_reports_unavailable_not_a_fresh_profile(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A catalog read that fails must not hand back the first-run remedy.

    "no SQLMesh models built yet; run refresh_run" is an actively wrong
    instruction for a database whose catalog cannot be read — it names a
    healthy state and a remedy that will not help. The invariant catches its
    own failure and says so, matching every other `_run_*` method.
    """
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    def _raise(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("catalog unreadable")

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
    monkeypatch.setattr("moneybin.services.doctor_service.model_presence", _raise)

    report = DoctorService(doctor_db).run_all()
    result = next(r for r in report.invariants if r.name == "transform_model_presence")

    assert result.status == "skipped"
    assert result.detail is not None
    assert "refresh_run" not in result.detail
    assert "catalog unreadable" in result.detail


@pytest.mark.unit
def test_model_presence_passes_when_every_registered_model_exists(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The invariant must be able to PASS, not just always fail.

    Its sibling test proves it fails on an incomplete catalog — which a
    permanently-broken check (a normalization regression on either side of the
    set difference) would also satisfy. This pins the other direction, so the
    invariant cannot ship green while flipping `moneybin system doctor` to
    exit 1 on every warehouse.
    """
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
    # Declare only models this fixture genuinely builds, so a pass is real
    # rather than an artifact of an empty registered set.
    monkeypatch.setattr(
        "moneybin.sqlmesh_registry.registered_model_names",
        lambda: frozenset({"prep.stg_probe", "core.dim_accounts"}),
    )
    doctor_db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    doctor_db.execute("CREATE VIEW prep.stg_probe AS SELECT 1 AS x")
    svc = DoctorService(doctor_db)

    result = next(
        r for r in svc.run_all().invariants if r.name == "transform_model_presence"
    )

    assert result.status == "pass"
    assert result.affected_ids == []
    assert result.detail is None


def _currency_result(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> InvariantResult:
    """Run the doctor and return the currency_integrity invariant."""
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
    report = DoctorService(doctor_db).run_all()
    return next(r for r in report.invariants if r.name == "currency_integrity")


@pytest.mark.unit
def test_currency_integrity_passes_on_a_single_currency_profile(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The common case stays silent — the fixture is USD throughout."""
    result = _currency_result(doctor_db, monkeypatch)
    assert result.status == "pass"
    assert result.detail is None


@pytest.mark.unit
def test_currency_integrity_warns_when_a_profile_holds_two_currencies(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mixed currency is legal but withholds every cross-currency total."""
    doctor_db.execute("""
        UPDATE core.fct_transactions SET currency_code = 'EUR'
        WHERE transaction_id = 'T2'
    """)  # noqa: S608 — test input, not user data

    result = _currency_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    detail = result.detail or ""
    assert "EUR" in detail
    assert "USD" in detail


@pytest.mark.unit
def test_currency_integrity_counts_and_names_a_third_currency(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Three currencies, drawn from two different tables.

    The two-currency case is satisfied by a check that only asks "is there more
    than one?" — a hardcoded count, or a list that dedups to a pair, reads the
    same. Sourcing the third from `dim_accounts` rather than a third transaction
    also proves the UNION reaches every table it claims to, not just the one the
    other tests mutate.
    """
    doctor_db.execute("""
        UPDATE core.fct_transactions SET currency_code = 'EUR'
        WHERE transaction_id = 'T2'
    """)  # noqa: S608 — test input, not user data
    doctor_db.execute("""
        UPDATE core.dim_accounts SET currency_code = 'GBP'
        WHERE account_id = 'ACC1'
    """)  # noqa: S608 — test input, not user data

    result = _currency_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    detail = result.detail or ""
    assert "3 currencies" in detail
    assert "EUR, GBP, USD" in detail


@pytest.mark.unit
def test_currency_integrity_warn_explains_the_withheld_balance_adjustment(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mixed currency also shrinks a carried balance — the warn has to say so.

    core.fct_balances_daily leaves a transaction denominated in another currency
    out of the account's carry, because no FX rate exists until M1K.2. The
    amount resurfaces as reconciliation drift, but a user who is never told
    where it went cannot read that drift as anything but a bug.
    """
    doctor_db.execute("""
        UPDATE core.fct_transactions SET currency_code = 'EUR'
        WHERE transaction_id = 'T2'
    """)  # noqa: S608 — test input, not user data

    result = _currency_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    detail = result.detail or ""
    assert "carried" in detail
    assert "balance_drift" in detail


@pytest.mark.unit
def test_currency_integrity_fail_names_the_transform_that_applies_the_fix(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`accounts set --currency` writes app state; core.* is a table, not a view.

    Follows dedup_reconciliation's convention in this same file: a remedy that
    only takes effect after `moneybin transform` must say so, or the user
    applies it, re-runs the doctor, sees the identical failure, and concludes
    the documented fix does not work.
    """
    doctor_db.execute("""
        UPDATE core.fct_transactions SET currency_code = NULL
        WHERE transaction_id = 'T2'
    """)  # noqa: S608 — test input, not user data

    result = _currency_result(doctor_db, monkeypatch)

    assert result.status == "fail"
    assert "transform" in (result.detail or "")


@pytest.mark.unit
def test_currency_integrity_fails_on_a_transaction_with_unknown_currency(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A NULL currency is an amount whose unit nobody knows — segment and flag."""
    doctor_db.execute("""
        UPDATE core.fct_transactions SET currency_code = NULL
        WHERE transaction_id = 'T2'
    """)  # noqa: S608 — test input, not user data

    result = _currency_result(doctor_db, monkeypatch)

    assert result.status == "fail"
    assert "unknown" in (result.detail or "").lower()
    assert result.affected_ids == ["transaction:T2"]


@pytest.mark.unit
def test_currency_integrity_prefixes_each_affected_id_with_its_grain(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Account and transaction ids must be distinguishable in one list.

    ``affected_ids`` mixes two grains, and a bare id says nothing about which
    tool fixes it — an account needs `accounts set --currency`, a transaction
    needs its own path. ``orphan_app_state`` established the ``note:``/``tag:``
    prefix convention in this same file for exactly that reason; a recipe
    written against an unprefixed list would have to re-query to tell them
    apart.
    """
    doctor_db.execute("""
        UPDATE core.fct_transactions SET currency_code = NULL
        WHERE transaction_id = 'T2'
    """)  # noqa: S608 — test input, not user data
    doctor_db.execute("""
        UPDATE core.dim_accounts SET currency_code = NULL
        WHERE account_id = 'ACC1'
    """)  # noqa: S608 — test input, not user data

    result = _currency_result(doctor_db, monkeypatch)

    assert result.status == "fail"
    assert result.affected_ids == ["account:ACC1", "transaction:T2"]


@pytest.mark.unit
def test_currency_integrity_fails_on_a_balance_with_unknown_currency(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Balances are a third grain with its own count, message, and metric label.

    A balance is where net worth comes from, so an unknown-currency balance is
    the one most likely to be read as a number in the reader's own currency.
    Every other currency_integrity test seeds NULL into transactions or
    accounts, which leaves this branch's SQL, its "balance observation(s)"
    wording, and its balances metric label unexercised.
    """
    # create_core_tables() installs fct_balances as an always-empty placeholder
    # view (`WHERE FALSE`), which is why nothing has reached this branch: there
    # is no base table to UPDATE. Replacing the view is the only way to put a
    # row in front of the doctor at unit level.
    doctor_db.execute("""
        CREATE OR REPLACE VIEW core.fct_balances AS
        SELECT 'ACC1'::VARCHAR AS account_id,
               CURRENT_DATE AS balance_date,
               100.00::DECIMAL(18, 2) AS balance,
               'ofx'::VARCHAR AS source_type,
               'b.qfx'::VARCHAR AS source_ref,
               CURRENT_TIMESTAMP AS updated_at,
               NULL::VARCHAR AS currency_code
    """)  # noqa: S608 — test input, not user data

    result = _currency_result(doctor_db, monkeypatch)

    assert result.status == "fail"
    assert "1 balance observation(s)" in (result.detail or "")


@pytest.mark.unit
def test_currency_integrity_fails_on_an_account_with_unknown_currency(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Accounts are flagged too — that is where the user assigns the fix."""
    doctor_db.execute("""
        UPDATE core.dim_accounts SET currency_code = NULL
        WHERE account_id = 'ACC1'
    """)  # noqa: S608 — test input, not user data

    result = _currency_result(doctor_db, monkeypatch)

    assert result.status == "fail"
    assert "account:ACC1" in result.affected_ids


@pytest.mark.unit
def test_currency_integrity_reports_unknown_currency_over_mere_mixing(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A profile that is both mixed and incomplete surfaces the fixable half."""
    doctor_db.execute("""
        UPDATE core.fct_transactions SET currency_code = 'EUR'
        WHERE transaction_id = 'T1'
    """)  # noqa: S608 — test input, not user data
    doctor_db.execute("""
        UPDATE core.fct_transactions SET currency_code = NULL
        WHERE transaction_id = 'T2'
    """)  # noqa: S608 — test input, not user data

    result = _currency_result(doctor_db, monkeypatch)

    assert result.status == "fail"
    assert "unknown" in (result.detail or "").lower()


@pytest.mark.unit
def test_currency_integrity_records_what_it_observed(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The check publishes the two numbers a operator would page on."""
    doctor_db.execute("""
        UPDATE core.fct_transactions SET currency_code = 'EUR'
        WHERE transaction_id = 'T1'
    """)  # noqa: S608 — test input, not user data
    doctor_db.execute("""
        UPDATE core.fct_transactions SET currency_code = NULL
        WHERE transaction_id = 'T2'
    """)  # noqa: S608 — test input, not user data

    _currency_result(doctor_db, monkeypatch)

    # EUR from T1 plus USD from the account row = 2 known currencies; T2 is the
    # one unknown-currency row.
    assert PROFILE_CURRENCIES._value.get() == 2  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
    assert (
        UNKNOWN_CURRENCY_ROWS.labels(grain="transactions")._value.get() == 1  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
    )
    assert UNKNOWN_CURRENCY_ROWS.labels(grain="accounts")._value.get() == 0  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals


@pytest.mark.unit
def test_currency_integrity_counts_past_the_reported_id_cap(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The count is a real COUNT(*), not the length of the capped id list.

    affected_ids is bounded so the envelope stays small; deriving the count
    from it would saturate at the cap and show the user the same number every
    run no matter how many rows they fixed.
    """
    doctor_db.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month,
            transaction_year_quarter
        )
        SELECT 'N' || i, 'ACC1', DATE '2026-01-03', -1.00, 1.00, 'expense',
               'no currency', 'DEBIT', false, NULL, 'ofx',
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
               2026, 1, 3, 5, '2026-01', '2026-Q1'
        FROM GENERATE_SERIES(1, 150) AS t(i)
    """)  # noqa: S608 — test input, not user data

    result = _currency_result(doctor_db, monkeypatch)

    assert result.status == "fail"
    assert "150 transaction(s)" in (result.detail or "")
    # The id list stays capped so the envelope does not carry 150 ids.
    assert len(result.affected_ids) == 100
    assert UNKNOWN_CURRENCY_ROWS.labels(grain="transactions")._value.get() == 150  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals


# ---------------------------------------------------------------------------
# duplicate_account_overlap — one real account imported under two identities
# ---------------------------------------------------------------------------


def _insert_overlap_account(
    db: Database, account_id: str, *, institution_slug: str
) -> None:
    """Insert one core.dim_accounts row carrying an institution slug.

    The `doctor_db` fixture's own ACC1 leaves `institution_slug` NULL, which
    the check scopes out — these tests supply their own accounts so the
    fixture's rows cannot contribute to a pair.
    """
    db.execute(
        """
        INSERT INTO core.dim_accounts (
            account_id, account_type, institution_name, institution_slug,
            source_type, source_file, extracted_at, loaded_at, updated_at,
            display_name, currency_code, archived, include_in_net_worth
        ) VALUES (?, 'CHECKING', 'Bank', ?, 'ofx', 'a.qfx',
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                  ?, 'USD', FALSE, TRUE)
        """,  # noqa: S608 — test input, not user data
        [account_id, institution_slug, account_id],
    )


def _insert_amount_ladder(
    db: Database,
    account_id: str,
    *,
    rows: int,
    first_index: int = 1,
    day_offset: int = 0,
    sign: int = -1,
    currency_code: str = "USD",
) -> None:
    """Insert ``rows`` transactions whose amounts and dates are all distinct.

    Row ``i`` (``first_index`` … ``first_index + rows - 1``) carries amount
    ``sign * i`` on day ``i + day_offset``. Two ladders sharing a
    ``first_index`` therefore mirror each other exactly, and ladders over
    disjoint index ranges share no amount and no date.
    """
    db.execute(
        """
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            currency_code, source_type
        )
        SELECT ? || '_' || i, ?, DATE '2026-01-01' + CAST(i + ? AS INTEGER),
               ? * i, ?, 'ofx'
        FROM GENERATE_SERIES(?, ?) AS t(i)
        """,  # noqa: S608 — test input, not user data
        [
            account_id,
            account_id,
            day_offset,
            sign,
            currency_code,
            first_index,
            first_index + rows - 1,
        ],
    )


def _insert_repeated_amount(
    db: Database, account_id: str, *, rows: int, amount: float
) -> None:
    """Insert ``rows`` transactions that all carry the SAME amount, one per day."""
    db.execute(
        """
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            currency_code, source_type
        )
        SELECT ? || '_r' || i, ?, DATE '2026-01-01' + CAST(i AS INTEGER), ?,
               'USD', 'ofx'
        FROM GENERATE_SERIES(1, ?) AS t(i)
        """,  # noqa: S608 — test input, not user data
        [account_id, account_id, amount, rows],
    )


def _overlap_result(db: Database, monkeypatch: pytest.MonkeyPatch) -> InvariantResult:
    """Run the full doctor report (SQLMesh mocked) and return the overlap invariant."""
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
    report = DoctorService(db).run_all()
    return next(r for r in report.invariants if r.name == "duplicate_account_overlap")


@pytest.mark.unit
def test_mirrored_accounts_at_one_institution_warn(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Chase split: every row of one account mirrored on its sibling.

    Offset by exactly `matching.date_window_days` so the check's window bound
    is inclusive — the live case that motivated this invariant had only 23% of
    its pairs on an exact date, the rest spread across posting lag.
    """
    settings = get_settings()
    rows = settings.doctor.duplicate_account_min_distinct_amounts
    _insert_overlap_account(doctor_db, "DUP_A", institution_slug="chase")
    _insert_overlap_account(doctor_db, "DUP_B", institution_slug="chase")
    _insert_amount_ladder(doctor_db, "DUP_A", rows=rows)
    _insert_amount_ladder(
        doctor_db, "DUP_B", rows=rows, day_offset=settings.matching.date_window_days
    )

    result = _overlap_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.affected_ids == ["DUP_A:DUP_B (100% overlap)"]


@pytest.mark.unit
def test_the_overlap_finding_publishes_a_recovery_for_the_case_it_predicts(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The finding already warns the sweep may propose nothing — so it must not stop there.

    This check measures transaction overlap; identity resolution matches on
    institution, last four and name. The detail says so itself, which means it
    names a remedy it knows can come back empty. The two-id form of
    ``accounts links run`` is what covers exactly that residue, and a finding
    that ends at the sweep leaves the user holding a confirmed duplicate and no
    next command.
    """
    import re

    from tests.cli_command_helpers import assert_published_commands_resolve

    settings = get_settings()
    rows = settings.doctor.duplicate_account_min_distinct_amounts
    _insert_overlap_account(doctor_db, "DUP_A", institution_slug="chase")
    _insert_overlap_account(doctor_db, "DUP_B", institution_slug="chase")
    _insert_amount_ladder(doctor_db, "DUP_A", rows=rows)
    _insert_amount_ladder(
        doctor_db, "DUP_B", rows=rows, day_offset=settings.matching.date_window_days
    )

    result = _overlap_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.detail is not None
    assert re.search(r"`moneybin accounts links run <[^>]+> <[^>]+>`", result.detail), (
        f"no two-id form published in {result.detail!r}"
    )
    assert_published_commands_resolve(result.detail)


@pytest.mark.unit
def test_mirrored_accounts_at_different_institutions_pass(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Identical to the warning case except the slug — isolates the institution gate."""
    settings = get_settings()
    rows = settings.doctor.duplicate_account_min_distinct_amounts
    _insert_overlap_account(doctor_db, "DUP_A", institution_slug="chase")
    _insert_overlap_account(doctor_db, "DUP_B", institution_slug="wells")
    _insert_amount_ladder(doctor_db, "DUP_A", rows=rows)
    _insert_amount_ladder(
        doctor_db, "DUP_B", rows=rows, day_offset=settings.matching.date_window_days
    )

    result = _overlap_result(doctor_db, monkeypatch)

    assert result.status == "pass"


@pytest.mark.unit
def test_mirrored_amounts_in_different_currencies_pass(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A USD account and a EUR account at one bank are two accounts, not one.

    Equal numerals across currencies are not evidence of mirroring — a USD
    checking account and a EUR travel account at the same institution can align
    on nominal amounts by ordinary coincidence. This check added a distinct-amount
    floor and a coverage ratio specifically to rule coincidence out, and matching
    on the numeral alone hands that precision back: the pair is reported as a
    likely duplicate and the user is pointed at an account merge.

    Isolation: identical to `test_mirrored_accounts_at_one_institution_warn` in
    every respect — same institution, same ladders, same offset, same coverage
    and distinct-amount counts — except the currency. Only the currency predicate
    can turn that warning into a pass.
    """
    settings = get_settings()
    rows = settings.doctor.duplicate_account_min_distinct_amounts
    _insert_overlap_account(doctor_db, "DUP_A", institution_slug="chase")
    _insert_overlap_account(doctor_db, "DUP_B", institution_slug="chase")
    _insert_amount_ladder(doctor_db, "DUP_A", rows=rows, currency_code="USD")
    _insert_amount_ladder(
        doctor_db,
        "DUP_B",
        rows=rows,
        day_offset=settings.matching.date_window_days,
        currency_code="EUR",
    )

    result = _overlap_result(doctor_db, monkeypatch)

    assert result.status == "pass"


@pytest.mark.unit
def test_one_repeated_amount_is_not_duplication(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Twin savings accounts posting the same interest are fully mirrored, not duplicates.

    Coverage is 100% and every date lines up, so only the distinct-amount floor
    can reject this — the fixture isolates that floor and nothing else.
    """
    settings = get_settings()
    rows = settings.doctor.duplicate_account_min_distinct_amounts * 2
    _insert_overlap_account(doctor_db, "TWIN_A", institution_slug="chase")
    _insert_overlap_account(doctor_db, "TWIN_B", institution_slug="chase")
    _insert_repeated_amount(doctor_db, "TWIN_A", rows=rows, amount=0.01)
    _insert_repeated_amount(doctor_db, "TWIN_B", rows=rows, amount=0.01)

    result = _overlap_result(doctor_db, monkeypatch)

    assert result.status == "pass"


@pytest.mark.unit
def test_a_gap_beyond_the_matcher_window_is_not_overlap(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One day past `matching.date_window_days` — isolates the window bound."""
    settings = get_settings()
    rows = settings.doctor.duplicate_account_min_distinct_amounts
    _insert_overlap_account(doctor_db, "DUP_A", institution_slug="chase")
    _insert_overlap_account(doctor_db, "DUP_B", institution_slug="chase")
    _insert_amount_ladder(doctor_db, "DUP_A", rows=rows)
    _insert_amount_ladder(
        doctor_db, "DUP_B", rows=rows, day_offset=settings.matching.date_window_days + 1
    )

    result = _overlap_result(doctor_db, monkeypatch)

    assert result.status == "pass"


@pytest.mark.unit
def test_transfers_between_sibling_accounts_are_not_overlap(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same institution, same dates, same magnitudes — only the sign differs.

    Every same-institution transfer has this shape, so it is the check's most
    likely false positive. Amount equality carries the sign, which is the one
    condition this fixture fails.
    """
    settings = get_settings()
    rows = settings.doctor.duplicate_account_min_distinct_amounts
    _insert_overlap_account(doctor_db, "XFER_OUT", institution_slug="chase")
    _insert_overlap_account(doctor_db, "XFER_IN", institution_slug="chase")
    _insert_amount_ladder(doctor_db, "XFER_OUT", rows=rows, sign=-1)
    _insert_amount_ladder(doctor_db, "XFER_IN", rows=rows, sign=1)

    result = _overlap_result(doctor_db, monkeypatch)

    assert result.status == "pass"


@pytest.mark.unit
def test_the_overlap_gauge_carries_the_pair_count(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The published gauge and the reported invariant agree on one number."""
    settings = get_settings()
    rows = settings.doctor.duplicate_account_min_distinct_amounts
    _insert_overlap_account(doctor_db, "DUP_A", institution_slug="chase")
    _insert_overlap_account(doctor_db, "DUP_B", institution_slug="chase")
    _insert_amount_ladder(doctor_db, "DUP_A", rows=rows)
    _insert_amount_ladder(
        doctor_db, "DUP_B", rows=rows, day_offset=settings.matching.date_window_days
    )
    DUPLICATE_ACCOUNT_PAIRS.set(_UNSET_PAIR_GAUGE)

    result = _overlap_result(doctor_db, monkeypatch)

    assert len(result.affected_ids) == 1
    assert REGISTRY.get_sample_value(_PAIR_GAUGE_SAMPLE) == 1.0


@pytest.mark.unit
def test_a_skipped_overlap_check_leaves_the_gauge_alone(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A skip must not publish a zero that reads as "no duplicate accounts".

    The check skips whenever the core views are missing — before the first
    transform, most often. Setting the gauge outside the success path turns
    that into a confident zero on a dashboard, and a duplicate-account alert
    that silently reports "clear" while unverified is worse than one that
    reports nothing.
    """
    doctor_db.execute("DROP TABLE core.fct_transactions")
    DUPLICATE_ACCOUNT_PAIRS.set(_UNSET_PAIR_GAUGE)

    result = _overlap_result(doctor_db, monkeypatch)

    assert result.status == "skipped"
    assert REGISTRY.get_sample_value(_PAIR_GAUGE_SAMPLE) == _UNSET_PAIR_GAUGE


@pytest.mark.unit
def test_partial_overlap_below_the_ratio_passes(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two real accounts sharing some amounts by coincidence — isolates the ratio.

    Both sides clear the distinct-amount floor on their shared rows, so only
    the coverage ratio can reject them. `total` is the smallest row count whose
    shared share falls below `duplicate_account_overlap_ratio`.
    """
    settings = get_settings()
    shared = settings.doctor.duplicate_account_min_distinct_amounts
    total = math.ceil(shared / settings.doctor.duplicate_account_overlap_ratio) + 1
    private = total - shared
    _insert_overlap_account(doctor_db, "REAL_A", institution_slug="chase")
    _insert_overlap_account(doctor_db, "REAL_B", institution_slug="chase")
    for account_id, private_base in (("REAL_A", 1000), ("REAL_B", 5000)):
        _insert_amount_ladder(doctor_db, account_id, rows=shared, first_index=1)
        _insert_amount_ladder(
            doctor_db, account_id, rows=private, first_index=private_base
        )

    result = _overlap_result(doctor_db, monkeypatch)

    assert result.status == "pass"


def _insert_unioned_row(
    db: Database,
    *,
    stid: str,
    source_type: str,
    source_origin: str = "bank",
    account_id: str = "ACC1",
    amount: str = "-50.00",
    transaction_date: str = "2026-01-01",
    source_file: str | None = None,
) -> None:
    """Insert one matcher-input row into prep.int_transactions__unioned.

    Defaults put every row in one account on one date at one amount, so a test
    only has to vary the field whose narrowing clause it is exercising.
    ``source_file`` defaults to NULL, which imposes no cardinality constraint —
    matching ``assign_components``' treatment of a row with an unknown file.
    """
    db.execute(
        """
        INSERT INTO prep.int_transactions__unioned (
            source_transaction_id, account_id, source_account_key,
            transaction_date, amount, description, currency_code,
            source_type, source_origin, source_file, is_pending
        ) VALUES (?, ?, ?, ?, ?, 'Coffee', 'USD', ?, ?, ?, false)
        """,  # noqa: S608 — test input, not user data
        [
            stid,
            account_id,
            account_id,
            transaction_date,
            amount,
            source_type,
            source_origin,
            source_file,
        ],
    )


def _unproposed_result(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> InvariantResult:
    """Run the full doctor report (SQLMesh mocked) and return the unproposed check."""
    mock_ctx = _make_mock_ctx(_CLEAN_AUDITS)

    @contextmanager
    def _fake_ctx(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield mock_ctx

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", _fake_ctx)
    report = DoctorService(db).run_all()
    return next(
        r for r in report.invariants if r.name == "unproposed_cross_source_duplicates"
    )


@pytest.mark.unit
def test_cross_source_pair_with_no_decision_either_side_warns(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The 2026-08-08 shape: two sources co-resident, matcher never looked.

    Every narrowing clause is satisfied deliberately, and each by exactly one
    fixture property: one account (ACC1 both sides), differing source_type
    (ofx/csv), equal amount, zero date distance, and no app.match_decisions row
    naming either id. Exactly one pair qualifies, so a warn here cannot come
    from anywhere but the predicate under test.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="ofx1", source_type="ofx")
    _insert_unioned_row(doctor_db, stid="csv1", source_type="csv")

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.affected_ids == ["ACC1 (up to 1 unreviewed pair)"]


@pytest.mark.unit
def test_a_fresh_three_way_cluster_reports_pairs_as_an_upper_bound(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Three mutually-duplicate rows, no prior decision: 3 pairs, 2 proposals.

    The closure CTEs read *persisted* decisions, while ``assign_components``
    additionally unions candidates as it walks them inside one run. With nothing
    persisted there are no edges to read, so all three pairwise combinations
    survive, while a rematch links them into one component and writes two. The
    figure is therefore an upper bound, and the finding has to say so — otherwise
    the remedy it recommends visibly under-delivers against its own number.

    Pinned rather than corrected: the pairs genuinely carry no decision, so the
    detection is right and only the arithmetic is loose. Simulating the dynamic
    union in SQL would trade a wording problem for a correctness risk in the one
    check that caught the 2026-08-08 incident.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="ofx1", source_type="ofx")
    _insert_unioned_row(doctor_db, stid="csv1", source_type="csv")
    _insert_unioned_row(doctor_db, stid="plaid1", source_type="plaid")

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    # 3 nodes with no persisted edge → all 3 pairwise combinations survive.
    assert result.affected_ids == ["ACC1 (up to 3 unreviewed pairs)"]
    assert result.detail is not None
    assert "upper bound" in result.detail


@pytest.mark.unit
def test_unproposed_duplicates_detail_publishes_runnable_commands(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The finding's two-step remedy has to be two commands the CLI registers.

    A `doctor` finding is read by someone who already knows something is wrong;
    a name that exits 2 sends them looking for a second fault that isn't there.
    Resolves what the invariant emitted rather than the literal, so the wording
    stays free to change.
    """
    from tests.cli_command_helpers import assert_published_commands_resolve

    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="ofx1", source_type="ofx")
    _insert_unioned_row(doctor_db, stid="csv1", source_type="csv")

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.detail is not None
    assert_published_commands_resolve(result.detail)


@pytest.mark.unit
def test_cross_source_pair_the_matcher_already_ruled_on_passes(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A decision on either side means the matcher saw the pair — nothing to flag.

    The decision is `rejected`, the least favourable status for this check: the
    user looked and said no. If the invariant keyed on accepted-only it would
    re-flag a pair its owner has already dismissed, nagging forever.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="ofx1", source_type="ofx")
    _insert_unioned_row(doctor_db, stid="csv1", source_type="csv")
    doctor_db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES ('m1', 'ofx1', 'ofx', 'bank', 'csv1', 'csv', 'bank', 'ACC1',
                  0.9, '{}', 'dedup', '3', NULL, 'rejected', NULL, 'user',
                  CURRENT_TIMESTAMP)
        """  # noqa: S608 — test input, not user data
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "pass"


@pytest.mark.unit
def test_same_source_pair_with_no_decision_passes(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Within-source pairs are legitimately dropped, so their silence is not evidence.

    Tier 2b writes no row when it declines a pair (`engine._classify_pair`
    returns None), so "no decision" is the normal resting state for two rows of
    one source and must not warn. Identical to the warning fixture in every
    field but source_type — if this failed, the check would be flagging
    ordinary within-source duplicates rather than the cross-source blind spot.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="ofx1", source_type="ofx")
    _insert_unioned_row(doctor_db, stid="ofx2", source_type="ofx")

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "pass"


@pytest.mark.unit
def test_same_source_type_from_two_origins_warns(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two CSV integrations are a cross-source pair to the matcher, so also here.

    ``scoring.py::_get_candidates`` blocks Tier 3 on
    ``source_type != source_type OR source_origin != source_origin`` — the
    second half is what admits two separate CSV bank integrations, or two Plaid
    connections, as candidates. Isolated by exactly one field: identical to the
    same-source fixture above in every respect but ``source_origin``, so a warn
    here can only come from the origin half of the predicate.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(
        doctor_db, stid="csv1", source_type="csv", source_origin="bank_a"
    )
    _insert_unioned_row(
        doctor_db, stid="csv2", source_type="csv", source_origin="bank_b"
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.affected_ids == ["ACC1 (up to 1 unreviewed pair)"]


@pytest.mark.unit
def test_a_rejection_against_a_different_partner_does_not_suppress(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A rejection excludes one pair, not a transaction from all future pairs.

    The matcher's rejected-pair check is an exact tuple, and rejected edges seed
    no union-find component — so a row rejected against one partner is still a
    live candidate against every other. Suppressing at node grain would hide a
    genuinely unproposed pair behind an unrelated rejection, which is the blind
    spot this check exists to close.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="ofx1", source_type="ofx")
    _insert_unioned_row(doctor_db, stid="csv1", source_type="csv")
    # ofx1 was rejected against some *other* CSV row, not against csv1.
    doctor_db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES ('m1', 'ofx1', 'ofx', 'bank', 'csv_other', 'csv', 'bank',
                  'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'rejected', NULL,
                  'user', CURRENT_TIMESTAMP)
        """  # noqa: S608 — test input, not user data
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.affected_ids == ["ACC1 (up to 1 unreviewed pair)"]


@pytest.mark.unit
def test_a_rejection_suppresses_on_the_matchers_key_not_on_origin(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The rule: this check's suppression key must be the matcher's key.

    ``scoring.py`` builds ``rejected_set`` from ``(source_type,
    source_transaction_id)`` on both sides plus ``account_id`` — origin is
    selected onto the decision row but never enters the tuple it tests. So the
    matcher skips a rejected pair whatever origin the rows now carry. A check
    that additionally demands origin equality warns about a pair the matcher
    will never propose, and the refresh it recommends cannot clear it.

    Here the rejection was recorded against the same two nodes under a different
    origin, which is reachable whenever a source-native id is reused across
    origins within one account.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="ofx1", source_type="ofx", source_origin="a")
    _insert_unioned_row(doctor_db, stid="csv1", source_type="csv", source_origin="b")
    doctor_db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES ('m1', 'ofx1', 'ofx', 'bank', 'csv1', 'csv', 'bank',
                  'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'rejected', NULL,
                  'user', CURRENT_TIMESTAMP)
        """  # noqa: S608 — test input, not user data
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "pass", (
        "warned about a pair the matcher's rejected-pair test already excludes"
    )


@pytest.mark.unit
def test_an_accepted_decision_on_only_one_side_does_not_suppress(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A node in an existing component is not globally claimed.

    When A–B is already matched and a merge makes a third copy C co-resident,
    ``assign_components`` attaches C with a new edge wherever source cardinality
    allows. Suppressing the A–C candidate because A already appears in some
    component would hide the newly co-resident row this check exists to find —
    the same class of silence as the original incident.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="ofx1", source_type="ofx")
    _insert_unioned_row(doctor_db, stid="csv1", source_type="csv")
    doctor_db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES ('m1', 'ofx1', 'ofx', 'bank', 'csv_other', 'csv', 'bank',
                  'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'accepted', NULL,
                  'auto', CURRENT_TIMESTAMP)
        """  # noqa: S608 — test input, not user data
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.affected_ids == ["ACC1 (up to 1 unreviewed pair)"]


@pytest.mark.unit
def test_decisions_in_disjoint_components_do_not_suppress(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both endpoints decided is not redundancy — same component is.

    ``assign_components`` skips an edge only on ``find(a) == find(b)``
    (``assignment.py``). Two rows each carrying an unrelated accepted decision
    sit in *disjoint* components, so the matcher would still evaluate the edge
    between them. Suppressing it would hide a live unproposed pair — the exact
    silence this invariant exists to break, relocated into the invariant itself.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="ofx1", source_type="ofx")
    _insert_unioned_row(doctor_db, stid="csv1", source_type="csv")
    doctor_db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES ('m1', 'ofx1', 'ofx', 'bank', 'csv_other', 'csv', 'bank',
                  'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'accepted', NULL,
                  'auto', CURRENT_TIMESTAMP),
                 ('m2', 'csv1', 'csv', 'bank', 'ofx_other', 'ofx', 'bank',
                  'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'accepted', NULL,
                  'auto', CURRENT_TIMESTAMP)
        """  # noqa: S608 — test input, not user data
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.affected_ids == ["ACC1 (up to 1 unreviewed pair)"]


@pytest.mark.unit
def test_two_rows_already_in_one_component_suppress(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The redundant edge the matcher genuinely drops, and the only one.

    ``ofx1`` and ``csv1`` are connected transitively through ``mid`` with no
    decision naming the pair itself. ``find(ofx1) == find(csv1)``, so
    ``assign_components`` skips the edge as redundant — and the invariant must
    stay quiet, or every collapsed duplicate group nags forever. Differs from
    the disjoint fixture above by exactly one property: whether the two
    decisions share an endpoint.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="ofx1", source_type="ofx")
    _insert_unioned_row(doctor_db, stid="csv1", source_type="csv")
    doctor_db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES ('m1', 'ofx1', 'ofx', 'bank', 'mid', 'plaid', 'bank',
                  'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'accepted', NULL,
                  'auto', CURRENT_TIMESTAMP),
                 ('m2', 'csv1', 'csv', 'bank', 'mid', 'plaid', 'bank',
                  'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'accepted', NULL,
                  'auto', CURRENT_TIMESTAMP)
        """  # noqa: S608 — test input, not user data
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "pass"


_TWO_PAIRED_COMPONENTS = """
    INSERT INTO app.match_decisions (
        match_id, source_transaction_id_a, source_type_a, source_origin_a,
        source_transaction_id_b, source_type_b, source_origin_b,
        account_id, confidence_score, match_signals, match_type, match_tier,
        account_id_b, match_status, match_reason, decided_by, decided_at
    ) VALUES ('m1', 'csv1', 'csv', 'bank', 'ofx1', 'ofx', 'bank',
              'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'accepted', NULL,
              'auto', CURRENT_TIMESTAMP),
             ('m2', 'csv2', 'csv', 'bank', 'ofx2', 'ofx', 'bank',
              'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'accepted', NULL,
              'auto', CURRENT_TIMESTAMP)
"""  # noqa: S608 — test input, not user data


@pytest.mark.unit
def test_two_components_sharing_a_physical_source_suppress(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The cardinality guard's pairs, which no refresh could ever clear.

    ``assign_components`` rejects an edge whose two components already hold a
    row from one ``(source_type, source_origin, source_file)`` — two rows of a
    single import file are distinct transactions by construction, so they must
    never land in one component. Here one CSV and one OFX file each contribute
    both of their rows, and the matcher has already paired them 1:1, so the two
    *cross* edges are precisely what the guard drops. Warning about them would
    nag forever: the remedy this check recommends is a refresh, and a refresh
    re-drops them.

    Differs from ``test_decisions_in_disjoint_components_do_not_suppress`` by
    exactly one property — whether the two components share a physical source.
    """
    _seed_prep_unioned(doctor_db, 0)
    for stid, source_type, source_file in (
        ("ofx1", "ofx", "jan.ofx"),
        ("ofx2", "ofx", "jan.ofx"),
        ("csv1", "csv", "march.csv"),
        ("csv2", "csv", "march.csv"),
    ):
        _insert_unioned_row(
            doctor_db, stid=stid, source_type=source_type, source_file=source_file
        )
    doctor_db.execute(_TWO_PAIRED_COMPONENTS)

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "pass"


@pytest.mark.unit
def test_a_shared_file_on_two_seed_only_rows_does_not_suppress(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guard reads the sources the matcher would register, not every row.

    ``assign_components`` seeds ``comp_sources`` from the endpoints of the
    current run's candidate pairs only — a node in a component that no
    candidate names contributes nothing and never blocks (``assignment.py``).
    Here ``ofx1``–``csv_seed`` and ``plaid1``–``csv_seed2`` are existing
    components, and the one live candidate is ``ofx1``–``plaid1``: the two
    ``csv`` rows sit at a different amount, so they pair with nothing. They
    share ``march.csv``, and a guard reading whole components would intersect
    on it and stay silent about a pair the next match pass would propose and
    persist — a false negative in the check whose entire job is to break that
    silence.

    Differs from ``test_two_components_sharing_a_physical_source_suppress`` by
    exactly one property: whether the rows supplying the shared file are
    themselves candidates.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(
        doctor_db, stid="ofx1", source_type="ofx", source_file="jan.ofx"
    )
    _insert_unioned_row(
        doctor_db, stid="plaid1", source_type="plaid", source_file="feb.json"
    )
    for stid in ("csv_seed", "csv_seed2"):
        _insert_unioned_row(
            doctor_db,
            stid=stid,
            source_type="csv",
            source_file="march.csv",
            amount="-11.00",
        )
    doctor_db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES ('m1', 'ofx1', 'ofx', 'bank', 'csv_seed', 'csv', 'bank',
                  'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'accepted', NULL,
                  'auto', CURRENT_TIMESTAMP),
                 ('m2', 'plaid1', 'plaid', 'bank', 'csv_seed2', 'csv', 'bank',
                  'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'accepted', NULL,
                  'auto', CURRENT_TIMESTAMP)
        """  # noqa: S608 — test input, not user data
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.affected_ids == ["ACC1 (up to 1 unreviewed pair)"]


@pytest.mark.unit
def test_a_rejected_pairs_own_endpoints_register_no_source_guard(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A rejected pair is dropped before the matcher registers anything.

    ``scoring.py``'s candidate functions ``continue`` past a rejected pair
    before appending it to ``results``, and ``assign_components`` seeds
    ``comp_sources`` from that list — so both endpoints of a rejected pair are
    seed-only to the matcher, a second reason a node can be seed-only beyond
    the one ``test_a_shared_file_on_two_seed_only_rows_does_not_suppress``
    covers.

    Here ``ofx_e``–``csv_y`` is rejected, ``ofx_e``–``plaid_b`` is an accepted
    component, and the one live candidate is ``plaid_b``–``ofx_c``. ``ofx_c``
    shares ``shared.ofx`` with ``ofx_e``, so registering the rejected row's
    endpoints puts that file under ``comp(ofx_e, plaid_b)``, where it
    intersects ``comp(ofx_c)`` and suppresses a pair the next match pass would
    propose and persist.

    The two amounts keep each blocking-eligible group to exactly one pair, so
    the count below is the live candidate alone. The accepted decision joining
    ``ofx_e`` and ``plaid_b`` needs no blocking eligibility — a component is
    built from decisions, not from candidates.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(
        doctor_db, stid="ofx_e", source_type="ofx", source_file="shared.ofx"
    )
    _insert_unioned_row(doctor_db, stid="csv_y", source_type="csv", source_file="y.csv")
    _insert_unioned_row(
        doctor_db,
        stid="plaid_b",
        source_type="plaid",
        source_file="b.json",
        amount="-11.00",
    )
    _insert_unioned_row(
        doctor_db,
        stid="ofx_c",
        source_type="ofx",
        source_file="shared.ofx",
        amount="-11.00",
    )
    doctor_db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES ('m1', 'csv_y', 'csv', 'bank', 'ofx_e', 'ofx', 'bank',
                  'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'rejected', NULL,
                  'user', CURRENT_TIMESTAMP),
                 ('m2', 'ofx_e', 'ofx', 'bank', 'plaid_b', 'plaid', 'bank',
                  'ACC1', 0.9, '{}', 'dedup', '3', NULL, 'accepted', NULL,
                  'auto', CURRENT_TIMESTAMP)
        """  # noqa: S608 — test input, not user data
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.affected_ids == ["ACC1 (up to 1 unreviewed pair)"]


@pytest.mark.unit
def test_one_matcher_node_split_across_origins_is_not_a_pair(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two origins, one matcher node — `_node_a(pair) == _node_b(pair)`.

    `NodeKey` is `(source_type, source_transaction_id, account_id)`
    (`assignment.py:86-100`) and carries no `source_origin`, so these two rows
    are a single node to the matcher: `find(a) == find(b)` holds before any
    edge is considered and `assign_components` drops the candidate without ever
    writing a decision. A node key that added `source_origin` would see two
    distinct undecided nodes and warn about a pair no refresh can clear.

    Staging keeps this out of production — `stg_tabular__transactions` and
    `stg_ofx__transactions` both dedup on `(transaction_id, account_id)` with no
    origin — so the fixture inserts into `prep.int_transactions__unioned`
    directly. The check must still agree with the matcher on it: this pins the
    two node keys together rather than relying on that staging behaviour.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(
        doctor_db, stid="shared1", source_type="csv", source_origin="bank_a"
    )
    _insert_unioned_row(
        doctor_db, stid="shared1", source_type="csv", source_origin="bank_b"
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "pass"


@pytest.mark.unit
def test_two_components_from_four_files_still_warn(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same shape with no shared file — the guard must not swallow this.

    Identical to the fixture above in every respect except the file names: each
    row now comes from its own import file, so ``sources_a & sources_b`` is
    empty and ``assign_components`` would genuinely evaluate both cross edges.
    Without this partner, a suppression that keyed on "both endpoints are in
    *some* component" would pass the test above and silently reinstate the
    blind spot this invariant exists to close.
    """
    _seed_prep_unioned(doctor_db, 0)
    for stid, source_type, source_file in (
        ("ofx1", "ofx", "jan.ofx"),
        ("ofx2", "ofx", "feb.ofx"),
        ("csv1", "csv", "march.csv"),
        ("csv2", "csv", "april.csv"),
    ):
        _insert_unioned_row(
            doctor_db, stid=stid, source_type=source_type, source_file=source_file
        )
    doctor_db.execute(_TWO_PAIRED_COMPONENTS)

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.affected_ids == ["ACC1 (up to 2 unreviewed pairs)"]


@pytest.mark.unit
def test_a_transfer_decision_does_not_count_as_dedup_consideration(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tier 4 runs after Tier 3 and never consults it, so a transfer proves nothing.

    A transfer decision says the row was paired *across* accounts, not that the
    dedup blocking join this invariant mirrors ever looked at it. Counting one
    as "already decided" would suppress a genuine warning — and on a transfer
    row ``account_id`` names only side A, so even the account correlation would
    not save it.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="ofx1", source_type="ofx")
    _insert_unioned_row(doctor_db, stid="csv1", source_type="csv")
    doctor_db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES ('m1', 'ofx1', 'ofx', 'bank', 'elsewhere', 'ofx', 'bank',
                  'ACC1', 0.9, '{}', 'transfer', NULL, 'ACC9', 'accepted',
                  NULL, 'auto', CURRENT_TIMESTAMP)
        """  # noqa: S608 — test input, not user data
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.affected_ids == ["ACC1 (up to 1 unreviewed pair)"]


@pytest.mark.unit
def test_another_accounts_decision_on_the_same_native_id_does_not_suppress(
    doctor_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A source-native id is only unique within its account, so scope the lookup.

    ``app.match_decisions.account_id`` is the "shared account (blocking
    requirement for dedup)", and the matcher's own rejected-pair identity
    (``scoring.py``) carries ``account_id`` for the same reason. An unrelated
    account holding a decision for an identically-spelled FITID must not mark
    this account's row as spoken for — that would silently suppress the warning
    on exactly the pair this check exists to surface.
    """
    _seed_prep_unioned(doctor_db, 0)
    _insert_unioned_row(doctor_db, stid="shared_id", source_type="ofx")
    _insert_unioned_row(doctor_db, stid="csv1", source_type="csv")
    doctor_db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES ('m1', 'shared_id', 'ofx', 'bank', 'other', 'csv', 'bank',
                  'ACC2', 0.9, '{}', 'dedup', '3', NULL, 'accepted', NULL,
                  'auto', CURRENT_TIMESTAMP)
        """  # noqa: S608 — test input, not user data
    )

    result = _unproposed_result(doctor_db, monkeypatch)

    assert result.status == "warn"
    assert result.affected_ids == ["ACC1 (up to 1 unreviewed pair)"]


@pytest.mark.unit
def test_unproposed_check_skips_without_echoing_the_raw_cause() -> None:
    """A crashed matcher-input query must not put DuckDB's text on the wire.

    ``detail`` is returned verbatim by ``doctor`` and ``system_status`` over
    both the CLI and MCP surfaces, and this query joins on transaction amounts,
    dates, and descriptions — so a conversion failure can carry a user's row
    into its message. Every other crash branch this feature added routes the
    cause to the local log and returns a fixed string; this one must too.
    """
    import duckdb

    db = MagicMock()
    db.execute.side_effect = duckdb.ConversionException(
        'Could not convert string "SAFEWAY #1234 -81.27" to DECIMAL'
    )

    result = DoctorService(db)._run_unproposed_cross_source_duplicates()  # pyright: ignore[reportPrivateUsage]

    assert result.status == "skipped"
    assert result.detail is not None
    assert "SAFEWAY" not in result.detail
    assert "81.27" not in result.detail
