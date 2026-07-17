"""Integration test: DoctorService's real SQLMesh standalone-audit wiring.

Guards the audit-revival regression: every file in
``src/moneybin/sqlmesh/audits/`` silently lacked ``standalone TRUE``, so
SQLMesh loaded them as *generic* (model-attached) audits that no model
referenced — ``moneybin doctor`` reported zero SQLMesh invariants for as long
as the audits existed, and three shipped audits never once executed against a
real database.

``make check test`` and ``make test-scenarios`` could not have caught this:
standalone audits are non-blocking by SQLMesh's own design (a violation
routes to ``console.log_warning``, never raises — see
``sqlmesh/core/scheduler.py``'s ``audit_errors_to_warn`` handling), so no
suite could go red from a failing one, and nothing in either suite invokes
the doctor. The only test that can catch a dead audit is one that drives the
real discovery path and asserts on what it finds — this is that test.

Two assertions matter, run against a real (non-mocked) ``sqlmesh_context``:

1. Every audit file under ``src/moneybin/sqlmesh/audits/`` is discovered — its name
   appears in ``DoctorService``'s report. An audit that loses its
   ``standalone TRUE`` line disappears from ``ctx.standalone_audits``
   *entirely*; it produces NO ``InvariantResult``, not a failing one. This
   is the assertion that actually would have caught the original bug.
2. Every discovered audit reports ``status == "pass"`` against data
   engineered to satisfy the convention it checks — including a legitimate
   $0.00 transaction, which ``fct_transactions_sign_convention`` must NOT
   flag (see that audit file's header for why zero is a modeled state, not
   a defect).

Verified by temporarily removing ``standalone TRUE`` from one audit file and
confirming assertion 1 goes red before restoring it.
"""

from __future__ import annotations

import re

import pytest

from moneybin.database import SQLMESH_ROOT, Database
from moneybin.services.doctor_service import DoctorService
from tests.moneybin.db_helpers import (
    CORE_FCT_INVESTMENT_TRANSACTIONS_DDL,
    create_core_tables,
)

pytestmark = pytest.mark.integration

_AUDITS_DIR = SQLMESH_ROOT / "audits"


def _discover_audit_names() -> set[str]:
    """The audit names declared in ``src/moneybin/sqlmesh/audits/*.sql``.

    Ground truth independent of what ``DoctorService`` reports — derived by
    parsing the real files, not by hardcoding a name list that could drift
    out of sync (and silently stop guarding new audits added later).
    """
    names: set[str] = set()
    for path in sorted(_AUDITS_DIR.glob("*.sql")):
        header = path.read_text().partition(";")[0]
        match = re.search(r"name\s+(\w+)", header)
        assert match, f"{path}: could not parse an AUDIT name from the header"
        names.add(match.group(1))
    return names


def _seed_clean_data(db: Database) -> None:
    """Populate every table the standalone audits read.

    Rows are engineered to satisfy each audit's convention — including a
    legitimate $0.00 transaction (``T_ZERO``), a balanced transfer pair, and
    a correctly-signed buy/sell — so a clean report means the audits are
    wired up AND agree with production's own conventions.
    """
    create_core_tables(db)
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    db.execute(
        """
        INSERT INTO core.dim_accounts (
            account_id, routing_number, account_type, institution_name,
            institution_fid, source_type, source_file, extracted_at, loaded_at,
            updated_at, display_name, currency_code,
            archived, include_in_net_worth
        ) VALUES ('ACC1', '111', 'CHECKING', 'Bank', 'fid', 'ofx',
                  'a.qfx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                  CURRENT_TIMESTAMP, 'Bank CHECKING', 'USD', FALSE, TRUE)
        """  # noqa: S608 — test input, not user data
    )
    db.execute(
        """
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month,
            transaction_year_quarter
        ) VALUES
        ('T_NORMAL', 'ACC1', '2026-01-01', -50.00, 50.00, 'expense', 'Coffee',
         'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         2026, 1, 1, 3, '2026-01', '2026-Q1'),
        ('T_ZERO', 'ACC1', '2026-01-02', 0.00, 0.00, 'zero', 'Waived fee',
         'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         2026, 1, 2, 4, '2026-01', '2026-Q1'),
        ('T_DEBIT', 'ACC1', '2026-01-03', -100.00, 100.00, 'expense',
         'Transfer out', 'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP,
         CURRENT_TIMESTAMP, 2026, 1, 3, 5, '2026-01', '2026-Q1'),
        ('T_CREDIT', 'ACC1', '2026-01-03', 100.00, 100.00, 'income',
         'Transfer in', 'CREDIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP,
         CURRENT_TIMESTAMP, 2026, 1, 3, 5, '2026-01', '2026-Q1')
        """  # noqa: S608 — test input, not user data
    )
    db.execute(
        """
        INSERT INTO core.bridge_transfers
            (transfer_id, debit_transaction_id, credit_transaction_id,
             date_offset_days, amount)
        VALUES ('XFER1', 'T_DEBIT', 'T_CREDIT', 0, 100.00)
        """  # noqa: S608 — test input, not user data
    )
    db.execute(
        """
        INSERT INTO core.fct_investment_transactions
            (investment_transaction_id, account_id, security_id, type, amount)
        VALUES
            ('INV_BUY', 'ACC1', 'SEC1', 'buy', -500.00),
            ('INV_SELL', 'ACC1', 'SEC1', 'sell', 600.00)
        """  # noqa: S608 — test input, not user data
    )


def test_standalone_audits_are_all_discovered_and_pass_on_clean_data(
    db: Database,
) -> None:
    expected_names = _discover_audit_names()
    assert expected_names, (
        "no audit files under src/moneybin/sqlmesh/audits/ — fixture is broken"
    )

    _seed_clean_data(db)
    report = DoctorService(db).run_all(verbose=True)
    observed = {r.name: r for r in report.invariants}

    # Assertion 1 — the actual regression guard. A non-standalone audit
    # never reaches ctx.standalone_audits, so it never produces an
    # InvariantResult at all; a missing name here IS the bug this test
    # exists to catch (verified by removing `standalone TRUE` from a file
    # and re-running — see module docstring).
    missing = expected_names - observed.keys()
    assert not missing, (
        f"{missing} declared in src/moneybin/sqlmesh/audits/ but absent from "
        f"DoctorService's "
        "report — check each file has `standalone TRUE` in its AUDIT(...) header"
    )

    # Assertion 2 — every discovered audit is clean against data engineered
    # to satisfy its convention, including the T_ZERO row (a legitimate
    # $0.00 transaction that must NOT be flagged as a sign-convention defect).
    for name in expected_names:
        result = observed[name]
        assert result.status == "pass", (
            f"{name}: expected pass, got {result.status} "
            f"(detail={result.detail!r}, affected_ids={result.affected_ids})"
        )
