"""Scenario: reconciliation status discloses a mixed-source account end to end.

=============================================================================
HAND-DERIVATION (independent of pipeline output)
=============================================================================
Home currency USD. Two investment accounts.

``acc_choice`` (overlapping — manual AND Plaid history):
  CHC (USD): manual buy 10 @ 100 on 2026-01-05, Plaid buy 10 @ 100 on the same
             day (the duplicate), manual sell 4 @ 120 on 2026-03-05.
  EUR fund : manual buy 1 for 25.00 EUR on 2026-02-01, manual sell 1 for
             30.00 EUR on 2026-04-01 (opens a 30.00 EUR currency lot).
  Cash     : a manual conversion on 2026-05-01, -30.00 EUR -> +48.00 USD,
             disposes the whole EUR currency lot (one FX gain).
  Before the source choice the ledger carries two source types, so every row
  of every model for this account is ``unreconciled``:
    ledger rows   = 5 (CHC manual buy, CHC Plaid buy, CHC sell, EUR buy, EUR sell)
    lots          = 3 (CHC manual, CHC Plaid, EUR)
    realized gains= 2 (CHC sell consumes 4 units from one 10-unit lot; EUR sell)
    CHC holding   = 10 + 10 - 4 = 16 units, valued at today's 120.00 close
                    = 1920.00 — published, never blanked.
  After ``investment_source_type = 'manual'`` the Plaid buy drops out: every
  row is ``reconciled``; CHC holds 10 - 4 = 6 units = 720.00.

``acc_clean_manual`` (manual only):
  CHC manual buy 10 @ 100, manual sell 4 @ 120. Always ``reconciled``; its
  lots, gains and holdings must not change when the other account's source
  is chosen.

Bundle manifest ``unreconciled_row_count`` before = the overlapping account's
counts above (ledger 5, lots 3, gains 2, holdings = its holding rows); after
the choice every carrier table still has rows (the clean account plus the
reconciled overlapping account) and reports 0.
=============================================================================
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.exports.snapshot import build_bundle_snapshot
from moneybin.services.account_service import AccountService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.investment_service import InvestmentService
from moneybin.services.transform_service import TransformService
from tests.moneybin.test_investments._overlap_seed import (
    ACCOUNT,
    seed_manual_buy,
    seed_manual_sell,
    seed_plaid_account,
    seed_plaid_buy,
    seed_security,
)
from tests.moneybin.test_investments.comparison_helpers import seed_account_metadata
from tests.scenarios._investments_seed import insert_event, insert_security
from tests.scenarios._runner import load_shipped_scenario, scenario_env

CLEAN = "acc_clean_manual"
_EUR_SECURITY = "sec_eur_fund"

_CORE_MODELS = (
    "core.fct_investment_transactions",
    "core.fct_investment_lots",
    "core.fct_realized_gains",
    "core.dim_holdings",
    "core.fct_currency_lots",
    "core.fct_realized_fx_gains",
)
_RAW_TABLES = (
    "raw.manual_investment_transactions",
    "raw.plaid_investment_transactions",
    "raw.plaid_investment_transaction_receipts",
    "raw.plaid_accounts",
    "raw.manual_transactions",
    "raw.security_prices",
    "raw.exchange_rates",
)
_CARRIER_BUNDLE_TABLES = (
    "investment_transactions",
    "investment_lots",
    "realized_gains",
    "holdings",
)
# Hand-derived above; holdings is checked against its own rows.
_EXPECTED_UNRECONCILED_BEFORE = {
    "investment_transactions": 5,
    "investment_lots": 3,
    "realized_gains": 2,
}
_CLEAN_PROJECTIONS = {
    table: (
        f"SELECT * EXCLUDE (reconciliation_status, updated_at) FROM {table} "  # noqa: S608  # fixed table names from this module
        "WHERE account_id = ? ORDER BY ALL"
    )
    for table in (
        "core.fct_investment_lots",
        "core.fct_realized_gains",
        "core.dim_holdings",
    )
}


def _seed(db: Database) -> None:
    db.execute(
        """
        UPDATE app.profile_settings
        SET home_currency = 'USD', updated_at = TIMESTAMPTZ '2025-12-01 09:00:00+00:00'
        WHERE scope = 'profile'
        """
    )
    seed_security(db)
    seed_plaid_account(db)
    seed_manual_buy(db, account_id=ACCOUNT, txn_id="overlap_manual_buy")
    seed_manual_sell(db, account_id=ACCOUNT, txn_id="overlap_manual_sell")
    seed_plaid_buy(db, loaded_at="2026-02-01 09:00:00")

    insert_security(
        db,
        security_id=_EUR_SECURITY,
        name="Euro Fund",
        security_type="equity",
        currency_code="EUR",
    )
    insert_event(
        db,
        investment_transaction_id="overlap_eur_buy",
        account_id=ACCOUNT,
        security_id=_EUR_SECURITY,
        type_="buy",
        trade_date="2026-02-01",
        quantity="1",
        amount="-25.00",
        fees="0.00",
        currency_code="EUR",
        created_at="2026-02-02 09:00:00",
    )
    insert_event(
        db,
        investment_transaction_id="overlap_eur_sell",
        account_id=ACCOUNT,
        security_id=_EUR_SECURITY,
        type_="sell",
        trade_date="2026-04-01",
        quantity="-1",
        amount="30.00",
        fees="0.00",
        currency_code="EUR",
        created_at="2026-04-02 09:00:00",
    )
    db.execute(
        """
        INSERT INTO raw.exchange_rates (
            from_currency, to_currency, rate_date, rate, source_type, loaded_at
        ) VALUES
            ('EUR', 'USD', '2026-04-01'::DATE, 1.50000000,
             'frankfurter', '2026-04-02 12:00:00+00:00'::TIMESTAMPTZ),
            ('EUR', 'USD', '2026-05-01'::DATE, 1.60000000,
             'frankfurter', '2026-05-02 12:00:00+00:00'::TIMESTAMPTZ)
        """
    )
    # A single-row conversion back to home currency disposes the EUR lot.
    db.execute(
        """
        INSERT INTO raw.manual_transactions (
            source_transaction_id, import_id, account_id, transaction_date,
            amount, to_amount, description, currency_code, to_currency,
            created_at, created_by
        ) VALUES ('overlap_eur_to_usd', 'import-fx', ?, '2026-05-01'::DATE,
                  -30.00, 48.00, 'FX conversion', 'EUR', 'USD',
                  '2026-05-02 10:00:00+00:00'::TIMESTAMPTZ, 'cli')
        """,
        [ACCOUNT],
    )

    seed_account_metadata(db, CLEAN)
    seed_manual_buy(db, account_id=CLEAN, txn_id="clean_buy")
    seed_manual_sell(db, account_id=CLEAN, txn_id="clean_sell")


def _apply(db: Database) -> None:
    result = TransformService(db).apply()
    assert result.applied, f"transform failed: {result.error}"


def _count(db: Database, table: str, account_id: str) -> int:
    row = db.execute(
        f"SELECT count(*) FROM {table} WHERE account_id = ?",  # noqa: S608  # fixed table names from this module
        [account_id],
    ).fetchone()
    assert row is not None
    return int(row[0])


def _statuses(db: Database, table: str, account_id: str) -> set[str]:
    rows = db.execute(
        f"SELECT DISTINCT reconciliation_status FROM {table} WHERE account_id = ?",  # noqa: S608  # fixed table names from this module
        [account_id],
    ).fetchall()
    return {str(r[0]) for r in rows}


def _raw_counts(db: Database) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in _RAW_TABLES:
        row = db.execute(f"SELECT count(*) FROM {table}").fetchone()  # noqa: S608  # fixed table names from this module
        assert row is not None
        counts[table] = int(row[0])
    return counts


def _manifest(db: Database) -> dict[str, dict[str, object]]:
    snap = build_bundle_snapshot(db, profile="scenario", created_at=datetime.now(UTC))
    tables = snap.manifest["tables"]
    assert isinstance(tables, list)
    return {str(t["name"]): t for t in tables}  # type: ignore[index]


def _chc_market_value(db: Database) -> Decimal | None:
    row = db.execute(
        """
        SELECT market_value FROM core.dim_holdings
        WHERE account_id = ? AND security_id = 'sec_choice_0001'
        """,
        [ACCOUNT],
    ).fetchone()
    assert row is not None, "no CHC holding for the overlapping account"
    return row[0]


def _clean_snapshot(db: Database) -> dict[str, list[tuple[object, ...]]]:
    return {
        table: db.execute(sql, [CLEAN]).fetchall()
        for table, sql in _CLEAN_PROJECTIONS.items()
    }


@pytest.mark.scenarios
@pytest.mark.slow
def test_reconciliation_status_discloses_and_clears_end_to_end() -> None:
    """Every overlapping row is marked; choosing a source clears every surface."""
    scenario = load_shipped_scenario("investments-reconciliation-status")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, _env):
        _seed(db)
        _apply(db)

        # --- Before: every overlapping row is unreconciled, clean rows are not.
        for table in _CORE_MODELS:
            assert _count(db, table, ACCOUNT) >= 1, f"no {table} rows seeded"
            assert _statuses(db, table, ACCOUNT) == {"unreconciled"}, table
            if table not in ("core.fct_currency_lots", "core.fct_realized_fx_gains"):
                assert _statuses(db, table, CLEAN) == {"reconciled"}, table
        assert _count(db, "core.fct_investment_transactions", ACCOUNT) == 5
        assert _count(db, "core.fct_investment_lots", ACCOUNT) == 3
        assert _count(db, "core.fct_realized_gains", ACCOUNT) == 2
        # Disclosure, not withholding: the doubled position still publishes.
        assert _chc_market_value(db) == Decimal("1920.00")
        # The user-facing FX report carries the marker its core model does.
        assert _statuses(db, "reports.realized_fx", ACCOUNT) == {"unreconciled"}

        service = InvestmentService(db)
        reads = {
            "holdings": service.holdings(),
            "lots": service.lots(open_only=False),
            "gains": service.gains(),
        }
        for name, result in reads.items():
            reason = result.degraded_reason
            assert reason is not None, f"{name} read not degraded"
            assert reason.startswith("investment_source_overlap:"), name
            assert "1 account(s)" in reason, name

        manifest = _manifest(db)
        for name, expected in _EXPECTED_UNRECONCILED_BEFORE.items():
            assert manifest[name]["unreconciled_row_count"] == expected, name
        holdings_unreconciled = manifest["holdings"]["unreconciled_row_count"]
        assert holdings_unreconciled == _count(db, "core.dim_holdings", ACCOUNT)
        assert isinstance(holdings_unreconciled, int) and holdings_unreconciled > 0
        assert "unreconciled_row_count" not in manifest["transactions"]

        clean_before = _clean_snapshot(db)
        assert all(clean_before.values()), "clean account has no derived rows"
        raw_before = _raw_counts(db)

        # --- The remedy: choose one source, then refresh.
        AccountService(db).settings_update(
            ACCOUNT,
            actor="scenario",
            investment_source_type="manual",
            verify_source_choice=InvestmentMembershipService(db)
            .preview_source_choice(ACCOUNT, "manual")
            .verify,
        )
        _apply(db)

        for table in _CORE_MODELS:
            assert _count(db, table, ACCOUNT) >= 1, f"{table} lost its rows"
            assert _statuses(db, table, ACCOUNT) == {"reconciled"}, table
        assert _count(db, "core.fct_investment_transactions", ACCOUNT) == 4
        assert _statuses(db, "reports.realized_fx", ACCOUNT) == {"reconciled"}
        assert _chc_market_value(db) == Decimal("720.00")

        service = InvestmentService(db)
        assert service.holdings().degraded_reason is None
        assert service.lots(open_only=False).degraded_reason is None
        assert service.gains().degraded_reason is None

        manifest = _manifest(db)
        for name in _CARRIER_BUNDLE_TABLES:
            entry = manifest[name]
            row_count = entry["row_count"]
            assert isinstance(row_count, int) and row_count > 0, name
            # Rows present, all reconciled: the count excludes reconciled rows.
            assert entry["unreconciled_row_count"] == 0, name

        assert _clean_snapshot(db) == clean_before
        assert _raw_counts(db) == raw_before
