"""Scenario: five accounts in five currencies keep their money apart.

`test_multi_currency_report_segmentation` proves segmentation through the Plaid
loader, where three currencies sit on a single account and each transaction
carries its own. This is the other half of the same invariant: the currency
rides on the *account*, five accounts hold five of them, and they arrive
through two different raw tables — `raw.ofx_balances.currency_code` for the OFX
accounts, `raw.tabular_accounts.currency` for the tabular ones. A persona
weighted to one source type would leave the other's threading unproven, so
`international` deliberately spans both.

Ground truth is derived from the deterministic `GeneratorEngine` — the
"persona / generator config" path in `.claude/rules/testing.md` — and never
from a report. What the persona declares (opening balances) plus what the
generator emits (transactions) fixes every closing figure by arithmetic. The
pipeline under test is everything downstream: both writers, the staging
currency capture, `core.fct_balances_daily`, and the `reports.net_worth`
GROUP BY.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.repositories.profile_settings_repo import ProfileSettingsRepo
from moneybin.services.networth_service import NetworthService
from moneybin.synthetic.engine import GeneratorEngine
from moneybin.synthetic.models import GenerationResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario, scenario_env
from tests.scenarios._runner.steps import run_step
from tests.scenarios._tier1_backfill import tier1_backfill
from tests.validation.result import AssertionResult

# Position per currency: (net_worth, account_count). The key is nullable
# because `reports.net_worth` pools every unknown-currency account into one
# NULL-coded segment — this persona must never produce one, and a `str`-only
# alias would make that assertion unwritable.
_Positions = dict[str | None, tuple[Decimal, int]]

_FX_RESTATE_MODELS = [
    "core.bridge_currency_conversions",
    "core.fct_currency_lots",
    "core.fct_realized_fx_gains",
]


def _expected_positions(result: GenerationResult, as_of: date) -> _Positions:
    """Net worth and account count per currency on `as_of`, by arithmetic.

    Both writers anchor an account at its declared opening balance before any
    activity and then emit its transactions, so the balance on any date is
    their running sum. No two accounts share a currency, so a currency's total
    is exactly its one account's, including its side of each explicit transfer.

    `as_of` is read from the report rather than assumed to be the generator's
    last day. `core.fct_balances_daily` ends its spine at the newest balance
    *observation*, and the synthetic OFX writer emits exactly one — so the
    report's latest date is set by whichever tabular account has the latest
    transaction, a few days short of the run. That gap is a filed follow-up,
    not this scenario's subject: the money is still derived here, only the
    window is taken from the report.
    """
    currency_of = {account.name: account.currency_code for account in result.accounts}
    flows: dict[str, Decimal] = defaultdict(Decimal)
    for txn in result.transactions:
        if txn.date <= as_of:
            flows[currency_of[txn.account_name]] += txn.amount

    counts: dict[str, int] = defaultdict(int)
    opening: dict[str, Decimal] = defaultdict(Decimal)
    for account in result.accounts:
        counts[account.currency_code] += 1
        opening[account.currency_code] += account.opening_balance

    return {code: (opening[code] + flows[code], counts[code]) for code in counts}


@pytest.mark.scenarios
@pytest.mark.slow
def test_international_multi_currency() -> None:
    """Each currency reports its own position; none of them blend."""
    scenario = load_shipped_scenario("international-multi-currency")
    assert scenario is not None
    setup = scenario.setup
    base = tier1_backfill(setup)

    generated = GeneratorEngine(
        setup.persona, seed=setup.seed, years=setup.years
    ).generate()
    currencies = {account.currency_code for account in generated.accounts}
    assert len(currencies) == 5, (
        f"the persona must declare five distinct currencies, got {sorted(currencies)}"
    )

    def extra(db: Database) -> list[AssertionResult]:
        as_of = _report_as_of(db)
        expected = _expected_positions(generated, as_of)
        return [
            *base(db),
            _segments_hold_their_own_currency(db, expected, as_of),
            _the_headline_refuses_to_blend(db, expected),
        ]

    result = run_scenario(scenario, extra_assertions=extra)
    assert result.passed, result.failure_summary()


@pytest.mark.scenarios
@pytest.mark.slow
def test_international_realized_fx_ground_truth() -> None:
    """Each completed month realizes exactly five Home-currency dollars."""
    scenario = load_shipped_scenario("international-multi-currency")
    assert scenario is not None
    setup = scenario.setup
    generated = GeneratorEngine(
        setup.persona, seed=setup.seed, years=setup.years
    ).generate()
    completed_months = setup.years * 12
    expected_pairs = completed_months * 2

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", setup, db, env=env)
        run_step("transform", setup, db, env=env)

        ProfileSettingsRepo(db).set_home_currency("USD", actor="system")
        _seed_ground_truth_transfer_decisions(db, expected_pairs=expected_pairs)
        accepted = db.execute(
            """
            SELECT COUNT(*) FROM app.match_decisions
            WHERE match_type = 'transfer' AND match_status = 'accepted'
              AND reversed_at IS NULL
            """
        ).fetchone()
        bridge = db.execute("SELECT COUNT(*) FROM core.bridge_transfers").fetchone()
        assert accepted == (expected_pairs,)
        assert bridge == (expected_pairs,)

        # app.match_decisions and app.profile_settings are external to SQLMesh,
        # so the already-covered FULL intervals need explicit restatement.
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                restate_models=_FX_RESTATE_MODELS,
                auto_apply=True,
                no_prompts=True,
            )

        conversions = db.execute(
            """
            SELECT conversion_id, transfer_pair_id, from_currency, to_currency,
                   coverage_status, coverage_reason
            FROM core.bridge_currency_conversions
            ORDER BY transfer_pair_id
            """
        ).fetchall()
        assert len(conversions) == expected_pairs
        coverage = db.execute(
            """
            SELECT coverage_status, coverage_reason, COUNT(*)
            FROM core.bridge_currency_conversions
            GROUP BY coverage_status, coverage_reason
            ORDER BY coverage_status, coverage_reason
            """
        ).fetchall()
        assert all(row[4:] == ("complete", None) for row in conversions), {
            "coverage": coverage,
            "profile": db.execute(
                "SELECT home_currency FROM app.profile_settings"
            ).fetchall(),
            "currencies": sorted({(row[2], row[3]) for row in conversions}),
        }

        acquisitions = {
            row[0] for row in conversions if row[2] == "USD" and row[3] == "EUR"
        }
        lots = db.execute(
            """
            SELECT source_conversion_id, currency_code, acquisition_type,
                   original_quantity, cost_basis_total, coverage_status
            FROM core.fct_currency_lots
            WHERE acquisition_type = 'conversion'
            ORDER BY acquisition_date, currency_lot_id
            """
        ).fetchall()
        assert len(lots) == completed_months
        assert {row[0] for row in lots} == acquisitions
        assert all(
            row[1:]
            == (
                "EUR",
                "conversion",
                Decimal("90.00"),
                Decimal("100.00"),
                "complete",
            )
            for row in lots
        )

        gains = db.execute(
            """
            SELECT disposed_amount, proceeds, cost_basis, gain_loss,
                   coverage_status, coverage_reason
            FROM core.fct_realized_fx_gains
            ORDER BY disposal_date, realized_fx_gain_id
            """
        ).fetchall()
        assert len(gains) == completed_months
        assert all(
            row
            == (
                Decimal("45.00"),
                Decimal("55.00"),
                Decimal("50.00"),
                Decimal("5.00"),
                "complete",
                None,
            )
            for row in gains
        )
        assert sum(row[3] for row in gains) == Decimal(completed_months * 5)

        as_of = _report_as_of(db)
        expected_positions = _expected_positions(generated, as_of)
        assert len(expected_positions) == 5
        assert _segments_hold_their_own_currency(db, expected_positions, as_of).passed
        assert _the_headline_refuses_to_blend(db, expected_positions).passed

        snapshot = NetworthService(db).current()
        aed = next(
            segment
            for segment in snapshot.per_currency
            if segment.currency_code == "AED"
        )
        assert (aed.net_worth, aed.account_count) == expected_positions["AED"]
        assert snapshot.net_worth is None


def _seed_ground_truth_transfer_decisions(db: Database, *, expected_pairs: int) -> None:
    """Accept only exact synthetic pairs, orienting debit before credit."""
    rows = db.execute(
        """
        WITH legs AS (
          SELECT
            gt.transfer_pair_id,
            matched.source_transaction_id,
            matched.source_type,
            matched.source_origin,
            matched.account_id,
            matched.amount,
            gt.generated_at
          FROM synthetic.ground_truth AS gt
          JOIN prep.int_transactions__matched AS matched
            ON gt.source_transaction_id = matched.source_transaction_id
          WHERE gt.transfer_pair_id IS NOT NULL
        )
        SELECT
          transfer_pair_id,
          COUNT(*) AS leg_count,
          COUNT(*) FILTER (WHERE amount < 0) AS debit_count,
          COUNT(*) FILTER (WHERE amount > 0) AS credit_count
        FROM legs
        GROUP BY transfer_pair_id
        ORDER BY transfer_pair_id
        """
    ).fetchall()
    assert len(rows) == expected_pairs
    assert all(row[1:] == (2, 1, 1) for row in rows)

    db.execute(
        """
        WITH legs AS (
          SELECT
            gt.transfer_pair_id,
            matched.source_transaction_id,
            matched.source_type,
            matched.source_origin,
            matched.account_id,
            matched.amount,
            gt.generated_at
          FROM synthetic.ground_truth AS gt
          JOIN prep.int_transactions__matched AS matched
            ON gt.source_transaction_id = matched.source_transaction_id
          WHERE gt.transfer_pair_id IS NOT NULL
        ), paired AS (
          SELECT
            transfer_pair_id,
            MAX(source_transaction_id) FILTER (WHERE amount < 0) AS debit_id,
            MAX(source_type) FILTER (WHERE amount < 0) AS debit_source_type,
            MAX(source_origin) FILTER (WHERE amount < 0) AS debit_source_origin,
            MAX(account_id) FILTER (WHERE amount < 0) AS debit_account_id,
            MAX(source_transaction_id) FILTER (WHERE amount > 0) AS credit_id,
            MAX(source_type) FILTER (WHERE amount > 0) AS credit_source_type,
            MAX(source_origin) FILTER (WHERE amount > 0) AS credit_source_origin,
            MAX(account_id) FILTER (WHERE amount > 0) AS credit_account_id,
            MAX(generated_at) AS generated_at
          FROM legs
          GROUP BY transfer_pair_id
          HAVING COUNT(*) = 2
             AND COUNT(*) FILTER (WHERE amount < 0) = 1
             AND COUNT(*) FILTER (WHERE amount > 0) = 1
        )
        INSERT INTO app.match_decisions (
          match_id,
          source_transaction_id_a,
          source_type_a,
          source_origin_a,
          source_transaction_id_b,
          source_type_b,
          source_origin_b,
          account_id,
          confidence_score,
          match_signals,
          match_type,
          match_tier,
          account_id_b,
          match_status,
          match_reason,
          decided_by,
          decided_at
        )
        SELECT
          'synthetic-fx-' || transfer_pair_id,
          debit_id,
          debit_source_type,
          debit_source_origin,
          credit_id,
          credit_source_type,
          credit_source_origin,
          debit_account_id,
          1.0000,
          CAST('{"synthetic_ground_truth": true}' AS JSON),
          'transfer',
          NULL,
          credit_account_id,
          'accepted',
          'exact synthetic transfer pair',
          'system',
          generated_at
        FROM paired
        """
    )


def _report_as_of(db: Database) -> date:
    """The latest date reports.net_worth covers."""
    row = db.execute("SELECT MAX(balance_date) FROM reports.net_worth").fetchone()
    assert row is not None and row[0] is not None, "reports.net_worth is empty"
    return row[0]


def _segments_hold_their_own_currency(
    db: Database, expected: _Positions, as_of: date
) -> AssertionResult:
    """`reports.net_worth` carries one row per currency, each holding its own.

    The negative half is the blended figure: adding the five segments together
    produces a number denominated in no currency at all, and a model that
    dropped `currency_code` from its GROUP BY would report exactly that, in one
    row, while every single-currency scenario stayed green.
    """
    rows = db.execute(
        """
        SELECT currency_code, net_worth, account_count
        FROM reports.net_worth
        WHERE balance_date = ?
        ORDER BY currency_code
        """,
        [as_of],
    ).fetchall()
    actual: _Positions = {row[0]: (row[1], row[2]) for row in rows}
    blended = sum(net_worth for net_worth, _ in expected.values())

    return AssertionResult(
        name="net_worth_segments_per_currency",
        passed=actual == expected,
        details={
            "as_of": str(as_of),
            "expected": {code: str(value) for code, (value, _) in expected.items()},
            "actual": {code: str(value) for code, (value, _) in actual.items()},
            "blended_total_would_be": str(blended),
            "unknown_currency_segment": None in actual,
        },
    )


def _the_headline_refuses_to_blend(
    db: Database, expected: _Positions
) -> AssertionResult:
    """The scalar goes null and `per_currency` carries the position instead.

    This is the contract `moneybin demo --persona international` depends on: a
    null headline is the correct answer for a five-currency profile, not a
    failed refresh. Reading it as one is what broke that command.
    """
    snapshot = NetworthService(db).current()
    actual: _Positions = {
        segment.currency_code: (segment.net_worth, segment.account_count)
        for segment in snapshot.per_currency
        if segment.currency_code is not None and segment.net_worth is not None
    }
    scalars_null = (
        snapshot.net_worth is None
        and snapshot.total_assets is None
        and snapshot.total_liabilities is None
        and snapshot.currency_code is None
    )
    expected_accounts = sum(count for _, count in expected.values())

    return AssertionResult(
        name="net_worth_headline_stays_null_across_currencies",
        passed=(
            scalars_null
            and actual == expected
            and snapshot.account_count == expected_accounts
        ),
        details={
            "headline_scalars_null": scalars_null,
            "net_worth": str(snapshot.net_worth),
            "segments": sorted(str(code) for code in actual),
            "account_count": snapshot.account_count,
            "expected_account_count": expected_accounts,
        },
    )
