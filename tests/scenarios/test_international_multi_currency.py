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

from moneybin.database import Database
from moneybin.services.networth_service import NetworthService
from moneybin.synthetic.engine import GeneratorEngine
from moneybin.synthetic.models import GenerationResult
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario
from tests.scenarios._tier1_backfill import tier1_backfill

# Position per currency: (net_worth, account_count). The key is nullable
# because `reports.net_worth` pools every unknown-currency account into one
# NULL-coded segment — this persona must never produce one, and a `str`-only
# alias would make that assertion unwritable.
_Positions = dict[str | None, tuple[Decimal, int]]


def _expected_positions(result: GenerationResult, as_of: date) -> _Positions:
    """Net worth and account count per currency on `as_of`, by arithmetic.

    Both writers anchor an account at its declared opening balance before any
    activity and then emit its transactions, so the balance on any date is
    their running sum. This persona has no transfers and no two accounts share
    a currency, so a currency's total is exactly its one account's.

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
