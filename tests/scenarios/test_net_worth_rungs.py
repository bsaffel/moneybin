"""Scenario: the three net-worth rungs under archival, an FX gap, and a live override.

Exercises three things together, on the shared `international` persona, that no
existing scenario combines: Requirement 9's date-scoped archival exclusion, a
multi-day rate-observation gap (`days_since_published >= 2` on a weekday, not
just the ordinary weekend carry), and Tier 1's "override precedence is live"
claim in its end-to-end form -- a written `app.exchange_rate_overrides` row
changing `reports.net_worth_accounts` with no `sqlmesh run` in between.

The persona's UAE Checking account (AED) never receives a provider quote (AED
is outside Frankfurter's coverage by design -- see
`synthetic/data/personas/international.yaml`), so every day it is held and
eligible, `reports.net_worth` is NULL there (fail-closed, per
`docs/specs/reports-net-worth-sql-surface.md`). Archiving it mid-history --
through `AccountService.settings_update`, with the wall clock frozen to a date
inside its balance span, because the service always stamps
`archived_at = date.today()` -- turns that permanent NULL into a boundary: NULL
before archival, priced after. EUR, GBP, and CAD get an explicit rate fixture
(never a live provider -- scenario tests run offline) so every date after
archival is fully priced; GBP's fixture carries a four-day Thursday-Friday-
weekend-Monday gap so `days_since_published` reaches 2+ on a weekday without
the pair ever going unpriced.

Ground truth (account balances) comes from the deterministic `GeneratorEngine`,
independently of any report read, per `.claude/rules/testing.md`. The
conversion rates are chosen by this file, not observed from a provider, so the
expected converted totals are exact by construction.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess  # noqa: S404  # explicit command list, never shell=True
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal

import pytest
import time_machine
from fastmcp import Client, FastMCP

from moneybin.database import Database, get_database, sqlmesh_context
from moneybin.reports._framework.registry import register_generic_reports_tool
from moneybin.repositories.profile_settings_repo import ProfileSettingsRepo
from moneybin.services.account_service import AccountService
from moneybin.services.currency_service import CurrencyService
from moneybin.synthetic.engine import GeneratorEngine
from moneybin.synthetic.models import GenerationResult
from moneybin.tables import (
    REPORTS_NET_WORTH,
    REPORTS_NET_WORTH_ACCOUNTS,
    REPORTS_NET_WORTH_CURRENCIES,
)
from tests.scenarios._runner import load_shipped_scenario, scenario_env
from tests.scenarios._runner.steps import run_step

# uv run moneybin ... subprocess timeout for the one-report CLI parity call --
# generous relative to the single-query cost, matching the spirit of steps.py's
# own transform-subprocess timeout without importing its narrower constant.
_CLI_SUBPROCESS_TIMEOUT_SEC = 120

# Fixed, hand-chosen rates -- never a live provider (scenario tests run
# offline). Round numbers keep the hand-derived ground truth exact.
_EUR_USD = Decimal("1.10")
_GBP_USD = Decimal("1.25")
_CAD_USD = Decimal("0.75")

# The fixture window: five business weeks, comfortably inside the persona's
# 2024-01-01..2025-12-31 span (years=2, per international-multi-currency.yaml).
_RATE_WINDOW_START = date(2025, 11, 1)
_RATE_WINDOW_END = date(2025, 12, 5)
# GBP's Thursday-last-quote, four-day gap (Fri 21st, weekend, Mon 24th),
# resuming Tuesday the 25th -- the shape spec Tier 1 names as the regression
# case for days_since_published, distinct from the ordinary weekend carry.
_GBP_GAP_DATES = (date(2025, 11, 21), date(2025, 11, 24))
_GBP_GAP_WEEKDAY = date(2025, 11, 24)  # Monday; days_since_published = 4 there.

# Inside the account's own balance span (2024-2025), so the date-scoped
# exclusion has real history on both sides, not the account's own edge.
_ARCHIVED_AT = date(2025, 11, 15)
# Before archival: AED is still held and never priced -> fails closed.
_NULL_DATE = date(2025, 11, 10)
# After archival: AED is excluded; EUR/GBP/CAD/USD are all priced.
_PRICED_DATE = date(2025, 12, 1)

# app.account_settings and raw.exchange_rates are both external to SQLMesh;
# core.dim_accounts, core.fct_balances_daily, and core.fct_exchange_rates_daily
# are kind FULL and already covered today's interval on the first transform,
# so each write below needs this same explicit restatement to actually reach
# the reports.* views (mirrors test_networth_correctness.py and
# test_account_settings_archive.py, which restate the same models for the
# same reason -- currency and archival respectively).
_RESTATE_MODELS = [
    "core.dim_accounts",
    "core.fct_balances_daily",
    "core.fct_exchange_rates_daily",
]


def _account_balance(
    generated: GenerationResult, account_name: str, as_of: date
) -> Decimal:
    """An account's own-currency balance on ``as_of``, by arithmetic.

    Opening balance plus every transaction dated on or before ``as_of`` -- the
    same running-sum logic `test_international_multi_currency.py`'s
    `_expected_positions` uses, scoped to one account rather than pooled by
    currency: this persona's currencies are 1:1 with accounts, but
    `net_worth_accounts` grains on the account.
    """
    account = next(a for a in generated.accounts if a.name == account_name)
    total = account.opening_balance
    for txn in generated.transactions:
        if txn.account_name == account_name and txn.date <= as_of:
            total += txn.amount
    return total


def _converted(amount: Decimal, rate: Decimal) -> Decimal:
    """``CAST(ROUND(amount * rate, 2) AS DECIMAL(18, 2))`` -- global-constraints.md."""
    return (amount * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _insert_rate_fixture(db: Database) -> None:
    """Weekday-only provider rates for EUR/GBP/CAD, GBP alone carrying the gap.

    raw.exchange_rates is append-only and not SQLMesh-managed -- a direct
    INSERT is the same shape a real fetch would write, without the network
    call scenario tests must not make.
    """
    for currency, rate in (("EUR", _EUR_USD), ("CAD", _CAD_USD)):
        db.execute(
            """
            INSERT INTO raw.exchange_rates
                (from_currency, to_currency, rate_date, rate, source_type)
            SELECT ?, 'USD', d::DATE, ?, 'frankfurter'
            FROM GENERATE_SERIES(?::DATE, ?::DATE, INTERVAL '1' DAY) AS t(d)
            WHERE ISODOW(d::DATE) NOT IN (6, 7)
            """,  # weekday spine; GENERATE_SERIES bounds are scenario-fixed dates
            [currency, rate, _RATE_WINDOW_START, _RATE_WINDOW_END],
        )
    db.execute(
        """
        INSERT INTO raw.exchange_rates
            (from_currency, to_currency, rate_date, rate, source_type)
        SELECT 'GBP', 'USD', d::DATE, ?, 'frankfurter'
        FROM GENERATE_SERIES(?::DATE, ?::DATE, INTERVAL '1' DAY) AS t(d)
        WHERE ISODOW(d::DATE) NOT IN (6, 7)
          AND d::DATE NOT IN (?, ?)
        """,  # same spine, minus the deliberate gap dates
        [_GBP_USD, _RATE_WINDOW_START, _RATE_WINDOW_END, *_GBP_GAP_DATES],
    )


def _run_cli_json(
    env: dict[str, str], *, from_date: date, to_date: date
) -> list[dict[str, object]]:
    """`moneybin reports net-worth-accounts --output json` via a real subprocess.

    A subprocess, not in-process `CliRunner`: DuckDB enforces a single-writer
    file lock, and the scenario's own ``db`` connection has the encrypted file
    attached in this process -- the CLI needs its own OS process to open a
    second connection, the same reason `_step_transform_via_subprocess`
    (`tests/scenarios/_runner/steps.py`) closes ``db`` before invoking.
    """
    cmd = [
        "uv",
        "run",
        "moneybin",
        "reports",
        "net-worth-accounts",
        "--output",
        "json",
        "--from-date",
        from_date.isoformat(),
        "--to-date",
        to_date.isoformat(),
    ]
    proc = subprocess.run(  # noqa: S603  # explicit command list; uv resolved via PATH
        cmd,
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=_CLI_SUBPROCESS_TIMEOUT_SEC,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    data = json.loads(proc.stdout)["data"]
    assert isinstance(data, list)
    return data


async def _call_mcp_net_worth_accounts(
    *, from_date: date, to_date: date
) -> list[dict[str, object]]:
    """The MCP `reports` tool's rows for `core:net_worth_accounts`, in-process.

    Safe to call in-process (unlike the CLI leg above) only once ``db`` is
    closed -- otherwise this hits the same single-writer file-lock conflict.
    """
    mcp = FastMCP("net-worth-rungs-parity")
    register_generic_reports_tool(mcp)
    async with Client(mcp) as client:
        result = await client.call_tool(
            "reports",
            {
                "report_id": "core:net_worth_accounts",
                "parameters": {
                    "from_date": from_date.isoformat(),
                    "to_date": to_date.isoformat(),
                },
            },
        )
    structured = result.structured_content
    assert structured is not None
    rows = structured["data"]["rows"]
    assert isinstance(rows, list)
    return rows


@pytest.mark.scenarios
@pytest.mark.slow
def test_net_worth_rungs() -> None:
    """Archival, a multi-day rate gap, and a live override -- across all three rungs."""
    scenario = load_shipped_scenario("international-multi-currency")
    assert scenario is not None
    setup = scenario.setup
    # Ground truth, derived before any report is read (testing.md): the real
    # wall clock here, matching the unfrozen `generate` step below.
    generated = GeneratorEngine(
        setup.persona, seed=setup.seed, years=setup.years
    ).generate()

    with scenario_env(scenario) as (db, _tmp, _env):
        run_step("generate", setup, db, env=_env)
        run_step("transform", setup, db, env=_env)

        ProfileSettingsRepo(db).set_home_currency("USD", actor="system")
        _insert_rate_fixture(db)

        uae_row = db.execute(
            "SELECT account_id FROM core.dim_accounts WHERE currency_code = 'AED'"
        ).fetchone()
        assert uae_row is not None, "persona must hold exactly one AED account"
        uae_account_id = str(uae_row[0])
        eur_row = db.execute(
            "SELECT account_id FROM core.dim_accounts WHERE currency_code = 'EUR'"
        ).fetchone()
        assert eur_row is not None, "persona must hold exactly one EUR account"
        eur_account_id = str(eur_row[0])

        # AccountService.settings_update always stamps archived_at with
        # date.today() on the FALSE -> TRUE transition (never a caller value)
        # -- the spec requires archiving go through the service rather than a
        # direct column write, so freeze the clock to land that stamp inside
        # the persona's own history instead of the real "today" (a year past
        # the persona's 2025-12-31 end). Travel to noon UTC, not a bare date:
        # time_machine treats a naive date as UTC midnight, which
        # date.today() reads back one calendar day earlier in a
        # negative-UTC-offset timezone (verified: bare _ARCHIVED_AT resolved
        # to 2025-11-14 locally).
        with time_machine.travel(
            datetime.combine(_ARCHIVED_AT, datetime.min.time()).replace(hour=12),
            tick=False,
        ):
            AccountService(db).settings_update(
                uae_account_id, archived=True, actor="system"
            )

        with sqlmesh_context(db) as ctx:
            ctx.plan(restate_models=_RESTATE_MODELS, auto_apply=True, no_prompts=True)

        # --- fail-closed before archival: AED still held, never priced ---
        row = db.execute(
            f"SELECT net_worth, unpriced_currency_count "  # noqa: S608  # TableRef constant
            f"FROM {REPORTS_NET_WORTH.full_name} WHERE balance_date = ?",
            [_NULL_DATE],
        ).fetchone()
        assert row is not None, f"no net_worth row for {_NULL_DATE}"
        assert row[0] is None, f"expected NULL net_worth on {_NULL_DATE}, got {row[0]}"
        assert row[1] == 1, f"expected exactly one unpriced currency, got {row[1]}"

        # --- fully priced after archival: matches hand-derived ground truth ---
        expected_total = (
            _converted(
                _account_balance(generated, "Eurozone Checking", _PRICED_DATE),
                _EUR_USD,
            )
            + _converted(
                _account_balance(generated, "UK Current Account", _PRICED_DATE),
                _GBP_USD,
            )
            + _converted(
                _account_balance(generated, "Canada Checking", _PRICED_DATE),
                _CAD_USD,
            )
            + _account_balance(generated, "US Checking", _PRICED_DATE).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        )
        row = db.execute(
            f"SELECT net_worth, unpriced_currency_count "  # noqa: S608  # TableRef constant
            f"FROM {REPORTS_NET_WORTH.full_name} WHERE balance_date = ?",
            [_PRICED_DATE],
        ).fetchone()
        assert row is not None, f"no net_worth row for {_PRICED_DATE}"
        assert row[1] == 0, (
            f"expected zero unpriced currencies on {_PRICED_DATE}, got {row[1]}"
        )
        actual_total = Decimal(str(row[0]))
        assert actual_total == expected_total, (
            f"{_PRICED_DATE}: expected {expected_total}, got {actual_total}"
        )

        # --- the currency rung sums to the same headline on the priced date ---
        currencies_sum = db.execute(
            f"SELECT SUM(net_worth_home) "  # noqa: S608  # TableRef constant
            f"FROM {REPORTS_NET_WORTH_CURRENCIES.full_name} WHERE balance_date = ?",
            [_PRICED_DATE],
        ).fetchone()
        assert currencies_sum is not None and currencies_sum[0] is not None
        assert Decimal(str(currencies_sum[0])) == actual_total

        # --- the archived account contributes on/before archived_at, not after ---
        uae_bounds = db.execute(
            f"SELECT MIN(balance_date), MAX(balance_date) "  # noqa: S608  # TableRef constant
            f"FROM {REPORTS_NET_WORTH_ACCOUNTS.full_name} WHERE account_id = ?",
            [uae_account_id],
        ).fetchone()
        assert uae_bounds is not None and uae_bounds[0] is not None, (
            "the archived account must still contribute before archived_at"
        )
        assert uae_bounds[1] == _ARCHIVED_AT, (
            "the archived account's last contributing date must be exactly "
            f"archived_at ({_ARCHIVED_AT}), got {uae_bounds[1]}"
        )
        after_count = db.execute(
            f"SELECT COUNT(*) "  # noqa: S608  # TableRef constant
            f"FROM {REPORTS_NET_WORTH_ACCOUNTS.full_name} "
            "WHERE account_id = ? AND balance_date > ?",
            [uae_account_id, _ARCHIVED_AT],
        ).fetchone()
        assert after_count == (0,), (
            "the archived account must not contribute after archived_at"
        )

        # --- the multi-day gap is visible in the effective rate view ---
        gap = db.execute(
            "SELECT days_since_published FROM core.fct_exchange_rates_effective "
            "WHERE from_currency = 'GBP' AND to_currency = 'USD' "
            "AND effective_date = ?",
            [_GBP_GAP_WEEKDAY],
        ).fetchone()
        assert gap is not None, f"no effective rate row for {_GBP_GAP_WEEKDAY}"
        assert gap[0] >= 2, (
            f"expected days_since_published >= 2 on {_GBP_GAP_WEEKDAY} "
            f"(a weekday), got {gap[0]}"
        )

        # --- CLI/MCP parity, before the override mutates the priced date ---
        # DuckDB's single-writer file lock means the CLI subprocess below
        # can't open the encrypted file while this connection holds it
        # attached; close, run both parity legs, then reopen to continue.
        db.close()
        cli_rows = _run_cli_json(_env, from_date=_PRICED_DATE, to_date=_PRICED_DATE)
        mcp_rows = asyncio.run(
            _call_mcp_net_worth_accounts(from_date=_PRICED_DATE, to_date=_PRICED_DATE)
        )
        assert mcp_rows, "expected at least one row on the priced date"
        assert mcp_rows == cli_rows
        db = get_database(read_only=False)

        # --- override precedence is live: no transform run in between ---
        eur_balance = _account_balance(generated, "Eurozone Checking", _PRICED_DATE)
        override_rate = Decimal("1.50")
        CurrencyService(db, actor="system").set_override(
            "EUR", "USD", _PRICED_DATE, override_rate, note="scenario test override"
        )
        overridden = db.execute(
            f"SELECT rate_source, account_balance_home "  # noqa: S608  # TableRef constant
            f"FROM {REPORTS_NET_WORTH_ACCOUNTS.full_name} "
            "WHERE account_id = ? AND balance_date = ?",
            [eur_account_id, _PRICED_DATE],
        ).fetchone()
        assert overridden is not None, "no net_worth_accounts row for EUR post-override"
        assert overridden[0] == "override", (
            f"expected rate_source='override', got {overridden[0]!r}"
        )
        assert Decimal(str(overridden[1])) == _converted(eur_balance, override_rate), (
            "the overridden EUR balance must reflect the new rate, not the "
            "provider's 1.10"
        )
        db.close()
