"""Scenario: Requirement 14's unanchored-account guard over real transforms.

Profiles (reports-net-worth-sql-surface.md §Tier 2 and §Tier 3):
  MIXED  — networth-correctness's two anchored CSV accounts plus Plaid accounts
           each carrying exactly one kind of evidence, and one carrying none.
  WHOLLY — no balance data at all; every account unanchored.
  LIQUIDATED — definitive-zero broker snapshots over real buy/sell history, and a
           checking account sharing a liquidated item.
Unanchored accounts are raw Plaid rows with no raw.plaid_balances row, so
core.fct_balances never anchors them. Account ids are synthetic.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import cast

import pytest
import time_machine

from moneybin.database import Database, sqlmesh_context
from moneybin.reports.definitions.net_worth import net_worth
from moneybin.reports.definitions.net_worth_accounts import net_worth_accounts
from moneybin.reports.definitions.net_worth_currencies import net_worth_currencies
from moneybin.repositories.profile_settings_repo import ProfileSettingsRepo
from moneybin.services.account_service import AccountService
from moneybin.services.doctor_service import DoctorService, InvariantResult
from tests.moneybin.test_reports.test_net_worth_runners import (
    _run,  # pyright: ignore[reportPrivateUsage]
)
from tests.scenarios._runner import load_shipped_scenario, scenario_env
from tests.scenarios._runner.steps import run_step

pytestmark = [pytest.mark.scenarios, pytest.mark.slow]

_RESTATE_AFTER_SETTINGS = ["core.dim_accounts", "core.fct_balances_daily"]
_CURRENCY_MEASURES = (
    "total_assets",
    "total_liabilities",
    "net_worth",
    "total_assets_home",
    "total_liabilities_home",
    "net_worth_home",
)


def _plaid_account(
    db: Database, account_id: str, account_type: str, origin: str
) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_accounts
            (account_id, account_type, name, source_file, source_type,
             source_origin, extracted_at)
        VALUES (?, ?, ?, 'sync_m2b3', 'plaid', ?, CURRENT_TIMESTAMP)
        """,
        [account_id, account_type, f"Fixture {account_id}", origin],
    )


def _plaid_txn(db: Database, account_id: str, origin: str, day: str) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             iso_currency_code, source_file, source_type, source_origin, extracted_at)
        VALUES (?, ?, ?, 25.00, 'Fixture purchase', 'USD', 'sync_m2b3', 'plaid', ?,
                CURRENT_TIMESTAMP)
        """,
        [f"txn_{account_id}_{day}", account_id, day, origin],
    )


def _plaid_inv_txn(
    db: Database,
    account_id: str,
    origin: str,
    *,
    kind: str,
    subtype: str,
    day: str,
    quantity: str | None,
    amount: str,
) -> None:
    """Plaid convention: positive amount = cash out; staging flips the sign."""
    txn_id = f"itx_{account_id}_{subtype}_{day}"
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transactions (
            investment_transaction_id, account_id, security_id,
            investment_transaction_type, investment_transaction_subtype,
            transaction_date, quantity, price, amount, fees, iso_currency_code,
            observation_version, source_type, source_origin
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 100.00, ?, 0.00, 'USD', 'm2b3', 'plaid', ?)
        """,
        [
            txn_id,
            account_id,
            None if quantity is None else "prov_m2b3_sec",
            kind,
            subtype,
            day,
            quantity,
            amount,
            origin,
        ],
    )
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transaction_receipts (
            investment_transaction_id, source_origin, source_file,
            observation_version, extracted_at, loaded_at
        ) VALUES (?, ?, 'sync_m2b3', 'm2b3', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [txn_id, origin],
    )


def _snapshot(
    db: Database, origin: str, holdings: list[tuple[str, str | None, str | None]]
) -> None:
    """One newest receipt for the item, plus (account_id, quantity, value) rows."""
    db.execute(
        """
        INSERT INTO raw.plaid_investment_holdings_snapshots (
            source_origin, source_file, holdings_date, holdings_count,
            transactions_window_start, source_type, extracted_at, loaded_at
        ) VALUES (?, 'sync_m2b3_holdings', CURRENT_DATE, ?, DATE '2024-01-01',
                  'plaid', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [origin, len(holdings)],
    )
    for account_id, quantity, value in holdings:
        db.execute(
            """
            INSERT INTO raw.plaid_investment_holdings (
                account_id, security_id, holdings_date, institution_price,
                institution_price_as_of, institution_value, cost_basis, quantity,
                iso_currency_code, transactions_window_start, source_file,
                source_type, source_origin, extracted_at, loaded_at
            ) VALUES (?, 'prov_m2b3_sec', CURRENT_DATE, 100.00, CURRENT_DATE, ?,
                      NULL, ?, 'USD', DATE '2024-01-01', 'sync_m2b3_holdings',
                      'plaid', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            [account_id, value, quantity, origin],
        )


def _today(db: Database) -> date:
    row = db.execute("SELECT CURRENT_DATE").fetchone()
    assert row is not None
    return row[0]


def _archive_on(db: Database, account_id: str, on: date) -> None:
    """Through the service (Requirement 9), wall clock frozen to noon on ``on``."""
    with time_machine.travel(
        datetime.combine(on, datetime.min.time()).replace(hour=12), tick=False
    ):
        AccountService(db).settings_update(account_id, archived=True, actor="system")


def _restate(db: Database) -> None:
    with sqlmesh_context(db) as ctx:
        ctx.plan(
            restate_models=_RESTATE_AFTER_SETTINGS, auto_apply=True, no_prompts=True
        )


def _invariant(db: Database, name: str) -> InvariantResult:
    report = DoctorService(db).run_all()
    return next(r for r in report.invariants if r.name == name)


def test_mixed_profile_guard() -> None:
    """Tier 2 scenarios 1, 2, 5, 6, 7, 8, 9 on one mixed profile."""
    scenario = load_shipped_scenario("networth-correctness")
    assert scenario is not None
    with scenario_env(scenario) as (db, _tmp, env):
        run_step("load_fixtures", scenario.setup, db, env=env)
        db.execute(
            """
            INSERT INTO app.balance_assertions (account_id, assertion_date, balance)
            VALUES ('networth-checking', '2024-01-01', 5000.00),
                   ('networth-checking', '2024-01-31', 4900.00),
                   ('networth-savings',  '2024-01-01', 5000.00),
                   ('networth-savings',  '2024-01-31', 5510.00)
            """
        )
        _plaid_account(db, "m2b3_broker", "investment", "item_m2b3_a")
        # Broker arm only. A value with no quantity declines the opening-lot
        # bootstrap (int_plaid__opening_positions: held_qty NULL -> nonpositive
        # -> review), which would otherwise write an opening_bootstrap row to
        # core.fct_investment_transactions and light the ledger arm too.
        _snapshot(db, "item_m2b3_a", [("m2b3_broker", None, "500.00")])
        _plaid_account(db, "m2b3_cash", "depository", "item_m2b3_b")
        _plaid_txn(db, "m2b3_cash", "item_m2b3_b", "2024-01-10")  # cash-ledger arm
        _plaid_account(db, "m2b3_dividend", "investment", "item_m2b3_c")
        _plaid_inv_txn(
            db,
            "m2b3_dividend",
            "item_m2b3_c",
            kind="cash",
            subtype="dividend",
            day="2024-01-15",
            quantity=None,
            amount="-12.00",
        )  # investment-ledger arm only
        _plaid_account(db, "m2b3_dormant", "depository", "item_m2b3_d")  # no evidence

        run_step("transform", scenario.setup, db, env=env)
        svc = AccountService(db)
        svc.settings_update("networth-checking", currency_code="USD", actor="system")
        svc.settings_update("networth-savings", currency_code="USD", actor="system")
        ProfileSettingsRepo(db).set_home_currency("USD", actor="system")
        _restate(db)

        candidates = {"m2b3_broker", "m2b3_cash", "m2b3_dividend"}
        present = {
            r[0]
            for r in db.execute("SELECT account_id FROM core.dim_accounts").fetchall()
        }
        assert candidates | {"m2b3_dormant"} <= present, (
            "fixture accounts must reach core.dim_accounts under their raw ids"
        )
        first_null = dict(
            db.execute(
                "SELECT account_id, MIN(balance_date) FROM core.fct_balances_daily "
                "WHERE balance IS NULL GROUP BY account_id"
            ).fetchall()
        )
        # First evidence per account, read off the fixture above: m2b3_cash's one
        # cash transaction, m2b3_dividend's one trade, m2b3_broker's broker as_of
        # (_snapshot stamps extracted_at = CURRENT_TIMESTAMP). Dormant has no
        # evidence (S5); the two CSV accounts are anchored by their assertions.
        assert first_null == {
            "m2b3_cash": date(2024, 1, 10),
            "m2b3_dividend": date(2024, 1, 15),
            "m2b3_broker": _today(db),
        }

        spine_row = db.execute(
            "SELECT MAX(balance_date) FROM core.fct_balances_daily"
        ).fetchone()
        assert spine_row is not None and spine_row[0] is not None
        spine_max: date = spine_row[0]
        # The broker as_of (today) is the newest evidence; the newest assertion
        # is 2024-01-31 and the newest transaction 2024-02-25 (savings.csv).
        assert spine_max == _today(db)

        # S1/S2/S7, per date. A candidate counts from its first evidence on:
        #   2024-01-09: none yet, so the total stands. checking 5000 - 10 - 15
        #     (Jan 05, Jan 07) = 4975, savings 5000 untouched -> 9975.00;
        #   2024-01-31: cash (from 01-10) and dividend (from 01-15), not broker;
        #   spine_max: all three.
        day = {
            r[0]: r[1:]
            for r in db.execute(
                "SELECT balance_date, unanchored_account_count, net_worth "
                "FROM reports.net_worth"
            ).fetchall()
        }
        assert day[date(2024, 1, 9)] == (0, Decimal("9975.00"))
        assert day[date(2024, 1, 31)] == (2, None)
        assert day[spine_max] == (3, None)
        assert max(day) == spine_max

        # Account rung: each candidate is a row at the spine max; dormant absent.
        acct = _run(db, net_worth_accounts(db))
        by_id = {r["account_id"]: r for r in acct}
        assert "m2b3_dormant" not in by_id
        for cid in candidates:
            assert by_id[cid]["balance_date"] == spine_max
            assert by_id[cid]["account_balance"] is None
            assert by_id[cid]["account_balance_home"] is None
            assert by_id[cid]["is_observed"] is False

        # Currency rung. A Plaid account's currency is dim_accounts' COALESCE of
        # its settings override and its raw.plaid_balances currency; the three
        # candidates have neither, so all three land in the NULL segment. The two
        # CSV accounts were set to USD above and stay priced and anchored there:
        # checking 4900 (Jan 31 assertion, no later txn); savings 5510 (Jan 31)
        # + 500 (Feb 10) + 10 (Feb 25) = 6020, carried to the spine end.
        cur = {r["currency_code"]: r for r in _run(db, net_worth_currencies(db))}
        assert set(cur) == {"USD", None}
        assert sum(cast(int, r["unanchored_account_count"]) for r in cur.values()) == 3
        assert cur[None]["unanchored_account_count"] == 3
        for measure in _CURRENCY_MEASURES:
            assert cur[None][measure] is None, measure
        assert cur["USD"]["unanchored_account_count"] == 0
        assert cur["USD"]["account_count"] == 2
        assert cur["USD"]["net_worth"] == Decimal("10920.00")

        # S6: a range before every evidence date and every anchored row (the
        # assertions start 2024-01-01) has no rows; nothing is synthesized.
        assert (
            _run(db, net_worth(db, from_date="2023-06-01", to_date="2023-06-30")) == []
        )

        # S8: each candidate appears once per date it has rows. In
        # spine_max-3..spine_max that is broker once (today only), cash and
        # dividend on all four dates.
        ranged = _run(
            db,
            net_worth_accounts(
                db,
                from_date=(spine_max - timedelta(days=3)).isoformat(),
                to_date=spine_max.isoformat(),
            ),
        )
        assert sum(r["account_id"] == "m2b3_broker" for r in ranged) == 1
        assert sum(r["account_id"] == "m2b3_cash" for r in ranged) == 4
        assert sum(r["account_id"] == "m2b3_dividend" for r in ranged) == 4
        # spine_max-20..spine_max-10 (11 dates, all after 2024-01-15) holds cash
        # and dividend on every date and broker on none: its rows start today.
        early_from = spine_max - timedelta(days=20)
        early_to = spine_max - timedelta(days=10)
        early_dates = [early_from + timedelta(days=d) for d in range(11)]
        early = _run(
            db,
            net_worth_accounts(
                db, from_date=early_from.isoformat(), to_date=early_to.isoformat()
            ),
        )
        assert {"networth-checking", "networth-savings"} <= {
            r["account_id"] for r in early
        }
        for cid in ("m2b3_cash", "m2b3_dividend"):
            assert [r["balance_date"] for r in early if r["account_id"] == cid] == (
                early_dates
            )
        assert not [r for r in early if r["account_id"] == "m2b3_broker"]

        # Doctor fails naming exactly the three.
        result = _invariant(db, "net_worth_unanchored_accounts")
        assert result.status == "fail"
        assert set(result.affected_ids) == {f"account:{c}" for c in candidates}

        # S9: archive m2b3_cash 5 days before the spine max. Its rows count
        # through archived_on and no later, so the unranged read (spine_max)
        # omits it, the early range (ending spine_max-10) keeps all 11 dates,
        # and archived_on-2..spine_max holds exactly archived_on-2..archived_on.
        archived_on = spine_max - timedelta(days=5)
        _archive_on(db, "m2b3_cash", archived_on)
        _restate(db)
        unranged = _run(db, net_worth_accounts(db))
        assert "m2b3_cash" not in {r["account_id"] for r in unranged}
        spanning = _run(
            db,
            net_worth_accounts(
                db, from_date=early_from.isoformat(), to_date=early_to.isoformat()
            ),
        )
        assert [
            r["balance_date"] for r in spanning if r["account_id"] == "m2b3_cash"
        ] == early_dates
        ranged_span = _run(
            db,
            net_worth_accounts(
                db,
                from_date=(archived_on - timedelta(days=2)).isoformat(),
                to_date=spine_max.isoformat(),
            ),
        )
        cash_rows = [r for r in ranged_span if r["account_id"] == "m2b3_cash"]
        assert [r["balance_date"] for r in cash_rows] == [
            archived_on - timedelta(days=d) for d in (2, 1, 0)
        ]
        assert set(_invariant(db, "net_worth_unanchored_accounts").affected_ids) == {
            f"account:{c}" for c in candidates - {"m2b3_cash"}
        }


def test_wholly_unanchored_profile_guard() -> None:
    """Tier 2 scenarios 3, 4, 11; Tier 3 'fails the gate' / 'not for an excluded account'."""
    scenario = load_shipped_scenario("networth-correctness")
    assert scenario is not None
    with scenario_env(scenario) as (db, _tmp, env):
        _plaid_account(db, "w_broker", "investment", "item_w_a")
        _snapshot(db, "item_w_a", [("w_broker", "3", None)])
        _plaid_account(db, "w_cash", "depository", "item_w_b")
        _plaid_txn(db, "w_cash", "item_w_b", "2025-12-01")
        run_step("transform", scenario.setup, db, env=env)
        ProfileSettingsRepo(db).set_home_currency("USD", actor="system")
        today = _today(db)

        # First evidence: w_cash its one transaction, 2025-12-01. w_broker's
        # snapshot holds quantity 3 against a transactions window starting
        # 2024-01-01 with no in-window trade, so the opening-lot bootstrap
        # (stg_plaid__opening_lots fallbacks) writes a transfer_in dated W - 1 =
        # 2023-12-31 to core.fct_investment_transactions, which is ledger
        # evidence older than its broker as_of (today). The spine ends today.
        broker_from = date(2023, 12, 31)
        cash_from = date(2025, 12, 1)
        first_null = dict(
            db.execute(
                "SELECT account_id, MIN(balance_date) FROM core.fct_balances_daily "
                "WHERE balance IS NULL GROUP BY account_id"
            ).fetchall()
        )
        assert first_null == {"w_broker": broker_from, "w_cash": cash_from}
        # One NULL row per account per date from first evidence to today.
        count_row = db.execute(
            "SELECT COUNT(*), COUNT(balance) FROM core.fct_balances_daily"
        ).fetchone()
        assert count_row == (
            (today - broker_from).days + 1 + (today - cash_from).days + 1,
            0,
        )

        # S3: the bare view has a row per date, w_broker alone until w_cash's
        # first evidence and both after; every total withheld.
        bare = db.execute(
            "SELECT balance_date, unanchored_account_count, net_worth "
            "FROM reports.net_worth ORDER BY balance_date"
        ).fetchall()
        assert bare[0] == (broker_from, 1, None)
        assert bare[-1] == (today, 2, None)
        assert len(bare) == (today - broker_from).days + 1
        assert all(r[2] is None for r in bare)
        assert {r[0]: r[1] for r in bare}[cash_from - timedelta(days=1)] == 1
        assert {r[0]: r[1] for r in bare}[cash_from] == 2
        (row,) = _run(db, net_worth(db))
        assert (row["balance_date"], row["unanchored_account_count"]) == (today, 2)
        acct = _run(db, net_worth_accounts(db))
        assert {r["account_id"] for r in acct} == {"w_broker", "w_cash"}
        assert {r["balance_date"] for r in acct} == {today}
        # Currency rung: neither Plaid account has a balance row or an override,
        # so both sit in the NULL segment, withheld.
        (cur,) = _run(db, net_worth_currencies(db))
        assert (cur["currency_code"], cur["unanchored_account_count"]) == (None, 2)
        for measure in _CURRENCY_MEASURES:
            assert cur[measure] is None, measure

        result = _invariant(db, "net_worth_unanchored_accounts")
        assert result.status == "fail"
        assert sorted(result.affected_ids) == ["account:w_broker", "account:w_cash"]

        # S4/S11: archive both 40 days ago (after both first-evidence dates).
        # Both count through archived_on and not after, so archived_on is the
        # last date anything counts: the unranged reads land there, count 2,
        # total withheld, never an empty "no net worth". The range
        # archived_on-30..archived_on+10 returns the 31 dates through
        # archived_on. Nothing counts on the spine's last date (today), so the
        # doctor passes.
        archived_on = today - timedelta(days=40)
        _archive_on(db, "w_broker", archived_on)
        _archive_on(db, "w_cash", archived_on)
        _restate(db)
        (row,) = _run(db, net_worth(db))
        assert (
            row["balance_date"],
            row["unanchored_account_count"],
            row["net_worth"],
        ) == (archived_on, 2, None)
        acct = _run(db, net_worth_accounts(db))
        assert sorted((r["account_id"], r["balance_date"]) for r in acct) == [
            ("w_broker", archived_on),
            ("w_cash", archived_on),
        ]
        hist = _run(
            db,
            net_worth(
                db,
                from_date=(archived_on - timedelta(days=30)).isoformat(),
                to_date=(archived_on + timedelta(days=10)).isoformat(),
            ),
        )
        assert [r["balance_date"] for r in hist] == [
            archived_on - timedelta(days=d) for d in range(30, -1, -1)
        ]
        assert all(r["unanchored_account_count"] == 2 for r in hist)
        assert all(r["net_worth"] is None for r in hist)
        assert _invariant(db, "net_worth_unanchored_accounts").status == "pass"


def test_liquidated_investment_accounts_still_fail_the_guard() -> None:
    """Tier 3: zero-row and empty-receipt liquidations over real buy/sell history.

    Sold at cost and at a gain, plus a checking account sharing a liquidated item.
    """
    scenario = load_shipped_scenario("networth-correctness")
    assert scenario is not None
    with scenario_env(scenario) as (db, _tmp, env):
        # Zero-quantity row; sold at cost (net cash effect exactly zero).
        _plaid_account(db, "liq_zero_row", "investment", "item_liq_a")
        _plaid_inv_txn(
            db,
            "liq_zero_row",
            "item_liq_a",
            kind="buy",
            subtype="buy",
            day="2025-06-01",
            quantity="10",
            amount="1000.00",
        )
        _plaid_inv_txn(
            db,
            "liq_zero_row",
            "item_liq_a",
            kind="sell",
            subtype="sell",
            day="2025-07-01",
            quantity="-10",
            amount="-1000.00",
        )
        _snapshot(db, "item_liq_a", [("liq_zero_row", "0", None)])
        # Empty receipt; sold at a gain; a depository sibling on the same item.
        _plaid_account(db, "liq_empty", "investment", "item_liq_b")
        _plaid_inv_txn(
            db,
            "liq_empty",
            "item_liq_b",
            kind="buy",
            subtype="buy",
            day="2025-06-01",
            quantity="10",
            amount="1000.00",
        )
        _plaid_inv_txn(
            db,
            "liq_empty",
            "item_liq_b",
            kind="sell",
            subtype="sell",
            day="2025-07-01",
            quantity="-10",
            amount="-1250.00",
        )
        _plaid_account(db, "liq_checking", "depository", "item_liq_b")
        _plaid_txn(db, "liq_checking", "item_liq_b", "2025-07-02")
        _snapshot(db, "item_liq_b", [])
        run_step("transform", scenario.setup, db, env=env)

        broker = dict(
            db.execute(
                "SELECT account_id, has_position FROM core.dim_holdings_broker_reported"
            ).fetchall()
        )
        assert broker.get("liq_zero_row") is False
        assert broker.get("liq_empty") is False
        assert "liq_checking" not in broker  # receipt scope

        # First evidence: both liquidated accounts' 2025-06-01 buys, the
        # checking account's 2025-07-02 transaction. Neither snapshot is
        # evidence (has_position FALSE; liq_zero_row's held quantity 0 also
        # declines the opening-lot bootstrap), so nothing reaches today: the
        # spine ends at the newest evidence, 2025-07-02.
        first_null = dict(
            db.execute(
                "SELECT account_id, MIN(balance_date) FROM core.fct_balances_daily "
                "WHERE balance IS NULL GROUP BY account_id"
            ).fetchall()
        )
        assert first_null == {
            "liq_zero_row": date(2025, 6, 1),
            "liq_empty": date(2025, 6, 1),
            "liq_checking": date(2025, 7, 2),
        }
        spine_row = db.execute(
            "SELECT MAX(balance_date) FROM core.fct_balances_daily"
        ).fetchone()
        assert spine_row == (date(2025, 7, 2),)

        # Day and currency rungs on that date: all three, NULL segment (no
        # balance row, no override), totals withheld.
        (row,) = _run(db, net_worth(db))
        assert (
            row["balance_date"],
            row["unanchored_account_count"],
            row["net_worth"],
        ) == (date(2025, 7, 2), 3, None)
        cur = _run(db, net_worth_currencies(db))
        assert sum(cast(int, r["unanchored_account_count"]) for r in cur) == 3
        assert [r["currency_code"] for r in cur] == [None]
        for measure in _CURRENCY_MEASURES:
            assert cur[0][measure] is None, measure

        result = _invariant(db, "net_worth_unanchored_accounts")
        assert result.status == "fail"
        assert sorted(result.affected_ids) == [
            "account:liq_checking",
            "account:liq_empty",
            "account:liq_zero_row",
        ]
