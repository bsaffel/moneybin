"""Unit tests for the core.fct_balances_daily winner-selection helper.

`_select_winning_observations` reduces multiple balance observations for one
account on one date to a single deterministic winner. Wiring Plaid balances in
gives `plaid` the same precedence tier as `ofx` (both institution snapshots),
so the previously-unreachable same-date tie is now reachable and must resolve
deterministically — freshest observation, then source_type as a final key.
"""

from __future__ import annotations

import importlib.util
from datetime import date
from decimal import Decimal

import pandas as pd

from moneybin.database import SQLMESH_ROOT

_MODEL_PATH = SQLMESH_ROOT / "models" / "core" / "fct_balances_daily.py"
_spec = importlib.util.spec_from_file_location("fct_balances_daily_mod", _MODEL_PATH)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
_select_winning_observations = _mod._select_winning_observations


def _obs(
    balance_date: date, balance: str, source_type: str, updated_at: str
) -> dict[str, object]:
    return {
        "balance_date": balance_date,
        "balance": Decimal(balance),
        "source_type": source_type,
        "updated_at": pd.Timestamp(updated_at),
    }


def test_precedence_wins_over_freshness() -> None:
    """A higher-precedence source wins even when a lower one is fresher."""
    d = date(2026, 4, 8)
    group = pd.DataFrame([
        _obs(d, "1000.00", "assertion", "2026-04-08T00:00:00"),
        _obs(d, "1234.56", "plaid", "2026-04-08T12:00:00"),  # fresher, lower tier
    ])
    winners = _select_winning_observations(group)
    assert len(winners) == 1
    assert winners.loc[0, "source_type"] == "assertion"
    assert winners.loc[0, "balance"] == Decimal("1000.00")


def test_ofx_plaid_tie_broken_by_freshness() -> None:
    """Same date, same precedence tier (ofx vs plaid): the freshest wins."""
    d = date(2026, 4, 8)
    plaid_fresher = pd.DataFrame([
        _obs(d, "500.00", "ofx", "2026-04-08T06:00:00"),
        _obs(d, "1234.56", "plaid", "2026-04-08T12:00:00"),
    ])
    winners = _select_winning_observations(plaid_fresher)
    assert len(winners) == 1
    assert winners.loc[0, "source_type"] == "plaid"
    assert winners.loc[0, "balance"] == Decimal("1234.56")

    # Reverse the timestamps: ofx now fresher → ofx wins. Deterministic either way.
    ofx_fresher = pd.DataFrame([
        _obs(d, "500.00", "ofx", "2026-04-08T18:00:00"),
        _obs(d, "1234.56", "plaid", "2026-04-08T12:00:00"),
    ])
    winners = _select_winning_observations(ofx_fresher)
    assert len(winners) == 1
    assert winners.loc[0, "source_type"] == "ofx"
    assert winners.loc[0, "balance"] == Decimal("500.00")


def test_equal_timestamp_tie_broken_by_source_type() -> None:
    """Identical precedence and timestamp fall back to source_type ascending."""
    d = date(2026, 4, 8)
    group = pd.DataFrame([
        _obs(d, "1234.56", "plaid", "2026-04-08T12:00:00"),
        _obs(d, "500.00", "ofx", "2026-04-08T12:00:00"),
    ])
    winners = _select_winning_observations(group)
    assert len(winners) == 1
    assert winners.loc[0, "source_type"] == "ofx"  # 'ofx' < 'plaid'


def test_distinct_dates_all_survive() -> None:
    """Observations on different dates are not collapsed."""
    group = pd.DataFrame([
        _obs(date(2026, 4, 8), "1234.56", "plaid", "2026-04-08T12:00:00"),
        _obs(date(2026, 4, 9), "1300.00", "plaid", "2026-04-09T12:00:00"),
    ])
    winners = _select_winning_observations(group)
    assert len(winners) == 2
    assert set(winners["balance_date"]) == {date(2026, 4, 8), date(2026, 4, 9)}
