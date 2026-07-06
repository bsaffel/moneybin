"""Unit tests for the cost-basis SQLMesh loader.

The loader is the testable seam the two Python models lack. These tests drive the
public ``load_engine_inputs`` with a fake ExecutionContext (no SQLMesh Context) to
pin: the VARCHAR->Decimal parsing that guards the float64 trap, the LedgerEvent
mapping, the per-(account, security) freshness map, and the method/selection
closures.

``load_engine_inputs`` issues four fetches in order — ledger, securities,
account settings, lot selections — so the fake queues frames in that order.
"""

from __future__ import annotations

import typing as t
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest

from moneybin.investments.sqlmesh_loader import load_engine_inputs

pytestmark = pytest.mark.unit


class _FakeContext:
    """Minimal ExecutionContext stand-in: fetchdf returns queued frames in order."""

    def __init__(self, *frames: pd.DataFrame) -> None:
        self._frames = list(frames)

    def resolve_table(self, name: str) -> str:
        return name

    def fetchdf(self, _sql: str) -> pd.DataFrame:
        return self._frames.pop(0)


def _ctx(*frames: pd.DataFrame) -> t.Any:
    """Fake context typed Any (the loader's ExecutionContext param is untyped)."""
    return _FakeContext(*frames)


def _ledger_frame() -> pd.DataFrame:
    """Two rows for one (account, security): a buy and a later sell.

    Columns mirror the loader's VARCHAR-cast fetch SQL — every decimal/date/
    timestamp arrives as a string, exactly as fetchdf returns after the CASTs.
    """
    return pd.DataFrame({
        "investment_transaction_id": ["inv_buy", "inv_sell"],
        "account_id": ["acct1", "acct1"],
        "security_id": ["sec1", "sec1"],
        "trade_date": ["2024-01-01", "2024-07-01"],
        "original_acquisition_date": [None, None],
        "type": ["buy", "sell"],
        "quantity": ["8.8888888888", "2.2222222222"],
        "price": ["112.50", "135.00"],
        "amount": ["-1000.00", "300.00"],
        "fees": ["0.00", "0.00"],
        "currency_code": ["USD", "USD"],
        "updated_at": ["2024-01-01 09:00:00", "2024-07-02 10:00:00"],
    })


def _securities_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "security_id": ["sec_hifo", "sec_fund_unset", "sec_stock_unset"],
        "cost_basis_method": ["hifo", None, None],
        # sec_fund_unset (etf) is average-eligible; sec_stock_unset (equity)
        # is not — together they prove the account-default 'average'
        # fallback applies only when the security's type permits it.
        "security_type": ["equity", "etf", "equity"],
    })


def _accounts_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "account_id": ["acct_avg"],
        "default_cost_basis_method": ["average"],
    })


def _selections_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "investment_transaction_id": ["d1", "d1", "d2"],
        "lot_id": ["lot_a", "lot_b", "lot_c"],
        "quantity": ["1.2345678901", "2.0000000000", "5.5"],
    })


def test_load_engine_inputs_maps_events_exactly_and_computes_freshness() -> None:
    events, _method_for, _selections_for, group_updated_at = load_engine_inputs(
        _ctx(
            _ledger_frame(),
            _securities_frame(),
            _accounts_frame(),
            _selections_frame(),
        )
    )

    assert len(events) == 2
    buy = events[0]
    assert buy.investment_transaction_id == "inv_buy"
    assert buy.account_id == "acct1"
    assert buy.security_id == "sec1"
    assert buy.trade_date == date(2024, 1, 1)
    assert buy.type == "buy"
    # Exact-Decimal equality proves the VARCHAR path avoids the float64 trap.
    assert buy.quantity == Decimal("8.8888888888")
    assert buy.amount == Decimal("-1000.00")
    assert buy.original_acquisition_date is None
    assert buy.currency_code == "USD"
    # Freshness is MAX over the group's ledger rows.
    assert group_updated_at == {("acct1", "sec1"): datetime(2024, 7, 2, 10, 0, 0)}


def test_load_engine_inputs_method_for_prefers_security_then_account_then_fifo() -> (
    None
):
    _events, method_for, _selections_for, _freshness = load_engine_inputs(
        _ctx(
            _ledger_frame(),
            _securities_frame(),
            _accounts_frame(),
            _selections_frame(),
        )
    )

    # Per-security election wins even when the account has a default.
    assert method_for("acct_avg", "sec_hifo") == "hifo"
    # Per-account 'average' default applies to an average-eligible security
    # (etf) when it has no per-security override.
    assert method_for("acct_avg", "sec_fund_unset") == "average"
    # Global fallback when neither elects.
    assert method_for("acct_other", "sec_fund_unset") == "fifo"


def test_load_engine_inputs_method_for_average_default_does_not_leak_to_stocks() -> (
    None
):
    # An account's 'average' default must not silently apply to a non-fund
    # security it happens to hold with no per-security override — Req 12
    # validates 'average' to mutual_fund/etf, and that restriction must hold
    # across the account-fallback path too, not just at direct election.
    _events, method_for, _selections_for, _freshness = load_engine_inputs(
        _ctx(
            _ledger_frame(),
            _securities_frame(),
            _accounts_frame(),
            _selections_frame(),
        )
    )
    assert method_for("acct_avg", "sec_stock_unset") == "fifo"


def test_load_engine_inputs_selections_group_by_disposal_with_exact_quantities() -> (
    None
):
    _events, _method_for, selections_for, _freshness = load_engine_inputs(
        _ctx(
            _ledger_frame(),
            _securities_frame(),
            _accounts_frame(),
            _selections_frame(),
        )
    )

    assert selections_for("d1") == [
        ("lot_a", Decimal("1.2345678901")),
        ("lot_b", Decimal("2.0000000000")),
    ]
    assert selections_for("d2") == [("lot_c", Decimal("5.5"))]
    assert selections_for("unknown") == []


def test_load_engine_inputs_cash_only_row_excluded_from_freshness_map() -> None:
    ledger = _ledger_frame()
    ledger.loc[1, "security_id"] = None  # a cash-only event opens no lot
    ledger.loc[1, "updated_at"] = "2025-01-01 00:00:00"
    events, _method_for, _selections_for, group_updated_at = load_engine_inputs(
        _ctx(ledger, _securities_frame(), _accounts_frame(), _selections_frame())
    )

    # The security-less row is still mapped as an event...
    assert len(events) == 2
    assert events[1].security_id is None
    # ...but never advances the (account, security) freshness key.
    assert group_updated_at == {("acct1", "sec1"): datetime(2024, 1, 1, 9, 0, 0)}
