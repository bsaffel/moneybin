"""The trade-implied branch must admit only genuine executions.

`core.fct_security_prices` treats a ledger row's `price` as a market close. That
is true of a trade and false of a cash distribution: `stg_plaid__investment_
transactions` NULLs `quantity` for cash-type events but passes `price` through
verbatim, and a dividend's `price` is the per-share distribution RATE. Unioned in
as a close, a $0.91/share dividend on a $290 ETF becomes that security's newest
price — and `dim_holdings.latest_price` takes the newest date with no source
filter, so a 500-share position publishes a market value ~300x too low, with
`valuation_status = 'valued'` and no warning anywhere.

These are unit tests over the model's SQL text, not the built model, so they run
in the fast gate rather than only under `make test-scenarios`.
"""

from __future__ import annotations

from moneybin.services.investment_service import (
    _AMOUNT_REQUIRED,  # pyright: ignore[reportPrivateUsage]  # the vocabulary under test
    _QTY_NULL,  # pyright: ignore[reportPrivateUsage]  # the vocabulary under test
    _SECURITY_REQUIRED,  # pyright: ignore[reportPrivateUsage]  # the vocabulary under test
)
from tests.moneybin.price_model_helpers import trade_implied_types


def test_no_cash_only_ledger_type_can_become_a_market_close() -> None:
    """Derived from the ledger vocabulary, so a NEW cash type is covered too.

    `_QTY_NULL` is the set of events that move no shares. Several of them
    legitimately carry both a security and a price — dividend, fee,
    capital_gain_distribution, return_of_capital — which is exactly why filtering
    on `security_id IS NOT NULL AND price > 0` does not separate them from
    trades. Asserting the disjointness rather than a literal list means adding a
    cash type to the vocabulary cannot quietly admit it here.
    """
    admitted = trade_implied_types()

    assert admitted.isdisjoint(_QTY_NULL), (
        "these cash-only ledger types are admitted as market closes: "
        f"{sorted(admitted & _QTY_NULL)}"
    )


def test_only_executions_are_admitted() -> None:
    """A price is a market close exactly when an execution set it.

    `_AMOUNT_REQUIRED` is that set — the events where cash actually changed hands
    against shares. The two transfer types are deliberately excluded even though
    `_SECURITY_REQUIRED` includes them: a transfer's price carries cost basis
    across accounts, so treating it as a close would publish an acquisition price
    from years earlier as today's value.
    """
    assert trade_implied_types() == set(_AMOUNT_REQUIRED)
    assert {"transfer_in", "transfer_out"} <= _SECURITY_REQUIRED
    assert not {"transfer_in", "transfer_out"} & trade_implied_types()
