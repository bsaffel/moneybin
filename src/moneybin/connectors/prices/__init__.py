"""External market-data price feeds.

Two adapters behind one Protocol, each pulling public prices into
raw.security_prices. Provider identity is data in the `source_type` column, so
adding one needs no runtime registration — see
docs/specs/investments-price-feeds.md § "Adding or retiring a provider".
"""

from moneybin.connectors.prices.coingecko import CoinGeckoPriceAdapter
from moneybin.connectors.prices.protocol import (
    PriceAdapter,
    PriceFetchFailure,
    PriceFetchResult,
    PriceObservation,
    SecurityRef,
)
from moneybin.connectors.prices.tiingo import TiingoPriceAdapter

__all__ = [
    "CoinGeckoPriceAdapter",
    "PriceAdapter",
    "PriceFetchFailure",
    "PriceFetchResult",
    "PriceObservation",
    "SecurityRef",
    "TiingoPriceAdapter",
]
