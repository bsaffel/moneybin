"""External exchange-rate feeds.

One adapter behind one Protocol, pulling public reference rates into
raw.exchange_rates. Provider identity is data in the `source` column, so adding
one needs no runtime registration.
"""

from moneybin.connectors.rates.frankfurter import FrankfurterRateAdapter
from moneybin.connectors.rates.protocol import RateAdapter, RateObservation

__all__ = [
    "FrankfurterRateAdapter",
    "RateAdapter",
    "RateObservation",
]
