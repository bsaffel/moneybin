"""Discretionary spending generation from merchant catalogs."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from moneybin.testing.synthetic.models import (
    GeneratedTransaction,
    MerchantCatalog,
    MerchantEntry,
    SpendingConfig,
)
from moneybin.testing.synthetic.seed import SeededRandom

_MONTH_NAMES = {
    1: "january",
    2: "february",
    3: "march",
    4: "april",
    5: "may",
    6: "june",
    7: "july",
    8: "august",
    9: "september",
    10: "october",
    11: "november",
    12: "december",
}

_CITIES = [
    "AUSTIN TX",
    "DENVER CO",
    "SEATTLE WA",
    "CHICAGO IL",
    "PHOENIX AZ",
    "PORTLAND OR",
    "MIAMI FL",
    "ATLANTA GA",
    "DALLAS TX",
    "NASHVILLE TN",
    "RALEIGH NC",
    "SAN JOSE CA",
    "COLUMBUS OH",
    "MINNEAPOLIS MN",
    "CHARLOTTE NC",
    "TAMPA FL",
    "SALT LAKE UT",
    "BOISE ID",
]


class SpendingGenerator:
    """Generate discretionary spending from merchant catalogs.

    Selects merchants by weighted random, sizes amounts from per-merchant
    log-normal distributions, applies day-of-week bias and seasonal modifiers.

    Args:
        spending: Spending configuration from persona YAML.
        catalogs: Loaded merchant catalogs keyed by category name.
        rng: Seeded random number generator.
    """

    def __init__(
        self,
        spending: SpendingConfig,
        catalogs: dict[str, MerchantCatalog],
        rng: SeededRandom,
    ) -> None:
        """Store configuration and seed for month-by-month generation."""
        self._categories = spending.categories
        self._catalogs = catalogs
        self._rng = rng

    def _make_description(self, merchant: MerchantEntry) -> str:
        """Generate a bank-statement-style description for a merchant.

        Args:
            merchant: The merchant entry from the catalog.

        Returns:
            A description string. If the merchant has a description_prefix,
            returns "PREFIX #XXXX CITY ST"; otherwise returns the merchant name.
        """
        if merchant.description_prefix:
            store_num = self._rng.randint(1000, 9999)
            city = self._rng.choice(_CITIES)
            return f"{merchant.description_prefix} #{store_num} {city}"
        return merchant.name

    def generate_month(self, year: int, month: int) -> list[GeneratedTransaction]:
        """Generate discretionary spending transactions for a single month.

        Args:
            year: Calendar year.
            month: Calendar month (1-12).

        Returns:
            List of spending transactions (amounts are negative).
        """
        txns: list[GeneratedTransaction] = []
        month_name = _MONTH_NAMES[month]

        for cat_config in self._categories:
            catalog = self._catalogs[cat_config.merchant_catalog]
            merchant_names = [m.name for m in catalog.merchants]
            merchant_weights = [float(m.weight) for m in catalog.merchants]
            merchant_lookup = {m.name: m for m in catalog.merchants}

            # Apply seasonal modifier to transaction count
            seasonal_mult = cat_config.seasonal_modifiers.get(month_name, 1.0)
            base_count = max(
                0,
                round(
                    self._rng.gauss(
                        cat_config.transactions_per_month.mean * seasonal_mult,
                        cat_config.transactions_per_month.stddev,
                    )
                ),
            )

            for _ in range(base_count):
                # Select merchant by weight
                merchant_name = self._rng.weighted_choice(
                    merchant_names, merchant_weights
                )
                merchant = merchant_lookup[merchant_name]

                # Generate amount from merchant's log-normal distribution
                amount = max(
                    0.01,
                    self._rng.log_normal(merchant.amount.mean, merchant.amount.stddev),
                )

                # Select account, optionally weighted
                if cat_config.account_weights:
                    account = self._rng.weighted_choice(
                        cat_config.accounts,
                        [float(w) for w in cat_config.account_weights],
                    )
                else:
                    account = self._rng.choice(cat_config.accounts)

                # Select day with optional day-of-week bias
                day_weights = cat_config.day_of_week_weights or None
                day = self._rng.day_in_month(year, month, day_weights)

                description = self._make_description(merchant)

                txns.append(
                    GeneratedTransaction(
                        date=date(year, month, day),
                        amount=Decimal(str(round(-amount, 2))),
                        description=description,
                        account_name=account,
                        category=cat_config.name,
                        transaction_type="DEBIT",
                    )
                )

        return txns
