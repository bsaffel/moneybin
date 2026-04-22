"""Recurring charge generation: rent, utilities, subscriptions, insurance."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from moneybin.testing.synthetic.models import (
    AmountDistribution,
    GeneratedTransaction,
    RecurringConfig,
)
from moneybin.testing.synthetic.seed import SeededRandom


class RecurringGenerator:
    """Generate fixed monthly charges with optional variability and price increases.

    Args:
        charges: Recurring charge configurations from persona YAML.
        start_year: First year of the generation range (for price increase offset).
        rng: Seeded random number generator.
    """

    def __init__(  # noqa: D107 — args documented in class docstring
        self,
        charges: list[RecurringConfig],
        start_year: int,
        rng: SeededRandom,
    ) -> None:
        self._charges = charges
        self._start_year = start_year
        self._rng = rng

        # Pre-sort price increases so _effective_amount doesn't sort each call
        self._sorted_increases = {
            i: sorted(c.price_increases, key=lambda p: p.after_months)
            for i, c in enumerate(charges)
            if c.price_increases
        }

    def _months_elapsed(self, year: int, month: int) -> int:
        """Months since the start of generation."""
        return (year - self._start_year) * 12 + (month - 1)

    def _effective_amount(
        self, index: int, config: RecurringConfig, year: int, month: int
    ) -> float:
        """Get the current amount, applying any price increases.

        Args:
            index: Index of the config in self._charges.
            config: The recurring charge configuration.
            year: Calendar year of the charge.
            month: Calendar month of the charge.

        Returns:
            The effective amount as a positive float.
        """
        if isinstance(config.amount, AmountDistribution):
            return max(
                1.0, self._rng.log_normal(config.amount.mean, config.amount.stddev)
            )
        base = config.amount
        elapsed = self._months_elapsed(year, month)
        for increase in self._sorted_increases.get(index, []):
            if elapsed >= increase.after_months:
                base = increase.new_amount
        return base

    def generate_month(self, year: int, month: int) -> list[GeneratedTransaction]:
        """Generate recurring charges for a single month.

        Args:
            year: Calendar year.
            month: Calendar month (1-12).

        Returns:
            List of recurring charge transactions (amounts are negative).
        """
        txns: list[GeneratedTransaction] = []

        for i, config in enumerate(self._charges):
            # Skip if month is filtered
            if config.months is not None and month not in config.months:
                continue

            amount = self._effective_amount(i, config, year, month)
            txn_date = date(year, month, config.day_of_month)
            txns.append(
                GeneratedTransaction(
                    date=txn_date,
                    amount=Decimal(str(round(-abs(amount), 2))),
                    description=config.description,
                    account_name=config.account,
                    category=config.category,
                    transaction_type="DEBIT",
                )
            )

        return txns
