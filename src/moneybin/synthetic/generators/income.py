"""Income generation: salary deposits and freelance invoices."""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from decimal import Decimal

from moneybin.synthetic.models import (
    AmountDistribution,
    GeneratedTransaction,
    IncomeConfig,
)
from moneybin.synthetic.seed import SeededRandom

# calendar.day_name uses Monday=0 convention matching date.weekday()
_DAY_MAP = {name.lower(): i for i, name in enumerate(calendar.day_name)}


class IncomeGenerator:
    """Generate income transactions: biweekly salary and irregular freelance.

    Args:
        incomes: Income configurations from persona YAML.
        start_year: First year of the generation range.
        end_year: Last year of the generation range.
        rng: Seeded random number generator.
    """

    def __init__(  # noqa: D107 — args documented in class docstring
        self,
        incomes: list[IncomeConfig],
        start_year: int,
        end_year: int,
        rng: SeededRandom,
    ) -> None:
        self._incomes = incomes
        self._start_year = start_year
        self._end_year = end_year
        self._rng = rng

        # Pre-compute biweekly pay dates keyed by (year, month) for O(1) lookup
        self._biweekly_dates: dict[int, dict[tuple[int, int], list[date]]] = {}
        for i, config in enumerate(incomes):
            if config.schedule == "biweekly":
                dates = self._compute_biweekly(
                    start_year, end_year, config.pay_day or "friday"
                )
                by_month: dict[tuple[int, int], list[date]] = {}
                for d in dates:
                    by_month.setdefault((d.year, d.month), []).append(d)
                self._biweekly_dates[i] = by_month

    def _compute_biweekly(
        self, start_year: int, end_year: int, pay_day: str
    ) -> list[date]:
        """Compute all biweekly pay dates across the full range."""
        target_dow = _DAY_MAP[pay_day.lower()]
        d = date(start_year, 1, 1)
        # Find first occurrence of pay_day
        while d.weekday() != target_dow:
            d += timedelta(days=1)
        dates: list[date] = []
        end = date(end_year, 12, 31)
        while d <= end:
            dates.append(d)
            d += timedelta(days=14)
        return dates

    def _apply_raise(self, base_amount: float, year: int, raise_pct: float) -> float:
        """Apply annual raises relative to start year."""
        years_elapsed = year - self._start_year
        return base_amount * (1 + raise_pct / 100) ** years_elapsed

    def generate_month(self, year: int, month: int) -> list[GeneratedTransaction]:
        """Generate income transactions for a single month.

        Args:
            year: Calendar year.
            month: Calendar month (1-12).

        Returns:
            List of income transactions for this month.
        """
        txns: list[GeneratedTransaction] = []

        for i, config in enumerate(self._incomes):
            if config.schedule == "biweekly":
                txns.extend(self._generate_biweekly(i, config, year, month))
            elif config.schedule == "monthly":
                txns.extend(self._generate_monthly(config, year, month))
            elif config.schedule == "irregular":
                txns.extend(self._generate_irregular(config, year, month))

        return txns

    def _generate_biweekly(
        self, index: int, config: IncomeConfig, year: int, month: int
    ) -> list[GeneratedTransaction]:
        dates = self._biweekly_dates[index].get((year, month), [])
        base = (
            config.amount.mean
            if isinstance(config.amount, AmountDistribution)
            else float(config.amount)
        )
        amount = self._apply_raise(base, year, config.annual_raise_pct)
        description = config.description_template.format(employer=config.employer or "")
        return [
            GeneratedTransaction(
                date=d,
                amount=Decimal(str(round(amount, 2))),
                description=description,
                account_name=config.account,
                category="income",
                transaction_type="DIRECTDEP",
            )
            for d in dates
        ]

    def _generate_monthly(
        self, config: IncomeConfig, year: int, month: int
    ) -> list[GeneratedTransaction]:
        base = (
            config.amount.mean
            if isinstance(config.amount, AmountDistribution)
            else float(config.amount)
        )
        amount = self._apply_raise(base, year, config.annual_raise_pct)
        # Pay on the 1st of the month
        pay_date = date(year, month, 1)
        description = config.description_template.format(employer=config.employer or "")
        return [
            GeneratedTransaction(
                date=pay_date,
                amount=Decimal(str(round(amount, 2))),
                description=description,
                account_name=config.account,
                category="income",
                transaction_type="DEP",
            )
        ]

    def _generate_irregular(
        self, config: IncomeConfig, year: int, month: int
    ) -> list[GeneratedTransaction]:
        if config.count_per_month is None:
            count = 1
        else:
            count = max(
                0,
                round(
                    self._rng.gauss(
                        config.count_per_month.mean, config.count_per_month.stddev
                    )
                ),
            )
        if count == 0:
            return []

        amount_dist = (
            config.amount
            if isinstance(config.amount, AmountDistribution)
            else AmountDistribution(mean=config.amount)
        )
        txns: list[GeneratedTransaction] = []
        for _ in range(count):
            amount = max(
                100.0, self._rng.log_normal(amount_dist.mean, amount_dist.stddev)
            )
            day = self._rng.day_in_month(year, month)
            txns.append(
                GeneratedTransaction(
                    date=date(year, month, day),
                    amount=Decimal(str(round(amount, 2))),
                    description=config.description_template or "PAYMENT RECEIVED",
                    account_name=config.account,
                    category="income",
                    transaction_type="DEP",
                )
            )
        return txns
