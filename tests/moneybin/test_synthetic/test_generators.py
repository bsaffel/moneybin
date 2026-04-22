# ruff: noqa: S101
"""Tests for all synthetic data generators."""

import pytest

from moneybin.testing.synthetic.models import (
    AmountDistribution,
    IncomeConfig,
)
from moneybin.testing.synthetic.seed import SeededRandom


class TestIncomeGenerator:
    """Test salary and freelance income generation."""

    @pytest.fixture
    def rng(self) -> SeededRandom:
        return SeededRandom(42)

    @pytest.fixture
    def salary_config(self) -> IncomeConfig:
        return IncomeConfig(
            type="salary",
            account="Checking",
            amount=4200.00,
            schedule="biweekly",
            pay_day="friday",
            annual_raise_pct=3.0,
            description_template="DIRECT DEP {employer}",
            employer="Acme Corp",
        )

    @pytest.fixture
    def freelance_config(self) -> IncomeConfig:
        return IncomeConfig(
            type="freelance",
            account="Business",
            amount=AmountDistribution(mean=5000.0, stddev=2000.0),
            schedule="irregular",
            count_per_month=AmountDistribution(mean=2.5, stddev=1.0),
            description_template="CLIENT PAYMENT",
        )

    def test_biweekly_salary_26_deposits_per_year(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2024, rng)
        all_txns = []
        for month in range(1, 13):
            all_txns.extend(gen.generate_month(2024, month))
        assert len(all_txns) == 26

    def test_salary_amounts_positive(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert all(t.amount > 0 for t in txns)

    def test_salary_description_includes_employer(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert all("Acme Corp" in t.description for t in txns)

    def test_annual_raise_increases_amount(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2025, rng)
        jan_2024 = gen.generate_month(2024, 1)
        jan_2025 = gen.generate_month(2025, 1)
        assert jan_2025[0].amount > jan_2024[0].amount

    def test_salary_transaction_type_is_directdep(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert all(t.transaction_type == "DIRECTDEP" for t in txns)

    def test_salary_category_is_income(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert all(t.category == "income" for t in txns)

    def test_freelance_generates_variable_count(
        self, rng: SeededRandom, freelance_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([freelance_config], 2024, 2024, rng)
        monthly_counts = [len(gen.generate_month(2024, m)) for m in range(1, 13)]
        # Not all months should have the same count
        assert len(set(monthly_counts)) > 1

    def test_freelance_amounts_positive(
        self, rng: SeededRandom, freelance_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([freelance_config], 2024, 2024, rng)
        all_txns = []
        for month in range(1, 13):
            all_txns.extend(gen.generate_month(2024, month))
        assert all(t.amount > 0 for t in all_txns)

    def test_deterministic_output(self) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        config = IncomeConfig(
            type="salary",
            account="Checking",
            amount=3000.0,
            schedule="biweekly",
            pay_day="friday",
        )
        gen1 = IncomeGenerator([config], 2024, 2024, SeededRandom(42))
        gen2 = IncomeGenerator([config], 2024, 2024, SeededRandom(42))
        txns1 = gen1.generate_month(2024, 3)
        txns2 = gen2.generate_month(2024, 3)
        assert [(t.date, t.amount) for t in txns1] == [
            (t.date, t.amount) for t in txns2
        ]
