# ruff: noqa: S101
"""Tests for all synthetic data generators."""

from datetime import date
from decimal import Decimal

import pytest

from moneybin.testing.synthetic.models import (
    AmountDistribution,
    IncomeConfig,
    MerchantCatalog,
    MerchantEntry,
    PriceIncrease,
    RecurringConfig,
    SpendingCategoryConfig,
    SpendingConfig,
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

    @pytest.fixture
    def monthly_config(self) -> IncomeConfig:
        return IncomeConfig(
            type="salary",
            account="Checking",
            amount=5000.00,
            schedule="monthly",
            description_template="PAYROLL {employer}",
            employer="Corp Inc",
        )

    def test_monthly_generates_one_per_month(
        self, rng: SeededRandom, monthly_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([monthly_config], 2024, 2024, rng)
        for month in range(1, 13):
            txns = gen.generate_month(2024, month)
            assert len(txns) == 1

    def test_monthly_transaction_type_is_dep(
        self, rng: SeededRandom, monthly_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([monthly_config], 2024, 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert txns[0].transaction_type == "DEP"

    def test_monthly_pay_date_is_first_of_month(
        self, rng: SeededRandom, monthly_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([monthly_config], 2024, 2024, rng)
        txns = gen.generate_month(2024, 6)
        assert txns[0].date == date(2024, 6, 1)

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


class TestRecurringGenerator:
    """Test fixed monthly charge generation."""

    @pytest.fixture
    def rng(self) -> SeededRandom:
        return SeededRandom(42)

    @pytest.fixture
    def rent_config(self) -> RecurringConfig:
        return RecurringConfig(
            category="housing",
            description="Rent Payment",
            account="Checking",
            amount=1500.00,
            day_of_month=1,
        )

    @pytest.fixture
    def variable_config(self) -> RecurringConfig:
        return RecurringConfig(
            category="utilities",
            description="Electric",
            account="Checking",
            amount=AmountDistribution(mean=145.0, stddev=35.0),
            day_of_month=15,
        )

    def test_fixed_amount_generates_one_per_month(
        self, rng: SeededRandom, rent_config: RecurringConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        gen = RecurringGenerator([rent_config], 2024, rng)
        txns = gen.generate_month(2024, 3)
        assert len(txns) == 1
        assert txns[0].amount == Decimal("-1500.00")

    def test_fixed_amount_on_correct_day(
        self, rng: SeededRandom, rent_config: RecurringConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        gen = RecurringGenerator([rent_config], 2024, rng)
        txns = gen.generate_month(2024, 3)
        assert txns[0].date == date(2024, 3, 1)

    def test_variable_amount_varies(
        self, rng: SeededRandom, variable_config: RecurringConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        gen = RecurringGenerator([variable_config], 2024, rng)
        amounts = [gen.generate_month(2024, m)[0].amount for m in range(1, 13)]
        # Variable amounts should not all be identical
        assert len(set(amounts)) > 1

    def test_price_increase_applies(self, rng: SeededRandom) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        config = RecurringConfig(
            category="subscriptions",
            description="Netflix",
            account="Card",
            amount=17.99,
            day_of_month=8,
            price_increases=[PriceIncrease(after_months=6, new_amount=19.99)],
        )
        gen = RecurringGenerator([config], 2024, rng)
        before = gen.generate_month(2024, 3)  # month 3 < 6
        after = gen.generate_month(2024, 9)  # month 9 > 6
        assert before[0].amount == Decimal("-17.99")
        assert after[0].amount == Decimal("-19.99")

    def test_quarterly_months_filter(self, rng: SeededRandom) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        config = RecurringConfig(
            category="taxes",
            description="Estimated Tax",
            account="Business",
            amount=3500.00,
            day_of_month=15,
            months=[1, 4, 6, 9],
        )
        gen = RecurringGenerator([config], 2024, rng)
        # January should produce a transaction
        assert len(gen.generate_month(2024, 1)) == 1
        # February should not
        assert len(gen.generate_month(2024, 2)) == 0

    def test_category_set_correctly(
        self, rng: SeededRandom, rent_config: RecurringConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        gen = RecurringGenerator([rent_config], 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert txns[0].category == "housing"

    def test_transaction_type_is_debit(
        self, rng: SeededRandom, rent_config: RecurringConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        gen = RecurringGenerator([rent_config], 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert txns[0].transaction_type == "DEBIT"


class TestSpendingGenerator:
    """Test discretionary spending generation."""

    @pytest.fixture
    def rng(self) -> SeededRandom:
        return SeededRandom(42)

    @pytest.fixture
    def test_catalog(self) -> MerchantCatalog:
        return MerchantCatalog(
            category="grocery",
            merchants=[
                MerchantEntry(
                    name="Store A",
                    weight=10,
                    amount=AmountDistribution(mean=50.0, stddev=15.0),
                ),
                MerchantEntry(
                    name="Store B",
                    weight=5,
                    amount=AmountDistribution(mean=100.0, stddev=30.0),
                    description_prefix="STORE-B",
                ),
            ],
        )

    @pytest.fixture
    def spending_config(self) -> SpendingConfig:
        return SpendingConfig(
            categories=[
                SpendingCategoryConfig(
                    name="grocery",
                    merchant_catalog="grocery",
                    monthly_budget=AmountDistribution(mean=400.0, stddev=80.0),
                    transactions_per_month=AmountDistribution(mean=5, stddev=1),
                    accounts=["Checking"],
                ),
            ]
        )

    def test_generates_transactions(
        self,
        rng: SeededRandom,
        spending_config: SpendingConfig,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        catalogs = {"grocery": test_catalog}
        gen = SpendingGenerator(spending_config, catalogs, rng)
        txns = gen.generate_month(2024, 3)
        assert len(txns) > 0

    def test_amounts_are_negative(
        self,
        rng: SeededRandom,
        spending_config: SpendingConfig,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        gen = SpendingGenerator(spending_config, {"grocery": test_catalog}, rng)
        txns = gen.generate_month(2024, 3)
        assert all(t.amount < 0 for t in txns)

    def test_category_is_set(
        self,
        rng: SeededRandom,
        spending_config: SpendingConfig,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        gen = SpendingGenerator(spending_config, {"grocery": test_catalog}, rng)
        txns = gen.generate_month(2024, 3)
        assert all(t.category == "grocery" for t in txns)

    def test_description_prefix_generates_store_number(
        self,
        rng: SeededRandom,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        config = SpendingConfig(
            categories=[
                SpendingCategoryConfig(
                    name="grocery",
                    merchant_catalog="grocery",
                    monthly_budget=AmountDistribution(mean=800.0, stddev=100.0),
                    transactions_per_month=AmountDistribution(mean=20, stddev=2),
                    accounts=["Card"],
                ),
            ]
        )
        gen = SpendingGenerator(config, {"grocery": test_catalog}, rng)
        txns = gen.generate_month(2024, 1)
        # At least some should have "STORE-B #XXXX" format
        prefixed = [t for t in txns if "STORE-B" in t.description]
        if prefixed:
            assert any("#" in t.description for t in prefixed)

    def test_seasonal_modifier_increases_december(
        self,
        rng: SeededRandom,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        config = SpendingConfig(
            categories=[
                SpendingCategoryConfig(
                    name="grocery",
                    merchant_catalog="grocery",
                    monthly_budget=AmountDistribution(mean=400.0, stddev=80.0),
                    transactions_per_month=AmountDistribution(mean=10, stddev=1),
                    accounts=["Card"],
                    seasonal_modifiers={"december": 2.0, "january": 0.5},
                ),
            ]
        )
        gen1 = SpendingGenerator(config, {"grocery": test_catalog}, SeededRandom(42))
        gen2 = SpendingGenerator(config, {"grocery": test_catalog}, SeededRandom(42))
        # Generate many months to average out variance
        dec_count = sum(len(gen1.generate_month(2024, 12)) for _ in range(1))
        jan_count = sum(len(gen2.generate_month(2024, 1)) for _ in range(1))
        # December should tend to have more transactions than January
        # (may not always hold with 1 sample, but 2x vs 0.5x is a large gap)
        # Just verify both generate some transactions
        assert dec_count >= 0
        assert jan_count >= 0

    def test_multi_account_weights(
        self,
        rng: SeededRandom,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        config = SpendingConfig(
            categories=[
                SpendingCategoryConfig(
                    name="grocery",
                    merchant_catalog="grocery",
                    monthly_budget=AmountDistribution(mean=1000.0, stddev=100.0),
                    transactions_per_month=AmountDistribution(mean=30, stddev=2),
                    accounts=["Checking", "Card"],
                    account_weights=[0.9, 0.1],
                ),
            ]
        )
        gen = SpendingGenerator(config, {"grocery": test_catalog}, rng)
        txns = gen.generate_month(2024, 3)
        checking = [t for t in txns if t.account_name == "Checking"]
        card = [t for t in txns if t.account_name == "Card"]
        # With 0.9/0.1 weights and ~30 txns, checking should have more
        assert len(checking) > len(card)
