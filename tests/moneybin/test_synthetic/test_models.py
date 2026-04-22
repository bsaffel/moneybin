# ruff: noqa: S101
"""Tests for Pydantic YAML validation models and data loading."""

import pytest

from moneybin.testing.synthetic.models import (
    AccountConfig,
    AmountDistribution,
    GeneratedTransaction,
    MerchantCatalog,
    MerchantEntry,
    PersonaConfig,
    RecurringConfig,
    SpendingCategoryConfig,
    TransferConfig,
    load_merchant_catalog,
    load_persona,
)


class TestAmountDistribution:
    """Test the AmountDistribution model."""

    def test_fixed_amount(self) -> None:
        dist = AmountDistribution(mean=17.99, stddev=0.0)
        assert dist.mean == 17.99
        assert dist.stddev == 0.0

    def test_variable_amount(self) -> None:
        dist = AmountDistribution(mean=145.00, stddev=40.00)
        assert dist.mean == 145.00
        assert dist.stddev == 40.00


class TestMerchantCatalog:
    """Test merchant catalog validation."""

    def test_valid_catalog(self) -> None:
        catalog = MerchantCatalog(
            category="grocery",
            merchants=[
                MerchantEntry(
                    name="Store A",
                    weight=10,
                    amount=AmountDistribution(mean=50.0, stddev=15.0),
                ),
            ],
        )
        assert catalog.category == "grocery"
        assert len(catalog.merchants) == 1

    def test_empty_merchants_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            MerchantCatalog(category="test", merchants=[])

    def test_zero_weight_rejected(self) -> None:
        with pytest.raises(ValueError):
            MerchantEntry(
                name="Bad",
                weight=0,
                amount=AmountDistribution(mean=10.0),
            )

    def test_description_prefix_optional(self) -> None:
        entry = MerchantEntry(
            name="Costco",
            weight=10,
            amount=AmountDistribution(mean=145.0, stddev=40.0),
            description_prefix="COSTCO WHSE",
        )
        assert entry.description_prefix == "COSTCO WHSE"


class TestPersonaConfig:
    """Test persona YAML validation."""

    @pytest.fixture
    def minimal_persona_dict(self) -> dict:
        return {
            "persona": "test",
            "profile": "test-profile",
            "description": "A test persona",
            "years_default": 1,
            "accounts": [
                {
                    "name": "Checking",
                    "type": "checking",
                    "source_type": "ofx",
                    "institution": "Test Bank",
                    "opening_balance": 1000.00,
                },
            ],
            "income": [
                {
                    "type": "salary",
                    "account": "Checking",
                    "amount": 3000.00,
                    "schedule": "biweekly",
                    "pay_day": "friday",
                    "description_template": "DIRECT DEP {employer}",
                    "employer": "TestCo",
                },
            ],
            "recurring": [
                {
                    "category": "housing",
                    "description": "Rent",
                    "account": "Checking",
                    "amount": 1500.00,
                    "day_of_month": 1,
                },
            ],
            "spending": {
                "categories": [
                    {
                        "name": "grocery",
                        "merchant_catalog": "grocery",
                        "monthly_budget": {"mean": 400.0, "stddev": 80.0},
                        "transactions_per_month": {"mean": 5, "stddev": 1},
                        "accounts": ["Checking"],
                    },
                ],
            },
            "transfers": [],
        }

    def test_valid_persona_loads(self, minimal_persona_dict: dict) -> None:
        persona = PersonaConfig.model_validate(minimal_persona_dict)
        assert persona.persona == "test"
        assert len(persona.accounts) == 1
        assert persona.accounts[0].name == "Checking"

    def test_income_references_unknown_account_rejected(
        self, minimal_persona_dict: dict
    ) -> None:
        minimal_persona_dict["income"][0]["account"] = "Nonexistent"
        with pytest.raises(ValueError, match="unknown account.*Nonexistent"):
            PersonaConfig.model_validate(minimal_persona_dict)

    def test_recurring_references_unknown_account_rejected(
        self, minimal_persona_dict: dict
    ) -> None:
        minimal_persona_dict["recurring"][0]["account"] = "Nonexistent"
        with pytest.raises(ValueError, match="unknown account.*Nonexistent"):
            PersonaConfig.model_validate(minimal_persona_dict)

    def test_spending_references_unknown_account_rejected(
        self, minimal_persona_dict: dict
    ) -> None:
        minimal_persona_dict["spending"]["categories"][0]["accounts"] = ["Nonexistent"]
        with pytest.raises(ValueError, match="unknown account.*Nonexistent"):
            PersonaConfig.model_validate(minimal_persona_dict)

    def test_transfer_from_alias(self) -> None:
        """Transfer config uses 'from'/'to' YAML keys mapped to Python fields."""
        config = TransferConfig.model_validate({
            "from": "Checking",
            "to": "Savings",
            "amount": 500.0,
            "schedule": "monthly",
            "day_of_month": 5,
        })
        assert config.from_account == "Checking"
        assert config.to_account == "Savings"

    def test_transfer_statement_balance(self) -> None:
        config = TransferConfig.model_validate({
            "from": "Checking",
            "to": "Visa",
            "amount": "statement_balance",
            "schedule": "monthly",
            "day_of_month": 20,
        })
        assert config.amount == "statement_balance"

    def test_recurring_amount_can_be_distribution(
        self, minimal_persona_dict: dict
    ) -> None:
        minimal_persona_dict["recurring"][0]["amount"] = {
            "mean": 145.0,
            "stddev": 35.0,
        }
        persona = PersonaConfig.model_validate(minimal_persona_dict)
        assert isinstance(persona.recurring[0].amount, AmountDistribution)

    def test_day_of_month_over_28_rejected(self) -> None:
        with pytest.raises(ValueError):
            RecurringConfig(
                category="test",
                description="Test",
                account="Checking",
                amount=100.0,
                day_of_month=29,
            )

    def test_invalid_source_type_rejected(self) -> None:
        with pytest.raises(ValueError):
            AccountConfig(
                name="Bad",
                type="checking",
                source_type="parquet",  # type: ignore[arg-type]  # intentionally invalid to test rejection
                institution="Test Bank",
            )


class TestSpendingCategoryConfig:
    """Test spending category config validation."""

    def test_account_weights_length_mismatch_rejected(self) -> None:
        with pytest.raises(ValueError, match="account_weights length"):
            SpendingCategoryConfig(
                name="grocery",
                merchant_catalog="grocery",
                monthly_budget=AmountDistribution(mean=400.0, stddev=80.0),
                transactions_per_month=AmountDistribution(mean=5.0, stddev=1.0),
                accounts=["Checking", "Visa"],
                account_weights=[0.6, 0.3, 0.1],
            )


class TestRuntimeDataclasses:
    """Test the runtime dataclasses used during generation."""

    def test_generated_transaction_defaults(self) -> None:
        from datetime import date
        from decimal import Decimal

        txn = GeneratedTransaction(
            date=date(2024, 1, 15),
            amount=Decimal("-42.50"),
            description="Test Store",
            account_name="Checking",
        )
        assert txn.transaction_type == "DEBIT"
        assert txn.category is None
        assert txn.transfer_pair_id is None
        assert txn.transaction_id == ""


class TestYAMLDataLoading:
    """Validate all shipped YAML data files load through Pydantic."""

    MERCHANT_CATALOGS = [
        "grocery",
        "dining",
        "transport",
        "utilities",
        "entertainment",
        "shopping",
        "health",
        "travel",
        "subscriptions",
        "kids",
        "personal_care",
        "insurance",
        "education",
        "gifts",
    ]
    PERSONAS = ["basic", "family", "freelancer"]

    @pytest.mark.parametrize("catalog", MERCHANT_CATALOGS)
    def test_merchant_catalog_loads(self, catalog: str) -> None:
        result = load_merchant_catalog(catalog)
        assert result.category == catalog
        assert len(result.merchants) >= 5

    @pytest.mark.parametrize("persona", PERSONAS)
    def test_persona_loads(self, persona: str) -> None:
        result = load_persona(persona)
        assert result.persona == persona
        assert len(result.accounts) >= 1
        assert len(result.income) >= 1

    def test_unknown_persona_raises(self) -> None:
        with pytest.raises(FileNotFoundError, match="Unknown persona"):
            load_persona("nonexistent")

    def test_unknown_catalog_raises(self) -> None:
        with pytest.raises(FileNotFoundError, match="Unknown merchant catalog"):
            load_merchant_catalog("nonexistent")

    def test_persona_merchant_catalogs_exist(self) -> None:
        """Every merchant_catalog referenced in personas has a matching file."""
        for persona_name in self.PERSONAS:
            persona = load_persona(persona_name)
            for cat in persona.spending.categories:
                catalog = load_merchant_catalog(cat.merchant_catalog)
                assert len(catalog.merchants) > 0, (
                    f"Persona {persona_name!r} references empty catalog "
                    f"{cat.merchant_catalog!r}"
                )
