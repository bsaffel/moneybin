# ruff: noqa: S101
"""Tests for Pydantic YAML validation models and data loading."""

from typing import Any

import pytest

from moneybin.synthetic.models import (
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
    def minimal_persona_dict(self) -> dict[str, Any]:
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

    def test_valid_persona_loads(self, minimal_persona_dict: dict[str, Any]) -> None:
        persona = PersonaConfig.model_validate(minimal_persona_dict)
        assert persona.persona == "test"
        assert len(persona.accounts) == 1
        assert persona.accounts[0].name == "Checking"

    def test_income_references_unknown_account_rejected(
        self, minimal_persona_dict: dict[str, Any]
    ) -> None:
        minimal_persona_dict["income"][0]["account"] = "Nonexistent"
        with pytest.raises(ValueError, match="unknown account.*Nonexistent"):
            PersonaConfig.model_validate(minimal_persona_dict)

    def test_recurring_references_unknown_account_rejected(
        self, minimal_persona_dict: dict[str, Any]
    ) -> None:
        minimal_persona_dict["recurring"][0]["account"] = "Nonexistent"
        with pytest.raises(ValueError, match="unknown account.*Nonexistent"):
            PersonaConfig.model_validate(minimal_persona_dict)

    def test_spending_references_unknown_account_rejected(
        self, minimal_persona_dict: dict[str, Any]
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

    def test_transfer_received_amount_defaults_to_none(self) -> None:
        config = TransferConfig.model_validate({
            "from": "Checking",
            "to": "Savings",
            "amount": 100.0,
            "schedule": "monthly",
            "day_of_month": 5,
        })

        assert config.received_amount is None

    @pytest.mark.parametrize("received_amount", [0, -1])
    def test_transfer_received_amount_must_be_positive(
        self, received_amount: int
    ) -> None:
        with pytest.raises(ValueError):
            TransferConfig.model_validate({
                "from": "Checking",
                "to": "Savings",
                "amount": 100.0,
                "received_amount": received_amount,
                "schedule": "monthly",
                "day_of_month": 5,
            })

    def test_recurring_amount_can_be_distribution(
        self, minimal_persona_dict: dict[str, Any]
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

    def test_currency_code_defaults_to_usd(self) -> None:
        """Existing personas declare no currency, so they must stay USD."""
        account = AccountConfig(
            name="Chase Checking",
            type="checking",
            source_type="ofx",
            institution="Chase Bank",
        )
        assert account.currency_code == "USD"

    def test_currency_code_is_configurable_per_account(self) -> None:
        account = AccountConfig(
            name="UAE Checking",
            type="checking",
            source_type="csv",
            institution="Emirates Bank",
            currency_code="AED",
        )
        assert account.currency_code == "AED"

    @pytest.mark.parametrize("bad_code", ["usd", "US", "USDD", "U5D", "", "  "])
    def test_malformed_currency_code_rejected(self, bad_code: str) -> None:
        """A typo would reach raw and core verbatim and split the report segments.

        `reports.net_worth` groups by this string and the doctor's currency check
        treats every non-null value as known, so `usd` becomes a second segment
        beside `USD` rather than an error.
        """
        with pytest.raises(ValueError, match="3 uppercase letters"):
            AccountConfig(
                name="Typo",
                type="checking",
                source_type="ofx",
                institution="Test Bank",
                currency_code=bad_code,
            )

    def test_provider_unsupported_currency_code_still_accepted(self) -> None:
        """Shape, not membership — AED is deliberately outside the FX provider."""
        account = AccountConfig(
            name="Dubai Checking",
            type="checking",
            source_type="csv",
            institution="Emirates Bank",
            currency_code="AED",
        )
        assert account.currency_code == "AED"

    def test_cross_currency_transfer_rejected(
        self, minimal_persona_dict: dict[str, Any]
    ) -> None:
        """A cross-currency transfer cannot infer its received amount."""
        minimal_persona_dict["accounts"].append({
            "name": "Eurozone Savings",
            "type": "savings",
            "source_type": "ofx",
            "institution": "Test Bank EU",
            "opening_balance": 500.00,
            "currency_code": "EUR",
        })
        minimal_persona_dict["transfers"] = [
            {
                "from": "Checking",
                "to": "Eurozone Savings",
                "amount": 100.0,
                "schedule": "monthly",
                "day_of_month": 5,
            }
        ]
        with pytest.raises(ValueError, match="cross-currency transfer"):
            PersonaConfig.model_validate(minimal_persona_dict)

    def test_cross_currency_transfer_with_received_amount_allowed(
        self, minimal_persona_dict: dict[str, Any]
    ) -> None:
        minimal_persona_dict["accounts"].append({
            "name": "Eurozone Savings",
            "type": "savings",
            "source_type": "ofx",
            "institution": "Test Bank EU",
            "opening_balance": 500.00,
            "currency_code": "EUR",
        })
        minimal_persona_dict["transfers"] = [
            {
                "from": "Checking",
                "to": "Eurozone Savings",
                "amount": 100.0,
                "received_amount": 90.0,
                "schedule": "monthly",
                "day_of_month": 5,
            }
        ]

        persona = PersonaConfig.model_validate(minimal_persona_dict)

        assert persona.transfers[0].received_amount == 90.0

    def test_cross_currency_statement_balance_rejected(
        self, minimal_persona_dict: dict[str, Any]
    ) -> None:
        minimal_persona_dict["accounts"].append({
            "name": "Eurozone Savings",
            "type": "savings",
            "source_type": "ofx",
            "institution": "Test Bank EU",
            "opening_balance": 500.00,
            "currency_code": "EUR",
        })
        minimal_persona_dict["transfers"] = [
            {
                "from": "Checking",
                "to": "Eurozone Savings",
                "amount": "statement_balance",
                "received_amount": 90.0,
                "schedule": "monthly",
                "day_of_month": 5,
            }
        ]

        with pytest.raises(ValueError, match="statement_balance"):
            PersonaConfig.model_validate(minimal_persona_dict)

    def test_same_currency_transfer_still_allowed(
        self, minimal_persona_dict: dict[str, Any]
    ) -> None:
        """The guard must reject only the unconvertible case, not transfers."""
        minimal_persona_dict["accounts"].append({
            "name": "Savings",
            "type": "savings",
            "source_type": "ofx",
            "institution": "Test Bank",
            "opening_balance": 500.00,
        })
        minimal_persona_dict["transfers"] = [
            {
                "from": "Checking",
                "to": "Savings",
                "amount": 100.0,
                "schedule": "monthly",
                "day_of_month": 5,
            }
        ]
        persona = PersonaConfig.model_validate(minimal_persona_dict)
        assert len(persona.transfers) == 1


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
    PERSONAS = ["basic", "family", "freelancer", "international"]

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

    # Personas that deliberately hold more than one currency. Everything else
    # is asserted USD-only below, so a new persona is covered the day it lands
    # rather than the day someone remembers to add it here.
    MULTI_CURRENCY_PERSONAS = {"international"}

    def test_single_currency_personas_stay_usd(self) -> None:
        """Every persona but the declared multi-currency ones must stay USD."""
        single = [p for p in self.PERSONAS if p not in self.MULTI_CURRENCY_PERSONAS]
        assert single, "the exemption set swallowed every persona"
        for persona_name in single:
            persona = load_persona(persona_name)
            currencies = {acct.currency_code for acct in persona.accounts}
            assert currencies == {"USD"}, (
                f"Persona {persona_name!r} changed currency: {currencies}"
            )

    def test_international_persona_spans_five_currencies(self) -> None:
        persona = load_persona("international")
        currencies = {acct.currency_code for acct in persona.accounts}
        assert currencies == {"USD", "EUR", "GBP", "CAD", "AED"}

    def test_international_persona_covers_an_unpriced_currency(self) -> None:
        """AED is absent from Frankfurter's 30-currency set.

        That is what exercises the FX_CURRENCY_UNSUPPORTED branch, which a
        wholly ECB-covered persona would never reach.
        """
        persona = load_persona("international")
        assert "AED" in {acct.currency_code for acct in persona.accounts}

    def test_international_persona_exercises_both_writer_paths(self) -> None:
        """OFX and tabular write currency to different tables and columns.

        A persona whose non-USD accounts all shared one source_type would
        leave the other path's hard-coded currency undetected.
        """
        persona = load_persona("international")
        foreign_source_types = {
            acct.source_type for acct in persona.accounts if acct.currency_code != "USD"
        }
        assert foreign_source_types == {"ofx", "csv"}

    def test_every_spending_account_is_funded(self) -> None:
        """An account that only spends drifts to a large negative balance.

        Explicit cross-currency transfers can fund their target account, but an
        account with neither income nor an inbound transfer can still drift
        thousands below zero and make the demo position unrealistic.
        """
        for persona_name in self.PERSONAS:
            persona = load_persona(persona_name)
            funded = {inc.account for inc in persona.income}
            funded |= {xfer.to_account for xfer in persona.transfers}
            spending = {
                account
                for category in persona.spending.categories
                for account in category.accounts
            }
            spending |= {rec.account for rec in persona.recurring}
            assert spending <= funded, (
                f"Persona {persona_name!r} spends from unfunded accounts: "
                f"{sorted(spending - funded)}"
            )

    def test_international_merchant_patterns_do_not_shadow_each_other(self) -> None:
        """`contains` matching means a shorter pattern swallows a longer one.

        The matcher takes the first merchant whose pattern is contained in the
        description, ordered by canonical name, so "COSTA COFFEE" would claim
        every "COSTA COFFEE AE ..." transaction and misattribute the merchant.
        """
        persona = load_persona("international")
        patterns = {
            merchant.description_prefix or merchant.name
            for category in persona.spending.categories
            for merchant in load_merchant_catalog(category.merchant_catalog).merchants
        }
        shadowed = [
            (short, long)
            for short in patterns
            for long in patterns
            if short != long and short in long
        ]
        assert shadowed == []

    def test_non_us_catalogs_declare_their_own_cities(self) -> None:
        """Otherwise a Dubai grocery run reads "CARREFOUR HYPER #4450 AUSTIN TX"."""
        persona = load_persona("international")
        foreign = {
            category.merchant_catalog
            for category in persona.spending.categories
            if category.merchant_catalog.rsplit("_", 1)[-1] in {"eu", "uk", "ca", "ae"}
        }
        assert foreign
        for catalog_name in sorted(foreign):
            catalog = load_merchant_catalog(catalog_name)
            assert catalog.cities, f"{catalog_name} has no cities of its own"

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
