"""Trusted Currency conversion loading from exact SQLMesh input frames."""

from __future__ import annotations

import importlib
import typing as t
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest

from moneybin.database import Database

pytestmark = pytest.mark.unit

_CANDIDATE_COLUMNS = (
    "source_shape",
    "transfer_pair_id",
    "from_transaction_id",
    "to_transaction_id",
    "from_account_id",
    "to_account_id",
    "from_date",
    "to_date",
    "from_amount",
    "from_currency",
    "to_amount",
    "to_currency",
    "from_source_type",
    "from_source_origin",
    "from_source_transaction_id",
    "to_source_type",
    "to_source_origin",
    "to_source_transaction_id",
    "candidate_updated_at",
)


class _FakeContext:
    """Minimal ExecutionContext whose exact frames are returned in query order."""

    def __init__(self, *frames: pd.DataFrame) -> None:
        self._frames = list(frames)
        self.queries: list[str] = []

    def resolve_table(self, name: str) -> str:
        return name

    def fetchdf(self, sql: str) -> pd.DataFrame:
        self.queries.append(sql)
        return self._frames.pop(0)


class _DatabaseContext:
    """ExecutionContext subset that runs loader SQL against the test database."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def resolve_table(self, name: str) -> str:
        return name

    def fetchdf(self, sql: str) -> pd.DataFrame:
        return self.db.execute(sql).fetchdf()


def _frame(*rows: Mapping[str, object]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=t.cast(t.Any, _CANDIDATE_COLUMNS))


def _linked(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "source_shape": "linked_two_row",
        "transfer_pair_id": "decision-1",
        "from_transaction_id": "txn-out",
        "to_transaction_id": "txn-in",
        "from_account_id": "acct-usd",
        "to_account_id": "acct-eur",
        "from_date": "2026-03-16",
        "to_date": "2026-03-16",
        "from_amount": "-100.00",
        "from_currency": "USD",
        "to_amount": "90.00",
        "to_currency": "EUR",
        "from_source_type": "ofx",
        "from_source_origin": "source-a",
        "from_source_transaction_id": "native-out",
        "to_source_type": "plaid",
        "to_source_origin": "source-b",
        "to_source_transaction_id": "native-in",
        "candidate_updated_at": "2026-03-16 14:00:00",
    }
    row.update(changes)
    return row


def _single(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "source_shape": "single_row",
        "transfer_pair_id": None,
        "from_transaction_id": "txn-single",
        "to_transaction_id": None,
        "from_account_id": "acct-eur",
        "to_account_id": "acct-eur",
        "from_date": "2026-03-16",
        "to_date": "2026-03-16",
        "from_amount": "-80.00",
        "from_currency": "EUR",
        "to_amount": "100.00",
        "to_currency": "USD",
        "from_source_type": "ofx",
        "from_source_origin": "source-a",
        "from_source_transaction_id": "native-single",
        "to_source_type": "ofx",
        "to_source_origin": "source-a",
        "to_source_transaction_id": "native-single",
        "candidate_updated_at": "2026-03-16 14:00:00",
    }
    row.update(changes)
    return row


def _missing() -> dict[str, object]:
    return _linked(
        from_transaction_id=None,
        to_transaction_id=None,
        from_account_id=None,
        to_account_id=None,
        from_date=None,
        to_date=None,
        from_amount=None,
        from_currency=None,
        to_amount=None,
        to_currency=None,
    )


def _profile(currency: str | None = "USD") -> pd.DataFrame:
    return pd.DataFrame({
        "home_currency": [currency],
        "profile_updated_at": ["2026-01-01 00:00:00"],
    })


def _overrides(*rows: Mapping[str, object]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=t.cast(
            t.Any,
            (
                "from_currency",
                "to_currency",
                "rate_date",
                "rate",
                "rate_updated_at",
            ),
        ),
    )


def _provider_rates(*rows: Mapping[str, object]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=t.cast(
            t.Any,
            (
                "from_currency",
                "to_currency",
                "rate_date",
                "rate",
                "source_type",
                "loaded_at",
            ),
        ),
    )


def _rate_watermarks(*rows: Mapping[str, object]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=t.cast(t.Any, ("target_id", "mutation_updated_at")),
    )


def _context(
    *,
    linked: pd.DataFrame | None = None,
    missing: pd.DataFrame | None = None,
    single: pd.DataFrame | None = None,
    home_currency: str | None = "USD",
    overrides: pd.DataFrame | None = None,
    provider_rates: pd.DataFrame | None = None,
    rate_watermarks: pd.DataFrame | None = None,
) -> _FakeContext:
    return _FakeContext(
        linked if linked is not None else _frame(),
        missing if missing is not None else _frame(),
        single if single is not None else _frame(),
        _profile(home_currency),
        pd.DataFrame(columns=t.cast(t.Any, ("target_id", "mutation_updated_at"))),
        overrides if overrides is not None else _overrides(),
        provider_rates if provider_rates is not None else _provider_rates(),
        rate_watermarks if rate_watermarks is not None else _rate_watermarks(),
    )


def _load(context: _FakeContext) -> list[t.Any]:
    module = importlib.import_module("moneybin.currency_lots.sqlmesh_loader")
    return module.load_conversion_rows(t.cast(t.Any, context))


def test_sent_currency_comes_from_canonical_account_for_single_row_shape(
    db: Database,
) -> None:
    """Merged terms stay authoritative while Account Currency completes rows."""
    db.execute(
        """
        CREATE OR REPLACE TABLE core.bridge_transfers (
            transfer_id VARCHAR,
            debit_transaction_id VARCHAR,
            credit_transaction_id VARCHAR
        )
        """
    )
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        """
        CREATE OR REPLACE TABLE prep.int_transactions__merged (
            transaction_id VARCHAR,
            account_id VARCHAR,
            transaction_date DATE,
            amount DECIMAL(18, 2),
            currency_code VARCHAR,
            conversion_from_date DATE,
            conversion_from_amount DECIMAL(18, 2),
            conversion_from_currency VARCHAR,
            to_amount DECIMAL(18, 2),
            to_currency VARCHAR,
            conversion_source_type VARCHAR,
            conversion_source_origin VARCHAR,
            conversion_source_transaction_id VARCHAR,
            loaded_at TIMESTAMP
        )
        """
    )
    db.execute(
        """
        CREATE OR REPLACE TABLE core.fct_transactions (
            transaction_id VARCHAR,
            currency_code VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    db.execute(
        """
        INSERT INTO core.bridge_transfers VALUES
            ('decision-linked', 'txn-linked-out', 'txn-linked-in')
        """
    )
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, account_id_b, match_type, match_status, decided_by,
            decided_at
        ) VALUES (
            'decision-linked', 'native-linked-out', 'manual', 'user',
            'native-linked-in', 'manual', 'user', 'acct-usd', 'acct-eur',
            'transfer', 'accepted', 'user',
            '2026-03-16 13:00:00'::TIMESTAMP
        )
        """
    )
    db.execute(
        """
        INSERT INTO prep.int_transactions__merged VALUES
            ('txn-linked-out', 'acct-usd', '2026-03-16'::DATE, -100.00,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
             '2026-03-16 10:00:00'::TIMESTAMP),
            ('txn-linked-in', 'acct-eur', '2026-03-16'::DATE, 90.00,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
             '2026-03-16 11:00:00'::TIMESTAMP),
            ('txn-single', 'acct-eur', '2026-03-17'::DATE, -80.00,
             NULL, '2026-03-17'::DATE, -80.00, NULL, 100.00, 'USD',
             'manual', 'user', 'native-single',
             '2026-03-17 12:00:00'::TIMESTAMP)
        """
    )
    db.execute(
        """
        CREATE OR REPLACE TABLE core.dim_accounts (
            account_id VARCHAR,
            currency_code VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    db.execute(
        """
        INSERT INTO core.dim_accounts VALUES
            ('acct-eur', 'EUR', '2026-03-19 09:00:00'::TIMESTAMP)
        """
    )
    db.execute(
        """
        INSERT INTO core.fct_transactions VALUES
            ('txn-linked-out', 'USD', '2026-03-18 09:00:00'::TIMESTAMP),
            ('txn-linked-in', 'EUR', '2026-03-18 10:00:00'::TIMESTAMP),
            ('txn-single', 'EUR', '2026-03-19 09:00:00'::TIMESTAMP)
        """
    )
    db.execute(
        """
        INSERT INTO app.profile_settings (home_currency, updated_at)
        VALUES ('USD', '2026-03-01 09:00:00'::TIMESTAMP)
        """
    )

    module = importlib.import_module("moneybin.currency_lots.sqlmesh_loader")
    rows = module.load_conversion_rows(t.cast(t.Any, _DatabaseContext(db)))
    by_shape = {row.source_shape: row for row in rows}

    linked = by_shape["linked_two_row"]
    assert (linked.from_currency, linked.to_currency) == ("USD", "EUR")
    assert (linked.from_amount, linked.to_amount) == (
        Decimal("100.00"),
        Decimal("90.00"),
    )
    assert linked.from_source_transaction_id == "native-linked-out"
    assert linked.coverage_status == "complete"
    assert str(linked.updated_at) == "2026-03-18 10:00:00"

    single = by_shape["single_row"]
    assert (single.from_currency, single.to_currency) == ("EUR", "USD")
    assert (single.from_amount, single.to_amount) == (
        Decimal("80.00"),
        Decimal("100.00"),
    )
    assert single.from_source_transaction_id == "native-single"
    assert single.coverage_status == "complete"
    assert str(single.updated_at) == "2026-03-19 09:00:00"

    db.execute(
        """
        UPDATE core.dim_accounts
           SET currency_code = NULL,
               updated_at = '2026-03-20 09:00:00'::TIMESTAMP
         WHERE account_id = 'acct-eur'
        """
    )

    cleared = {
        row.source_shape: row
        for row in module.load_conversion_rows(t.cast(t.Any, _DatabaseContext(db)))
    }["single_row"]
    assert cleared.from_currency is None
    assert cleared.coverage_reason == "unknown_currency"
    assert str(cleared.updated_at) == "2026-03-20 09:00:00"

    db.execute(
        """
        UPDATE core.fct_transactions
           SET currency_code = NULL,
               updated_at = '2026-03-21 09:00:00'::TIMESTAMP
         WHERE transaction_id = 'txn-linked-in'
        """
    )

    cleared_linked = {
        row.source_shape: row
        for row in module.load_conversion_rows(t.cast(t.Any, _DatabaseContext(db)))
    }["linked_two_row"]
    assert cleared_linked.to_currency is None
    assert cleared_linked.coverage_reason == "unknown_currency"
    assert str(cleared_linked.updated_at) == "2026-03-21 09:00:00"

    db.execute(
        """
        INSERT INTO app.audit_log (
            audit_id, occurred_at, actor, action, target_schema, target_table,
            target_id, operation_id
        ) VALUES (
            'audit-decision-linked', '2026-03-22 09:00:00'::TIMESTAMP,
            'system', 'match.restore', 'app', 'match_decisions',
            'decision-linked', 'op-decision-linked'
        )
        """
    )

    restored_linked = {
        row.source_shape: row
        for row in module.load_conversion_rows(t.cast(t.Any, _DatabaseContext(db)))
    }["linked_two_row"]
    assert str(restored_linked.updated_at) == "2026-03-22 09:00:00"

    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, account_id_b, match_type, match_status, decided_by,
            decided_at
        ) VALUES (
            'decision-missing', 'native-missing-out', 'manual', 'user',
            'native-missing-in', 'manual', 'user', 'acct-usd', 'acct-eur',
            'transfer', 'accepted', 'user',
            '2026-03-16 14:00:00'::TIMESTAMP
        )
        """
    )
    db.execute(
        """
        INSERT INTO app.audit_log (
            audit_id, occurred_at, actor, action, target_schema, target_table,
            target_id, operation_id
        ) VALUES (
            'audit-decision-missing', '2026-03-23 09:00:00'::TIMESTAMP,
            'system', 'match.restore', 'app', 'match_decisions',
            'decision-missing', 'op-decision-missing'
        )
        """
    )

    missing = next(
        row
        for row in module.load_conversion_rows(t.cast(t.Any, _DatabaseContext(db)))
        if row.transfer_pair_id == "decision-missing"
    )
    assert missing.coverage_reason == "missing_leg"
    assert str(missing.updated_at) == "2026-03-23 09:00:00"

    db.execute(
        """
        UPDATE prep.int_transactions__merged
           SET conversion_from_date = transaction_date,
               conversion_from_amount = amount,
               conversion_from_currency = 'USD',
               to_amount = 95.00,
               to_currency = 'EUR',
               conversion_source_type = 'manual',
               conversion_source_origin = 'user',
               conversion_source_transaction_id = 'native-linked-out'
         WHERE transaction_id = 'txn-linked-out'
        """
    )

    overlapping = [
        row
        for row in module.load_conversion_rows(t.cast(t.Any, _DatabaseContext(db)))
        if row.from_transaction_id == "txn-linked-out"
    ]
    assert len(overlapping) == 1
    assert overlapping[0].source_shape == "linked_two_row"


def test_missing_home_currency_uses_profile_audit_freshness() -> None:
    module = importlib.import_module("moneybin.currency_lots.sqlmesh_loader")
    context = _FakeContext(
        pd.DataFrame(columns=t.cast(t.Any, ["home_currency", "profile_updated_at"])),
        pd.DataFrame({
            "target_id": ["profile"],
            "mutation_updated_at": ["2026-03-22 09:00:00"],
        }),
    )

    home_currency, updated_at = module._load_home_currency(  # pyright: ignore[reportPrivateUsage]
        t.cast(t.Any, context)
    )

    assert home_currency is None
    assert updated_at == datetime(2026, 3, 22, 9)


@pytest.mark.parametrize(
    ("from_amount", "from_currency", "to_amount", "to_currency", "rate"),
    [
        ("-100.00", "USD", "90.00", "EUR", Decimal("0.90000000")),
        ("-90.00", "EUR", "100.00", "USD", Decimal("1.11111111")),
    ],
)
def test_accepted_two_row_conversion_preserves_orientation_and_actual_terms(
    from_amount: str,
    from_currency: str,
    to_amount: str,
    to_currency: str,
    rate: Decimal,
) -> None:
    rows = _load(
        _context(
            linked=_frame(
                _linked(
                    from_amount=from_amount,
                    from_currency=from_currency,
                    to_amount=to_amount,
                    to_currency=to_currency,
                )
            )
        )
    )

    assert len(rows) == 1
    row = rows[0]
    assert (row.from_currency, row.to_currency) == (from_currency, to_currency)
    assert row.from_amount == abs(Decimal(from_amount))
    assert row.to_amount == abs(Decimal(to_amount))
    assert row.executed_rate == rate
    assert row.home_value == Decimal("100.00")
    assert row.valuation_source_type == "actual"
    assert row.coverage_status == "complete"


def test_home_to_foreign_actual_valuation_uses_sent_leg_date() -> None:
    """Actual Home value is dated by the Home leg that supplies it."""
    row = _load(
        _context(linked=_frame(_linked(from_date="2026-03-15", to_date="2026-03-16")))
    )[0]

    assert row.valuation_source_type == "actual"
    assert row.valuation_rate_date == date(2026, 3, 15)


def test_source_provided_single_row_keeps_one_evidence_identity() -> None:
    row = _load(_context(single=_frame(_single())))[0]

    assert row.source_shape == "single_row"
    assert row.transfer_pair_id is None
    assert row.from_transaction_id == "txn-single"
    assert row.to_transaction_id is None
    assert row.to_account_id == "acct-eur"
    assert row.from_source_transaction_id == "native-single"
    assert row.to_source_transaction_id == "native-single"


def test_conversion_id_ignores_corrected_amounts_and_dates() -> None:
    original = _load(_context(linked=_frame(_linked())))[0]
    corrected = _load(
        _context(
            linked=_frame(
                _linked(
                    from_date="2026-03-17",
                    to_date="2026-03-17",
                    from_amount="-110.00",
                    to_amount="99.00",
                )
            )
        )
    )[0]

    assert original.conversion_id == corrected.conversion_id


def test_linked_conversion_id_uses_transfer_evidence_identity() -> None:
    row = _load(_context(linked=_frame(_linked())))[0]

    assert row.conversion_id == "fxc_18b6a46c9f164284"


def test_single_row_conversion_id_uses_transaction_evidence_identity() -> None:
    row = _load(_context(single=_frame(_single())))[0]

    assert row.conversion_id == "fxc_2bbdacbd8d0310a6"


def test_only_accepted_unreversed_transfer_decisions_are_queried() -> None:
    context = _context()

    assert _load(context) == []
    decision_queries = "\n".join(context.queries[:2]).lower()
    assert "match_status = 'accepted'" in decision_queries
    assert "reversed_at is null" in decision_queries
    assert "match_type = 'transfer'" in decision_queries
    assert "match_proposals" not in "\n".join(context.queries).lower()


def test_same_currency_transfer_is_not_a_currency_conversion() -> None:
    assert (
        _load(_context(linked=_frame(_linked(from_currency="USD", to_currency="USD"))))
        == []
    )


def test_positive_sent_amount_is_incomplete_shape() -> None:
    row = _load(_context(linked=_frame(_linked(from_amount="100.00"))))[0]

    assert row.coverage_status == "incomplete"
    assert row.coverage_reason == "incomplete_shape"
    assert row.executed_rate is None


@pytest.mark.parametrize("to_amount", ["0.00", "-90.00"])
def test_nonpositive_received_amount_is_incomplete_shape(to_amount: str) -> None:
    row = _load(_context(linked=_frame(_linked(to_amount=to_amount))))[0]

    assert row.coverage_status == "incomplete"
    assert row.coverage_reason == "incomplete_shape"
    assert row.executed_rate is None


def test_reversed_transfer_terms_are_incomplete_shape() -> None:
    row = _load(
        _context(linked=_frame(_linked(from_amount="100.00", to_amount="-90.00")))
    )[0]

    assert row.coverage_status == "incomplete"
    assert row.coverage_reason == "incomplete_shape"
    assert row.executed_rate is None


def test_invalid_same_currency_transfer_stays_inspectable() -> None:
    rows = _load(
        _context(
            linked=_frame(
                _linked(
                    from_amount="100.00",
                    from_currency="USD",
                    to_currency="USD",
                )
            )
        )
    )

    assert len(rows) == 1
    assert rows[0].coverage_status == "incomplete"
    assert rows[0].coverage_reason == "incomplete_shape"


def test_accepted_decision_missing_from_transfer_bridge_stays_inspectable() -> None:
    row = _load(_context(missing=_frame(_missing())))[0]

    assert row.transfer_pair_id == "decision-1"
    assert row.coverage_status == "incomplete"
    assert row.coverage_reason == "missing_leg"
    assert row.executed_rate is None


@pytest.mark.parametrize("missing_field", ["to_amount", "to_currency"])
def test_single_row_with_one_received_field_is_incomplete_shape(
    missing_field: str,
) -> None:
    row = _load(_context(single=_frame(_single(**{missing_field: None}))))[0]

    assert row.coverage_status == "incomplete"
    assert row.coverage_reason == "incomplete_shape"


def test_unknown_currency_precedes_missing_home_currency() -> None:
    row = _load(
        _context(
            linked=_frame(_linked(to_currency=None)),
            home_currency=None,
        )
    )[0]

    assert row.coverage_reason == "unknown_currency"


def test_missing_home_currency_is_visible() -> None:
    row = _load(_context(linked=_frame(_linked()), home_currency=None))[0]

    assert row.coverage_reason == "missing_home_currency"


def test_exact_date_override_outranks_provider_cache() -> None:
    override = {
        "from_currency": "GBP",
        "to_currency": "USD",
        "rate_date": "2026-03-16",
        "rate": "1.25000000",
        "rate_updated_at": "2026-03-16 16:00:00",
    }
    provider = {
        "from_currency": "GBP",
        "to_currency": "USD",
        "rate_date": "2026-03-16",
        "rate": "1.20000000",
        "source_type": "frankfurter",
        "loaded_at": "2026-03-16 15:00:00",
    }

    row = _load(
        _context(
            single=_frame(
                _single(
                    from_currency="EUR",
                    to_currency="GBP",
                    to_amount="100.00",
                )
            ),
            overrides=_overrides(override),
            provider_rates=_provider_rates(provider),
        )
    )[0]

    assert row.home_value == Decimal("125.00")
    assert row.valuation_rate == Decimal("1.25000000")
    assert row.valuation_rate_date == date(2026, 3, 16)
    assert row.valuation_source_type == "override"


def test_weekend_uses_only_the_certain_friday_cache_row() -> None:
    friday_rate = {
        "from_currency": "GBP",
        "to_currency": "USD",
        "rate_date": "2026-03-13",
        "rate": "1.20000000",
        "source_type": "frankfurter",
        "loaded_at": "2026-03-13 15:00:00",
    }
    row = _load(
        _context(
            single=_frame(
                _single(
                    from_date="2026-03-15",
                    to_date="2026-03-15",
                    from_currency="EUR",
                    to_currency="GBP",
                    to_amount="100.00",
                )
            ),
            provider_rates=_provider_rates(friday_rate),
        )
    )[0]

    assert row.home_value == Decimal("120.00")
    assert row.valuation_rate_date == date(2026, 3, 13)
    assert row.valuation_source_type == "frankfurter"


def test_missing_weekday_rate_never_substitutes_an_earlier_day() -> None:
    monday_rate = {
        "from_currency": "GBP",
        "to_currency": "USD",
        "rate_date": "2026-03-16",
        "rate": "1.20000000",
        "source_type": "frankfurter",
        "loaded_at": "2026-03-16 15:00:00",
    }
    row = _load(
        _context(
            single=_frame(
                _single(
                    from_date="2026-03-17",
                    to_date="2026-03-17",
                    from_currency="EUR",
                    to_currency="GBP",
                )
            ),
            provider_rates=_provider_rates(monday_rate),
        )
    )[0]

    assert row.coverage_reason == "missing_valuation_rate"
    assert row.home_value is None
    assert row.valuation_rate is None


def test_foreign_to_foreign_uses_received_currency_rate_and_its_provenance() -> None:
    received_rate = {
        "from_currency": "GBP",
        "to_currency": "USD",
        "rate_date": "2026-03-16",
        "rate": "1.234567895",
        "source_type": "provider-b",
        "loaded_at": "2026-03-16 15:00:00",
    }
    wrong_sent_rate = {
        "from_currency": "EUR",
        "to_currency": "USD",
        "rate_date": "2026-03-16",
        "rate": "9.00000000",
        "source_type": "provider-a",
        "loaded_at": "2026-03-16 15:00:00",
    }
    row = _load(
        _context(
            single=_frame(
                _single(
                    from_currency="EUR",
                    to_currency="GBP",
                    to_amount="100.00",
                )
            ),
            provider_rates=_provider_rates(received_rate, wrong_sent_rate),
        )
    )[0]

    assert row.home_value == Decimal("123.46")
    assert row.valuation_rate == Decimal("1.23456790")
    assert row.valuation_source_type == "provider-b"


def test_newest_provider_row_then_source_type_is_deterministic() -> None:
    rows = [
        {
            "from_currency": "GBP",
            "to_currency": "USD",
            "rate_date": "2026-03-16",
            "rate": "1.10000000",
            "source_type": "old-provider",
            "loaded_at": "2026-03-16 14:00:00",
        },
        {
            "from_currency": "GBP",
            "to_currency": "USD",
            "rate_date": "2026-03-16",
            "rate": "1.30000000",
            "source_type": "z-provider",
            "loaded_at": "2026-03-16 15:00:00",
        },
        {
            "from_currency": "GBP",
            "to_currency": "USD",
            "rate_date": "2026-03-16",
            "rate": "1.20000000",
            "source_type": "a-provider",
            "loaded_at": "2026-03-16 15:00:00",
        },
    ]
    row = _load(
        _context(
            single=_frame(
                _single(from_currency="EUR", to_currency="GBP", to_amount="10.00")
            ),
            provider_rates=_provider_rates(*rows),
        )
    )[0]

    assert row.home_value == Decimal("12.00")
    assert row.valuation_source_type == "a-provider"


def test_amount_and_date_proximity_without_accepted_evidence_produces_no_row() -> None:
    assert _load(_context()) == []
