"""Currency-event adaptation to the unchanged investment cost-basis engine."""

from __future__ import annotations

import dataclasses
import hashlib
import typing as t
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest

from moneybin.currency_lots import sqlmesh_loader

pytestmark = pytest.mark.unit

D = Decimal
T1 = datetime(2026, 1, 1, 10)
T2 = datetime(2026, 2, 1, 10)
T3 = datetime(2026, 3, 1, 10)
T4 = datetime(2026, 4, 1, 10)


def _conversion(**changes: object) -> sqlmesh_loader.CurrencyConversionRow:
    values: dict[str, t.Any] = {
        "conversion_id": "fxc_acquire",
        "source_shape": "linked_two_row",
        "transfer_pair_id": "decision-acquire",
        "from_transaction_id": "usd-out",
        "to_transaction_id": "eur-in",
        "from_account_id": "acct-usd",
        "to_account_id": "acct-eur",
        "from_date": date(2026, 1, 1),
        "to_date": date(2026, 1, 1),
        "from_amount": D("100.00"),
        "from_currency": "USD",
        "to_amount": D("80.00"),
        "to_currency": "EUR",
        "executed_rate": D("0.80000000"),
        "home_currency": "USD",
        "home_value": D("100.00"),
        "valuation_rate": D("1.25000000"),
        "valuation_rate_date": date(2026, 1, 1),
        "valuation_source_type": "actual",
        "from_source_type": "ofx",
        "from_source_origin": "source-a",
        "from_source_transaction_id": "native-out",
        "to_source_type": "plaid",
        "to_source_origin": "source-b",
        "to_source_transaction_id": "native-in",
        "coverage_status": "complete",
        "coverage_reason": None,
        "updated_at": T1,
    }
    values.update(changes)
    return sqlmesh_loader.CurrencyConversionRow(**values)


def _sale(**changes: object) -> sqlmesh_loader.ForeignSecuritySale:
    values: dict[str, t.Any] = {
        "investment_transaction_id": "security-sale-1",
        "account_id": "acct-eur",
        "trade_date": date(2026, 2, 1),
        "net_proceeds": D("40.00"),
        "fees": D("2.00"),
        "currency_code": "EUR",
        "home_currency": "USD",
        "home_value": D("50.00"),
        "valuation_rate": D("1.25000000"),
        "valuation_rate_date": date(2026, 2, 1),
        "valuation_source_type": "override",
        "updated_at": T2,
    }
    values.update(changes)
    return sqlmesh_loader.ForeignSecuritySale(**values)


def _derive(
    *conversions: t.Any,
    sales: tuple[t.Any, ...] = (),
    methods: Mapping[str, str | None] | None = None,
) -> t.Any:
    return sqlmesh_loader.derive_currency_accounting(
        conversions,
        sales,
        methods or {},
    )


def _public_lot_id(account_id: str, currency: str, source_event_id: str) -> str:
    digest = hashlib.sha256(
        f"{account_id}|{currency}|{source_event_id}".encode()
    ).hexdigest()[:16]
    return f"clot_{digest}"


def _public_gain_id(disposal_id: str, lot_id: str) -> str:
    digest = hashlib.sha256(f"{disposal_id}|{lot_id}".encode()).hexdigest()[:16]
    return f"rfx_{digest}"


def _mutate_frozen(instance: object, attribute: str, value: object) -> None:
    setattr(instance, attribute, value)


def test_accounting_contract_types_are_frozen_and_outputs_are_tuples() -> None:
    result = _derive()
    sale = _sale()

    assert dataclasses.is_dataclass(sqlmesh_loader.ForeignSecuritySale)
    assert dataclasses.is_dataclass(sqlmesh_loader.CurrencyAccountingResult)
    with pytest.raises(dataclasses.FrozenInstanceError):
        _mutate_frozen(sale, "account_id", "changed")
    with pytest.raises(dataclasses.FrozenInstanceError):
        _mutate_frozen(result, "lots", ())
    assert isinstance(result.lots, tuple)
    assert isinstance(result.gains, tuple)


def test_home_to_foreign_opens_lot_at_actual_home_basis() -> None:
    result = _derive(_conversion())

    assert result.gains == ()
    assert len(result.lots) == 1
    lot = result.lots[0]
    assert lot.account_id == "acct-eur"
    assert lot.currency_code == "EUR"
    assert lot.acquisition_type == "conversion"
    assert lot.original_quantity == D("80.00")
    assert lot.remaining_quantity == D("80.00")
    assert lot.cost_basis_total == D("100.00")
    assert lot.cost_basis_remaining == D("100.00")
    assert lot.cost_basis_method == "fifo"
    assert lot.home_currency == "USD"
    assert lot.source_conversion_id == "fxc_acquire"
    assert lot.source_investment_transaction_id is None
    assert lot.coverage_status == "complete"
    assert lot.updated_at == T1


def test_partial_then_full_foreign_to_home_disposals_use_fifo() -> None:
    partial = _conversion(
        conversion_id="fxc_partial",
        transfer_pair_id="decision-partial",
        from_transaction_id="eur-out-1",
        to_transaction_id="usd-in-1",
        from_account_id="acct-eur",
        to_account_id="acct-usd",
        from_date=date(2026, 2, 1),
        to_date=date(2026, 2, 1),
        from_amount=D("30.00"),
        from_currency="EUR",
        to_amount=D("45.00"),
        to_currency="USD",
        executed_rate=D("1.50000000"),
        home_value=D("45.00"),
        valuation_rate=D("1.50000000"),
        valuation_rate_date=date(2026, 2, 1),
        updated_at=T2,
    )
    full = _conversion(
        conversion_id="fxc_full",
        transfer_pair_id="decision-full",
        from_transaction_id="eur-out-2",
        to_transaction_id="usd-in-2",
        from_account_id="acct-eur",
        to_account_id="acct-usd",
        from_date=date(2026, 3, 1),
        to_date=date(2026, 3, 1),
        from_amount=D("50.00"),
        from_currency="EUR",
        to_amount=D("70.00"),
        to_currency="USD",
        executed_rate=D("1.40000000"),
        home_value=D("70.00"),
        valuation_rate=D("1.40000000"),
        valuation_rate_date=date(2026, 3, 1),
        updated_at=T3,
    )

    result = _derive(_conversion(), partial, full)

    assert len(result.lots) == 1
    assert result.lots[0].remaining_quantity == D("0.00")
    assert result.lots[0].cost_basis_remaining == D("0.00")
    assert [gain.disposed_amount for gain in result.gains] == [D("30.00"), D("50.00")]
    assert [gain.cost_basis for gain in result.gains] == [D("37.50"), D("62.50")]
    assert [gain.gain_loss for gain in result.gains] == [D("7.50"), D("7.50")]
    assert [gain.conversion_id for gain in result.gains] == [
        "fxc_partial",
        "fxc_full",
    ]
    assert all(gain.fee_amount == D("0.00") for gain in result.gains)
    assert all(gain.updated_at == T3 for gain in result.gains)


def test_multiple_rates_fifo_consumes_oldest_basis() -> None:
    later_acquisition = _conversion(
        conversion_id="fxc_acquire_2",
        transfer_pair_id="decision-acquire-2",
        from_transaction_id="usd-out-2",
        to_transaction_id="eur-in-2",
        from_date=date(2026, 1, 15),
        to_date=date(2026, 1, 15),
        from_amount=D("160.00"),
        to_amount=D("80.00"),
        executed_rate=D("0.50000000"),
        home_value=D("160.00"),
        valuation_rate=D("2.00000000"),
        valuation_rate_date=date(2026, 1, 15),
        updated_at=T2,
    )
    disposal = _conversion(
        conversion_id="fxc_dispose",
        transfer_pair_id="decision-dispose",
        from_transaction_id="eur-out",
        to_transaction_id="usd-in",
        from_account_id="acct-eur",
        to_account_id="acct-usd",
        from_date=date(2026, 2, 1),
        to_date=date(2026, 2, 1),
        from_amount=D("100.00"),
        from_currency="EUR",
        to_amount=D("150.00"),
        to_currency="USD",
        executed_rate=D("1.50000000"),
        home_value=D("150.00"),
        valuation_rate=D("1.50000000"),
        valuation_rate_date=date(2026, 2, 1),
        updated_at=T3,
    )

    result = _derive(_conversion(), later_acquisition, disposal)

    by_date = {gain.acquisition_date: gain for gain in result.gains}
    assert by_date[date(2026, 1, 1)].disposed_amount == D("80.00")
    assert by_date[date(2026, 1, 1)].cost_basis == D("100.00")
    assert by_date[date(2026, 1, 15)].disposed_amount == D("20.00")
    assert by_date[date(2026, 1, 15)].cost_basis == D("40.00")


def test_multiple_rates_average_uses_pooled_basis() -> None:
    second = _conversion(
        conversion_id="fxc_acquire_2",
        from_date=date(2026, 1, 15),
        to_date=date(2026, 1, 15),
        from_amount=D("160.00"),
        to_amount=D("80.00"),
        home_value=D("160.00"),
        updated_at=T2,
    )
    disposal = _conversion(
        conversion_id="fxc_dispose",
        from_account_id="acct-eur",
        to_account_id="acct-usd",
        from_date=date(2026, 2, 1),
        to_date=date(2026, 2, 1),
        from_amount=D("100.00"),
        from_currency="EUR",
        to_amount=D("150.00"),
        to_currency="USD",
        home_value=D("150.00"),
        updated_at=T3,
    )

    result = _derive(
        _conversion(),
        second,
        disposal,
        methods={"acct-eur": "average"},
    )

    assert sum((g.cost_basis or D("0") for g in result.gains), D("0")) == D("162.50")
    assert sum(
        (lot.cost_basis_remaining or D("0") for lot in result.lots), D("0")
    ) == D("97.50")
    assert all(row.cost_basis_method == "average" for row in result.lots)
    assert all(row.cost_basis_method == "average" for row in result.gains)


def test_foreign_to_foreign_disposes_sent_and_acquires_received_currency() -> None:
    acquire_gbp = _conversion(
        conversion_id="fxc_gbp_acquire",
        to_account_id="acct-gbp",
        to_amount=D("100.00"),
        to_currency="GBP",
    )
    gbp_to_eur = _conversion(
        conversion_id="fxc_gbp_eur",
        from_account_id="acct-gbp",
        to_account_id="acct-eur",
        from_date=date(2026, 2, 1),
        to_date=date(2026, 2, 1),
        from_amount=D("40.00"),
        from_currency="GBP",
        to_amount=D("50.00"),
        to_currency="EUR",
        home_value=D("75.00"),
        valuation_rate=D("1.50000000"),
        valuation_rate_date=date(2026, 2, 1),
        valuation_source_type="frankfurter",
        updated_at=T2,
    )

    result = _derive(acquire_gbp, gbp_to_eur)

    gbp_lot = next(lot for lot in result.lots if lot.currency_code == "GBP")
    eur_lot = next(lot for lot in result.lots if lot.currency_code == "EUR")
    assert gbp_lot.remaining_quantity == D("60.00")
    assert eur_lot.original_quantity == D("50.00")
    assert eur_lot.cost_basis_total == D("75.00")
    assert result.gains[0].proceeds == D("75.00")
    assert result.gains[0].valuation_source_type == "frankfurter"


def test_foreign_security_sale_opens_net_proceeds_without_subtracting_fees() -> None:
    result = _derive(sales=(_sale(),))

    assert result.gains == ()
    lot = result.lots[0]
    assert lot.acquisition_type == "security_sale"
    assert lot.original_quantity == D("40.00")
    assert lot.cost_basis_total == D("50.00")
    assert lot.source_conversion_id is None
    assert lot.source_investment_transaction_id == "security-sale-1"


def test_public_ids_are_deterministic_and_do_not_expose_private_currency_key() -> None:
    disposal = _conversion(
        conversion_id="fxc_dispose",
        from_account_id="acct-eur",
        to_account_id="acct-usd",
        from_date=date(2026, 2, 1),
        to_date=date(2026, 2, 1),
        from_amount=D("20.00"),
        from_currency="EUR",
        to_amount=D("30.00"),
        to_currency="USD",
        home_value=D("30.00"),
        updated_at=T2,
    )

    first = _derive(_conversion(), disposal)
    second = _derive(_conversion(), disposal)
    expected_lot = _public_lot_id("acct-eur", "EUR", "fxc_acquire:acquire")
    expected_gain = _public_gain_id("fxc_dispose", expected_lot)

    assert first == second
    assert first.lots[0].currency_lot_id == expected_lot
    assert first.gains[0].currency_lot_id == expected_lot
    assert first.gains[0].realized_fx_gain_id == expected_gain
    assert "currency:" not in repr(first)


@pytest.mark.parametrize("method", ["hifo", "specific"])
def test_unsupported_method_produces_lot_and_gain_placeholders(method: str) -> None:
    disposal = _conversion(
        conversion_id="fxc_dispose",
        from_account_id="acct-eur",
        to_account_id="acct-usd",
        from_date=date(2026, 2, 1),
        to_date=date(2026, 2, 1),
        from_amount=D("20.00"),
        from_currency="EUR",
        to_amount=D("30.00"),
        to_currency="USD",
        home_value=D("30.00"),
        updated_at=T2,
    )

    result = _derive(_conversion(), disposal, methods={"acct-eur": method})

    assert len(result.lots) == 1
    assert result.lots[0].cost_basis_total is None
    assert result.lots[0].cost_basis_remaining is None
    assert result.lots[0].coverage_reason == "unsupported_method"
    assert result.lots[0].basis_incomplete is True
    assert len(result.gains) == 1
    assert result.gains[0].cost_basis is None
    assert result.gains[0].gain_loss is None
    assert result.gains[0].coverage_reason == "unsupported_method"
    assert result.gains[0].currency_lot_id is None


def test_oversold_disposal_maps_empty_engine_lot_to_negative_inventory() -> None:
    disposal = _conversion(
        conversion_id="fxc_oversold",
        from_account_id="acct-eur",
        to_account_id="acct-usd",
        from_date=date(2026, 2, 1),
        to_date=date(2026, 2, 1),
        from_amount=D("100.00"),
        from_currency="EUR",
        to_amount=D("150.00"),
        to_currency="USD",
        home_value=D("150.00"),
        updated_at=T2,
    )

    result = _derive(_conversion(), disposal)
    repeated = _derive(_conversion(), disposal)

    complete = next(gain for gain in result.gains if gain.coverage_status == "complete")
    incomplete = next(
        gain for gain in result.gains if gain.coverage_status == "incomplete"
    )
    assert complete.disposed_amount == D("80.00")
    assert complete.cost_basis == D("100.00")
    assert incomplete.disposed_amount == D("20.00")
    assert incomplete.currency_lot_id is None
    assert incomplete.cost_basis is None
    assert incomplete.gain_loss is None
    assert incomplete.coverage_reason == "negative_inventory"
    assert incomplete.realized_fx_gain_id == repeated.gains[1].realized_fx_gain_id
    assert incomplete.realized_fx_gain_id != _public_gain_id("fxc_oversold", "")


def test_nonempty_incomplete_engine_lot_maps_to_incomplete_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine_lot = t.cast(
        t.Any,
        type(
            "Lot",
            (),
            {
                "lot_id": "engine-lot",
                "account_id": "acct-eur",
                "security_id": "currency:EUR",
                "acquisition_date": date(2026, 1, 1),
                "acquisition_type": "buy",
                "original_quantity": D("80.00"),
                "remaining_quantity": D("0.00"),
                "cost_basis_total": D("0.00"),
                "cost_basis_remaining": D("0.00"),
                "cost_basis_method": "fifo",
                "source_transaction_id": "fxc_acquire:acquire",
                "basis_incomplete": True,
            },
        )(),
    )
    engine_gain = t.cast(
        t.Any,
        type(
            "Gain",
            (),
            {
                "account_id": "acct-eur",
                "security_id": "currency:EUR",
                "disposal_txn_id": "fxc_dispose:dispose",
                "lot_id": "engine-lot",
                "quantity": D("10.00"),
                "acquisition_date": date(2026, 1, 1),
                "disposal_date": date(2026, 2, 1),
                "proceeds": D("15.00"),
                "cost_basis": D("0.00"),
                "gain_loss": D("15.00"),
                "cost_basis_method": "fifo",
                "basis_incomplete": True,
            },
        )(),
    )

    def incomplete_engine(
        *_args: object, **_kwargs: object
    ) -> tuple[list[t.Any], list[t.Any]]:
        return [engine_lot], [engine_gain]

    monkeypatch.setattr(sqlmesh_loader, "compute_lots_and_gains", incomplete_engine)
    disposal = _conversion(
        conversion_id="fxc_dispose",
        from_account_id="acct-eur",
        to_account_id="acct-usd",
        from_date=date(2026, 2, 1),
        to_date=date(2026, 2, 1),
        from_amount=D("10.00"),
        from_currency="EUR",
        to_amount=D("15.00"),
        to_currency="USD",
        home_value=D("15.00"),
    )

    result = _derive(_conversion(), disposal)

    assert result.lots[0].coverage_reason == "incomplete_history"
    assert result.lots[0].cost_basis_total is None
    assert result.gains[0].coverage_reason == "incomplete_history"
    assert result.gains[0].cost_basis is None
    assert result.gains[0].gain_loss is None


def test_incomplete_bridge_row_never_enters_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def recording_engine(
        *_args: object, **_kwargs: object
    ) -> tuple[list[object], list[object]]:
        nonlocal called
        called = True
        return [], []

    monkeypatch.setattr(sqlmesh_loader, "compute_lots_and_gains", recording_engine)

    result = _derive(
        _conversion(
            coverage_status="incomplete",
            coverage_reason="missing_valuation_rate",
            home_value=None,
        )
    )

    assert called is False
    assert result.lots == ()
    assert result.gains == ()


def test_missing_sale_home_valuation_is_visible_without_fabricated_basis() -> None:
    result = _derive(
        sales=(
            _sale(
                home_value=None,
                valuation_rate=None,
                valuation_rate_date=None,
                valuation_source_type=None,
            ),
        )
    )

    assert len(result.lots) == 1
    assert result.lots[0].cost_basis_total is None
    assert result.lots[0].cost_basis_remaining is None
    assert result.lots[0].coverage_status == "incomplete"
    assert result.lots[0].coverage_reason == "missing_valuation_rate"


@pytest.mark.parametrize(
    ("currency", "home", "reason"),
    [("", "USD", "unknown_currency"), ("EUR", "", "missing_home_currency")],
)
def test_incomplete_security_sale_currency_context_is_visible(
    currency: str, home: str, reason: str
) -> None:
    result = _derive(sales=(_sale(currency_code=currency, home_currency=home),))

    assert result.lots[0].coverage_reason == reason
    assert result.lots[0].cost_basis_total is None


class _FakeContext:
    def __init__(self, *frames: pd.DataFrame) -> None:
        self.frames = list(frames)
        self.queries: list[str] = []
        self.resolved_tables: list[str] = []

    def resolve_table(self, name: str) -> str:
        self.resolved_tables.append(name)
        return name

    def fetchdf(self, sql: str) -> pd.DataFrame:
        self.queries.append(sql)
        return self.frames.pop(0)


def _empty_conversion_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=t.cast(
            t.Any,
            [
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
            ],
        )
    )


def test_load_currency_accounting_values_foreign_sale_from_exact_cached_rate() -> None:
    context = _FakeContext(
        pd.DataFrame(),
        pd.DataFrame({"home_currency": ["USD"], "profile_updated_at": [str(T1)]}),
        pd.DataFrame(
            columns=t.cast(
                t.Any,
                [
                    "from_currency",
                    "to_currency",
                    "rate_date",
                    "rate",
                    "rate_updated_at",
                ],
            )
        ),
        pd.DataFrame({
            "from_currency": ["EUR"],
            "to_currency": ["USD"],
            "rate_date": ["2026-02-02"],
            "rate": ["1.25000000"],
            "source_type": ["frankfurter"],
            "loaded_at": [str(T2)],
        }),
        pd.DataFrame({
            "investment_transaction_id": ["security-sale-1"],
            "account_id": ["acct-eur"],
            "trade_date": ["2026-02-02"],
            "net_proceeds": ["40.00"],
            "fees": ["2.00"],
            "currency_code": ["EUR"],
            "event_updated_at": [str(T1)],
        }),
        pd.DataFrame({
            "account_id": ["acct-eur"],
            "default_cost_basis_method": [None],
            "method_updated_at": [str(T4)],
        }),
    )

    result = sqlmesh_loader.load_currency_accounting(t.cast(t.Any, context))

    assert result.lots[0].original_quantity == D("40.00")
    assert result.lots[0].cost_basis_total == D("50.00")
    assert result.lots[0].updated_at == T4
    query_text = "\n".join(context.queries).lower()
    assert "type = 'sell'" in query_text
    assert "amount > 0" in query_text
    assert "http" not in query_text


class _RoutingContext:
    def __init__(self, conversion_frame: pd.DataFrame) -> None:
        self.conversion_frame = conversion_frame
        self.queries: list[str] = []
        self.resolved_tables: list[str] = []

    def resolve_table(self, name: str) -> str:
        self.resolved_tables.append(name)
        return name

    def fetchdf(self, sql: str) -> pd.DataFrame:
        self.queries.append(sql)
        normalized = " ".join(sql.lower().split())
        if "from core.bridge_currency_conversions" in normalized:
            return self.conversion_frame
        if "from core.bridge_transfers" in normalized:
            return _empty_conversion_frame()
        if "from app.match_decisions" in normalized:
            return _empty_conversion_frame()
        if "from prep.int_transactions__merged" in normalized:
            return _empty_conversion_frame()
        if "from app.profile_settings" in normalized:
            return pd.DataFrame({
                "home_currency": ["USD"],
                "profile_updated_at": [str(T1)],
            })
        if "from app.exchange_rate_overrides" in normalized:
            return pd.DataFrame(
                columns=t.cast(
                    t.Any,
                    [
                        "from_currency",
                        "to_currency",
                        "rate_date",
                        "rate",
                        "rate_updated_at",
                    ],
                )
            )
        if "from raw.exchange_rates" in normalized:
            return pd.DataFrame(
                columns=t.cast(
                    t.Any,
                    [
                        "from_currency",
                        "to_currency",
                        "rate_date",
                        "rate",
                        "source_type",
                        "loaded_at",
                    ],
                )
            )
        if "from core.fct_investment_transactions" in normalized:
            return pd.DataFrame(
                columns=t.cast(
                    t.Any,
                    [
                        "investment_transaction_id",
                        "account_id",
                        "trade_date",
                        "net_proceeds",
                        "fees",
                        "currency_code",
                        "event_updated_at",
                    ],
                )
            )
        if "from app.account_settings" in normalized:
            return pd.DataFrame(
                columns=t.cast(
                    t.Any,
                    [
                        "account_id",
                        "default_cost_basis_method",
                        "method_updated_at",
                    ],
                )
            )
        raise AssertionError(f"unexpected query: {sql}")


def test_load_currency_accounting_reads_typed_materialized_conversion_rows() -> None:
    conversion = dataclasses.asdict(_conversion())
    conversion["from_date"] = str(conversion["from_date"])
    conversion["to_date"] = str(conversion["to_date"])
    conversion["from_amount"] = str(conversion["from_amount"])
    conversion["to_amount"] = str(conversion["to_amount"])
    conversion["executed_rate"] = str(conversion["executed_rate"])
    conversion["home_value"] = str(conversion["home_value"])
    conversion["valuation_rate"] = str(conversion["valuation_rate"])
    conversion["valuation_rate_date"] = str(conversion["valuation_rate_date"])
    conversion["conversion_updated_at"] = str(conversion.pop("updated_at"))
    context = _RoutingContext(pd.DataFrame([conversion]))

    result = sqlmesh_loader.load_currency_accounting(t.cast(t.Any, context))

    assert len(result.lots) == 1
    assert result.lots[0].source_conversion_id == "fxc_acquire"
    assert context.resolved_tables[0] == "core.bridge_currency_conversions"
    conversion_query = context.queries[0].lower()
    assert "from_amount::varchar" in conversion_query
    assert "updated_at::varchar as conversion_updated_at" in conversion_query
