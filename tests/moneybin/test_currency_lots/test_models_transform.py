"""Integration coverage for the three M1K.3 SQLMesh Core models."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.metrics import registry as metrics_registry
from moneybin.services.currency_service import CurrencyService
from moneybin.services.transform_service import TransformService
from moneybin.services.undo_service import UndoService

pytestmark = pytest.mark.integration

_REASONS = (
    "complete",
    "incomplete_shape",
    "missing_leg",
    "unknown_currency",
    "missing_home_currency",
    "missing_valuation_rate",
    "negative_inventory",
    "incomplete_history",
    "unsupported_method",
)

_SCHEMAS = {
    "bridge_currency_conversions": [
        ("conversion_id", "VARCHAR"),
        ("transfer_pair_id", "VARCHAR"),
        ("from_transaction_id", "VARCHAR"),
        ("to_transaction_id", "VARCHAR"),
        ("from_account_id", "VARCHAR"),
        ("to_account_id", "VARCHAR"),
        ("from_source_transaction_id", "VARCHAR"),
        ("to_source_transaction_id", "VARCHAR"),
        ("source_shape", "VARCHAR"),
        ("from_currency", "VARCHAR"),
        ("to_currency", "VARCHAR"),
        ("home_currency", "VARCHAR"),
        ("valuation_source_type", "VARCHAR"),
        ("from_source_type", "VARCHAR"),
        ("from_source_origin", "VARCHAR"),
        ("to_source_type", "VARCHAR"),
        ("to_source_origin", "VARCHAR"),
        ("coverage_status", "VARCHAR"),
        ("coverage_reason", "VARCHAR"),
        ("from_amount", "DECIMAL(18,2)"),
        ("to_amount", "DECIMAL(18,2)"),
        ("executed_rate", "DECIMAL(18,8)"),
        ("home_value", "DECIMAL(18,2)"),
        ("valuation_rate", "DECIMAL(18,8)"),
        ("from_date", "DATE"),
        ("to_date", "DATE"),
        ("valuation_rate_date", "DATE"),
        ("updated_at", "TIMESTAMP"),
    ],
    "fct_currency_lots": [
        ("currency_lot_id", "VARCHAR"),
        ("account_id", "VARCHAR"),
        ("source_conversion_id", "VARCHAR"),
        ("source_investment_transaction_id", "VARCHAR"),
        ("source_transfer_id", "VARCHAR"),
        ("currency_code", "VARCHAR"),
        ("acquisition_type", "VARCHAR"),
        ("cost_basis_method", "VARCHAR"),
        ("home_currency", "VARCHAR"),
        ("coverage_status", "VARCHAR"),
        ("coverage_reason", "VARCHAR"),
        ("original_quantity", "DECIMAL(18,2)"),
        ("remaining_quantity", "DECIMAL(18,2)"),
        ("cost_basis_total", "DECIMAL(18,2)"),
        ("cost_basis_remaining", "DECIMAL(18,2)"),
        ("basis_incomplete", "BOOLEAN"),
        ("acquisition_date", "DATE"),
        ("updated_at", "TIMESTAMP"),
    ],
    "fct_realized_fx_gains": [
        ("realized_fx_gain_id", "VARCHAR"),
        ("account_id", "VARCHAR"),
        ("conversion_id", "VARCHAR"),
        ("currency_lot_id", "VARCHAR"),
        ("currency_code", "VARCHAR"),
        ("home_currency", "VARCHAR"),
        ("cost_basis_method", "VARCHAR"),
        ("valuation_source_type", "VARCHAR"),
        ("coverage_status", "VARCHAR"),
        ("coverage_reason", "VARCHAR"),
        ("disposed_amount", "DECIMAL(18,2)"),
        ("proceeds", "DECIMAL(18,2)"),
        ("cost_basis", "DECIMAL(18,2)"),
        ("gain_loss", "DECIMAL(18,2)"),
        ("fee_amount", "DECIMAL(18,2)"),
        ("valuation_rate", "DECIMAL(18,8)"),
        ("acquisition_date", "DATE"),
        ("disposal_date", "DATE"),
        ("valuation_rate_date", "DATE"),
        ("updated_at", "TIMESTAMP"),
    ],
}


def _metric(grain: str, reason: str) -> float:
    return metrics_registry.FX_ACCOUNTING_ROWS.labels(
        grain=grain, coverage_reason=reason
    )._value.get()  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals


def _catalog_schema(db: Database, table: str) -> list[tuple[str, str]]:
    return db.execute(
        """
        SELECT column_name, data_type
        FROM duckdb_columns()
        WHERE schema_name = 'core' AND table_name = ?
        ORDER BY column_index
        """,
        [table],
    ).fetchall()


def _insert_conversion(
    db: Database,
    *,
    source_id: str,
    account_id: str,
    txn_date: str,
    amount: str,
    currency: str,
    created_at: str,
    to_amount: str | None = None,
    to_currency: str | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO raw.manual_transactions (
            source_transaction_id, import_id, account_id, transaction_date,
            amount, to_amount, description, currency_code, to_currency,
            created_at, created_by
        ) VALUES (?, 'import-fx', ?, ?::DATE, ?::DECIMAL(18,2),
                  ?::DECIMAL(18,2), 'FX fixture', ?, ?, ?::TIMESTAMP, 'cli')
        """,
        [
            source_id,
            account_id,
            txn_date,
            amount,
            to_amount,
            currency,
            to_currency,
            created_at,
        ],
    )


def _seed_real_transform_inputs(db: Database) -> None:
    db.execute(
        """
        INSERT INTO app.profile_settings (home_currency, updated_at)
        VALUES ('USD', '2025-12-01 09:00:00'::TIMESTAMP)
        """
    )
    db.execute(
        """
        INSERT INTO app.account_settings (
            account_id, default_cost_basis_method, updated_at
        ) VALUES (
            'acct-gbp-average', 'average', '2026-01-01 08:00:00'::TIMESTAMP
        )
        """
    )

    _insert_conversion(
        db,
        source_id="linked-usd-out",
        account_id="acct-usd",
        txn_date="2026-01-05",
        amount="-100.00",
        currency="USD",
        created_at="2026-01-05 09:00:00",
    )
    _insert_conversion(
        db,
        source_id="linked-eur-in",
        account_id="acct-eur-fifo",
        txn_date="2026-01-05",
        amount="80.00",
        currency="EUR",
        created_at="2026-01-05 10:00:00",
    )
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, account_id_b, confidence_score, match_type,
            match_status, decided_by, decided_at
        ) VALUES (
            'match-fx-home-eur', 'linked-usd-out', 'manual', 'user',
            'linked-eur-in', 'manual', 'user', 'acct-usd', 'acct-eur-fifo',
            1.0000, 'transfer', 'accepted', 'user',
            '2026-01-05 11:00:00'::TIMESTAMP
        )
        """
    )

    for values in (
        (
            "single-eur-dispose",
            "acct-eur-fifo",
            "2026-02-01",
            "-40.00",
            "EUR",
            "2026-02-02 10:00:00",
            "70.00",
            "USD",
        ),
        (
            "single-gbp-acquire-1",
            "acct-gbp-average",
            "2026-01-10",
            "-120.00",
            "USD",
            "2026-01-10 10:00:00",
            "100.00",
            "GBP",
        ),
        (
            "single-gbp-acquire-2",
            "acct-gbp-average",
            "2026-01-20",
            "-180.00",
            "USD",
            "2026-01-20 10:00:00",
            "100.00",
            "GBP",
        ),
        (
            "single-gbp-dispose-home",
            "acct-gbp-average",
            "2026-02-10",
            "-50.00",
            "GBP",
            "2026-02-11 10:00:00",
            "100.00",
            "USD",
        ),
        (
            "single-gbp-to-eur",
            "acct-gbp-average",
            "2026-03-01",
            "-40.00",
            "GBP",
            "2026-03-02 10:00:00",
            "50.00",
            "EUR",
        ),
        (
            "single-incomplete",
            "acct-incomplete",
            "2026-05-01",
            "-10.00",
            "EUR",
            "2026-05-01 10:00:00",
            None,
            "USD",
        ),
    ):
        _insert_conversion(
            db,
            source_id=values[0],
            account_id=values[1],
            txn_date=values[2],
            amount=values[3],
            currency=values[4],
            created_at=values[5],
            to_amount=values[6],
            to_currency=values[7],
        )

    db.execute(
        """
        INSERT INTO raw.exchange_rates (
            from_currency, to_currency, rate_date, rate, source_type, loaded_at
        ) VALUES
            ('EUR', 'USD', '2026-03-01'::DATE, 1.50000000,
             'frankfurter', '2026-03-02 12:00:00'::TIMESTAMP),
            ('EUR', 'USD', '2026-04-01'::DATE, 1.60000000,
             'frankfurter', '2026-04-02 12:00:00'::TIMESTAMP)
        """
    )
    db.execute(
        """
        INSERT INTO app.securities (
            security_id, name, security_type, currency_code
        ) VALUES ('sec-eur', 'Foreign Equity Fixture', 'equity', 'EUR')
        """
    )
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id, type,
            trade_date, quantity, amount, fees, currency_code, description,
            created_at, created_by, investment_transaction_id
        ) VALUES (
            'security-sale-eur', 'import-fx', 'acct-eur-fifo', 'sec-eur',
            'sell', '2026-04-01'::DATE, -1::DECIMAL(28,10),
            30.00::DECIMAL(18,2), 5.00::DECIMAL(18,2), 'EUR',
            'Foreign sale fixture', '2026-04-02 10:00:00'::TIMESTAMP,
            'cli', 'inv-eur-sale'
        )
        """
    )


@pytest.mark.slow
def test_empty_transform_materializes_exact_currency_accounting_schemas(
    db: Database,
) -> None:
    """Empty inputs still build all fixed schemas without yielding empty frames."""
    result = TransformService(db).apply()
    assert result.applied, f"transform apply failed: {result.error}"

    for table, expected in _SCHEMAS.items():
        assert _catalog_schema(db, table) == expected
        assert db.execute(f"SELECT COUNT(*) FROM core.{table}").fetchone() == (0,)  # noqa: S608  # table names are fixed test constants

    for grain in ("conversion", "currency_lot", "realized_fx_gain"):
        assert [_metric(grain, reason) for reason in _REASONS] == [0.0] * 9


@pytest.mark.slow
def test_transform_moves_same_currency_basis_and_realizes_only_on_later_disposal(
    db: Database,
) -> None:
    db.execute(
        """
        INSERT INTO app.profile_settings (home_currency, updated_at)
        VALUES ('USD', '2025-12-01 09:00:00'::TIMESTAMP)
        """
    )
    for source_id, account_id, txn_date, amount, currency, created_at in (
        (
            "acquire-usd-out",
            "acct-usd",
            "2026-01-01",
            "-100.00",
            "USD",
            "2026-01-01 09:00:00",
        ),
        (
            "acquire-eur-in",
            "acct-eur-source",
            "2026-01-01",
            "80.00",
            "EUR",
            "2026-01-01 10:00:00",
        ),
        (
            "move-eur-out",
            "acct-eur-source",
            "2026-02-01",
            "-40.00",
            "EUR",
            "2026-02-01 09:00:00",
        ),
        (
            "move-eur-in",
            "acct-eur-destination",
            "2026-02-01",
            "40.00",
            "EUR",
            "2026-02-01 10:00:00",
        ),
    ):
        _insert_conversion(
            db,
            source_id=source_id,
            account_id=account_id,
            txn_date=txn_date,
            amount=amount,
            currency=currency,
            created_at=created_at,
        )
    _insert_conversion(
        db,
        source_id="dispose-eur",
        account_id="acct-eur-destination",
        txn_date="2026-03-01",
        amount="-20.00",
        currency="EUR",
        created_at="2026-03-01 10:00:00",
        to_amount="30.00",
        to_currency="USD",
    )
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, account_id_b, confidence_score, match_type,
            match_status, decided_by, decided_at
        ) VALUES
            (
                'match-acquisition', 'acquire-usd-out', 'manual', 'user',
                'acquire-eur-in', 'manual', 'user', 'acct-usd',
                'acct-eur-source', 1.0000, 'transfer', 'accepted', 'user',
                '2026-01-01 11:00:00'::TIMESTAMP
            ),
            (
                'match-movement', 'move-eur-out', 'manual', 'user',
                'move-eur-in', 'manual', 'user', 'acct-eur-source',
                'acct-eur-destination', 1.0000, 'transfer', 'accepted', 'user',
                '2026-02-01 11:00:00'::TIMESTAMP
            )
        """
    )

    result = TransformService(db).apply()

    assert result.applied, f"transform apply failed: {result.error}"
    acquisition_conversion_id = db.execute(
        """
        SELECT conversion_id
        FROM core.bridge_currency_conversions
        WHERE transfer_pair_id = 'match-acquisition'
        """
    ).fetchone()
    assert acquisition_conversion_id is not None
    assert db.execute(
        """
        SELECT COUNT(*)
        FROM core.bridge_currency_conversions
        WHERE transfer_pair_id = 'match-movement'
        """
    ).fetchone() == (0,)
    moved = db.execute(
        """
        SELECT acquisition_type, source_transfer_id, source_conversion_id,
               original_quantity, remaining_quantity, cost_basis_total,
               cost_basis_remaining, coverage_status, coverage_reason
        FROM core.fct_currency_lots
        WHERE account_id = 'acct-eur-destination'
        """
    ).fetchone()
    assert moved == (
        "transfer",
        "match-movement",
        acquisition_conversion_id[0],
        Decimal("40.00"),
        Decimal("20.00"),
        Decimal("50.00"),
        Decimal("25.00"),
        "complete",
        None,
    )
    assert db.execute(
        """
        SELECT disposed_amount, proceeds, cost_basis, gain_loss,
               coverage_status, coverage_reason
        FROM core.fct_realized_fx_gains
        WHERE account_id = 'acct-eur-destination'
        """
    ).fetchone() == (
        Decimal("20.00"),
        Decimal("30.00"),
        Decimal("25.00"),
        Decimal("5.00"),
        "complete",
        None,
    )


@pytest.mark.slow
def test_transform_materializes_currency_lots_gains_and_bounded_metrics(
    db: Database,
) -> None:
    """Real conversion and sale inputs materialize exact M1K.3 accounting rows."""
    _seed_real_transform_inputs(db)

    result = TransformService(db).apply()
    assert result.applied, f"transform apply failed: {result.error}"

    conversion_summary = db.execute(
        """
        SELECT conversion_id, source_shape, coverage_status, coverage_reason
        FROM core.bridge_currency_conversions
        ORDER BY conversion_id
        """
    ).fetchall()
    assert conversion_summary == [
        ("fxc_03b40bc949e0c038", "single_row", "complete", None),
        ("fxc_45f0e643265d8758", "single_row", "complete", None),
        ("fxc_4e5ad73a4149fbda", "linked_two_row", "complete", None),
        ("fxc_9498dd3eb745369b", "single_row", "complete", None),
        ("fxc_b57f91bede719d3c", "single_row", "complete", None),
        ("fxc_ba4a7ca07e3d4d8e", "single_row", "incomplete", "incomplete_shape"),
        ("fxc_d6dd679f2cca8a9d", "single_row", "complete", None),
    ]

    linked = db.execute(
        """
        SELECT transfer_pair_id, from_transaction_id, to_transaction_id,
               from_account_id, to_account_id, from_date, to_date,
               from_amount, from_currency, to_amount, to_currency,
               executed_rate, home_currency, home_value, valuation_rate,
               valuation_rate_date, valuation_source_type,
               from_source_type, from_source_origin,
               from_source_transaction_id, to_source_type, to_source_origin,
               to_source_transaction_id, updated_at
        FROM core.bridge_currency_conversions
        WHERE conversion_id = 'fxc_4e5ad73a4149fbda'
        """
    ).fetchone()
    assert linked == (
        "match-fx-home-eur",
        "6c7b33570d8c57c6",
        "2a960429624a8060",
        "acct-usd",
        "acct-eur-fifo",
        date(2026, 1, 5),
        date(2026, 1, 5),
        Decimal("100.00"),
        "USD",
        Decimal("80.00"),
        "EUR",
        Decimal("0.80000000"),
        "USD",
        Decimal("100.00"),
        Decimal("1.25000000"),
        date(2026, 1, 5),
        "actual",
        "manual",
        "user",
        "linked-usd-out",
        "manual",
        "user",
        "linked-eur-in",
        datetime(2026, 1, 5, 11, 0, 0),
    )

    foreign_exchange = db.execute(
        """
        SELECT from_amount, from_currency, to_amount, to_currency,
               executed_rate, home_value, valuation_rate,
               valuation_rate_date, valuation_source_type, updated_at
        FROM core.bridge_currency_conversions
        WHERE conversion_id = 'fxc_9498dd3eb745369b'
        """
    ).fetchone()
    assert foreign_exchange == (
        Decimal("40.00"),
        "GBP",
        Decimal("50.00"),
        "EUR",
        Decimal("1.25000000"),
        Decimal("75.00"),
        Decimal("1.50000000"),
        date(2026, 3, 1),
        "frankfurter",
        datetime(2026, 3, 2, 12, 0, 0),
    )

    lots = {
        row[0]: row[1:]
        for row in db.execute(
            """
            SELECT currency_lot_id, account_id, currency_code,
                   acquisition_date, acquisition_type, original_quantity,
                   remaining_quantity, cost_basis_total, cost_basis_remaining,
                   cost_basis_method, home_currency, source_conversion_id,
                   source_investment_transaction_id, basis_incomplete,
                   coverage_status, coverage_reason, updated_at
            FROM core.fct_currency_lots
            ORDER BY currency_lot_id
            """
        ).fetchall()
    }
    assert lots == {
        "clot_981fc3557ff89cf3": (
            "acct-eur-fifo",
            "EUR",
            date(2026, 1, 5),
            "conversion",
            Decimal("80.00"),
            Decimal("40.00"),
            Decimal("100.00"),
            Decimal("50.00"),
            "fifo",
            "USD",
            "fxc_4e5ad73a4149fbda",
            None,
            False,
            "complete",
            None,
            datetime(2026, 4, 2, 12, 0, 0),
        ),
        "clot_b48056b8a404035f": (
            "acct-gbp-average",
            "GBP",
            date(2026, 1, 10),
            "conversion",
            Decimal("100.00"),
            Decimal("10.00"),
            Decimal("120.00"),
            Decimal("15.00"),
            "average",
            "USD",
            "fxc_45f0e643265d8758",
            None,
            False,
            "complete",
            None,
            datetime(2026, 3, 2, 12, 0, 0),
        ),
        "clot_872a3c437737ba98": (
            "acct-gbp-average",
            "GBP",
            date(2026, 1, 20),
            "conversion",
            Decimal("100.00"),
            Decimal("100.00"),
            Decimal("180.00"),
            Decimal("150.00"),
            "average",
            "USD",
            "fxc_03b40bc949e0c038",
            None,
            False,
            "complete",
            None,
            datetime(2026, 3, 2, 12, 0, 0),
        ),
        "clot_9ac7325df7857fbe": (
            "acct-gbp-average",
            "EUR",
            date(2026, 3, 1),
            "conversion",
            Decimal("50.00"),
            Decimal("50.00"),
            Decimal("75.00"),
            Decimal("75.00"),
            "average",
            "USD",
            "fxc_9498dd3eb745369b",
            None,
            False,
            "complete",
            None,
            datetime(2026, 3, 2, 12, 0, 0),
        ),
        "clot_e05af1a4b4dba32e": (
            "acct-eur-fifo",
            "EUR",
            date(2026, 4, 1),
            "security_sale",
            Decimal("30.00"),
            Decimal("30.00"),
            Decimal("48.00"),
            Decimal("48.00"),
            "fifo",
            "USD",
            None,
            "inv-eur-sale",
            False,
            "complete",
            None,
            datetime(2026, 4, 2, 12, 0, 0),
        ),
    }

    gains = {
        row[0]: row[1:]
        for row in db.execute(
            """
            SELECT realized_fx_gain_id, account_id, conversion_id,
                   currency_lot_id, currency_code, home_currency,
                   acquisition_date, disposal_date, disposed_amount, proceeds,
                   cost_basis, gain_loss, fee_amount, cost_basis_method,
                   valuation_rate, valuation_rate_date, valuation_source_type,
                   coverage_status, coverage_reason, updated_at
            FROM core.fct_realized_fx_gains
            ORDER BY realized_fx_gain_id
            """
        ).fetchall()
    }
    assert gains == {
        "rfx_a3f769064fbf488d": (
            "acct-eur-fifo",
            "fxc_b57f91bede719d3c",
            "clot_981fc3557ff89cf3",
            "EUR",
            "USD",
            date(2026, 1, 5),
            date(2026, 2, 1),
            Decimal("40.00"),
            Decimal("70.00"),
            Decimal("50.00"),
            Decimal("20.00"),
            Decimal("0.00"),
            "fifo",
            Decimal("1.75000000"),
            date(2026, 2, 1),
            "actual",
            "complete",
            None,
            datetime(2026, 4, 2, 12, 0, 0),
        ),
        "rfx_7be9cd658f8b45f5": (
            "acct-gbp-average",
            "fxc_d6dd679f2cca8a9d",
            "clot_b48056b8a404035f",
            "GBP",
            "USD",
            date(2026, 1, 10),
            date(2026, 2, 10),
            Decimal("50.00"),
            Decimal("100.00"),
            Decimal("75.00"),
            Decimal("25.00"),
            Decimal("0.00"),
            "average",
            Decimal("2.00000000"),
            date(2026, 2, 10),
            "actual",
            "complete",
            None,
            datetime(2026, 3, 2, 12, 0, 0),
        ),
        "rfx_31e33aed4cbcefe5": (
            "acct-gbp-average",
            "fxc_9498dd3eb745369b",
            "clot_b48056b8a404035f",
            "GBP",
            "USD",
            date(2026, 1, 10),
            date(2026, 3, 1),
            Decimal("40.00"),
            Decimal("75.00"),
            Decimal("60.00"),
            Decimal("15.00"),
            Decimal("0.00"),
            "average",
            Decimal("1.50000000"),
            date(2026, 3, 1),
            "frankfurter",
            "complete",
            None,
            datetime(2026, 3, 2, 12, 0, 0),
        ),
    }

    public_rows = (
        db.execute("SELECT * FROM core.bridge_currency_conversions").fetchall()
        + db.execute("SELECT * FROM core.fct_currency_lots").fetchall()
        + db.execute("SELECT * FROM core.fct_realized_fx_gains").fetchall()
    )
    assert "currency:" not in repr(public_rows)

    assert _metric("conversion", "complete") == 6
    assert _metric("conversion", "incomplete_shape") == 1
    assert _metric("currency_lot", "complete") == 5
    assert _metric("realized_fx_gain", "complete") == 3

    first_ids = {
        "conversion": tuple(
            row[0]
            for row in db.execute(
                """
                SELECT conversion_id FROM core.bridge_currency_conversions
                WHERE coverage_status = 'complete' ORDER BY conversion_id
                """
            ).fetchall()
        ),
        "currency_lot": tuple(sorted(lots)),
        "realized_fx_gain": tuple(sorted(gains)),
    }
    db.execute(
        "DELETE FROM raw.manual_transactions "
        "WHERE source_transaction_id = 'single-incomplete'"
    )
    result = TransformService(db).apply()
    assert result.applied, f"second transform apply failed: {result.error}"

    second_ids = {
        "conversion": tuple(
            row[0]
            for row in db.execute(
                "SELECT conversion_id FROM core.bridge_currency_conversions "
                "ORDER BY conversion_id"
            ).fetchall()
        ),
        "currency_lot": tuple(
            row[0]
            for row in db.execute(
                "SELECT currency_lot_id FROM core.fct_currency_lots "
                "ORDER BY currency_lot_id"
            ).fetchall()
        ),
        "realized_fx_gain": tuple(
            row[0]
            for row in db.execute(
                "SELECT realized_fx_gain_id FROM core.fct_realized_fx_gains "
                "ORDER BY realized_fx_gain_id"
            ).fetchall()
        ),
    }
    assert second_ids == first_ids
    assert _metric("conversion", "incomplete_shape") == 0
    assert _metric("conversion", "complete") == 6
    assert _metric("currency_lot", "complete") == 5
    assert _metric("realized_fx_gain", "complete") == 3

    override_event = CurrencyService(db, actor="test").set_override(
        "EUR",
        "USD",
        date(2026, 3, 1),
        Decimal("2.00000000"),
        note="fixture correction",
    )
    corrected = db.execute(
        """
        SELECT home_value, valuation_rate, valuation_source_type, updated_at
        FROM core.bridge_currency_conversions
        WHERE conversion_id = 'fxc_9498dd3eb745369b'
        """
    ).fetchone()
    assert corrected is not None
    assert corrected[:3] == (Decimal("100.00"), Decimal("2.00000000"), "override")
    corrected_gain = db.execute(
        """
        SELECT proceeds, cost_basis, gain_loss, updated_at
        FROM core.fct_realized_fx_gains
        WHERE conversion_id = 'fxc_9498dd3eb745369b'
        """
    ).fetchone()
    assert corrected_gain is not None
    assert corrected_gain[:3] == (
        Decimal("100.00"),
        Decimal("60.00"),
        Decimal("40.00"),
    )
    corrected_lot_updated_at = db.execute(
        """
        SELECT updated_at FROM core.fct_currency_lots
        WHERE source_conversion_id = 'fxc_9498dd3eb745369b'
        """
    ).fetchone()
    assert corrected_lot_updated_at is not None

    UndoService(db).undo(override_event.operation_id, actor="test")
    restored = db.execute(
        """
        SELECT home_value, valuation_rate, valuation_source_type, updated_at
        FROM core.bridge_currency_conversions
        WHERE conversion_id = 'fxc_9498dd3eb745369b'
        """
    ).fetchone()
    assert restored is not None
    assert restored[:3] == (
        Decimal("75.00"),
        Decimal("1.50000000"),
        "frankfurter",
    )
    restored_gain = db.execute(
        """
        SELECT proceeds, cost_basis, gain_loss, updated_at
        FROM core.fct_realized_fx_gains
        WHERE conversion_id = 'fxc_9498dd3eb745369b'
        """
    ).fetchone()
    assert restored_gain is not None
    assert restored_gain[:3] == (
        Decimal("75.00"),
        Decimal("60.00"),
        Decimal("15.00"),
    )
    restored_lot_updated_at = db.execute(
        """
        SELECT updated_at FROM core.fct_currency_lots
        WHERE source_conversion_id = 'fxc_9498dd3eb745369b'
        """
    ).fetchone()
    assert restored_lot_updated_at is not None
    assert restored[3] > corrected[3]
    assert restored_gain[3] > corrected_gain[3]
    assert restored_lot_updated_at[0] > corrected_lot_updated_at[0]
