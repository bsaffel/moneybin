"""core:realized_fx — realized foreign-exchange gain/loss by lot allocation."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    Binding,
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import REALIZED_FX_COVERAGE, validate_date
from moneybin.services._validators import validate_currency_code
from moneybin.tables import REPORTS_REALIZED_FX


@report(
    report_id="core:realized_fx",
    name="realized_fx",
    view=REPORTS_REALIZED_FX,
    classes={
        "realized_fx_gain_id": DataClass.RECORD_ID,
        "conversion_id": DataClass.RECORD_ID,
        "currency_lot_id": DataClass.RECORD_ID,
        "account_id": DataClass.RECORD_ID,
        "transfer_pair_id": DataClass.RECORD_ID,
        "from_transaction_id": DataClass.RECORD_ID,
        "to_transaction_id": DataClass.RECORD_ID,
        "from_source_transaction_id": DataClass.RECORD_ID,
        "to_source_transaction_id": DataClass.RECORD_ID,
        "source_conversion_id": DataClass.RECORD_ID,
        "source_investment_transaction_id": DataClass.RECORD_ID,
        "source_transfer_id": DataClass.RECORD_ID,
        "account_name": DataClass.USER_NOTE,
        "currency_code": DataClass.CURRENCY,
        "home_currency": DataClass.CURRENCY,
        "from_currency": DataClass.CURRENCY,
        "to_currency": DataClass.CURRENCY,
        "source_shape": DataClass.TXN_TYPE,
        "acquisition_type": DataClass.TXN_TYPE,
        "cost_basis_method": DataClass.TXN_TYPE,
        "valuation_source_type": DataClass.TXN_TYPE,
        "from_source_type": DataClass.TXN_TYPE,
        "from_source_origin": DataClass.TXN_TYPE,
        "to_source_type": DataClass.TXN_TYPE,
        "to_source_origin": DataClass.TXN_TYPE,
        "coverage_status": DataClass.TXN_TYPE,
        "coverage_reason": DataClass.TXN_TYPE,
        "acquisition_date": DataClass.TXN_DATE,
        "disposal_date": DataClass.TXN_DATE,
        "valuation_rate_date": DataClass.TXN_DATE,
        "executed_rate": DataClass.AGGREGATE,
        "valuation_rate": DataClass.AGGREGATE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "from_amount": DataClass.TXN_AMOUNT,
        "to_amount": DataClass.TXN_AMOUNT,
        "disposed_amount": DataClass.TXN_AMOUNT,
        "proceeds": DataClass.BALANCE,
        "cost_basis": DataClass.BALANCE,
        "fee_amount": DataClass.TXN_AMOUNT,
        "gain_loss": DataClass.BALANCE,
    },
    parameter_classes={
        "from_date": DataClass.TXN_DATE,
        "to_date": DataClass.TXN_DATE,
        "currency": DataClass.CURRENCY,
        "coverage": DataClass.TXN_TYPE,
    },
    columns=(
        OutputColumn(
            "realized_fx_gain_id",
            "Stable realized FX gain identifier.",
            DataClass.RECORD_ID,
        ),
        OutputColumn(
            "conversion_id",
            "Disposal conversion identifier.",
            DataClass.RECORD_ID,
        ),
        OutputColumn(
            "currency_lot_id",
            "Consumed Currency lot; null for unmatched-inventory placeholders.",
            DataClass.RECORD_ID,
        ),
        OutputColumn("account_id", "Holding account identifier.", DataClass.RECORD_ID),
        OutputColumn(
            "transfer_pair_id",
            "Accepted Transfer Decision linking the disposal legs.",
            DataClass.RECORD_ID,
        ),
        OutputColumn(
            "from_transaction_id",
            "Canonical sent-leg Transaction identifier.",
            DataClass.RECORD_ID,
        ),
        OutputColumn(
            "to_transaction_id",
            "Canonical received-leg Transaction identifier, when present.",
            DataClass.RECORD_ID,
        ),
        OutputColumn(
            "from_source_transaction_id",
            "Native reference supplying the sent leg.",
            DataClass.RECORD_ID,
        ),
        OutputColumn(
            "to_source_transaction_id",
            "Native reference supplying the received leg, when present.",
            DataClass.RECORD_ID,
        ),
        OutputColumn(
            "source_conversion_id",
            "Conversion that originally acquired the consumed lot.",
            DataClass.RECORD_ID,
        ),
        OutputColumn(
            "source_investment_transaction_id",
            "Foreign Security sale that originally acquired the lot.",
            DataClass.RECORD_ID,
        ),
        OutputColumn(
            "source_transfer_id",
            "Accepted same-Currency Transfer that last moved the lot.",
            DataClass.RECORD_ID,
        ),
        OutputColumn("account_name", "Account display name.", DataClass.USER_NOTE),
        OutputColumn(
            "currency_code", "ISO 4217 Currency disposed.", DataClass.CURRENCY
        ),
        OutputColumn(
            "home_currency",
            "ISO 4217 Home currency for the accounting amounts.",
            DataClass.CURRENCY,
        ),
        OutputColumn(
            "from_currency",
            "ISO 4217 Currency sent in the conversion.",
            DataClass.CURRENCY,
        ),
        OutputColumn(
            "to_currency",
            "ISO 4217 Currency received in the conversion.",
            DataClass.CURRENCY,
        ),
        OutputColumn(
            "source_shape", "linked_two_row or single_row.", DataClass.TXN_TYPE
        ),
        OutputColumn(
            "acquisition_type",
            "conversion, security_sale, or transfer.",
            DataClass.TXN_TYPE,
        ),
        OutputColumn(
            "cost_basis_method", "FIFO or average cost basis.", DataClass.TXN_TYPE
        ),
        OutputColumn(
            "valuation_source_type",
            "Actual, override, or Provider valuation source.",
            DataClass.TXN_TYPE,
        ),
        OutputColumn("from_source_type", "Sent-leg Source type.", DataClass.TXN_TYPE),
        OutputColumn(
            "from_source_origin", "Sent-leg Source origin.", DataClass.TXN_TYPE
        ),
        OutputColumn("to_source_type", "Received-leg Source type.", DataClass.TXN_TYPE),
        OutputColumn(
            "to_source_origin", "Received-leg Source origin.", DataClass.TXN_TYPE
        ),
        OutputColumn(
            "coverage_status",
            "complete or incomplete accounting coverage.",
            DataClass.TXN_TYPE,
        ),
        OutputColumn(
            "coverage_reason",
            "Closed reason when accounting coverage is incomplete.",
            DataClass.TXN_TYPE,
        ),
        OutputColumn(
            "acquisition_date",
            "Date the consumed Currency was acquired.",
            DataClass.TXN_DATE,
        ),
        OutputColumn(
            "disposal_date", "Date the Currency was disposed.", DataClass.TXN_DATE
        ),
        OutputColumn(
            "valuation_rate_date",
            "Date of the actual terms or stored valuation rate.",
            DataClass.TXN_DATE,
        ),
        OutputColumn(
            "executed_rate",
            "Actual received units per sent unit.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "valuation_rate",
            "Rate used for Home-currency proceeds.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "updated_at",
            "Latest contributing input timestamp.",
            DataClass.TIMESTAMP_OBSERVABILITY,
        ),
        OutputColumn(
            "from_amount",
            "Positive magnitude actually sent in from_currency.",
            DataClass.TXN_AMOUNT,
            money_kind="magnitude",
        ),
        OutputColumn(
            "to_amount",
            "Positive magnitude actually received in to_currency.",
            DataClass.TXN_AMOUNT,
            money_kind="magnitude",
        ),
        OutputColumn(
            "disposed_amount",
            "Positive amount of Currency disposed.",
            DataClass.TXN_AMOUNT,
            money_kind="magnitude",
        ),
        OutputColumn(
            "proceeds",
            "Home-currency disposal proceeds.",
            DataClass.BALANCE,
            money_kind="magnitude",
        ),
        OutputColumn(
            "cost_basis",
            "Home-currency basis of the disposed Currency.",
            DataClass.BALANCE,
            money_kind="magnitude",
        ),
        OutputColumn(
            "fee_amount",
            "Home-currency fee allocated to the disposal.",
            DataClass.TXN_AMOUNT,
            money_kind="magnitude",
        ),
        OutputColumn(
            "gain_loss",
            "Home-currency proceeds less basis; positive is a gain.",
            DataClass.BALANCE,
            money_kind="flow",
        ),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency=None,
        sign=(
            "disposed_amount, proceeds, cost_basis, and fee_amount are positive "
            "magnitudes; gain_loss is signed, with positive meaning gain"
        ),
        kind="flow",
        valuation_basis=(
            "actual disposal conversion terms and the consumed Currency lot's "
            "historical Home-currency basis"
        ),
        fx_basis=(
            "each row is deliberately mixed-unit: disposed and conversion-leg "
            "amounts retain their stated currencies, while proceeds, cost_basis, "
            "fee_amount, and gain_loss retain the historical Home currency. Display "
            "conversion does not re-price these audited amounts"
        ),
        time_basis=(
            "inclusive acquisition-to-disposal history when a consumed Currency lot exists"
        ),
        denominator=None,
        comparison_window=None,
        exclusions=(
            "unaccepted conversion candidates",
            "Home-currency movements with no realized FX consequence",
        ),
        provenance=("reports.realized_fx",),
    ),
    default_columns=(
        "currency_code",
        "home_currency",
        "coverage_status",
        "disposal_date",
        "gain_loss",
    ),
)
def realized_fx(
    db: Database,  # contract handle; this runner builds pure SQL
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    currency: str | None = None,
    coverage: str = "all",
) -> ReportQuery:
    """Realized FX gain/loss by disposal and lot allocation.

    Disposed amount is in ``currency_code``; proceeds, cost basis, fees, and
    gain/loss are in ``home_currency``. The report deliberately retains both units;
    display conversion does not re-price historical realized accounting. An
    unmatched-inventory placeholder has no ``currency_lot_id`` and stays visible as
    incomplete.

    Args:
        db: Open read-only database connection.
        from_date: Earliest disposal date to include, as YYYY-MM-DD.
        to_date: Latest disposal date to include, as YYYY-MM-DD.
        currency: ISO 4217 disposed Currency; case and surrounding spaces ignored.
        coverage: complete | incomplete | all.

    Examples:
        reports(report_id="core:realized_fx")
        reports(report_id="core:realized_fx", parameters={"currency": "EUR"})
    """
    if from_date is not None:
        validate_date(from_date, "from_date")
    if to_date is not None:
        validate_date(to_date, "to_date")
    if coverage not in REALIZED_FX_COVERAGE:
        raise ValueError(f"Unknown coverage: {coverage}")
    if currency is not None:
        # Keep Polars and the rate service off the CLI's import-only cold path.
        from moneybin.services.currency_service import canonical_currency

        currency = canonical_currency(currency)
        validate_currency_code(currency)

    sql = f"""
        SELECT realized_fx_gain_id, conversion_id, currency_lot_id, account_id,
               transfer_pair_id, from_transaction_id, to_transaction_id,
               from_source_transaction_id, to_source_transaction_id,
               source_conversion_id, source_investment_transaction_id,
               source_transfer_id, account_name, currency_code, home_currency,
               from_currency, to_currency, source_shape, acquisition_type,
               cost_basis_method, valuation_source_type, from_source_type,
               from_source_origin, to_source_type, to_source_origin,
               coverage_status, coverage_reason, acquisition_date, disposal_date,
               valuation_rate_date, executed_rate, valuation_rate, updated_at,
               from_amount, to_amount, disposed_amount, proceeds, cost_basis,
               fee_amount, gain_loss
        FROM {REPORTS_REALIZED_FX.full_name}
        WHERE 1=1
    """  # noqa: S608  # TableRef interpolation
    params: list[Binding] = []
    if from_date is not None:
        sql += " AND disposal_date >= ?"
        params.append(Binding(from_date, DataClass.TXN_DATE))
    if to_date is not None:
        sql += " AND disposal_date <= ?"
        params.append(Binding(to_date, DataClass.TXN_DATE))
    if currency is not None:
        sql += " AND currency_code = ?"
        params.append(Binding(currency, DataClass.CURRENCY))
    if coverage != "all":
        sql += " AND coverage_status = ?"
        params.append(Binding(coverage, DataClass.TXN_TYPE))
    sql += " ORDER BY disposal_date DESC, conversion_id, currency_lot_id"

    if from_date is not None and to_date is not None:
        period = f"{from_date} to {to_date}"
    elif from_date is not None:
        period = f"from {from_date}"
    elif to_date is not None:
        period = f"through {to_date}"
    else:
        period = None
    return ReportQuery(sql, params, period=period)
