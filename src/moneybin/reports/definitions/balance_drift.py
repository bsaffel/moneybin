"""core:balance_drift — asserted vs computed balance reconciliation."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    Binding,
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import DRIFT_STATUSES, validate_date
from moneybin.services.account_service import AccountService
from moneybin.tables import REPORTS_BALANCE_DRIFT

#: The |drift| bucket edges, in the currency the drift is denominated in.
#: Mirrors the CASE in ``sqlmesh/models/reports/balance_drift.sql`` — SQL cannot
#: read a Python constant, so the two must be changed together, and that file
#: says so beside its own copy.
_CLEAN_BELOW = Decimal("1.00")
_WARNING_BELOW = Decimal("10.00")

#: Statuses that say nothing about magnitude, so conversion cannot restate them.
#: ``no-data`` has no computed balance to drift from and ``currency-mismatch``
#: means no drift was computable at all — re-bucketing either from an amount
#: that does not exist would invent a reconciliation verdict.
_MAGNITUDE_FREE_STATUSES = frozenset({"no-data", "currency-mismatch"})


def _rebucket_status(rows: list[dict[str, Any]], _currency: str) -> None:
    """Re-derive the drift and everything read off it from the converted balances.

    Two separate things go stale. The bucket edges are absolute amounts, so they
    only describe the currency the drift is actually in: left alone, a 500 JPY
    drift shown as 3.40 USD keeps the ``drift`` label it earned at 500, a
    verdict that contradicts the figure printed beside it. And ``drift`` is
    declared as asserted minus computed, but conversion prices all three columns
    independently and rounds each — asserted 1.00, computed 0.01 and drift 0.99
    at rate 0.50 give 0.50, 0.01 and 0.50, so the published difference disagrees
    with the two balances it claims to be the difference of.

    Restating the drift from the converted balances fixes the identity, and the
    magnitude, percentage and bucket all follow from it rather than from three
    separately rounded values.
    """
    for row in rows:
        if row.get("status") in _MAGNITUDE_FREE_STATUSES:
            continue
        asserted = row.get("asserted_balance")
        computed = row.get("computed_balance")
        if isinstance(asserted, Decimal) and isinstance(computed, Decimal):
            row["drift"] = asserted - computed
            # Guarded exactly as the SQL model guards it: a balance asserted at
            # zero has no ratio, and dividing would raise on a converted read
            # where the unconverted one returned a null.
            row["drift_pct"] = (
                float((asserted - computed) / asserted) if asserted else None
            )
        drift = row.get("drift")
        if not isinstance(drift, Decimal):
            continue
        magnitude = abs(drift)
        # Derived from the same column, so it is restated here rather than left
        # to independent rounding of a second converted value.
        row["drift_abs"] = magnitude
        if magnitude < _CLEAN_BELOW:
            row["status"] = "clean"
        elif magnitude < _WARNING_BELOW:
            row["status"] = "warning"
        else:
            row["status"] = "drift"


@report(
    report_id="core:balance_drift",
    name="balance_drift",
    view=REPORTS_BALANCE_DRIFT,
    classes={
        "account_id": DataClass.RECORD_ID,
        # dim_accounts.display_name (user-authored) → USER_NOTE; not the bank's
        # official_name (INSTITUTION) nor gsheet_connections.account_name.
        "account_name": DataClass.USER_NOTE,
        "currency_code": DataClass.CURRENCY,
        "assertion_date": DataClass.TXN_DATE,
        "asserted_balance": DataClass.BALANCE,
        "computed_balance": DataClass.BALANCE,
        "drift": DataClass.TXN_AMOUNT,
        "drift_abs": DataClass.TXN_AMOUNT,
        "drift_pct": DataClass.AGGREGATE,
        # CURRENT_DATE is public, so a day-count is bijective with
        # assertion_date (assertion_date = CURRENT_DATE - days_since_assertion) —
        # this is a date, not an aggregate; a LOW-tier session must not see it
        # unmasked when assertion_date itself would be masked.
        "days_since_assertion": DataClass.TXN_DATE,
        "status": DataClass.TXN_TYPE,
    },
    parameter_classes={
        "account": DataClass.ACCOUNT_IDENTIFIER,
        "status": DataClass.TXN_TYPE,
        "since": DataClass.TXN_DATE,
    },
    columns=(
        OutputColumn("account_id", "Owning account identifier.", DataClass.RECORD_ID),
        OutputColumn("account_name", "Account display name.", DataClass.USER_NOTE),
        OutputColumn(
            "currency_code",
            "ISO 4217 currency this row is denominated in; null means unknown.",
            DataClass.CURRENCY,
        ),
        OutputColumn(
            "assertion_date", "User-asserted balance date.", DataClass.TXN_DATE
        ),
        OutputColumn(
            "asserted_balance",
            "User-entered balance as of assertion_date.",
            DataClass.BALANCE,
        ),
        OutputColumn(
            "computed_balance",
            "Independent transaction-derived position as of assertion_date.",
            DataClass.BALANCE,
        ),
        OutputColumn(
            "drift",
            "Asserted balance minus computed balance.",
            DataClass.TXN_AMOUNT,
        ),
        OutputColumn("drift_abs", "Absolute balance drift.", DataClass.TXN_AMOUNT),
        OutputColumn(
            "drift_pct",
            "Drift divided by asserted balance.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "days_since_assertion",
            "Days from assertion_date through current date.",
            DataClass.TXN_DATE,
        ),
        OutputColumn("status", "Reconciliation status bucket.", DataClass.TXN_TYPE),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency="currency_code",
        sign="drift is asserted balance minus computed balance; drift_abs is unsigned",
        kind="position",
        valuation_basis=(
            "transaction-derived position reconstructed from daily balance minus "
            "reconciliation_delta"
        ),
        fx_basis=(
            "each row compares one account's balances on one date, so a requested "
            "display currency prices it at that date's rate; a row that cannot be "
            "priced leaves the whole report segmented per currency_code, never "
            "blended"
        ),
        fx_date="assertion_date",
        time_basis=(
            "asserted and transaction-derived positions compared as of "
            "assertion_date; freshness measured from assertion_date through "
            "current date"
        ),
        denominator="asserted_balance for drift_pct; null when asserted balance is zero",
        comparison_window=(
            "asserted position versus independent transaction-derived position on "
            "assertion_date"
        ),
        exclusions=("archived accounts",),
        provenance=("reports.balance_drift",),
    ),
    class_downgrades={
        "drift_pct": "ratio of two already-declared BALANCE columns "
        "(drift / asserted_balance); a percentage reveals no absolute "
        "balance figure",
        "status": "coarse 4-way bucket on |drift| (<$1 / <$10 / >=$10 / "
        "no-data, currency-mismatch), never the drift or balance values themselves",
    },
    on_converted=_rebucket_status,
)
def balance_drift(
    db: Database,
    *,
    account: str | None = None,
    status: str = "all",
    since: str | None = None,
) -> ReportQuery:
    """Balance reconciliation drift: asserted vs computed, one row per assertion.

    Balances are positions in the account's own currency_code. Drift is asserted balance
    minus the independent transaction-derived position for assertion_date.

    Rows interleave the currencies, worst drift first within each, so a
    truncated result still represents every currency. Compare drift_abs only
    between rows sharing a currency_code.

    Args:
        db: Open read-only database connection.
        account: Filter to an account; accepts account_id or case-insensitive
            display_name. Ambiguous display_name matches raise; None for all.
        status: drift | warning | clean | no-data | currency-mismatch | all.
            Selects on the bucket in each row's own currency. A display-
            converted read re-buckets what it returns, so combining this with
            a display currency can return a row whose displayed status differs
            from the one asked for — the filter runs in SQL, before any rate is
            known. Filter on `all` and read the returned status when converting.
        since: ISO date; only assertions on or after.

    Examples:
        reports(report_id="core:balance_drift", parameters={"status": "drift"})
        reports(report_id="core:balance_drift", parameters={"account": "Checking"})
    """
    if status not in DRIFT_STATUSES:
        raise ValueError(f"Unknown status: {status}")
    if since is not None:
        # since binds to assertion_date >= ?; a malformed string compares
        # lexicographically and silently mis-filters.
        validate_date(since, "since")
    sql = f"""
        SELECT account_id, account_name, currency_code, assertion_date, asserted_balance,
               computed_balance, drift, drift_abs, drift_pct,
               days_since_assertion, status
        FROM {REPORTS_BALANCE_DRIFT.full_name}
        WHERE 1=1
    """  # noqa: S608  # TableRef interpolation
    params: list[Binding] = []
    if account:
        sql += " AND account_id = ?"
        # Bind the filter to the resolved account_id (free-text → id at the
        # boundary; raises on ambiguity) per the identifiers rule.
        #
        # R9's worked example: the *parameter* is declared
        # ACCOUNT_IDENTIFIER — free text a user typed — while the *binding* is a
        # minted opaque surrogate, RECORD_ID. Neither class describes the other's
        # value, so the class travels with the binding.
        params.append(
            Binding(AccountService(db).resolve_strict(account), DataClass.RECORD_ID)
        )
    if status != "all":
        sql += " AND status = ?"
        params.append(Binding(status, DataClass.TXN_TYPE))
    if since:
        sql += " AND assertion_date >= ?"
        params.append(Binding(since, DataClass.TXN_DATE))
    # Interleave the currencies rather than ranking their magnitudes together.
    # The framework truncates with `records[:max_rows]`, so a global
    # `ORDER BY drift_abs DESC` lets one high-denomination currency fill the cap
    # and drop the others out of the response entirely. Ties break on
    # currency_code because "which drift is larger" has no answer across
    # currencies without conversion. A single-currency profile is unaffected:
    # its ROW_NUMBER already ascends in drift_abs order.
    sql += """
        ORDER BY ROW_NUMBER() OVER (
            PARTITION BY currency_code ORDER BY drift_abs DESC
        ), currency_code
    """

    actions = [
        "Rerun reports(report_id='core:balance_drift', "
        "parameters={'account': '<name or id>'}) to filter to one account",
        "Rerun reports(report_id='core:balance_drift', "
        "parameters={'status': 'drift'}) to show drift rows",
    ]
    return ReportQuery(sql, params, actions=actions)
