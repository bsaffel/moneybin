"""reports_balance_drift — asserted vs computed balance reconciliation."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import DRIFT_STATUSES, validate_date
from moneybin.services.account_service import AccountService
from moneybin.tables import REPORTS_BALANCE_DRIFT


@report(
    report_id="core:balance_drift",
    name="balance_drift",
    view=REPORTS_BALANCE_DRIFT,
    classes={
        "account_id": DataClass.RECORD_ID,
        # dim_accounts.display_name (user-authored) → USER_NOTE; not the bank's
        # official_name (INSTITUTION) nor gsheet_connections.account_name.
        "account_name": DataClass.USER_NOTE,
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
        currency="summary.display_currency",
        sign="drift is asserted balance minus computed balance; drift_abs is unsigned",
        kind="position",
        valuation_basis=(
            "transaction-derived position reconstructed from daily balance minus "
            "reconciliation_delta"
        ),
        fx_basis="no FX conversion in v1; assumes single-currency inputs",
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
        "no-data), never the drift or balance values themselves",
    },
)
def balance_drift(
    db: Database,
    *,
    account: str | None = None,
    status: str = "all",
    since: str | None = None,
) -> ReportQuery:
    """Balance reconciliation drift: asserted vs computed, one row per assertion.

    Balances are positions in summary.display_currency. Drift is asserted balance
    minus the independent transaction-derived position for assertion_date.

    Args:
        db: Open read-only database connection.
        account: Filter to an account; accepts account_id or case-insensitive
            display_name. Ambiguous display_name matches raise; None for all.
        status: drift | warning | clean | no-data | all.
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
        SELECT account_id, account_name, assertion_date, asserted_balance,
               computed_balance, drift, drift_abs, drift_pct,
               days_since_assertion, status
        FROM {REPORTS_BALANCE_DRIFT.full_name}
        WHERE 1=1
    """  # noqa: S608  # TableRef interpolation
    params: list[object] = []
    if account:
        sql += " AND account_id = ?"
        # Bind the filter to the resolved account_id (free-text → id at the
        # boundary; raises on ambiguity) per the identifiers rule.
        params.append(AccountService(db).resolve_strict(account))
    if status != "all":
        sql += " AND status = ?"
        params.append(status)
    if since:
        sql += " AND assertion_date >= ?"
        params.append(since)
    sql += " ORDER BY drift_abs DESC"

    actions = [
        "Rerun reports(report_id='core:balance_drift', "
        "parameters={'account': '<name or id>'}) to filter to one account",
        "Rerun reports(report_id='core:balance_drift', "
        "parameters={'status': 'drift'}) to show drift rows",
    ]
    return ReportQuery(sql, params, actions=actions)
