"""Complete stored-selection preflight for an equivalent Account merge."""

from collections import Counter
from dataclasses import replace
from decimal import Decimal
from importlib import resources
from typing import Literal

import duckdb

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.cost_basis import (
    LedgerEvent,
    compute_lot_id,
    compute_lots_and_gains,
)
from moneybin.investments.identity import manual_identity_sql
from moneybin.tables import (
    DIM_ACCOUNTS,
    FCT_INVESTMENT_LOTS,
    FCT_INVESTMENT_TRANSACTIONS,
    LOT_SELECTIONS,
    MANUAL_INVESTMENT_TRANSACTIONS,
    PLAID_INVESTMENT_HOLDINGS,
    PLAID_INVESTMENT_TRANSACTIONS,
)


def _refuse() -> UserError:
    return UserError(
        "Cannot merge investment identity: complete lot selection preservation "
        "cannot be proved from current investment models. Run a transform and "
        "correct unresolved selections or currency/transfer lineage before retrying.",
        code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
    )


def _current_plaid_sql() -> str:
    """Read the canonical Plaid views directly, including bootstrap dependencies."""
    names = (
        "stg_plaid__investment_transactions",
        "stg_plaid__investment_holdings_snapshots",
        "int_plaid__opening_positions",
        "stg_plaid__opening_lots",
    )
    queries: list[str] = []
    for name in names:
        model = (
            resources
            .files("moneybin")
            .joinpath(f"sqlmesh/models/prep/{name}.sql")
            .read_text()
        )
        query = model.split(";", 1)[1].strip().removesuffix(";")
        # Bind all dependencies to these live queries, never installed models
        # that may still reflect the preceding transform's definition or inputs.
        for dependency in names:
            query = query.replace(f"prep.{dependency}", dependency)
        queries.append(f"{name} AS ({query})")
    return f"""
        WITH {", ".join(queries)}, current_plaid AS (
            SELECT * FROM stg_plaid__investment_transactions WHERE ledger_include
        ), ledger AS (
            SELECT investment_transaction_id, account_id, security_id, trade_date,
                   original_acquisition_date, type, quantity, price, amount, fees,
                   currency_code
            FROM current_plaid
            UNION ALL
            SELECT investment_transaction_id, account_id, security_id, trade_date,
                   original_acquisition_date, type, quantity, price, amount, fees,
                   currency_code
            FROM stg_plaid__opening_lots
        )
        SELECT l.investment_transaction_id, l.account_id, l.security_id,
               l.trade_date, l.original_acquisition_date, l.type, l.quantity,
               l.price, l.amount, l.fees,
               COALESCE(l.currency_code, a.currency_code)
        FROM ledger AS l
        LEFT JOIN {DIM_ACCOUNTS.full_name} AS a ON a.account_id = l.account_id
    """  # noqa: S608  # fixed repository models and TableRef


def _validate_plaid_sources(
    db: Database,
    rows: list[tuple[object, ...]],
    dimension: Literal["account_id", "security_id"],
    provisional: str,
    survivor: str,
    selected_ids: set[str],
) -> None:
    """Require complete current inputs on both sides of each affected position."""
    try:
        # A profile without Plaid inputs needs no Plaid model dependencies.
        # Require both Raw branches and materialized Plaid to be empty; an
        # absent receipt or removed observation must not hide a stale Core row.
        if (
            not any(row[11] == "plaid" for row in rows)
            and not db.execute(
                f"""SELECT 1 FROM {PLAID_INVESTMENT_TRANSACTIONS.full_name}
                UNION ALL
                SELECT 1 FROM {PLAID_INVESTMENT_HOLDINGS.full_name}
                LIMIT 1"""  # noqa: S608  # TableRef constants; existence only
            ).fetchone()
        ):
            return
        current = db.execute(_current_plaid_sql()).fetchall()
    except (duckdb.CatalogException, duckdb.BinderException):
        raise _refuse() from None
    index = 1 if dimension == "account_id" else 2

    def position(row: tuple[object, ...]) -> tuple[object, object]:
        account, security = row[1:3]
        if index == 1 and account == provisional:
            account = survivor
        if index == 2 and security == provisional:
            security = survivor
        return account, security

    groups = {
        position(row)
        for row in [*rows, *current]
        if row[index] in (provisional, survivor)
    }
    if not any(
        str(row[0]) in selected_ids and position(row) in groups
        for row in [*rows, *current]
    ):
        return
    materialized = [row[:11] for row in rows if row[11] == "plaid"]
    affected_ids = {
        row[0] for row in [*materialized, *current] if position(row) in groups
    }
    # Multisets catch missing, newly arrived, rerouted and duplicated events.
    # Compare before applying the proposed merge so stale routes cannot pass
    # merely because both versions would end up on the same survivor.
    if Counter(row for row in materialized if row[0] in affected_ids) != Counter(
        row for row in current if row[0] in affected_ids
    ):
        raise _refuse()


def plan_account_lot_selections(
    db: Database,
    provisional: str,
    survivor: str,
) -> dict[str, list[tuple[str, Decimal]]]:
    """Map whole disposal selections or refuse before the first identity write."""
    if not db.execute(
        f"SELECT 1 FROM {LOT_SELECTIONS.full_name} LIMIT 1"  # noqa: S608  # TableRef constant; existence only
    ).fetchone():
        return {}
    try:
        rows = db.execute(
            f"""
            SELECT ls.investment_transaction_id, ls.lot_id, ls.quantity,
                   t.account_id, t.security_id, t.currency_code,
                   l.account_id, l.security_id, l.currency_code,
                   l.acquisition_date, l.source_transaction_id
            FROM {LOT_SELECTIONS.full_name} AS ls
            LEFT JOIN {FCT_INVESTMENT_TRANSACTIONS.full_name} AS t
              ON t.investment_transaction_id = ls.investment_transaction_id
            LEFT JOIN {FCT_INVESTMENT_LOTS.full_name} AS l ON l.lot_id = ls.lot_id
            ORDER BY ls.investment_transaction_id, ls.lot_id
            """,  # noqa: S608  # TableRef constants
        ).fetchall()
        currency_row = db.execute(
            f"SELECT currency_code FROM {DIM_ACCOUNTS.full_name} WHERE account_id = ?",  # noqa: S608  # TableRef and parameterized id
            [survivor],
        ).fetchone()
    except (duckdb.CatalogException, duckdb.BinderException):
        raise _refuse() from None
    if any(row[3] is None or row[6] is None for row in rows):
        raise _refuse()
    affected = {str(row[0]) for row in rows if provisional in (row[3], row[6])}
    current_manual = {
        str(row[0]): (row[1], row[2])
        for row in db.execute(
            f"""
            SELECT COALESCE(t.investment_transaction_id, t.source_transaction_id),
                   i.account_id, i.security_id
            FROM {MANUAL_INVESTMENT_TRANSACTIONS.full_name} AS t
            JOIN ({manual_identity_sql()}) AS i USING (source_transaction_id)
            """,  # noqa: S608  # TableRef and canonical repository query
        ).fetchall()
    }
    plan: dict[str, list[tuple[str, Decimal]]] = {}
    for row in rows:
        disposal, lot_id = str(row[0]), str(row[1])
        if (
            disposal in current_manual and current_manual[disposal] != (row[3], row[4])
        ) or (
            row[10] in current_manual and current_manual[row[10]] != (row[6], row[7])
        ):
            raise _refuse()
        if disposal not in affected:
            continue
        if currency_row is None or currency_row[0] is None:
            raise _refuse()
        target_currency = str(currency_row[0]).strip().upper()
        account = survivor if row[3] == provisional else row[3]
        lot_account = survivor if row[6] == provisional else row[6]
        if (
            account != lot_account
            or row[4] != row[7]
            or row[4] is None
            or any(
                value is None or str(value).strip().upper() != target_currency
                for value in (row[5], row[8])
            )
            or (
                disposal in current_manual
                and current_manual[disposal] != (row[3], row[4])
            )
            or (
                row[10] in current_manual
                and current_manual[row[10]] != (row[6], row[7])
            )
        ):
            raise _refuse()
        new_lot_id = lot_id
        if row[6] == provisional:
            if (
                row[9] is None
                or row[10] is None
                or lot_id
                != compute_lot_id(str(row[6]), str(row[7]), row[9], str(row[10]))
            ):
                raise _refuse()
            new_lot_id = compute_lot_id(survivor, str(row[7]), row[9], str(row[10]))
        selections = plan.setdefault(disposal, [])
        if any(existing == new_lot_id for existing, _quantity in selections):
            raise _refuse()
        selections.append((new_lot_id, Decimal(row[2])))
    result = {disposal: sorted(selections) for disposal, selections in plan.items()}
    validate_selection_quantities(db, "account_id", provisional, survivor, result)
    return result


def validate_selection_quantities(
    db: Database,
    dimension: Literal["account_id", "security_id"],
    provisional: str,
    survivor: str,
    replacements: dict[str, list[tuple[str, Decimal]]],
) -> None:
    """Replay complete merged positions and prove every stored lot remains selectable."""
    selection_rows = db.execute(
        f"SELECT investment_transaction_id, lot_id, quantity FROM {LOT_SELECTIONS.full_name} ORDER BY investment_transaction_id, lot_id",  # noqa: S608  # TableRef constant
    ).fetchall()
    if not selection_rows:
        return
    selections: dict[str, list[tuple[str, Decimal]]] = {}
    for disposal, lot, quantity in selection_rows:
        selections.setdefault(str(disposal), []).append((str(lot), Decimal(quantity)))
    selections.update(replacements)
    try:
        rows = db.execute(
            f"""
            SELECT investment_transaction_id, account_id, security_id, trade_date,
                   original_acquisition_date, type, quantity, price, amount, fees,
                   currency_code, source_type
            FROM {FCT_INVESTMENT_TRANSACTIONS.full_name}
            """,  # noqa: S608  # TableRef constant
        ).fetchall()
    except (duckdb.CatalogException, duckdb.BinderException):
        raise _refuse() from None
    _validate_plaid_sources(db, rows, dimension, provisional, survivor, set(selections))
    events: list[LedgerEvent] = []
    missing_trade_dates = {str(row[0]) for row in rows if row[3] is None}
    groups: set[tuple[str, str | None]] = set()
    for row in rows:
        if row[1] is None or row[2] is None:
            continue
        event = LedgerEvent(
            investment_transaction_id=str(row[0]),
            account_id=str(row[1]),
            security_id=str(row[2]),
            trade_date=row[3],
            original_acquisition_date=row[4],
            type=str(row[5]),
            quantity=row[6],
            price=row[7],
            amount=row[8],
            fees=row[9],
            currency_code=row[10],
        )
        original_identity = getattr(event, dimension)
        if original_identity == provisional:
            event = replace(event, **{dimension: survivor})
        if original_identity in (provisional, survivor):
            groups.add((event.account_id, event.security_id))
        events.append(event)
    events = [
        event for event in events if (event.account_id, event.security_id) in groups
    ]
    if not any(event.investment_transaction_id in selections for event in events):
        return
    if len({event.investment_transaction_id for event in events}) != len(events) or any(
        event.investment_transaction_id in missing_trade_dates for event in events
    ):
        raise _refuse()
    ledger_state = {
        str(row[0]): (row[1], row[2], row[3], row[5], row[6], row[8]) for row in rows
    }
    manual = db.execute(
        f"""
        SELECT COALESCE(t.investment_transaction_id, t.source_transaction_id),
               i.account_id, i.security_id, t.trade_date, t.type, t.quantity, t.amount
        FROM {MANUAL_INVESTMENT_TRANSACTIONS.full_name} AS t
        JOIN ({manual_identity_sql()}) AS i USING (source_transaction_id)
        """,  # noqa: S608  # TableRef and canonical repository query
    ).fetchall()
    for row in manual:
        account = (
            survivor if dimension == "account_id" and row[1] == provisional else row[1]
        )
        security = (
            survivor if dimension == "security_id" and row[2] == provisional else row[2]
        )
        if (account, security) in groups and ledger_state.get(str(row[0])) != tuple(
            row[1:]
        ):
            raise _refuse()
    # Both disposal types consume in the same engine order; sell additionally
    # returns the per-lot quantities needed to verify a transfer-out election.
    replay = [
        replace(event, type="sell") if event.type == "transfer_out" else event
        for event in events
    ]
    _lots, gains = compute_lots_and_gains(
        replay,
        method_for=lambda _account, _security: "specific",
        selections_for=lambda disposal: selections.get(disposal, []),
    )
    consumed: dict[tuple[str, str], Decimal] = {}
    for gain in gains:
        key = (gain.disposal_txn_id, gain.lot_id)
        consumed[key] = consumed.get(key, Decimal("0")) + gain.quantity
    for event in events:
        for lot, quantity in selections.get(event.investment_transaction_id, []):
            if (
                quantity <= 0
                or consumed.get((event.investment_transaction_id, lot), Decimal("0"))
                < quantity
            ):
                raise _refuse()
