"""Reconstruct opening rows from retained snapshots and selected membership."""

from collections.abc import Mapping, Sequence
from typing import Any

from sqlglot import exp

from moneybin.database import Database
from moneybin.investments.event_projection import (
    project_members,
    read_rows,
    source_revision,
)
from moneybin.investments.opening_sql import model_query, opening_query
from moneybin.tables import (
    PLAID_INVESTMENT_HOLDINGS_SNAPSHOTS,
    STG_PLAID_INVESTMENT_TRANSACTIONS,
)

# Stored provenance references immutable rows; financial values remain in Raw.
ALGORITHM_VERSION = "opening_membership_v1"


def _relation(
    rows: Sequence[Mapping[str, Any]], columns: Mapping[str, str]
) -> tuple[str, list[Any]]:
    if not rows:
        return "SELECT " + ", ".join(
            f"NULL::{kind} AS {name}" for name, kind in columns.items()
        ) + " WHERE FALSE", []
    placeholders = (
        "(" + ", ".join(f"CAST(? AS {kind})" for kind in columns.values()) + ")"
    )
    return (
        "SELECT * FROM (VALUES "  # noqa: S608  # fixed columns, all values are placeholders
        + ", ".join(placeholders for _ in rows)
        + ") AS v("
        + ", ".join(columns)
        + ")",
        [row[name] for row in rows for name in columns],
    )


def first_snapshots(db: Database) -> list[dict[str, Any]]:
    """Rank only holdings-bearing receipts, independently for each native account."""
    query = model_query("int_plaid__opening_positions")
    first = next(
        cte.this
        for cte in query.find_all(exp.CTE)
        if cte.alias_or_name == "first_snapshot"
    )
    sql = first.sql(dialect="duckdb").replace(
        "prep.stg_plaid__investment_holdings_snapshots",
        PLAID_INVESTMENT_HOLDINGS_SNAPSHOTS.full_name,
    )
    return read_rows(db, sql)


def reconstruct_openings(
    db: Database,
    members: Sequence[Mapping[str, Any]],
    evidence: Mapping[tuple[str, str, str, str], Mapping[str, Any]],
    resolutions: Sequence[Mapping[str, Any]],
    *,
    snapshots: Sequence[Mapping[str, Any]] | None = None,
    guards: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str, str], dict[str, Any]]]:
    """Reuse canonical opening SQL with its transaction input bound to exact choices."""
    snapshots = first_snapshots(db) if snapshots is None else snapshots
    if not snapshots:
        return [], {}
    selected = {
        row["investment_transaction_id"]: row
        for row in project_members(members, evidence, resolutions)
    }
    transactions: list[dict[str, Any]] = []
    for member in members:
        if member["source_type"] != "plaid":
            continue
        observed = evidence[source_revision(member)]
        chosen = selected[member["investment_transaction_id"]]
        transactions.append({
            **observed,
            **{key: chosen[key] for key in ("quantity", "type", "trade_date")},
        })
    if guards is None:
        guards = read_rows(
            db,
            f"SELECT source_type, source_origin, investment_transaction_id AS native_reference, observation_version FROM {STG_PLAID_INVESTMENT_TRANSACTIONS.full_name} WHERE type = 'split'",  # noqa: S608  # TableRef constant
        )  # TableRef constant
    for guard in guards:
        transactions.append(dict(evidence[source_revision(guard)]))
    tx_sql, parameters = _relation(
        transactions,
        {
            "source_account_key": "VARCHAR",
            "source_security_key": "VARCHAR",
            "source_origin": "VARCHAR",
            "quantity": "DECIMAL(28, 10)",
            "type": "VARCHAR",
            "trade_date": "DATE",
            "ledger_include": "BOOLEAN",
        },
    )
    snapshot_sql, snapshot_params = _relation(
        snapshots,
        {"account_id": "VARCHAR", "source_origin": "VARCHAR", "source_file": "VARCHAR"},
    )
    rows = read_rows(
        db,
        opening_query(tx_sql, snapshot_sql).sql(dialect="duckdb"),
        parameters + snapshot_params,
    )
    contexts: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            row["source_account_key"],
            row["source_security_key"],
            row["source_origin"],
        )
        leg_ids = {
            m["investment_transaction_id"]
            for m in members
            if m["source_type"] == "plaid"
            and tuple(
                evidence[source_revision(m)][k]
                for k in ("source_account_key", "source_security_key", "source_origin")
            )
            == key
            and row["reconstruction_window_start"]
            <= selected[m["investment_transaction_id"]]["trade_date"]
            <= row["reconstruction_snapshot_date"]
        }
        contexts[key] = {
            "algorithm_version": ALGORITHM_VERSION,
            "snapshot": {
                "account_id": key[0],
                "source_origin": key[2],
                "source_file": row["first_snapshot_source_file"],
            },
            "source_member_ids": sorted(
                m["membership_id"]
                for m in members
                if m["investment_transaction_id"] in leg_ids
            ),
            "resolution_ids": sorted(
                r["resolution_id"]
                for r in resolutions
                if r["investment_transaction_id"] in leg_ids
            ),
            "split_revisions": [
                dict(g)
                for g in guards
                if tuple(
                    evidence[source_revision(g)][k]
                    for k in (
                        "source_account_key",
                        "source_security_key",
                        "source_origin",
                    )
                )
                == key
                and row["reconstruction_window_start"]
                <= evidence[source_revision(g)]["trade_date"]
                <= row["reconstruction_snapshot_date"]
            ],
        }
    return rows, contexts
