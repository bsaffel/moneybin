"""One crash-safe freshness boundary for membership-dependent financial reads."""

from collections.abc import Iterable
from datetime import datetime

import duckdb
from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.scope import traverse_scope

from moneybin import error_codes, sqlmesh_registry
from moneybin.database import Database
from moneybin.errors import RecoveryAction, UserError
from moneybin.tables import (
    ACCOUNT_SETTINGS,
    FCT_INVESTMENT_TRANSACTIONS,
    INVESTMENT_EVENT_MEMBERS,
)


def latest_projection_change(db: Database) -> datetime | None:
    """Retired rows and explicit source-choice edits still owe a rebuild."""
    row = db.execute(
        f"""SELECT MAX(changed_at) FROM (
            SELECT projection_changed_at AS changed_at FROM {INVESTMENT_EVENT_MEMBERS.full_name}
            UNION ALL
            SELECT s.investment_source_type_changed_at FROM {ACCOUNT_SETTINGS.full_name} AS s
            WHERE s.account_id IN (SELECT bound_account_id FROM {INVESTMENT_EVENT_MEMBERS.full_name})
        )"""  # noqa: S608  # fixed TableRefs
    ).fetchone()
    return row[0] if row else None


def projection_pending(db: Database) -> bool:
    """Require every dependent relation and every rebuild model's actual execution."""
    try:
        changed = latest_projection_change(db)
        if changed is None:
            return False
        expected = sqlmesh_registry.relations_downstream_of(
            FCT_INVESTMENT_TRANSACTIONS.full_name
        )
        rebuild = expected & sqlmesh_registry.materialized_model_names()
        if not rebuild:
            return True
        present = {
            f"{schema}.{name}"
            for schema, name in db.execute(
                "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = current_database()"
            ).fetchall()
        }
        if expected - present:
            return True
        executions = dict(
            db.execute(
                "SELECT model_name, last_executed_at FROM meta.model_freshness"
            ).fetchall()
        )
        # VIEW intervals record creation, not read-through recomputation. Their
        # presence is required; only their materialized ancestors owe execution.
        return any(name not in executions for name in expected) or any(
            executions.get(name) is None or executions[name] < changed
            for name in rebuild
        )
    except duckdb.Error:
        return True


def _stale() -> UserError:
    return UserError(
        "Investment decisions are saved, but dependent financial data needs rebuilding.",
        code=error_codes.REFRESH_STALE,
        hint="Run `moneybin refresh --step transform` before reading investment-dependent data.",
        recovery_actions=[
            RecoveryAction(
                tool="refresh_run",
                arguments={"steps": ["transform"]},
                rationale="Rebuild all financial projections from the saved membership.",
                confidence="suggested",
                idempotent=True,
            )
        ],
    )


def require_current_relations(db: Database, relations: Iterable[str]) -> None:
    """Refuse dependent reads while preserving unrelated and recovery surfaces."""
    dependent = sqlmesh_registry.relations_downstream_of(
        FCT_INVESTMENT_TRANSACTIONS.full_name
    )
    if dependent.intersection(
        name.lower() for name in relations
    ) and projection_pending(db):
        raise _stale()


def _read_relations(sql: str) -> set[str]:
    tree = parse_one(sql, read="duckdb")
    return {
        f"{source.db}.{source.name}".lower() if source.db else source.name.lower()
        for scope in traverse_scope(tree)
        for source in scope.sources.values()
        if isinstance(source, exp.Table)
    }


def require_current_query(db: Database, query: str) -> None:
    """Resolve query scopes and catalog views, including indirect and predicate reads."""
    if not projection_pending(db):
        return
    dependent = sqlmesh_registry.relations_downstream_of(
        FCT_INVESTMENT_TRANSACTIONS.full_name
    )
    try:
        views = {
            f"{schema}.{name}".lower(): sql
            for schema, name, sql in db.execute(
                "SELECT schema_name, view_name, sql FROM duckdb_views() WHERE database_name = current_database()"
            ).fetchall()
            if sql
        }
        pending = list(_read_relations(query))
        seen: set[str] = set()
        while pending:
            name = pending.pop()
            if name in seen:
                continue
            seen.add(name)
            if name in dependent or (
                "." not in name
                and any(item.rsplit(".", 1)[-1] == name for item in dependent)
            ):
                raise _stale()
            if name in views:
                pending.extend(_read_relations(views[name]))
    except (duckdb.Error, SqlglotError) as exc:
        # Unresolvable lineage cannot certify a financial read as current.
        raise _stale() from exc
