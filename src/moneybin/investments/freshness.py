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
    INVESTMENT_EVENT_LEGS,
    INVESTMENT_EVENT_MEMBERS,
    INVESTMENT_OPENING_PROJECTION,
    INVESTMENT_SOURCE_PROJECTION,
    MODEL_FRESHNESS,
    STG_PLAID_INVESTMENT_HOLDINGS_SNAPSHOTS,
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


def projection_pending(
    db: Database, *, latest_raw_landing: datetime | None = None
) -> bool:
    """Require every dependent relation and every rebuild model's actual execution."""
    try:
        changed = latest_projection_change(db)
        cut_roots: set[str] = set()
        if changed is None:
            from moneybin.services.restate import _has_prod_environment  # pyright: ignore[reportPrivateUsage]  # same established SQLMesh presence boundary

            if not _has_prod_environment(db):
                return False
            cut_roots = {
                INVESTMENT_SOURCE_PROJECTION.full_name,
                INVESTMENT_OPENING_PROJECTION.full_name,
            }
        expected = (
            sqlmesh_registry.relations_downstream_of(
                FCT_INVESTMENT_TRANSACTIONS.full_name
            )
            | cut_roots
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
                f"SELECT model_name, last_executed_at FROM {MODEL_FRESHNESS.full_name}"  # noqa: S608  # fixed TableRef
            ).fetchall()
        )
        from moneybin.services.transform_service import TransformService

        landed = latest_raw_landing
        if landed is None:
            landed = TransformService(db)._max_raw_landed_at(  # pyright: ignore[reportPrivateUsage]  # reuse receipt/import scan for membership inputs
                for_models={
                    INVESTMENT_EVENT_LEGS.full_name,
                    INVESTMENT_SOURCE_PROJECTION.full_name,
                    INVESTMENT_OPENING_PROJECTION.full_name,
                    STG_PLAID_INVESTMENT_HOLDINGS_SNAPSHOTS.full_name,
                }
            )
        timestamps = [value for value in (changed, landed) if value is not None]
        if cut_roots:
            epochs = dict(
                db.execute(
                    f"SELECT model_name, last_changed_at FROM {MODEL_FRESHNESS.full_name}"  # noqa: S608  # fixed TableRef
                ).fetchall()
            )
            for root in cut_roots:
                epoch = epochs.get(root)
                if not isinstance(epoch, datetime):
                    return True
                timestamps.append(epoch)
        watermark = max(timestamps)
        # VIEW intervals record creation, not read-through recomputation. Their
        # presence is required; only their materialized ancestors owe execution.
        return any(name not in executions for name in expected) or any(
            executions.get(name) is None or executions[name] < watermark
            for name in rebuild
        )
    except duckdb.Error:
        return True


def _stale() -> UserError:
    return UserError(
        "Investment data needs rebuilding before it can be read.",
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
    try:
        relations = _read_relations(query)
    except SqlglotError as exc:
        raise _stale() from exc
    if not relations:
        return
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
        pending = list(relations)
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
