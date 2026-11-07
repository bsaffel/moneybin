"""Read-only temporal-contract inspection for encrypted Database startup."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timedelta
from functools import cache
from typing import TYPE_CHECKING, Annotated, Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import duckdb
import sqlglot
from pydantic import ConfigDict, PlainSerializer, TypeAdapter
from pydantic.dataclasses import dataclass
from sqlglot import expressions as exp

from moneybin.tables import (
    OFX_BALANCES,
    OFX_TRANSACTIONS,
    PROFILE_SETTINGS,
    UTC_UPGRADE_PROVENANCE,
    VERSIONS,
)
from moneybin.temporal import as_utc, format_utc, interpret_legacy_time, parse_utc

if TYPE_CHECKING:
    from moneybin.database import Database
    from moneybin.services.audit_service import AuditEvent

TemporalContract = Literal["empty", "legacy", "incomplete", "utc"]


class UTCUpgradeRequiredError(Exception):
    """The profile does not satisfy the maintained UTC temporal contract."""


@cache
def _expected_instant_columns() -> frozenset[tuple[str, str, str]]:
    """Discover maintained instant fields from current bootstrap DDL."""
    from moneybin.schema import (
        _all_schema_files,  # pyright: ignore[reportPrivateUsage]  # canonical bootstrap inventory
    )

    columns: set[tuple[str, str, str]] = set()
    for path in _all_schema_files():
        for statement in sqlglot.parse(path.read_text(), dialect="duckdb"):
            if not isinstance(statement, exp.Create) or statement.kind != "TABLE":
                continue
            table = statement.find(exp.Table)
            if table is None:
                continue
            for column in statement.find_all(exp.ColumnDef):
                kind = column.args.get("kind")
                if kind is not None and kind.sql(dialect="duckdb") == "TIMESTAMPTZ":
                    columns.add((table.db, table.name, column.name))
    return frozenset(columns)


def _base_tables(connection: duckdb.DuckDBPyConnection) -> set[tuple[str, str]]:
    return set(
        connection.execute(
            """
            SELECT schema_name, table_name FROM duckdb_tables()
            WHERE database_name = current_database() AND NOT internal
            """
        ).fetchall()
    )


def _utc_schema_complete(connection: duckdb.DuckDBPyConnection) -> bool:
    columns = connection.execute(
        """
        SELECT schema_name, table_name, column_name, data_type
        FROM duckdb_columns()
        WHERE database_name = current_database() AND NOT internal
        """
    ).fetchall()
    tables = _base_tables(connection)
    aware = {
        (schema, table, column)
        for schema, table, column, kind in columns
        if kind == "TIMESTAMP WITH TIME ZONE" and (schema, table) in tables
    }
    # Include nested and dynamically-created types; an undeclared naive instant
    # cannot inherit the contract merely because declared fields are correct.
    naive = any(
        re.search(r"\bTIMESTAMP(?:_NS|_MS|_S)?\b(?! WITH TIME ZONE)", kind)
        for _schema, _table, _column, kind in columns
    )
    dates = {
        (schema, table, column)
        for schema, table, column, kind in columns
        if kind == "DATE" and (schema, table) in tables
    }
    source_dates = {
        (OFX_TRANSACTIONS.schema, OFX_TRANSACTIONS.name, "date_posted_source_date"),
        (OFX_BALANCES.schema, OFX_BALANCES.name, "statement_start_source_date"),
        (OFX_BALANCES.schema, OFX_BALANCES.name, "statement_end_source_date"),
        (OFX_BALANCES.schema, OFX_BALANCES.name, "ledger_balance_source_date"),
    }
    return not naive and _expected_instant_columns() <= aware and source_dates <= dates


def inspect_temporal_contract(
    connection: duckdb.DuckDBPyConnection,
) -> TemporalContract:
    """Read only catalog/type/marker evidence, without interpreting old values."""
    objects = connection.execute(
        """
        SELECT schema_name, table_name FROM duckdb_tables()
        WHERE database_name = current_database() AND NOT internal
        UNION ALL
        SELECT schema_name, view_name FROM duckdb_views()
        WHERE database_name = current_database() AND NOT internal
        UNION ALL
        SELECT schema_name, sequence_name FROM duckdb_sequences()
        WHERE database_name = current_database()
        """
    ).fetchall()
    schemas = connection.execute(
        """
        SELECT schema_name FROM duckdb_schemas()
        WHERE database_name = current_database() AND NOT internal
          AND schema_name <> 'main'
        """
    ).fetchall()
    if not objects and not schemas:
        return "empty"

    version_columns = connection.execute(
        """
        SELECT column_name, data_type FROM duckdb_columns()
        WHERE database_name = current_database()
          AND schema_name = ? AND table_name = ?
        """,
        [VERSIONS.schema, VERSIONS.name],
    ).fetchall()
    types = dict(version_columns)
    if types.get("component") != "VARCHAR" or types.get("version") != "VARCHAR":
        return "legacy"
    if (VERSIONS.schema, VERSIONS.name) not in _base_tables(connection):
        return "incomplete"
    marker = connection.execute(
        f"""SELECT version FROM {VERSIONS.full_name} WHERE component = ?""",  # noqa: S608  # TableRef constant
        ["utc_contract"],
    ).fetchall()
    if not marker:
        return "legacy"
    if marker != [("2",)] or not _utc_schema_complete(connection):
        return "incomplete"
    if not _reporting_policy_complete(connection):
        return "incomplete"
    return "utc"


def _reporting_policy_complete(connection: duckdb.DuckDBPyConnection) -> bool:
    """An UTC warehouse must retain its explicitly selected reporting calendar."""
    from moneybin.repositories.profile_settings_repo import validate_reporting_timezone

    try:
        rows = connection.execute(
            f"SELECT scope, reporting_timezone FROM {PROFILE_SETTINGS.full_name}"  # noqa: S608  # TableRef constant
        ).fetchall()
        if len(rows) != 1 or rows[0][0] != "profile" or not isinstance(rows[0][1], str):
            return False
        validate_reporting_timezone(rows[0][1])
    except (duckdb.BinderException, duckdb.CatalogException, ValueError):
        return False
    return True


def certify_fresh_temporal_contract(connection: duckdb.DuckDBPyConnection) -> None:
    """Certify a genuinely empty Database bootstrap only after complete init."""
    from moneybin.repositories.utc_upgrade_provenance_repo import (
        UTCUpgradeProvenanceRepo,
    )

    if not _utc_schema_complete(connection) or not _reporting_policy_complete(
        connection
    ):
        raise UTCUpgradeRequiredError(
            "Fresh initialization produced an incomplete UTC schema; the profile "
            "requires separately arranged offline maintenance before it can be opened."
        )
    UTCUpgradeProvenanceRepo(connection).initialize_fresh()
    connection.execute(
        f"""INSERT INTO {VERSIONS.full_name} (component, version) VALUES (?, ?)""",  # noqa: S608  # TableRef constant
        ["utc_contract", "2"],
    )


@dataclass(frozen=True, config=ConfigDict(extra="forbid", strict=True))
class LegacyWriterEvidence:
    """Human-reviewed historical attribution, bound to the inspected source."""

    source_fingerprint: str
    schema_fingerprint: str
    writer_id: str
    historical_zone_reference: str


@dataclass(frozen=True, config=ConfigDict(extra="forbid", strict=True))
class LegacyTimeRule:
    """Accepted writer provenance for a fixed instant column and audit era.

    Bounds select already-converted audit occurred_at, not the captured value.
    Offline maintenance must establish writer/zone/fold attribution before
    persisting a rule;
    a date range alone is not evidence. Typed-column migration needs its own
    proven row attribution or uniform column provenance, not a guessed
    recorded_at column. These rules never discover provenance themselves.
    """

    schema: str
    table: str
    column: str
    writer_era: Literal["utc-naive", "local-naive"]
    recorded_from: Annotated[datetime, PlainSerializer(format_utc, when_used="json")]
    recorded_until: Annotated[datetime, PlainSerializer(format_utc, when_used="json")]
    zone: str
    fold: Literal[0, 1] | None = None
    evidence: LegacyWriterEvidence | None = None
    audit_id: str | None = None
    image_side: Literal["before", "after"] | None = None

    def __post_init__(self) -> None:
        """Validate accepted evidence shape without inferring source semantics."""
        if (self.audit_id is None) != (self.image_side is None):
            raise ValueError("Legacy image provenance requires audit_id and image_side")
        if self.audit_id is not None and not self.audit_id.strip():
            raise ValueError("Legacy image provenance requires a nonempty audit_id")
        if (self.schema, self.table, self.column) not in _expected_instant_columns():
            raise ValueError("Legacy rule requires a maintained instant column")
        for bound in (self.recorded_from, self.recorded_until):
            if bound.utcoffset() != timedelta(0):
                raise ValueError("Legacy writer era bounds require aware UTC instants")
        if self.recorded_from >= self.recorded_until:
            raise ValueError("Legacy writer era must be a nonempty half-open interval")
        try:
            ZoneInfo(self.zone)
        except (ValueError, ZoneInfoNotFoundError):
            raise ValueError("Invalid legacy timezone") from None
        if self.writer_era == "utc-naive" and (
            self.zone != "UTC" or self.fold is not None
        ):
            raise ValueError("UTC-naive provenance requires UTC without fold evidence")


@dataclass(frozen=True, config=ConfigDict(extra="forbid", strict=True))
class AuditTemporalPolicy:
    """Immutable accepted provenance; absent rules never license naive replay."""

    rules: tuple[LegacyTimeRule, ...]


def read_audit_temporal_policy(db: Database) -> AuditTemporalPolicy:
    """Read migration-owned provenance without changing historical evidence."""
    table = UTC_UPGRADE_PROVENANCE
    if (table.schema, table.name) not in _base_tables(db.conn):
        return AuditTemporalPolicy(rules=())
    rows = db.execute(
        f"SELECT contract_version, rules FROM {table.full_name}"  # noqa: S608  # TableRef constant
    ).fetchall()
    return parse_audit_temporal_policy(rows)


def parse_audit_temporal_policy(rows: Sequence[Sequence[Any]]) -> AuditTemporalPolicy:
    """Validate persisted provenance identically for Database and SQLMesh reads."""
    if len(rows) != 1 or rows[0][0] != 2:
        raise ValueError(
            "Audit temporal provenance requires exactly one supported version"
        )
    rules = TypeAdapter(tuple[LegacyTimeRule, ...]).validate_json(rows[0][1])
    return AuditTemporalPolicy(rules=rules)


def decode_audit_row(
    event: AuditEvent,
    image: Mapping[str, object],
    policy: AuditTemporalPolicy,
    *,
    image_side: Literal["before", "after"],
) -> dict[str, object]:
    """Decode only known instant fields in a copy, before any replay mutation."""
    decoded = deepcopy(dict(image))
    for column, value in image.items():
        key = (event.target_schema, event.target_table, column)
        if key not in _expected_instant_columns() or value is None:
            continue
        if isinstance(value, datetime):
            instant = value
        elif isinstance(value, str):
            try:
                instant = datetime.fromisoformat(value)
            except ValueError:
                raise ValueError("Audit instant is not an ISO datetime") from None
        else:
            raise ValueError("Audit instant must be an ISO datetime")
        if instant.utcoffset() is not None:
            decoded[column] = as_utc(instant)
            continue
        try:
            recorded_at = parse_utc(event.occurred_at)
        except ValueError:
            raise ValueError(
                "Legacy replay requires an aware audit occurrence"
            ) from None
        matches = [
            rule
            for rule in policy.rules
            if (rule.schema, rule.table, rule.column) == key
            and rule.recorded_from <= recorded_at < rule.recorded_until
            and (
                rule.audit_id is None
                or (rule.audit_id == event.audit_id and rule.image_side == image_side)
            )
        ]
        if len(matches) != 1:
            raise ValueError(
                "Legacy audit instant requires exactly one provenance rule"
            )
        rule = matches[0]
        decoded[column] = interpret_legacy_time(
            instant, ZoneInfo(rule.zone), fold=rule.fold
        )
    return decoded
