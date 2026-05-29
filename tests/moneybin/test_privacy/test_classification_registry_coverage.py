"""Live catalog coverage for the privacy classification registry."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import CLASSIFICATION

_TARGET_SCHEMAS = {"app", "core"}


def _catalog_columns(db: Database) -> set[tuple[str, str, str]]:
    rows = db.execute(
        """
        SELECT schema_name, table_name, column_name
        FROM duckdb_columns()
        WHERE schema_name IN ('app', 'core')
        """
    ).fetchall()
    return {(schema, table, column) for schema, table, column in rows}


def _registry_columns() -> set[tuple[str, str, str]]:
    return {
        (schema, table, column)
        for (schema, table), columns in CLASSIFICATION.items()
        if schema in _TARGET_SCHEMAS
        for column in columns
    }


def _format_columns(columns: set[tuple[str, str, str]]) -> str:
    return "\n".join(
        f"- {schema}.{table}.{column}" for schema, table, column in sorted(columns)
    )


def test_classification_registry_covers_every_app_and_core_column(
    schema_catalog_db: Database,
) -> None:
    """Every live app/core column must have a privacy classification."""
    missing = _catalog_columns(schema_catalog_db) - _registry_columns()

    assert not missing, (
        f"CLASSIFICATION is missing live catalog columns:\n{_format_columns(missing)}"
    )


def test_classification_registry_has_no_stale_app_or_core_columns(
    schema_catalog_db: Database,
) -> None:
    """Registry entries must point at columns that still exist."""
    stale = _registry_columns() - _catalog_columns(schema_catalog_db)

    assert not stale, (
        "CLASSIFICATION contains stale columns not present in the catalog:\n"
        f"{_format_columns(stale)}"
    )
