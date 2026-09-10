"""Regression guard for SQLMesh's declared external schemas."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import TypedDict, cast
from unittest.mock import MagicMock

import pytest
import yaml
from sqlglot import exp

from moneybin.database import Database, sqlmesh_context

_EXTERNAL_MODELS_PATH = (
    Path(__file__).parents[2] / "src" / "moneybin" / "sqlmesh" / "external_models.yaml"
)


class _ExternalModelDeclaration(TypedDict):
    name: str
    columns: dict[str, str]


def _load_external_models() -> list[_ExternalModelDeclaration]:
    """Load the fixed external-model declaration shape from YAML."""
    return cast(
        list[_ExternalModelDeclaration],
        yaml.safe_load(_EXTERNAL_MODELS_PATH.read_text(encoding="utf-8")),
    )


def _relation_name(name: str) -> str:
    """Normalize SQLMesh's quoted relation name for catalog comparison."""
    parts = name.replace('"', "").lower().split(".")
    return ".".join(parts[-2:])


def _data_type(data_type: str) -> str:
    """Normalize equivalent DuckDB type spellings before comparing schemas."""
    return exp.DataType.build(data_type, dialect="duckdb").sql(dialect="duckdb")


def _declared_external_models(
    expected_relations: set[str],
    declarations: list[tuple[str, dict[str, str]]],
) -> dict[str, dict[str, str]]:
    """Require a discovered dependency for every unique declaration."""
    assert expected_relations
    assert len({name for name, _ in declarations}) == len(declarations)
    return dict(declarations)


def test_external_model_guard_rejects_duplicate_declarations() -> None:
    """A duplicate declaration cannot be hidden by conversion to a dict."""
    with pytest.raises(AssertionError):
        _declared_external_models(
            {"raw.transactions"},
            [("raw.transactions", {}), ("raw.transactions", {})],
        )


def test_external_model_guard_rejects_no_discovered_dependencies() -> None:
    """The guard must inspect at least one SQLMesh raw/app dependency."""
    with pytest.raises(AssertionError):
        _declared_external_models(set(), [("raw.transactions", {})])


def test_external_models_match_initialized_raw_and_app_tables(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    """Every raw/app relation read by SQLMesh has its live column schema declared."""
    db = Database(
        tmp_path / "external_models.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    try:
        with sqlmesh_context(db) as context:
            expected_relations = {
                _relation_name(dependency)
                for model in context.models.values()
                if not model.kind.is_external
                for dependency in model.depends_on
                if _relation_name(dependency).startswith(("raw.", "app."))
            }
        declarations = [
            (_relation_name(str(entry["name"])), dict(entry["columns"]))
            for entry in _load_external_models()
            if _relation_name(str(entry["name"])).startswith(("raw.", "app."))
        ]
        declared = _declared_external_models(expected_relations, declarations)
        observed_columns: defaultdict[str, dict[str, str]] = defaultdict(dict)
        for relation, column, data_type in db.execute(
            """
            SELECT LOWER(schema_name || '.' || table_name), column_name, data_type
            FROM duckdb_columns()
            WHERE schema_name IN ('raw', 'app')
            """
        ).fetchall():
            observed_columns[relation][column] = _data_type(data_type)

        assert set(declared) == expected_relations
        assert {
            relation: {
                column: _data_type(data_type)
                for column, data_type in declared[relation].items()
            }
            for relation in expected_relations
        } == {relation: observed_columns[relation] for relation in expected_relations}
    finally:
        db.close()
