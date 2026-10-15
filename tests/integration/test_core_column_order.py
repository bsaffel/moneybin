"""Rule A guard: every ``core`` relation projects ids → strings → numerics → booleans → dates → timestamps.

SQLMesh resolves ``core`` column types as UNKNOWN offline, so the guard reads
real types from a built database. See ``.claude/rules/column-ordering.md``.
"""

from __future__ import annotations

import re
from collections.abc import Generator
from itertools import pairwise
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.database import Database
from moneybin.services.transform_service import TransformService
from tests.integration.conftest import make_secret_store

_CORE_MODELS_DIR = (
    Path(__file__).parents[2] / "src" / "moneybin" / "sqlmesh" / "models" / "core"
)
_CORE_MODELS = sorted(
    p.stem for p in _CORE_MODELS_DIR.iterdir() if p.suffix in {".sql", ".py"}
)

_RANKS = ("id", "string", "numeric", "boolean", "date", "timestamp")

# `*_id` columns that are lookup values rather than keys, so they rank as strings.
_NON_KEY_IDS = {("dim_securities", "coingecko_id")}

_NUMERIC = re.compile(
    r"(DECIMAL|TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|UTINYINT|USMALLINT"
    r"|UINTEGER|UBIGINT|FLOAT|DOUBLE)\b"
)
# Nested types carry no Rule A rank; they sit beside the column they belong with.
_NESTED = re.compile(r"(\[\]$|^STRUCT\(|^MAP\()")


def _rank(table: str, column: str, data_type: str) -> str | None:
    if column.endswith("_id") and (table, column) not in _NON_KEY_IDS:
        return "id"
    if _NESTED.search(data_type):
        return None
    if data_type == "VARCHAR":
        return "string"
    if _NUMERIC.match(data_type):
        return "numeric"
    if data_type == "BOOLEAN":
        return "boolean"
    if data_type == "DATE":
        return "date"
    if data_type.startswith("TIMESTAMP"):
        return "timestamp"
    raise AssertionError(
        f"core.{table}.{column} has type {data_type}, which Rule A does not rank; "
        "classify it in this guard"
    )


@pytest.fixture(scope="module")
def core_columns(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[dict[str, list[tuple[str, str]]], None, None]:
    """Build every SQLMesh model once and return each core relation's columns in order."""
    clear_settings_cache()
    set_current_profile("test")
    db_path = tmp_path_factory.mktemp("core_column_order") / "core.duckdb"
    with pytest.MonkeyPatch.context() as mp:
        settings = MagicMock()
        settings.database.path = db_path
        mp.setattr("moneybin.database.get_settings", lambda: settings)
        db = Database(db_path, secret_store=make_secret_store(), read_only=False)
        try:
            TransformService(db).apply()
            rows = db.execute(
                """
                SELECT table_name, column_name, data_type
                FROM duckdb_columns()
                WHERE schema_name = 'core'
                ORDER BY table_name, column_index
                """
            ).fetchall()
        finally:
            db.close()
    columns: dict[str, list[tuple[str, str]]] = {}
    for table, column, data_type in rows:
        columns.setdefault(table, []).append((column, data_type))
    yield columns
    clear_settings_cache()


@pytest.mark.integration
@pytest.mark.slow
def test_every_core_model_is_built_and_checked(
    core_columns: dict[str, list[tuple[str, str]]],
) -> None:
    """The guard covers exactly the core model files, so none escapes the order check."""
    assert sorted(core_columns) == _CORE_MODELS


@pytest.mark.integration
@pytest.mark.slow
def test_non_key_id_exemptions_name_real_columns(
    core_columns: dict[str, list[tuple[str, str]]],
) -> None:
    """A stale exemption would silently stop guarding nothing; fail it instead."""
    present = {
        (table, column) for table, cols in core_columns.items() for column, _ in cols
    }
    assert _NON_KEY_IDS <= present


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.parametrize("table", _CORE_MODELS)
def test_core_projection_follows_rule_a(
    table: str, core_columns: dict[str, list[tuple[str, str]]]
) -> None:
    """Each ranked column's category is never lower than the one before it."""
    ranked = [
        (column, rank)
        for column, data_type in core_columns[table]
        if (rank := _rank(table, column, data_type)) is not None
    ]
    out_of_order = [
        f"{column} ({rank}) after {prev_column} ({prev_rank})"
        for (prev_column, prev_rank), (column, rank) in pairwise(ranked)
        if _RANKS.index(rank) < _RANKS.index(prev_rank)
    ]
    assert not out_of_order, (
        f"core.{table} breaks Rule A ({' → '.join(_RANKS)}): " + "; ".join(out_of_order)
    )
