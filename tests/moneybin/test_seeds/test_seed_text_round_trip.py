"""Every TEXT seed column must load exactly as its CSV spells it.

SQLMesh reads seed CSVs with pandas type inference, then casts declared TEXT
columns back to strings. An all-digit column round-trips through int64, so a
value like ``0123`` would silently load as ``'123'``. The FutureWarning that
inference raises is filtered at runtime (``moneybin.database``), so this test is
the guard on the data itself.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest
from sqlglot import exp
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import SeedModel, load_sql_based_model

from moneybin.database import SQLMESH_ROOT

_SEED_SQL = sorted((SQLMESH_ROOT / "models" / "seeds").glob("*.sql"))


@pytest.mark.parametrize("sql_path", _SEED_SQL, ids=lambda p: p.stem)
def test_text_seed_values_match_csv(sql_path: Path) -> None:
    model = load_sql_based_model(
        parse(sql_path.read_text(), default_dialect="duckdb"),
        path=sql_path,
        dialect="duckdb",
    )
    assert isinstance(model, SeedModel)
    text_columns = [
        name
        for name, dtype in (model.columns_to_types or {}).items()
        if dtype.is_type(*exp.DataType.TEXT_TYPES)
    ]
    seed_path: Path = model.seed_path
    with seed_path.open(newline="") as f:
        raw_rows = list(csv.DictReader(f))
    loaded = next(model.render_seed())  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]  # sqlmesh types the seed frame loosely
    assert isinstance(loaded, pd.DataFrame)
    loaded_rows: list[dict[str, object]] = loaded.to_dict("records")  # pyright: ignore[reportAssignmentType, reportUnknownMemberType]  # pandas types keys as Hashable

    assert len(loaded_rows) == len(raw_rows)
    mismatches = [
        (i, column, raw[column], loaded[column])
        for i, (raw, loaded) in enumerate(zip(raw_rows, loaded_rows, strict=True))
        for column in text_columns
        if (raw[column] or None) != loaded[column]
    ]
    assert not mismatches, mismatches
