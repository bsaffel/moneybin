"""Parse SQLMesh models off disk with the project's macros, and no Context."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import TYPE_CHECKING

from moneybin.database import SQLMESH_ROOT

if TYPE_CHECKING:
    from sqlmesh.core.model import Model


def load_model(path: Path) -> Model:
    """Load one ``.sql`` model the way SQLMesh would, minus the database.

    A Context registers ``macros/*.py`` before it loads models; without that a
    model calling a project macro loads but cannot render. Importing each macro
    module runs its ``@macro()`` decorator, which fills the default registry
    ``load_sql_based_model`` reads.
    """
    # Imported here, not at module scope: importing SQLMesh changes how sqlglot
    # parses `$name` process-wide, so collection must not trigger it.
    from sqlmesh.core.dialect import parse as sqlmesh_parse
    from sqlmesh.core.model import load_sql_based_model

    for macro_file in sorted((SQLMESH_ROOT / "macros").glob("*.py")):
        if macro_file.stem != "__init__":
            importlib.import_module(f"moneybin.sqlmesh.macros.{macro_file.stem}")
    return load_sql_based_model(
        sqlmesh_parse(path.read_text(), default_dialect="duckdb"),
        path=path,
        dialect="duckdb",
    )


def rendered_model_sql(path: Path) -> str:
    """One model's query as executable DuckDB SQL, macros expanded."""
    return load_model(path).render_query_or_raise().sql(dialect="duckdb")
