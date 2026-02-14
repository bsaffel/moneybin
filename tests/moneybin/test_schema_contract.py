"""Schema contract tests — verify SQL DDL files stay in sync with dbt models.

The core SQL DDL files (src/moneybin/sql/schema/core_*.sql) are used by MCP
test fixtures to create tables with the correct schema.  The dbt YAML files
(dbt/models/core/*.yml) are the authoritative column spec for each core model.

These tests catch drift between the two so that a dbt model change that
adds, removes, or renames a column immediately fails CI until the
corresponding DDL file is updated to match.
"""

import re
from pathlib import Path

import pytest
import yaml

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DBT_MODELS_DIR = _PROJECT_ROOT / "dbt" / "models" / "core"
_SCHEMA_DIR = _PROJECT_ROOT / "src" / "moneybin" / "sql" / "schema"

# (dbt YAML file, SQL DDL file) for each core model
_CORE_MODELS = [
    ("dim_accounts.yml", "core_dim_accounts.sql"),
    ("fct_transactions.yml", "core_fct_transactions.sql"),
]


def _parse_dbt_yaml_columns(yaml_path: Path) -> list[str]:
    """Extract column names from a dbt model YAML file.

    Args:
        yaml_path: Path to the dbt YAML schema file.

    Returns:
        Ordered list of column names defined in the YAML.
    """
    data = yaml.safe_load(yaml_path.read_text())
    columns: list[dict[str, str]] = data["models"][0]["columns"]
    return [col["name"] for col in columns]


def _parse_ddl_columns(sql_path: Path) -> list[str]:
    """Extract column names from a CREATE TABLE DDL file.

    Expects the standard format used in ``src/moneybin/sql/schema/``::

        CREATE TABLE IF NOT EXISTS schema.table (
            column_name TYPE [constraints],
            ...
        );

    Args:
        sql_path: Path to the SQL DDL file.

    Returns:
        Ordered list of column names defined in the DDL.
    """
    text = sql_path.read_text()
    paren_match = re.search(r"\((.*)\)", text, re.DOTALL)
    if not paren_match:
        return []

    columns: list[str] = []
    for line in paren_match.group(1).strip().splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        match = re.match(r"(\w+)\s+\w+", line)
        if match:
            columns.append(match.group(1))
    return columns


@pytest.mark.unit
@pytest.mark.parametrize(
    ("dbt_yaml_file", "ddl_file"),
    _CORE_MODELS,
    ids=[pair[0].removesuffix(".yml") for pair in _CORE_MODELS],
)
def test_ddl_columns_match_dbt_yaml(dbt_yaml_file: str, ddl_file: str) -> None:
    """Verify SQL DDL column names match the dbt YAML model spec.

    This contract test ensures the hand-maintained DDL files used by MCP
    test fixtures stay in sync with the authoritative dbt model definitions.
    If this test fails, update the DDL file to match the dbt YAML.
    """
    yaml_path = _DBT_MODELS_DIR / dbt_yaml_file
    sql_path = _SCHEMA_DIR / ddl_file

    dbt_columns = set(_parse_dbt_yaml_columns(yaml_path))
    ddl_columns = set(_parse_ddl_columns(sql_path))

    in_dbt_not_ddl = sorted(dbt_columns - ddl_columns)
    in_ddl_not_dbt = sorted(ddl_columns - dbt_columns)

    errors: list[str] = []
    if in_dbt_not_ddl:
        errors.append(f"Columns in dbt YAML but missing from DDL: {in_dbt_not_ddl}")
    if in_ddl_not_dbt:
        errors.append(f"Columns in DDL but missing from dbt YAML: {in_ddl_not_dbt}")

    assert not errors, (
        f"Schema drift detected between {dbt_yaml_file} and {ddl_file}:\n"
        + "\n".join(f"  - {e}" for e in errors)
        + f"\n\nUpdate {ddl_file} to match the dbt model definition."
    )
