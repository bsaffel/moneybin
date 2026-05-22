"""Tests for prefix-discipline validation."""

from pathlib import Path
from textwrap import dedent

from moneybin.packages._framework.prefix import (
    validate_cli_prefixes,
    validate_mcp_tool_prefixes,
    validate_schema_filenames,
    validate_sql_write_prefixes,
)


def _write_sql(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / name
    path.write_text(dedent(body).strip())
    return path


def test_sql_write_prefixes_unparseable_returns_violation(tmp_path: Path) -> None:
    """Unparseable SQL surfaces a PrefixViolation instead of crashing bootstrap."""
    sql = _write_sql(tmp_path, "bad.sql", "CREATE TABLE oops syntax ;;;")
    violations = validate_sql_write_prefixes(
        package_name="assets",
        owns_prefix="assets",
        sql_files=[sql],
    )
    assert len(violations) == 1
    assert violations[0].surface == "sql_write"
    assert "could not parse" in violations[0].message


def test_sql_writes_under_prefix_pass(tmp_path: Path) -> None:
    sql = _write_sql(
        tmp_path,
        "ok.sql",
        """
        CREATE TABLE app.assets_state (x INT);
        CREATE VIEW reports.assets_summary AS SELECT 1;
        """,
    )
    violations = validate_sql_write_prefixes(
        package_name="assets",
        owns_prefix="assets",
        sql_files=[sql],
    )
    assert violations == []


def test_sql_writes_outside_prefix_fail(tmp_path: Path) -> None:
    sql = _write_sql(
        tmp_path,
        "bad.sql",
        """
        CREATE TABLE app.assets_state (x INT);
        CREATE TABLE app.other_state (x INT);
        """,
    )
    violations = validate_sql_write_prefixes(
        package_name="assets",
        owns_prefix="assets",
        sql_files=[sql],
    )
    assert len(violations) == 1
    assert violations[0].offender == "app.other_state"
    assert violations[0].surface == "sql_write"


def test_schema_filenames_must_start_with_prefix(tmp_path: Path) -> None:
    ok = tmp_path / "raw_assets_imports.sql"
    bad = tmp_path / "raw_other_imports.sql"
    ok.touch()
    bad.touch()

    violations = validate_schema_filenames(
        package_name="assets",
        owns_prefix="assets",
        schema_files=[ok, bad],
    )
    assert len(violations) == 1
    assert "raw_other_imports.sql" in violations[0].offender


def test_mcp_tool_prefix_check() -> None:
    violations = validate_mcp_tool_prefixes(
        package_name="assets",
        owns_prefix="assets",
        tool_names=["assets_summary", "assets_set", "rogue_tool"],
    )
    assert len(violations) == 1
    assert violations[0].offender == "rogue_tool"
    assert violations[0].surface == "mcp_tool"


def test_cli_prefix_check() -> None:
    violations = validate_cli_prefixes(
        package_name="us_tax",
        owns_prefix="us_tax",
        cli_commands=["us-tax schedule-d", "rogue-command"],
    )
    # CLI prefix in kebab-case mirrors snake_case in Python per spec
    assert len(violations) == 1
    assert violations[0].offender == "rogue-command"
