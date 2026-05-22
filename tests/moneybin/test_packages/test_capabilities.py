"""Tests for capability declarations and the write-glob validator."""

from pathlib import Path
from textwrap import dedent

from moneybin.packages._framework.capabilities import (
    CapabilityViolation,
    is_write_allowed,
    validate_writes,
)
from moneybin.packages._framework.manifest import CapabilityDeclarations


def _make_sql_dir(tmp_path: Path, files: dict[str, str]) -> Path:
    sql_dir = tmp_path / "schema"
    sql_dir.mkdir()
    for name, body in files.items():
        (sql_dir / name).write_text(dedent(body).strip())
    return sql_dir


def test_writes_inside_declared_glob_pass(tmp_path: Path) -> None:
    """A CREATE TABLE matching the declared glob is accepted."""
    sql_dir = _make_sql_dir(
        tmp_path,
        {
            "app_test_state.sql": """
                CREATE TABLE IF NOT EXISTS app.test_synthetic_state (
                    id TEXT PRIMARY KEY
                );
            """,
        },
    )
    capability = CapabilityDeclarations(
        writes=["app.test_synthetic_*", "reports.test_synthetic_*"],
    )

    violations = validate_writes(
        package_name="test_synthetic",
        sql_files=list(sql_dir.glob("*.sql")),
        capability=capability,
    )

    assert violations == []


def test_writes_outside_declared_glob_fail(tmp_path: Path) -> None:
    """A CREATE TABLE outside every declared glob raises CapabilityViolation."""
    sql_dir = _make_sql_dir(
        tmp_path,
        {
            "leak.sql": """
                CREATE TABLE IF NOT EXISTS app.test_synthetic_state (
                    id TEXT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS core.fct_transactions (
                    id TEXT
                );
            """,
        },
    )
    capability = CapabilityDeclarations(writes=["app.test_synthetic_*"])

    violations = validate_writes(
        package_name="test_synthetic",
        sql_files=list(sql_dir.glob("*.sql")),
        capability=capability,
    )

    assert len(violations) == 1
    violation = violations[0]
    assert isinstance(violation, CapabilityViolation)
    assert violation.target == "core.fct_transactions"
    assert "leak.sql" in violation.sql_file


def test_glob_supports_wildcards(tmp_path: Path) -> None:
    """The 'reports.assets_*' style globs match any suffix."""
    sql_dir = _make_sql_dir(
        tmp_path,
        {
            "assets.sql": """
                CREATE VIEW reports.assets_summary AS SELECT 1;
                CREATE VIEW reports.assets_net_worth_contribution AS SELECT 1;
            """,
        },
    )
    capability = CapabilityDeclarations(writes=["reports.assets_*"])

    violations = validate_writes(
        package_name="assets",
        sql_files=list(sql_dir.glob("*.sql")),
        capability=capability,
    )
    assert violations == []


def test_multiple_globs_combine(tmp_path: Path) -> None:
    """A CREATE matching ANY declared glob is accepted."""
    sql_dir = _make_sql_dir(
        tmp_path,
        {
            "a.sql": "CREATE TABLE app.assets_one (x INT);",
            "b.sql": "CREATE VIEW reports.assets_two AS SELECT 1;",
            "c.sql": "CREATE TABLE raw.assets_three (x INT);",
        },
    )
    capability = CapabilityDeclarations(
        writes=["app.assets_*", "reports.assets_*", "raw.assets_*"],
    )

    violations = validate_writes(
        package_name="assets",
        sql_files=list(sql_dir.glob("*.sql")),
        capability=capability,
    )
    assert violations == []


def test_empty_writes_with_no_sql_passes(tmp_path: Path) -> None:
    """A package with no CREATE statements and no writes declaration is fine."""
    sql_dir = tmp_path / "schema"
    sql_dir.mkdir()
    capability = CapabilityDeclarations(writes=[])

    violations = validate_writes(
        package_name="empty",
        sql_files=list(sql_dir.glob("*.sql")),
        capability=capability,
    )
    assert violations == []


def test_writes_case_insensitive_match(tmp_path: Path) -> None:
    """Mixed-case SQL identifiers match lowercase declared globs."""
    sql_dir = _make_sql_dir(
        tmp_path,
        {"mixed.sql": "CREATE TABLE App.Assets_State (id TEXT);"},
    )
    capability = CapabilityDeclarations(writes=["app.assets_*"])

    violations = validate_writes(
        package_name="assets",
        sql_files=list(sql_dir.glob("*.sql")),
        capability=capability,
    )

    assert violations == []


def test_writes_case_insensitive_glob(tmp_path: Path) -> None:
    """Uppercase globs match lowercase-normalized targets."""
    sql_dir = _make_sql_dir(
        tmp_path,
        {"lower.sql": "CREATE TABLE app.assets_state (id TEXT);"},
    )
    capability = CapabilityDeclarations(writes=["App.assets_*"])

    violations = validate_writes(
        package_name="assets",
        sql_files=list(sql_dir.glob("*.sql")),
        capability=capability,
    )

    assert violations == []


def test_is_write_allowed_matches_glob(tmp_path: Path) -> None:
    """is_write_allowed returns True for a target matching a declared glob."""
    capability = CapabilityDeclarations(writes=["app.assets_*"])
    assert is_write_allowed(capability, "app.assets_state") is True
    assert is_write_allowed(capability, "core.fct_transactions") is False
