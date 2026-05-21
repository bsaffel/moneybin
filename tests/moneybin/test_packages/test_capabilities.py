"""Tests for capability declarations and the write-glob validator."""

from pathlib import Path
from textwrap import dedent

from moneybin.packages._framework.capabilities import (
    Capability,
    CapabilityViolation,
    validate_writes,
)


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
    capability = Capability(
        writes=["app.test_synthetic_*", "reports.test_synthetic_*"],
        reads=[],
        network=[],
        secrets=[],
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
    capability = Capability(
        writes=["app.test_synthetic_*"],
        reads=[],
        network=[],
        secrets=[],
    )

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
    capability = Capability(
        writes=["reports.assets_*"], reads=[], network=[], secrets=[]
    )

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
    capability = Capability(
        writes=["app.assets_*", "reports.assets_*", "raw.assets_*"],
        reads=[],
        network=[],
        secrets=[],
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
    capability = Capability(writes=[], reads=[], network=[], secrets=[])

    violations = validate_writes(
        package_name="empty",
        sql_files=list(sql_dir.glob("*.sql")),
        capability=capability,
    )
    assert violations == []
