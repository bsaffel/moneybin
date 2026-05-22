"""Tests for capability declarations and the write-glob validator."""

from pathlib import Path
from textwrap import dedent

from moneybin.packages._framework.capabilities import (
    CapabilityViolation,
    is_write_allowed,
    validate_schema_layers,
    validate_statement_types,
    validate_writes,
)
from moneybin.packages._framework.manifest import CapabilityDeclarations


def _make_sql_dir(tmp_path: Path, files: dict[str, str]) -> Path:
    sql_dir = tmp_path / "schema"
    sql_dir.mkdir()
    for name, body in files.items():
        (sql_dir / name).write_text(dedent(body).strip())
    return sql_dir


def test_statement_types_flags_dml_and_destructive_ddl(tmp_path: Path) -> None:
    """DML / DROP / ALTER in a schema file are flagged (they bypass write-glob).

    validate_writes only inspects CREATE targets, so without this check a
    DELETE/INSERT/DROP would run unvalidated when Plan 4 executes the SQL.
    """
    sql_dir = _make_sql_dir(
        tmp_path,
        {
            "ok.sql": "CREATE TABLE app.test_synthetic_state (id TEXT);",
            "bad.sql": (
                "DELETE FROM core.fct_transactions WHERE id = '1';\n"
                "DROP TABLE app.other;"
            ),
        },
    )

    violations = validate_statement_types(
        package_name="test_synthetic",
        sql_files=sorted(sql_dir.glob("*.sql")),
    )

    descriptors = {v.target for v in violations}
    assert "(DELETE)" in descriptors
    assert "(DROP)" in descriptors
    # The clean CREATE-only file produces no statement-type violation.
    assert all("ok.sql" not in v.sql_file for v in violations)


def test_statement_types_passes_create_table_and_view(tmp_path: Path) -> None:
    """A schema file of only CREATE TABLE/VIEW yields no statement-type violation."""
    sql_dir = _make_sql_dir(
        tmp_path,
        {
            "ok.sql": (
                "CREATE TABLE app.test_synthetic_state (id TEXT);\n"
                "CREATE VIEW reports.test_synthetic_v AS SELECT 1 AS x;"
            ),
        },
    )
    violations = validate_statement_types(
        package_name="test_synthetic",
        sql_files=sorted(sql_dir.glob("*.sql")),
    )
    assert violations == []


def test_unparseable_sql_returns_violation_not_raise(tmp_path: Path) -> None:
    """Unparseable SQL surfaces a CapabilityViolation instead of crashing.

    extract_create_targets raises ValueError on unparseable SQL; validate_writes
    must catch it and return a violation (its contract is never to raise), so a
    malformed package file can't crash framework bootstrap.
    """
    sql_dir = _make_sql_dir(tmp_path, {"bad.sql": "CREATE TABLE oops syntax ;;;"})
    capability = CapabilityDeclarations(writes=["app.test_synthetic_*"])

    violations = validate_writes(
        package_name="test_synthetic",
        sql_files=list(sql_dir.glob("*.sql")),
        capability=capability,
    )

    assert len(violations) == 1
    assert violations[0].target == "(unparseable)"
    assert "could not parse" in violations[0].message


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


def test_schema_layers_rejects_reports_and_core_targets(tmp_path: Path) -> None:
    """schema/ bootstrap DDL may only target raw/app — never reports/core.

    A package may legitimately declare reports.<prefix>_* as a write capability
    (satisfied later by its models/ directory), so validate_writes accepts a
    CREATE VIEW into reports. But schema/ files execute as bootstrap DDL: a
    raw_/app_-named schema file that CREATEs into the framework-managed reports
    (or core/prep) layer must be refused. This is the sibling check to
    validate_statement_types.
    """
    sql_dir = _make_sql_dir(
        tmp_path,
        {
            "app_assets_state.sql": (
                "CREATE TABLE app.assets_state (id TEXT);\n"
                "CREATE VIEW reports.assets_summary AS SELECT 1;\n"
                "CREATE TABLE core.assets_leak (id TEXT);"
            ),
        },
    )

    violations = validate_schema_layers(
        package_name="assets",
        sql_files=list(sql_dir.glob("*.sql")),
    )

    offenders = {v.target for v in violations}
    assert offenders == {"reports.assets_summary", "core.assets_leak"}
    assert all(isinstance(v, CapabilityViolation) for v in violations)


def test_schema_layers_passes_raw_and_app(tmp_path: Path) -> None:
    """CREATE targets in raw/app produce no layer violation."""
    sql_dir = _make_sql_dir(
        tmp_path,
        {
            "ok.sql": (
                "CREATE TABLE raw.assets_imports (id TEXT);\n"
                "CREATE TABLE app.assets_state (id TEXT);"
            ),
        },
    )
    violations = validate_schema_layers(
        package_name="assets",
        sql_files=list(sql_dir.glob("*.sql")),
    )
    assert violations == []


def test_schema_layers_unparseable_returns_violation(tmp_path: Path) -> None:
    """Unparseable SQL surfaces a violation instead of crashing bootstrap."""
    sql_dir = _make_sql_dir(tmp_path, {"bad.sql": "CREATE TABLE oops syntax ;;;"})
    violations = validate_schema_layers(
        package_name="assets",
        sql_files=list(sql_dir.glob("*.sql")),
    )
    assert len(violations) == 1
    assert violations[0].target == "(unparseable)"
    assert "could not parse" in violations[0].message


def test_is_write_allowed_matches_glob(tmp_path: Path) -> None:
    """is_write_allowed returns True for a target matching a declared glob."""
    capability = CapabilityDeclarations(writes=["app.assets_*"])
    assert is_write_allowed(capability, "app.assets_state") is True
    assert is_write_allowed(capability, "core.fct_transactions") is False
