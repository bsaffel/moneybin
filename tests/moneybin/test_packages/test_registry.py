"""Tests for register_package() orchestration."""

from pathlib import Path
from textwrap import dedent
from unittest.mock import MagicMock

import pytest

from moneybin.packages._framework.discovery import PackageInfo
from moneybin.packages._framework.errors import (
    CapabilityViolation,
    PrefixViolation,
)
from moneybin.packages._framework.manifest import PackageManifest
from moneybin.packages._framework.registry import (
    PackageRegistry,
    register_package,
    validate_package,
)


def _make_minimal_pkg(tmp_path: Path) -> PackageInfo:
    """Build a minimum valid PackageInfo on disk."""
    (tmp_path / "moneybin_package.yaml").write_text(
        dedent(
            """
            name: test_synthetic
            display_name: Test
            version: 1.0.0
            quality_scale: bronze
            owns_prefix: test_synthetic
            publisher: {name: Test, verified: false}
            description: Test
            capabilities:
              writes:
                - app.test_synthetic_*
              reads: []
              network: []
              secrets: []
            requires: {moneybin: ">=1.0.0"}
            entry_points:
              tools: x:y
              cli: x:y
              models: x
              schema: x
            """
        ).strip()
    )
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir()
    (schema_dir / "app_test_synthetic_state.sql").write_text(
        "CREATE TABLE app.test_synthetic_state (id TEXT PRIMARY KEY);"
    )
    manifest = PackageManifest.from_yaml(tmp_path / "moneybin_package.yaml")
    return PackageInfo(manifest=manifest, root=tmp_path)


def test_validate_package_clean_returns_empty(tmp_path: Path) -> None:
    info = _make_minimal_pkg(tmp_path)
    errors = validate_package(info)
    assert errors == []


def test_validate_package_with_capability_leak_fails(tmp_path: Path) -> None:
    info = _make_minimal_pkg(tmp_path)
    # Add a SQL file writing outside declared capabilities.
    (info.root / "schema" / "leak.sql").write_text(
        "CREATE TABLE core.test_synthetic_leak (x INT);"
    )
    errors = validate_package(info)
    # SQL writes to core but capability only declares app.test_synthetic_* →
    # capability violation. The CREATE name 'test_synthetic_leak' DOES start
    # with the prefix so prefix validator passes; only the capability one fires.
    assert any(isinstance(e, CapabilityViolation) for e in errors)


def test_validate_package_with_prefix_leak_fails(tmp_path: Path) -> None:
    info = _make_minimal_pkg(tmp_path)
    (info.root / "schema" / "leak.sql").write_text(
        "CREATE TABLE app.other_thing (x INT);"
    )
    errors = validate_package(info)
    # Capability says app.test_synthetic_* → app.other_thing fails both
    # the capability check AND the prefix check.
    assert any(isinstance(e, CapabilityViolation) for e in errors)
    assert any(isinstance(e, PrefixViolation) for e in errors)


def test_register_package_invokes_tools_and_cli(tmp_path: Path) -> None:
    info = _make_minimal_pkg(tmp_path)
    mcp = MagicMock()
    cli = MagicMock()
    tools_register = MagicMock()
    cli_register = MagicMock()

    register_package(
        info=info,
        mcp=mcp,
        cli=cli,
        tools_callable=tools_register,
        cli_callable=cli_register,
    )

    tools_register.assert_called_once_with(mcp)
    cli_register.assert_called_once_with(cli)


def test_register_package_rejects_invalid_package(tmp_path: Path) -> None:
    info = _make_minimal_pkg(tmp_path)
    (info.root / "schema" / "leak.sql").write_text(
        "CREATE TABLE core.test_synthetic_leak (x INT);"
    )
    mcp = MagicMock()
    cli = MagicMock()
    tools_register = MagicMock()
    cli_register = MagicMock()

    with pytest.raises(CapabilityViolation):
        register_package(
            info=info,
            mcp=mcp,
            cli=cli,
            tools_callable=tools_register,
            cli_callable=cli_register,
        )

    tools_register.assert_not_called()
    cli_register.assert_not_called()


def test_registry_singleton_holds_registered_packages(tmp_path: Path) -> None:
    info = _make_minimal_pkg(tmp_path)
    registry = PackageRegistry()
    registry.add(info)
    assert registry.get("test_synthetic") is info
    assert "test_synthetic" in [p.manifest.name for p in registry.all()]
    with pytest.raises(KeyError):
        registry.get("does_not_exist")


def test_init_schemas_executes_additional_files(tmp_path: Path) -> None:
    """init_schemas() accepts and executes package-contributed DDL files."""
    import duckdb

    from moneybin.schema import init_schemas

    pkg_sql = tmp_path / "app_test_synthetic_state.sql"
    pkg_sql.write_text(
        "CREATE TABLE IF NOT EXISTS app.test_synthetic_state (id TEXT PRIMARY KEY);"
    )

    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA app;")  # init_schemas creates schemas via core files
    init_schemas(conn, additional_files=[pkg_sql])

    result = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='app' AND table_name='test_synthetic_state'"
    ).fetchone()
    assert result is not None and result[0] == 1
