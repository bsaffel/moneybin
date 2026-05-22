"""Tests for register_package() orchestration."""

from pathlib import Path
from textwrap import dedent
from unittest.mock import MagicMock, patch

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
    # at least one CapabilityViolation fires. Several validators flag this file
    # (write-glob, schema-layer, and the leak.sql filename); the assertion only
    # needs one CapabilityViolation, so it stays robust to that overlap.
    assert any(isinstance(e, CapabilityViolation) for e in errors)


def test_validate_package_rejects_quoted_identifier(tmp_path: Path) -> None:
    """A quoted (case-sensitive) CREATE target is refused by validate_package.

    'CREATE TABLE "App".test_synthetic_state' would pass the lowercase
    capability check yet execute against a distinct 'App' schema — confirm the
    identifier-safety validator is wired into validate_package to block it.
    """
    info = _make_minimal_pkg(tmp_path)
    (info.root / "schema" / "quoted.sql").write_text(
        'CREATE TABLE "App".test_synthetic_state (id TEXT);'
    )
    errors = validate_package(info)
    assert any(
        isinstance(e, CapabilityViolation) and "quoted identifier" in e.message
        for e in errors
    )


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

    with patch(
        "moneybin.packages._framework.registry._global_registry",
        PackageRegistry(),
    ):
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


def test_validate_package_missing_schema_dir_with_declared_writes_fails(
    tmp_path: Path,
) -> None:
    """A package declaring writes but shipping no schema/ dir is rejected."""
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
              writes: [app.test_synthetic_*]
              reads: []
              network: []
              secrets: []
            requires: {moneybin: ">=1.0.0"}
            entry_points: {tools: x:y, cli: x:y, models: x, schema: x}
            """
        ).strip()
    )
    # Deliberately NO schema/ directory.
    manifest = PackageManifest.from_yaml(tmp_path / "moneybin_package.yaml")
    info = PackageInfo(manifest=manifest, root=tmp_path)
    errors = validate_package(info)
    assert any(isinstance(e, CapabilityViolation) for e in errors)


def test_register_package_rolls_back_registry_on_callable_failure(
    tmp_path: Path,
) -> None:
    """If a callable raises, the package is not left in the registry (retryable)."""
    info = _make_minimal_pkg(tmp_path)
    fresh = PackageRegistry()

    def boom(_: object) -> None:
        raise RuntimeError("tool registration failed")

    with patch("moneybin.packages._framework.registry._global_registry", fresh):
        with pytest.raises(RuntimeError, match="tool registration failed"):
            register_package(
                info=info,
                mcp=MagicMock(),
                cli=MagicMock(),
                tools_callable=boom,
                cli_callable=MagicMock(),
            )
        # Registry must not retain the half-registered package.
        with pytest.raises(KeyError):
            fresh.get("test_synthetic")


def test_validate_package_validates_nested_schema_sql(tmp_path: Path) -> None:
    """SQL nested in a schema/ subdirectory is still validated (rglob, not glob).

    Otherwise a package could hide a cross-prefix CREATE in schema/sub/ and
    pass the capability gate.
    """
    info = _make_minimal_pkg(tmp_path)
    nested = tmp_path / "schema" / "sub"
    nested.mkdir()
    (nested / "leak.sql").write_text("CREATE TABLE core.test_synthetic_leak (x INT);")

    errors = validate_package(info)

    assert any(isinstance(e, CapabilityViolation) for e in errors), (
        f"Nested SQL escaped validation; got {[type(e).__name__ for e in errors]}"
    )


def test_validate_package_flags_dml_in_schema(tmp_path: Path) -> None:
    """A DELETE in a schema file is flagged — DML must not bypass validation."""
    info = _make_minimal_pkg(tmp_path)
    (tmp_path / "schema" / "evil.sql").write_text(
        "DELETE FROM core.fct_transactions WHERE id = '1';"
    )

    errors = validate_package(info)

    assert any(
        isinstance(e, CapabilityViolation) and "DELETE" in e.message for e in errors
    ), f"DML escaped validation; got {[type(e).__name__ for e in errors]}"


def test_init_schemas_rejects_additional_file_outside_package_root(
    tmp_path: Path,
) -> None:
    """init_schemas raises when an additional_files path escapes package_root."""
    import duckdb

    from moneybin.schema import init_schemas

    pkg_root = tmp_path / "pkg"
    (pkg_root / "schema").mkdir(parents=True)
    outside = tmp_path / "evil.sql"
    outside.write_text("CREATE TABLE app.test_synthetic_evil (id TEXT);")

    # Raw connection: this test exercises init_schemas' path guard directly,
    # not the Database wrapper.
    conn = duckdb.connect()
    with pytest.raises(ValueError, match="outside package root"):
        init_schemas(conn, additional_files=[outside], package_root=pkg_root)


def test_init_schemas_requires_package_root_for_additional_files(
    tmp_path: Path,
) -> None:
    """additional_files without package_root is refused (guard can't be skipped)."""
    import duckdb

    from moneybin.schema import init_schemas

    pkg_sql = tmp_path / "app_test_synthetic_state.sql"
    pkg_sql.write_text("CREATE TABLE app.test_synthetic_state (id TEXT);")

    conn = duckdb.connect()
    with pytest.raises(ValueError, match="requires package_root"):
        init_schemas(conn, additional_files=[pkg_sql])


def test_register_package_twice_raises_and_does_not_reinvoke(tmp_path: Path) -> None:
    """A second register_package for the same name raises and re-runs nothing."""
    info = _make_minimal_pkg(tmp_path)
    fresh = PackageRegistry()
    tools_register = MagicMock()
    cli_register = MagicMock()

    with patch("moneybin.packages._framework.registry._global_registry", fresh):
        register_package(
            info=info,
            mcp=MagicMock(),
            cli=MagicMock(),
            tools_callable=tools_register,
            cli_callable=cli_register,
        )
        with pytest.raises(ValueError, match="already registered"):
            register_package(
                info=info,
                mcp=MagicMock(),
                cli=MagicMock(),
                tools_callable=tools_register,
                cli_callable=cli_register,
            )

    # Callables invoked exactly once (the first, successful registration).
    tools_register.assert_called_once()
    cli_register.assert_called_once()
    assert len(fresh.all()) == 1


def test_register_package_uninstalled_entry_point_raises_value_error(
    tmp_path: Path,
) -> None:
    """An uninstalled entry-point module surfaces as ValueError, not ModuleNotFoundError.

    register_package documents 'Raises: ValidationError subclass'; the minimal
    fixture's entry points point at module 'x' (not installed), so resolving
    them must raise ValueError rather than leaking a bare ModuleNotFoundError.
    """
    info = _make_minimal_pkg(tmp_path)
    fresh = PackageRegistry()

    with patch("moneybin.packages._framework.registry._global_registry", fresh):
        with pytest.raises(ValueError, match="is not installed"):
            register_package(info=info, mcp=MagicMock(), cli=MagicMock())


def test_register_package_preserves_transitive_import_error(tmp_path: Path) -> None:
    """Transitive import failures propagate instead of being masked.

    A package module that exists but has a failing internal import must not be
    rewritten as 'not installed' — the real ModuleNotFoundError propagates.
    """
    info = _make_minimal_pkg(tmp_path)  # entry point module is "x"
    fresh = PackageRegistry()

    # Simulate "x" existing but importing a missing transitive dependency.
    transitive = ModuleNotFoundError(
        "No module named 'missing_lib'", name="missing_lib"
    )

    with patch("moneybin.packages._framework.registry._global_registry", fresh):
        with patch(
            "moneybin.packages._framework.registry.importlib.import_module",
            side_effect=transitive,
        ):
            with pytest.raises(ModuleNotFoundError, match="missing_lib"):
                register_package(info=info, mcp=MagicMock(), cli=MagicMock())


def test_register_package_preserves_nameless_import_error(tmp_path: Path) -> None:
    """A ModuleNotFoundError with name=None is not masked as 'not installed'.

    C-extension / shared-library load failures (and bare ModuleNotFoundError())
    carry exc.name=None. That is not evidence the entry module is missing, so
    the original error must propagate rather than be rewritten as 'not installed'.
    """
    info = _make_minimal_pkg(tmp_path)  # entry point module is "x"
    fresh = PackageRegistry()

    nameless = ModuleNotFoundError("dynamic module does not define init function")

    with patch("moneybin.packages._framework.registry._global_registry", fresh):
        with patch(
            "moneybin.packages._framework.registry.importlib.import_module",
            side_effect=nameless,
        ):
            with pytest.raises(
                ModuleNotFoundError, match="does not define init function"
            ):
                register_package(info=info, mcp=MagicMock(), cli=MagicMock())


def test_validate_package_rejects_schema_dir_symlink_escape(tmp_path: Path) -> None:
    """A schema/ symlink pointing outside the package root is refused.

    init_schemas applies a path-containment guard before executing additional
    files; validate_package must match it so validation ⊇ execution. A schema/
    symlink escaping the root would otherwise let validators bless out-of-tree
    SQL the executor would reject.
    """
    info = _make_minimal_pkg(tmp_path)
    # Replace the real schema/ dir with a symlink to an out-of-tree directory.
    outside = tmp_path.parent / "outside_schema"
    outside.mkdir()
    (outside / "app_test_synthetic_state.sql").write_text(
        "CREATE TABLE app.test_synthetic_state (id TEXT);"
    )
    schema_dir = info.root / "schema"
    for child in schema_dir.iterdir():
        child.unlink()
    schema_dir.rmdir()
    schema_dir.symlink_to(outside, target_is_directory=True)

    errors = validate_package(info)

    assert len(errors) == 1
    assert isinstance(errors[0], CapabilityViolation)
    assert "outside the package root" in errors[0].message


def test_init_schemas_executes_additional_files(tmp_path: Path) -> None:
    """init_schemas() accepts and executes package-contributed DDL files."""
    import duckdb

    from moneybin.schema import init_schemas

    pkg_sql = tmp_path / "app_test_synthetic_state.sql"
    pkg_sql.write_text(
        "CREATE TABLE IF NOT EXISTS app.test_synthetic_state (id TEXT PRIMARY KEY);"
    )

    # Raw connection rather than Database: init_schemas() operates on a bare
    # duckdb connection, and this test exercises that pass-through directly
    # without the Database wrapper's schema bootstrapping.
    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA app;")  # init_schemas creates schemas via core files
    # package_root is required when additional_files is non-empty (path guard).
    init_schemas(conn, additional_files=[pkg_sql], package_root=tmp_path)

    result = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='app' AND table_name='test_synthetic_state'"
    ).fetchone()
    assert result is not None and result[0] == 1
