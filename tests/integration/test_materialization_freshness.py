"""Structural guard: a model's kind is a freshness contract.

A ``FULL`` model may read only values whose every writer restates it
(docs/specs/materialization-freshness-contract.md). This builds every model,
records the SQL each Python model runs, and follows column lineage through
``VIEW`` models to the ``app.*`` columns each materialized model reads. Lineage
follows projected expressions, not ``WHERE`` or ``JOIN`` predicates; the
write-then-apply guard in ``test_materialization_freshness_writes.py`` covers
those.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Generator, Iterator
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlglot import exp, parse_one
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlmesh.core.context import BaseContext
from sqlmesh.core.lineage import column_dependencies
from sqlmesh.core.model.definition import PythonModel

from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.database import Database, sqlmesh_context
from moneybin.services.transform_service import TransformService
from moneybin.sqlmesh_freshness import (
    APP_INPUTS,
    InputFreshness,
    fully_restates,
    input_class,
)
from moneybin.tables import TableRef
from tests.integration.conftest import make_secret_store

pytestmark = [pytest.mark.integration, pytest.mark.slow]

# A built model's physical table: "sqlmesh__core"."core__fct_transactions__123".
_PHYSICAL = re.compile(r"^sqlmesh__(?P<schema>\w+)$")


def _short(name: str) -> str:
    """'"moneybin"."core"."dim_accounts"' -> 'core.dim_accounts'."""
    parts = name.replace('"', "").split(".")
    return ".".join(parts[-2:])


def _relation(table: exp.Table) -> str:
    """The logical model or table a physical reference names."""
    match = _PHYSICAL.match(table.db)
    if match is None:
        return f"{table.db}.{table.name}"
    schema = match["schema"]
    name = re.sub(r"__\d+$", "", table.name).removeprefix(f"{schema}__")
    return f"{schema}.{name}"


@dataclass
class _Build:
    kinds: dict[str, str]
    depends_on: dict[str, set[str]]
    python_reads: dict[str, list[str]]
    schema: dict[str, Any]
    lineage: dict[tuple[str, str], dict[str, set[str]]] = field(default_factory=dict)
    # Rendered SQL of every SQL model a FULL model reads through.
    feeding_sql: dict[str, str] = field(default_factory=dict)


# The Python model rendering on this thread. SQLMesh runs a model's code from
# its serialized source, so the stack carries no model file to read it from.
_rendering = threading.local()


@pytest.fixture(scope="module")
def build(tmp_path_factory: pytest.TempPathFactory) -> Generator[_Build, None, None]:
    """Build every model once, recording each Python model's fetchdf SQL."""
    clear_settings_cache()
    set_current_profile("test")
    db_path = tmp_path_factory.mktemp("freshness") / "freshness.duckdb"
    reads: dict[str, list[str]] = {}
    original_fetchdf = BaseContext.fetchdf
    original_render = PythonModel.render  # pyright: ignore[reportUnknownMemberType]  # sqlmesh ships no stubs

    def recording_render(self: PythonModel, **kwargs: Any) -> Iterator[Any]:
        _rendering.model = _short(self.name)
        try:
            yield from original_render(self, **kwargs)
        finally:
            _rendering.model = None

    def recording_fetchdf(
        self: BaseContext, query: Any, *args: Any, **kwargs: Any
    ) -> Any:
        model = getattr(_rendering, "model", None)
        if model is not None:
            text = query if isinstance(query, str) else query.sql(dialect="duckdb")
            reads.setdefault(model, []).append(text)
        return original_fetchdf(self, query, *args, **kwargs)

    with pytest.MonkeyPatch.context() as mp:
        settings = MagicMock()
        settings.database.path = db_path
        mp.setattr("moneybin.database.get_settings", lambda: settings)
        mp.setattr(BaseContext, "fetchdf", recording_fetchdf)
        mp.setattr(PythonModel, "render", recording_render)
        db = Database(db_path, secret_store=make_secret_store(), read_only=False)
        try:
            assert TransformService(db).apply().applied
            schema: dict[str, Any] = {}
            for schema_name, table, column, data_type in db.execute(
                """
                SELECT schema_name, table_name, column_name, data_type
                FROM duckdb_columns() WHERE database_name = 'moneybin'
                """
            ).fetchall():
                schema.setdefault("moneybin", {}).setdefault(
                    schema_name, {}
                ).setdefault(table, {})[column] = data_type
            with sqlmesh_context(db) as ctx:
                kinds = {_short(n): m.kind.name for n, m in ctx.models.items()}
                depends_on = {
                    _short(n): {_short(d) for d in m.depends_on}
                    for n, m in ctx.models.items()
                }
                result = _Build(kinds, depends_on, reads, schema)
                feeding = {
                    _short(up)
                    for name, model in ctx.models.items()
                    if model.kind.is_full
                    for up in ctx.dag.upstream(name)
                }
                for name, model in ctx.models.items():
                    if _short(name) in feeding and not (
                        model.kind.is_symbolic or model.is_python
                    ):
                        query = model.render_query()
                        if query is not None:
                            result.feeding_sql[_short(name)] = query.sql(
                                dialect="duckdb"
                            )
                for name, model in ctx.models.items():
                    if model.kind.is_symbolic or model.is_python:
                        continue
                    for column in model.columns_to_types or {}:
                        deps = column_dependencies(ctx, name, column)  # pyright: ignore[reportUnknownArgumentType]  # sqlmesh ships no stubs
                        result.lineage[(_short(name), column)] = {
                            _short(parent): set(cols) for parent, cols in deps.items()
                        }
        finally:
            db.close()
    yield result
    clear_settings_cache()


def _columns_read(build: _Build, sql: str) -> Iterator[tuple[str, str, Scope]]:
    """Every (relation, column) one captured statement reads, with its scope."""
    tree = qualify(
        parse_one(sql, dialect="duckdb"),
        schema=build.schema,
        dialect="duckdb",
        catalog="moneybin",
        validate_qualify_columns=False,
        identify=False,
    )
    for scope in traverse_scope(tree):
        for column in scope.columns:
            source = scope.sources.get(column.table)
            if isinstance(source, exp.Table):
                yield _relation(source), column.name, scope


def _sources(
    build: _Build, relation: str, column: str, seen: set[tuple[str, str]]
) -> Iterator[tuple[str, str]]:
    """The external (table, column) leaves a column reaches through VIEW models."""
    if (relation, column) in seen:
        return
    seen.add((relation, column))
    kind = build.kinds.get(relation)
    if kind is None or kind == "EXTERNAL":
        yield relation, column
        return
    if kind != "VIEW":
        return  # a materialized parent is guarded on its own
    for parent, columns in build.lineage.get((relation, column), {}).items():
        for parent_column in columns:
            yield from _sources(build, parent, parent_column, seen)


def _full_model_reads(build: _Build) -> Iterator[tuple[str, str, str]]:
    """(FULL model, relation, column) for every column a FULL model reads directly."""
    for model, kind in build.kinds.items():
        if kind != "FULL":
            continue
        if model in build.python_reads:
            for sql in build.python_reads[model]:
                for relation, column, _ in _columns_read(build, sql):
                    yield model, relation, column
            continue
        for (name, _column), parents in build.lineage.items():
            if name != model:
                continue
            for parent, columns in parents.items():
                for parent_column in columns:
                    yield model, parent, parent_column


def _app_class(relation: str, column: str) -> InputFreshness | None:
    schema, name = relation.split(".")
    if schema != "app":
        return None
    entry = input_class(TableRef(schema, name))
    return None if entry is None else entry.of(column)


def test_every_app_external_model_is_classified(build: _Build) -> None:
    """The registry and the declared app.* external models are the same set."""
    externals = {
        name
        for name, kind in build.kinds.items()
        if kind == "EXTERNAL" and name.startswith("app.")
    }
    assert {c.table.full_name for c in APP_INPUTS} == externals


def test_every_app_table_a_model_reads_is_declared(build: _Build) -> None:
    """An app.* read outside external_models.yaml is invisible to the restate cascade."""
    undeclared = sorted(
        f"{model} reads {dep}"
        for model, deps in build.depends_on.items()
        for dep in deps
        if dep.startswith("app.") and build.kinds.get(dep) != "EXTERNAL"
    )
    assert not undeclared


def test_python_models_declare_every_relation_they_read(build: _Build) -> None:
    """Without the edge, a write to the relation never restates the model."""
    assert build.python_reads, "no Python model SQL was captured"
    missing = sorted(
        f"{model} reads {relation} without depends_on"
        for model, statements in build.python_reads.items()
        for sql in statements
        for relation, _, _ in _columns_read(build, sql)
        if relation != model and relation not in build.depends_on[model]
    )
    assert not missing


def test_no_materialized_model_reads_a_read_through_column(build: _Build) -> None:
    """Curation is read only through views; a FULL model would hold it stale."""
    violations: set[str] = set()
    for model, relation, column in _full_model_reads(build):
        for leaf, leaf_column in _sources(build, relation, column, set()):
            if _app_class(leaf, leaf_column) is InputFreshness.READ_THROUGH:
                violations.add(
                    f"{model} <- {relation}.{column} <- {leaf}.{leaf_column}"
                )
    assert not sorted(violations)


def _target_tables(scope: Scope) -> set[str] | None:
    """The literal target_table values a scope reading app.audit_log filters to."""
    where = scope.expression.args.get("where")
    if where is None:
        return None
    found: set[str] = set()
    for node in where.find_all(exp.EQ, exp.In):
        column = node.this if isinstance(node, exp.In) else node.left
        if not (isinstance(column, exp.Column) and column.name == "target_table"):
            continue
        literals = node.expressions if isinstance(node, exp.In) else [node.right]
        found.update(lit.name for lit in literals if isinstance(lit, exp.Literal))
    return found or None


def test_audit_log_reads_are_scoped_to_restating_tables(build: _Build) -> None:
    """An audit row is as fresh as its target only when every write to it restates."""
    statements = [sql for reads in build.python_reads.values() for sql in reads]
    statements += build.feeding_sql.values()
    assert any("audit_log" in sql for sql in statements), "no audit_log read seen"
    offenders: set[str] = set()
    for sql in statements:
        for relation, _, scope in _columns_read(build, sql):
            if relation != "app.audit_log":
                continue
            targets = _target_tables(scope)
            if targets is None:
                offenders.add(f"unscoped audit_log read: {sql[:80]}")
                continue
            offenders.update(
                f"audit_log read scoped to {t}, which does not fully restate"
                for t in targets
                if not fully_restates(TableRef("app", t))
            )
    assert not sorted(offenders)
