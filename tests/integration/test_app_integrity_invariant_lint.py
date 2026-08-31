"""Reject raw mutations of repository-owned ``app.*`` tables."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

import pytest

import moneybin.tables as tables_module
from moneybin.repositories import concrete_repo_classes

pytestmark = pytest.mark.integration

_MUTATION_RE = re.compile(
    r"\b(?:INSERT\s+(?:OR\s+(?:IGNORE|REPLACE)\s+)?INTO|UPDATE|DELETE\s+FROM)\s+"
    r"(app\.[a-z_][a-z0-9_]*)\b",
    re.IGNORECASE,
)
_EXEMPT_TABLES = frozenset({
    "app.audit_log",
    "app.metrics",
    "app.schema_migrations",
    "app.seed_source_priority",
    "app.versions",
})
_PROTECTED_TABLES = (
    frozenset(
        cls.table_ref.full_name
        for cls in concrete_repo_classes()
        if cls.table_ref.schema == "app"
    )
    - _EXEMPT_TABLES
)
_SOURCE_ROOT = Path(__file__).resolve().parents[2] / "src" / "moneybin"


@dataclass(frozen=True)
class _Violation:
    path: Path
    line: int
    table: str


def _table_ref_bindings(tree: ast.Module) -> dict[str, list[str]]:
    bindings: dict[str, list[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "moneybin.tables":
            for imported in node.names:
                value = getattr(tables_module, imported.name, None)
                if isinstance(value, tables_module.TableRef):
                    name = imported.asname or imported.name
                    if value.full_name not in bindings.setdefault(name, []):
                        bindings[name].append(value.full_name)
    return bindings


def _local_bindings(
    scope: ast.AST,
    *,
    before_line: int | None,
) -> dict[str, list[ast.expr]]:
    candidates: dict[str, list[ast.expr]] = {}

    def add(name: str, node: ast.expr, line: int) -> None:
        if before_line is None or line < before_line:
            candidates.setdefault(name, []).append(node)

    class Collector(ast.NodeVisitor):
        def visit_Assign(self, node: ast.Assign) -> None:
            for target in node.targets:
                if isinstance(target, ast.Name):
                    add(target.id, node.value, node.lineno)
            self.generic_visit(node.value)

        def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
            if node.value is not None:
                if isinstance(node.target, ast.Name):
                    add(node.target.id, node.value, node.lineno)
                self.generic_visit(node.value)

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            return

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            return

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            return

        def visit_Lambda(self, node: ast.Lambda) -> None:
            return

    # The executing scope replaces straight-line assignments; outer/control-flow
    # candidates append conservatively.
    for statement in getattr(scope, "body", []):
        if isinstance(statement, ast.Assign) and (
            before_line is None or statement.lineno < before_line
        ):
            for target in statement.targets:
                if isinstance(target, ast.Name):
                    if before_line is None:
                        add(target.id, statement.value, statement.lineno)
                    else:
                        candidates[target.id] = [statement.value]
            continue
        if (
            isinstance(statement, ast.AnnAssign)
            and statement.value is not None
            and (before_line is None or statement.lineno < before_line)
        ):
            if isinstance(statement.target, ast.Name):
                if before_line is None:
                    add(statement.target.id, statement.value, statement.lineno)
                else:
                    candidates[statement.target.id] = [statement.value]
            continue
        Collector().visit(statement)
    return candidates


def _static_table_names(
    node: ast.expr,
    *,
    table_refs: dict[str, list[str]],
    locals_: dict[str, list[ast.expr]],
    seen_names: frozenset[str],
) -> list[str]:
    if not isinstance(node, ast.Name) or node.id in seen_names:
        return []
    if node.id in table_refs:
        return table_refs[node.id]
    return [
        table
        for value in locals_.get(node.id, [])
        for table in _static_table_names(
            value,
            table_refs=table_refs,
            locals_=locals_,
            seen_names=seen_names | {node.id},
        )
    ]


def _static_sqls(
    node: ast.expr,
    *,
    table_refs: dict[str, list[str]],
    locals_: dict[str, list[ast.expr]],
    seen_names: frozenset[str] = frozenset(),
) -> list[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.Name):
        if node.id in seen_names:
            return []
        return [
            sql
            for value in locals_.get(node.id, [])
            for sql in _static_sqls(
                value,
                table_refs=table_refs,
                locals_=locals_,
                seen_names=seen_names | {node.id},
            )
        ]
    if not isinstance(node, ast.JoinedStr):
        return []
    sqls = [""]
    for part in node.values:
        if isinstance(part, ast.Constant) and isinstance(part.value, str):
            choices = [part.value]
        elif (
            isinstance(part, ast.FormattedValue)
            and isinstance(part.value, ast.Attribute)
            and part.value.attr == "full_name"
        ):
            choices = _static_table_names(
                part.value.value,
                table_refs=table_refs,
                locals_=locals_,
                seen_names=seen_names,
            ) or [" "]
        else:
            choices = [" "]
        sqls = [prefix + choice for prefix in sqls for choice in choices]
    return sqls


def _scope_bindings(
    node: ast.Call,
    *,
    tree: ast.Module,
    parents: dict[ast.AST, ast.AST],
) -> dict[str, list[ast.expr]]:
    scopes: list[ast.AST] = []
    scope = node
    while scope is not tree:
        scope = parents[scope]
        if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
            scopes.append(scope)
    scopes.append(tree)

    bindings: dict[str, list[ast.expr]] = {}
    for enclosing_scope in reversed(scopes):
        before_line = node.lineno if enclosing_scope is scopes[0] else None
        bindings.update(_local_bindings(enclosing_scope, before_line=before_line))
    return bindings


def _violations_in_path(path: Path) -> list[_Violation]:
    is_repository = path.parent.name == "repositories" and path.name.endswith(
        "_repo.py"
    )
    is_audit_service = (
        path.parent.name == "services" and path.name == "audit_service.py"
    )
    is_migration = (
        path.parent.name == "migrations"
        and path.name.startswith("V")
        and path.suffix == ".py"
    )
    if is_repository or is_audit_service or is_migration:
        return []
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    table_refs = _table_ref_bindings(tree)
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }

    violations: list[_Violation] = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("execute", "executemany")
            and node.args
        ):
            continue
        sqls = _static_sqls(
            node.args[0],
            table_refs=table_refs,
            locals_=_scope_bindings(node, tree=tree, parents=parents),
        )
        tables = {
            match.group(1).lower()
            for sql in sqls
            for match in _MUTATION_RE.finditer(sql)
            if match.group(1).lower() in _PROTECTED_TABLES
        }
        for table in sorted(tables):
            violations.append(
                _Violation(
                    path=path,
                    line=node.lineno,
                    table=table,
                )
            )
    return violations


def test_rejects_inline_protected_write_in_service(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def create(db):\n"
        '    db.execute(f"INSERT INTO {USER_CATEGORIES.full_name} VALUES (?)", ["id"])\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=4, table="app.user_categories")
    ]


def test_rejects_bulk_protected_write_in_service(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def create_many(db, rows):\n"
        '    db.executemany(f"INSERT INTO {USER_CATEGORIES.full_name} VALUES (?)", rows)\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=4, table="app.user_categories")
    ]


def test_rejects_function_local_protected_write_in_service(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def deactivate(db):\n"
        '    sql = f"UPDATE {USER_CATEGORIES.full_name} SET active = FALSE"\n'
        "    db.execute(sql)\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=5, table="app.user_categories")
    ]


def test_rejects_protected_write_with_function_local_import(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "def create(db):\n"
        "    from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        '    db.execute(f"INSERT INTO {USER_CATEGORIES.full_name} VALUES (?)")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=4, table="app.user_categories")
    ]


def test_rejects_protected_write_through_table_ref_alias(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def create(db):\n"
        "    table = USER_CATEGORIES\n"
        '    db.execute(f"INSERT INTO {table.full_name} VALUES (?)")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=5, table="app.user_categories")
    ]


def test_rejects_protected_write_captured_from_enclosing_scope(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def outer(db):\n"
        '    sql = f"DELETE FROM {USER_CATEGORIES.full_name}"\n'
        "    def run():\n"
        "        db.execute(sql)\n"
        "    run()\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=6, table="app.user_categories")
    ]


def test_rejects_closure_defined_before_captured_write_assignment(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def outer(db):\n"
        "    def run():\n"
        "        db.execute(sql)\n"
        '    sql = f"DELETE FROM {USER_CATEGORIES.full_name}"\n'
        "    run()\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=5, table="app.user_categories")
    ]


def test_rejects_closure_invoked_before_safe_outer_reassignment(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def outer(db):\n"
        '    sql = f"DELETE FROM {USER_CATEGORIES.full_name}"\n'
        "    def run():\n"
        "        db.execute(sql)\n"
        "    run()\n"
        '    sql = "SELECT * FROM app.user_categories"\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=6, table="app.user_categories")
    ]


def test_uses_latest_assignment_before_execute(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def delete(db):\n"
        '    sql = f"DELETE FROM {USER_CATEGORIES.full_name} WHERE active = FALSE"\n'
        "    db.execute(sql)\n"
        '    sql = "SELECT * FROM app.user_categories"\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=5, table="app.user_categories")
    ]


def test_ignores_overwritten_straight_line_mutation(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def query(db):\n"
        '    sql = f"DELETE FROM {USER_CATEGORIES.full_name}"\n'
        '    sql = "SELECT * FROM app.user_categories"\n'
        "    db.execute(sql)\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


def test_ignores_bindings_from_nested_scope(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def delete(db):\n"
        '    sql = f"UPDATE {USER_CATEGORIES.full_name} SET active = FALSE"\n'
        "    def nested():\n"
        '        sql = "SELECT * FROM app.user_categories"\n'
        "        return sql\n"
        "    db.execute(sql)\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=8, table="app.user_categories")
    ]


def test_rejects_any_protected_control_flow_binding(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def mutate(db, unsafe):\n"
        "    if unsafe:\n"
        '        sql = f"DELETE FROM {USER_CATEGORIES.full_name}"\n'
        "    else:\n"
        '        sql = "SELECT * FROM app.user_categories"\n'
        "    db.execute(sql)\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=8, table="app.user_categories")
    ]


def test_rejects_insert_or_ignore_in_service(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def create(db):\n"
        '    db.execute(f"INSERT OR IGNORE INTO {USER_CATEGORIES.full_name} VALUES (?)")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=4, table="app.user_categories")
    ]


@pytest.mark.parametrize(
    ("relative_path", "sql"),
    [
        (
            "src/moneybin/repositories/example_repo.py",
            "DELETE FROM app.user_categories WHERE category_id = ?",
        ),
        (
            "src/moneybin/services/audit_service.py",
            "INSERT INTO app.user_categories VALUES (?)",
        ),
        (
            "src/moneybin/sql/migrations/V999_test.py",
            "UPDATE app.user_categories SET active = FALSE",
        ),
        (
            "src/moneybin/services/example_service.py",
            "INSERT INTO app.metrics VALUES (?)",
        ),
        (
            "src/moneybin/services/example_service.py",
            "INSERT INTO app.audit_log VALUES (?)",
        ),
        (
            "src/moneybin/services/example_service.py",
            "UPDATE app.seed_source_priority SET priority = 1",
        ),
        (
            "src/moneybin/services/example_service.py",
            "DELETE FROM app.schema_migrations WHERE version = 1",
        ),
        (
            "src/moneybin/services/example_service.py",
            "UPDATE app.versions SET version = '1'",
        ),
        (
            "src/moneybin/services/example_service.py",
            "SELECT * FROM app.user_categories",
        ),
    ],
)
def test_allows_sanctioned_writes_and_reads(
    tmp_path: Path, relative_path: str, sql: str
) -> None:
    source = tmp_path / relative_path
    source.parent.mkdir(parents=True)
    source.write_text(
        f"def delete(db):\n    db.execute({sql!r})\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


def test_runtime_app_mutations_are_repository_routed() -> None:
    violations = [
        violation
        for path in sorted(_SOURCE_ROOT.rglob("*.py"))
        for violation in _violations_in_path(path)
    ]

    assert not violations, (
        "Raw protected app.* mutation bypasses a repository:\n"
        + "\n".join(
            f"{violation.path}:{violation.line}: {violation.table}"
            for violation in violations
        )
    )
