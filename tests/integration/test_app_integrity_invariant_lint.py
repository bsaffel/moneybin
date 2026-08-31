"""Reject raw mutations of repository-owned ``app.*`` tables."""

from __future__ import annotations

import ast
import re
import runpy
from dataclasses import dataclass
from pathlib import Path

import pytest

import moneybin.tables as tables_module

pytestmark = pytest.mark.integration

_MUTATION_RE = re.compile(
    r"\b(?:INSERT\s+(?:OR\s+(?:IGNORE|REPLACE)\s+)?INTO|MERGE\s+INTO|UPDATE|"
    r"DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?|CREATE\s+OR\s+REPLACE\s+TABLE|"
    r"DROP\s+TABLE(?:\s+IF\s+EXISTS)?|"
    r"ALTER\s+TABLE(?:\s+IF\s+EXISTS)?(?:\s+ONLY)?)\s+"
    r'"?(app)"?\s*\.\s*"?([a-z_][a-z0-9_]*)"?(?![a-z0-9_"])',
    re.IGNORECASE,
)
_COPY_FROM_RE = re.compile(
    r"\bCOPY\s+"
    r'"?(app)"?\s*\.\s*"?([a-z_][a-z0-9_]*)"?(?![a-z0-9_"])'
    r"(?:\s*\([^)]*\))?\s+FROM\b",
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
        ref.full_name
        for ref in vars(tables_module).values()
        if isinstance(ref, tables_module.TableRef) and ref.schema == "app"
    )
    - _EXEMPT_TABLES
)
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class _Violation:
    path: Path
    line: int
    table: str


class _TablesModuleBinding:
    pass


class _UnknownBinding:
    pass


_TABLES_MODULE = _TablesModuleBinding()
_UNKNOWN = _UnknownBinding()
type _BindingValue = (
    ast.expr | tables_module.TableRef | _TablesModuleBinding | _UnknownBinding
)


def _import_bindings(
    node: ast.Import | ast.ImportFrom,
) -> list[tuple[str, _BindingValue]]:
    if isinstance(node, ast.ImportFrom) and node.module == "moneybin.tables":
        bindings: list[tuple[str, _BindingValue]] = []
        for imported in node.names:
            value = getattr(tables_module, imported.name, None)
            if isinstance(value, tables_module.TableRef):
                bindings.append((imported.asname or imported.name, value))
        return bindings
    if isinstance(node, ast.Import):
        return [
            (imported.asname, _TABLES_MODULE)
            for imported in node.names
            if imported.name == "moneybin.tables" and imported.asname is not None
        ]
    return []


def _assignment_bindings(
    target: ast.expr,
    value: ast.expr,
) -> list[tuple[str, ast.expr]]:
    if isinstance(target, ast.Name):
        return [(target.id, value)]
    if (
        isinstance(target, (ast.Tuple, ast.List))
        and isinstance(value, (ast.Tuple, ast.List))
        and len(target.elts) == len(value.elts)
    ):
        return [
            binding
            for target_item, value_item in zip(
                target.elts,
                value.elts,
                strict=True,
            )
            for binding in _assignment_bindings(target_item, value_item)
        ]
    return []


def _resolved_binding_values(
    value: ast.expr,
    *,
    visible_bindings: dict[str, list[_BindingValue]],
) -> list[_BindingValue]:
    if isinstance(value, ast.Name) and value.id in visible_bindings:
        return visible_bindings[value.id]
    return [value]


def _target_names(target: ast.expr) -> list[str]:
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        return [name for item in target.elts for name in _target_names(item)]
    return []


def _static_dict_entries(
    node: ast.expr,
    *,
    locals_: dict[str, list[_BindingValue]],
    seen_names: frozenset[str],
) -> list[tuple[ast.expr, ast.expr]]:
    if isinstance(node, ast.Dict):
        return [
            (key, value)
            for key, value in zip(node.keys, node.values, strict=True)
            if key is not None
        ]
    if not isinstance(node, ast.Name) or node.id in seen_names:
        return []
    return [
        entry
        for value in locals_.get(node.id, [])
        if isinstance(value, ast.expr)
        for entry in _static_dict_entries(
            value,
            locals_=locals_,
            seen_names=seen_names | {node.id},
        )
    ]


def _static_loop_values(
    node: ast.expr,
    *,
    locals_: dict[str, list[_BindingValue]],
    seen_names: frozenset[str] = frozenset(),
) -> list[ast.expr]:
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        return list(node.elts)
    if isinstance(node, ast.Dict):
        return [key for key in node.keys if key is not None]
    if isinstance(node, ast.Name):
        if node.id in seen_names:
            return []
        return [
            item
            for value in locals_.get(node.id, [])
            if isinstance(value, ast.expr)
            for item in _static_loop_values(
                value,
                locals_=locals_,
                seen_names=seen_names | {node.id},
            )
        ]
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"items", "keys", "values"}
        and not node.args
        and not node.keywords
    ):
        entries = _static_dict_entries(
            node.func.value,
            locals_=locals_,
            seen_names=seen_names,
        )
        if node.func.attr == "items":
            return [
                ast.Tuple(elts=[key, value], ctx=ast.Load()) for key, value in entries
            ]
        return [key if node.func.attr == "keys" else value for key, value in entries]
    return []


def _for_bindings(
    target: ast.expr,
    iterable: ast.expr,
    *,
    locals_: dict[str, list[_BindingValue]],
) -> list[tuple[str, _BindingValue]]:
    bindings = [
        binding
        for value in _static_loop_values(iterable, locals_=locals_)
        for binding in _assignment_bindings(target, value)
    ]
    bound_names = {name for name, _ in bindings}
    return [
        *bindings,
        *(
            (name, _UNKNOWN)
            for name in _target_names(target)
            if name not in bound_names
        ),
    ]


def _contains_node(root: ast.AST, node: ast.AST) -> bool:
    return any(candidate is node for candidate in ast.walk(root))


def _comprehension_bindings(
    scope: ast.ListComp | ast.SetComp | ast.DictComp | ast.GeneratorExp,
    *,
    node: ast.Call,
    outer_bindings: dict[str, list[_BindingValue]],
) -> dict[str, list[_BindingValue]]:
    candidates: dict[str, list[_BindingValue]] = {}
    for generator in scope.generators:
        if _contains_node(generator.iter, node):
            break
        visible_bindings = {**outer_bindings, **candidates}
        current: dict[str, list[_BindingValue]] = {}
        for name, value in _for_bindings(
            generator.target,
            generator.iter,
            locals_=visible_bindings,
        ):
            current.setdefault(name, []).append(value)
        candidates.update(current)
        if any(_contains_node(condition, node) for condition in generator.ifs):
            break
    return candidates


def _local_bindings(
    scope: ast.AST,
    *,
    before_line: int | None,
    outer_bindings: dict[str, list[_BindingValue]],
) -> dict[str, list[_BindingValue]]:
    candidates: dict[str, list[_BindingValue]] = {}

    def add(name: str, node: _BindingValue, line: int) -> None:
        if before_line is None or line < before_line:
            candidates.setdefault(name, []).append(node)

    class Collector(ast.NodeVisitor):
        def visit_Assign(self, node: ast.Assign) -> None:
            visible_bindings = {**outer_bindings, **candidates}
            for target in node.targets:
                for name, value in _assignment_bindings(target, node.value):
                    for resolved in _resolved_binding_values(
                        value,
                        visible_bindings=visible_bindings,
                    ):
                        add(name, resolved, node.lineno)
            self.generic_visit(node.value)

        def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
            if node.value is not None:
                if isinstance(node.target, ast.Name):
                    visible_bindings = {**outer_bindings, **candidates}
                    for resolved in _resolved_binding_values(
                        node.value,
                        visible_bindings=visible_bindings,
                    ):
                        add(node.target.id, resolved, node.lineno)
                self.generic_visit(node.value)

        def visit_Import(self, node: ast.Import) -> None:
            for name, value in _import_bindings(node):
                add(name, value, node.lineno)

        def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
            for name, value in _import_bindings(node):
                add(name, value, node.lineno)

        def visit_For(self, node: ast.For) -> None:
            visible_bindings = {**outer_bindings, **candidates}
            for name, value in _for_bindings(
                node.target,
                node.iter,
                locals_=visible_bindings,
            ):
                add(name, value, node.lineno)
            self.visit(node.iter)
            for statement in [*node.body, *node.orelse]:
                self.visit(statement)

        def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
            visible_bindings = {**outer_bindings, **candidates}
            for name, value in _for_bindings(
                node.target,
                node.iter,
                locals_=visible_bindings,
            ):
                add(name, value, node.lineno)
            self.visit(node.iter)
            for statement in [*node.body, *node.orelse]:
                self.visit(statement)

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            return

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            return

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            return

        def visit_Lambda(self, node: ast.Lambda) -> None:
            return

    if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
        arguments = [
            *scope.args.posonlyargs,
            *scope.args.args,
            *scope.args.kwonlyargs,
        ]
        if scope.args.vararg is not None:
            arguments.append(scope.args.vararg)
        if scope.args.kwarg is not None:
            arguments.append(scope.args.kwarg)
        for argument in arguments:
            candidates[argument.arg] = [_UNKNOWN]

    # The executing scope replaces straight-line assignments; outer/control-flow
    # candidates append conservatively.
    for statement in getattr(scope, "body", []):
        if isinstance(statement, ast.Assign) and (
            before_line is None or statement.lineno < before_line
        ):
            bindings = [
                binding
                for target in statement.targets
                for binding in _assignment_bindings(target, statement.value)
            ]
            visible_bindings = {**outer_bindings, **candidates}
            for name, value in bindings:
                resolved = _resolved_binding_values(
                    value,
                    visible_bindings=visible_bindings,
                )
                if before_line is None:
                    for binding in resolved:
                        add(name, binding, statement.lineno)
                else:
                    candidates[name] = resolved
            continue
        if (
            isinstance(statement, ast.AnnAssign)
            and statement.value is not None
            and (before_line is None or statement.lineno < before_line)
        ):
            if isinstance(statement.target, ast.Name):
                visible_bindings = {**outer_bindings, **candidates}
                resolved = _resolved_binding_values(
                    statement.value,
                    visible_bindings=visible_bindings,
                )
                if before_line is None:
                    for binding in resolved:
                        add(statement.target.id, binding, statement.lineno)
                else:
                    candidates[statement.target.id] = resolved
            continue
        if isinstance(statement, (ast.Import, ast.ImportFrom)) and (
            before_line is None or statement.lineno < before_line
        ):
            for name, value in _import_bindings(statement):
                if before_line is None:
                    add(name, value, statement.lineno)
                else:
                    candidates[name] = [value]
            continue
        Collector().visit(statement)
    return candidates


def _resolves_tables_module(
    node: ast.expr,
    *,
    locals_: dict[str, list[_BindingValue]],
    seen_names: frozenset[str],
) -> bool:
    if not isinstance(node, ast.Name) or node.id in seen_names:
        return False
    return any(
        isinstance(value, _TablesModuleBinding)
        or (
            isinstance(value, ast.expr)
            and _resolves_tables_module(
                value,
                locals_=locals_,
                seen_names=seen_names | {node.id},
            )
        )
        for value in locals_.get(node.id, [])
    )


def _static_table_names(
    node: ast.expr,
    *,
    locals_: dict[str, list[_BindingValue]],
    seen_names: frozenset[str],
) -> list[str]:
    if isinstance(node, ast.Attribute) and _resolves_tables_module(
        node.value,
        locals_=locals_,
        seen_names=seen_names,
    ):
        value = getattr(tables_module, node.attr, None)
        return [value.full_name] if isinstance(value, tables_module.TableRef) else []
    if not isinstance(node, ast.Name) or node.id in seen_names:
        return []
    return [
        table
        for value in locals_.get(node.id, [])
        for table in (
            [value.full_name]
            if isinstance(value, tables_module.TableRef)
            else (
                _static_table_names(
                    value,
                    locals_=locals_,
                    seen_names=seen_names | {node.id},
                )
                if isinstance(value, ast.expr)
                else []
            )
        )
    ]


def _static_full_name_targets(
    node: ast.expr,
    *,
    locals_: dict[str, list[_BindingValue]],
    seen_names: frozenset[str],
) -> list[str]:
    if isinstance(node, ast.Attribute) and node.attr == "full_name":
        return _static_table_names(
            node.value,
            locals_=locals_,
            seen_names=seen_names,
        )
    if not isinstance(node, ast.Name) or node.id in seen_names:
        return []
    return [
        table
        for value in locals_.get(node.id, [])
        if isinstance(value, ast.expr)
        for table in _static_full_name_targets(
            value,
            locals_=locals_,
            seen_names=seen_names | {node.id},
        )
    ]


def _static_sqls(
    node: ast.expr,
    *,
    locals_: dict[str, list[_BindingValue]],
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
            if isinstance(value, ast.expr)
            for sql in _static_sqls(
                value,
                locals_=locals_,
                seen_names=seen_names | {node.id},
            )
        ]
    if isinstance(node, ast.IfExp):
        return [
            sql
            for branch in (node.body, node.orelse)
            for sql in _static_sqls(
                branch,
                locals_=locals_,
                seen_names=seen_names,
            )
        ]
    if not isinstance(node, ast.JoinedStr):
        return []
    sqls = [""]
    for part in node.values:
        if isinstance(part, ast.Constant) and isinstance(part.value, str):
            choices = [part.value]
        elif isinstance(part, ast.FormattedValue):
            choices = _static_full_name_targets(
                part.value,
                locals_=locals_,
                seen_names=seen_names,
            ) or [" "]
        else:
            choices = [" "]
        sqls = [prefix + choice for prefix in sqls for choice in choices]
    return sqls


def _sql_without_literals_and_comments(sql: str) -> str:
    result: list[str] = []
    index = 0

    def mask(text: str) -> None:
        result.extend("\n" if char == "\n" else " " for char in text)

    while index < len(sql):
        if sql.startswith("--", index):
            end = sql.find("\n", index + 2)
            end = len(sql) if end == -1 else end
            mask(sql[index:end])
            index = end
            continue
        if sql.startswith("/*", index):
            start = index
            depth = 1
            index += 2
            while index < len(sql) and depth:
                if sql.startswith("/*", index):
                    depth += 1
                    index += 2
                elif sql.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            mask(sql[start:index])
            continue
        if sql[index] == "$":
            delimiter = re.match(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$", sql[index:])
            if delimiter is not None:
                start = index
                marker = delimiter.group(0)
                index += len(marker)
                end = sql.find(marker, index)
                index = len(sql) if end == -1 else end + len(marker)
                mask(sql[start:index])
                continue
        if sql[index] == "'":
            start = index
            escape_backslashes = index > 0 and sql[index - 1] in {"e", "E"}
            index += 1
            while index < len(sql):
                if escape_backslashes and sql[index] == "\\" and index + 1 < len(sql):
                    index += 2
                    continue
                if sql[index] != "'":
                    index += 1
                    continue
                if index + 1 < len(sql) and sql[index + 1] == "'":
                    index += 2
                    continue
                index += 1
                break
            mask(sql[start:index])
            continue
        if sql[index] == '"':
            start = index
            index += 1
            while index < len(sql):
                if sql[index] != '"':
                    index += 1
                    continue
                if index + 1 < len(sql) and sql[index + 1] == '"':
                    index += 2
                    continue
                index += 1
                break
            quoted = sql[start:index]
            identifier = quoted[1:-1].replace('""', '"')
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
                result.extend(quoted)
            else:
                mask(quoted)
            continue
        result.append(sql[index])
        index += 1
    return "".join(result)


def _static_table_targets(
    node: ast.expr,
    *,
    locals_: dict[str, list[_BindingValue]],
    seen_names: frozenset[str] = frozenset(),
) -> list[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.Attribute) and node.attr == "full_name":
        return _static_table_names(
            node.value,
            locals_=locals_,
            seen_names=seen_names,
        )
    if isinstance(node, ast.IfExp):
        return [
            table
            for branch in (node.body, node.orelse)
            for table in _static_table_targets(
                branch,
                locals_=locals_,
                seen_names=seen_names,
            )
        ]
    if isinstance(node, ast.JoinedStr):
        return _static_sqls(
            node,
            locals_=locals_,
            seen_names=seen_names,
        )
    if not isinstance(node, ast.Name) or node.id in seen_names:
        return []
    return [
        table
        for value in locals_.get(node.id, [])
        if isinstance(value, ast.expr)
        for table in _static_table_targets(
            value,
            locals_=locals_,
            seen_names=seen_names | {node.id},
        )
    ]


def _scope_bindings(
    node: ast.Call,
    *,
    tree: ast.Module,
    parents: dict[ast.AST, ast.AST],
) -> dict[str, list[_BindingValue]]:
    scopes: list[ast.AST] = []
    scope = node
    while scope is not tree:
        scope = parents[scope]
        if isinstance(
            scope,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
                ast.ListComp,
                ast.SetComp,
                ast.DictComp,
                ast.GeneratorExp,
            ),
        ):
            scopes.append(scope)
    scopes.append(tree)

    execution_scope = next(
        (
            scope
            for scope in scopes
            if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef))
        ),
        tree,
    )
    bindings: dict[str, list[_BindingValue]] = {}
    for enclosing_scope in reversed(scopes):
        if isinstance(
            enclosing_scope,
            (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp),
        ):
            bindings.update(
                _comprehension_bindings(
                    enclosing_scope,
                    node=node,
                    outer_bindings=bindings,
                )
            )
        else:
            bindings.update(
                _local_bindings(
                    enclosing_scope,
                    before_line=(
                        node.lineno if enclosing_scope is execution_scope else None
                    ),
                    outer_bindings=bindings,
                )
            )
    return bindings


def _static_call_methods(
    node: ast.expr,
    *,
    locals_: dict[str, list[_BindingValue]],
    seen_names: frozenset[str] = frozenset(),
) -> list[str]:
    if isinstance(node, ast.Attribute):
        return [node.attr]
    if not isinstance(node, ast.Name) or node.id in seen_names:
        return []
    return [
        method
        for value in locals_.get(node.id, [])
        if isinstance(value, ast.expr)
        for method in _static_call_methods(
            value,
            locals_=locals_,
            seen_names=seen_names | {node.id},
        )
    ]


def _violations_in_path(path: Path) -> list[_Violation]:
    is_repository = path.parts[-4:-1] == ("src", "moneybin", "repositories") and (
        path.name == "base.py" or path.name.endswith("_repo.py")
    )
    is_audit_service = path.parts[-4:] == (
        "src",
        "moneybin",
        "services",
        "audit_service.py",
    )
    is_migration = (
        path.parts[-5:-1] == ("src", "moneybin", "sql", "migrations")
        and path.name.startswith("V")
        and path.suffix == ".py"
    )
    if is_repository or is_audit_service or is_migration:
        return []
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }

    violations: list[_Violation] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        locals_ = _scope_bindings(node, tree=tree, parents=parents)
        call_methods = set(_static_call_methods(node.func, locals_=locals_))
        protected_tables: set[str] = set()
        if call_methods & {"execute", "executemany", "from_query", "query", "sql"}:
            query = (
                node.args[0]
                if node.args
                else next(
                    (
                        keyword.value
                        for keyword in node.keywords
                        if keyword.arg == "query"
                    ),
                    None,
                )
            )
            if query is not None:
                sqls = _static_sqls(
                    query,
                    locals_=locals_,
                )
                protected_tables.update(
                    f"{match.group(1)}.{match.group(2)}".lower()
                    for sql in sqls
                    for mutation_re in (_MUTATION_RE, _COPY_FROM_RE)
                    for match in mutation_re.finditer(
                        _sql_without_literals_and_comments(sql)
                    )
                    if f"{match.group(1)}.{match.group(2)}".lower() in _PROTECTED_TABLES
                )
        if "ingest_dataframe" in call_methods:
            target = (
                node.args[0]
                if node.args
                else next(
                    (
                        keyword.value
                        for keyword in node.keywords
                        if keyword.arg == "table"
                    ),
                    None,
                )
            )
            if target is not None:
                protected_tables.update(
                    table.lower()
                    for table in _static_table_targets(
                        target,
                        locals_=locals_,
                    )
                    if table.lower() in _PROTECTED_TABLES
                )
        if call_methods & {"append", "insert_into"}:
            target = (
                node.args[0]
                if node.args
                else next(
                    (
                        keyword.value
                        for keyword in node.keywords
                        if keyword.arg == "table_name"
                    ),
                    None,
                )
            )
            if target is not None:
                protected_tables.update(
                    table.lower()
                    for table in _static_table_targets(
                        target,
                        locals_=locals_,
                    )
                    if table.lower() in _PROTECTED_TABLES
                )
        for table in sorted(protected_tables):
            violations.append(
                _Violation(
                    path=path,
                    line=node.lineno,
                    table=table,
                )
            )
    return violations


def _runtime_violations(repository_root: Path) -> list[_Violation]:
    runtime_roots = (
        repository_root / "src" / "moneybin",
        repository_root / "scripts",
    )
    return [
        violation
        for runtime_root in runtime_roots
        for path in sorted(runtime_root.rglob("*.py"))
        for violation in _violations_in_path(path)
    ]


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


@pytest.mark.parametrize(
    "sql",
    [
        'DELETE FROM "app"."user_categories"',
        'UPDATE app."user_categories" SET active = FALSE',
        'INSERT INTO "app".user_categories VALUES (?)',
    ],
)
def test_rejects_quoted_protected_table_identifiers(
    tmp_path: Path,
    sql: str,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        f"def mutate(db):\n    db.execute({sql!r})\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=2, table="app.user_categories")
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


def test_rejects_protected_write_through_sql_method(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def delete(db):\n"
        '    db.sql(f"DELETE FROM {USER_CATEGORIES.full_name}")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=4, table="app.user_categories")
    ]


@pytest.mark.parametrize(
    "call",
    [
        'db.conn.query("DELETE FROM app.user_categories")',
        'db.conn.query(query="DELETE FROM app.user_categories")',
        'db.conn.from_query("DELETE FROM app.user_categories")',
        'db.conn.from_query(query="DELETE FROM app.user_categories")',
        'db.conn.append("app.user_categories", frame)',
        'db.conn.append(table_name="app.user_categories", df=frame)',
        'db.conn.sql("SELECT 1").insert_into("app.user_categories")',
        'db.conn.sql("SELECT 1").insert_into(table_name="app.user_categories")',
    ],
)
def test_rejects_protected_write_through_raw_duckdb_connection(
    tmp_path: Path,
    call: str,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        f"def mutate(db, frame):\n    {call}\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=2, table="app.user_categories")
    ]


@pytest.mark.parametrize(
    "call",
    [
        "db.ingest_dataframe(USER_CATEGORIES.full_name, frame)",
        "db.ingest_dataframe(table=USER_CATEGORIES.full_name, df=frame)",
        'db.ingest_dataframe(f"{USER_CATEGORIES.full_name}", frame)',
        'db.ingest_dataframe("app.user_categories", frame)',
    ],
)
def test_rejects_protected_ingest_dataframe_target(
    tmp_path: Path,
    call: str,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        f"from moneybin.tables import USER_CATEGORIES\n\ndef load(db, frame):\n    {call}\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=4, table="app.user_categories")
    ]


def test_rejects_protected_ingest_dataframe_local_target(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def load(db, frame):\n"
        "    table = USER_CATEGORIES.full_name\n"
        "    db.ingest_dataframe(table, frame)\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=5, table="app.user_categories")
    ]


@pytest.mark.parametrize(
    "call",
    [
        "db.ingest_dataframe({target}, frame)",
        "db.ingest_dataframe(table={target}, df=frame)",
        "db.conn.append({target}, frame)",
        "db.conn.append(table_name={target}, df=frame)",
    ],
)
@pytest.mark.parametrize(
    "target",
    [
        "USER_CATEGORIES.full_name if unsafe else DIM_ACCOUNTS.full_name",
        "DIM_ACCOUNTS.full_name if safe else USER_CATEGORIES.full_name",
    ],
)
def test_rejects_protected_conditional_ingestion_target(
    tmp_path: Path,
    call: str,
    target: str,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import DIM_ACCOUNTS, USER_CATEGORIES\n"
        "\n"
        "def load(db, frame, unsafe, safe):\n"
        f"    {call.format(target=target)}\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=4, table="app.user_categories")
    ]


def test_rejects_nested_conditional_ingestion_target_through_local_name(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import DIM_ACCOUNTS, USER_CATEGORIES\n"
        "\n"
        "def load(db, frame, first, second):\n"
        "    table = (DIM_ACCOUNTS.full_name if first else "
        "(USER_CATEGORIES.full_name if second else DIM_ACCOUNTS.full_name))\n"
        "    db.ingest_dataframe(table, frame)\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=5, table="app.user_categories")
    ]


def test_allows_safe_conditional_ingestion_target(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import DIM_ACCOUNTS, FCT_TRANSACTIONS\n"
        "\n"
        "def load(db, frame, accounts):\n"
        "    table = (DIM_ACCOUNTS.full_name if accounts "
        "else FCT_TRANSACTIONS.full_name)\n"
        "    db.ingest_dataframe(table, frame)\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


@pytest.mark.parametrize(
    ("binding", "call"),
    [
        (
            "run = db.execute",
            'run(f"DELETE FROM {USER_CATEGORIES.full_name}")',
        ),
        (
            "run = db.executemany",
            'run(f"INSERT INTO {USER_CATEGORIES.full_name} VALUES (?)", [])',
        ),
        (
            "run = db.sql",
            'run(f"UPDATE {USER_CATEGORIES.full_name} SET active = FALSE")',
        ),
        (
            "run = db.ingest_dataframe",
            "run(USER_CATEGORIES.full_name, frame)",
        ),
    ],
)
def test_rejects_protected_write_through_local_callable_alias(
    tmp_path: Path,
    binding: str,
    call: str,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def mutate(db, frame):\n"
        f"    {binding}\n"
        f"    {call}\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=5, table="app.user_categories")
    ]


@pytest.mark.parametrize(
    "call",
    [
        'db.execute(query=f"INSERT INTO {USER_CATEGORIES.full_name} VALUES (?)")',
        'db.executemany(query=f"INSERT INTO {USER_CATEGORIES.full_name} VALUES (?)", params=[["id"]])',
        'db.sql(query=f"DELETE FROM {USER_CATEGORIES.full_name}")',
    ],
)
def test_rejects_keyword_query_protected_write(
    tmp_path: Path,
    call: str,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        f"from moneybin.tables import USER_CATEGORIES\n\ndef create(db):\n    {call}\n",
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


def test_rejects_protected_write_through_module_import(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "import moneybin.tables as tables\n"
        "\n"
        "def create(db):\n"
        '    db.execute(f"INSERT INTO {tables.USER_CATEGORIES.full_name} VALUES (?)")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=4, table="app.user_categories")
    ]


def test_keeps_table_ref_import_aliases_scoped_to_their_functions(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "def delete_category(db):\n"
        "    from moneybin.tables import USER_CATEGORIES as table\n"
        '    db.execute(f"DELETE FROM {table.full_name}")\n'
        "\n"
        "def update_account(db):\n"
        "    from moneybin.tables import DIM_ACCOUNTS as table\n"
        '    db.execute(f"UPDATE {table.full_name} SET active = FALSE")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=3, table="app.user_categories")
    ]


def test_local_binding_shadows_outer_table_ref_import(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import DIM_ACCOUNTS, USER_CATEGORIES as table\n"
        "\n"
        "def update_account(db):\n"
        "    table = DIM_ACCOUNTS\n"
        '    db.execute(f"UPDATE {table.full_name} SET active = FALSE")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


def test_function_parameter_shadows_outer_table_ref_import(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES as table\n"
        "\n"
        "def update(db, table):\n"
        '    db.execute(f"UPDATE {table.full_name} SET active = FALSE")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


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


def test_rejects_protected_write_through_full_name_alias(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def delete(db):\n"
        "    table = USER_CATEGORIES.full_name\n"
        '    db.execute(f"DELETE FROM {table}")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=5, table="app.user_categories")
    ]


def test_ignores_mutation_text_in_fstring_value(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "def preview(db):\n"
        '    note = "DELETE FROM app.user_categories"\n'
        "    db.execute(f\"SELECT '{note}'\")\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'DELETE FROM app.user_categories'",
        "SELECT E'UPDATE app.user_categories SET active = FALSE'",
        "SELECT $$INSERT INTO app.user_categories VALUES (1)$$",
        "SELECT $note$DELETE FROM app.user_categories$note$",
        "SELECT '-- DELETE FROM app.user_categories'",
        "SELECT 'COPY app.user_categories FROM categories.csv'",
        "SELECT 'TRUNCATE app.user_categories'",
        "SELECT 'DROP TABLE app.user_categories'",
        "SELECT $$ALTER TABLE app.user_categories DROP active$$",
        "-- DELETE FROM app.user_categories\nSELECT 1",
        "-- COPY app.user_categories FROM categories.csv\nSELECT 1",
        "/* UPDATE app.user_categories SET active = FALSE */ SELECT 1",
        "/* TRUNCATE app.user_categories */ SELECT 1",
    ],
)
def test_ignores_mutation_text_in_sql_literals_and_comments(
    tmp_path: Path,
    sql: str,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        f"def preview(db):\n    db.execute({sql!r})\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


@pytest.mark.parametrize(
    "sql",
    [
        "-- harmless comment\nDELETE FROM app.user_categories",
        "SELECT 'safe'; DELETE FROM app.user_categories",
        "-- harmless comment\nDROP TABLE app.user_categories",
    ],
)
def test_rejects_protected_write_after_sql_literals_and_comments(
    tmp_path: Path,
    sql: str,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        f"def mutate(db):\n    db.execute({sql!r})\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=2, table="app.user_categories")
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


def test_rejects_protected_write_through_straight_line_unpacking(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def delete(db):\n"
        "    table, ignored = USER_CATEGORIES, None\n"
        '    db.execute(f"DELETE FROM {table.full_name}")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=5, table="app.user_categories")
    ]


def test_rejects_protected_write_through_control_flow_unpacking(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def delete(db, unsafe):\n"
        "    if unsafe:\n"
        "        [ignored, (table,)] = [None, (USER_CATEGORIES,)]\n"
        "    else:\n"
        "        [ignored, (table,)] = [None, (None,)]\n"
        '    db.execute(f"DELETE FROM {table.full_name}")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=8, table="app.user_categories")
    ]


def test_rejects_protected_write_through_for_loop_binding(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def delete(db):\n"
        "    for table in [USER_CATEGORIES]:\n"
        '        db.execute(f"DELETE FROM {table.full_name}")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=5, table="app.user_categories")
    ]


def test_rejects_protected_write_through_for_loop_unpacking(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "DELETIONS = {USER_CATEGORIES.full_name: 'WHERE TRUE'}\n"
        "\n"
        "def delete(db):\n"
        "    for table, where in DELETIONS.items():\n"
        '        db.execute(f"DELETE FROM {table} {where}")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=7, table="app.user_categories")
    ]


def test_rejects_for_loop_binding_through_seeded_alias_cycle(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def delete(db):\n"
        "    first = USER_CATEGORIES\n"
        "    second = first\n"
        "    first = second\n"
        "    for table in [first]:\n"
        '        db.execute(f"DELETE FROM {table.full_name}")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=8, table="app.user_categories")
    ]


def test_for_loop_binding_shadows_outer_table_ref_import(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import DIM_ACCOUNTS, USER_CATEGORIES as table\n"
        "\n"
        "def query(db):\n"
        "    for table in [DIM_ACCOUNTS]:\n"
        '        db.execute(f"UPDATE {table.full_name} SET active = FALSE")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


def test_unknown_for_loop_binding_shadows_outer_table_ref_import(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES as table\n"
        "\n"
        "def query(db, tables):\n"
        "    for table in tables:\n"
        '        db.execute(f"UPDATE {table.full_name} SET active = FALSE")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


@pytest.mark.parametrize(
    "expression",
    [
        '[db.execute(f"DELETE FROM {table.full_name}") for table in [USER_CATEGORIES]]',
        '{db.execute(f"DELETE FROM {table.full_name}") for table in [USER_CATEGORIES]}',
        '{table.full_name: db.execute(f"DELETE FROM {table.full_name}") for table in [USER_CATEGORIES]}',
        'list(db.execute(f"DELETE FROM {table.full_name}") for table in [USER_CATEGORIES])',
        '[db.execute(f"DELETE FROM {table.full_name}") for tables in [[USER_CATEGORIES]] for table in tables]',
        '[db.execute(f"DELETE FROM {table.full_name}") for _, table in [(0, USER_CATEGORIES)]]',
        '[None for table in [USER_CATEGORIES] if db.execute(f"DELETE FROM {table.full_name}")]',
        '[None for table in [USER_CATEGORIES] for _ in db.execute(f"DELETE FROM {table.full_name}")]',
    ],
)
def test_rejects_protected_write_through_comprehension_binding(
    tmp_path: Path,
    expression: str,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import DIM_ACCOUNTS as table, USER_CATEGORIES\n"
        "\n"
        "def delete(db):\n"
        f"    {expression}\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=4, table="app.user_categories")
    ]


def test_safe_comprehension_binding_shadows_protected_outer_import(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import DIM_ACCOUNTS, USER_CATEGORIES as table\n"
        "\n"
        "def update(db):\n"
        '    [db.execute(f"UPDATE {table.full_name} SET active = FALSE") '
        "for table in [DIM_ACCOUNTS]]\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


def test_unknown_comprehension_binding_shadows_protected_outer_import(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES as table\n"
        "\n"
        "def update(db, tables):\n"
        '    [db.execute(f"UPDATE {table.full_name} SET active = FALSE") '
        "for table in tables]\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


def test_comprehension_target_does_not_shadow_its_own_iterable(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES as table\n"
        "\n"
        "def delete(db):\n"
        '    [None for table in db.execute(f"DELETE FROM {table.full_name}")]\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=4, table="app.user_categories")
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
    "target",
    [
        "app.user_categories",
        '"app"."user_categories"',
    ],
)
def test_rejects_merge_into_protected_table(tmp_path: Path, target: str) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "def merge(db):\n"
        f"    db.execute('MERGE INTO {target} AS target USING source ON FALSE')\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=2, table="app.user_categories")
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE OR REPLACE TABLE app.user_categories (category_id VARCHAR)",
        'CREATE OR REPLACE TABLE "app"."user_categories" AS SELECT 1',
        "DROP TABLE app.user_categories",
        'DROP TABLE IF EXISTS "app"."user_categories"',
        "ALTER TABLE app.user_categories ADD COLUMN unsafe BOOLEAN",
        'ALTER TABLE IF EXISTS ONLY "app"."user_categories" DROP unsafe',
    ],
)
def test_rejects_destructive_ddl_of_protected_table(
    tmp_path: Path,
    sql: str,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        f"def mutate(db):\n    db.execute({sql!r})\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=2, table="app.user_categories")
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE app.user_categories (category_id VARCHAR)",
        "CREATE TABLE IF NOT EXISTS app.user_categories (category_id VARCHAR)",
        "DROP TABLE core.dim_accounts",
        "ALTER TABLE app.metrics ADD COLUMN value DOUBLE",
    ],
)
def test_allows_non_destructive_or_exempt_ddl(tmp_path: Path, sql: str) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        f"def mutate(db):\n    db.execute({sql!r})\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


def test_rejects_destructive_ddl_through_table_ref(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from moneybin.tables import USER_CATEGORIES\n"
        "\n"
        "def mutate(db):\n"
        '    db.execute(f"DROP TABLE {USER_CATEGORIES.full_name}")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=4, table="app.user_categories")
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "COPY app.user_categories FROM 'categories.csv'",
        'COPY "app"."user_categories"(category_id) FROM \'categories.csv\'',
        "TRUNCATE app.user_categories",
        'TRUNCATE TABLE "app"."user_categories"',
    ],
)
def test_rejects_copy_or_truncate_of_protected_table(
    tmp_path: Path,
    sql: str,
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        f"def mutate(db):\n    db.execute({sql!r})\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=2, table="app.user_categories")
    ]


def test_allows_copying_protected_table_to_file(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "def export(db):\n"
        "    db.execute(\"COPY app.user_categories TO 'categories.csv'\")\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


@pytest.mark.parametrize(
    "expression",
    [
        "'DELETE FROM app.user_categories' if unsafe else 'SELECT 1'",
        "'SELECT 1' if safe else 'DELETE FROM app.user_categories'",
    ],
)
def test_rejects_protected_write_in_conditional_expression(
    tmp_path: Path, expression: str
) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        f"def delete(db, unsafe, safe):\n    sql = {expression}\n    db.execute(sql)\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=3, table="app.user_categories")
    ]


def test_allows_safe_conditional_expression(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/example_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "def query(db, detailed):\n"
        "    sql = 'SELECT * FROM app.user_categories' if detailed else 'SELECT 1'\n"
        "    db.execute(sql)\n",
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


def test_allows_base_repository_writes(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/repositories/base.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "def delete(db):\n"
        '    db.execute("DELETE FROM app.user_categories WHERE category_id = ?")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == []


def test_rejects_repository_named_service_subdirectory(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/repositories/rogue_repo.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "def delete(db):\n"
        '    db.execute("DELETE FROM app.user_categories WHERE category_id = ?")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=2, table="app.user_categories")
    ]


def test_rejects_audit_service_named_outside_canonical_path(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/connectors/services/audit_service.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "def delete(db):\n"
        '    db.execute("DELETE FROM app.user_categories WHERE category_id = ?")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=2, table="app.user_categories")
    ]


def test_new_declared_app_table_is_protected_without_a_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        tables_module,
        "UNREPOSITORY_TABLE",
        tables_module.TableRef("app", "unrepository_table"),
        raising=False,
    )

    reloaded = runpy.run_path(__file__)

    assert "app.unrepository_table" in reloaded["_PROTECTED_TABLES"]


def test_metrics_remains_exempt_if_it_gains_a_table_ref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        tables_module,
        "METRICS",
        tables_module.TableRef("app", "metrics"),
        raising=False,
    )

    reloaded = runpy.run_path(__file__)

    assert "app.metrics" not in reloaded["_PROTECTED_TABLES"]


def test_rejects_migration_named_runtime_module(tmp_path: Path) -> None:
    source = tmp_path / "src/moneybin/services/migrations/V999_cleanup.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        'def delete(db):\n    db.execute("DELETE FROM app.user_categories")\n',
        encoding="utf-8",
    )

    assert _violations_in_path(source) == [
        _Violation(path=source, line=2, table="app.user_categories")
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
            "src/moneybin/sql/migrations/V999_test.py",
            "DROP TABLE app.user_categories",
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


def test_runtime_scan_includes_executable_scripts(tmp_path: Path) -> None:
    source = tmp_path / "scripts/maintenance.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        'def mutate(db):\n    db.execute("UPDATE app.user_categories SET active = FALSE")\n',
        encoding="utf-8",
    )

    assert _runtime_violations(tmp_path) == [
        _Violation(path=source, line=2, table="app.user_categories")
    ]


def test_runtime_app_mutations_are_repository_routed() -> None:
    violations = _runtime_violations(_REPOSITORY_ROOT)

    assert not violations, (
        "Raw protected app.* mutation bypasses a repository:\n"
        + "\n".join(
            f"{violation.path}:{violation.line}: {violation.table}"
            for violation in violations
        )
    )
