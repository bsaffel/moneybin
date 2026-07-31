"""Taxonomy + completeness tests for moneybin.error_codes."""

from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

from moneybin import error_codes

_ROOT = Path(__file__).parents[3]
_SRC_DIR = _ROOT / "src/moneybin"

# Cross-cutting prefixes, then one per MCP tool namespace. A code's prefix
# names the domain it came from, so an agent can branch on the family without
# enumerating every member.
VALID_PREFIXES = (
    "import_",
    "mutation_",
    "audit_",
    "refresh_",
    "undo_",
    "recovery_",
    "infra_",
    "sync_",
    "gsheet_",
    "price_feed_",
    "sql_",
    "account_",
    "entity_",
    "investment_",
    "privacy_",
    "report_",
    "review_",
    "taxonomy_",
    "transaction_",
)


def _all_code_constants() -> dict[str, str]:
    """Every uppercase module-level attribute whose value is a string."""
    return {
        name: value
        for name, value in vars(error_codes).items()
        if name.isupper() and isinstance(value, str)
    }


class TestErrorCodeTaxonomy:
    """Tests for error code taxonomy constraints."""

    def test_at_least_one_code_per_prefix(self) -> None:
        codes = set(_all_code_constants().values())
        for prefix in VALID_PREFIXES:
            assert any(c.startswith(prefix) for c in codes), (
                f"No error code uses prefix '{prefix}'"
            )

    def test_every_code_uses_valid_prefix(self) -> None:
        codes = _all_code_constants()
        for name, value in codes.items():
            assert value.startswith(VALID_PREFIXES), (
                f"{name}={value!r} does not start with any of {VALID_PREFIXES}"
            )

    def test_constant_name_matches_value_uppercase(self) -> None:
        """ENUM_LIKE constant names must mirror their string values."""
        codes = _all_code_constants()
        for name, value in codes.items():
            assert name == value.upper(), (
                f"Constant {name} = {value!r}; expected {value.upper()!r}"
            )

    def test_all_codes_lowercase_snake_case(self) -> None:
        codes = _all_code_constants()
        snake = re.compile(r"^[a-z][a-z0-9_]*$")
        for name, value in codes.items():
            assert snake.match(value), f"{name}={value!r} is not lowercase snake_case"

    def test_no_duplicate_values(self) -> None:
        codes = _all_code_constants()
        values = list(codes.values())
        duplicates = {v for v in values if values.count(v) > 1}
        assert not duplicates, f"Duplicate code values: {duplicates}"


def _emitted_code_literals(tree: ast.AST) -> list[tuple[int, str]]:
    """Literals passed as ``code=`` — what a raise site puts on the wire."""
    return [
        (node.lineno, keyword.value.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == "code"
        and isinstance(keyword.value, ast.Constant)
        and isinstance(keyword.value.value, str)
    ]


def _own_nodes(scope: ast.AST) -> Iterator[ast.AST]:
    """Nodes belonging to this scope's own body, not to a nested function's.

    `ast.walk` cannot express this: it is a flat generator over every
    descendant, so skipping a nested `FunctionDef` node still yields that
    function's children. A `code = ...` inside a nested body therefore leaked
    into the enclosing scope's bindings and resolved a runtime `code=code`
    nothing had declared — the guard passed on exactly the shape it exists to
    reject.

    `ast.Lambda` is deliberately *not* a boundary. Lambdas are absent from the
    scope list in :func:`_computed_code_expressions`, so excluding them here
    would leave a `code=` inside one belonging to no scope at all — unchecked
    rather than misattributed, which is the worse of the two failures.
    """
    for child in ast.iter_child_nodes(scope):
        if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        yield child
        yield from _own_nodes(child)


def _code_bindings(scope: ast.AST) -> dict[str, ast.expr]:
    """Names bound to a value inside one function body (params + assignments).

    Scoped deliberately: resolving against the whole module lets an unrelated
    `code = ...` elsewhere in the file decide what a local `code=code` means.
    """
    bindings: dict[str, ast.expr] = {}
    if isinstance(scope, ast.FunctionDef | ast.AsyncFunctionDef):
        args = scope.args
        for arg, default in zip(
            args.args[len(args.args) - len(args.defaults) :],
            args.defaults,
            strict=True,
        ):
            bindings[arg.arg] = default
        for arg, kw_default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
            if kw_default is not None:
                bindings[arg.arg] = kw_default
    for node in _own_nodes(scope):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    bindings[target.id] = node.value
    return bindings


def _computed_code_expressions(tree: ast.AST) -> list[int]:
    """Lines where ``code=`` is built at runtime instead of named.

    A literal is checkable; a value assembled at runtime is not.
    ``code=f"revert_{status}"`` put four undeclared strings on the wire while
    the literal scan saw nothing, because an f-string is an ``ast.JoinedStr``.
    Accepted forms are a literal, a reference to a declared constant, a
    conditional whose branches are all accepted, or a pass-through of an
    already-classified ``.code`` — everything else is rejected rather than
    evaluated.
    """
    scopes: list[ast.AST] = [
        tree,
        *(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        ),
    ]
    lines: set[int] = set()
    for scope in scopes:
        bindings = _code_bindings(scope)
        for node in _own_nodes(scope):
            if not isinstance(node, ast.Call):
                continue
            for keyword in node.keywords:
                if keyword.arg != "code":
                    continue
                if not _names_a_constant(keyword.value, bindings):
                    lines.add(keyword.value.lineno)
    return sorted(lines)


def _names_a_constant(node: ast.expr, assigned: dict[str, ast.expr]) -> bool:
    """Whether an expression's value is drawn from the declared taxonomy."""
    if isinstance(node, ast.Constant):
        return isinstance(node.value, str)
    if isinstance(node, ast.Attribute):
        # `error_codes.NAME`, or a pass-through of an already-classified code.
        return node.attr.isupper() or node.attr == "code"
    if isinstance(node, ast.Name):
        # A local alias — resolve it once; a self-reference is not a constant.
        target = assigned.get(node.id)
        if target is None or isinstance(target, ast.Name):
            return node.id.isupper()
        return _names_a_constant(target, assigned)
    if isinstance(node, ast.IfExp):
        return _names_a_constant(node.body, assigned) and _names_a_constant(
            node.orelse, assigned
        )
    if isinstance(node, ast.BoolOp):
        # `maybe_code or error_codes.FALLBACK` — the fallback is what runs when
        # nothing upstream supplied a code, so it is the value to pin.
        return _names_a_constant(node.values[-1], assigned)
    if isinstance(node, ast.Call):
        # `mapping.get(key, error_codes.FALLBACK)` — the fallback is declared
        # and the mapping's values are literals the emitted-literal scan sees.
        return bool(node.args) and _names_a_constant(node.args[-1], assigned)
    return False


def _compared_code_literals(tree: ast.AST) -> list[tuple[int, str]]:
    """Literals compared against a ``.code`` attribute — control flow reads.

    A code is a two-sided contract: something emits it and something branches
    on it. Renaming a code while a comparison still holds the old string does
    not fail loudly — the branch silently stops matching. That is exactly how
    a swallow-this-error arm in reviews.py started re-raising when
    ``schema_out_of_date`` became ``infra_schema_drift``.
    """

    def _reads_code(expr: ast.expr) -> bool:
        return isinstance(expr, ast.Attribute) and expr.attr == "code"

    def _strings(expr: ast.expr) -> list[str]:
        items = (
            expr.elts if isinstance(expr, ast.Tuple | ast.List | ast.Set) else [expr]
        )
        return [
            item.value
            for item in items
            if isinstance(item, ast.Constant) and isinstance(item.value, str)
        ]

    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            # Either orientation: `exc.code == "x"` and `"x" == exc.code`.
            if _reads_code(node.left):
                for operand in node.comparators:
                    found.extend((node.lineno, value) for value in _strings(operand))
            elif any(_reads_code(operand) for operand in node.comparators):
                found.extend((node.lineno, value) for value in _strings(node.left))
        elif isinstance(node, ast.Match) and _reads_code(node.subject):
            for case in node.cases:
                for pattern in ast.walk(case.pattern):
                    if isinstance(pattern, ast.MatchValue):
                        found.extend(
                            (case.pattern.lineno, value)
                            for value in _strings(pattern.value)
                        )
    return found


def _wire_code_literals() -> list[tuple[str, int, str]]:
    """Every error-code string hardcoded in ``src``, emitted or branched on.

    ``vars(error_codes)`` only proves the taxonomy is internally consistent —
    it says nothing about what actually reaches an agent. A hardcoded string at
    a raise site never appears there, which is how 104 undeclared codes shipped
    on the wire while the taxonomy tests stayed green. This walks the emitting
    and consuming surfaces instead.
    """
    literals: list[tuple[str, int, str]] = []
    for path in sorted(_SRC_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        relative = str(path.relative_to(_ROOT))
        for line, value in _emitted_code_literals(tree) + _compared_code_literals(tree):
            literals.append((relative, line, value))
    return literals


class TestWireCodes:
    """What reaches an agent, not just what the module declares."""

    def test_no_error_code_is_built_at_runtime(self) -> None:
        """A `code=` value must be a literal or an `error_codes.NAME` reference.

        Anything computed — an f-string, a variable assigned from bare strings —
        is unverifiable by the literal scan, and that is exactly how four
        `revert_*` codes and two cursor codes reached agents undeclared.
        """
        computed: list[str] = []
        for path in sorted(_SRC_DIR.rglob("*.py")):
            tree = ast.parse(path.read_text(), filename=str(path))
            relative = str(path.relative_to(_ROOT))
            computed.extend(
                f"{relative}:{line}" for line in _computed_code_expressions(tree)
            )
        computed.sort()

        assert computed == [], (
            f"{len(computed)} `code=` value(s) are built at runtime rather than "
            f"naming an error_codes constant:\n" + "\n".join(computed)
        )

    def test_every_hardcoded_code_is_a_declared_constant(self) -> None:
        declared = set(_all_code_constants().values())
        undeclared = sorted({
            f"{path}:{line}: {code!r}"
            for path, line, code in _wire_code_literals()
            if code not in declared
        })

        assert undeclared == [], (
            f"{len(undeclared)} hardcoded error-code string(s) are not declared "
            f"in moneybin.error_codes:\n" + "\n".join(undeclared)
        )


class TestSpecificCodes:
    """Codes called out explicitly in the spec must exist."""

    @pytest.mark.parametrize(
        "code",
        [
            "import_parse_error",
            "import_file_not_found",
            "import_format_unknown",
            "import_superseded",
            "mutation_constraint_violation",
            "mutation_not_found",
            "mutation_ambiguous",
            "mutation_confirmation_declined",
            "mutation_confirmation_expired",
            "mutation_confirmation_mismatch",
            "mutation_confirmation_replayed",
            "mutation_confirmation_required",
            "mutation_invalid_input",
            "mutation_nothing_to_do",
            "audit_fk_violation",
            "audit_sign_violation",
            "audit_unbalanced_transfer",
            "audit_orphan_state",
            "refresh_match_failed",
            "refresh_categorize_failed",
            "refresh_model_failed",
            "undo_operation_not_found",
            "undo_already_undone",
            "undo_cascade_blocked",
            "recovery_no_path",
            "infra_database_locked",
            "infra_invalid_input",
            "infra_not_found",
            "infra_file_not_found",
            "sync_error",
            "gsheet_error",
        ],
    )
    def test_code_exists(self, code: str) -> None:
        codes = set(_all_code_constants().values())
        assert code in codes, f"Code {code!r} not declared in error_codes"


class TestWireCodeScanner:
    """The scanner's own extraction logic, against synthetic sources.

    Everything else in `TestWireCodes` exercises the scanner only through the
    live tree, so a scanner that quietly stopped matching would read as "no
    findings" — the failure mode that let 104 undeclared codes ship.
    """

    @pytest.mark.parametrize(
        ("source", "expected"),
        [
            ('raise E("m", code="literal_value")', ["literal_value"]),
            ("raise E(\"m\", code='literal_value')", ["literal_value"]),
            ('if exc.code == "compared_value": pass', ["compared_value"]),
            ('if "compared_value" == exc.code: pass', ["compared_value"]),
            ('if exc.code in ("a_value", "b_value"): pass', ["a_value", "b_value"]),
            (
                "match exc.code:\n    case 'matched_value':\n        pass",
                ["matched_value"],
            ),
            ('raise E("m", code=error_codes.NAMED)', []),
        ],
    )
    def test_scanner_extracts_every_hardcoded_form(
        self, source: str, expected: list[str]
    ) -> None:
        tree = ast.parse(source)
        found = _emitted_code_literals(tree) + _compared_code_literals(tree)

        assert sorted(value for _, value in found) == sorted(expected)

    @pytest.mark.parametrize(
        ("source", "flagged"),
        [
            ('raise E("m", code=f"revert_{status}")', True),
            ('raise E("m", code=some_unresolvable)', True),
            ('raise E("m", code="literal")', False),
            ('raise E("m", code=error_codes.NAMED)', False),
            ('raise E("m", code=exc.code)', False),
            (
                "code = error_codes.A if flag else error_codes.B\n"
                'raise E("m", code=code)',
                False,
            ),
            (
                "def f(*, code=error_codes.DEFAULT):\n    raise E('m', code=code)",
                False,
            ),
            ('raise E("m", code=maybe or error_codes.FALLBACK)', False),
            # A nested function's assignment must not decide what an enclosing
            # runtime `code=code` means. `outer`'s parameter has no default, so
            # nothing declares it — the only `code = ...` in the file belongs to
            # a body that never runs on this path.
            (
                "def outer(code):\n"
                "    def inner():\n"
                '        code = "unrelated_literal"\n'
                "\n"
                "    raise E('m', code=code)",
                True,
            ),
        ],
    )
    def test_scanner_flags_only_runtime_built_codes(
        self, source: str, flagged: bool
    ) -> None:
        """Breaks if the runtime-built check stops rejecting f-strings.

        The f-string case is the one that shipped `revert_not_found` and three
        siblings to agents while every taxonomy test stayed green.
        """
        assert bool(_computed_code_expressions(ast.parse(source))) is flagged
