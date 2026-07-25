"""Taxonomy + completeness tests for moneybin.error_codes."""

from __future__ import annotations

import ast
import re
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


def _compared_code_literals(tree: ast.AST) -> list[tuple[int, str]]:
    """Literals compared against a ``.code`` attribute — control flow reads.

    A code is a two-sided contract: something emits it and something branches
    on it. Renaming a code while a comparison still holds the old string does
    not fail loudly — the branch silently stops matching. That is exactly how
    a swallow-this-error arm in reviews.py started re-raising when
    ``schema_out_of_date`` became ``infra_schema_drift``.
    """
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        reads_code = isinstance(node.left, ast.Attribute) and node.left.attr == "code"
        if not reads_code:
            continue
        for operand in node.comparators:
            candidates = (
                operand.elts
                if isinstance(operand, ast.Tuple | ast.List | ast.Set)
                else [operand]
            )
            found.extend(
                (node.lineno, item.value)
                for item in candidates
                if isinstance(item, ast.Constant) and isinstance(item.value, str)
            )
    return found


def _wire_code_literals() -> list[tuple[str, int, str]]:
    """Every error-code string hardcoded in ``src``, emitted or branched on.

    ``vars(error_codes)`` only proves the taxonomy is internally consistent —
    it says nothing about what actually reaches an agent. A hardcoded string at
    a raise site never appears there, which is how 103 undeclared codes shipped
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
