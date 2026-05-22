"""Taxonomy + completeness tests for moneybin.error_codes."""

from __future__ import annotations

import re

import pytest

from moneybin import error_codes

VALID_PREFIXES = (
    "import_",
    "mutation_",
    "audit_",
    "refresh_",
    "undo_",
    "recovery_",
    "infra_",
    "sync_",
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
            "mutation_invalid_input",
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
        ],
    )
    def test_code_exists(self, code: str) -> None:
        codes = set(_all_code_constants().values())
        assert code in codes, f"Code {code!r} not declared in error_codes"
