"""Facts about the client-visible MCP tool surface.

Plain constants, no FastMCP import: `mcp install` needs to cite these and must not
pay the server's import cost to do it (`.claude/rules/cli.md`, Cold-Start Hygiene).
They are kept honest by `tests/moneybin/test_mcp/test_tool_surface_budget.py`, which
asserts them against the live registry — so they cannot quietly go stale.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from moneybin.mcp.surface_inventory import SurfaceInventory


# Cascade (Windsurf): "Cascade has a limit of 100 total tools that it has access to
# at any given time." Verified 2026-07-11 against the Windsurf MCP docs. It is a
# ceiling on ACTIVE tools across ALL of the user's MCP servers, so MoneyBin's share
# of the budget is smaller still for anyone running a second server alongside us.
WINDSURF_ACTIVE_TOOL_CAP = 100

STANDARD_TOOL_NAMES = frozenset({
    "system_status",
    "system_audit",
    "system_audit_undo",
    "reports",
    "accounts",
    "accounts_set",
    "accounts_balances",
    "accounts_balance_assert",
    "investments",
    "investments_record",
    "investments_securities_set",
    "investments_lots_select",
    "transactions",
    "transactions_create",
    "transactions_annotate",
    "transactions_categorize_assist",
    "transactions_categorize_commit",
    "transactions_categorize_run",
    "transactions_categorize_rules",
    "transactions_categorize_rules_set",
    "reviews",
    "reviews_decide",
    "identity_links_decide",
    "taxonomy",
    "taxonomy_set",
    "import_files",
    "import_preview",
    "import_confirm",
    "import_status",
    "import_revert",
    "import_inbox_sync",
    "import_labels_set",
    "sync_link",
    "sync_status",
    "sync_pull",
    "sync_disconnect",
    "gsheet",
    "gsheet_connect",
    "gsheet_pull",
    "gsheet_disconnect",
    "privacy",
    "privacy_consent_set",
    "refresh_run",
    "sql_query",
    "sql_schema",
})
STANDARD_TOOL_COUNT = 45
VISIBLE_TOOL_COUNT = STANDARD_TOOL_COUNT
HARD_TOOL_LIMIT = 50
CARRYING_WEIGHT_REVIEW_AT = 40
ADMITTED_OUTPUT_SCHEMA_NAMES: frozenset[str] = frozenset()

DESCRIPTION_OPENING_LENGTH = 60
FIRST_SENTENCE_CHARACTER_LIMIT = 120
DESCRIPTION_CHARACTER_LIMIT = 900


@dataclass(frozen=True, slots=True)
class DescriptionBudgetViolation:
    """One description that exceeds a standard-registry prose budget."""

    tool_name: str
    budget: str
    actual: int
    limit: int


def assert_surface_contract(
    inventory: SurfaceInventory,
    *,
    expected_names: frozenset[str],
    enforce_hard_limit: bool,
    enforce_description_budget: bool,
) -> None:
    """Assert the selected registry still has its frozen names and enabled gates."""
    actual_names = frozenset(row.name for row in inventory.tools)
    if actual_names != expected_names:
        raise AssertionError(_name_delta(expected_names, actual_names))
    if inventory.tool_count != len(actual_names):
        raise AssertionError("standard MCP registry contains duplicate tool names")
    if enforce_hard_limit and inventory.tool_count > HARD_TOOL_LIMIT:
        raise AssertionError(f"standard MCP registry exceeds {HARD_TOOL_LIMIT} tools")
    if enforce_description_budget:
        _assert_description_budget(inventory)


def description_budget_violations(
    inventory: SurfaceInventory,
) -> tuple[DescriptionBudgetViolation, ...]:
    """Return all opening, sentence, and total-description budget violations."""
    descriptions = {
        row.name: str(row.definition.get("description", "")) for row in inventory.tools
    }
    violations: list[DescriptionBudgetViolation] = []
    for tool_name, description in descriptions.items():
        if not description.strip():
            violations.append(
                DescriptionBudgetViolation(
                    tool_name=tool_name,
                    budget="missing_description",
                    actual=0,
                    limit=1,
                )
            )
            continue
        first_sentence = _first_sentence(description)
        if len(first_sentence) > FIRST_SENTENCE_CHARACTER_LIMIT:
            violations.append(
                DescriptionBudgetViolation(
                    tool_name=tool_name,
                    budget="first_sentence",
                    actual=len(first_sentence),
                    limit=FIRST_SENTENCE_CHARACTER_LIMIT,
                )
            )
        if len(description) > DESCRIPTION_CHARACTER_LIMIT:
            violations.append(
                DescriptionBudgetViolation(
                    tool_name=tool_name,
                    budget="description",
                    actual=len(description),
                    limit=DESCRIPTION_CHARACTER_LIMIT,
                )
            )

    for opening, names in _opening_groups(descriptions).items():
        if len(names) > 1:
            violations.extend(
                DescriptionBudgetViolation(
                    tool_name=name,
                    budget="opening",
                    actual=len(opening),
                    limit=DESCRIPTION_OPENING_LENGTH,
                )
                for name in names
            )
    return tuple(violations)


def _assert_description_budget(inventory: SurfaceInventory) -> None:
    violations = description_budget_violations(inventory)
    if violations:
        details = ", ".join(
            f"{violation.tool_name}:{violation.budget}" for violation in violations
        )
        raise AssertionError(f"description budget violations: {details}")


def _name_delta(expected_names: frozenset[str], actual_names: frozenset[str]) -> str:
    missing = ", ".join(sorted(expected_names - actual_names)) or "none"
    unexpected = ", ".join(sorted(actual_names - expected_names)) or "none"
    return f"MCP tool-name contract drift. Missing: {missing}; unexpected: {unexpected}"


def _first_sentence(description: str) -> str:
    for index, character in enumerate(description):
        if character in ".!?":
            return description[: index + 1]
    return description


def _opening_groups(descriptions: dict[str, str]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for name, description in descriptions.items():
        if description.strip():
            groups.setdefault(description[:DESCRIPTION_OPENING_LENGTH], []).append(name)
    return groups
