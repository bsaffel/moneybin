"""The client-visible tool count is a public-surface fact with a hard client ceiling.

MoneyBin retired client-driven progressive disclosure (`mcp-architecture.md` §3):
every registered tool is visible at connect, so "registered" and "visible" are the
same number. That number is not free — Cascade (Windsurf), a client `mcp install`
supports, enforces a hard ceiling on how many tools it will hold at once.

These tests pin the count so crossing a client's limit is a conscious act recorded
in a diff, not something a user discovers when their tools silently stop working.
"""

import asyncio
import inspect
import json
from pathlib import Path

import pytest
from fastmcp.tools import FunctionTool
from mcp.types import Tool

from moneybin.mcp.surface import (
    ADMITTED_OUTPUT_SCHEMA_NAMES,
    HARD_TOOL_LIMIT,
    STANDARD_TOOL_COUNT,
    STANDARD_TOOL_NAMES,
    WINDSURF_ACTIVE_TOOL_CAP,
    assert_surface_contract,
    description_budget_violations,
)
from moneybin.mcp.surface_inventory import SurfaceInventory

FIXTURES_PATH = Path(__file__).parents[2] / "fixtures/mcp_surface"
BASELINE_PATH = FIXTURES_PATH / "baseline-2026-07-17.json"
STANDARD_PATH = FIXTURES_PATH / "standard-45.json"

_REPLACED_TOOL_NAME_COHORTS = {
    "system_status": frozenset({
        "system_status",
        "system_doctor",
        "transactions_categorize_stats",
    }),
    "system_audit": frozenset({
        "system_audit",
        "system_audit_history",
        "system_audit_get",
    }),
    "accounts": frozenset({
        "accounts",
        "accounts_get",
        "accounts_summary",
        "accounts_resolve",
    }),
    "accounts_balances": frozenset({
        "accounts_balances",
        "accounts_balance_history",
        "accounts_balance_assertions",
        "accounts_balance_reconcile",
    }),
    "investments": frozenset({
        "investments",
        "investments_holdings",
        "investments_lots",
        "investments_gains",
        "investments_securities",
    }),
    "transactions": frozenset({"transactions_get"}),
    "transactions_categorize_rules": frozenset({"transactions_categorize_rules"}),
    "reviews": frozenset({
        "review",
        "transactions_categorize_pending",
        "transactions_categorize_auto_review",
        "transactions_matches_pending",
        "transactions_matches_history",
        "accounts_links_pending",
        "accounts_links_history",
        "merchants_links_pending",
        "merchants_links_history",
        "investments_securities_links_pending",
        "investments_securities_links_history",
    }),
    "taxonomy": frozenset({"categories", "merchants"}),
    "import_status": frozenset({
        "import_status",
        "import_formats",
        "import_inbox_pending",
    }),
    "gsheet": frozenset({"gsheet", "gsheet_status"}),
    "privacy": frozenset({"privacy_status", "privacy_log"}),
    "accounts_balance_assert": frozenset({
        "accounts_balance_assert",
        "accounts_balance_assertion_delete",
    }),
    "transactions_annotate": frozenset({
        "transactions_notes_add",
        "transactions_notes_edit",
        "transactions_notes_delete",
        "transactions_tags_set",
        "transactions_tags_rename",
        "transactions_splits_set",
    }),
    "transactions_categorize_rules_set": frozenset({
        "transactions_categorize_rules_create",
        "transactions_categorize_rules_delete",
    }),
    "reviews_decide": frozenset({
        "transactions_matches_set",
        "transactions_categorize_auto_accept",
    }),
    "identity_links_decide": frozenset({
        "accounts_links_set",
        "merchants_links_set",
        "investments_securities_links_set",
    }),
    "taxonomy_set": frozenset({
        "categories_create",
        "categories_set",
        "categories_delete",
        "merchants_create",
    }),
    "privacy_consent_set": frozenset({
        "privacy_consent_grant",
        "privacy_consent_revoke",
    }),
}

_CANONICAL_CARRYING_WEIGHT_BYTES = {
    "system_status": (663, 2_725),
    "system_audit": (746, 1_958),
    "accounts": (830, 2_240),
    "accounts_balances": (877, 2_786),
    "investments": (987, 4_908),
    "transactions": (1_287, 2_383),
    "transactions_categorize_rules": (564, 318),
    "reviews": (690, 8_687),
    "taxonomy": (669, 620),
    "import_status": (642, 1_236),
    "gsheet": (441, 1_016),
    "privacy": (590, 1_007),
    "accounts_balance_assert": (1_416, 1_679),
    "transactions_annotate": (2_641, 3_653),
    "transactions_categorize_rules_set": (2_965, 2_670),
    "reviews_decide": (1_802, 2_566),
    "identity_links_decide": (2_758, 5_762),
    "taxonomy_set": (3_337, 3_223),
    "privacy_consent_set": (1_217, 2_188),
}

_STANDARD_CALLBACK_NAMES = {
    "system_status": "system_status_coarse",
    "system_audit": "system_audit_coarse",
    "system_audit_undo": "system_audit_undo",
    "reports": "reports",
    "accounts": "accounts_coarse",
    "accounts_set": "accounts_set",
    "accounts_balances": "accounts_balances_coarse",
    "accounts_balance_assert": "accounts_balance_assert_coarse",
    "investments": "investments_coarse",
    "investments_record": "investments_record",
    "investments_securities_set": "investments_securities_set",
    "investments_lots_select": "investments_lots_select",
    "transactions": "transactions_coarse",
    "transactions_create": "transactions_create",
    "transactions_annotate": "transactions_annotate_coarse",
    "transactions_categorize_assist": "transactions_categorize_assist",
    "transactions_categorize_commit": "transactions_categorize_commit",
    "transactions_categorize_run": "transactions_categorize_run",
    "transactions_categorize_rules": "transactions_categorize_rules_coarse",
    "transactions_categorize_rules_set": ("transactions_categorize_rules_set_coarse"),
    "reviews": "reviews_coarse",
    "reviews_decide": "reviews_decide_coarse",
    "identity_links_decide": "identity_links_decide_coarse",
    "taxonomy": "taxonomy_coarse",
    "taxonomy_set": "taxonomy_set_coarse",
    "import_files": "import_files_coarse",
    "import_preview": "import_preview_coarse",
    "import_confirm": "import_confirm_coarse",
    "import_status": "import_status_coarse",
    "import_revert": "import_revert_coarse",
    "import_inbox_sync": "import_inbox_sync_coarse",
    "import_labels_set": "import_labels_set_coarse",
    "sync_link": "sync_link_coarse",
    "sync_status": "sync_status_coarse",
    "sync_pull": "sync_pull_coarse",
    "sync_disconnect": "sync_disconnect",
    "gsheet": "gsheet_coarse",
    "gsheet_connect": "gsheet_connect_coarse",
    "gsheet_pull": "gsheet_pull_coarse",
    "gsheet_disconnect": "gsheet_disconnect_coarse",
    "privacy": "privacy_coarse",
    "privacy_consent_set": "privacy_consent_set_coarse",
    "refresh_run": "refresh_run",
    "sql_query": "sql_query",
    "sql_schema": "sql_schema",
}

# The declared counts live in `moneybin.mcp.surface` because `mcp install` cites them
# in its Windsurf warning and cannot afford to boot the server to compute them. This
# module is what keeps that declaration honest against the live registry — bump it
# deliberately, not reflexively: read `docs/guides/mcp-clients.md` → Windsurf first,
# and if the change pushes us further past the cap, say so in the PR.


def _visible_tool_names() -> set[str]:
    """Tool names a connecting client actually receives (visibility filters applied)."""
    from moneybin.mcp.server import init_db, mcp

    init_db()
    return {tool.name for tool in asyncio.run(mcp.list_tools())}


def _inventory_server_sync() -> SurfaceInventory:
    from scripts.mcp_surface_snapshot import inventory_server

    return asyncio.run(inventory_server())


def _load_inventory(path: Path) -> SurfaceInventory:
    payload = json.loads(path.read_text())
    tools = [Tool.model_validate(row["definition"]) for row in payload["tools"]]
    return SurfaceInventory.from_tools(tools)


def _inventory(*tools: Tool) -> SurfaceInventory:
    return SurfaceInventory.from_tools(list(tools))


def _tool(
    name: str,
    description: str | None = "Describe a distinct operation.",
) -> Tool:
    return Tool(
        name=name,
        description=description,
        inputSchema={"type": "object"},
    )


def test_surface_contract_rejects_name_drift() -> None:
    inventory = _inventory(_tool("accounts"))

    with pytest.raises(AssertionError, match="Missing: transactions"):
        assert_surface_contract(
            inventory,
            expected_names=frozenset({"transactions"}),
            enforce_hard_limit=False,
            enforce_description_budget=False,
        )


def test_surface_contract_enforces_hard_limit_when_enabled() -> None:
    inventory = _inventory(*[_tool(f"tool_{index}") for index in range(51)])

    with pytest.raises(AssertionError, match="exceeds 50 tools"):
        assert_surface_contract(
            inventory,
            expected_names=frozenset(tool.name for tool in inventory.tools),
            enforce_hard_limit=True,
            enforce_description_budget=False,
        )


def test_description_budget_violations_measure_each_kind_of_debt() -> None:
    shared_opening = (
        "A distinct operation does useful work for this domain and its callers."
    )
    inventory = _inventory(
        _tool("duplicate_one", f"{shared_opening} First variation."),
        _tool("duplicate_two", f"{shared_opening} Second variation."),
        _tool("long_sentence", f"{'x' * 121}. Short tail."),
        _tool("long_description", f"Short. {'x' * 900}"),
    )

    violations = description_budget_violations(inventory)
    observed = {(violation.tool_name, violation.budget) for violation in violations}

    assert ("duplicate_one", "opening") in observed
    assert ("duplicate_two", "opening") in observed
    assert ("long_sentence", "first_sentence") in observed
    assert ("long_description", "description") in observed


@pytest.mark.parametrize("description", [None, "", "   "])
def test_description_budget_rejects_missing_or_empty_prose(
    description: str | None,
) -> None:
    inventory = _inventory(_tool("missing_description", description))

    violations = description_budget_violations(inventory)

    assert [
        (
            violation.tool_name,
            violation.budget,
            violation.actual,
            violation.limit,
        )
        for violation in violations
    ] == [("missing_description", "missing_description", 0, 1)]


def test_surface_contract_enforces_description_budget_when_enabled() -> None:
    inventory = _inventory(_tool("long_description", f"Short. {'x' * 900}"))

    with pytest.raises(AssertionError, match="description budget"):
        assert_surface_contract(
            inventory,
            expected_names=frozenset({"long_description"}),
            enforce_hard_limit=False,
            enforce_description_budget=True,
        )


@pytest.mark.integration
def test_standard_surface_is_smaller_than_baseline() -> None:
    baseline = _load_inventory(BASELINE_PATH)
    standard = _inventory_server_sync()

    assert baseline.total_bytes == 90_734
    assert standard.tool_count == 45
    assert standard.total_bytes < baseline.total_bytes
    assert {
        row.name for row in standard.tools if row.output_schema_bytes > 0
    } == ADMITTED_OUTPUT_SCHEMA_NAMES


@pytest.mark.integration
def test_no_advertised_aliases() -> None:
    actual = {row.name for row in _inventory_server_sync().tools}

    assert actual == STANDARD_TOOL_NAMES


@pytest.mark.integration
def test_standard_snapshot_matches_live_surface() -> None:
    expected = json.loads(STANDARD_PATH.read_text())
    actual = _inventory_server_sync()

    assert expected == actual.to_dict()


def test_carrying_weight_cohorts_use_canonical_definition_bytes() -> None:
    baseline = _load_inventory(BASELINE_PATH)
    standard = _load_inventory(STANDARD_PATH)
    baseline_rows = {row.name: row for row in baseline.tools}
    standard_rows = {row.name: row for row in standard.tools}

    assert set(_REPLACED_TOOL_NAME_COHORTS) == set(_CANONICAL_CARRYING_WEIGHT_BYTES)
    replaced_names = [
        name for cohort in _REPLACED_TOOL_NAME_COHORTS.values() for name in cohort
    ]
    assert len(replaced_names) == len(set(replaced_names))
    for operation, cohort in _REPLACED_TOOL_NAME_COHORTS.items():
        candidate_bytes, replaced_bytes = _CANONICAL_CARRYING_WEIGHT_BYTES[operation]
        assert standard_rows[operation].total_bytes == candidate_bytes
        assert sum(baseline_rows[name].total_bytes for name in cohort) == (
            replaced_bytes
        )


@pytest.mark.integration
def test_live_surface_matches_standard_registry() -> None:
    inventory = _inventory_server_sync()
    actual_names = frozenset(tool.name for tool in inventory.tools)

    assert actual_names == STANDARD_TOOL_NAMES, (
        f"missing={sorted(STANDARD_TOOL_NAMES - actual_names)!r}; "
        f"added={sorted(actual_names - STANDARD_TOOL_NAMES)!r}"
    )
    assert STANDARD_TOOL_COUNT == len(STANDARD_TOOL_NAMES) == inventory.tool_count
    assert STANDARD_TOOL_COUNT <= HARD_TOOL_LIMIT
    assert_surface_contract(
        inventory,
        expected_names=STANDARD_TOOL_NAMES,
        enforce_hard_limit=True,
        enforce_description_budget=True,
    )
    advertised = frozenset(
        tool.name for tool in inventory.tools if tool.output_schema_bytes > 0
    )
    assert advertised == ADMITTED_OUTPUT_SCHEMA_NAMES


@pytest.mark.integration
def test_standard_surface_fits_windsurf_active_tool_cap() -> None:
    assert STANDARD_TOOL_COUNT < WINDSURF_ACTIVE_TOOL_CAP


@pytest.mark.integration
async def test_live_tools_preserve_callback_schema_annotation_and_actor_identity() -> (
    None
):
    from moneybin.mcp.server import init_db, mcp

    init_db()
    callbacks: list[object] = []
    for name in sorted(STANDARD_TOOL_NAMES):
        tool = await mcp.get_tool(name)
        assert tool is not None
        assert isinstance(tool, FunctionTool)
        wire_callback = tool.fn
        closure = inspect.getclosurevars(wire_callback).nonlocals
        callback = closure["fn"]
        callbacks.append(callback)

        assert callback.__name__ == _STANDARD_CALLBACK_NAMES[name]
        actor = closure["privacy_actor"] or callback.__name__
        assert actor == name
        assert set(tool.parameters["properties"]) == set(
            inspect.signature(callback).parameters
        )
        assert tool.output_schema is None
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is getattr(
            callback, "_mcp_read_only", True
        )
        assert tool.annotations.destructiveHint is getattr(
            callback, "_mcp_destructive", False
        )
        assert tool.annotations.idempotentHint is getattr(
            callback, "_mcp_idempotent", True
        )
        assert tool.annotations.openWorldHint is getattr(
            callback, "_mcp_open_world", False
        )

    assert len({id(callback) for callback in callbacks}) == STANDARD_TOOL_COUNT


@pytest.mark.integration
def test_nothing_is_hidden_from_connecting_clients() -> None:
    """Guards the §3 claim itself: no tool is quietly withheld at connect.

    If this ever fails, MoneyBin has grown a hidden-tool tier — which would change
    the Windsurf math below and mean the docs (and the cap arithmetic) are stale.
    """
    from moneybin.mcp.server import init_db, mcp

    init_db()
    registered = {tool.name for tool in asyncio.run(mcp._list_tools())}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]  # public API filters by visibility; we want the raw registry
    assert registered == _visible_tool_names()
