"""Durable documentation checks for the bounded MCP registry."""

from __future__ import annotations

import json
import re
from pathlib import Path

from fastmcp.tools import FunctionTool

from moneybin.mcp.surface import STANDARD_TOOL_NAMES

ROOT = Path(__file__).parents[3]
SCALING_SPEC = ROOT / "docs/specs/mcp-tool-surface-scaling.md"
ARCHITECTURE_SPEC = ROOT / "docs/specs/mcp-architecture.md"
MCP_SPEC = ROOT / "docs/specs/moneybin-mcp.md"
ARCHIVED_MCP_SPEC = ROOT / "docs/specs/archived/moneybin-mcp-pre-cutover.md"
CAPABILITIES_SPEC = ROOT / "docs/specs/moneybin-capabilities.md"
EXTENSIONS_SPEC = ROOT / "docs/specs/extension-contracts.md"
INDEX = ROOT / "docs/specs/INDEX.md"
ADR = ROOT / "docs/decisions/016-bounded-mcp-tool-registry.md"
MCP_RULE = ROOT / ".claude/rules/mcp.md"
SURFACE_RULE = ROOT / ".claude/rules/surface-design.md"
RESOURCES = ROOT / "src/moneybin/mcp/resources.py"
PROMPTS = ROOT / "src/moneybin/mcp/prompts.py"
CHANGELOG = ROOT / "CHANGELOG.md"
CLIENT_GUIDE = ROOT / "docs/guides/mcp-clients.md"
MCP_SERVER_GUIDE = ROOT / "docs/guides/mcp-server.md"
STANDARD_SNAPSHOT = ROOT / "tests/fixtures/mcp_surface/standard-45.json"
BASELINE_SNAPSHOT = ROOT / "tests/fixtures/mcp_surface/baseline-2026-07-17.json"
PUBLIC_MCP_GUIDES = (*sorted((ROOT / "docs/guides").rglob("*.md")),)


def test_documented_standard_names_match_runtime() -> None:
    text = SCALING_SPEC.read_text()
    registry = text.partition("## Standard registry")[2].partition(
        "### Review decision persistence"
    )[0]
    documented = frozenset(re.findall(r"`([a-z][a-z0-9_]+)`", registry))

    assert documented == STANDARD_TOOL_NAMES


def test_governing_spec_records_runtime_facts_without_promotion_claim() -> None:
    text = " ".join(SCALING_SPEC.read_text().split())

    for fact in (
        "45 tools",
        "44,926 bytes",
        "be0982903e876d4d9420160eeb9045e22613fc506ec1c9ebca620b8c76075ece",
        "90,734 bytes",
        "ea87a21b01e0f5181b80cef120beef2e9f46b31df121c7941329d9c493b48f79",
        "-45,808 bytes (-50.5%)",
        "zero advertised output schemas",
        "contract_passed: true",
        "promotion_ready: false",
        "context budget: not_observed",
        "host-native deferral: not_observed",
    ):
        assert fact in text
    assert "**Status:** in-progress" in text
    assert "**Status:** implemented" not in text


def test_governance_describes_one_current_registry_and_future_admission() -> None:
    for path in (
        ARCHITECTURE_SPEC,
        MCP_SPEC,
        CAPABILITIES_SPEC,
        EXTENSIONS_SPEC,
        INDEX,
        ADR,
        MCP_RULE,
        SURFACE_RULE,
        CLIENT_GUIDE,
        MCP_SERVER_GUIDE,
    ):
        text = " ".join(path.read_text().split())
        assert "45-tool standard registry" in text, path
        assert "same registry" in text, path

    adr = " ".join(ADR.read_text().split())
    rule = " ".join(MCP_RULE.read_text().split())
    extensions = " ".join(EXTENSIONS_SPEC.read_text().split())
    client_guide = " ".join(CLIENT_GUIDE.read_text().split())
    assert "**Status:** Proposed" in adr
    assert "promotion_ready: false" in adr
    assert "seven-question admission record" in rule
    assert "reports never consume tool slots" in extensions
    assert "without reconnect, packs, or profiles" in client_guide


def test_current_mcp_guidance_uses_only_standard_tool_names() -> None:
    prompt_text = PROMPTS.read_text()
    resource_text = RESOURCES.read_text()

    assert "accounts_balances" in prompt_text
    assert "accounts(view='balances')" not in prompt_text
    assert "sql_query" in resource_text
    assert "45-tool standard registry" in resource_text


def test_runtime_mcp_modules_do_not_point_to_removed_spec_sections() -> None:
    for path in (PROMPTS, RESOURCES):
        text = path.read_text()
        assert "moneybin-mcp.md`` section 14" not in text
        assert "moneybin-mcp.md`` section 15" not in text


def test_changelog_records_prelaunch_surface_cutover() -> None:
    text = CHANGELOG.read_text()

    assert "45-tool standard registry" in text
    assert "pre-launch" in text
    assert "reports" in text


def test_spec_index_describes_the_current_mcp_contract() -> None:
    row = next(
        line for line in INDEX.read_text().splitlines() if "[MoneyBin MCP]" in line
    )

    for current_fact in (
        "45-tool standard registry",
        "seven prompts",
        "single `reports` catalog",
        "outcome parity",
        "zero output schemas",
        "Promotion",
    ):
        assert current_fact in row
    assert "`reports_*`" not in row
    assert "sync + transform" not in row


def test_active_governance_does_not_teach_legacy_registry_names() -> None:
    active_paths = (
        ARCHITECTURE_SPEC,
        MCP_SPEC,
        EXTENSIONS_SPEC,
        MCP_RULE,
        SURFACE_RULE,
        CLIENT_GUIDE,
        MCP_SERVER_GUIDE,
    )
    stale_terms = (
        "reports_spending",
        "reports_cashflow",
        "reports_networth",
        "transactions_get",
        "accounts_get",
        "transactions_review",
        "privacy_status",
        "accounts_links_pending",
        "gsheet_reconnect",
        "105-tool",
        "approximately 45",
        "proposal does not change operating",
    )

    for path in active_paths:
        text = path.read_text()
        for term in stale_terms:
            assert term not in text, f"{path}: {term}"


def test_public_mcp_guides_do_not_teach_retired_tool_names() -> None:
    baseline = json.loads(BASELINE_SNAPSHOT.read_text())
    current = json.loads(STANDARD_SNAPSHOT.read_text())
    # One-word retired tool names overlap with preserved CLI command words.
    # Underscore names are unambiguously MCP public identifiers in prose.
    retired_names = {
        tool["name"] for tool in baseline["tools"] if "_" in tool["name"]
    } - {tool["name"] for tool in current["tools"]}

    for path in PUBLIC_MCP_GUIDES:
        text = path.read_text()
        taught = {
            name for name in retired_names if re.search(rf"`{re.escape(name)}`", text)
        }
        assert not taught, f"{path}: retired MCP tools {sorted(taught)}"


def test_mcp_spec_is_current_and_archives_the_pre_cutover_catalog() -> None:
    text = MCP_SPEC.read_text()
    registry = text.partition("## Standard registry")[2].partition(
        "## Contract matrix"
    )[0]
    documented = frozenset(re.findall(r"`([a-z][a-z0-9_]+)`", registry))

    assert documented == STANDARD_TOOL_NAMES
    assert ARCHIVED_MCP_SPEC.exists()
    assert "Archived pre-cutover catalog" in ARCHIVED_MCP_SPEC.read_text()


async def test_mcp_spec_enumerates_the_registered_prompts_and_resource() -> None:
    from moneybin.mcp.server import init_db, mcp

    init_db()
    text = MCP_SPEC.read_text()
    prompt_section = text.partition("### Registered prompts")[2].partition(
        "### Resources"
    )[0]
    documented_prompts = set(re.findall(r"`([a-z][a-z0-9_]+)`", prompt_section))
    registered_prompts = {
        prompt.name for prompt in await mcp.list_prompts(run_middleware=False)
    }

    assert documented_prompts == registered_prompts
    assert "`moneybin://schema`" in text
    architecture = ARCHITECTURE_SPEC.read_text()
    assert "Seven prompts" in architecture
    assert "`sync_review`" in architecture


def test_mcp_contract_matrix_uses_the_snapshot_input_property_names() -> None:
    matrix = (
        MCP_SPEC
        .read_text()
        .partition("## Contract matrix")[2]
        .partition("## Response contract")[0]
    )
    documented = dict(re.findall(r"^\| `([^`]+)` \| (.*?) \|", matrix, re.MULTILINE))
    snapshot = json.loads(STANDARD_SNAPSHOT.read_text())

    expected = {
        tool["name"]: ", ".join(
            f"`{property_name}`"
            for property_name in sorted(tool["definition"]["inputSchema"]["properties"])
        )
        for tool in snapshot["tools"]
    }

    assert documented == expected


async def test_mcp_contract_matrix_matches_live_sensitivity_metadata() -> None:
    from inspect import getclosurevars

    from moneybin.mcp.server import init_db, mcp

    init_db()
    matrix = (
        MCP_SPEC
        .read_text()
        .partition("## Contract matrix")[2]
        .partition("## Response contract")[0]
    )
    documented = dict(
        re.findall(r"^\| `([^`]+)` \| .*? \| .*? \| (.*?) \|$", matrix, re.MULTILINE)
    )

    assert set(documented) == STANDARD_TOOL_NAMES
    for name in STANDARD_TOOL_NAMES:
        tool = await mcp.get_tool(name)
        assert isinstance(tool, FunctionTool)
        callback = getclosurevars(tool.fn).nonlocals["fn"]
        maximum = callback._mcp_maximum_sensitivity  # type: ignore[attr-defined]
        assert maximum is not None, f"{name}: missing declared maximum sensitivity"
        sensitivity = maximum.value
        is_dynamic = callback._mcp_dynamic_classification  # type: ignore[attr-defined]
        safety = documented[name].lower()

        assert f"maximum {sensitivity}" in safety, f"{name}: {documented[name]}"
        if is_dynamic:
            assert "dynamic" in safety, f"{name}: {documented[name]}"

    assert (
        documented["reports"].lower()
        == "read / dynamic / maximum critical / report-derived"
    )


def test_mcp_rule_repeats_the_exact_seven_question_admission_record() -> None:
    text = MCP_RULE.read_text()
    admission = text.partition("**Admission sequence.**")[2].partition(
        "**Output-schema admission.**"
    )[0]

    for question in (
        "Which capability ID and user intent does it serve?",
        "What is the closest existing tool?",
        "Why can it not be an existing filter, projection, method, batch input,",
        "Which safety, authorization, sensitivity, confirmation, output, audit, or",
        "What serialized count and byte delta does it add?",
        "Which evaluation tasks prove the new surface is better?",
        "Does the resulting standard registry remain within budget and workflow",
    ):
        assert question in admission


def test_surface_rule_uses_current_registry_examples() -> None:
    text = SURFACE_RULE.read_text()

    for example in (
        "`accounts_set`",
        "`taxonomy_set`",
        "`privacy_consent_set`",
        "`transactions_create`",
        "`refresh_run`",
        "`system_status`, `import_status`, `sync_status`",
        "`_revert`, `_disconnect`, `_decide`,\n`_annotate`",
    ):
        assert example in text
    for stale_example in (
        "accounts_summary",
        "budget_set",
        "tags_set",
        "categories_create",
        "categories_delete",
        "`_status` | Not admitted",
    ):
        assert stale_example not in text


def test_active_consent_guidance_discloses_deferred_enforcement() -> None:
    for path in (MCP_SPEC, ARCHITECTURE_SPEC, MCP_RULE):
        text = path.read_text()
        assert "global consent enforcement is deferred" in text, path

    current_contract = MCP_SPEC.read_text()
    rule = MCP_RULE.read_text()
    assert "Sensitivity is middleware-enforced" not in current_contract
    assert (
        "Without consent, tools return useful degraded envelopes"
        not in current_contract
    )
    assert "The middleware enforces consent and redaction automatically" not in rule
    assert "Tools without consent return **degraded responses**" not in rule


def test_client_guide_keeps_where_data_goes_body_before_next_section() -> None:
    text = CLIENT_GUIDE.read_text()
    where_data_goes = text.partition("## Where data goes")[2].partition(
        "## Bounded tool surface"
    )[0]

    assert "The MCP transport is local-only" in where_data_goes
    assert "The MCP client" in where_data_goes
    assert "Sensitivity tiers" in where_data_goes
    assert "Other MoneyBin surfaces" in where_data_goes
    assert "Local-LLM clients" in where_data_goes
