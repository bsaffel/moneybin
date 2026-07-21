"""Durable documentation checks for the bounded MCP registry."""

from __future__ import annotations

import json
import re
from inspect import getdoc
from pathlib import Path

from fastmcp.tools import FunctionTool

from moneybin.mcp.surface import STANDARD_TOOL_NAMES

ROOT = Path(__file__).parents[3]
SCALING_SPEC = ROOT / "docs/specs/mcp-tool-surface-scaling.md"
ARCHITECTURE_SPEC = ROOT / "docs/specs/mcp-architecture.md"
MCP_SPEC = ROOT / "docs/specs/moneybin-mcp.md"
CLI_SPEC = ROOT / "docs/specs/moneybin-cli.md"
CLIENT_COMPATIBILITY_SPEC = ROOT / "docs/specs/ai-client-compatibility.md"
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
FEATURES = ROOT / "docs/features.md"
STANDARD_SNAPSHOT = ROOT / "tests/fixtures/mcp_surface/standard-45.json"
BASELINE_SNAPSHOT = ROOT / "tests/fixtures/mcp_surface/baseline-2026-07-17.json"
OUTCOME_MAP = ROOT / "tests/fixtures/mcp_capabilities/outcome-map.json"
CURRENT_PUBLIC_ROOTS = (
    ROOT / "README.md",
    *sorted((ROOT / ".claude/rules").glob("*.md")),
    *sorted((ROOT / "docs").rglob("*.md")),
)
HISTORICAL_PUBLIC_PREFIXES = (
    ROOT / "docs/decisions",
    ROOT / "docs/specs/archived",
)
RETIRED_COUNT_PATTERNS = (
    re.compile(
        r"(?:~|\b(?:about|approximately|around)\s+)?"
        r"105(?:-|\s+(?:registered\s+)?)tools?\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bmore than 100 tools\b", re.IGNORECASE),
    re.compile(r"\bour 105\b", re.IGNORECASE),
    re.compile(r"\btotal_count:\s*105\b", re.IGNORECASE),
)
INLINE_CODE_SPAN_PATTERN = re.compile(r"(?<!`)`([^`\n]+)`(?!`)")


def _current_public_mcp_docs() -> tuple[Path, ...]:
    return tuple(
        path
        for path in CURRENT_PUBLIC_ROOTS
        if path.is_file()
        and not any(prefix in path.parents for prefix in HISTORICAL_PUBLIC_PREFIXES)
    )


def _retired_mcp_code_spans(
    text: str,
    retired_names: set[str],
    current_names: frozenset[str] = frozenset(),
) -> set[str]:
    retired_spans: set[str] = set()
    for span in INLINE_CODE_SPAN_PATTERN.findall(text):
        name, has_arguments, _ = span.partition("(")
        if name in retired_names and (not has_arguments or span.endswith(")")):
            retired_spans.add(span)
        elif (
            span.endswith("_*")
            and any(
                retired_name.startswith(span[:-1]) for retired_name in retired_names
            )
            and not any(
                current_name.startswith(span[:-1]) for current_name in current_names
            )
        ):
            retired_spans.add(span)
    return retired_spans


def test_documented_standard_names_match_runtime() -> None:
    text = SCALING_SPEC.read_text()
    registry = text.partition("## Standard registry")[2].partition(
        "### Review decision persistence"
    )[0]
    documented = frozenset(re.findall(r"`([a-z][a-z0-9_]+)`", registry))

    assert documented == STANDARD_TOOL_NAMES


def test_governing_spec_records_runtime_facts_without_promotion_claim() -> None:
    text = " ".join(SCALING_SPEC.read_text().split())
    snapshot = json.loads(STANDARD_SNAPSHOT.read_text())
    output_schema_count = sum(
        tool["definition"].get("outputSchema") is not None for tool in snapshot["tools"]
    )

    assert output_schema_count == 0

    for fact in (
        f"{snapshot['tool_count']} tools",
        f"{snapshot['total_bytes']:,} bytes",
        snapshot["sha256"],
        "zero advertised output schemas",
        "contract_passed: true",
        "promotion_ready: false",
        "context budget: not_observed",
        "host-native deferral: not_observed",
    ):
        assert fact in text
    assert "**Status:** in-progress" in text
    assert "**Status:** implemented" not in text
    assert "pre-cutover registry" in text
    assert "ADR-016" in text


def test_client_compatibility_records_current_windsurf_headroom() -> None:
    text = " ".join(CLIENT_COMPATIBILITY_SPEC.read_text().split())
    index_row = next(
        line
        for line in INDEX.read_text().splitlines()
        if "[AI Client Compatibility & Distribution]" in line
    )

    for current_fact in (
        "45 MoneyBin tools",
        "100-active-tool",
        "55 tool slots",
    ):
        assert current_fact in text
        assert current_fact in index_row
    assert "over the ceiling" not in index_row


def test_cli_mcp_examples_use_coarse_operations_with_selectors() -> None:
    text = CLI_SPEC.read_text()

    for mapping in (
        '`accounts get <id>` | `accounts(view="detail", reference=<id>)`',
        '`accounts balance history` | `accounts_balances(view="history", reference=...)`',
        '`reports networth` | `reports(report_id="core:networth")`',
        '`transactions matches pending` | `reviews(kind="matches", status="pending")`',
        '`transactions matches run` | `refresh_run(steps=["match"])`',
    ):
        assert mapping in text
    assert "`transactions matches undo <match_id>`" in text
    assert '`system_audit(view="history", ...)`' in text
    assert '`system_audit(view="events", ...)`' in text
    assert "`system_audit_undo(operation_id=<operation_id>)`" in text
    assert "The identifiers are not interchangeable" in text
    assert (
        "`transactions matches undo <id>` | `system_audit_undo(operation_id=<id>)`"
    ) not in text
    assert "MCP mirrors CLI exactly" not in text


def test_architecture_documents_validated_manual_transaction_creation() -> None:
    text = " ".join(ARCHITECTURE_SPEC.read_text().split())

    assert "`transactions_create` is the validated batch-creation surface" in text
    assert "No general-purpose transaction insertion surface" not in text


def test_features_maps_categorization_queue_to_the_reviews_capability() -> None:
    outcome_map = json.loads(OUTCOME_MAP.read_text())
    reviews_capability = next(
        item for item in outcome_map if item["capability_id"] == "reviews.read"
    )
    snapshot = json.loads(STANDARD_SNAPSHOT.read_text())
    reviews_tool = next(tool for tool in snapshot["tools"] if tool["name"] == "reviews")
    kinds = reviews_tool["definition"]["inputSchema"]["properties"]["kind"]["enum"]

    assert "transactions categorize pending" in reviews_capability["cli_commands"]
    assert reviews_capability["mcp_tools"] == ["reviews"]
    assert "categorization" in kinds

    queue_line = next(
        line
        for line in FEATURES.read_text().splitlines()
        if "Curator-impact queue" in line
    )
    assert '`reviews(kind="categorization", status="pending")`' in queue_line
    assert "transactions_categorize_assist" not in queue_line


def test_features_documents_the_executable_manual_batch_contract() -> None:
    from moneybin.mcp.tools.curation import transactions_create

    outcome_map = json.loads(OUTCOME_MAP.read_text())
    create_capability = next(
        item for item in outcome_map if item["capability_id"] == "transactions.create"
    )
    doc = getdoc(transactions_create)

    assert create_capability["mcp_tools"] == ["transactions_create"]
    assert create_capability["service_methods"] == [
        "moneybin.services.transaction_service.TransactionService.create_manual_batch"
    ]
    assert doc is not None
    batch_range = re.search(r"Create (\d+)\.\.(\d+) manual transactions", doc)
    assert batch_range is not None

    manual_line = next(
        line
        for line in FEATURES.read_text().splitlines()
        if "Manual transaction entry" in line
    )
    assert (
        f"validated batch of {batch_range.group(1)}–{batch_range.group(2)} transactions"
        in manual_line
    )
    assert "one at a time" not in manual_line
    assert "not yet wired" not in manual_line


def test_cli_spec_describes_outcome_parity_without_input_identity() -> None:
    text = CLI_SPEC.read_text()

    assert (
        "Equivalent requests reach the mapped services and preserve observable "
        "outcomes."
    ) in " ".join(text.split())
    assert "Equal inputs reach the same services" not in text


def test_future_mcp_capabilities_remain_unnamed_until_admission() -> None:
    for path in (ARCHITECTURE_SPEC, MCP_SPEC, SCALING_SPEC, MCP_RULE, SURFACE_RULE):
        text = " ".join(path.read_text().split())
        assert "Future MCP capabilities remain unnamed until admission" in text, path

    combined = "\n".join(
        path.read_text()
        for path in (ARCHITECTURE_SPEC, MCP_SPEC, SCALING_SPEC, MCP_RULE, SURFACE_RULE)
    )
    for speculative_name in (
        "investments.record_trade",
        "airtable_connect",
        "smartsheet_connect",
        "notion_connect",
    ):
        assert speculative_name not in combined


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
    assert CHANGELOG.exists()
    assert ADR.exists()
    assert ARCHIVED_MCP_SPEC.exists()
    assert BASELINE_SNAPSHOT.exists()
    assert STANDARD_SNAPSHOT.exists()

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


def test_spec_index_keeps_deferred_loading_optional() -> None:
    row = next(
        line
        for line in INDEX.read_text().splitlines()
        if "[MCP Tool Surface Scaling]" in line
    )

    assert "deferred-loading hosts may use that same registry" in row
    assert "deferred-loading hosts use that same registry" not in row


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


def test_retired_mcp_code_spans_include_calls_and_wildcard_families() -> None:
    baseline = json.loads(BASELINE_SNAPSHOT.read_text())
    current = json.loads(STANDARD_SNAPSHOT.read_text())
    retired_names = {
        tool["name"] for tool in baseline["tools"] if "_" in tool["name"]
    } - {tool["name"] for tool in current["tools"]}

    text = (
        "`system_audit_history(...)` and `transactions_matches_*` are retired; "
        "`categories` remains a valid domain noun."
    )

    assert _retired_mcp_code_spans(text, retired_names) == {
        "system_audit_history(...)",
        "transactions_matches_*",
    }


def test_retired_mcp_code_spans_ignore_generic_inline_code() -> None:
    retired_names = {"system_audit_history", "transactions_matches_run"}

    assert _retired_mcp_code_spans("`categories` and `*`", retired_names) == set()


def test_current_public_docs_do_not_repeat_the_retired_mcp_surface() -> None:
    baseline = json.loads(BASELINE_SNAPSHOT.read_text())
    current = json.loads(STANDARD_SNAPSHOT.read_text())
    # One-word retired tool names overlap with preserved CLI command words.
    # Underscore names are unambiguously MCP public identifiers in prose.
    retired_names = {
        tool["name"] for tool in baseline["tools"] if "_" in tool["name"]
    } - {tool["name"] for tool in current["tools"]}

    violations: list[str] = []

    for path in _current_public_mcp_docs():
        text = path.read_text()
        relative = path.relative_to(ROOT)
        for span in sorted(
            _retired_mcp_code_spans(text, retired_names, STANDARD_TOOL_NAMES)
        ):
            violations.append(f"{relative}: retired MCP identifier `{span}`")
        for pattern in RETIRED_COUNT_PATTERNS:
            if match := pattern.search(text):
                violations.append(f"{relative}: retired count {match.group()!r}")

    assert not violations, "\n".join(violations)


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
