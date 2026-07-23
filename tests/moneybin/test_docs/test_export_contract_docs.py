"""Durable documentation checks for export delivery."""

from __future__ import annotations

import json
from pathlib import Path

from moneybin.exports.catalog import BUNDLE_TABLES
from moneybin.mcp.surface import HARD_TOOL_LIMIT, STANDARD_TOOL_COUNT

ROOT = Path(__file__).parents[3]
EXPORT_SPEC = ROOT / "docs/specs/export.md"
CLI_SPEC = ROOT / "docs/specs/moneybin-cli.md"
MCP_SPEC = ROOT / "docs/specs/moneybin-mcp.md"
CAPABILITIES_SPEC = ROOT / "docs/specs/moneybin-capabilities.md"
CAPABILITIES_MAP = ROOT / "tests/fixtures/mcp_capabilities/outcome-map.json"
CLI_GUIDE = ROOT / "docs/guides/cli-reference.md"
MCP_GUIDE = ROOT / "docs/guides/mcp-server.md"
FEATURES = ROOT / "docs/features.md"
THREAT_MODEL = ROOT / "docs/guides/threat-model.md"
SMART_IMPORT_INBOX = ROOT / "docs/specs/smart-import-inbox.md"
SYSTEM_PAYLOADS = ROOT / "src/moneybin/privacy/payloads/system.py"

CANONICAL_BUNDLE_TABLES = (
    "accounts",
    "transactions",
    "transaction_lines",
    "transfers",
    "balances",
    "balances_daily",
    "categories",
    "merchants",
    "securities",
    "investment_transactions",
    "investment_lots",
    "realized_gains",
    "holdings",
)


def _flat(path: Path) -> str:
    return " ".join(path.read_text().split())


def test_export_spec_locks_the_closed_bundle_and_local_artifact_contract() -> None:
    text = _flat(EXPORT_SPEC)

    assert tuple(table.name for table in BUNDLE_TABLES) == CANONICAL_BUNDLE_TABLES
    assert "closed catalog of 13 canonical" in text
    assert all(f"`{name}`" in text for name in CANONICAL_BUNDLE_TABLES)
    assert "`~/Documents/MoneyBin/<profile>/exports/`" in text
    assert "CSV and Parquet use directory bundles" in text
    assert "XLSX creates one timestamped workbook" in text
    assert "ZIP is the only v1 compression format" in text
    assert "XLSX is already a ZIP container and rejects" in text


def test_export_docs_lock_per_run_redaction_and_sheets_separation() -> None:
    spec = _flat(EXPORT_SPEC)
    guide = _flat(CLI_GUIDE)

    for text in (spec, guide):
        assert "redacted" in text
        assert "--unredacted" in text
        assert "per-run" in text
        assert "local:exports" in text
    assert "never publish into a workbook registered as an inbound connection" in spec
    assert "latest-state presentations, not archives" in spec
    assert "only the matching managed tabs" in spec
    assert "preserve the last known-good visible state" in spec


def test_export_docs_match_runtime_redaction_selection() -> None:
    spec = _flat(EXPORT_SPEC).lower()
    cli = _flat(CLI_GUIDE).lower()
    mcp = _flat(MCP_GUIDE).lower()

    for text in (spec, cli):
        assert "interactive cli omission prompts on every run" in text
        assert "`--yes` and non-tty execution select the safe redacted default" in text
        assert "`--unredacted` selects unredacted output affirmatively" in text
    for text in (spec, mcp):
        assert "explicit `redaction_mode` does not prompt" in text
        assert "`mutation_redaction_choice_required`" in text


def test_cli_docs_publish_the_exact_export_command_grammar() -> None:
    commands = (
        "moneybin export bundle",
        "moneybin export report <report-id>",
        "moneybin export destination list",
        "moneybin export destination add local <name> <path>",
        "moneybin export destination add sheets <name> <url>",
        "moneybin export destination remove <name>",
    )
    for path in (EXPORT_SPEC, CLI_SPEC, CLI_GUIDE):
        text = path.read_text()
        assert all(command in text for command in commands), path

    active_cli_docs = f"{CLI_SPEC.read_text()}\n{CLI_GUIDE.read_text()}"
    assert "export run" not in active_cli_docs
    assert "export` | Future spec" not in active_cli_docs
    assert "`export *` | Future spec" not in active_cli_docs


def test_mcp_docs_lock_two_export_tools_status_and_registry_budget() -> None:
    for path in (EXPORT_SPEC, MCP_SPEC, MCP_GUIDE):
        text = _flat(path)
        assert "`export_run`" in text, path
        assert "`exports_set`" in text, path
        assert '`system_status(sections=["exports"])`' in text, path
        assert f"{STANDARD_TOOL_COUNT}-tool standard registry" in text, path
        assert f"{HARD_TOOL_LIMIT}-tool hard limit" in text, path

    registry = (
        MCP_SPEC
        .read_text()
        .partition("## Standard registry")[2]
        .partition("## Contract matrix")[0]
    )
    export_row = next(line for line in registry.splitlines() if "| Exports |" in line)
    assert export_row == "| Exports | `export_run`, `exports_set` |"


def test_capability_contract_has_two_export_outcomes_over_shared_owners() -> None:
    rows = json.loads(CAPABILITIES_MAP.read_text())
    export_rows = [row for row in rows if row["capability_id"].startswith("exports.")]

    assert [row["capability_id"] for row in export_rows] == [
        "exports.delivery.run",
        "exports.destinations.set",
    ]
    assert export_rows[0]["mcp_tools"] == ["export_run"]
    assert export_rows[0]["cli_commands"] == ["export bundle", "export report"]
    assert export_rows[0]["service_methods"] == [
        "moneybin.exports.service.ExportService.run"
    ]
    assert export_rows[1]["mcp_tools"] == ["system_status", "exports_set"]
    assert export_rows[1]["cli_commands"] == [
        "export destination list",
        "export destination add local",
        "export destination add sheets",
        "export destination remove",
    ]
    capability_text = _flat(CAPABILITIES_SPEC)
    assert "Export delivery" in capability_text
    assert "Export destination target state" in capability_text


def test_public_feature_snapshot_lists_export_as_shipped() -> None:
    text = FEATURES.read_text()

    shipped = text.partition("## What's planned")[0]
    planned = text.partition("## What's planned")[2]
    assert "Canonical export delivery" in shipped
    assert "Plaintext export" not in planned
    assert 'A turnkey "export everything to CSV" command is on the roadmap' not in (
        THREAT_MODEL.read_text()
    )
    assert "future exports" not in SMART_IMPORT_INBOX.read_text().lower()


def test_system_payload_docs_state_export_destination_name_sensitivity() -> None:
    text = SYSTEM_PAYLOADS.read_text()

    assert "user-supplied destination names" in text
    assert "``ExportsStatus``               → Tier.MEDIUM" in text
    assert "``SystemStatusCLIPayload``      → Tier.MEDIUM" in text
    assert "privacy-safe export destination names" not in text
