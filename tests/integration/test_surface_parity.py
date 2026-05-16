"""Enforce MCP ↔ CLI surface parity by walking the live registries.

Encodes the contract from `.claude/rules/mcp-server.md` and
`docs/specs/architecture-shared-primitives.md` §MCP/CLI/SQL Symmetry:
every MCP tool has a CLI sibling under the same canonical name (and vice
versa) unless the divergence falls into a documented exemption category.

The test walks the live FastMCP tool registry and the live Typer command
tree (no fixture file — a fixture would record drift, not prevent it),
canonicalizes both into underscore-joined names, and asserts the
symmetric difference is empty modulo the allowlists below.

## Known drift (xfail backlog)

The test is marked ``xfail(strict=True)`` because pre-existing drift
between the two surfaces exceeds the documented exemption categories.
Strict mode flips to a CI failure the moment the drift resolves,
prompting removal of the xfail. Categories of pending drift, captured
from the diff at test introduction:

- **A. Naming-pattern mismatches.** MCP ``*_get`` vs CLI ``*_show``
  across the ``reports_*`` family (9 tools), ``accounts_get`` vs
  ``accounts_show``, ``accounts_settings_update`` vs ``accounts_set``,
  ``transactions_review_status`` vs ``transactions_review``,
  ``import_list_formats`` vs ``import_formats_list``, ``system_doctor``
  vs top-level ``doctor``, ``reports_budget_status`` vs
  ``reports_budget``, ``accounts_balance_assertion_delete`` vs
  ``accounts_balance_delete``.
- **B. Set-semantic vs verb-list split.** MCP collapses to ``*_set``
  while CLI splits into add/remove/list/clear:
  ``import_labels_set`` / ``import_labels_{add,remove,list}``,
  ``transactions_splits_set`` / ``transactions_splits_{add,remove,clear,list}``,
  ``transactions_tags_set`` / ``transactions_tags_{add,remove,list}``.
- **C. MCP gaps.** CLI commands that need an MCP sibling but lack one:
  ``transactions_matches_*`` (4), ``transactions_categorize_ml_*`` (3),
  ``tax_deductions``, ``privacy_redact``, ``sync_login``/``sync_logout``,
  ``transactions_audit``, ``transactions_list``, ``transactions_review``,
  ``transactions_notes_list``, ``export_run``, ``budget_delete``,
  ``categories_delete``, ``system_audit_show``, ``import_history``,
  ``import_inbox_path``, ``import_labels_*``,
  ``transactions_categorize_apply_from_file``,
  ``transactions_categorize_auto_rules``,
  ``transactions_categorize_export_uncategorized``,
  ``transactions_categorize_rules_apply``, plus the singular-show family
  (``accounts_show``, ``accounts_balance_show``,
  ``accounts_investments_show``).
- **D. CLI gaps.** MCP tools that need a CLI sibling but lack one:
  ``moneybin_discover`` (MCP-only by design — the visibility-disclosure
  tool), ``accounts_summary``, ``import_inbox_sync``,
  ``transactions_categorize_assist``,
  ``transactions_categorize_pending_list``,
  ``transactions_categorize_rule_delete``,
  ``transactions_categorize_rules_create``, ``transactions_get``,
  ``transactions_recurring_list``.
"""

# ruff: noqa: S101

from __future__ import annotations

import asyncio

import click
import pytest
import typer

from moneybin.cli.main import app as cli_app
from moneybin.mcp import server as mcp_server

# CLI-only by security policy: secret material through the LLM context
# window. See `.claude/rules/mcp-server.md` "When CLI-only is justified"
# category 1.
_SECRET_MATERIAL: frozenset[str] = frozenset({
    "db_init",
    "db_unlock",
    "db_key_rotate",
    "db_key_show",
    "db_key_export",
    "db_key_import",
    "db_key_verify",
    "sync_key_rotate",
})

# CLI-only by operator policy: bootstrapping, recovery, and developer-
# tooling that require physical operator presence. See
# `.claude/rules/mcp-server.md` "When CLI-only is justified" category 2.
_OPERATOR_TERRITORY: frozenset[str] = frozenset({
    # Database lifecycle
    "db_init",
    "db_lock",
    "db_ps",
    "db_kill",
    "db_shell",
    "db_ui",
    "db_migrate_apply",
    "db_migrate_status",
    "db_backup",
    "db_restore",
    "db_info",
    # Raw SQL access — agent path is `sql_query` MCP tool.
    "db_query",
    # MCP server lifecycle + operator introspection
    "mcp_serve",
    "mcp_install",
    "mcp_config_path",
    "mcp_list_tools",
    "mcp_list_prompts",
    # Profile + identity
    "profile_create",
    "profile_delete",
    "profile_list",
    "profile_set",
    "profile_show",
    "profile_switch",
    # Developer tooling
    "logs",
    "stats",
    "synthetic_generate",
    "synthetic_reset",
    "transform_seed",
    "transform_restate",
})

CLI_ONLY_ALLOWED: frozenset[str] = _SECRET_MATERIAL | _OPERATOR_TERRITORY

# No MCP tool should be missing its CLI sibling. If a justification ever
# exists, document it here with a citation to the spec section that
# approves it.
MCP_ONLY_ALLOWED: frozenset[str] = frozenset()


def _collect_mcp_tool_names() -> set[str]:
    """Every registered MCP tool name, transforms bypassed.

    Uses ``_local_provider.list_tools()`` (FastMCP 3.x internal) so the
    parity check sees the full registered surface regardless of the
    ``progressive_disclosure`` setting — a tool hidden by the Visibility
    transform is still registered and still owes a CLI sibling.
    """
    mcp_server.register_core_tools()
    raw = asyncio.run(mcp_server.mcp._local_provider.list_tools())  # pyright: ignore[reportPrivateUsage]
    return {tool.name for tool in raw}


def _collect_cli_command_names() -> set[str]:
    """Walk the Typer command tree → underscore-joined canonical names.

    ``moneybin transactions categorize apply`` →
    ``transactions_categorize_apply``. Hyphens in command names are
    normalized to underscores (``list-tools`` → ``list_tools``) to match
    the MCP convention.
    """

    def walk(group: click.Group, prefix: tuple[str, ...]) -> set[str]:
        names: set[str] = set()
        for cmd_name, cmd in group.commands.items():
            path = (*prefix, cmd_name.replace("-", "_"))
            if isinstance(cmd, click.Group):
                names |= walk(cmd, path)
            else:
                names.add("_".join(path))
        return names

    root = typer.main.get_command(cli_app)
    assert isinstance(root, click.Group), "moneybin root CLI should be a Group"
    return walk(root, ())


def _format_diff(label: str, names: set[str]) -> str:
    return f"{label} ({len(names)}):\n  " + "\n  ".join(sorted(names))


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Pre-existing MCP↔CLI drift exceeds the documented exemption "
        "categories. See module docstring for the A–D backlog. Strict "
        "xfail flips to FAIL the moment drift resolves — remove this "
        "marker then."
    ),
)
def test_cli_mcp_surface_parity() -> None:
    """Every MCP tool has a CLI sibling (or is allowlisted), and vice versa."""
    mcp_names = _collect_mcp_tool_names()
    cli_names = _collect_cli_command_names()

    mcp_only = mcp_names - cli_names - MCP_ONLY_ALLOWED
    cli_only = cli_names - mcp_names - CLI_ONLY_ALLOWED

    messages: list[str] = []
    if mcp_only:
        messages.append(
            _format_diff("MCP tools without CLI siblings", mcp_only)
            + "\n  → Add the CLI command, or add to MCP_ONLY_ALLOWED with "
            "a citation to the spec section that approves it."
        )
    if cli_only:
        messages.append(
            _format_diff("CLI commands without MCP siblings", cli_only)
            + "\n  → Add the MCP tool, or add to CLI_ONLY_ALLOWED. The only "
            "approved CLI-only justifications are in `.claude/rules/"
            "mcp-server.md` 'When CLI-only is justified'."
        )

    assert not messages, "\n\n".join(messages)
