"""Detect accidental MCP ↔ CLI canonical-name drift.

**Scope.** This test compares the canonical *names* registered on each
surface, not the functional coverage. Names should match where doing so
costs nothing; divergence is allowed when it has a documented reason
(batch-vs-fine-grained shape, surface-idiom conventions, secret-material
or operator-territory exemptions per `.claude/rules/mcp-server.md`). The
allowlists below carry those citations inline.

**What this test is NOT.** It is not a functional-parity check. Two
surfaces with identical user-outcome coverage but different names will
fail this test; one MCP batch tool replacing four CLI commands will
fail this test — even though the agent's call path is strictly better.
Functional-parity (every user-outcome reachable from every supported
surface, with surface-appropriate idioms) lives in a separate
N-surface coverage spec (planned, see `docs/specs/` once authored) and
is enforced by PR review against `moneybin-mcp.md`'s
"Surface change discipline" rule, not by this test.

**Mechanism.** Walks the live FastMCP tool registry and the live Typer
command tree (no fixture file — a fixture would record drift, not
prevent it), canonicalizes both into underscore-joined names, and
asserts the symmetric difference is empty modulo the allowlists.

## Known drift (xfail backlog)

The test is marked ``xfail(strict=True)`` because pre-existing
canonical-name divergence exceeds what the current allowlists cover.
Strict mode flips to a CI failure the moment the drift resolves,
prompting removal of the xfail. Each backlog item is one of:
**rename** (accidental drift; pick the canonical form and update one
side), **allowlist** (intentional divergence; add to the relevant
frozen set below with a citation), or **build** (functional gap; add
the missing tool/command — caught by the functional-coverage check,
not by this test). Categories captured from the diff at test
introduction:

- **A. Naming-pattern mismatches.** MCP ``*_get`` vs CLI ``*_show``
  across the ``reports_*`` family (9 tools), ``accounts_get`` vs
  ``accounts_show``, ``accounts_set`` vs ``accounts_set``,
  ``transactions_review`` vs ``transactions_review``,
  ``import_formats_list`` vs ``import_formats_list``, ``system_doctor``
  vs top-level ``doctor``, ``reports_budget`` vs
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
  ``accounts_summary``, ``import_inbox_sync``,
  ``transactions_categorize_assist``,
  ``transactions_categorize_pending_list``,
  ``transactions_categorize_rules_delete``,
  ``transactions_categorize_rules_create``, ``transactions_get``,
  ``transactions_recurring_list``. ``moneybin_discover`` is MCP-only by
  design (visibility-disclosure mechanism) and lives in
  ``MCP_ONLY_ALLOWED`` rather than the drift backlog.
"""

# ruff: noqa: S101

from __future__ import annotations

import asyncio

import click
import pytest
import typer

from moneybin.cli.main import app as cli_app
from moneybin.mcp import server as mcp_server

# Names allowed CLI-only by `.claude/rules/mcp-server.md` "When CLI-only
# is justified." Grouped by category for documentation; the test only
# uses the union.
CLI_ONLY_ALLOWED: frozenset[str] = frozenset({
    # Category 1 — secret material through the LLM context window
    # (passphrases, encryption keys, key-derivation material).
    "db_init",
    "db_unlock",
    "db_key_rotate",
    "db_key_show",
    "db_key_export",
    "db_key_import",
    "db_key_verify",
    "sync_key_rotate",
    # Category 2 — operator territory. Database lifecycle:
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
    "db_query",  # raw SQL access; agent path is `sql_query` MCP tool
    # MCP server lifecycle + operator introspection:
    "mcp_serve",
    "mcp_install",
    "mcp_config_path",
    "mcp_list_tools",
    "mcp_list_prompts",
    # Profile + identity:
    "profile_create",
    "profile_delete",
    "profile_list",
    "profile_set",
    "profile_show",
    "profile_switch",
    # Developer tooling:
    "logs",
    "stats",
    "synthetic_generate",
    "synthetic_reset",
    "transform_seed",
    "transform_restate",
})

# MCP-only by design — tools that implement MCP-protocol-specific
# mechanisms with no CLI semantic. ``moneybin_discover`` is the
# visibility-disclosure tool that re-enables extended-namespace tools
# per MCP session; the CLI has no notion of session-scoped tool
# visibility. See `.claude/rules/mcp-server.md` Tool Taxonomy
# (progressive disclosure paragraph) and `docs/specs/mcp-architecture.md`
# §3.
MCP_ONLY_ALLOWED: frozenset[str] = frozenset({
    "moneybin_discover",
})


def _collect_mcp_tool_names() -> set[str]:
    """Every registered MCP tool name, transforms bypassed.

    Uses ``_list_tools()`` (FastMCP 3.x internal) so the parity check
    sees the full registered surface regardless of the
    ``progressive_disclosure`` setting — a tool hidden by the Visibility
    transform is still registered and still owes a CLI sibling. The
    public ``list_tools()`` filters by visibility. Matches the
    established convention used by ``src/moneybin/mcp/resources.py:150``
    and ``src/moneybin/cli/commands/mcp.py:597``.
    """
    mcp_server.register_core_tools()
    raw = asyncio.run(mcp_server.mcp._list_tools())  # noqa: SLF001  # fastmcp internal — public list_tools() filters by visibility  # pyright: ignore[reportPrivateUsage]
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
                # Groups with `invoke_without_command=True` are also callable
                # bare (e.g., `moneybin import inbox` drains the inbox via its
                # callback). Record the group path as a command in addition
                # to recursing into subcommands.
                if cmd.invoke_without_command:
                    names.add("_".join(path))
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
    raises=AssertionError,
    reason=(
        "Pre-existing canonical-name drift exceeds current allowlists. "
        "See module docstring for the A–D triage backlog (rename / "
        "allowlist / build). Strict xfail flips to FAIL the moment drift "
        "resolves — remove this marker then. ``raises=AssertionError`` "
        "ensures only the parity assertion is masked; setup or "
        "introspection errors still fail the test."
    ),
)
def test_cli_mcp_name_drift() -> None:
    """Every registered name matches across surfaces, or is allowlisted."""
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
