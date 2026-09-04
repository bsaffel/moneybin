<!-- Last reviewed: 2026-09-04 -->
# MCP Server

MoneyBin exposes one **50-tool standard registry** to every generic MCP client.
A capable host may optionally defer schemas from that same registry to reduce prompt cost,
but tool names, approvals, allowlists, annotations, and audit identity do not
change. Reports are registered catalog entries behind the single `reports`
tool, not extra tool slots.

The canonical contract is [`moneybin-mcp.md`](../specs/moneybin-mcp.md): its
matrix names every tool and its current input properties. The registry budget,
admission record, byte evidence, and promotion gates live in
[`mcp-tool-surface-scaling.md`](../specs/mcp-tool-surface-scaling.md). For
client installation and local-data handling, use
[`mcp-clients.md`](mcp-clients.md). Every tool's client-visible definition —
description, parameters, annotations, and maximum sensitivity — is generated
from the code into the [MCP tool reference](../reference/mcp-tools.md).

## Connect and orient

Install a client entry with:

```bash
moneybin mcp install --client <name>
```

[`mcp-clients.md`](mcp-clients.md) lists the supported clients, config paths,
and restart requirements. Remove the MoneyBin entry from that client config to
disconnect it; there is no `mcp uninstall` command. If no profile exists, the
first tool call either elicits a profile name or returns the CLI profile-creation
instruction, depending on the client's elicitation support.

After the client starts `moneybin mcp serve`, ask it to call `system_status`
first. Use `reports` without a `report_id` to inspect the analytical catalog,
then call `reports(report_id=..., parameters=...)` for a selected report.
`sql_schema` and the `moneybin://schema` resource explain the curated
read-only SQL surface; `sql_schema(table='raw.*')` lists the queryable
relations one schema at a time, curated or not; `sql_query` is the operator
escape hatch.

## Prompts

Alongside the tools, the server registers seven prompts — conversation starters
a client can offer as a menu entry. Client support varies; run
`moneybin mcp list-prompts` for the live catalog of your installed version.

| Prompt | Purpose |
|---|---|
| `monthly_review` | Review spending, cash flow, balances, and recurring charges. |
| `categorization_organize` | Work through uncategorized transactions and propose rules. |
| `review_auto_rules` | Review pending auto-categorization rules before accepting them. |
| `onboarding` | Import initial data, verify accounts, and inspect categorization coverage. |
| `curate_recent_transactions` | Add useful tags and notes to recent transactions. |
| `review_curation_history` | Summarize recent curation activity from the audit log. |
| `sync_review` | Review sync health and suggest next steps. |

All seven are defined in
[`src/moneybin/mcp/prompts.py`](../../src/moneybin/mcp/prompts.py). Each returns
text, not data: a prompt describes a workflow over the tools above and grants no
capability of its own, so it takes no tool slot and changes no approval.

## Export data

The 50-tool standard registry sits at the 50-tool hard limit exactly and uses
exactly two export-specific tools:

- `export_run` publishes the closed 13-table canonical bundle or one registered
  report to a named local or Google Sheets destination. Supply
  `redaction_mode="redacted"` or `redaction_mode="unredacted"` on every run. If
  the value is omitted, clients with elicitation ask; other clients receive a
  structured `mutation_redaction_choice_required` refusal. An explicit `redaction_mode`
  does not prompt.
- `exports_set` asserts one named local or Sheets destination's target state.
  It creates, updates, or removes MoneyBin configuration; removal does not
  delete existing files, workbooks, or tabs, and requires a payload-bound
  confirmation token when elicitation is unavailable.

Call `system_status(sections=["exports"])` to inspect destination readiness
without adding a third export tool. Sheets destinations are output-only and
cannot be the same workbook as an inbound `gsheet` connection. MoneyBin stages
and validates its managed tabs before promotion, preserves the latest good
visible tabs on failure, and never touches user-owned tabs.

## Data handling

The server runs locally, while a cloud-hosted MCP client can send prompts and
tool results to its model provider. Sensitivity classification and critical
field masking are wired today. The consent ledger exists, but global consent
enforcement and automatic degraded responses are deferred; treat data requested
through a cloud client as data shared with that provider.
Read [`what-the-ai-sees.md`](what-the-ai-sees.md) before connecting real data:
it is the detailed, code-verified account of provider exposure, masking, local
records, connector egress, and local-model use.

## Contract status

The 50-tool registry is operating. It advertises zero output schemas and has
passed its deterministic contract check. Observed host-native deferral evidence remains absent.
Promotion remains blocked until both observed context-budget evidence and observed
host-native-deferral evidence exist. Do not add a tool, report
slot, profile, pack, or reconnect mode without the admission record in the
scaling spec.
