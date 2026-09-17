<!-- Last reviewed: 2026-09-14 -->
# MCP Server

MoneyBin exposes one **50-tool standard registry** to every generic MCP client,
spanning 13 user-facing domain groups across 17 literal tool-name prefixes (a
prefix is the part of a tool name before the first underscore).
A capable host may defer schemas from that same registry to reduce prompt cost,
but tool names, approvals, allowlists, annotations, and audit identity do not
change. Reports are registered catalog entries behind the single `reports`
tool, not extra tool slots.

The canonical contract is [`moneybin-mcp.md`](../specs/moneybin-mcp.md): its
matrix names every tool and its current input properties. The registry budget,
admission record, byte evidence, and promotion gates live in
[`mcp-tool-surface-scaling.md`](../specs/mcp-tool-surface-scaling.md). For
client installation and local-data handling, use
[`mcp-clients.md`](mcp-clients.md). Every tool's client-visible definition —
description, parameters, annotations, and declared sensitivity — is generated
from the code into the [MCP tool reference](../reference/mcp-tools.md).

## Connect and orient

Install a client entry with `moneybin mcp install --client <name>`. Pass
`--print` to see the exact bytes without writing any file:

```console
$ uv run moneybin mcp install --client claude-desktop --print
Using profile: demo
{
  "mcpServers": {
    "MoneyBin (demo)": {
      "command": "/opt/homebrew/bin/uv",
      "args": [
        "run",
        "moneybin",
        "--profile",
        "demo",
        "mcp",
        "serve"
      ],
      "env": {
      }
    }
  }
}
```

Three lines are trimmed from that block: the two `args` entries
`"--directory"` and the absolute path of the checkout `uv` runs from, and the
`"MONEYBIN_HOME"` entry inside `env`, which carries the absolute path of the
MoneyBin home directory that was set when install ran (the `env` block appears
only when `MONEYBIN_HOME` is set). Every option the command takes is in the
[`moneybin mcp` reference](../reference/cli/mcp.md#moneybin-mcp-install).

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

```console
$ uv run moneybin mcp list-prompts
Registered MCP tools — full surface visible at connect
  categorization_organize  Organize uncategorized transactions into categories.
  curate_recent_transactions  Walk the user through curating recently-imported transactions.
  monthly_review  Monthly financial review — spending, budget status, and trends.
  onboarding  First-time setup — import data and establish baseline.
  review_auto_rules  Review persisted categorization rules and apply confirmed state changes.
  review_curation_history  Summarize the last 7 days of curation activity from the audit log.
  sync_review  Review sync health and suggest the next action.
```

The first line is a log record the server emits while it builds the registry,
not a heading over the list: the seven indented rows are prompts.

All seven are defined in
[`src/moneybin/mcp/prompts.py`](../../src/moneybin/mcp/prompts.py). Each returns
text, not data: a prompt describes a workflow over the tools above and grants no
capability of its own, so it takes no tool slot and changes no approval.

## Export data

The 50-tool standard registry sits at the 50-tool hard limit exactly and uses
exactly two export-specific tools:

- `export_run` publishes the closed 13-table canonical bundle
  or one registered report to a named local or Google Sheets destination. Supply
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

The registry is at its 50-tool limit, advertises zero output schemas, and
passes its deterministic contract check. No host has been measured deferring
MoneyBin's schemas, and no context-budget measurement exists; until both
measurements exist, no tool, report slot, profile, pack, or reconnect mode is
added without the admission record in the scaling spec.

## What is not built yet

- **Consent gating is not enforced.** The consent ledger records grants, but no
  tool call is refused or degraded on the basis of one. Treat anything a
  cloud-hosted client asks for as shared with that client's model provider;
  [`what-the-ai-sees.md`](what-the-ai-sees.md) states the boundary field by
  field.
- **No `mcp uninstall` command.** Remove the MoneyBin entry from the client's
  config file by hand;
  [`mcp-clients.md`](mcp-clients.md#uninstall-and-reset) lists the file and the
  key per client.
- **Host-native schema deferral is unobserved.** No host has been measured
  deferring MoneyBin's schemas. Generic clients receive all 50 tool schemas at
  connect.
- **No output schemas.** Every tool returns the documented envelope, and clients
  that can validate structured output have nothing to validate against. Read the
  payload shapes from the [MCP tool reference](../reference/mcp-tools.md).
