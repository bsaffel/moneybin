<!-- Last reviewed: 2026-07-19 -->
# MCP Server

MoneyBin exposes one **45-tool standard registry** to every generic MCP client.
Supported hosts may defer schemas from that same registry to reduce prompt cost,
but tool names, approvals, allowlists, annotations, and audit identity do not
change. Reports are registered catalog entries behind the single `reports`
tool, not extra tool slots.

The canonical contract is [`moneybin-mcp.md`](../specs/moneybin-mcp.md): its
matrix names every tool and its current input properties. The registry budget,
admission record, byte evidence, and promotion gates live in
[`mcp-tool-surface-scaling.md`](../specs/mcp-tool-surface-scaling.md). For
client installation and local-data handling, use
[`mcp-clients.md`](mcp-clients.md).

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
read-only SQL surface; `sql_query` is the operator escape hatch.

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

The 45-tool registry is operating. It advertises zero output schemas and has
passed its deterministic contract check, but promotion remains pending observed
context-budget and host-native-deferral evidence. Do not add a tool, report
slot, profile, pack, or reconnect mode without the admission record in the
scaling spec.
