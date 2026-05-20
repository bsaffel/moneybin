# MoneyBin Capabilities — Cross-Surface Coverage Map

> **Status:** draft (bootstrap rows only). The bootstrap rows below
> demonstrate every coverage pattern the catalog will track. Full
> population (~30–60 rows) lands incrementally as follow-up work
> closes the parity backlog.

This spec is the cross-surface outcome map: one row per user-facing
capability, one column per active surface. It is the canonical answer to
"what user outcome reaches which surface, and under what registered
name." Per-surface implementation detail (parameter schemas, sensitivity
tiers, response envelope shapes, CLI flag conventions) lives in the
surface-specific specs.

## What is a capability?

A **capability** is a user-language verb-object pair describing
something a person can accomplish with MoneyBin. Capabilities are stated
in domain terms, not implementation terms:

| Capability (good)                                  | Implementation (not a capability)                        |
|----------------------------------------------------|----------------------------------------------------------|
| "List transactions filtered by date and account"   | Call `transactions_get(date=..., account=...)`           |
| "Set the complete tag set on a transaction"        | Invoke `transactions_tags_set`                           |
| "Rotate the database encryption key"               | Run `db_key_rotate` and update keychain                  |

Granularity: roughly one capability per primary user task. The full
catalog is expected to land at ~30–60 capabilities once populated.

## How to read the map

- Each row is one capability.
- Each surface column shows the **registered name** reaching this
  capability on that surface, OR an **exemption citation** linking back
  to `.claude/rules/mcp-server.md` "When CLI-only is justified."
- The **Status** column indicates:
  - `live` — reachable today on every non-exempt surface.
  - `pending-build (<surface>)` — capability is real but the named
    surface has not yet built its name.
  - `future` — planned; no surface implemented yet.

A cell containing `—` means "no name on this surface today." Pair it
with the Status column to disambiguate exempt-by-policy from
not-yet-built.

## Active surfaces

| Surface  | Active            | Spec                                   | Live registry              |
|----------|-------------------|----------------------------------------|----------------------------|
| MCP      | yes               | [`moneybin-mcp.md`](moneybin-mcp.md)   | `moneybin.mcp.server`      |
| CLI      | yes               | [`moneybin-cli.md`](moneybin-cli.md)   | `moneybin.cli.main`        |
| REST API | no (planned M3D)  | `moneybin-rest-api.md` (future)        | —                          |

## Capability map

> Bootstrap rows only — chosen to demonstrate each pattern. Names
> verified against the live MCP registry (`mcp._list_tools()`) and the
> live Typer command tree as of 2026-05-16.

| # | Capability                                                       | MCP                          | CLI                                                | REST (M3D) | Status                |
|---|------------------------------------------------------------------|------------------------------|----------------------------------------------------|------------|-----------------------|
| 1 | List recent transactions filtered by date/account/category       | — *(pending-build)*          | `transactions_list`                                | —          | pending-build (MCP)   |
| 2 | Fetch a single transaction by ID                                 | `transactions_get`           | — *(pending-build)*                                | —          | pending-build (CLI)   |
| 3 | List all accounts                                                | `accounts`                   | `accounts_list`                                    | —          | live                  |
| 4 | Update an account's settings (display name, include/archive)     | `accounts_set`               | `accounts_set` *(`--display-name`, `--include/--exclude`, `--archive/--unarchive`)* | —          | live                  |
| 5 | Set the complete tag set on a transaction                        | `transactions_tags_set`      | `transactions_tags_{add,remove,list}` *(cat 3)*    | —          | live                  |
| 6 | Sync the import inbox                                            | `import_inbox_sync`          | `import_inbox` (bare group call)                   | —          | live                  |
| 7 | Summarize an account's activity                                  | `accounts_summary`           | — *(pending-build)*                                | —          | pending-build (CLI)   |
| 8 | Refresh derived tables after raw data changes (full or per-step) | `refresh_run` *(`steps=`)*   | `refresh` *(`--step transform|match|categorize`)*  | —          | live                  |
| 9 | Rotate the database encryption key                               | — *(cat 1 — secret material)*| `db_key_rotate`                                    | —          | live                  |
| 10| Run the MCP server                                               | — *(cat 2 — operator)*       | `mcp_serve`                                        | —          | live                  |
| 11| Hard-delete a user-created category                              | `categories_delete`          | `categories delete`                                | —          | live                  |
| 12| Create one or more categorization rules (single + batch)         | `transactions_categorize_rules_create` | `transactions categorize rules create` | —          | live                  |
| 13| Soft-delete a categorization rule by ID                          | `transactions_categorize_rules_delete` | `transactions categorize rules delete` | —          | live                  |
| 14| Commit externally-decided categorizations (LLM workflow's terminal step) | `transactions_categorize_commit`        | `transactions categorize commit`        | —          | live                  |
| 15| Run the categorization engine cascade (rules + merchants)        | `transactions_categorize_run`            | `transactions categorize run`            | —          | live                  |
| 16| Get redacted batch for LLM categorization                        | `transactions_categorize_assist`         | `transactions categorize assist`         | —          | live                  |

| 17| Inspect SQLMesh model state (status/plan/validate/audit)        | — *(cat 2 — operator)*       | `transform status|plan|validate|audit`             | —          | live (CLI-only)       |

*(Bootstrap rows only; full table populates incrementally as
follow-up work closes the parity backlog. A prior row covering
"Discover currently-hidden MCP tools" was removed 2026-05-17
when client-driven progressive disclosure was retired (see
[`mcp-architecture.md`](mcp-architecture.md) §3); the current
rows 12–13 are unrelated and were added 2026-05-17 with the
rules-CLI parity work. Row 17 added 2026-05-19: transform_* de-registered
from MCP (PR #185) — operator territory per mcp-server.md category 2.
`sync_schedule_set/show/remove` stubs removed from MCP (PR #185) — were
not-implemented placeholders with no backing spec.)*

## Exemption categories

Defined in [`.claude/rules/mcp-server.md`](../../.claude/rules/mcp-server.md)
"When CLI-only is justified":

| # | Category                  | Short description                                                           | Status            |
|---|---------------------------|-----------------------------------------------------------------------------|-------------------|
| 1 | Secret material           | Passphrases/keys flowing through the LLM context window                     | formalized        |
| 2 | Operator territory        | Bootstrapping, recovery, dev tooling that needs physical operator presence  | formalized        |
| 3 | Surface-shape asymmetry   | Batch-set on MCP vs verb-list on CLI (e.g., `*_set` vs `*_{add,remove,list}`) | *pending follow-up* |
| 4 | File-based agent bridge   | CLI is the data plane for file-shaped ops                                   | *pending follow-up* |
| 5 | Privacy-design            | Tools that introspect/bypass redaction                                      | *pending follow-up* |

A capability marked exempt on a surface cites the category by number.
Categories 3–5 are documented here for forward-compatibility; the
formal definitions land with the follow-up allowlist refactor.

**MCP-only exemptions.** Reserved for tools that implement MCP-protocol-specific
mechanisms with no CLI semantic. Empty today — the prior entry
(`moneybin_discover` for session-scoped visibility re-enable) was
retired 2026-05-17. If the inventory grows again, exemptions will be
cited inline rather than via a numbered category, because the
CLI-only-justification list does not cover the reverse case.

## Contributor recipe

When a PR adds, renames, or removes a tool or command:

1. **Update the surface-specific spec** ([`moneybin-mcp.md`](moneybin-mcp.md)
   for MCP, [`moneybin-cli.md`](moneybin-cli.md) for CLI). Per-surface
   detail (parameter schemas, sensitivity tiers, envelope shape, flag
   conventions) lives there.
2. **Update this map** in the same PR. Add a new row (new capability)
   or update the existing row's cell (rename, removed, exempt change).
3. **Verify the user-language description** matches what the surface
   actually does — reviewer responsibility, not author judgment.
4. **If exempting a surface,** cite the category by number and ensure
   the citation is consistent with
   [`.claude/rules/mcp-server.md`](../../.claude/rules/mcp-server.md).

PR review enforces 1 and 2; the surface-change-discipline rule in
`.claude/rules/mcp-server.md` cites this contract.

## What this spec is NOT

- **Not an implementation spec.** Per-tool parameter schemas, response
  envelope shapes, sensitivity tiers, CLI flag conventions — those live
  in the surface-specific specs.
- **Not a test fixture.** The name-drift test
  (`tests/integration/test_surface_parity.py`, landing per PR #152)
  will derive its allowlist frozen sets from this spec once a
  follow-up wires the derivation. The spec itself is for humans first.
- **Not a marketing page.** That is [`docs/features.md`](../features.md).

## Related

- [`moneybin-mcp.md`](moneybin-mcp.md) — MCP-specific tool surface.
- [`moneybin-cli.md`](moneybin-cli.md) — CLI command taxonomy and conventions.
- [`mcp-architecture.md`](mcp-architecture.md) — Design-level MCP architecture (not surface-level).
- [`architecture-shared-primitives.md`](architecture-shared-primitives.md) — Cross-protocol symmetry contract.
- [`.claude/rules/mcp-server.md`](../../.claude/rules/mcp-server.md) — Surface change discipline and CLI-only justifications.
