# MoneyBin Capabilities — Executable Cross-Surface Outcomes

> **Status:** implemented

This spec defines parity between MoneyBin's two active user surfaces. Parity
means that the CLI and the 45-tool standard registry can produce the same
durable user outcome. It does not require similar command or tool names.
Generic clients and supported deferred-loading hosts use that same registry;
deferral never creates a second capability surface.

The checked source of truth is
[`tests/fixtures/mcp_capabilities/outcome-map.json`](../../tests/fixtures/mcp_capabilities/outcome-map.json).
Its tests resolve the live Typer tree, the standard MCP registry, and every
named service method. A prose table cannot provide those guarantees and is not
duplicated here.

## Contract

Every map row contains:

| Field | Meaning |
|---|---|
| `capability_id` | Stable domain-oriented identifier; unique across the map |
| `mcp_tools` | Exact names in the standard 45-tool MCP registry |
| `cli_commands` | Exact space-delimited executable Typer paths |
| `service_methods` | Importable callables that own the behavior |
| `observable_outcomes` | Stable rows, states, counts, audit operations, or results used to judge equivalence |
| `exemption` | `null`, or one narrow single-surface policy exception with a written reason |

A non-exempt row must name both active surfaces, at least one service method,
and at least one observable outcome. Multiple commands may map to one MCP tool
and one command may participate in more than one outcome. Consolidated
boundaries are intentional: for example, CLI annotation verbs converge on
`transactions_annotate`, while all registered reports converge on `reports`.

The future REST surface is out of scope until it has an executable registry.
When it becomes active, it must join this contract rather than create a second
coverage catalog.

## Coverage

As implemented in July 2026, the map contains:

- 45 non-exempt capability rows covering all 45 standard MCP tools.
- 172 implemented Typer paths, including hidden compatibility aliases, with
  exact equality against the live command tree after explicit unimplemented
  stubs are removed.
- 7 policy-exempt rows.
- 10 reserved Typer paths that are still explicit `_not_implemented` stubs.

The stub list is executable, not documentary: every excluded path is invoked
with valid minimal arguments and must return the not-implemented outcome.
Implementing a reserved command therefore fails parity until its outcome row
is added.

## Consolidated families

| Family | Standard MCP boundary | Representative CLI paths | Outcome |
|---|---|---|---|
| System and audit | `system_status`, `system_audit`, `system_audit_undo` | `system status`, `system audit *`, `transactions matches undo` | Same health state, audit history, and reversible operation |
| Reports | `reports` | `reports networth`, `reports spending`, and other registered reports | Same catalog runner, rows, period, provenance, and truncation |
| Accounts | `accounts`, `accounts_set`, `accounts_balances`, `accounts_balance_assert` | `accounts list/get/summary/set`, `accounts balance *` | Same account projections, settings, observations, and assertions |
| Investments | `investments`, `investments_record`, `investments_securities_set`, `investments_lots_select` | `investments *` | Same ledger, holdings, lots, securities, and gains |
| Transactions | `transactions`, `transactions_create`, `transactions_annotate` | `transactions list/create`, notes, tags, and splits | Same transaction rows, stable-ID note lifecycle, and complete tag/split target state |
| Categorization | `transactions_categorize_*`, `reviews*`, `identity_links_decide` | `transactions categorize *`, match and identity review commands | Same engine results, rules, queue state, and decisions |
| Taxonomy | `taxonomy`, `taxonomy_set` | `categories *`, `merchants *` | Same category and merchant target state through `CategorizationService` |
| Import | `import_*` | `import files/preview/confirm/status/revert/inbox/labels`, `import formats *` | Same import log, raw rows, confirmation state, labels, and audited saved-format lifecycle |
| Sync | `sync_link`, `sync_status`, `sync_pull`, `sync_disconnect` | `sync login/link/status/pull/disconnect/logout` | Same authenticated, linked, pulled, disconnected, or logged-out state |
| Google Sheets | `gsheet`, `gsheet_connect`, `gsheet_pull`, `gsheet_disconnect` | `gsheet *` | Same connection and pulled source state |
| Privacy | `privacy`, `privacy_consent_set` | `privacy status/log/grant/revoke/revoke-all` | Same effective grants and privacy log |
| Refresh | `refresh_run` | `refresh`, match/identity commands, `transform apply` | Same selected step outcomes and proposal state |
| SQL | `sql_query`, `sql_schema` | `sql query` | Same rows, classification, cap, and CRITICAL masking |

Five parity gaps discovered by executing this model were closed as part of
the implementation:

1. `accounts(view="summary")` now has `moneybin accounts summary`.
2. `transactions_categorize_run(operation="improve_ai")` now reaches the same
   provider-native upgrade owned by
   `moneybin transactions categorize improve-ai`.
3. The existing sync quartet now exposes device authorization without adding
   tools: `sync_link(mode="login")` begins, `sync_status(auth_session_id=...)`
   polls, and `sync_disconnect(mode="logout")` clears credentials and pending
   profile-scoped sessions. Institution disconnects use payload-bound
   confirmation and re-resolve the live connection immediately before the
   remote delete. The CLI login remains a blocking wrapper over the same
   begin/poll client primitives.
4. `transactions categorize rules apply` now invokes only the rules engine,
   matching its command intent and
   `transactions_categorize_run(methods=["rules"])`; it no longer applies
   merchant or provider-native categorizations.
5. The existing destructive `import_revert` boundary now uses a strict
   discriminated operation for either import rollback or audited user-saved
   format deletion. Built-in formats remain immutable, and no read tool
   mutates format state.

The audit also found that category and merchant CLI names were placeholders.
`categories list/create/set` and `merchants list/create` now execute the same
`CategorizationService` behavior as `taxonomy` and `taxonomy_set`.

## Exemptions

Only these categories are allowed:

| Category | Allowed use |
|---|---|
| `secret-material` | Database keys, passphrases, and key derivation that must never enter an LLM context |
| `operator-territory` | Local database/process/server/profile/bootstrap or physical filesystem control |
| `granular-operator-debug` | Surgical pipeline, metrics, log, synthetic-data, or local redaction inspection |
| `protocol-only` | Machine-to-machine payload mechanics with no useful human command |

Every exempt row still names its owning service callable and observable
outcome. OAuth or browser interaction alone is not an exemption: an MCP agent
can safely present a verification URL and user code while secret device codes
remain in the profile-scoped `SecretStore`.

## Enforcement

[`tests/moneybin/test_mcp/test_capability_parity.py`](../../tests/moneybin/test_mcp/test_capability_parity.py)
enforces:

- unique, well-formed rows;
- exact coverage of `STANDARD_TOOL_NAMES`;
- exact coverage of every implemented Typer path;
- executable not-implemented outcomes for reserved paths;
- explicit coverage and delegation guards for hidden compatibility aliases;
- importable callable service methods;
- executable outcome parity on isolated copies of the same initialized
  database for refresh match/identity, reports, annotations, taxonomy,
  consent, import, and SQL; and
- equivalent persisted secret-session logout behavior for sync.

The old canonical-name symmetric-difference test is retired. Similar names can
still improve discoverability, but they are neither necessary nor sufficient
for capability parity.
