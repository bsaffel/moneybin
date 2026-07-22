# MoneyBin MCP

> **Status:** in-progress — the 47-tool registry operates today; promotion is
> pending observed context-budget and host-native-deferral evidence.
>
> Companions: [`mcp-tool-surface-scaling.md`](mcp-tool-surface-scaling.md)
> (registry, budgets, evidence, and admission),
> [`mcp-architecture.md`](mcp-architecture.md) (layers and transport), and
> [`moneybin-capabilities.md`](moneybin-capabilities.md) (CLI/MCP outcomes).

## Purpose

This is the concrete current MCP contract. Generic clients receive the complete
47-tool standard registry. Supported hosts may defer schemas from that same
registry without reconnect, packs, or profiles; names, annotations, approvals,
allowlists, and audit identity do not change. The previous per-tool catalog is
archived at [`archived/moneybin-mcp-pre-cutover.md`](archived/moneybin-mcp-pre-cutover.md).

## Standard registry

| Domain | Tools |
|---|---|
| System | `system_status`, `system_audit`, `system_audit_undo` |
| Reports | `reports` |
| Accounts | `accounts`, `accounts_set`, `accounts_balances`, `accounts_balance_assert` |
| Investments | `investments`, `investments_record`, `investments_securities_set`, `investments_lots_select` |
| Transactions | `transactions`, `transactions_create`, `transactions_annotate`, `transactions_categorize_assist`, `transactions_categorize_commit`, `transactions_categorize_run`, `transactions_categorize_rules`, `transactions_categorize_rules_set` |
| Reviews | `reviews`, `reviews_decide`, `identity_links_decide` |
| Taxonomy | `taxonomy`, `taxonomy_set` |
| Import | `import_files`, `import_preview`, `import_confirm`, `import_status`, `import_revert`, `import_inbox_sync`, `import_labels_set` |
| Sync | `sync_link`, `sync_status`, `sync_pull`, `sync_disconnect`, `gsheet`, `gsheet_connect`, `gsheet_pull`, `gsheet_disconnect` |
| Privacy | `privacy`, `privacy_consent_set` |
| Exports | `export_run`, `exports_set` |
| Platform | `refresh_run`, `sql_query`, `sql_schema` |

## Contract matrix

The rendered **standard-47 snapshot** from `tools/list` is the canonical exact
input schema for every row below. This matrix is the stable selection guide: it
names each tool, its primary selector or discriminator, its intent, and its
safety family without duplicating FastMCP's drifting JSON schema.

| Tool | Selector or discriminator | Intent | Safety family |
|---|---|---|---|
| `system_status` | `detail`, `sections` | Orientation and pending-work inventory | Read / dynamic / maximum medium / status-derived |
| `system_audit` | `audit_id`, `cursor`, `limit`, `operation_id`, `view` | Audited mutation history | Read / dynamic / maximum high / audit-derived |
| `system_audit_undo` | `operation_id` | Reverse one undoable operation | Audited recovery / maximum low |
| `reports` | `limit`, `parameters`, `report_id` | Catalog or execute a registered report | Read / dynamic / maximum critical / report-derived |
| `accounts` | `cursor`, `include_closed`, `limit`, `query`, `reference`, `view` | Account collection | Read / dynamic / maximum critical / view-derived |
| `accounts_set` | `account_id`, `account_subtype`, `clear_fields`, `credit_limit`, `currency_code`, `default_cost_basis_method`, `display_name`, `holder_category`, `include_in_net_worth`, `is_archived`, `last_four`, `official_name` | Account target state | Audited write / maximum critical |
| `accounts_balances` | `as_of`, `cursor`, `end`, `limit`, `reference`, `start`, `threshold`, `view` | Balance projection and reconciliation | Read / dynamic / maximum high / balance-derived |
| `accounts_balance_assert` | `account`, `amount`, `as_of`, `confirmation_token`, `state` | Record a balance assertion | Audited write / maximum medium |
| `investments` | `account`, `cursor`, `end`, `limit`, `open_only`, `security`, `start`, `view` | Holdings and ledger projection | Read / dynamic / maximum high / view-derived |
| `investments_record` | `events` | Record an investment event | Audited write / maximum low |
| `investments_securities_set` | `coingecko_id`, `cost_basis_method`, `currency_code`, `cusip`, `exchange`, `figi`, `is_cash_equivalent`, `isin`, `name`, `security_id`, `security_type`, `ticker` | Securities-catalog target state | Audited write / maximum low |
| `investments_lots_select` | `disposal_txn_id`, `selections` | Full lot-selection target state | Audited write / maximum high |
| `transactions` | `account`, `category`, `cursor`, `end`, `limit`, `max_amount`, `merchant`, `min_amount`, `start`, `text` | Transaction projection | Read / maximum high |
| `transactions_create` | `transactions` | Create a manual transaction | Audited write / maximum low |
| `transactions_annotate` | `confirmation_token`, `requests` | Batch stable-ID note lifecycle, tag/split target states, and tag rename | Audited write / non-idempotent / dynamically destructive / maximum low |
| `transactions_categorize_assist` | `account_filter`, `date_range`, `limit` | Scrubbed categorization candidates | Read / scrubbed / maximum medium |
| `transactions_categorize_commit` | `items` | Commit reviewed categorizations | Confirmed write / maximum low |
| `transactions_categorize_run` | `methods`, `operation` | Run categorization engines | Audited workflow / maximum low |
| `transactions_categorize_rules` | `view` | Current categorization rules | Read / maximum high |
| `transactions_categorize_rules_set` | `confirmation_token`, `rules` | Rule target state | Confirmed write / maximum low |
| `reviews` | `cursor`, `kind`, `limit`, `status` | Pending/history queues, including current blast-radius evidence for pending `kind='auto_rules'` rows | Read / dynamic / maximum high / queue-derived |
| `reviews_decide` | `decisions` | Resolve ordinary or auto-rule review items; `kind='auto_rule'` carries proposal-scoped `allow_broad` | Confirmed write / maximum low |
| `identity_links_decide` | `confirmation_token`, `decisions` | Resolve identity links | Confirmed write / maximum low |
| `taxonomy` | `cursor`, `include_inactive`, `limit`, `query`, `view` | Read taxonomy projections | Read / dynamic / maximum medium / view-derived |
| `taxonomy_set` | `confirmation_token`, `items` | Taxonomy target state | Audited write / maximum low |
| `import_files` | `force`, `paths`, `refresh` | Import files | Audited workflow / maximum critical / file-derived |
| `import_preview` | `file_path` | Inspect an import before mutation | Read / dynamic / maximum critical / file-derived |
| `import_confirm` | `account_bindings`, `account_id`, `account_metadata`, `account_name`, `bridge_response`, `confirmation_token`, `preview_id`, `save_format` | Ratify an import proposal | Confirmed write / dynamic / maximum medium / preview-derived |
| `import_status` | `cursor`, `import_id`, `limit`, `sections` | Import lifecycle status | Read / dynamic / maximum medium / import-derived |
| `import_revert` | `confirmation_token`, `format_name`, `import_id`, `operation` | Revert an import batch | Audited recovery / maximum low |
| `import_inbox_sync` | `refresh` | Drain the import inbox | Audited workflow / maximum medium |
| `import_labels_set` | `import_id`, `labels` | Import-label target state | Audited write / maximum medium |
| `sync_link` | `institution`, `mode` | Start mediated provider linking | Credential flow / maximum medium |
| `sync_status` | `auth_session_id`, `session_id` | Provider connection status | Read / dynamic / maximum medium / session-derived |
| `sync_pull` | `institution` | Pull linked-provider data | External mutation / maximum medium |
| `sync_disconnect` | `confirmation_token`, `institution`, `mode` | Disconnect provider or credentials | Institution disconnect is a confirmed destructive write; logout is recoverable / maximum low |
| `gsheet` | `connection_id`, `view` | Google Sheets connections | Read / dynamic / maximum medium / connection-derived |
| `gsheet_connect` | `accept_seed_fallback`, `account_id`, `account_name`, `adapter`, `alias`, `column_mapping`, `confirm_mapping`, `connection_id`, `force_reauth`, `no_initial_pull`, `url` | Bind user-controlled storage | Credential flow / dynamic / maximum medium / connection-derived |
| `gsheet_pull` | `connection_id` | Pull sheet data | External mutation / maximum medium |
| `gsheet_disconnect` | `confirmation_token`, `connection_id`, `state` | Disconnect or purge a sheet binding | Destructive write / dynamic / maximum medium / connection-derived |
| `privacy` | `cursor`, `limit`, `view` | Privacy and consent projection | Read / dynamic / maximum low / privacy-derived |
| `privacy_consent_set` | `backend`, `categories`, `confirmation_token`, `mode`, `state` | Set consent state | Audited write / maximum low |
| `export_run` | `destination`, `redaction_mode`, `subject` | Publish a bundle or registered report to a named destination | External delivery / dynamic / non-idempotent / maximum medium |
| `exports_set` | `target` | Export-destination target state | Audited write / maximum medium |
| `refresh_run` | `steps` | Refresh derived state | Audited workflow / maximum medium |
| `sql_query` | `query` | Operator SQL escape hatch | Read / dynamic / maximum critical / query-derived |
| `sql_schema` | `table` | Curated SQL schema | Read / dynamic / maximum critical / schema-derived |

### Transaction annotation requests

`transactions_annotate` is one atomic workflow umbrella, not a collection
replacement for every annotation type. Its discriminated `requests` union is:

- `note_add(transaction_id, text)` — append a note and return the generated
  `note_id` in that outcome's `target_ids`;
- `note_edit(note_id, text)` — change only the addressed note while retaining
  its identity and audit chain;
- `note_delete(note_id)` — delete only the addressed note;
- `tags_set(transaction_id, tags)` and
  `splits_set(transaction_id, splits)` — declare complete collection state;
- `tag_rename(old_name, new_name)` — rename one tag globally.

Every request is preflighted before the first write and the batch shares one
`operation_id`. Note add and edit do not dynamically request confirmation;
note delete and other changed removals do. Because note append is an event, the
umbrella honestly advertises `idempotentHint=false` even though its target-state
variants remain individually idempotent.

### Ranked account resolution

`accounts(view="resolve", query=..., limit=...)` is a bounded ranked search.
It returns candidates in confidence-descending, stable-account-ID order and
reports an exact total plus `has_more` when `limit` truncates the result. It
does not issue or accept a cursor for this view: confidence is derived from
mutable account names and metadata, so it is not a safe stateless keyset.
Callers refine the query or rerun with a larger limit. The `list` view and
resumable `accounts_balances` views retain immutable-key cursors.

## Response contract

Every tool returns canonical JSON text and equivalent structured content with a
`summary`, `data`, and `actions` envelope. Amounts use the accounting
convention (negative expense, positive income) unless the tool explicitly
states a presentation override; currency-bearing responses name their currency
in `summary.display_currency`. Initial registry tools advertise zero output
schemas. A future schema needs the consumer-driven admission record in the
governing spec.

Sensitivity classification and critical-field masking are wired today. The
consent ledger exists, but **global consent enforcement is deferred**: tools
must not rely on an automatic consent gate or degraded response yet. Read tools
and writes still declare their sensitivity and mutable-state, audit, recovery,
and confirmation contracts.

## Coarse contracts and workflow boundaries

- `reports(report_id, parameters, limit)` first returns the catalog without a
  report ID, then executes a selected report. New reports are catalog entries,
  never new tool slots.
- `accounts`, `investments`, `transactions`, `reviews`, `taxonomy`, `privacy`,
  and `gsheet` expose typed views or filters under one domain identity. Their
  paired `_set`, `_decide`, or domain verb tools retain material write and
  confirmation boundaries.
- `import_files` and `import_preview` establish an import; `import_confirm`
  ratifies system proposals, including an elicited human decision for a PDF
  sign inversion. Clients without elicitation receive an opaque,
  payload-bound `confirmation_token` and retry the same operation; both paths
  recompute and compare the live proposal inside the write transaction before
  importing. `import_status` and `import_revert` provide recovery.
  `refresh_run` owns the bounded derived-state workflow.
- `sql_query` is the read-only escape hatch and `sql_schema` explains the
  interface schema. They do not replace domain validation for writes.

### Export delivery

The 47-tool standard registry contains exactly two export-specific tools and
stays below the 50-tool hard limit:

- `export_run` publishes either the closed 13-table canonical bundle or one
  catalog report to a named local or Sheets destination. Every call supplies
  `redaction_mode`; omission elicits a choice where supported and otherwise
  returns a structured refusal. `redacted` is the safe default, never a saved
  destination preference.
- `exports_set` asserts one named local or Sheets destination's typed target
  state. It shares the same service/repository owners as
  `moneybin export destination ...`; removing configuration never deletes
  artifacts, workbooks, or tabs.
- `system_status(sections=["exports"])` reports destination readiness through
  the existing orientation tool, so status does not consume a third export
  slot.

Sheets destinations are output-only and cannot overlap an inbound `gsheet`
connection. Publication replaces only MoneyBin-managed tabs after staging and
validation; a failure preserves the latest known-good visible tabs. Local
delivery publishes immutable CSV, Parquet, or XLSX artifacts, with ZIP limited
to completed CSV and Parquet bundles.

## Prompts and resources

Prompts are workflow guidance, not an alternate registry. They use only the
standard names above and lead with `system_status` when orientation is needed.

### Registered prompts

`monthly_review`, `categorization_organize`, `review_auto_rules`, `onboarding`,
`curate_recent_transactions`, `review_curation_history`, and `sync_review`.

### Resources

`moneybin://schema` is the one registered ambient resource for privacy-safe
read-only SQL; it does not create a discovery, pack, or profile mode.

## Capability parity and exemptions

MCP/CLI parity is capability and observable-outcome parity, not name equality.
The executable map and isolated-state tests live in
[`tests/fixtures/mcp_capabilities/outcome-map.json`](../../tests/fixtures/mcp_capabilities/outcome-map.json)
and `test_capability_parity.py`. Explicit CLI-only exemptions are limited to
secret material and hands-on operator territory; see
[`mcp-architecture.md`](mcp-architecture.md) and the outcome map.

## Registration and verification

`register_core_tools()` must exactly equal `STANDARD_TOOL_NAMES`; no hidden
FastMCP aliases are allowed. Tests inventory the actual `tools/list` response,
render coarse schemas, enforce description and metadata budgets, prove parity,
and compare the deterministic 47-tool capture against the frozen baseline.

The deterministic comparison passed, but `promotion_ready: false`: the context
budget and host-native deferral are not observed. The governing spec and
ADR-016 therefore remain in-progress and Proposed respectively.
