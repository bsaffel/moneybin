# Account Identifiers and PII Handling

MoneyBin uses several distinct identifiers for accounts. This doc defines what
each one is, where it lives, and how PII is masked across the project.

## Identifier glossary

| Name | Type | Lives in | Source | Safe to log? |
|---|---|---|---|---|
| `account_id` | Synthetic stable ID | `core.dim_accounts.account_id` (PK), foreign key in `fct_transactions`, `fct_balances`, `app.account_settings`, `app.balance_assertions` | Derived: OFX `<ACCTID>` after sanitization, tabular content hash, future Plaid `account_id` | Yes — opaque, no PII |
| `account_number` | Full bank account number | **Never stored** | Source files | Never |
| `last_four` | Last 4 digits | `app.account_settings.last_four` (validated `^[0-9]{4}$`) | User-asserted in v1; Plaid `mask` in future | Reference by name only (`<account_id>.last_four`), never as a value |
| `routing_number` | ABA routing number | `core.dim_accounts.routing_number` | OFX `<BANKACCTFROM><BANKID>` or tabular | PII-adjacent (publicly listed but identifies institution); log only when essential for diagnostics |
| `display_name` | Human-readable label | `core.dim_accounts.display_name` (resolved from `app.account_settings.display_name` → derived default) | User override or auto-derived | Yes — user-controlled label |

## Known gap: `account_id` is not opaque for OFX-sourced accounts

The table above describes `account_id` as "opaque, no PII." That is **true
for Plaid-sourced accounts** (Plaid generates a synthetic `account_id`) and
for tabular content-hash IDs, but **false for OFX-sourced accounts** —
`src/moneybin/extractors/ofx_extractor.py` stores the raw OFX `<ACCTID>`
field, which for retail banks is the actual bank account number. The
"source-provided IDs are stored as-is" rule in
[`.claude/rules/identifiers.md`](../../.claude/rules/identifiers.md) was
followed correctly; the contradiction is between that rule and this doc's
opacity claim. Fixing it requires a one-way-door schema decision (hashing
breaks every `account_id` foreign key and persisted `app.account_settings`
row) and is tracked as outstanding work — do not assume opacity when
reasoning about MCP or log surface for OFX accounts until this is resolved.

## Why `account_number` is never stored

Loaders extract only `last_four` from raw inputs. The full number is dropped at
the parser boundary. This eliminates an entire class of breach impact: a
database leak does not expose full account numbers because they are not there.

## Masking story

Even with disciplined input handling, account-shaped digit sequences can leak
into logs through error messages, stack traces, or accidentally interpolated
SQL. The `SanitizedLogFormatter` (`src/moneybin/log_sanitizer.py`) is the
runtime safety net — it inspects every log record for patterns matching:

- 9+ digit sequences (catches account numbers, SSNs, routing numbers in
  contexts where they shouldn't be)
- Currency amount patterns (`$NNN.NN`)

When detected, the formatter masks the value (keeping the last 4 digits for
debug context) and emits a separate WARNING about the masking, so the original
incident is visible without leaking the value.

See `docs/specs/privacy-data-protection.md` for the full classification of
allowed vs prohibited log content.

## Relation to `account_id` stability

`account_id` is the join key everywhere. It must be stable across re-imports
of the same upstream account (re-importing the same OFX file must not produce
new `account_id`s). Hash-derived IDs achieve this by being a pure function of
the source content. Future Plaid integration uses Plaid's stable
`account_id` directly.

When two records that look like the same real-world account land under
different `account_id`s (e.g., the user re-linked an institution and Plaid
issued a new ID), the v1 answer is to leave both in place. Account merging is
explicitly out of scope for v1 — see `docs/specs/account-management.md`
§Out of Scope.
