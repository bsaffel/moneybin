# Account identifier provenance — what each field actually holds

Cited from [`.claude/rules/identifiers.md`](../rules/identifiers.md) §"Account
identifiers — never conflate them". Read this before working on an import
channel, the account resolver, or anything that surfaces an account reference.
The rule file keeps the three-field comparison and the masking rules; this file
carries the per-channel and per-path detail behind them.

**`source_account_key` is not uniformly the institution's identifier**, and
assuming it is re-creates the conflation this section exists to prevent. Its
value depends on the channel and on what the caller pinned:

| Case | What `source_account_key` holds |
|---|---|
| OFX | `<ACCTID>` — the institution's |
| PDF, unpinned | `pdf_doc_<digest>` — MoneyBin-synthesized from the exact document bytes; account anchors are separate match evidence (`import_service.py:_pdf_source_account`) |
| PDF pinned with `--account-id` | The key this account is already accepted under for `pdf`/`document`, oldest decision first — **not** the same key as unpinned. A statement's digest is its bytes, and a bank regenerates the same statement with fresh internal metadata, so a pin that minted per document would fork staging's dedup on every re-download. Falls back to this document's own `pdf_doc_<digest>` when the account has no key yet, and keeps that digest whenever it is already accepted, so a document bound to another account still refuses instead of loading here (`import_service.py:_pdf_source_account`) |
| Bare tabular (no account column) | `_bare_account_key(file_path, source_bytes)` — MoneyBin-synthesized from filename + content |
| Tabular pinned with `--account-id` | The key this account is already accepted under for this `source_type`/`source_origin`, oldest decision first — the same rule as PDF, and `--account-name` does not change it. A label that keyed the row would re-key the export the moment the flag was added or dropped, and tabular derives `transaction_id` FROM this key, so every row in the overlap would double-count. The label seeds only a first import with nothing to reuse (`slugify(account_name)`, else `_bare_account_key(file_path, source_bytes)`) — which is what keeps two accounts exported to one file on separate keys. Refuses rather than guess when the account holds several keys and no label says which (`_pinned_native_key`) |
| Other supported channel with `--account-id` pinned | The key that channel derives unpinned. `source_account_key` is ALWAYS a source-native key — a pin may substitute the one this account already answers to *in the same source*, never a canonical `account_id`; that is pre-fix residue. Whenever a pin does substitute, the key the file yields on its own is still recorded as a link (`AccountResolver._teach_unpinned_key`), so an unpinned re-import of that file still recognises the account |

**`account_id` is not unconditionally MoneyBin's either.** Nine staging models
project it as `COALESCE(links.account_id, a.account_id)`, each annotated
*"canonical via the import-time resolver link; source-native only if
unresolved"* — `stg_ofx__accounts.sql:25`, `stg_tabular__accounts.sql:7`,
`stg_plaid__accounts.sql:51` and six more. `core.dim_accounts` is built from
those models, so an account with **no resolver link** — exactly what
`accounts links run` exists to backfill — surfaces its source-native key
through `account_id`, a `RECORD_ID` field that every surface prints readably.
On OFX that is a real `<ACCTID>`.

**`proposed_account_id` on the mint path is neither.** When
`AccountResolver.propose()` finds no account to adopt (`is_new=True`) it returns
a preview `uuid.uuid4().hex[:12]` that its own docstring calls "NOT written
anywhere" (`account_resolver.py:672-673`, preview mint at `:708`) — `resolve()`
mints a *different* real id when the import commits (`:577`, `:584` — the
`force_standalone` and candidate-pass mint sites). Retaining one as a later
reference resolves to nothing. It is display-only, and only for the life of
the proposal.

This is why `proposal_ref` exists, and why it — not `account_id` — is the
referent to put in front of a user or an agent.

**`@N` indexes the file's full detected source-account list, not the proposals
you can see.** `_gate_account_proposals` enumerates every source account and
omits the ones already bound or not confirming, so a file's only *visible*
proposal is legitimately `@1` when `@0` was answered in an earlier call
(`import_service.py:2434`, and `:1395` builds the valid set from
`range(len(source_accounts))`; pinned by `test_import_binding.py:165-193`).
Renumbering the surfaced list to start at `@0` would bind the wrong account.
