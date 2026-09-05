<!-- Last reviewed: 2026-07-21 -->

# Changelog

All notable changes to MoneyBin are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). MoneyBin is pre-1.0 and pre-launch; entries are grouped by **milestone** rather than semantic releases until 1.0 ships. See [`docs/roadmap.md`](docs/roadmap.md) for the current milestone scheme.

> **Milestone taxonomy revised 2026-05-30.** The scheme is now four phase-aligned milestones — **M0 Foundation · M1 Ingestion Core · M2 Analysis & Reports · M3 Productization & Distribution** — with lettered increments (e.g. `M1J`) beneath. Entries written before this date (the `[Unreleased]` narrative and the dated sections below) reference the **pre-revision** grid (old M0/M1/M2A–C/M3A–F) and are preserved as historical record. See [`docs/roadmap.md`](docs/roadmap.md) for the old→new mapping.

## [Unreleased]

### Changed
- **MCP tools publish a sensitivity floor, not a ceiling, and the reference now
  says which.** A statically classified tool's declared tier could overwrite a
  higher tier the response had already derived, understating both the response
  and its privacy audit row; it now raises but never lowers, matching how
  `discloses=` already behaved. Every entry in the tool reference and the
  contract matrix reads `at least <tier>` for a statically classified tool and
  `up to <tier>` for one that classifies per call. (#535)
- **The public docs have one index, one reference directory, and a test that
  every command they cite exists.** `docs/architecture/` and `docs/tech/` are
  folded into `docs/reference/`; `docs/guides/README.md` and
  `docs/reference/prompts/README.md` are merged into `docs/README.md` and a
  new Prompts section of the MCP server guide. A documentation-policy test now
  parses every `moneybin …` invocation in the public docs, including the
  command and flags columns of the CLI reference tables, and resolves it
  against the registered command tree; CI now runs the unit suite on
  docs-only pull requests, which skipped every test job before. The 34
  invocations it caught are corrected —
  among them `db rotate-key`, `db shell -c`, `import file`, `reports summary`,
  `mcp serve --profile`, a bare `moneybin doctor`, and 13 reference rows that
  showed a positional (`db restore <backup-path>`, `sync pull [<item-id>]`)
  where the command takes an option (`--from`, `--institution`).

  One corrected claim was about privacy. The CLI reference said
  `transactions categorize assist` sends description and memo text redacted.
  It sends that text in full, masking only embedded identifiers such as
  account numbers, and omits amount, date, and account id; the wording now
  matches the command's own help.

  Stale mechanics are brought up to date across the guides and references:
  the refresh cascade has six steps (`rates` is the sixth); cross-source
  merges rank `manual, gsheet, ofx, plaid` ahead of the tabular formats;
  `dim_accounts` merges per field rather than keeping one winning row; five
  price sources are registered; multi-currency display conversion has
  shipped; metrics flush once at session end with no interval setting;
  `stats --output json` emits `{"metrics": [...]}` rather than the envelope;
  the `system doctor` check list matches the implementation; and the
  `reports networth` JSON sample is real output — a list with one totals row
  per currency followed by one row per account.

  The unused `docs` dependency group (MkDocs Material and friends) is dropped
  from `pyproject.toml`, and ADR-011 records why: Material for MkDocs entered
  maintenance mode in November 2025, so the docs site is deferred to the first
  public release and builds on its successor if that has reached 1.0 by then.
  (#516)

- **`accounts list` and `transactions list` now name the account in a column
  you can quote back.** `accounts list` had glued the id onto the display name
  (`Checking (acct_a1b2)`) and `transactions list` headed its id column
  `account`; both now render an `account_id` column, so the two tables visibly
  join and `--output json` is unchanged. (#515)

- **A truncated `transactions list` page says how much it left behind instead
  of printing a cursor.** `Next page: --cursor <token>` is replaced by
  `20 of 2,046 shown · raise --limit for more`, with the continuation offered
  only where a further page exists; `--cursor` is unchanged for
  `--output json`. (#515)

- **The last eight commands that drew their own columns now render like every
  other one.** `db ps`, `db kill`, `demo`, `fx list`, `import history`,
  `import formats list`, and the four `investments` list commands each built a
  table by padding an f-string — their own header row, their own fixed-width
  rule, and their own idea of how wide a column should be. A 100-character rule
  under a 130-character row, and a path that ran past both, were the visible
  symptoms. All eight now call `render_rows`, so a value too wide for the
  terminal folds inside its column instead of running past the frame.

  Three of them printed `key=value` at the reader rather than a table at all —
  `investments list`, `holdings`, and `gains` emitted lines like
  `qty=10 cost_basis=1000.00 avg_cost=100.00 market_value=…`, repeating a field
  name on every row. The names now sit in the header once.

  This empties `_AWAITING_RENDER_ROWS`, the exemption set that carried these
  eight modules, and retires the second guard that existed only to keep that
  list from rotting. One guard now holds every CLI module unconditionally.

- **`demo` and the investments commands format their amounts the way every
  other command does.** `demo` printed `Net worth: 12345.67` while
  `reports networth` printed `12,480.22` for the same kind of figure — its own
  comment claimed the two matched. Amounts on these surfaces now carry
  thousands separators, two decimal places, and the design system's `−` for a
  negative, and a realized or unrealized gain carries its sign and its colour.
  A missing amount renders `-` rather than `n/a`, matching every other table.

  **Per-unit prices are deliberately exempt.** An exchange rate, a security's
  close, and a holding's average cost are stored to ten decimal places, and
  rounding them to two would render a sub-cent price as `0.00`. Those columns
  print as stored.

- **Wide list commands now show a curated set of columns and take `--wide` for
  the rest.** `investments holdings`, `gains`, `lots list`, `import history`,
  and `import formats list --type=pdf` each name the columns that answer the
  question they exist to answer, and disclose the narrowing
  (`6 of 9 columns shown — --wide for all`). Without this, nine columns in an
  80-column terminal split an amount across two lines — `1,200.00` rendered as
  `1,200.` above `00`, which reads as a smaller number rather than as a wrapped
  one. The default sets are chosen by hand rather than measured: fitting by
  width keeps the first and last columns, which on `holdings` drops
  `market value`.

  A column that says a figure beside it is *untrustworthy* stays in the default
  view, because it qualifies the answer rather than commenting on the run:
  `holdings` keeps `status`, which is the only thing separating an unpriced
  position from one with a known-wrong share count when both render `-`, and
  `lots list` keeps the marker that says a cost basis is a floor rather than a
  figure. Each had one substitute — a warning line — and `-q` suppresses those.
  `gains` meets the same requirement the other way, because it cannot seat the
  column: at 80 columns a seventh entry folds the disposal date and the
  security id and breaks `⚠️ basis_incomplete` itself across three lines. So
  the marker is declared and reachable with `--wide`, and the line disclosing
  that a realized gain was computed against an incomplete basis is no longer
  silenced by `-q` — the figures on a 1099-B surface would otherwise read as
  authoritative with nothing on screen to say they are conservative.

  **`import history --wide` shows the whole source path.** The column had been
  projecting the basename, so two imports of `january.csv` from a per-account
  `checking/` and `savings/` folder rendered as indistinguishable rows —
  and `source_file` is part of the dedup key on `raw.tabular_transactions`
  precisely because the same content read from a different path is a different
  import. The full path now reaches the renderer, which folds text rather than
  discarding the half that tells the two apart.

  An amount's denomination stays with it for the same reason. None of
  `investments list`, `gains`, `lots list`, or `holdings` takes a currency
  filter, so one call can span accounts denominated differently, and two rows
  reading `1,500.00` are then not the same quantity. `investments list`,
  `gains` and `holdings` keep `currency` in the default view. `gains` and
  `lots list` had not declared the column at all, so `--wide` could not reach
  it either; both declare it now. `lots list` is the sole table that cannot
  seat it by default — at 80 columns its six columns already fold a full-length
  lot id, and a seventh folds the security id with it — so there `--wide` is
  the way to it.

  `investments lots list --all` gains an open/closed `state` column instead.
  That view deliberately returns both kinds and the table named neither. The
  state is strictly derivable — a lot is open when its remaining quantity
  exceeds zero — but that rule appears neither on screen nor in `--help`, so
  the reader was left inferring a lifecycle state from a numeric cell. Under
  the default `--open` the answer is constant, so that view does not pay for
  the column.

  Commands whose tables already fit — `fx list`, `investments list`,
  `investments prices list`, `securities list`, `db ps` — show every column and
  gain no flag.

  **A number that is not an amount no longer folds either.** The renderer kept
  amounts whole by declaring them in `money=`, because `1,200.00` folded after
  the decimal point renders `1,200.` above `00` and the first line is a
  complete, plausible number two orders of magnitude off. Columns holding a
  per-unit price, a share count, an FX rate or a match score are deliberately
  *not* amounts — rounding a `DECIMAL(28,10)` close to two places would print
  `0.00` — and that exclusion silently took the no-fold guarantee with it, so
  `8.2987654321` folded to `8.298` above `7654321`. `render_rows` now takes a
  second declaration, `numeric=`, carrying atomicity without formatting, and
  every column holding a bare number uses one of the two. Alignment is
  unchanged: amounts stay right-aligned, these stay left. While a table fits
  its terminal the output is identical; where one does not, the fold now lands
  on the identifier beside the number rather than on the number.

- **Report columns are now ordered grain-first, with each report's headline
  figure last.** Every report's projection reads
  `grain keys → labels → dimensions → dates → provenance → measures`, and
  within the measures the components precede the figure they compose — so a
  narrowed terminal keeps the column that answers the report rather than one of
  its inputs. Two reports read backwards before this and change most visibly:
  `core:networth` led with `net_worth` and trailed its own components, and
  `core:merchants` led with `total_spend`; both now end on that figure. The one
  exception is a report whose headline measure is also the base its comparatives
  are measured against: `core:spending` now *leads* its measure block with
  `total_spend`, because a delta printed before the quantity it is a delta of
  has no referent, and current-then-prior-then-change is the layout every
  variance report uses.

  This changes JSON key order, MCP response field order, and export column
  order, and it changes the `columns` array each report publishes through the
  MCP `reports` catalog and `moneybin reports list` / `describe` — so an agent
  that reads a report's description sees the new order too. Values, column
  names, and types are unchanged, and no column was added or removed — but a
  caller reading results **by position** rather than by name must be updated.
  Two reports' declared column tuples also disagreed with what
  their query actually returned (`core:cashflow`, `core:recurring`); the
  declaration and the projection now agree, and a test holds them together. The
  convention is `.claude/rules/column-ordering.md`.

  The seven `reports.*` SQLMesh model projections are swept to the same order,
  so `SELECT * FROM reports.net_worth` through `sql_query` or
  `moneybin sql query` returns `currency_code, balance_date, account_count,
  total_assets, total_liabilities, net_worth` rather than leading with the
  total. A display-currency conversion now places the `original_currency_code`
  it attaches beside `currency_code` instead of after the amounts — in the rows
  as well as the column list, so the JSON body and the column list it ships with
  agree. `core:networth_history` follows the same base-before-comparison rule as
  `core:spending`: it now returns `currency_code, period, net_worth, change_abs,
  change_pct`, leading with the position the two changes are measured from.

- **`moneybin system doctor` reports two data-quality checks more strictly.**
  `bridge_transfers_balanced` now requires a confirmed transfer pair to cancel
  exactly, instead of tolerating a $0.01 residue, and reports a pair whose leg
  has left `core.fct_transactions` rather than skipping it. The transfer matcher
  pairs on exactly equal amounts and `amount` is `DECIMAL(18,2)` throughout, so
  a cent of residue is missing money, not rounding. `fct_transactions_sign_convention`
  now also reports a row whose `transaction_direction` or `amount_absolute`
  contradicts its own `amount`; it still treats `$0.00` as a legitimate third
  direction, and it deliberately does not judge an amount's sign against its
  category label, which would report every refund and statement credit as a
  defect. A profile that was healthy before may surface a new failure on either
  check; both name the offending transaction ids under `--verbose`. (#504)

- **Every CLI command's `--output json` now returns the standard response
  envelope, and the JSON shapes moved with it.** Twenty-four output paths
  printed a bare `{"key": [...]}` object or a raw model dump beside
  `render_or_json` — the one path that derives the sensitivity tier from the
  payload type, applies the redaction transforms, and writes the privacy audit
  row. `.claude/rules/cli.md`, `docs/features.md`, and the CLI reference all
  described the envelope as universal; for those commands it was not.
  Scripts and agents parsing these commands must read the payload under `data`:
  `transactions matches pending` / `matches history`, `import history`,
  `import status`, `import formats list` / `formats show`,
  `transactions categorize stats`, `categorize auto stats` / `auto rules` /
  `auto review`, `sync link` / `link-status` / `pull` / `status` /
  `disconnect`, and `gsheet auth` / `connect` / `pull` / `list` / `status` /
  `reconnect` / `disconnect`. Field-level moves worth naming: the categorize
  coverage keys are now `total_transactions`, `percent_categorized`, and a
  nested `by_source` map; the format catalogue reports `institution_name`, and
  its date-only `last_used` becomes `last_used_at` carrying the full timestamp
  the MCP tool has always returned (the text table still prints the date); the
  `auto rules` total moved to `summary.total_count`; and
  the match queues return the same typed rows the MCP tools return, which drops
  the internal `app.match_decisions` columns neither surface displayed. Six
  commands stay deliberately outside the envelope, each named in the CLI
  reference: the `db query` operator bypass, the `db info` / `db ps` reads that
  describe the database file rather than its contents, and the `stats` /
  `logs` / `migrate status` operations-metadata reads. Nothing these commands
  returned would have been masked by today's transforms, so this is a contract
  and audit fix rather than a disclosure fix.
- **`--json-fields` now works on every command that offers it.** The projection
  only ever applied to a bare list payload, so on a typed payload the flag was
  accepted and silently did nothing. It now descends into a typed payload's
  single collection field, after redaction rather than instead of it, and
  no-ops cleanly when a payload carries no collection or more than one. A
  refresh's diagnostic lists (`identity_errors`, the three `rate_pairs_*`
  lists, `self_heal_actions`) and the both-manual-and-Plaid overlap warning
  (`investment_source_overlap_accounts`) are not collections for this purpose —
  they describe the rows rather than being a second set of them.
- **`import formats` (MCP) and `import formats list` (CLI) return one list.**
  The MCP tool previously split its answer into `formats` and `pdf_formats`;
  both surfaces now return a single `formats` list whose rows carry a `type`
  discriminator, so `jq '.data.formats | map(select(.type == "pdf"))'` filters
  it and an unfiltered read needs no special case. Tabular rows also carry
  `source` (`builtin` or `user`), which only the CLI reported before.
- **`sync link` (MCP) reports `link_type`, and `gsheet_pull` (MCP) reports its
  refresh outcome.** Both fields existed on the CLI side only; the shared
  payloads now carry them so the two surfaces answer alike. `gsheet_pull`
  through MCP runs no refresh, so its refresh fields report `null` rather than
  zero — "the step did not run" and "the step found nothing" stay distinct.
- **`transactions_matches_history` (MCP) carries `match_tier` and both
  `source_type_*` columns.** The terminal has always rendered them; the tool's
  rows had not.
- **`confidence_score` is nullable on the match queues.** An exact-id match
  records no score, and both `transactions_matches_pending` and
  `transactions_matches_history` (MCP and `--output json` alike) coerced that
  NULL to `0.0` — which reads as the engine having compared the pair and found
  nothing in common, the opposite of what happened. The field is now `null` for
  such a row on both surfaces, matching the dash the text tables have always
  printed. Consumers doing arithmetic on `confidence_score` must handle `null`.
- **`gsheet pull`, `import files` and `sync pull` report the row count they
  actually returned.** `summary.returned_count` and `summary.total_count`
  reported `1` regardless of how many connections were pulled, files imported,
  or institutions covered, because diagnostic lists riding beside the real row
  collection were counted as row collections themselves — the post-load
  refresh's four best-effort lists on the first two, and the
  both-manual-and-Plaid overlap warning on `sync pull`. The privacy audit row
  inherited the same wrong count. `import formats show` had the mirror-image
  version of the same defect — its one returned format carries a
  `header_signature` list, so the shipped Mint, Tiller and YNAB formats
  reported their column counts (9, 8 and 11) as row counts — and now states
  the count rather than leaving it to be inferred.

### Fixed
- **A missing or locked keychain entry no longer prints a stack trace.**
  `moneybin db info`, `db unlock` and the DuckDB init-script builder read the
  encryption key directly, and the secret-store exceptions had no branch in the
  error classifier — so the CLI showed a raw traceback and MCP returned
  `infra_unclassified_error`. They now classify: a keychain that denies the
  read reports `infra_permission_denied` with an unlock hint, and a missing
  secret or absent keyring backend reports `infra_setup_required` naming the
  command that stores it. (#522)

- **A category of spaces no longer counts as a category.** An imported
  transaction whose category cell held only whitespace was hidden from
  `core.uncategorized_queue` (which selects `WHERE category IS NULL`) while
  claiming to carry a category; `category` and `subcategory` now arrive `NULL`
  from every source when blank. Blank means what the write path means by it —
  a non-breaking space pasted from a spreadsheet and an ideographic space
  typed in CJK input both count — while a padded `'  Groceries  '` still
  arrives as `Groceries`. (#517)

- **`transactions splits add --category "   "` is refused rather than
  stored.** MCP already refused a whitespace-only category while the CLI
  stored it; both now refuse, naming the field — `subcategory must be
  non-empty`, or `splits[2].subcategory …` when setting a batch — so a caller
  is pointed at the flag they got wrong. A split already carrying one is
  backfilled to `NULL` on the next migration, and a blanked category takes its
  subcategory with it, since a subcategory without its category would render
  under the parent transaction's instead. (#517)

- **A subcategory with no category is refused everywhere, not just on MCP.**
  A subcategory is a child of a category here, so a lone one never resolves to
  a `category_id` and renders under the parent transaction's category instead
  — `splits add`, `splits set`, `transactions create`, and merchant creation
  now refuse it the way MCP's split contract always has. Manual entry was the
  quietest of them: a lone subcategory was dropped from the batch without a
  word and the call still reported success, and a blank subcategory beside a
  real category was stored against a `NULL` category_id. The import path stops
  producing one too: a category blanked on the way in takes its subcategory
  with it, while a blank subcategory under a real category still nulls only
  itself. `docs/reference/mcp-tools.md` states the rule as well, so an agent
  reading the reference learns it before calling rather than as a refusal.
  (#517)

- **`merchants create --default-category "   "` is refused rather than
  stored.** A merchant's stored default is copied verbatim into a
  transaction's category by the auto-categorization sweep, which skipped only
  a missing value, so whitespace reached `core.fct_transactions` through the
  one write path the earlier sweep left open. It now takes the same blank-text
  and hierarchy rules a split takes, and a merchant already holding a blank
  default is backfilled to `NULL` on the next migration — along with the blank
  categories it already copied, so those transactions return to the
  uncategorized queue instead of staying hidden. (#517)

- **`categories create "   "` and `budgets set --category "   "` are refused
  rather than stored.** The taxonomy is the one surface that stores a category
  instead of a reference to one, so a blank name reached `categories list` and
  gave category resolution a row nothing can usefully match; a blank budget
  category stored a target reporting against nothing. Both now take the same
  blank-text and length rules every other category write takes. (#517)

- **`transactions create --category "   "` is refused rather than absorbed.**
  It stored nothing wrong — the row simply landed uncategorized — but
  `splits add` and `merchants create` refuse the identical string, so one
  input had two answers depending on which command you reached for. Passing no
  category at all remains the way to create an uncategorized transaction.
  (#517)

- **A blank category is removed from the taxonomy, along with everything
  pointing at it.** The backfills above null a category's display snapshot,
  but `category_id` is the canonical reference and every reader prefers it — a
  split, merchant or categorization still pointing at a blank taxonomy row
  rendered the whitespace anyway, and would have kept doing so once the
  snapshot columns are dropped. The next migration clears those references
  across all seven tables that carry one, then deletes the blank categories
  themselves, so an empty-named category no longer appears in
  `categories list`. Rows that merely *referenced* one keep their data and
  lose only the unusable default; a budget or rule whose own category text was
  blank named nothing and is removed. (#517)

- **A rejected category reports a write error, not an infrastructure one.**
  `transactions splits add`, `splits set` and merchant creation classified a
  blank, over-long, or wrong-typed category as `infra_invalid_input`, so a
  script or agent branching on the error family read bad user input as a
  broken system. Splits now report `transaction_invalid_input` and merchant
  writes `mutation_invalid_input`. (#517)

- **A Plaid transaction's `category` no longer holds Plaid's own category
  code.** `prep.int_transactions__unioned` had aliased the raw
  personal-finance-category code into `category`, so one column mixed
  `FOOD_AND_DRINK` with `Food & Drink` depending only on the row's source; the
  code now stays in `plaid_category`, where the categorizer already reads it,
  and report grouping no longer splits one category's total in two. (#515)

- **`core.uncategorized_queue` and every count drawn from it grow on a Plaid
  profile.** The queue selects `WHERE category IS NULL`, so the aliased code
  above had been hiding transactions the categorizer never resolved — this is
  the inverse of the drop in #502, and for the same reason. (#515)

- **An absent category renders `Uncategorized`, and the line beneath the table
  counts it.** `transactions list` printed an empty cell and
  `transactions splits` printed `-` for the same state; both now draw on one
  placeholder and disclose the count (`… · 7 uncategorized`), counting only
  genuinely absent categories so a category authored as the literal word
  `Uncategorized` renders as itself. (#515)

- **An amount no longer folds across two lines.** Folding is the right failure
  for an identifier — an account id or a display name ending in a masked last
  four wraps rather than losing the characters that tell two rows apart — but
  it is the wrong one for money: `1,200.00` folded after the decimal point
  renders `1,200.` above `00`, and the first line reads as a complete number
  two orders of magnitude smaller. Money columns are now unwrappable, so a
  narrow terminal spends its squeeze on the text columns first; `investments
  holdings --wide` fits nine columns into 80 with every amount intact, where
  before it folded ordinary values like `1,000.00`. When even that is not
  enough the cell is marked `1,234,5…` rather than cropped to a shorter number
  that looks whole.

- **A command with nothing to show no longer draws an empty table.** A header
  row with a closing rule and no rows between them reads as a rendering
  failure, and `-q` could not suppress it, because result output is never
  quieted. `investments prices pull` printed one whenever every security
  priced successfully — that is, on its most successful runs — and
  `accounts list` and `reports networth-history` printed one whenever a filter
  or a date range matched nothing. They now print nothing, which is what the
  loops they replaced did.

- **A failed `--output json` read is audited like the successful one beside
  it.** Three commands now raise their not-found and no-database errors into
  the shared handler rather than hand-writing a JSON error branch, which is
  what earns those failures a `privacy.log.jsonl` row at all — but the handler
  falls back to the conservative `high` tier, with no returned classes, unless
  the command names its payload. `gsheet status <unknown-id>` was recorded as
  `cli.unknown`/`high` beside a success path recording
  `cli.gsheet_status`/`medium`; `import formats show <unknown-name>` and
  `import status` on a machine with no database were recorded at `high` beside
  success paths classified `medium` and `low`. All three now record the actor
  and classification their success path records, so one command no longer
  writes two provenances depending only on whether the thing it was asked for
  exists.
- **Every command now names itself in the audit trail when it fails, not just
  the three above.** `privacy.log.jsonl` recorded `actor="cli.unknown"` for any
  `--output json` failure whose command did not hand the shared error handler a
  name, which was 117 of its 164 call sites — so the trail could say which
  command returned a result but not which one failed. 84 commands declared a
  name on the success path and none on the failure path, writing two different
  provenances for one command depending only on whether it succeeded. Both
  paths now read the name off the command actually invoked
  (`moneybin mcp list-tools` audits as `cli.mcp_list_tools`), so a failure and a
  success from one command agree. No existing actor string changes: the 22
  commands whose hand-written name predates this keep it, because renaming a
  shipped actor falsifies past audit rows — and each now hands that same name
  to its failure path, since deriving one there would have disguised the split
  rather than closed it, a derived failure row reading as authoritative where
  `cli.unknown` was visibly unattributed. That covers the commands reached
  through an alias or a shared helper too: `moneybin sync connect` runs
  `sync link`'s body and `moneybin transactions review` shares its body with
  `moneybin review`, so each was recording its successes under the name of the
  command whose code it borrows. One command keeps two identities on purpose —
  `transform plan --apply` delegates to the apply and audits as
  `transform_apply`, which is the operation that actually ran. `cli.unknown` survives only for a
  call with no command behind it at all. The failure row's *tier* is unchanged
  and still defaults to the conservative `high` with no returned classes unless
  the command names its payload — that value is knowable only from the envelope
  the command builds, which a failure never reached.
- **"Uncategorized" now means one thing, and the number is smaller.**
  `moneybin review`, `system_status` and the import-drain hint counted every
  transaction with no row in `app.transaction_categories`, while the review
  queue beside them listed `core.uncategorized_queue` — which also excludes
  confirmed transfer legs, archived accounts, and transactions whose source
  system already supplied a category. Nothing errored; the count simply
  disagreed with the queue it pointed at. `core.uncategorized_queue` is now
  the single definition every surface counts, so the reported figure drops
  wherever those rows were being counted. Categorization itself is unchanged —
  only which rows are called curator work. A missing queue view is now
  reported as schema drift on the review surface rather than rendering as an
  empty queue, which had told a curator their work was done when the refresh
  that builds the view had never run. (#502)

- **MCP tool calls now record `moneybin_mcp_tool_calls_total` and `moneybin_mcp_tool_duration_seconds`.** The observability spec described this instrumentation as automatic, but no code path ever recorded either metric — a dashboard built from them stayed at zero permanently. `ValidationErrorMiddleware.on_call_tool`, the single boundary every `tools/call` request passes through, now records both metrics on every call, whether it succeeds, is translated to a validation-error envelope, or raises something else. (#495)

### Removed
- **The unused `@tracked` decorator and `track_duration()` context manager.** Neither had a production call site — every live metric already used the manual-registry pattern (`METRIC.labels(...).inc()` / `.observe()`) that is now the sole documented instrumentation contract. Removed along with the generic `moneybin_tracked_calls_total` / `moneybin_tracked_duration_seconds` / `moneybin_tracked_errors_total` series they wrote, which never carried real data. (#495)

### Deprecated
- **`MONEYBIN_MCP__MAX_CHARS` and `MONEYBIN_MCP__ALLOWED_TABLES` remain accepted but are inert compatibility settings.** `moneybin mcp config` no longer presents `max_chars` as an active limit. (#481)

### Added
- **Three references are generated from the code and pinned by a test.**
  `make generate-docs` renders `docs/reference/cli/` (one page per top-level
  command plus an index) from the Typer command tree,
  `docs/reference/mcp-tools.md` (every tool's description, parameters,
  annotations, and maximum sensitivity) from the tool list the MCP server hands
  a connecting client, and `docs/reference/configuration.md` (every setting's
  variable, type, default, and description) from `MoneyBinSettings`, and
  `test_generated_references_are_current` fails while any page is stale.
  Rendering that text exposed defects fixed at the source — five settings had
  no description, the `gsheet auth` help named a retired MCP tool, and ten tool
  descriptions spelled the undo hint as a positional call — and the CLI
  reference guide now keeps its prose and links each group's generated page in
  place of its command tables. (#525)
- **`core.bridge_merchant_entities`** — a new queryable core view mapping each
  transaction to the merchant identifier its source system assigned, alongside
  the source that issued it and the merchant name that source stated. Available
  through `moneybin sql query` and the MCP schema surface. Categorization and
  merchant harvesting now read it instead of the internal staging layer, whose
  shape carries no stability guarantee. `core.fct_transactions` is unchanged.
  (#494)
- **A Google Sheet that tracks several accounts in one tab now imports as
  several accounts.** `gsheet connect` previously required you to name one
  destination account, and every row from every account was filed under it —
  no error, no warning, just wrong balances. Detection had recognized the
  sheet's `Account` column all along and the transform discarded it.

  Omit `--account-name` / `--account-id` for such a sheet and each row is now
  attributed to the account it names, keyed the same way a CSV import keys the
  same file, so one account exported through both routes lands as one account.
  Accounts the sheet names are resolved on every pull: one seen before is
  re-adopted, a genuinely new one is created, and one resembling an account you
  already have is queued in the account-link review queue rather than becoming
  a silent duplicate. Naming an account still binds the whole sheet to it, so
  existing connections are unchanged; sheets with no account column still
  require one. Because that column now decides where every row lands, emptying
  it marks such a connection `drift_detected` instead of quietly re-filing the
  whole ledger under one nameless account, and a reconnect that would leave the
  connection with no way to key its rows is refused rather than saved broken.

  Renaming an account in the sheet leaves its transactions where they are. A
  transaction's id folds its account key, so re-deriving that key from the
  edited label would soft-delete and re-insert every row the account owns and
  orphan the notes and splits attached to them. A label that appears is matched
  to a departed account by the transactions it carries rather than by counting
  labels, because closing one account and opening another looks identical
  otherwise — one label gone, one arrived. So a rename re-labels the account,
  and a newly opened account never inherits a closed one's history. Where the
  shared history is too thin to be sure, the import creates a separate account
  you can see and merge rather than folding two accounts into one silently.

  A label is only honored while the sheet still shows the account wearing it.
  A connection remembers every label it has ever registered, so closing an
  account, dropping it from the sheet, and later giving a new account the same
  name would otherwise file the new account's transactions under the closed
  one. Such a label is now matched by its rows like any other, so the two stay
  apart — while an account that simply went quiet for a pull and returns with
  its own history keeps the key, and the id of every transaction on it.

  `gsheet disconnect --purge` now also removes the account rows a
  multi-account connection registered, counts them in the total it asks you to
  approve, and scopes both deletions to this connection's own import channel
  so a file import can never lose rows to a sheet's purge.

- **You can propose a merge for two accounts nothing automatic would pair.**
  `accounts links run` and the newly registered `accounts_links_run` MCP tool
  now accept two account ids and queue exactly that pair for review, under a
  `manual` signal with no confidence score — nothing was measured, and a number
  there would rank a bare assertion against real evidence. This is the escape
  hatch for a duplicate no signal reaches: different last four, different
  institution, nothing in common but your knowledge that it is one account.
  Until now, surfacing such a pair took a code change to the resolver, which put
  it out of reach of every surface.

  With no ids, both surfaces still sweep every account for twins, exactly as
  before. Naming only one id is an error rather than a sweep: silently
  backfilling the whole book because the second id was forgotten writes
  proposals nobody asked for. Neither form merges anything — both write pending
  proposals that clear the same confirmation gate, so the pair still has to be
  accepted by a human before any data moves. Registering the tool takes the
  50-tool standard registry to ADR-016's hard maximum exactly — admitting
  another tool now means retiring one. (#450)

  Either id may name an account an import has only just created. Those live in
  the link records before the next transform materializes them into
  `core.dim_accounts`, and imports do not refresh by default — so checking the
  materialized table alone would have refused the freshest half of every pair,
  which is the half you are most likely to have just noticed. The places that
  announce a duplicate now name this form too: the import's created-account
  hint on both surfaces, and the `duplicate_account_overlap` doctor finding,
  which measures transaction overlap and already warned that identity
  resolution may propose nothing at all for the pair it just flagged.

- **`moneybin --home <path>` picks the data directory.** Until now `MONEYBIN_HOME`
  was the only way to point MoneyBin at a different set of profiles, config and
  databases, and it appeared in no `--help` output — so the override was easy to
  own and hard to find. The flag is exported as `MONEYBIN_HOME` before any
  config loads, which means it reaches subprocesses and composes with
  `mcp install`: `moneybin --home /srv/finance mcp install` writes a client
  config pinned to that home. It wins over the environment variable when both
  are given, and `moneybin --help` now lists both together.

- **Refresh gathers the exchange rates your own data implies (M1K.2).** A new
  `rates` step runs last in the refresh cascade — after gsheet, match,
  transform, categorize and identity — and caches the reference rates needed to
  convert what you actually hold. It reads the currencies and earliest dates
  off your own transactions, daily balances, investment trades and open
  positions, then asks the provider for one date range per currency pair — a
  span covering 1999 to today is a single request.

  It asks for the whole span every time rather than resuming after the newest
  rate it already holds. Stored rates cannot prove a span was ever fetched:
  looking up two single dates years apart with `moneybin fx rate` leaves two
  rows that a resume would read as "everything between these is covered", and
  because the window only moves forward, those years would never be fetched.
  The cache discards what it already has, so the cost is one request per
  currency per refresh and the gap cannot happen.

  This is why a report can convert offline. Display conversion prices every row
  at its own date, so a three-year report needs a rate for every date it
  touches; fetching those during the report read would put a network call and
  the exclusive writer lock behind a command that looks read-only, and would
  fail outright whenever a sync held that lock. Refresh already holds it.

  The step is best-effort and never fails the command; the rest of the cascade
  keeps its results either way. A pair the provider could not answer is warned
  on stderr by name, listed in `rate_pairs_failed` under `--output json` and in
  the MCP envelope, and retried on the next refresh. The MCP envelope also
  carries an executable `refresh_run(steps=["rates"])` recovery action for it,
  matching what the matching and categorization steps already offer; the other
  two pair lists get none, because no number of retries fills them. A crash in
  the step itself is reported the same way, as `rate_backfill_error` with the
  same retry — without it a step that ran and failed is indistinguishable from
  one that correctly declined to run. A currency the provider does not publish
  at all is reported separately, as `rate_pairs_unsupported`, because retrying
  will never fill it — that warning names `moneybin fx set`, which will. Either
  side of the pair can be the unpublished one, so a home currency the provider
  does not carry is caught too. Telling that apart takes a second request, for
  the provider's list of published currencies, and when that request is the one
  that fails the pair is reported as failed: nothing at that moment separates a
  currency that will never publish from one briefly unreachable, and only the
  retryable outcome leaves a later run able to separate them. A pair whose
  answer was partly unusable — dated outside the window, or too small for the
  rate column — is listed as `rate_pairs_discarded`, which says coverage may
  be short on some dates rather than that the pair is missing. A series that
  does not span what was asked for lands there too: a currency the provider
  only started publishing partway through your history answers every date it
  has and none of the ones before, and one that stopped being published
  answers nothing recent. Neither drops a rate, so nothing else would mark
  them, and neither is filled by waiting — the window opens at your earliest
  row, so it moves back only when earlier data is imported, and a series that
  has stopped does not resume because you refreshed again. An ordinary
  unpublished today is not this: the check allows two weeks, so it fires on a
  series that has stopped, not on one that lags.

  A profile with no home currency set fetches nothing. Run it alone with
  `moneybin refresh --step rates` or `refresh_run(steps=["rates"])`. A
  `gsheet pull` runs it too, because a pulled sheet can carry foreign-currency
  rows, and reports every one of those outcomes the way `moneybin refresh`
  does — as warnings, and on the JSON envelope under `--output json`. Only
  currency codes and dates leave the machine.

  Every other command that closes with a refresh reports it the same way.
  `moneybin import files`, `moneybin sync pull`, the inbox drain and their MCP
  tools all run the full cascade, so each of them reaches a rate provider on
  your behalf; each now surfaces the outcome instead of reporting only whether
  SQLMesh applied. The three best-effort steps that were already silent on
  those surfaces — matching, categorization and identity — come with it, so a
  crash in any of them is named wherever it happens rather than only under
  `moneybin refresh`. `rates_written` is `null` when the step did not run and
  `0` when it ran and had nothing to fetch: an empty pair list reads the same
  either way, so that field is what separates them. Their MCP envelopes carry
  the same executable `refresh_run` retries `refresh_run` itself returns — a
  step that crashed inside an import is no less retryable for having crashed
  there, and MCP has no stderr to warn on. They withhold those retries when the
  SQLMesh apply itself failed, the way `refresh_run` always has: the apply is
  the blocker, and a step retry beside it would run against the same broken
  warehouse and fail identically. A crashed identity pass now offers its retry
  too, which is the one step that carried its failure with nothing executable
  beside it.

  A drifted `core.*` schema is reported rather than skipped. Refresh runs
  before the first transform, so a missing `core.*` is the normal state of a
  new install and the step stays quiet for it — but a renamed column or a
  dropped model on a mature database raises the same error from the same call,
  and answering `rates_written: null` with no error made real drift look like a
  profile that had nothing to fetch. The quiet path now applies only when none
  of the relations it reads exist at all.

  The window closes on the UTC day rather than the host's. Reference rates are
  published against UTC dates, so east of UTC a host-local "today" asks for a
  day the provider has not published yet.

  A pair no retry can fill now names its remedy on every surface. `moneybin fx
  set` is a CLI command, so it can never be an executable MCP action — the CLI
  had warned about it all along while the MCP envelopes carried the field and
  said nothing about closing the gap. The refusal to offer a futile retry is
  unchanged; what is added is the sentence saying what does work.

  A malformed currency catalog is refused rather than believed. The provider's
  published-currency list is what separates "this pair will never publish" from
  "the provider is having a bad minute", and a 200 carrying `{}` or a status
  message is object-shaped enough to have been read as the catalog itself —
  which reported every real currency as permanently unsupported and sent the
  user to write manual overrides.
- **A multi-currency demo persona.** `moneybin demo --persona international`
  (and `moneybin synthetic generate --persona international`) builds five
  accounts at five banks in EUR, GBP, CAD, AED, and USD, each funded and spent
  in its own currency, with local merchants and local cities on the statement
  lines. AED sits outside the FX provider's published set on purpose, so the
  unpriced-pair path is reachable from a shipped fixture. Because the profile
  holds more than one currency, `demo` prints net worth per currency instead of
  a single total, and its JSON emits `net_worth: null` beside a `per_currency`
  breakdown — a null a consumer can read, never the string `"None"`.

  A persona's `currency_code` is shape-checked when the YAML loads, by the same
  validator every other currency entry point uses: a typo like `usd` would
  otherwise reach `raw` and `core` verbatim and surface as a second segment
  beside `USD`. A well-formed code the FX provider does not publish, like AED,
  still loads — the check is shape, not membership. A transfer between two
  accounts in different currencies is now refused outright: the generator moves
  one magnitude to both sides without converting, so it would have paid a 100
  USD outflow as a +100 EUR inflow. Fund each currency from its own income
  until M1K.3 gives the generator a conversion of its own — read-time display
  conversion, below, prices what a report shows and never rewrites a stored
  amount, so it does not lift this restriction.
- **Exchange rates, and your own corrections to them (M1K.2).** `moneybin fx
  rate USD EUR 2026-03-13` answers with the rate, the day it was published for,
  and which layer supplied it. Precedence is your own correction, then the
  cached provider rate, then the business day a weekend resolves back to, then
  one live fetch from Frankfurter — cached to `raw.exchange_rates`, so the same
  question costs at most one network call. A pair with no published rate is
  refused rather than answered with a nearby one, and the refusal separates a
  currency the provider prices on no date from a date that happens to lack one:
  the first needs your own number and always will, the second needs a different
  date. An answer carried back to an earlier business day names that day.

  `moneybin fx set USD EUR 2026-03-13 0.87138` records your own rate — your
  bank's, when that is the rate your statement used. It outranks every cached
  provider rate for that date, leaves other dates untouched, writes
  `app.exchange_rate_overrides` with a paired audit row, and takes effect
  immediately. `moneybin fx delete` returns one date to provider pricing, and
  `moneybin fx list` reads the stored series off disk without fetching. A
  non-positive rate is refused: a zero would convert every balance in that
  currency to nothing, and because an override outranks the provider, nothing
  downstream would contradict it. Rates are stored to eight decimal places as
  `DECIMAL`, never through a float, and only currency codes and dates leave the
  machine — no amount, account, or description is part of a rate request.

- **Read a report in one currency (M1K.2).** `--display-currency EUR` on every
  report-reading command, and `display_currency` on the `reports` MCP tool,
  price a report's amounts into one currency at read time. Omit it and the
  target is the currency you set with `moneybin profile set home_currency EUR`;
  a report with nothing to price is unaffected either way.
  `summary.display_currency` names what the numbers are in, and
  `summary.applied_rates` names the rates that got them there — each distinct
  pair with its rate, its source, the date asked for, and the date actually
  priced, which differ whenever a weekend or holiday falls back to the previous
  published day (Requirement 10). The terminal prints the rate when one priced
  the whole report and a summary line when several did. Only rates that were
  actually applied appear: a row already in the target currency resolves to an
  identity rate that was never stored, and listing it would announce a
  conversion on a single-currency report where nothing was priced.

  `moneybin export report` deliberately takes no `--display-currency`: an
  exported file outlives the rate that priced it, so exports carry original
  currency and conversion stays a read-time act.

  A value *derived* from a converted amount is restated rather than left
  describing the old currency. `core:balance_drift` restates `drift` itself as
  the converted asserted balance minus the converted computed one — all three
  columns are priced independently and round apart, so the published difference
  would otherwise disagree with the two balances printed beside it — then
  derives `drift_abs`, `drift_pct`, and the clean/warning/drift verdict from
  that single figure: a 500 JPY drift shown as 3.40 USD reads `warning`, not
  the `drift` it was at 500. `core:networth` recomputes `net_worth` from its
  own converted parts, so independent per-column rounding cannot leave the
  total disagreeing with assets plus liabilities. A converted `core:networth`
  read also collapses its per-currency totals into a single headline row,
  because conversion relabels every row into the target currency and several
  rows all claiming the same unit would hand a consumer an arbitrary fraction
  of the position; the row `limit` applies to that collapsed result, so asking
  for fewer rows than the profile holds currencies no longer cuts a subtotal
  out of the sum. That headline sums the per-currency totals, each converted
  and rounded once, so it can sit a cent under the converted account rows it
  summarizes; summing those rows instead would make the headline follow an
  `account_ids` filter and report one account as the whole position.
  `no-data` and `currency-mismatch` are untouched:
  neither describes a magnitude. The `status` *filter* still selects in the
  account's own currency, so filter on `all` and read the returned status when
  converting.

  Three of the eight registered reports convert exactly, because each of their
  rows is one event on one date: `core:large_transactions` at its transaction
  date, `core:balance_drift` at its assertion date, and `core:networth` at its
  balance date. `core:large_transactions` returns
  `amount_zscore_account`, `amount_zscore_category`, and `is_top_100` as null
  on each row a read actually repriced: those are cut in
  SQL against each row's original currency, and rates move between transaction
  dates, so a converted read is not one scaling of the amounts they scored.
  Which rows come back is still decided by original-currency magnitude,
  including the `anomaly` filter. A row already in the requested currency was
  not repriced and keeps its scores, so a single-currency profile is unaffected
  and a mixed one loses the lens only where a rate was applied.
  A priced row also carries `original_currency_code`, naming which
  `applied_rates` entry converted it — the set is deduplicated by currency and
  date, so two source currencies on one date would otherwise publish two rates
  with nothing tying either to a row whose currency label has been rewritten to
  the target. Added by the framework on a converted read only, and null on
  `core:networth`'s summed headline, which no single rate priced.
  `summary.applied_rates` names only rates that priced a row you were sent: a
  capped read prices one row past the cap to decide `has_more`, and that row's
  rate is dropped with it rather than published alongside its `requested_date`.
  Stored rates are gathered into the home currency, so asking for any other
  display currency generally finds none and falls back to per-currency
  segmentation with a reason; extending the refresh planner to cover chosen
  display targets is filed as a followup.
  The other five — cash flow, spending trend, merchant activity,
  recurring subscriptions, and the net-worth history series — aggregate with
  `currency_code` in the grouping key, so a row is already a per-currency
  subtotal and pricing it would leave several rows sharing one grain key under
  one currency label. Those stay segmented per currency and say so in
  `summary.degraded_reason`, which now also reaches the terminal on
  `moneybin reports networth` and `networth-history`. A pair no stored rate
  covers falls back the same way, naming `moneybin refresh` as the remedy;
  requesting a currency code that names no currency is refused outright, before
  the query runs, so an empty report cannot label itself in a currency that does
  not exist.

  `moneybin reports networth` prints one position per currency, and one combined
  position once conversion has priced them into the same one. The
  `core:networth` rows change shape to match: per-currency totals now lead as
  rows of their own, and the account-breakdown rows that follow carry null
  `net_worth`, `total_assets`, `total_liabilities`, and `account_count` rather
  than repeating the headline figures on every row. A consumer that read every
  row as a position must now branch on the row kind — the two are distinguished
  by which half is null, and `docs/specs/reports-net-worth.md` states the
  contract. Repeating the headline is what conversion makes unsafe: pricing
  relabels each row's `currency_code`, so a repeated position would arrive as
  several indistinguishable ones and anything summing them would count it once
  per account. `moneybin
  investments holdings` publishes a portfolio total across currencies for the
  first time, pricing each position at its own close's rate and printing the
  original per-currency amounts beside the converted figure;
  `data.total_market_value_currency` names its unit, and `data.applied_rates`
  names every rate behind it, in the same six fields `moneybin fx rate`
  publishes; the terminal prints that rate too, through the same renderer the
  reports surface uses. `moneybin refresh` now gathers rates for each holding's own
  `price_date` rather than for today, so a carried-forward foreign position
  whose close predates the refresh is priced instead of dropping the combined
  total. Investment positions still do not contribute to net worth — that
  integration is unbuilt.

  Conversion is presentation only. No converted amount is stored, and no
  original-currency column in `raw.*` or `core.*` is touched: the original
  amount stays the auditable one, and a converted figure is recomputed on every
  read. Reads never fetch a rate — `moneybin refresh` gathers them, because a
  read holds no writer lock.
- **`sql_schema` can name what it does not curate.** Two sets governed the SQL
  surface and nothing said they were different: `sql_query` reads five schemas
  (`core`, `app`, `reports`, `raw`, `prep`), while the curated catalog describes
  only the 35 of 93 `TableRef`s tagged `audience="interface"`. Everything in the
  gap — every `raw` and `prep` model, the internal `app` tables — was queryable
  but undiscoverable, and asking `sql_schema` for one returned
  `sql_unknown_table`. That is a false negative on a table that exists and reads
  fine, and it steers an agent toward correcting the name instead of reaching
  for `DESCRIBE`.

  `sql_schema(table='<schema>.*')` — for example `table='raw.*'` — now lists one
  schema's live relations with their `kind` (`table` or `view`) and a `curated`
  flag. A name that exists but carries no curated entry returns the new
  `sql_table_not_curated` code, whose hint names `DESCRIBE <table>`; a schema
  outside the queryable five returns `sql_schema_not_allowed`. Compact-catalog
  entries gained `kind` for the same reason, and the compact response's
  `actions` name the `'<schema>.*'` path so the default call reaches it. `table`
  is matched the way DuckDB resolves an unquoted identifier — case-insensitively
  — so `'RAW.*'` and `'raw.*'` are one request, and any spelling `sql_query`
  accepts, `sql_schema` accepts. Every `sql_schema` response now reports
  `summary.returned_count` as the number of tables it actually returned; the
  compact catalog and full document previously reported `1` regardless.

  This narrows disclosure rather than widening it. The listing is bounded by the
  same `ALLOWED_QUERY_SCHEMAS` that gates querying, so it can never name a
  relation the caller cannot then query and never reaches `meta` or `seeds` —
  unlike the `SHOW ALL TABLES` catalog route, which does surface their shape. No
  new tool and no new parameter: the existing `table` argument gained a pattern
  alongside the `'*'` it already accepted.
- **`system_status` names the build it is running.** The envelope carries a
  `build` block with the package `version` and the `revision` the source
  checkout was on when the process started (`null` for an installed wheel).
  A stdio MCP server is a long-lived process pinned to whatever the checkout
  held at boot, and until now nothing in the surface said which. A caller
  comparing live behaviour against `main` could not separate a stale process
  from a real defect — and did not: a three-day-old server rendered a
  confirmation prompt without the ledger-evidence sentence added in #387, which
  was written up as the confirmation gate being absent. The gate was present.

  Both fields are resolved once at import rather than per call. A checkout can
  move underneath a running server and an environment can be upgraded beneath
  one; a per-call read would then report the new commit or the new version while
  the process still ran the old code — corroborating precisely the wrong
  conclusion. `version` is the more dangerous of the two to read late, because
  a wheel install reports no revision and leaves the version as the only signal.
  Reported on the degraded database-locked path too, since it needs no database
  connection and that is when a caller most needs it.

  The registered `system_status` description tells the agent to cite
  `overview.build` before concluding that live behavior contradicts the code.
  A docstring cannot: the agent never reads one, and the field is only as
  useful as the instruction to look at it. `revision` is read from the
  repository top level only, so a wheel installed beneath an unrelated checkout
  reports `null` rather than that project's commit.
- **Read the ingestion pipeline through `sql_query` (M2O.2).** `sql_query` and
  `moneybin sql query` reach `raw` and `prep` alongside `core`, `app`, and
  `reports` — five schemas, up from three. The seed sheets the gsheet and PDF
  importers were built to expose (`raw.gsheet_<alias>`, `raw.pdf_<alias>`) are
  readable from the agent surface for the first time, and an import is
  debuggable without dropping to the unmasked operator CLI. `meta` and `seeds`
  stay refused, by `DESCRIBE` just as by `SELECT`.

  Masking holds. 34 `raw`/`prep` columns are declared CRITICAL and masked by
  class: every institution account number, routing number, and account label
  derived from one; the account-prefixed `match_group_id` composite; plus two
  opaque loader payloads (`import_log.account_names` and
  `import_preview_snapshots.source_bytes`) that are masked whole because their
  contents cannot be enumerated. Every other value there is scanned per run
  and masked when it is shaped like an SSN (`***-**-****`) or holds an unbroken
  run of eight or more digits (`****...<last four>`). The scan reads text and
  integers only, so three shapes pass through: an account number of four to
  seven digits, one written with separators such as `1234-5678`, and any value
  the column types `DECIMAL` or `FLOAT`. Declare such a column in
  `INTERNAL_CRITICAL` to mask it by class instead.

  `SHOW ALL TABLES` is the discovery statement the schema catalog hands an
  agent. `duckdb_tables()` excludes views, and `prep` is entirely views — as are
  the seed views — so the older catalog query listed none of what this change
  opened.

  A saved report reads the same five schemas. `reports create` names the columns
  riding the scan rather than a declaration, beside the columns it masks whole.
  A `raw`/`prep` report runs and exports; `reports explain` reports that it
  cannot graduate to a materialized `reports.*` view, which still requires
  `core`/`app` upstreams.
- **Save your own reports (M2P.2).** `moneybin reports create <name> --sql "..."`
  turns a query into a durable report that behaves like a shipped one: it appears
  in `reports list`, runs through `reports run` or `moneybin export report`, and
  is masked by the same rules. You never declare privacy classes — MoneyBin reads
  them off the SQL at save time and stores them, so a routing number in your own
  report is masked exactly as it is in a built-in. If an upstream column is later
  reclassified as more sensitive, the saved report notices and masks that column
  rather than serving the stale class, and the response says so
  (`summary.degraded`) rather than masking silently — including on the plain-text
  path, where `reports run` prints the reason under the table, alongside the
  `reports explain` command that names every column's derived class whenever any
  column masked, instead of leaving a bare `*****` for the reader to interpret.
  An exported artifact records
  the same verdict in its provenance receipt, so a file opened months later still
  distinguishes columns masked by drift from columns that were empty at source.
  `reports set --archive`
  hides a report from `reports list` without retiring it — an archived report
  still runs, exports, and explains by id or name, and
  `reports list --include-archived` shows it marked as archived. `reports set`
  re-derives on any SQL or
  parameter change; `reports delete` is undoable through `system audit undo`;
  `reports reclassify` lowers one column's masking floor on an explicit human
  confirmation, and its audit row records whether that confirmation came from the
  prompt or from `--yes`. That confirmation names the class the report's SQL
  derives *right now* — `'spend' from txn_amount to aggregate` — so an upstream
  reclassification cannot turn an approval you read one way into a permanent
  downgrade of something else, and the approval is refused if the class moves
  between the question and the write. A blank `--reason` is refused: it is the
  only durable record of why the floor was lowered, and both it and the set of
  downgrades a report accumulates are length-bounded, since every later edit
  copies them into its audit record. `reports set --clear-params`
  drops every declared parameter, which is the only way to move a parameterized
  report to SQL with no placeholders left: omitting `--param` means "leave the
  declarations alone", every occurrence of the flag requires a value, and a
  declaration the new SQL no longer interpolates is refused.

  Because the SQL is yours, two boundaries treat it as data rather than as text
  to echo. A `redacted` export withholds it — the receipt carries `sql: null`
  while keeping `lineage`, `parameter_classes`, and `output_classes`, so what the
  export read stays auditable without republishing a literal your rows would have
  masked; the unredacted artifact still carries the statement. Every name you
  wrote goes with it — columns and parameters alike are published as
  `redacted_column_1` / `redacted_parameter_1`, and the drift note reports its
  reason code rather than naming the columns that moved. A name is your own text
  and can carry a literal on its own (`SELECT 1 AS "021000021"`), so a redacted
  artifact withholds all of them rather than guessing which are safe; the class
  of each column and the query's lineage stay in the receipt. And when an
  upstream rename invalidates a stored query, `reports run` and `export report`
  report `report_query_execution_failed` and name the likely cause instead of
  surfacing a DuckDB binder error, which quotes the statement it failed to bind.
  The log keeps the exception type and a SHA-256 digest of the query, and a
  report whose stored SQL a later release can no longer parse says so by
  exception type rather than repeating the fragment it choked on. A parameter
  your query *returns* (`SELECT $acct AS acct`) is masked like the filter value
  it is, rather than published in the clear because no column backs it.

  `reports run` caps a text run at 1,000,000 rows, which `--limit --help` now
  states and the table says whenever it bites — a truncated financial answer
  never renders as a complete one. A `reports reclassify` approval is also
  re-checked against current policy on every re-derivation, so a release that
  raises the class you downgraded *to* retires the approval and masks the column
  rather than serving it under a floor nobody would grant today. It covers the one
  column you confirmed and nothing else: if anything else about the report's
  classification has drifted since it was saved — another output column, or one of
  its filter parameters — the downgrade is refused and names what moved, so
  approving one thing cannot quietly store a change to another. Save the report
  again and the approval goes through.

  The same rule covers every name you choose, not only the SQL. A report's name,
  its output aliases, and its declared parameter names never enter a log record:
  each site logs the report id with a count or a class and echoes the detail to
  the terminal, because `SanitizedLogFormatter` masks account numbers and dollar
  amounts by pattern and cannot recognize `amazon_spend`. A test enumerates the
  shape across the reports surface so a new log line cannot reintroduce it. (#367)
- **Every report can show its work: `moneybin reports explain <handle>` (M2I).**
  Returns the query in two forms — the executed form with parameters rendered as
  literals, and the stored template — plus each output column's privacy class and
  which upstream column it descends from, the tables it reads, when its
  classification was last derived, and whether it can be promoted to a
  materialized view (with the reason when it cannot). Works for built-in,
  extension, and saved reports alike, and runs nothing. A parameter classed above
  the lowest tier keeps its placeholder in the executed form: rendering is not
  execution, so it gets no redaction pass and must not publish a value the report's
  own rows would mask.
  All seven `reports` verbs are CLI-only; the MCP registry gains no tools.
  (#367)
- **Report listings now show the handle you type.** The report catalog entry
  carries `name` beside `report_id`, and `reports list` leads with it. Those two
  differ for every tier: a built-in's id is namespaced (`core:networth`) and a
  saved report's is minted (`user:r` plus twelve hex characters), while the CLI
  command, `reports run`, `reports explain`, and `export report` all accept the
  name. Publishing only the id left the one string those commands take
  undiscoverable once the create response scrolled away. Applies to the MCP
  `reports` catalog too. (#367)
- **A profile can now declare its home currency (M1K.1).** `moneybin profile set
  home_currency EUR` records which currency the profile treats as home;
  `moneybin profile show` lists it under `Settings (database)`, separate from the
  `config.yaml` values. The setting lives in `app.profile_settings` rather than
  `config.yaml` because the report views that read it are SQLMesh models, so every
  write is audited and reversible through `system_audit_undo`. A profile that has
  not chosen one reports null — MoneyBin does not assume USD, which would relabel a
  EUR-only user's money. Setting it converts nothing: every transaction and balance
  keeps its original currency. The 49-tool standard registry adds two tools: `profile`
  reads the active profile's metadata and managed settings, and `profile_set`
  writes the home currency. Two of the three remaining slots below the 50-tool hard
  limit are now spent.
- **Reports sub-total each currency instead of blending them (M1K.1).** Every
  report that sums money — net worth, cash flow, spending trend, merchant
  activity, large transactions, recurring subscriptions — now carries a
  `currency_code` column and groups by it, so a profile holding dollars and
  euros gets one sub-total per currency rather than one meaningless number.
  Anomaly z-scores and the top-100 flag in `reports large-transactions` compare
  each charge against transactions in its own currency. `reports networth`
  withholds its headline total when more than one currency contributes and
  reports each currency's position instead; conversion to a single display
  currency arrives in M1K.2. **A single-currency profile sees the same figures
  it always did**, plus the currency they are denominated in. `moneybin system
  doctor` gained a `currency_integrity` check: it fails on any account or
  transaction whose currency is unknown (those amounts join no total until you
  run `accounts set --currency`, then `moneybin transform`) and warns when a
  profile holds more than one currency, so segmented totals are explained
  rather than surprising.
- **`accounts` and `accounts_get` report an unknown currency as null, not the
  string `"None"` (M1K.1).** Both read paths coerced the column with `str()`,
  which renders SQL NULL as a four-character string. That was unreachable while
  `dim_accounts` defaulted to `'USD'`; removing the default made it routine, and
  an agent reading `"currency_code": "None"` could take it for a denomination.
  `AccountSummary.currency_code` and `AccountDetail.currency_code` are now
  nullable, matching every other currency-bearing payload.
- **Daily balances no longer add foreign-currency transactions to an account's
  running balance (M1K.1).** `core.fct_balances_daily` carries a balance forward
  adjusted by the transactions in between. Because a transaction resolves its own
  currency, a USD account can hold a EUR charge — and adding the two produced a
  number in no unit that nothing downstream could flag, since the row still
  reported USD. The carry now applies only transactions denominated in the
  currency being carried. The excluded movement is not lost: it appears in that
  account's `reconciliation_delta` and as drift in `reports balance-drift`, and
  the `system doctor` multi-currency warning names the behaviour. **A
  single-currency profile is unaffected.**
- **`top` now means "top N within each currency" (M1K.1).** On `reports
  merchants` and `reports large-transactions`, ranking across currencies compares
  unlike units, so one high-denomination currency could otherwise take every slot
  and hide the rest entirely. A single-currency profile gets the same N rows as
  before.
- **A report's `summary.display_currency` names the currency its rows are in
  (M1K.1).** It reported `USD` unconditionally. It now names the one currency the
  rows agree on, and is null when they span more than one, when the currency is
  unknown, or when the report has no currency column at all — a report that
  counts or ranks states no denomination rather than borrowing one. Read each
  row's `currency_code` whenever it is null.
- **`core.fct_transaction_lines` carries the transaction's currency (M1K.1).**
  The split-expanded grain — one row per unsplit transaction, N per split —
  projected every column of its parent fact except the denomination, so the
  canonical grain for per-line analysis could not tell a EUR line from a USD
  one. It now carries `currency_code`, and the curated `sql_schema` examples
  that sum money across `core.*` group by it rather than blending units.
- **Every ranked report interleaves its currencies (M1K.1).** `reports
  balance-drift`, `large-transactions`, `merchants`, and `recurring` each sorted
  one currency's rows ahead of the next, and the surface row cap keeps a prefix
  — so one high-denomination currency could fill every slot and drop the others
  out of the response entirely, absent rather than merely ranked lower. Rows now
  interleave: rank 1 of every currency, then rank 2. Compare amounts only
  between rows sharing a `currency_code`. A single-currency profile sees the
  same order as before.
- **The curated `sql_schema` examples interleave their currencies too
  (M1K.1).** Fourteen examples led their sort with `currency_code`, and both
  the `sql_query` row cap and any `LIMIT` an agent adds keep a prefix — so an
  agent copying "Top merchants by lifetime spend" got one currency's rows and
  none of the others. Twelve now rank within each currency first, or lead with
  the month or term the query is really grouped by. The other two return
  exactly one row per currency, where ordering is not the lever.
- **Grouped reports and the categorization queue interleave their currencies
  too (M1K.1).** The two entries above fixed this defect where it was found —
  four ranked reports, then fourteen curated SQL examples — and it kept
  resurfacing in siblings nobody had swept for. Enumerating every
  truncation-reachable sort found four more. `reports cashflow` and `reports
  spending` sorted each month currency-major, so a capped month reported one
  currency's categories and dropped the rest. `reports networth-history` walked
  one currency's entire series before starting the next, so a currency opened
  partway through the window disappeared from a capped response rather than
  showing a shorter series. `transactions categorize pending --sort impact`
  ranked `ABS(amount) * age_days` across denominations, letting the
  highest-denomination currency fill the whole queue — the one case with no
  `currency_code` in its sort at all. All four now rank within each currency and
  sort on that rank. A single source guard replaces the old literal scan and
  covers both report channels, the SQL runners and the service-backed reports,
  so the next sibling fails a test rather than a review round.
- **`sql_query` names the currency its rows agree on (M1K.1).** The envelope
  derived `summary.display_currency` from dataclass and Pydantic rows only, and
  `sql_query` returns plain dicts — so every ad-hoc query reported an unknown
  currency, including `SELECT amount, currency_code ...` where every row agreed.
  Mapping rows now derive like typed ones. A query whose rows disagree, or that
  omits the column, still reports null.
- **`transactions` rows name their currency (M1K.1).** The `transactions` MCP
  tool and `moneybin transactions list --output json` returned bare amounts.
  A mixed-currency page reports `summary.display_currency: null` by design, so
  the row was the only place that could name the unit and it did not — two
  −30.00 rows in different currencies read as the same charge. Each row now
  carries `currency_code` from `core.fct_transactions`.
- **The uncategorized review queue names each row's currency (M1K.1).**
  `transactions categorize pending` and `transactions_categorize_pending` asked
  you to act on a bare amount. Each row now carries `currency_code`. Its
  `impact` sort and `--min-amount` filter still compare nominal magnitudes, so
  they are only meaningful within one currency.
- **`reports balance-drift` withholds a drift across two currencies (M1K.1).**
  A balance observation states its own currency, and an account can carry a
  different one after `accounts set --currency`. The report subtracted them and
  labelled the result with the account's currency; it now reports the new
  `currency-mismatch` status with no drift value, which is also selectable via
  `--status`. Its `clean`/`warning` thresholds (1 and 10) are absolute amounts
  in each row's own currency and are not converted.
- **An account whose currency nobody stated is now unknown, not USD (M1K.1).**
  `core.dim_accounts` took `USD` whenever an account had no explicit currency
  setting, and every transaction and balance inherits its account's currency, so
  one guess relabelled the whole ledger. An account now takes the currency its
  own source reported — OFX `CURDEF`, Plaid `iso_currency_code`, the tabular
  `currency` column — and stays unknown when no source stated one. Imports from
  OFX and Plaid are unaffected: those formats always carry a currency. A CSV
  without a currency column now reports unknown, and `moneybin system doctor`
  names the accounts to fix with `accounts set --currency`. Unknown amounts join
  no cross-currency total until you set one.
- **Balance reads name their own currency (M1K.1).** `accounts balance show`,
  `history`, and `reconcile` — and the `accounts_balances` MCP views behind them —
  returned every amount labelled `USD`, because the response envelope defaults to
  it and nothing overrode the default. Each observation now carries its own
  `currency_code`, and `summary.display_currency` names the one currency a
  response shares or is null when its rows span several, matching how registered
  reports already answer. The text output prints the currency beside the amount
  (`n/a` when unknown).
- **No response invents a currency it was not told (M1K.1).** The response
  envelope defaulted `summary.display_currency` to `"USD"`, so every one of the
  251 places that builds one claimed dollars for free — and nine of the eleven
  tools that return money never overrode it. `accounts`, `accounts_set` and the
  five `investments` reads all echoed a EUR account's credit limit, holdings and
  cost basis labelled USD, while their own descriptions told the agent to read
  `summary.display_currency`. The envelope now reads the currency off the payload
  it was handed: a response whose rows carry `currency_code` reports the one they
  agree on, and reports null when they disagree or none is known. Naming a
  currency explicitly still overrides it, so a report that resolved the currency
  across every matching row keeps that answer rather than the returned page's.
  **A single-currency profile sees the same value it always did.** Responses that
  carry no money — and the ones whose money has no currency recorded anywhere
  (`transactions`, `reviews`, `import_files`, `import_preview`, `system_audit`,
  `investments_lots_select`, `transactions_categorize_rules`) — now report null
  instead of an unfounded `USD`; threading a currency through those payloads is
  tracked separately.
- **Balance assertions state the currency they are in (M1K.1).**
  `accounts balance assert` / `assertion-list` and the `accounts_balances`
  assertions view returned a bare number: `app.balance_assertions` stores no
  currency, so nothing on the response said what unit it was. An assertion is a
  statement about one account, so each row now carries that account's
  `currency_code`, joined at read time rather than stored so it cannot drift from
  the account. The CLI prints it beside the amount (`n/a` when unknown).
- **`system doctor`'s `currency_integrity` labels which ids it is naming
  (M1K.1).** Its `affected_ids` mixed bare account and transaction ids in one
  list with nothing to tell them apart, though each needs a different fix. They
  are now prefixed `account:` / `transaction:`, matching the convention the
  `orphan_app_state` check already uses.
- **`moneybin profile show` no longer crashes on a database from before this
  release (M1K.1).** Reading the profile's settings opens the database read-only,
  and read-only opens skip schema initialization and migrations — so the first
  command run after upgrading met a missing `app.profile_settings` and printed a
  DuckDB traceback instead of the profile. An absent table now reads as "no home
  currency chosen", the same answer a fresh profile gives. The `profile` MCP tool
  took the identical path and is fixed with it.
- **Canonical bundle and registered-report export delivery (M1O).**
  `moneybin export bundle` and `moneybin export report` publish redacted CSV by
  default to immutable profile-scoped artifacts, with Parquet, XLSX, ZIP, named
  local destinations, and output-only Google Sheets targets also supported.
  `export_run`, `exports_set`, and `system_status(sections=["exports"])` expose
  the same service outcomes to MCP; unredacted output is an explicit per-run
  choice. Report artifacts always contain the complete registered-report result,
  receipts identify the selected format, bundle/report Sheets metadata remain
  independently verifiable, and cancellable publication runs without holding
  the global DuckDB writer lock over filesystem or Google API I/O.
- **Independent price feeds for held securities.** `moneybin investments prices pull`
  refreshes closes from Tiingo (equities, ETFs, and mutual fund NAVs) and CoinGecko
  (crypto), so a position values from a source other than the broker that reports
  it. Fetch scope comes from open positions rather than the whole catalog. A feed
  key binds silently only when the symbol names one catalog entry and the provider
  agrees about its exchange and issuer name; anything ambiguous is queued for
  review instead, because a ticker is not an identifier — the same symbol names
  different securities across exchanges and gets recycled after a delisting.
  `investments prices set` records your own price for a security and date, which
  outranks every provider close for that date; `investments prices delete` returns
  the date to provider-derived valuation; `investments prices list` shows the
  resolved series with the source that won each date. A non-positive mark is
  refused — a worthless position is a ledger event, not a zero price. Store the
  Tiingo token with `investments prices token`; CoinGecko needs no credential.
  `moneybin system doctor` gains four checks over the price series: two feeds
  quoting the same security, date, and currency more than 2% apart (one of them
  is wrong, and valuation picks a winner by rank without saying so); held
  positions carrying no usable price, which report no market value and are
  absent from every total that sums one; positions still valued from a close
  older than their security type allows — four days for a stock, one for crypto
  — so a holding no feed covers can no longer sit in your net worth at a price
  nobody has confirmed in years; and price rows whose source the
  pipeline cannot resolve, which are discarded before they can value anything.
  One source failing does not cost you the others: a missing Tiingo token still
  refreshes crypto, and the refresh names the source that failed and why. Prices
  implied by your own ledger now come only from executions — a dividend's
  per-share rate is no longer read as that security's market close. Accepting a
  queued feed key binds it (`moneybin investments securities links set <id>
  --accept --into <security>`); accepting an identity decision still merges the
  two securities, and the confirmation you are shown says which one it is.
  Pulled closes reach your holdings once the models rebuild: pass
  `--refresh` to do it in the same command, or run `moneybin refresh run`
  afterwards — the pull names the step rather than leaving new rows silently
  unreachable. `set` and `delete` cross that same boundary and carry the same
  `--refresh` flag and hint, so correcting a wrong price cannot report success
  while every total keeps showing the value you just overrode. A failed rebuild
  exits non-zero and tells you to retry the rebuild alone, since the closes are
  already stored and re-pulling would spend the provider's rate limit
  re-fetching them. A mark takes the currency the position is actually held in
  rather than assuming dollars, and asks when a security is held in two — a
  price only values a holding quoted in the same currency, so a fixed default
  reported success on a mark that valued nothing. When a provider is rate-limited
  or down, the refresh says so instead of reporting your holdings as unsupported,
  and stops asking that provider rather than spending the rest of the quota
  confirming the same failure. `--currency` must be a real ISO-4217 code and a
  price must be a finite number: `USDX` and `NaN` are refused up front rather
  than stored as a mark that quietly matches no position. A `--note` is bounded
  at 2,000 characters, the same limit every other note in MoneyBin carries.
  Prices in
  `core.fct_security_prices` are now treated as your own financial data rather
  than public reference data, because that column can carry the exact price you
  paid or a valuation you wrote yourself. A crypto `--since` reaching further
  back than CoinGecko's keyless tier serves is refused, naming the earliest date
  it can return — it previously narrowed the window to 365 days and reported a
  full backfill — and equities still pull over the window you asked for. In the
  review queue, accepting a price-feed symbol through `identity_links_decide`
  now binds it instead of failing with "nothing to merge away", and the
  confirmation you read before a security merge names and counts every category
  it moves — including the price marks you set by hand — instead of listing tax
  lots alone. Binding a feed key no longer reports that security's whole ledger
  as affected: it creates one link and moves nothing, so it says so. A `--since`
  after the last complete day is refused before any request, rather than reaching
  the providers as an inverted range and coming back as a feed outage. A price
  outside what the stored column can represent is refused as a usage error rather
  than rounding on the way in, so the price echoed back is the price stored. When
  a ticker rename retires an auto-derived feed key, the closes already stored
  under the old symbol keep valuing that security instead of disappearing from
  reports, and the old key is retired only once a replacement is in hand — a
  provider that cannot answer for the new symbol no longer leaves the holding
  unpriced. Because ticker symbols get reused, a feed key now belongs to one
  security only for the stretch it was bound: the next security to list under a
  recycled symbol values itself from that symbol's closes going forward, not from
  the previous company's earlier prices — which stay with the previous company
  rather than appearing in both series at once. Recording a mark for a date two
  feeds disagreed on now settles that date and clears it from `system doctor`,
  which previously kept reporting the same disagreement after you followed its
  own advice. CoinGecko quotes about 60 of ISO-4217's ~180 currencies, and a
  holding denominated in one of the other 120 no longer costs you every other
  coin's prices: that holding reports its own failure and the rest of the batch
  still refreshes. A price mark moved onto the surviving security by a merge
  keeps the date you authored it instead of being restamped with the merge
  time. A provider close too large for the stored column is reported against its
  own security rather than ending the refresh and discarding every security
  priced alongside it. Pulls now measure the last complete day in UTC, the day
  the providers themselves close on, so a machine east of UTC no longer asks for
  a day that has not finished and one west of UTC no longer skips a day that
  has. (#373)
- **Brokerage positions now carry a market value.** `moneybin investments holdings`
  reports `market_value` and `unrealized_gain` for every position priced by the close
  your broker already sends through `sync pull` — no new network calls, no credentials.
  Each row states the date of the price it used and how many days old that price is.
  A position with no usable price reports no value rather than zero, and one whose
  share count is known to be wrong — a split the broker reported but MoneyBin could
  not derive — withholds its value instead of publishing a number wrong by the split
  factor. Previously `investments holdings` reported cost basis alone. A security no
  connected broker prices stays unvalued until external feeds land, and positions do
  not yet fold into net worth. (#347)
- **An AI assistant can now resolve a credit-card PDF's sign inversion
  without you leaving the chat.** `import_preview(file_path=...)` followed by
  `import_confirm(preview_id=...)` shows you the statement's evidence and
  printed-vs-recorded sample rows and asks you to approve; approving imports
  the statement, and declining imports nothing. Clients without an in-chat
  prompt receive a single-use confirmation token for retrying the same bound
  request. The assistant cannot approve on your behalf, and if the statement
  turns out to have no such question pending, nothing is imported. Previously
  this one case sent you to a terminal, even though the same inversion already
  asked you in place on spreadsheet and AI-extracted-PDF imports.
- **`moneybin system doctor` now reports one account imported under two
  identities.** When the same real account arrives from two sources and account
  identity fails to bind them, every transaction is held twice under two ids —
  the balance and the spending both read double. Nothing caught it: the
  transaction matcher only compares pairs within one `account_id`, so it never
  considered the duplicates, and the raw→core row accounting reconciled
  perfectly because nothing was lost. The new `duplicate_account_overlap` check
  warns when a large share of one account's transactions have a same-amount
  counterpart within five days on a sibling account at the same institution,
  and names the pair with its overlap percentage. Transfers between two
  accounts at one bank do not trigger it — amount equality carries the sign —
  and neither do two accounts held in different currencies, where equal numerals
  are coincidence rather than evidence.
  Try `accounts links run` on a flagged pair, then `accounts links pending`.
  Identity resolution matches on institution+last-four and name similarity, not
  on transaction overlap, so a flagged pair may raise no proposal — that same
  binding failure is what split the account in the first place.

### Fixed
- **An investment event recorded without a currency now inherits its account's
  currency instead of being written as USD.** `moneybin investments add` and the
  `investments_record` MCP tool both substituted a literal `USD` when the caller
  omitted one, and the `raw.manual_investment_transactions` DDL and the manual
  staging model each supplied the same literal underneath them. A brokerage
  account denominated in anything but dollars therefore had its lots, cost basis
  and realized gains labelled in a currency it does not hold. Omitting the
  currency now stores none, and `core.fct_investment_transactions` resolves it
  the way `core.fct_transactions` already resolves the cash grain: the event's
  own currency if given, else the account's, else unknown — never a guess.
  Passing `--currency` / `currency` still wins. Events written before this
  change keep their stored currency: every write path passed `USD` explicitly,
  so a fabricated value cannot be told apart from one you typed, and rewriting
  them would erase real answers. Setting the account's currency with
  `accounts set --currency` repairs every event that carries *no* currency; one
  that already carries a wrong one cannot be relabelled in-product yet, because
  a manual investment event has no delete or revert and re-recording it appends
  a second row rather than replacing the first. A position whose open lots mix a
  known currency with an unknown one now withholds its market value instead of
  pricing the combined quantity at the known currency's close — the same guard
  that already withheld a position holding two different known currencies.
- **A stale or forged pagination cursor no longer re-serves rows an earlier
  page already returned.** A continuation key that sorted ahead of its
  snapshot widened the page back to page one and returned duplicates as an
  ordinary successful response; the `transactions`, `system_audit` and
  `reviews` MCP tools accepted such a cursor, and a date or timestamp written
  in a different-but-valid ISO spelling could slip one past the other paged
  views too; cursors MoneyBin mints are unaffected. (#498)
- **Account merge proposals no longer fire on a shared generated label alone,
  on either side of the comparison.** Two unrelated accounts whose *display
  name* was never set by a person or a source — both resolving to a bare
  `checking`, or to the same `institution + subtype + last four` shape — read
  as a name match and queued a merge proposal with no real evidence behind it.
  This applied to `accounts links run`'s backfill sweep and to every live
  import: OFX has no account-name field, so every OFX-sourced account name is
  generated, and a same-institution pair could still be proposed as a merge
  purely because their generated descriptors happened to coincide.
  `core.dim_accounts` now carries a `display_name_is_user_set` provenance
  flag, `SourceAccount` carries the equivalent `account_name_is_user_set` for
  a source account presented at import time, and the resolver's weak name
  signal requires the applicable flag on both sides of a match. A PDF
  statement's captured account nickname is now also persisted to
  `raw.tabular_accounts.account_label` (previously only held in memory for
  the current import), so a genuinely person-named PDF account keeps reading
  as named on the next import or backfill sweep. Every source channel sets
  this flag — OFX, tabular (four import branches), PDF, Plaid, and Google
  Sheets. (#493)

### Changed
- **`system doctor` stops narrating its successes.** Every run printed a ✅ line
  per invariant, so the one ❌ that mattered sat in a block of nine lines saying
  nothing was wrong. A clean run now prints just its summary; a run with
  problems prints only the invariants that have one. Warnings and skipped
  checks still print — the summary counts them without naming them, so hiding
  either would leave you knowing something was off and unable to see what.
  `--verbose` shows the full roll, alongside the affected transaction IDs it
  already showed. `--output json` is unchanged and still carries every
  invariant. The summary itself now survives `-q`, which previously took it:
  with passing invariants no longer narrated, a quiet clean run would otherwise
  have printed nothing at all. `-q` now silences the 💡 next-step hints and
  nothing else, and a failing invariant still prints under it.

- **A report's table fits your terminal, and says what it left out.** Six of the
  eight built-in reports returned nine to fourteen columns and printed all of
  them, so an 80-column terminal wrapped every row into an unreadable block —
  `large-transactions` alone needed 243 characters. Each report now declares the
  columns that answer it, and prints those. `--wide` restores the rest, and when
  anything is omitted the table is followed by `4 of 13 columns shown — --wide
  for all`, so a narrowed view is never a silent one. That line prints to stdout
  with the table and survives `-q`: redirecting a report to a file has to capture
  the disclosure along with the data.

  `--output json` and every MCP caller keep the full projection — the column
  choice is a text-rendering decision, and `--json-fields` remains the JSON
  caller's own filter. The currency column stays visible in every default set:
  amounts are aggregated per currency, so two rows differing only in a hidden
  `currency_code` would read as one row counted twice.

  `reports spending --compare` now changes what you see. It validated its
  argument and then ignored it — the view returns all three comparisons
  regardless — so `--compare mom` was documented as intent and observable
  nowhere. It selects which comparison the table shows by default.

  A report that declares no columns — one you saved yourself, or one a
  third-party extension provides — is fitted to your terminal instead. It keeps
  the first and last columns, drops from the middle outwards until the table
  fits the window you actually have, and marks the gap with `…`, the way DuckDB
  and pandas render a result too wide to print. So the same saved report shows
  more of itself in a maximized window than in a narrow one, and `--wide` still
  returns the whole projection.

  A converted read keeps its `original_currency_code` column whichever
  narrowing applies. Display conversion relabels every amount into the target
  currency, so dropping it would leave the table stating what each row is worth
  and losing what it was — and the only other disclosure goes to stderr, which
  a redirect to a file does not capture.
- **Google Sheets connects with no setup.** MoneyBin now ships the OAuth client
  secret alongside its public client ID, so `moneybin gsheet auth` completes on
  a bare install. Google's Desktop clients require both halves, and a wheel
  carries no dotenv to read a user-supplied secret from — so shipping only the
  ID meant every user first registered their own Desktop client in the Google
  Cloud Console, the 15 minutes of setup this connector chose OAuth to avoid. A
  credential shipped to every user is not confidential (RFC 8252 §8.5); PKCE
  and the loopback redirect carry the security: Google delivers an
  authorization code only to a redirect URI the client registered, and a
  Desktop client may register only loopback, so a mailed consent link delivers
  the code to the victim's own machine rather than the sender's. The shipped
  client declares `spreadsheets.readonly` alone, and MoneyBin now refuses to
  request write access while running on it — exporting *to* a sheet needs your
  own client, as it always has — which keeps Google's unverified-app warning
  meaningful on any screen that asks to edit your spreadsheets. The shared
  client draws on one Google project's quota of 300 read requests per minute,
  so `MONEYBIN_GSHEET__OAUTH_CLIENT_ID` and
  `MONEYBIN_GSHEET__OAUTH_CLIENT_SECRET` remain supported and are the
  documented remedy for anyone throttled, or unwilling to trust MoneyBin's
  project identity on the consent screen. Setting a secret without its own
  client ID is still refused by name. The reasoning and its sources — RFC 8252
  §8.5, Google's own installed-app documentation, and rclone's 2026 retirement
  of its shared client — are written up in `docs/guides/connect-gsheet.md`
  under "Why MoneyBin ships a client secret" (#475).
- **A Google Sheets grant is now bound to the client that obtained it.**
  Google issues a refresh token to one specific OAuth client, so a grant
  obtained under your own client cannot be refreshed under MoneyBin's shipped
  one, or the reverse. MoneyBin records the issuing client alongside each grant
  and refuses to reuse it under a different one: `gsheet auth` reports the
  grant as unauthorized and re-runs consent. Previously it reported
  `already_authorized`, served the cached access token until it aged out, and
  only then failed with "OAuth token refresh failed. See application logs for
  detail." Grants stored before this release record no issuing client, so each
  needs one `moneybin gsheet connect` to re-authorize (#475).

- **Every table the CLI prints is now built the same way.** Twelve commands
  rendered rows through five different idioms — a shared Rich helper for three,
  and a hand-padded f-string per command for the rest, each with its own guessed
  column widths. They all go through one `render_rows` now, which sizes each
  column to its widest value, so `accounts links history` no longer aligns only
  the rows that fell back to ids. Two more renderers cover the other shapes a
  command prints: `render_summary` for a labelled block like `reports networth`,
  and `render_note` for the status lines `-q` silences. Result rows and
  summaries have no way to be silenced — neither renderer accepts a quiet flag.

  No value is ever elided to make a row fit: a name too wide for the terminal
  wraps, because a resolved account name ends in the masked last four and
  clipping it removes exactly the digits that tell two candidates apart
  (#470).

- **Amounts print with thousands separators and a sign that means something.**
  Every money column now declares what its number *is* — a signed flow, a
  positive magnitude like `SUM(ABS(amount))`, a change in one, or a balance —
  and the renderer reads that declaration instead of guessing from the value.
  `reports spending` no longer risks rendering spending as green income, and a
  rise in spending reads as a rise in spending rather than as a gain. Negative
  amounts carry `−` (U+2212), matching the rest of the product. A negative net
  worth keeps its minus: "balances unsigned" only ever meant no decorative `+`
  on a positive position. Colour is redundant with the sign glyph and appears
  only on a terminal with `NO_COLOR` unset, so piping or redirecting output
  loses nothing (#470).

- **`transactions matches` stops reporting an unscored match as `0.00`.** An
  exact-id match records no confidence score; the two match tables printed that
  as zero, which reads as the engine having compared the pair and found nothing
  in common. It now prints `-`, matching what the merchant and security link
  queues already did (#470).

- **`--help` no longer lists commands that aren't built yet.** Twelve
  whole-command placeholders — `budget delete/set`, `sync key rotate`, `sync
  schedule set/show/remove`, `transactions categorize ml apply/status/train`,
  and `db key export/import/verify` — are hidden from `--help`, along with the
  four groups (`budget`, `sync key`, `sync schedule`, `transactions categorize
  ml`) that contained nothing else. `db key` stays listed because `db key show`
  and `db key rotate` work. Nothing is removed: every one of them is still
  invocable and keeps its exit code, so a script calling `sync key rotate`
  today behaves exactly as before — it just no longer appears in the menu of
  things MoneyBin claims to do. They now report "not yet implemented" on stderr
  at every `MONEYBIN_LOGGING__LEVEL`, so the three `db key` placeholders can no
  longer exit `1` with no output — previously they printed nothing at
  `WARNING`, and would have printed nothing at `ERROR` or `CRITICAL` (#457).
- **Messages name transforms, not the library that runs them.** Help text,
  option descriptions, progress lines, and migration warnings said "SQLMesh" —
  a dependency you did not choose and cannot act on. They now say "transform".
  `system doctor` follows: two check names it printed verbatim are now
  `transform_model_presence` and `transform_audits_unavailable`, so a script
  matching the old names needs updating. `moneybin logs sqlmesh` is unchanged:
  that one is a log-file name you type, not vocabulary. `db migrate` also no
  longer reports the transform engine's internal state-schema number as though
  it were your MoneyBin version. The profile banner names the single source
  that actually resolved your profile instead of listing candidates (#457).
- **Your file's own account name is now what MoneyBin calls the account.** A
  spreadsheet's Account column, `--account-name`, and Plaid's per-account name
  were read for matching and then discarded before the account was named, so a
  sheet whose Account column named the account produced one called
  `Unnamed account`, and a named account fell through to a bare last four. That
  name now outranks the institution and account type MoneyBin would otherwise
  assemble a label from — the account list already prints both in their own
  columns, so the name is where the part it cannot show belongs — and keeps the
  last four beside it (`Vacation Fund …1111`), because a name you chose is not
  necessarily unique: two of a household's accounts are routinely given the
  same product name by their bank. A name that already shows four digits of its own is left
  exactly as it is, so nothing ever prints two separate pieces of one account
  number. Account numbers embedded in the label are masked,
  and one that ends the label becomes that account's last four in the form
  every surface uses (`Checking 987654321098` → `Checking …1098`). A label
  holding no letters at all is treated as an account number rather than a
  name, so it keeps the assembled label instead.
  A connected account takes the new name on the next `moneybin refresh`, since
  its name was already in the raw table. A spreadsheet-sourced one takes it on
  the next import of that file: the new column is deliberately not backfilled,
  because the masking and last-four rules live in Python and a SQL backfill
  would be a second copy of them. Refresh alone leaves those accounts named as
  they are today. An explicit `moneybin accounts set --display-name` still
  wins over everything (#446).
- **Account-merge candidates carry measured ledger overlap instead of a
  constant `confidence`.** An import that offers to merge a file into an
  account you already have returned a `confidence` on every candidate — a
  literal stamped per resolution rung that no input could move. Two candidates
  found on the same rung tied at the same number however differently their
  ledgers overlapped, so a caller ranking on it could not tell an account
  sharing every transaction from one sharing none, and the field's name invited
  reading that tie as a judgement. `import_files` and `import_confirm` no longer
  return the field; rank on `overlap_matched` / `overlap_comparable`, which the
  candidate already carried. The review queue and decision history had dropped
  it for the same reason, so all three surfaces now agree. Agents parsing
  `account_proposals[].candidates[].confidence` must switch to the overlap
  pair; an absent pair means the two ledgers share no comparable period, which
  is absence of evidence rather than a zero (#416).
- **A merge whose rebuild failed, or an accept the reconciliation refused, now
  exits non-zero.** `accounts links set`, `transactions matches set`, and
  `transactions review --confirm/--confirm-all` printed a warning and exited
  `0`, while `refresh` exits `1` on the identical failure. A script or agent
  gating on exit status could not tell that a merge is still invisible in
  `core.dim_accounts`, or that the status it asked for is not the one that
  committed, without scraping stderr. All three now exit `1`; a `--confirm X
  --reject Y` invocation still performs both before reporting (#388).
- **Breaking:** **PDF account matching now uses statement identity without a
  new secret or user setting.** Exact files get opaque content-derived keys;
  validated complete account identifiers are retained only as routing-scoped
  `full_number` links in the encrypted database. Masked, suffix-only, and
  unscoped statements stay review-only; labelled account metadata is captured
  and incoming-ledger overlap is shown as matched/comparable evidence. The old
  issuer-plus-last-four PDF links are migration candidates rather than silent
  matches, preventing distinct same-bank accounts with the same suffix from
  merging invisibly. Same-path legacy statements retain migration evidence;
  a statement that was both renamed and reclassified under another issuer
  requires manual binding until exact-file hash provenance is wired through.
- **The account-link queue now shows how much of the ledger already matches,
  instead of a confidence score that never moved.** Every proposal in
  `accounts links pending` carried `0.5` or `0.4` — a constant picked by which
  signal fired, not a measurement of anything about your accounts — so the one
  number offered for the decision could not tell a certain merge from a doubtful
  one. Each candidate now reports how many of the provisional account's
  transactions already appear in the candidate's ledger, matched on amount
  within a few days to absorb the gap between a statement's transaction date and
  a feed's posting date. Against a real duplicated card that reads `345 of 346`;
  against an unrelated account at the same institution, `0 of 346`. The
  comparison is scoped to the period both accounts cover, so a statement archive
  that predates a feed's download window reports `no shared period` rather than
  a zero that would read as evidence against a correct merge. The group header
  also states how many transactions the merge would move, so deciding no longer
  costs a second command (#387). Two rows count as the same transaction only
  when they agree on currency as well as amount: at a multi-currency
  institution a USD 10.00 row and a EUR 10.00 row are different money, and
  without that a USD checking account and a EUR savings account proposed
  together by their name could read as a perfect twin. Silence is not
  disagreement — two ledgers that never stated a currency still match each
  other, since refusing them would switch the evidence off for every account
  whose source never reported one. The veto reads the currency and not its
  spelling: a spreadsheet column holding `usd` names the same money as a
  statement's `USD`, padding and letter case included, and a currency column
  left blank on domestic rows counts as unstated rather than as a currency of
  its own (#387).
- **One merge now reads the same way wherever it is proposed.** The CLI prompt,
  `accounts_links_set`, and `identity_links_decide` each described the same
  decision differently, and the worst of the three named both accounts by their
  display label — identical text on both sides in exactly the split-account case
  the feature exists to fix. All three now render one sentence that names each
  side by what *differs* between them (which source reported it, how much
  history it holds, the period it covers, its subtype, its currency), states
  which account is absorbed and which survives, carries the overlap evidence,
  and names the reversal in the syntax of the surface you are reading it on —
  `moneybin system audit undo <operation_id>` at the CLI,
  `system_audit_undo(operation_id=...)` over MCP. It is the one clause that
  differs between the two, because a recovery command is worth nothing unless it
  runs where it is read. When the surviving account has
  no transactions of its own — a malformed placeholder offered as the survivor,
  which the live queue produced — the prompt says so and tells you to check the
  direction before accepting. The closing paragraph now describes only the link
  kinds the batch actually contains, so a card-to-card merge is no longer warned
  about security tax lots and hand-set price marks it never touches. An account
  that arrived over `moneybin sync` is described by that channel rather than by
  the provider the sync server happens to speak to, which is an implementation
  detail behind the server and not something a confirmation prompt should
  name (#387).
- **`identity_links_decide` now reports `sensitivity: "medium"`.** Its response
  still carries only record ids and counts, but its merge prompt shows
  transaction dates and the account labels you wrote, and the privacy audit
  event recorded the response's tier rather than what you were shown. A tool
  that renders classified data in a confirmation prompt now declares that tier
  alongside the one derived from its response, and the higher of the two wins
  (#387).
- **A name match no longer proposes a merge across two different stated last
  fours.** Two accounts that agree on a fuzzy name and *disagree* on a last four
  they both state are evidence of two different accounts, not one; the name rung
  proposed them anyway. It now skips that pair. Silence is not disagreement — an
  account with no known last four still reaches the name rung, since vetoing
  there would drop a proposal nothing else surfaces. At the same institution the
  pair re-surfaces under `institution_reissue`, which is the signal a reissued
  card actually carries, so the proposal is retyped rather than lost. That
  retype is offered alongside any other name match rather than only when there
  is none, so an unrelated account that happens to share the name and states no
  last four cannot stand in for the reissue and hide it (#387).
- **`moneybin accounts links set --into` now shows what the merge moves and asks
  before committing.** Accepting a link folds one account's whole history into
  another, and no command splits it back apart — but this was the last accept
  path that ran on a single unprompted invocation, while the same decision driven
  through `identity_links_decide` had prompted since M1. The command now prints
  the same sentence the MCP prompt shows, counting the accounts, transactions,
  tax lots, and hand-set price marks that move, and waits for an answer. Pass
  `--yes` to answer in advance. `--standalone` is unchanged and never asks:
  keeping an account separate destroys nothing. Answering the prompt binds the
  merge to what you read: inside the write transaction the command re-derives
  both the counts it printed and the exact link and decision rows the write
  will repoint and settle, and refuses rather than committing a merge that
  changed while the question was on screen. That covers changes the printed
  sentence has no words for — a sibling proposal a concurrent
  `accounts links run` added — and changes no count can express, such as one
  pending sibling being resolved elsewhere while another arrives. Declining
  prints `Cancelled — nothing was merged.` and exits 0, matching every other
  confirm in the CLI (#385).
- **`identity_links_decide`'s merge confirmation now binds to the same exact
  rows.** The approval token digested the two account ids and a set of counts,
  so a same-count change between the prompt and the commit verified cleanly.
  It now carries the link and decision ids as well, through the `resolved_ids`
  field the grant already covers. Tokens issued before this change no longer
  verify — re-run the decision to get a current one (#385).
- **A confirmation prompt no longer expires against the tool's timeout.** That
  cap exists to release a wedged database connection, and charging a person's
  reading time to it produced a dead end rather than a safeguard: at 30 seconds —
  the default for 10 of the 16 operations that ask — the prompt was cancelled and
  the answer came back `timed_out` with no token to retry with, so there was no
  way to finish the operation at all. The cap now pauses while a prompt is on
  screen and resumes with its remaining budget. Prompts get their own 120-second
  window (`MONEYBIN_MCP__ELICITATION_WAIT_SECONDS`), after which the operation
  degrades to the token path it already had for clients that cannot prompt —
  still gated, still finishable. An unanswered export-redaction prompt refuses
  instead, rather than falling back to a policy nobody chose.
- **Breaking:** **Every import now stops before it merges a file into an
  account you already have (#378).** Previously only CSV/Excel stopped to ask;
  OFX and PDF resolved and bound the account on their own, so the one moment
  MoneyBin could be wrong about *whose* transactions these are went by unseen.
  All three channels now stop when a file could plausibly be an existing
  account — showing which ones — and load nothing until you answer. Answer
  with `--account-binding REF=ACCOUNT_ID` (or `=new` to keep it separate) on
  `moneybin import files`, or the `account_bindings` parameter on the
  `import_files` and `import_confirm` tools. Pinning up front with
  `--account-id` / `--account-name` still skips the question entirely, and a
  statement for an account you have already confirmed is still silent — you
  answer once per account, not once per file.

  **A file that matches nothing is not a question, so it is not asked.** Its
  account is created and named back to you instead: `👀 Created account:
  sample_bank CHECKING (e3a84714695d)`, with the rename and merge commands on
  the next line. The same pair of fields arrives as `accounts_created` on
  `--output json`, on the `import_files` per-file rows, and on
  `import_confirm`. Asking here charged one confirmation per file on a first
  import, each with exactly one answer available. The exception is a file that
  states no account at all — a bare Date/Description/Amount CSV, or a PDF
  statement with no readable account number. There the only name available is
  the filename, and MoneyBin asks rather than guessing.

  Each account the gate shows is labeled `@0`, `@1`, … for the file in front
  of you, and that label is what `REF` takes — the account's own key works
  too. The labels are why an assistant can answer this at all: the key is an
  account identifier, so it reaches an assistant masked as `****1234`, and a
  question you can only answer by typing something you cannot read is not a
  question. The labels number the file's accounts, so `@0` means the same
  account whether you answer the first time or the fourth. They are not names
  to keep: they mean nothing on the next import.

  Two behavior changes fall out of this. **An agent no longer gets a
  different answer than you do:** it used to be allowed past this question,
  quietly creating a provisional account and filing a suggestion for you to
  review later, which put MoneyBin's weakest guess into effect on the surface
  where nobody is watching. It now stops exactly where you would.
  **Imports no longer add to the account-review queue** — the suggestions
  arrive while you are importing instead of accumulating for later. That
  queue still exists and is still filled by account sync.

  *Upgrading:* nothing to migrate, and no re-import is needed. Scripted
  imports of OFX or PDF files that resemble an account you already have will
  now stop and ask; add `--account-binding` (or an `--account-id` pin) to
  those calls. A known account, and a genuinely new one, keep importing
  unattended. `--account-binding` answers one file, so `moneybin import files`
  refuses it alongside several paths rather than dropping it: run those calls
  one file at a time. It also cannot contradict an `--account-id` pin on the
  same account — send whichever one you mean.
- **Breaking:** **An account you named on a file type that cannot use it is now
  an error instead of silence (#378).** `--account-name` and `--account-meta`
  reached only spreadsheet imports, and `--account-id` only spreadsheets and
  PDFs; passing one with any other file type was accepted and discarded, so the
  import bound whatever it worked out for itself while you believed you had
  chosen. Each of those combinations now refuses before anything loads and
  points at `--account-binding`, which every file type honors. The MCP tools
  refuse the same combinations, from the same table. (Error code
  `import_pdf_account_signal_unsupported` is replaced by
  `import_account_signal_unsupported`, since the refusal is no longer PDF-only.)
- **`moneybin import confirm` takes `--institution` (#378).** An OFX whose
  issuer is underivable from `<FI><ORG>`, the FID lookup, and the filename
  fails before the account question is ever reached, so the only way to reach
  that question is `moneybin import files <file> --institution <name>`. The
  recovery command printed there dropped the override, so pasting it hit the
  institution error again. It now carries it, and `import confirm` accepts it.
  Refused alongside `--bridge-response`, which has no institution to apply.
- **Cross-source duplicates now auto-merge on description agreement rather than
  on the calendar date, and the candidate window widens from 3 days to 5
  (#377).** Previously any pair landing on the same day merged silently no
  matter how differently the two sources described it, and a pair one day apart
  did not. Both halves were wrong: the same-day band held amount collisions
  between genuinely different merchants, while pairs whose descriptions already
  agreed sat in the review queue because the card had posted a day late. A pair
  now auto-merges when one description contains the other, at any gap inside the
  window — provided the shared text carries something more than transaction-type
  boilerplate, since `DEBIT` sits inside most card descriptions and identifies no
  merchant. Two sources writing the *identical* description are exempt from that:
  a bank labelling a row `Deposit` in both exports cannot name a merchant — but
  only when both rows posted the same day, since two different charges of one
  amount days apart are both `DEBIT` too; that pair goes to review instead.
  Punctuation no longer decides the question — `STARBUCKS #1234` and
  `STARBUCKS 1234` agree — while a differing reference number still does, so
  `SHELL 1234` and `SHELL 1235` stay two transactions. A number on its own is not
  merchant text either: a source rendering a row as `POS 1234` no longer agrees
  with every longer description that prints the same card digits. Nor is a name
  buried inside a longer word — `ARCO` and `MARCOS PIZZA` name different
  merchants and no longer agree. A shared string may still stop mid-word, which
  is what a source truncating to a fixed width does, but only when a whole word
  naming a merchant matched before the cut: `STARBUCKS STO` agrees with
  `STARBUCKS STORE 1234 NEW YORK NY`, while a bare `SHELL` no longer agrees with
  `SHELLY'S CAFE` — one word that happens to begin another is as easily two
  merchants as one truncation, and that pair now goes to review. Boilerplate does
  not count as the word that matched, so `CARD SHELL` and `CARD SHELLY'S CAFE`
  are reviewed too rather than merged on a word every card description carries.
  Two transactions in different currencies are never paired at all, whatever
  their descriptions say: a EUR 10.00 and a USD 10.00 charge are equal as
  numbers and unequal as money, and the wider window and description-led merge
  would otherwise have collapsed them. A source that records no currency is not
  treated as a mismatch, so this costs no existing match. Which existing
  duplicates merge and which go to review both change on
  the next `refresh`. `matching.date_window_days` is shared with transfer
  detection, so its new default widens that candidate window too.
- **A high score no longer merges two transactions on its own (#377).** Closeness
  and description agreement were both feeding one number, and a pair landing on
  the same day could clear the auto-merge bar on closeness alone — two distinct
  charges at one merchant differing only in a trailing reference number scored
  0.97 and one of them was deleted, with no review entry and nothing recorded.
  Agreement is now required at the decision itself, for same-source duplicates as
  well as cross-source. Same-source pairs have no review queue, so a disagreeing
  pair there is left unmerged: both rows stay in the ledger, which is a
  double-count you can see in a total rather than a deletion you cannot. The
  rebalanced weights narrow same-source auto-merging for the same reason — a
  same-day pair now needs 0.93 description similarity to merge silently where it
  needed 0.92 — so a few near-duplicates inside one source that used to merge
  will stay as two rows. A pair that cannot merge no longer takes an assignment
  slot from one that can: when a row had two candidates in one file, a
  higher-scoring disagreeing pair used to claim it and then be discarded, and the
  agreeing pair behind it was lost with it.
- **Every cross-source duplicate candidate now reaches the review queue (#377).**
  Pairs scoring below `matching.review_threshold` used to be dropped and logged
  at DEBUG — the duplicate stayed in the ledger and nobody was told. Expect the
  pending-review count to rise on the first `refresh` after upgrading; the pairs
  were always there.
- **An account minted without a last four now proposes into the identity review
  queue instead of appearing silently (#377).** Such an account cannot
  participate in last-four resolution at all, so its silence was never evidence
  that it was a distinct account. Cash accounts, manually created accounts, and
  sources that never publish the digits will each raise one proposal to confirm
  or dismiss. A source that sends the digits as blank — an empty string or only
  spaces — counts as missing them, so a connector reporting an unavailable mask
  either way is quarantined too rather than minting a second copy of a card you
  already have. Digits that arrive padded (`" 1234 "`) now resolve against the
  account holding `1234`, where before the padding made the lookup miss and minted
  a second account for a ledger that already had one. When one of these accounts
  does raise a proposal, every existing account is offered as a merge target —
  not the first 25 in id order, and not only those at a matching institution:
  you can only merge into an account the proposal itself lists, so an omitted
  one left no way to resolve the duplicate except declaring the account
  standalone, which re-created the duplicate the proposal existed to prevent.
  Accounts at the institution the source names are still listed first, so the
  likely answer stays at the top of a long list.
- **BREAKING for anything branching on an error `code`: 104 code values were
  renamed.** They were raised from tool paths without ever being declared in
  the taxonomy, so they had never been reviewed for shape; each now carries the
  prefix of the domain it came from (`ACCOUNT_QUERY_REQUIRED` →
  `account_query_required`, `NOTE_REFERENCE_NOT_FOUND` →
  `transaction_note_not_found`, bare `ambiguous` →
  `sync_institution_ambiguous`, bare `invariant_failure` →
  `audit_invariant_failure`). Any prompt, script, or watchdog matching an old
  string stops matching silently — that exact failure occurred inside MoneyBin
  during this change and is now caught by a test that scans comparisons as well
  as raise sites. `moneybin system doctor --output json` is the one documented
  surface affected; `docs/guides/observability.md` is updated.
- **An unclassified tool failure is now a successful MCP call carrying an error
  envelope, not a protocol-level error.** Clients see `isError` false with
  `status: "error"`, a `code`, and a `hint`, where they previously got a
  transport error whose only content was a bare string.
- **`moneybin transactions list --cursor` tokens from before this release are
  rejected.** The cursor changed from base64 offset to the keyset envelope MCP
  already used; restart the walk from page one.
- **`moneybin db key export|import|verify` no longer name an internal tracker
  file in their not-yet-implemented message.** The three commands printed a
  path to a repository-internal document no reader outside the project can
  open; they now state only that the command is not yet implemented.
- **`reports.*` column privacy classes are now derived from each SQLMesh
  model's source and verified in CI**, replacing a hand-maintained bridge
  file. A report's declared `classes=` map is checked against an
  independent, connectionless re-derivation of the same model on every
  build; an undeclared or under-classified column now fails CI instead of
  shipping quietly. (#330 follow-up)
- **`core.uncategorized_queue` (the categorization curator queue) moved out
  of the `reports` schema into `core`.** It was never a user-facing report —
  its only reader is the categorization surface
  (`transactions_categorize_pending`) — so it no longer appears under
  `reports.*` in `sql_query` / `moneybin sql query` results. Query it as
  `core.uncategorized_queue` instead.
- **`transactions_categorize_pending`'s `age_days` field is now declared
  `TXN_DATE` (MEDIUM) instead of `AGGREGATE` (LOW).** A declaration
  correction, not a behavior change: both classes redact via the same
  pass-through, and the response already carries HIGH-tier `amount`, so
  masked output and the response's overall tier are unaffected.
- **`sql_query` / `moneybin sql query` responses can now report `unresolved`
  in `classes_returned`.** This is the fail-closed class for a column
  reaching the caller without lineage having positively established what it
  holds; seeing it always means the value was masked, not that something
  broke.
- **Your accounts now show the bank's name instead of its routing code.**
  A Chase account read as `B1` and a Wells Fargo one as `WF`, because OFX
  files carry a short institution code where you'd expect a name. Those now
  resolve to `Chase` and `Wells Fargo`. Credit-card statements also no longer
  come through untyped — a card that showed as `B1  …4242` now reads
  `Chase credit card …4242`.
- **`core.dim_accounts.account_type` now uses one vocabulary for every
  source.** It previously carried whatever each source called things — OFX
  said `CHECKING`/`CREDITLINE`, Plaid said `depository`/`credit`, a
  spreadsheet said whatever was in the column — so `accounts --type credit`
  silently missed accounts, the by-type summary split one concept into
  several buckets, and an account could change its label when a different
  source refreshed it. Values are now `depository`, `credit`, `loan`,
  `investment`, `other` (`NULL` if the source spelling isn't recognized),
  with the finer distinction kept in `account_subtype` (`checking`,
  `savings`, `credit card`, ...). Account names are built from the subtype,
  so they read `Wells Fargo checking …7777`, not `Wells Fargo depository
  …7777`. Queries filtering on the old uppercase values need updating; run
  `moneybin transform apply` to rebuild.
- **`core.dim_accounts` gained an `institution_slug` column, and account
  matching now compares it instead of `institution_name` (#375).** OFX files
  identify the bank two ways, and only one of them is a name: Chase's `<ORG>`
  is `B1`, so an OFX import offered `b1` while the account dimension held
  `Chase`, and every institution-based match missed. Slugifying the display
  name doesn't close the gap either — `U.S. Bank` gives `u-s-bank`, not the
  registry's `us_bank`. Every source now resolves to the same registry slug
  before comparison: OFX by exact `<FID>`, tabular and Plaid by matching their
  institution text against `seeds.institutions` with case and punctuation
  stripped, so a spreadsheet's `U.S. Bank` and a statement's `<FID>` reach the
  same account. An unregistered institution keeps its own text. Where several
  sources merge into one account, a resolved slug wins over unresolved text
  regardless of arrival order, so one unrecognized spelling in a later
  spreadsheet can't overwrite the canonical slug.
  `institution_name` is unchanged and stays the display column. Run `moneybin
  transform apply` to rebuild; until then the MCP server reports the missing
  column as schema drift at boot and in `system_status`.
- **`accounts_set`'s currency parameter is now `currency_code`, not
  `iso_currency_code`.** Aligns the account-currency parameter name with
  every other currency field in the schema. Pre-launch, so this is a direct
  rename with no deprecation alias — any script or agent calling
  `accounts_set(iso_currency_code=...)` needs to update to `currency_code`.
  The CLI's `moneybin accounts set --currency` flag is unaffected.
- **`sql_query` (and `moneybin sql query`) can now read the `reports`
  schema in addition to `core`/`app`.** Report columns are masked by each
  report's declared privacy classes, same as the typed tools — account and
  routing numbers stay masked (`****<last4>`). (#330)
- **A confirmation refusal can now report `reason="unreadable_date"` (#372).**
  It narrows `unknown_layout` to the one cause a column correction cannot
  answer: a mapped date column whose values nothing could parse. The CLI, the
  inbox sidecar, and the MCP envelope use it to prescribe `--date-format`
  instead of an accept/override retry that returns to the same gate. A file
  with no date column mapped keeps reporting `unknown_layout`, because there a
  mapping override is the real recovery. Agents branching on `reason` should
  handle the new value.
- **A malformed PDF `bridge_response` now reports `infra_invalid_input`
  instead of `import_bridge_response_invalid` (#372).** The bridge-specific
  code is retired along with the branch that raised it; the failure is an
  input-validation error like any other. Callers matching the old code need
  updating. Three further codes are retired with the branches that raised
  them: `import_confirm_channel_conflict`, `import_confirm_requires_signal`,
  and `import_file_changed_during_confirmation`.
- **`import_confirm` now declares a maximum sensitivity of `critical`, up
  from `medium` (#372).** Its refusal envelope can carry
  `account_proposals[].source_account_key`, which is an account identifier,
  so the tool's declared ceiling has to admit the critical tier for the
  masking middleware to apply. Hosts that gate tools on declared sensitivity
  will see `import_confirm` move band.

### Removed
- **`confidence` no longer appears in the account-link review payloads.** The
  `Conf` column is gone from `accounts links pending` and `accounts links
  history`, and the `confidence` field is gone from their `--output json`
  envelopes and from `reviews(kind="account_links")`. It reported a constant
  chosen by which signal fired, so anything reading it was reading the signal
  name in a less legible form — `signal` still carries that, and
  `overlap_matched` / `overlap_comparable` now carry the measurement it looked
  like it was making. The `app.account_link_decisions.confidence_score` column
  is unchanged: it is what was recorded when the proposal was written, and the
  audit trail keeps it (#387).

### Fixed
- **Transforming fresh data no longer repeats a full rebuild when the initial
  SQLMesh plan already scheduled every FULL model.** A view-only model change
  still triggers that rebuild when new raw data landed, so refreshed reports do
  not miss the new rows. (#483)

- **A Google Sheet with a repeated header connects instead of being refused.**
  The connector rejected any sheet whose header row repeated a name, telling you
  to rename a column first — a precondition for reading data MoneyBin only ever
  reads. Repeats are renamed to `name`, `name_duplicated_0`, … now, the same
  naming polars already applies to the equivalent CSV, and the rename is
  reported on connect, on reconnect, and in both CLI and MCP output. What the
  rename costs depends on the adapter and the note says which: a transactions
  mapping reads only the headers it matched, so the renamed copy is not
  imported; the seed adapter stores every column. A header that gains a twin
  *after* connect is a different case and stops the pull as drift — the pinned
  mapping would keep importing the first column and silently drop the second's
  amounts (#488).

- **A non-finite amount no longer crashes a report, or prices as though it were
  a number.** A `NaN` in a money column left `format_money` through
  `amount < 0` as a raw `decimal.InvalidOperation` traceback rather than a
  clean CLI error, because ordering a `Decimal("NaN")` raises instead of
  returning false; an infinity did not raise at all and printed as the signed,
  coloured word `Infinity`. The same value in the currency converter was
  quieter and worse — `NaN` times a rate is `NaN`, so it converted without
  complaint and reached the reader labelled in the display currency, a
  conversion that never happened. Both `_as_decimal` helpers now refuse a
  non-finite value the way they already refuse unparseable text: the renderer
  prints it absent, and the converter segments and says why. Reachable from any
  report whose money column is backed by a float computation, including the
  out-of-repo `@report` extensions `docs/specs/extension-contracts.md`
  addresses (#470).

- **`transactions list` no longer clips a long description.** The command cut
  the description to 49 characters and appended an ellipsis before the value
  reached the renderer, so it fired on a wide terminal too — and a raw bank
  description carries the detail that separates two similar charges at the
  end. It folds now, like every other value this renderer prints (#470).

- **A withheld amount no longer prints as an absent one.** A money column
  carrying a whole-masking privacy class (`ROUTING_NUMBER`,
  `COMPOSITE_IDENTIFIER`, `UNRESOLVED`) reaches the renderer already replaced
  by its `*****` sentinel. `format_money` read that as unparseable and printed
  `-`, so the text table contradicted both its own masking and the
  `--output json` result for the same query, and a reader could not tell a
  withheld amount from a SQL NULL. Text in a money cell now prints itself —
  matched by shape rather than against the sentinels in use today, so a new
  mask cannot quietly start reading as absent. Non-numeric text in a money
  column that was never a mask now shows through for the same reason, instead
  of being reported as no data (#470).

- **A `delta` money column with no `polarity` is rejected where it is
  declared.** `OutputColumn` accepted the pair and only the text renderer
  refused it — inside `money_columns`, which the generated CLI command reaches
  after the report has already run and written its audit-log and metrics side
  effects, and which a JSON or MCP caller never reaches at all. The same broken
  declaration was therefore loud on one surface and silent on the others. It
  now raises at construction, so an unrenderable report is unbuildable
  everywhere at once. No in-repo report was affected; this closes the gap for
  the out-of-repo authors `docs/specs/extension-contracts.md` addresses (#470).

  The same guard now checks the declared values themselves. `money_kind` and
  `polarity` are `Literal` types, which bind a type checker and nothing at
  runtime, so an author running none got no signal from either wrong value —
  and neither one fails loudly by itself. An unrecognized kind falls through
  the renderer to an unsigned, uncoloured amount that reads as a deliberate
  balance, and every polarity that is not `income` colours as `expense`, so
  `polarity="up"` inverts a delta's colours rather than raising. A polarity on
  any kind but `delta` is refused too, because nothing reads one there:
  accepting it silently tells an author their column is polarized when the
  rendered output will not be (#470).

- **`-q/--quiet` now works on the report commands.** `reports networth`,
  `networth-history`, `reports run`, and every generated built-in report
  command accepted the flag and then dropped it, so their next-step hints
  ("run `moneybin reports explain …`") printed regardless. Each forwards it
  now. What `-q` still does not silence is any statement about how far the
  numbers can be trusted — a truncated result, a degraded report, or a
  currency conversion — because asking for less chatter is not a claim that
  the truncation stopped (#470).

- **`transactions categorize pending` formats its amounts like every other
  table.** Its `amount` column printed raw (`-42.5`, left-aligned, no
  separator) while the rest of the CLI moved to `−42.50` (#470).

- **Google Sheets authorization fails up front instead of after a consent
  screen that could never complete.** `gsheet auth` sent an empty client
  secret in the code→token exchange, so Google returned an authorization code
  and then rejected the exchange 63ms later: the browser looked like it had
  worked, no token was stored, and the error said only "See application logs
  for detail." Google's Desktop clients require the secret even under PKCE —
  only Android, iOS and Chrome clients are exempt, because they bind to a
  signing certificate instead. MoneyBin ships the client ID but no secret, so
  `MONEYBIN_GSHEET__OAUTH_CLIENT_SECRET` is now required alongside
  `MONEYBIN_GSHEET__OAUTH_CLIENT_ID`, and both the authorization and refresh
  grants refuse by name when either is missing rather than failing somewhere
  less legible. `docs/guides/connect-gsheet.md` covers bringing your own client.
  The refresh grant carried the same defect and would have failed about an hour
  after an authorization that looked healthy. Setting only the secret is
  refused too, because it pairs your secret with MoneyBin's embedded client ID,
  which Google never issued it for. And
  `gsheet auth` no longer reports an existing connection as authorized when
  either variable is missing: it re-authorizes and names the gap instead of
  succeeding now and failing at the next refresh. (#456)
- **An import now announces a new account under the name you will find it by.**
  A first-contact import mints the account without asking and reports what it
  created, and the MCP tool tells the agent to relay that to the user — but the
  reported name was built separately from the one MoneyBin stores. OFX files
  were announced by their raw `<ORG>` routing code and raw type spelling while
  the account list showed a resolved institution, a normalized type and a last
  four; two accounts at one institution each collapsed onto one string, so the
  label could not tell apart the accounts it described.
  `accounts_created[].display_name` — on the CLI, in `import_files` per-file
  rows, and in `import_confirm` — is now the same resolved label
  `moneybin accounts` shows, on every channel (#446).

  Putting those two readers on one string meant trimming a setting on the way
  in, and MoneyBin now judges a value *after* that trim rather than before.
  `accounts set --subtype " checking "` used to be refused outright in a
  non-interactive run, and the MCP tool used to store the canonical `checking`
  while warning in the same breath that it had never heard of `  checking  `.
  Separately, an account whose settings row still held a blank written before
  that trim existed could no longer be read or changed at all: every settings
  mutator loads the row first, and the row now failed the length check it had
  passed when it was written. A stored blank loads as the absent value it
  always meant (#446).

  The name a person wrote is masked before it can become a display name, and
  two gaps let that mask publish more of an account number than the four digits
  every masked surface already shows. A label carrying *two* long identifiers
  masked both and then kept both — eight digits, drawn from two distinct
  numbers — because the guard asked only whether a digit had survived outside a
  mask, never how many masks there were. Separately, a four-digit run written
  in a non-Latin script was invisible to the "this label already shows four
  digits" test, so MoneyBin appended its own last four beside one and published
  eight again. Both are closed, the second across all three encodings of the
  name ladder: the SQL model, its Python mirror, and the raw fallback that
  answers before the first refresh (#446).

- **`sql_query` no longer refuses a read-only `SELECT` for a write keyword
  that isn't actually a write.** `SELECT 'export' AS probe` was rejected as
  though it were a real `EXPORT` statement — one character away,
  `SELECT 'expor' AS control` was always accepted, because the write-operation
  guard scanned raw query text with no regard for where the word appeared. It
  now checks the parsed SQL's structure instead of its text: a word matters
  only when it produces an actual write statement (`INSERT`, `UPDATE`,
  `DROP`, etc.), never when it merely appears inside a string literal (any
  quoting style), a quoted identifier, or a `--` comment. This unblocks
  `... WHERE action LIKE 'export%'` against `app.audit_log`, the documented
  way to find an export's audit trail from the agent-safe SQL surface (#447).

- **Account merge proposals key on the last four, not on the institution.** The
  proposer both missed real cross-source duplicates and filed proposals for
  unrelated pairs, and each failure had its own cause.

  The last-four rung required the source to carry a resolved institution. A
  tabular export names its account only inside a label — `Everyday Spending (...)`
  — and no institution is parsed from it, so the account it minted had an exact
  last four and no institution, invisible to every last-four comparison. The
  OFX copy of the same account minted separately, both counted toward spending
  and net worth, and nothing proposed the merge. Institution is now evidence
  rather than a precondition: it still vetoes a pair that states two different
  banks, but a pair where either side names none surfaces under a new
  `last_four` signal, kept distinct from `institution_last4` so the queue can
  tell "both sides named this bank" from "one side named nothing".

  Separately, `institution_reissue` fired on a shared institution plus a last
  four that differed — which, in an established book, is every pair of cards at
  one bank. It never checked that the two ledgers were sequential, which is what
  a reissue means, so it proposed pairs that ran side by side for months, each
  carrying its own refutation in zero matched transactions over the period they
  shared. A proposal is now dropped only when that whole refutation holds: the
  two ledgers ran at once for longer than a statement cycle **and** shared no
  transaction over a period both of them covered. Requiring the second half
  matters most for the duplicate this queue exists to catch — one account
  arriving from two sources overlaps in dates by construction and matches on
  every row, so dropping on the dates alone would have silently withheld the
  pair and left it double-counting. Only positive concurrency drops it: an
  account with no published ledger, or no comparable period to measure, keeps
  its proposal, which is the import-time state the signal was written for. An
  unstated currency counts as silence here for the same reason: the overlap
  probe normally reads a one-sided blank as a mismatch, which is affordable
  where the count is only shown beside a proposal and inverts where it
  suppresses one — a tabular export leaving the column empty beside a feed
  that states USD would otherwise score zero against a ledger it agrees with
  row for row, and the drop would read that as disagreement. Two stated and
  differing currencies still refute.

  Ledger overlap now also states the posting-lag tolerance it matched within, on
  every surface that reports it. "345 of 346" otherwise reads as exact-date
  agreement, a stronger claim than the probe makes and a different basis for
  ratifying an irreversible merge. (#450)

- **A denied keychain read is no longer reported as a missing key.** macOS
  reports a sandbox-denied keychain read identically to a genuinely absent
  item (`errSecItemNotFound`), so three call sites each guessed differently:
  `db shell`/`db query` said the database was locked, opening the database
  said the encryption key was missing and to run `db init` — even against a
  database that already existed, contradicting its own hint one line below —
  and `db unlock` asked whether `--passphrase` mode was ever used.
  `SecretStore.get_key()` now raises a distinct `SecretUnavailableError` when
  the keychain backend itself can tell a denied read apart from a routine
  miss (e.g. a locked Linux secret service); every call site routes through
  one existence-aware hint instead: `db init` only when no database file
  exists yet, otherwise `db unlock` or the `MONEYBIN_DATABASE__ENCRYPTION_KEY`
  env var — which needs no further keychain access, exactly what may be
  unavailable (#419, PR #453).

- **Google Sheets ships its own OAuth client ID.** The connector shipped
  without an OAuth client of its own, so `moneybin gsheet auth` on a fresh
  install refused outright — "Google Sheets OAuth client ID is not configured"
  — until you registered your own Google Cloud project. MoneyBin now ships its
  own public client ID: a native app cannot keep a shipped credential
  confidential, so the security rests on PKCE and a loopback redirect rather
  than on the identifier being hidden. Point
  `MONEYBIN_GSHEET__OAUTH_CLIENT_ID` at your own project to use its API quota
  instead, or set it empty to disable the connector. This does not yet remove
  the Google Cloud Console step — Google's Desktop clients also require a
  client secret, which MoneyBin does not ship; see the #456 entry above.
  (#452)
- **An account with nothing to identify it now reads `Unnamed account`, not
  `Account <id>`.** `core.dim_accounts.display_name` ended its fallback chain
  with the account's grain key, which for an account imported before the
  identity resolver is the institution's own account number — so a column
  declared to hold user notes could surface one, including through the
  `reports.*` models that project it as `account_name`. The id is dropped from
  that fallback entirely rather than shown only where it is safe, because the
  staging layer resolves the key before `dim_accounts` sees it and the model
  cannot tell the two cases apart. `accounts links pending`, `accounts links
  history` and the MCP `reviews` summaries render the same phrase for an
  account whose name is absent, so one state no longer reads two ways in one
  table. An account that has only a last four is now named by it — `…4521`, or
  `checking …4521` — rather than falling to the placeholder, so accounts that
  differ still read differently in account lists and reports. Because that
  placeholder is one shared string, matching on it is refused everywhere a name
  identifies an account: it no longer proposes a merge between two unnameable
  accounts, `accounts` resolve no longer returns them at full confidence to
  anyone who types it back, and neither the strict resolver behind `--account`
  nor the account-reference matching in the `accounts`, `transactions` and
  `investments` tools binds it to whichever account happens to wear it — which
  had let a categorization, an investment write, a balance assertion or a sheet
  connection land on an account the caller never chose. Such an account offers
  no name to match rather than offering its id: an account with no resolver
  link carries the institution's own account number as its id, and the
  not-found error lists every candidate's name into a message the CLI writes to
  its durable log. It is still addressable by that id, which `moneybin accounts
  list` now prints beside the placeholder — the listing had been the one place
  the id was legible, and `accounts set` takes nothing else. Setting that
  placeholder as an account's *own* name is now refused, which nothing had
  stopped: once set, an account MoneyBin could perfectly well name dropped out
  of the same lookups the generated placeholder is filtered from, silently. The
  refusal folds the name with the same `normalize_reference` the resolver's
  third rung matches on, so a case variant, padding, a doubled space or an
  NFKC-equivalent character is refused too: anything that fold collapses onto
  the label would otherwise answer a request for the label *another* account
  displays, and with generated placeholders filtered out of the candidate-name
  slot such a row is the unique hit. Reserving on a narrower fold than the
  matcher uses would leave exactly that difference as a hole. The check sits on
  the write path, not in `AccountSettings.__post_init__`, so a row that already
  carries the label still loads rather than raising on read (#435).

- **Pinning an import with `--account-id` no longer renames the account the
  file describes** — the raw row keeps the source's own key on every path, a
  pin reuses the key its account already answers to so a statement your bank
  regenerated is not imported twice, and pinning a statement already bound to
  another account now errors. If a file was imported under the old scheme,
  delete its previous import batch before re-importing it or it will be counted
  twice ([`account-identifiers.md`](docs/reference/account-identifiers.md),
  #438, #418).

- **Account-merge prompts and the decision log now name the accounts instead of
  showing their ids.** Each side leads with its name and shows the masked last
  four as evidence; where MoneyBin holds nothing to tell two accounts apart the
  prompt says so, and a `core.dim_accounts` label that is only
  `Account <account_id>` is refused, showing the masked last four in its place.
  Both names are frozen onto the decision when it is made (migration V051) so
  `accounts links history` can still name an accepted merge, though existing
  rows are not backfilled (#417).

- **Syncing an institution that has removed transactions no longer aborts the
  whole pull.** `moneybin-sync` widened `removed_transactions` from a bare
  transaction id to a full provider-native record while the client still
  declared a list of strings, so response validation failed and every
  institution with removals synced nothing. Only institutions that happened to
  have none kept working, which is what made the break easy to miss. The client
  now reads the id out of the record and still accepts a bare string, so it
  validates against a server on either side of the change (#412).

- **An account merge can no longer be confirmed by the agent asking for it.**
  `identity_links_decide` and `accounts_links_set` gated a merge behind a
  confirmation, but a caller could hand the opaque `confirmation_token` from the
  first call's refusal straight back on the second and merge without the prompt
  reaching a person. An accepted account link now takes the prompt or nothing —
  a supplied token is refused, and a client that cannot prompt is pointed at
  `moneybin accounts links set`; merchant- and security-link accepts keep the
  token path, since neither re-keys a transaction (#414).

- **`MONEYBIN_HOME` in a `.env` file no longer fails silently.** That file is
  looked up inside `<base>` — the very directory the setting would name — so the
  home is already resolved by the time it is read, and pydantic-settings dropped
  the key without a word. MoneyBin now refuses to start and names the two routes
  that work (`--home`, or a real environment variable) rather than quietly using
  a different database than the one you wrote down. `export KEY=value` form is
  caught too.

- **A worktree no longer gets its own empty database.** `get_base_dir()` treated
  any directory with a `.git` and a moneybin `pyproject.toml` as its own data
  home, and a linked git worktree satisfies both — so `moneybin` commands run
  from one silently read an empty database and reported zeros, indistinguishable
  from a clean result. A worktree now resolves the main checkout's `.moneybin`.
  An explicit `MONEYBIN_HOME` still outranks it, a submodule still resolves to
  itself, and outside a checkout nothing changes: `~/.moneybin` as before.

- **`sqlmesh` no longer scatters profile state to the repo root.** The startup
  anchor set `MONEYBIN_HOME` to `<repo-root>` where every other branch of
  `get_base_dir()` returns `<repo-root>/.moneybin`, so a bare `sqlmesh`
  invocation wrote profiles to `<repo-root>/profiles/` — the root-level twin of
  the `src/moneybin/profiles/` scatter already recorded in `.gitignore`. It now
  anchors at the data dir, and resolves a linked worktree to the main checkout
  so it cannot re-break the fix above through a second channel. `/profiles/` is
  gitignored for the residue.

- **`make claude-mcp` no longer switches off your other MCP servers.** The
  launcher passed `--strict-mcp-config`, which tells Claude Code to ignore
  every other configured MCP server for that session — so opting in to
  MoneyBin silently cost you Linear, Playwright, and any project `.mcp.json`
  for as long as the session lived. It now passes `--mcp-config` alone.
  Nothing about the opt-in changes: MoneyBin's config still lives in the
  profile directory rather than Claude Code's own config sources, so a plain
  `claude` in the repo still doesn't load MoneyBin and still doesn't take the
  database lock. A test now asserts the launcher script and the launch hint
  `mcp install --print` prints carry the same flags, so the two hand-maintained
  copies of that command line can't drift apart again.

- **`make claude-mcp` with no `PROFILE=` now uses the active profile, as
  documented.** `mcp config path --client claude-code` read only the profile
  set in-process, which nothing sets on that path unless `--profile` or
  `MONEYBIN_PROFILE` names one — so the bare form always failed with "No
  active profile and --profile not supplied", even with a profile recorded in
  `<base>/config.yaml`. It now falls back to that recorded profile, matching
  what `profile show` and `profile switch` already do. The error still fires,
  non-interactively, when no profile is active anywhere.

- **A synthetic account's opening balance now means the same thing on both
  import paths, so its reported balance is no longer short by its first day.**
  The generator's OFX writer stamped the opening balance on the first
  transaction date, and `core.fct_balances_daily` treats an observed balance as
  final for its day — so day one's transactions were never added to the series,
  and every OFX account in every persona reported a balance short by its day-one
  net for the whole run. The tabular writer had always treated the same YAML
  field as the balance before any activity. The anchor now sits on the day before
  the first transaction, which is what `opening_balance` says. Demo net worth for
  `basic` moves from 212913.05 to 211413.05 at the same date; transaction
  counts, categories, and every real-import path are untouched.
- **Accepting an account-link merge now re-runs the matcher, so the duplicates it
  makes visible actually get found.** The transaction matcher blocks candidate
  pairs on `account_id`, so while a reissued card's two sources sit under
  separate accounts it cannot pair their duplicates — it does not decline them,
  it never sees them. Accepting the link is what makes them comparable, but
  `CANONICAL_STEPS` runs `match` three stages before `identity`, so no refresh
  ever observed its own accepts: the accept repointed the links and stopped. One
  card carrying OFX and CSV history for the same period held 377 duplicated rows
  with zero proposals raised, and needed hand-remediation to recover.

  Both accept paths now trigger the pass — the direct
  `accounts links set` / `identity_links_decide` route, and the batched review
  route, whose inner calls join the outer transaction and return before their own
  post-commit tail. Because that pass can auto-merge rows without asking, it
  reports what it did on both: the CLI prints how many were auto-merged and how
  many are newly queued, and `accounts_links_set` and `identity_links_decide`
  both return `rematch_auto_merged` / `rematch_pending_review`. All are null
  after a reject, which runs no pass at all — distinct from a pass that ran and
  found nothing. `--yes` waives the confirmation prompt, never the report. A
  half-failed pass says so: matching and the rebuild fail independently, and
  either alone still leaves the merge unfinished in a way the user would
  otherwise have to discover for themselves.

  Telemetry does not decide whether that pass runs. Both accept paths refresh
  the review-queue gauge in the post-commit tail, immediately ahead of the
  rematch. A metrics failure there aborted the tail with the accept already
  committed, and an accepted decision is refused on a retry — so the merged
  account's duplicates would have waited for an unrelated refresh with nothing
  reporting it. That refresh is now best-effort and logs what it lost; a stale
  gauge is the cheaper loss. The retirement counter is best-effort for the same
  reason and by the same mechanism: every one of its three emissions stands
  after the reversal it counts is already durable, so a raise there reports
  failure for committed work a retry will not replay.

  The match pass also retires transfers a dedup collapse invalidates.
  Deduplication is blocked on `a.account_id = b.account_id`, so two rows each
  claimed as a transfer leg by a *different* account can never be dedup
  candidates — until a merge makes those accounts one, and neither dedup tier
  declines a row on the grounds that a transfer already claims it.
  `core.bridge_transfers` resolves every leg through the dedup mapping, so two
  surviving decisions would name the same physical transaction and double-count
  it in anything joining `fct_transactions` to `bridge_transfers`. Tier 4
  already refuses to *propose* that shape; the matcher now enforces the same
  rule against decisions that predate the collapse — a dedup component is a leg
  of at most one accepted transfer, earliest decision keeps it. Only accepted
  dedup decisions count toward a component: a pending one is an unreviewed
  proposal that leaves both rows distinct in `core`, so it never retires a
  transfer. Reversed, not deleted, and reported as `rematch_transfers_retired`
  on both tools plus a CLI warning naming `moneybin system audit undo`, because the
  user accepted those transfers. That counter also covers the account-level
  form of the same collapse — a transfer whose two endpoints became one
  account, retired during the re-key — which previously reversed an accepted
  transfer while every surface reported `0`.

  The reconciliation is not merge-only. Every path that folds a duplicate runs
  it: the matcher (so any `refresh` covers it), a review-queue accept
  (`moneybin review --confirm`, `transactions_matches_set`), and a bulk
  `moneybin review --confirm-all`. `core.fct_transactions` and
  `core.bridge_transfers` are views over `app.match_decisions`, so a collision
  double-counts on the next read rather than waiting for a refresh — which is
  why the two accept paths, which re-derive nothing, each fold the reversals
  into the transaction that accepted the duplicate. Inside the matcher it runs
  between the dedup tiers and transfer detection, so a leg the reversal frees is
  re-examined as a transfer candidate by the same pass and the reversal lands
  before `transform`, leaving the corrupt bridge never built rather than rebuilt
  correctly one refresh later.

  A `refresh` covering the reconciliation is not the same as the surfaces that
  *call* `refresh` reporting it. Six embedded callers reach it and previously
  copied only the three transform fields off its result: `moneybin import
  files` on both its batch and its single-file path, `moneybin sync pull` (and
  `sync link`'s auto-pull), the inbox drain, the single-file MCP import path,
  and `moneybin gsheet pull` — the last by naming `match` explicitly rather
  than running the full cascade. Each could therefore reverse an accepted
  transfer and report nothing but a successful import. All six now carry
  `transfers_retired` back — the CLI through the same warning naming `moneybin
  system audit undo`, the MCP twins (`import_files`, `sync_pull`,
  `import_inbox_sync`) through the payload and an `actions[]` hint pointing at
  `system_audit_undo()`. Every CLI surface prints that warning even under
  `--quiet`, which suppresses informational output and not a reversal of the
  user's own decision.

  The single-file import's failure path reports it too. That path is fail-loud:
  the refresh reconciles inside its `match` step and commits there, so a
  transform apply that dies afterwards leaves the reversal on disk while the
  raise discards the result that would have named it. The exception now carries
  the count, so `moneybin import files <one-file>` names the reversal and still
  exits non-zero for the transform.

  Two smaller gaps in the same disclosure closed with them. The
  `unproposed_cross_source_duplicates` invariant's crash branch put DuckDB's
  raw exception text in a `detail` that `doctor` and `system_status` return
  over both surfaces — and that query joins on amounts, dates, and
  descriptions, so a conversion failure could echo a row back; it now logs the
  frame chain locally and returns a fixed string, matching every other crash
  branch here. And `identity_links_decide`'s tool description, the only prose
  an agent reads when choosing it, never mentioned that accepting an account
  decision re-runs matching; its sibling `reviews_decide` already did.

  Because that pass walks every accepted transfer — including the row the accept
  just wrote — **an accept can be the decision it reverses**, and the surfaces
  now report the committed status instead of the requested one.
  `transactions_matches_set` returns the re-read value in `data.match_status`;
  `moneybin transactions matches set` and `moneybin review --confirm` print a
  refusal naming the standing decision rather than a success mark.
  `moneybin review --confirm-all` counts only the rows that stood and names the
  rest separately, since a batch can hold both a duplicate and the transfer that
  duplicate invalidates. A crashed match step no longer drops the count either:
  the matcher wraps no transaction around a run, so reversals already committed
  are reported even when the reconciliation or a later tier raises. The recovery
  hint follows the same status: `transactions_matches_set` offered
  `transactions matches undo` unconditionally, and undo refuses a row that is
  already reversed — so the one outcome the reconciliation had just produced was
  handed the one command certain to fail. It is now offered only for a decision
  that stood, and a reversed one gets a route that works.

  `refresh_run` and `moneybin refresh` now disclose what the match step decided
  — `matches_auto_merged`, `matches_pending_review`,
  `matches_pending_transfers`, `matching_skipped`, and `transfers_retired` —
  and the CLI warns when a transfer was retired. Any refresh reaches the match
  step, including the one every import and sync triggers, so a pass could
  previously auto-merge duplicates or reverse an accepted transfer and report an
  ordinary success. `matching_skipped` separates a zero that means "found
  nothing" from one that means "never looked".

  Every surface that can reach the reconciliation now discloses it, not just
  the ones that report matches. `moneybin transactions matches run`,
  `moneybin transactions matches backfill`, and the `transactions_matches_run`
  tool each ran the pass and reported only what the tiers found, so a run that
  reversed an accepted transfer while finding nothing printed "No new matches
  found". The two counts are independent: the reconciliation fires whatever the
  tiers return.

  That disclosure no longer depends on the output mode. `moneybin sync pull`
  and `moneybin import inbox` placed the warning inside their text branch, so
  `--output json` — the mode an unattended caller actually uses — dropped the
  sentence naming `system audit undo` while still carrying the raw count. Both
  now warn ahead of either branch, as `moneybin refresh` and `gsheet pull`
  already did. `refresh_run`'s registered description had the same shape of
  gap: the prose an agent reads at tool-selection time still said "No revert
  path" while the tool could reverse a transfer, and `import_files`,
  `import_inbox_sync`, and `sync_pull` never mentioned the reversal at all.

  The retirement notices no longer claim the triggering action caused the
  invalidation. The pass walks every accepted transfer, so a count can include
  one an earlier decision had already broken and this run merely found —
  wording that said "this decision invalidated" asserted a cause the number
  does not carry. The notices now report what was reversed and leave the cause
  to the audit log.

  A retirement now survives the crash that follows it. The reconciliation
  commits its reversals as it goes, so a later transfer-tier failure leaves them
  on disk — which is why the error carries the count. Only `refresh` read it:
  `transactions_matches_run`, `moneybin transactions matches run`, and `matches
  backfill` let the exception through, and the sole record that a decision the
  user made had been undone died with it. All three now report the count and the
  way back before failing. `refresh_run` had the opposite half of the same gap —
  it reported the count with no action beside it — so the surface most users
  reach the reconciliation through named the reversal without naming the
  restore. Both halves now match what the accept path already did.

  That guard started one step too low. It opened after the two dedup tiers, but
  the tiers are the run's *first* durable writes: each persists one decision per
  pair with no transaction around the loop, so a pair that raises leaves every
  earlier merge on disk — suppressing the duplicate side of those transactions —
  while the caller was told only that matching failed. A late `CatalogException`
  from a tier was worse than silent: `refresh` read it as the first-load "views
  not built yet" precondition and reported a *skipped* step, claiming nothing had
  been examined after decisions were already written. The guard now spans every
  step that writes, and the error carries the whole partial result rather than
  the retirement count alone, so all four surfaces report the merges as well as
  the reversals. A run that committed nothing is still left unwrapped, which is
  what keeps a genuine first load quiet.

  Four smaller corrections to the same disclosure. `transfers_retired` counted
  the row the *caller* had just accepted whenever the reconciliation reversed it,
  so an accept that refused itself reported a standing transfer as undone and
  pointed at an undo that only returns the proposal to `pending`; both the single
  and bulk accept paths now discount their own flipped rows, which
  `match_status` and `reversed_by_reconciliation` already report. Those accept
  paths reconcile and return without a transfer-detection pass, so a leg the
  reversal freed stayed unproposed until an unrelated refresh — they now say so
  and name the pass. The MCP matcher error interpolated the raw cause, sending
  DuckDB binder text and file paths through the tool boundary; the cause is
  logged locally and the counts still cross. And retirements now increment
  `moneybin_transfer_retirements_total`, labelled by which collapse caused them —
  the only counter here that measures an undo of something the user decided,
  which the match counts cannot show.

  The post-merge re-match's crash branch names what landed. Carrying the partial
  counts through `MatchRunError` made them real on that path, but both the CLI
  warning and the agent-facing action still said duplicates "may" have been
  merged — spending an exact number on a hedge, about the one outcome that
  changes the ledger without being asked. Both now name the committed merges and
  proposals, and say plainly when nothing had committed, which the carrier makes
  trustworthy.

  `doctor`'s unproposed-duplicates finding now reports its pair count as an upper
  bound. Its component closure reads persisted decisions, while the matcher also
  links candidates as it walks them, so three mutually duplicate rows with no
  prior decision count as three pairs and become two proposals — the remedy the
  finding recommends under-delivered against its own number.

  The batch review path reconciles too. `reviews_decide` accepts match rows
  through `ReviewDecisionsService.apply_ordinary`, which wrote them straight to
  the repo — so an agent folding a queued duplicate that way left two accepted
  transfers resolving to one gold transaction, the exact corruption the single
  and bulk accepts were fixed for. The batch now runs the same reconciliation
  once after its writes, reports `transfers_retired` in `data` beside an
  `actions[]` route back, and re-reads each decision's committed status: an
  accept that loses the reconciliation's tiebreak comes back `reversed` instead
  of claiming it stands.

  A crashed matcher no longer reaches the terminal as a traceback.
  `MatchRunError`'s own message is `str(cause)` — DuckDB binder text, file
  paths — and it was registered nowhere in the user-error classifier, whose
  contract is that unrecognized exceptions propagate unchanged. `matches run`
  and `matches backfill` therefore printed all of it, the leak the MCP twin was
  hardened against in this same change. Both now exit through the `❌` + code-1
  path with a message MoneyBin wrote; the frame chain, not the message, goes to
  the log.

  `refresh` was the third surface holding the same cause, and it held it in a
  returned field rather than a traceback: `matching_error` and
  `categorization_error` were assigned `str(exc)`, and both are declared
  `DESCRIPTION` on `RefreshRunPayload` — so `refresh_run` handed the raw text to
  the model provider and `moneybin refresh --output json` wrote it to stdout.
  All three crash branches now return the classifier's wording, the same
  boundary the matcher commands use, and a type it does not recognize returns a
  generic line naming the step. The counts beside the error are unchanged: they
  were always the disclosable half.

  `transactions_matches_run` withheld the cause from its envelope and then wrote
  it to the log, message and full traceback, one line above. Nine other failure
  logs on the matcher path record the frame chain instead — a traceback's last
  line is `<Type>: <str(exc)>`, so `exc_info` re-admits exactly what the
  envelope refused. It now matches them.

  The re-match a merge triggers now audits as the surface that triggered it.
  `refresh` ran the match step with no actor, so decisions written because a
  user accepted a link recorded `system` — the value
  `app-integrity-invariant.md` reserves for automated callers. `moneybin
  refresh`, `refresh_run`, and the scenario runner still record `system`; the
  post-merge pass records `cli` or `mcp`, so audit history stops attributing a
  user's merge to the pipeline.
- **An accepted merge no longer strands the match decisions made under the old
  account, which could silently reverse a rejection.** Accepting a link
  re-points `app.account_links`, but a row in `app.match_decisions` stores the
  `account_id` it was decided under — and that column is what the matcher keys
  its rejected-pair tuple and its active-edge node on. Left behind, the row
  stopped describing any live pair. For a rejection that is the worst case: it
  no longer matched itself, so the next match pass treated the pair as new and,
  above the confidence threshold with agreeing descriptions, auto-accepted the
  two transactions the user had explicitly said were not duplicates — with
  nothing to show it happened. The merge now re-keys both account columns onto
  the surviving account in the same transaction, one audit row apiece so an
  undo can replay them individually. Reachable before through any refresh
  following a merge; the post-merge re-match above would have made it
  deterministic.
- **`moneybin system doctor` can now see a duplicate nobody proposed.** Neither existing
  invariant could. `dedup_reconciliation` asserts
  `raw_total - core_count == dedup_absorbed`, which balances whether or not a
  duplicate was ever *proposed* — a pair nobody looked at moves both sides of the
  equation together, so it stayed green across all 377 rows. `duplicate_account_overlap`
  saw the split while it was still two accounts and stopped applying the instant
  the link was accepted, which is precisely when the rows became matchable. The
  new `unproposed_cross_source_duplicates` warns when a pair the matcher's own
  Tier 3 blocking test would admit — differing `source_type` **or** differing
  `source_origin`, so two CSV integrations and two Plaid connections count —
  matches on amount and date within the matcher's window and no live match
  decision explains the silence. "Explains" mirrors each of the matcher's own
  reasons for dropping a candidate: the two rows are already in one component
  (the transitive closure of accepted and pending dedup edges), their components
  already share a `(source_type, source_origin, source_file)` — the cardinality
  guard that keeps two rows of one import file apart — or the exact pair was
  rejected. Accepted and pending decisions read at component grain, rejections at
  pair grain, and each keyed exactly as the matcher keys it — components on
  `NodeKey` and rejections on the pair tuple `scoring.py` actually tests, neither
  of which carries `source_origin`. `get_rejected_pairs` selects origin, but the
  matcher discards it when building its rejected set, so requiring it here would
  warn about a pair no refresh can clear. Scoping both by `account_id` is what
  keeps a source-native id reused by an unrelated account from marking a row
  "already decided."
- **Reconnecting a bank no longer splits one account's ledger in two.** Plaid
  reissues every `account_id` when an institution is relinked, so an account that
  came back under a new id read as one MoneyBin had never seen: it minted
  a second canonical account beside the first, and the history divided between
  them. Plaid also sends `persistent_account_id`, which survives a relink and
  exists to answer exactly this question — but MoneyBin's wire model never
  declared the field, and an undeclared field is discarded during validation
  without an error or a log line, so every pull threw it away.
  MoneyBin now stores it on `raw.plaid_accounts` and hands it to the
  account resolver as the strong reference that reunites a returning account with
  its own ledger. Plaid populates it for depository accounts at the three
  institutions using tokenized account numbers — Chase, PNC, and US Bank — so a
  credit card, or an account anywhere else, still resolves through the
  weak-match review as before. Accounts synced before this change hold no value
  for it — nothing retained a copy, so there is nothing to backfill — and pick
  one up on their next sync (#395).
- **Two accounts sharing one product name no longer match on it.**
  MoneyBin named a Plaid account after `official_name`, which is the product's
  marketing label rather than the account's: two different Chase cards both
  read "Ultimate Rewards®" in name matching. The account's own `name` now leads
  that fallback, with `official_name` behind it and the institution name behind
  that. The review queue is unaffected — both sides of a merge proposal are
  already labelled from `core.dim_accounts.display_name`, which is built from
  institution, subtype, and last four (#395).
- **Undoing a decision puts it back in the review queue, and the queue count now
  says so.** `system audit undo` restores a link decision to pending, but the
  counter that reports how many decisions await review was refreshed only by the
  accept and reject paths — so a reversed accept left the queue re-filled and the
  count reading zero. The count is the prompt to go look at the queue, so
  under-reporting it is the one direction nothing else signals.
- **A saved PDF recipe that misreads a masked account number now repairs
  itself (#380).** Recipes saved before the anchor fix in #371 read
  `Account Number: XXXX XXXX XXXX 1234` as the bare mask, producing an account
  with no last four that MoneyBin could not connect to the same card arriving by
  OFX, CSV, or a bank connection — and because those statements still reconcile
  to the cent, nothing flagged it. MoneyBin now treats a digit-free account id as
  a stale-recipe signal and re-derives the saved recipe in place, audited and
  reversible through `system audit undo`.
- **An OFX transaction whose id changed between imports is no longer counted
  twice.** Some institutions stamp two distinct transactions with one `FITID` —
  a foreign purchase and its fee, for instance. MoneyBin gives every member of
  such a collision a content-derived suffix so neither is lost, but a `FITID`
  that arrives alone in one statement and collides in a later one was written
  plain the first time and suffixed the second. The plain row could not be
  overwritten and survived alongside its own replacement: one real transaction,
  two rows, inflating the balance and the spending. It never looked like a
  duplicate id, so no uniqueness check caught it. MoneyBin now recognizes the
  superseded row and drops it during the transform, preventing the next
  occurrence. Nothing is deleted from imported data — a row is only suppressed
  when a replacement carries identical content, and one whose description
  drifted between statements is left for duplicate review rather than silently
  removed. Suppression is keyed on MoneyBin's own record of which ids it
  rewrote, not on spotting the suffix in the id text: the OFX format does not
  reserve `#`, so a bank is free to issue `X` and `X#reference` as two ordinary
  unrelated ids, and reading the second as a replacement for the first would
  delete a real transaction. That record only exists for statements imported
  from this release onward, and it is not guessed retroactively — so if this
  already happened to you, the affected statement still shows both rows until
  you re-import it, which is what tells MoneyBin which id it rewrote.
- **A replacement card no longer lands as a second account with no trace
  (#375).** A reissued card changes its last four digits by definition, so the
  institution+last-four match cannot fire, and on the PDF path the account name
  is the filename, so the name match misses too — the replacement minted a fresh
  account with no confirm and no review entry at all. A same-institution account
  whose last four differs now files a review proposal naming the original card,
  listed by `moneybin accounts links pending`, once the original has been
  through a `moneybin refresh` — every account-matching signal reads the account
  dimension the refresh builds, so importing both cards in one batch files
  nothing. The import still creates the second account and does not stop to ask:
  only CSV/Excel has a pre-load confirmation today, and extending that to OFX
  and PDF is the next change.
- **An OFX file you renamed or moved is no longer re-imported as a new one
  (#375).** Duplicate detection compared the file's *path*, so a second download
  saved as `statement (1).qfx`, or a statement filed out of Downloads before
  importing, read as a brand-new file. Every import batch — OFX, CSV/Excel, and
  PDF — now records a SHA-256 of the file's bytes, and OFX duplicate detection
  identifies a document by those bytes. Saving July's statement over June's
  under the same filename is therefore a new import, not a duplicate: this
  recognizes the same document, not the same name. Batches imported before this
  change carry no digest and keep matching on path alone, because their source
  file may be long gone. CSV/Excel and PDF have no duplicate check at either
  level — `--force` is OFX-only — so re-importing one still creates a second
  batch; row-level dedup, not the import log, is what keeps the totals right
  there.
- **An export you ran earlier is findable afterwards (#374).** `export_run` and
  `moneybin export` returned the receipt — export id, destination, artifact
  name, row counts, and checksums — exactly once and stored none of it, so a
  later turn or session had no way to confirm what an export produced or verify
  it against the file on disk. Sheets destinations, which leave no local
  manifest, had no recovery path at all. Each successful run now records that
  receipt to the audit log under action `export.run`; read it back with
  `system_audit` or `moneybin system audit`. The artifact is named, never
  path-qualified, so the audit log stays free of local filesystem layout — the
  record tells you what a run produced, and its checksums confirm a file on
  disk is that artifact. Report parameters are not recorded in any form; the
  export id and checksums already separate two runs. Recording is best-effort:
  if it fails after a successful publish, the run still succeeds and the
  returned receipt is your only copy. The published artifact itself stays
  permanent and is not undoable.
- **A mapping override that switches to debit/credit columns no longer keeps
  the discarded column's number format (#372).** The detector reads the number
  format from the single `amount` column, so correcting a layout to a
  `debit_amount`/`credit_amount` pair left the format derived from the column
  the correction retired. A US-formatted `Amount` beside European split columns
  parsed `1.234,56` as `1.23456` — wrong by three orders of magnitude, with no
  error — and saved that format for later imports of the same layout. Both the
  MCP preview and the CLI first-contact path now re-read the format from the
  surviving amount columns.
- **An import no longer reports success on a statement MoneyBin could not
  read (#372).** `import_confirm` replayed a staged preview whose column mapping
  scored low — or whose date format was never detected — straight into the
  loader, which parsed zero rows and returned a successful import of nothing.
  `moneybin import confirm <file> --accept` had the same hole on first contact:
  a date column that matched by header but whose values nothing could parse
  scored medium, which `--accept` resolved unconditionally.
  Both cases now return `confirmation_required` carrying the file's own column
  names and sample values, and name the recovery path: `import_preview` with a
  `mapping` override stages a corrected preview to confirm instead. An override
  that switches between a single `amount` column and a `debit`/`credit` pair now
  also retires the detector's sign rule, which would otherwise have rejected
  every row. An override that leaves a header row consumed as data — or a date
  column whose values nothing can parse — holds the confirmation gate open at
  the tier the detector actually scored, instead of reporting high confidence
  beside a preview that says it is not confirmable. When the header row itself
  was a transaction, the preview now says so and points at the source file:
  no column correction recovers a record consumed as column names. A date
  column the detector carries no candidate for is still importable through
  `moneybin import files <file> --confirm --date-format <strptime>`; that
  override is now checked against the column's own values first — whether the
  layout was detected fresh, matched a saved format, or matched a built-in one
  — so a format that cannot read the file is refused rather than loading
  nothing. A refusal that replays a staged preview also reports the score and
  flagged fields the preview showed, instead of re-deriving a clean score that
  named nothing to correct. `moneybin import files`, `moneybin import confirm`,
  and the inbox sidecar now name both real recoveries for that refusal —
  `--mapping transaction_date=<column>` when a status column claimed the date
  alias, `--date-format <strptime>` when the mapped column is right and its
  format simply has no candidate — in place of a bare `--confirm`, which
  returns to the same gate. `moneybin import preview` prints `Date format: not
  detected` rather than dropping the line.
- **A card imported from both a PDF statement and a bank file no longer loads
  twice (#371).** PDF import built its account key as a string and skipped the
  identity resolver every other source uses, so the same card arriving as a PDF
  and as OFX had nothing to be matched against and both halves loaded. PDF now
  resolves through the same ladder, and the resulting link is scoped to the
  issuer rather than the filename, so consecutive statements of one card land on
  one account instead of a fresh account per file.
- **Statements that print the card number in groups keep their last four
  digits (#371).** Metadata capture stopped at the first whitespace-delimited
  token, so `Account Number: XXXX XXXX XXXX 1234` yielded the bare mask `XXXX` —
  an account key carrying no digits at all — and an unmasked
  `1234 5678 9012 3456` yielded `1234`, reporting the *leading* four as the last
  four. The whole grouped number is captured and reduced to its trailing four.
  Masks also normalise, so one card whose statements render `****1234` and
  `xxxx1234` no longer keys two different ways. Institution account tokens that
  are not digit/mask runs (`ACCT-9Z`, `123-ABC-456`) are captured whole rather
  than truncated to a leading digit group.

  **Upgrading — revert before re-importing.** Statements already imported under
  the old key need `moneybin import revert <import_id>` *first*, then a fresh
  import. Do not simply re-import: the corrected key feeds both the
  `raw.tabular_transactions` primary key and the `transaction_id` content hash,
  so the new rows do not collide with the old ones — they land alongside them
  and double the statement. Reverting first is also what clears the stale
  per-file link scope; a re-import on its own leaves the old rows, and the
  transactions they carry, uncanonicalized. `moneybin import history` lists the
  import IDs.
- **Picking tax lots no longer reports success on a write nothing will read.**
  `moneybin investments lots select` and `investments_lots_select` accepted a
  lot selection for any security, but the cost-basis engine reads
  `app.lot_selections` only under specific identification — so a selection made
  against a FIFO, HIFO, or average-cost position was saved, echoed back, and
  then discarded at the next refresh, leaving realized gains unchanged with no
  indication why. Both surfaces now refuse a non-empty selection unless the
  security resolves to `specific`, naming the election that would fix it
  (`investments securities set --method specific`). Clearing a selection still
  works under any method, so overrides made while a security was `specific`
  stay removable. The security → account-default → FIFO election chain moved
  into one shared resolver, so the method a selection is checked against is the
  method the disposal replays under.
- **A query that measured a masked column answered "This is a MoneyBin bug".**
  `sql_query` and a saved report both classify an expression by the column it
  reads, so `SELECT length(last_four) …` kept the account-number class and reached
  a mask that measures its input — `len()` on an integer, raising from inside
  redaction. Nothing leaked, because the failure happened before any row was
  returned, but the query could not be answered on any surface and a report saved
  that way was creatable and permanently unrunnable. Values that are not text now
  mask whole (`*****`) rather than partially, which is stronger than the mask it
  replaces: there are no last four digits to keep in a value that is not text. A
  redacted export of such a report declares the type the mask produced rather than
  the one it replaced, so its Parquet file and its manifest agree. (#367)
- **"Transforms up to date" now accounts for everything you can add, not just
  accounts.** The staleness flag on `system_status` and `moneybin transform
  status` watched three account tables out of the seventeen a refresh reads, so
  a manually recorded transaction, a manually recorded investment trade, a
  synced holding, and a fetched security price all landed while MoneyBin
  reported nothing to refresh — the reports you then ran were built without
  them. All seventeen are watched, compared against SQLMesh's own record of
  when it last rebuilt each model, and a raw table wired into a model but left
  out of that set now fails the build rather than going unwatched. Rebuilding
  one model on its own — `moneybin transform restate --model` — no longer
  clears the flag for the models it didn't touch. Manual entries also
  close their import batch on success; a batch left open reads as a crashed
  write in `import history` and `import status`, with no completion time and no
  row counts.
- **`--profile` now logs like any other run.** Naming a profile explicitly —
  `moneybin -p work sync pull`, or `MONEYBIN_PROFILE=work` — wrote no log files
  at all: no `cli_*.log`, no `sqlmesh_*.log`. With no log file to hold them,
  the console filter stood down by design rather than destroy records, so
  `sync pull` printed several thousand `Executing SQL: …` and
  `Evaluating snapshot …` lines, including every `CREATE OR REPLACE VIEW` and
  `COMMENT ON COLUMN` body, ahead of the four lines that mattered. Explicitly
  naming a profile now resolves it exactly as switching to it does: log files
  written, profile directory checked, and SQLMesh's output in
  `sqlmesh_YYYY-MM-DD.log` where it belongs. Warnings and errors still reach
  the console.
- **An assistant and a person now get the same truthful, structured answer
  from MoneyBin.** `system_status` and `reviews` degrade section by section
  and queue by queue instead of failing whole, so one broken check no longer
  hides the rest; a timed-out read releases its database connection, which is
  what previously wedged the doctor until the server restarted. A failure the
  server cannot classify arrives as a structured envelope with a code and a
  hint — never a bare string — carrying the exception type only; the local
  log adds where it was raised, and withholds the message.
  `summary.total_count` means "every row matching your request" on both
  surfaces: `moneybin transactions list --limit 1 --output json` reported 1
  where MCP reported 1952 for the same query. That CLI command also paged by
  offset, so deleting a row above the page boundary silently skipped an
  unserved one and a newly-arriving row silently repeated a served one; it now
  uses the same keyset cursor MCP does.
  A SQLMesh model that was registered but never built used to be invisible to
  every health signal — the doctor now fails and names it, and `system_status`
  reports which models are absent. Next-step hints in CLI output now name
  `moneybin ...` commands: they previously named MCP tools, some of them
  retired, none of them runnable by whoever received the hint. The four
  import-preview refusals — not found, consumed, expired, changed — now say
  how to recover instead of only stating the fact.
- **`moneybin review` prints the counts its help promises instead of refusing to
  run.** A bare invocation, and `--type <queue>` on its own, reached a
  not-implemented stub while the counts sat behind `--status`; counts are now the
  default and the unbuilt item-by-item walk is reachable only via
  `--interactive`. The recovery hints printed after `transactions matches run`,
  `transactions matches backfill`, and `refresh` — plus the
  `transactions_matches_pending` MCP hint — pointed at
  `moneybin transactions review --type matches`, which was both the deprecated
  alias and the stubbed path; they now name `transactions matches pending` and
  `review --type matches --confirm`. Passing two of `--status`, `--interactive`, and
  `--confirm`/`--reject`/`--confirm-all` is a usage error rather than a silent
  pick. (#358)
- **`sync pull` no longer buries its results in diagnostic logging.** HTTP
  status lines, raw-loader row counts, per-tier match counts, and the
  profile-resolution source now go to the log file instead of the console, and
  bulk merchant creation reports a count rather than one line per id. Progress,
  per-institution totals, and what an AI assistant reads over MCP are
  unchanged. (#356)
- **A permission-denied import now tells you how to fix it.** Importing a file
  the OS refuses to open returns the new `infra_permission_denied` code with a
  hint matched to the actual cause: a file-mode problem says to check ownership
  and permissions, while a macOS block on `~/Documents`, `~/Desktop`, or
  `~/Downloads` says to grant Full Disk Access in System Settings → Privacy &
  Security and restart the app — the only step that works, and one no amount of
  `chmod` would have achieved. A denial that is neither says so plainly rather
  than guessing. The inbox no longer suggests `chmod` for a macOS access block.
- **A file that fails to import now reports why.** Per-file failures in
  `import_files` previously reported only the exception's class name
  (`PermissionError`), which told the user nothing actionable; each failure now
  carries the classified message, `error_code`, and `hint`. A batch in which
  every file failed now reports `status: "error"` instead of `"ok"` on both
  surfaces — the `import_files` tool and `moneybin import files --output json`,
  which also exits non-zero so a script checking `$?` no longer proceeds as
  though the data landed. Exceptions
  MoneyBin does not recognize still report only the class name — raw exception
  text can embed file contents.
- **`moneybin import preview` no longer prints a raw traceback on failure.** It
  now emits the same classified error every sibling import command does.
- **The consolidated MCP surface now preserves the safety and recovery
  contracts of the operations it replaces.** Permanent institution
  disconnects require payload-bound confirmation; human import decisions keep
  their 180-second window; PDF sign inversions can be approved over MCP against
  immutable preview bytes; partial import/sync failures retain actionable
  guidance; auto-rule proposals retain blast-radius review and proposal-scoped
  approval; abandoned confirmation tokens are evicted after their TTL; bounded
  account resolution remains confidence-ranked; investment and taxonomy
  continuations stay within their initial high-water boundary;
  transaction continuations retain their initial eligible-row count; multi-note
  threads retain stable note identities; and orphan annotations and accepted
  matches again expose executable recovery through the standard 45-tool
  registry. (#344)
- **Import preview parsing no longer drops rows or provenance at edge cases.**
  Header detection counts physical CSV lines, UTF-8 probing tolerates a
  multibyte character at the sample boundary, path-based detection stays
  bounded instead of loading the whole file, oversized PDFs are rejected before
  they can exhaust memory or inflate the encrypted snapshot store, and
  completed preview-to-import records survive snapshot cleanup. (#344)
- **Coarse reads no longer return plausible but incomplete results.** Balance
  drift distinguishes interpolated days from first observations, transaction
  account filters reject unresolved partial matches, archived accounts resolve
  by exact ID, and report limits must be positive. (#344)
- **`moneybin import preview` can now read a PDF statement.** It previously
  rejected every PDF with `Unsupported file type: '.pdf'`, because preview
  routed all files through the spreadsheet detector — so the only way to ask
  "will this statement extract cleanly, and how many rows?" without importing
  was through an AI assistant. The command now reports the extraction verdict,
  row count, confidence, and any pending credit-card sign confirmation (with
  the evidence and printed-vs-recorded samples behind it). An unreadable file
  — common on macOS, where statements sit in a folder your terminal hasn't
  been granted access to — now explains itself and names the fix instead of
  printing a stack trace. On a machine with no database yet, it points at
  `db init` rather than `db unlock` — the latter cannot work before a database
  exists. Spreadsheet-only options (`--format`, `--sheet`, `--delimiter`,
  `--encoding`, `--override`) now say they were ignored when passed with a PDF,
  instead of silently doing nothing.
- **When a repaired statement layout wants to reverse a direction you already
  approved, the choices you're offered now match what the commands do.** The
  prompt was written for the common case — "is this a credit card?" — where the
  answer always points the same way. A self-repaired layout can also propose the
  *opposite* flip, and there the card wording described `--confirm` as doing the
  reverse of what it does, and offered no command at all for keeping the
  direction you already had. Both choices are now named by what they do, in
  whichever direction the repair actually goes — in the terminal, in the
  approval an AI assistant puts in front of you, in its suggested next steps,
  and in the inbox's pending-file notes.
- **A saved statement layout that stops reading correctly now repairs itself
  instead of failing forever.** MoneyBin remembers how to read each statement
  layout the first time it sees one. That saved recipe was a frozen copy, so
  when an extraction bug was fixed, every layout already saved kept the old
  broken behavior — the fix could never reach it, and each new statement of that
  layout landed as an unparsed dump. Now, when a saved layout stops balancing,
  MoneyBin re-reads the statement from scratch and, if the fresh read balances to
  the cent, imports it and updates the saved layout. Two things it will not do on
  its own: replace a layout you or the assisted reader authored, or change a
  statement's income/expense direction. A layout you authored is left alone
  entirely; a direction change is shown to you with the evidence and the
  printed-vs-recorded samples, and nothing is imported until you approve or
  override it — in either direction, including when the re-read wants to *undo*
  an inversion you approved earlier. The repair is recorded in the audit log and
  can be undone.
- **Replacing a statement while its approval prompt is open no longer applies
  your answer to the new file.** Re-saving a corrected export over the same path
  mid-prompt could previously reverse every amount in a document you never
  reviewed; the import is now refused instead. Affects all three confirmation
  paths (spreadsheet, AI-extracted PDF, and card statement).
- **Choosing an account for a PDF import now fails loudly instead of quietly
  doing something else.** Both PDF import paths only ever supported pinning by
  account id, but passing `account_bindings` or `account_metadata` was accepted
  and then ignored — the transactions landed in an account derived from the
  statement or the filename while you believed you had chosen one. Those
  parameters are now refused with a message naming the one that works.
- **Real credit-card PDF statements now extract their transactions instead of
  falling back to a raw dump.** Chase card statements (and others shaped like
  them) print their transaction table in three ways no synthetic sample did: a
  column header wrapped across two physical lines ("Date of" above
  "Transaction … $ Amount"), section sub-headers ("PAYMENTS AND OTHER CREDITS",
  "PURCHASE", "INTEREST CHARGED") interleaved among the rows, and dates printed
  as MM/DD with the year only on a separate "Opening/Closing Date" line.
  Previously every such statement extracted zero transactions and was stored as
  an unparsed seed; now the table is reconstructed from the rows' shape, each
  row's year is resolved from the statement's billing period (correct even when
  the cycle crosses year-end), and the statement imports like any other. A
  statement whose columns genuinely can't be derived deterministically still
  escalates to the assisted reader rather than being silently seeded. (#329)
- **Non-USD transactions and balances are no longer silently relabeled
  USD.** OFX's per-statement currency (`CURDEF`) and Plaid's per-balance
  currency were parsed but discarded; every transaction and balance landed
  with an unrecorded, assumed `USD`. Currency is now captured end-to-end
  from OFX and Plaid, and a transaction or balance with no currency of its
  own inherits its account's explicit currency setting when one exists.
  `moneybin accounts set --currency` (or MCP
  `accounts_set(currency_code=...)`) sets that setting. An account with no
  explicit setting still defaults to `USD` — closing that gap with a
  genuine "unknown, not guessed" terminal case is scoped to a follow-up
  (M1K.1 Part B), alongside full currency-aware reporting (a home-currency
  setting and a guard against silently summing mixed currencies).
- **Credit-card PDF statements now import with correct signs.** A statement that
  names itself a credit card (via its required disclosures — "minimum payment",
  "credit limit", and the like) derives the inverted convention
  (`negative_is_income`) behind an explicit confirmation: charges record as
  expenses, payments as credits. Previously every card statement was refused,
  because the sign convention could not be expressed and guessing it would have
  silently inverted the ledger. The confirmation is once per statement format —
  confirm it is a card (`moneybin import files <path> --confirm`), or overrule a
  false detection (`--sign negative_is_expense`), and that override survives every
  future replay of the format. Confirming a card also types its account as
  `credit`, so it is counted as a liability in net worth. Agent-authored PDF
  bridge recipes now require an MCP human-confirmation prompt before they can
  invert a ledger; clients without that prompt use `moneybin import confirm
  <path> --bridge-response response.json --confirm`. Tabular credit-card
  inferences now likewise pause after mapping confirmation until a person runs
  `moneybin import confirm <path> --accept --confirm-sign`; accepting a column
  mapping alone can never approve the ledger-wide sign inversion. The matching
  “keep amounts as printed” recovery is now the lossless
  `moneybin import confirm <path> --accept --sign negative_is_expense`; both
  alternatives retain any mapping, format-save, and account-binding inputs. (#324)
- **Auto-rule proposals can no longer silently mass-mislabel the ledger.** A
  transaction description that normalizes to a 1–2 character token (e.g. "TO",
  from a truncated "TRANSFER TO ...") previously became a `contains` rule —
  matching any description containing that substring, including unrelated
  merchants like STORE, AUTO, and TOTAL. Accepting the proposal would
  recategorize all of them as Internal Transfer, which also drops those rows
  out of every spend report. A short, machine-invented pattern is now proposed
  as an `exact` match instead of `contains` (a user-authored merchant pattern
  is untouched); every proposal reports how many transactions it would
  actually recategorize (`estimated_match_count`); and a proposal whose blast
  radius outruns its evidence (`is_broad`) is skipped on accept unless the
  caller explicitly opts in (MCP `allow_broad`, CLI `--allow-broad`). A
  proposal already pending from before this change keeps its original
  `contains` pattern and won't be reinforced by further matching evidence
  under the new `exact` lookup, so a second, `exact`-typed proposal for the
  same evidence may appear alongside it. Both are still subject to the same
  broad-match check before either can be promoted to a rule, so this is
  fail-safe — just occasionally duplicative until the older proposal is
  reviewed or ages out.
- **Directly creating a categorization rule can no longer bypass the
  short-`contains`-pattern guard.** The auto-rule proposer downgrades an
  overly short machine-invented pattern (e.g. "TO") to `exact` so it can't
  mass-mislabel the ledger, but `transactions_categorize_rules_create` (and
  `moneybin transactions categorize rules create`) let a caller author that
  same dangerous rule directly, with no check at all. A `contains` rule
  whose pattern is shorter than `auto_rule_min_contains_length` (default 4)
  is now refused rather than inserted — the item is counted in `skipped`
  and `error_details` explains the refusal and how to proceed
  (`match_type="exact"`, or `allow_broad=True`/`--allow-broad` to accept the
  risk). `exact` patterns of any length are unaffected, and an ordinarily
  broad but selective `contains` pattern (e.g. `"AMAZON"`) is never gated —
  this is a specificity floor, not a breadth-vs-evidence check like
  auto-rule review's `allow_broad`.
- **The uncategorized-transactions queue no longer treats an unresolved
  transfer leg as an ordinary row.** A transaction awaiting a transfer-match
  decision was previously indistinguishable from any other uncategorized row;
  categorizing it double-counts it against the eventual transfer pair once
  matching resolves. Rows with a pending transfer match are now flagged, with
  a hint to resolve the match first — they are still returned, never hidden.
- **The MCP server's agent-facing instructions no longer claim a consent gate
  that doesn't exist.** The onboarding text injected at session start said tools
  "degrade to aggregates" without consent — no such behavior is implemented. It
  now states the truth: account/routing numbers are masked, all other fields
  reach the model provider as-is, and there is no consent gate yet.
- **`sql_query` and `moneybin sql query` now name the column an unknown-column
  query got wrong, instead of a bare "Query execution failed." (#382).** DuckDB
  raises `BinderException` for an unknown column, so that case fell through to a
  generic no-detail bucket; it now returns `sql_unknown_table` with a hint naming
  the unresolved identifier, and that code's message widened to "Query could not
  be bound to the schema." because it covers every binder rejection rather than
  only a missing table or column — an agent branching on the error code should
  update.

### Security
- **`sql_query` no longer publishes the tail of a serialized JSON column as if
  it were an account number's last four digits.**
  `app.account_link_decisions.match_signals` — a JSON column holding the weak
  signals behind a proposed account link (institution last-four, name
  similarity) — was classified `ACCOUNT_IDENTIFIER`, whose mask keeps
  `value[-4:]`. A JSON column reaches the masking transform as a serialized
  `str`, so the four characters that survived were the tail of the JSON
  text — closing punctuation plus whatever key happened to sort last — not a
  real signal value, and which characters leaked depended on key order rather
  than anything the declaration controlled. A new `COMPOSITE_IDENTIFIER` class
  masks this and any future serialized/composite column WHOLE instead,
  matching the precedent already set for two sibling JSON columns
  (`raw.import_log.account_names`, `prep.int_transactions__matched
  .match_group_id`). A regression test walks every JSON-typed `core.*`/`app.*`
  column and fails if one is ever classified with a partial mask again. (#451)
- **Six CVEs cleared, and the audit that missed two of them now sees every
  dependency group.** `cryptography` 49.0.0 → 50.0.0 (PYSEC-2026-3552), `pip`
  26.1.2 → 26.2.1 (PYSEC-2026-3721), and `pymdown-extensions` 10.21.3 → 11.0.2
  (PYSEC-2026-3609, PYSEC-2026-3654). `sqlparse` carried four more
  (PYSEC-2026-3696/3697/3698/3699) and was dropped rather than pinned: it was
  stranded when `sqlfluff` was removed in favour of `sqlmesh format`, and
  nothing has referenced it since — no source import, no Makefile or
  pre-commit invocation, and no reverse dependency in the resolved tree. A
  security floor for a package nothing uses only defers the next advisory.

  The `pymdown-extensions` pair was invisible to CI. `make audit` ran
  `uv run pip-audit`, which resolves only the default dependency groups, so
  the `docs` and `server` groups went unaudited by both the Security workflow
  and the Release pipeline — they call that one target precisely so the
  accepted-vuln list cannot drift. It now runs `uv run --all-groups
  pip-audit`, which covers the whole dependency surface and needed no change
  to either workflow.
- **CLI tracebacks no longer render local variables, and database attach
  errors are sanitized.** DuckDB cannot parameterize `ENCRYPTION_KEY`, so the
  database key is written inline into the `ATTACH` statement — which puts it in
  frame locals, and in some failure modes into the error text DuckDB hands
  back. Neither is a log record, so the log sanitizer never applied to either.
  The root CLI app now pins locals off instead of inheriting the dependency's
  default, and attach failures are stripped of key material before they
  surface, keeping their exception type and lock-contention classification
  intact.
- **A rejected currency code no longer reaches the durable CLI log (#393).**
  `moneybin fx` and `moneybin investments prices set` / `delete` take the
  currency as free text, and both services interpolated whatever was typed into
  `UserError.message`. Text-mode `handle_cli_errors` sends `message` to
  `logger.error`, and the file handler has no level filter, so a mis-pasted
  argument — an account fragment, a note, anything on the clipboard — persisted
  verbatim to `cli_YYYY-MM-DD.log`. The rejected value now rides the `hint`,
  which reaches stderr and the JSON envelope but never the logger, matching the
  split the FX date path already used. The message states the rule rather than
  the value, and the hint still names what was rejected, so a caller who passed
  two codes can tell which one failed.
- **`import_revert` no longer deletes raw rows on the first call (#391).**
  `import_revert(operation="revert_import")` explicitly *rejected* a
  `confirmation_token` and went straight to the delete, so one call permanently
  destroyed a batch's raw rows with no prompt, no token, and no undo — 368 rows
  across 14 batches went in a single session before anyone noticed. The branch
  now plans read-only, binds approval to the exact live row counts, and verifies
  that binding against live state inside the write transaction, matching the
  `delete_saved_format` branch beside it. Outcomes that would delete nothing
  (`not_found`, `already_reverted`, `unsupported`, `superseded`) return their
  error without prompting. The CLI already confirmed and still does; its prompt
  now names the row count. The registered description carried the same defect —
  its confirmation and `system_audit_undo` clauses sat next to the format branch
  but read as covering the whole tool, so an agent could infer reversions were
  recoverable. Each clause now sits with its own branch, and reversion is
  labelled `permanent — no revert`.
- **An unanswered confirmation prompt no longer mints a redeemable token
  (#389).**
  `grant_confirmation_or_raise` waited `elicitation_wait_seconds` (120 by
  default) for a human and then issued an opaque confirmation token, which is
  returned to the caller — so ignoring a destructive prompt for two minutes
  handed the calling agent a key to its own unconfirmed operation. The timeout
  now refuses with `MUTATION_CONFIRMATION_DECLINED` and `reason: "timeout"`,
  minting nothing. Clients that never declared elicitation support keep the
  token path, the only case with no prompt to retry. All 13 confirm-gated tool
  modules share the helper, so this covers every destructive confirm.
- **A `UserError` hint shown on the CLI no longer reaches the durable log file
  (#382).** `handle_cli_errors` logged every hint via `logger.info` and the CLI
  file handler has no level filter, which became a disclosure once `sql_query`'s
  new hint started carrying caller-typed text; the hint now prints to the
  console via `typer.echo(..., err=True)` instead, so only its destination
  changed.
- **Fixed a redaction bypass that returned a source account identifier in the
  clear through `import_confirm`'s `confirmation_required` envelope (#372).**
  The tool declares `dynamic_classification=True`, and the decorator skips
  `redact_typed` for dynamic tools — so the raw-dict envelope shipped
  `account_proposals[].source_account_key` (the native OFX/Plaid identifier,
  `ACCOUNT_IDENTIFIER` → CRITICAL) unmasked, with no `classes_returned`
  recorded. It is now a typed `ImportConfirmRequiredPayload` routed through the
  redaction path, the key is masked, and `classes_returned` reports
  `account_identifier`. A related hint in `actions[]` interpolated the same raw
  key into prose, which the middleware never redacts; it no longer names the
  key at all.
- **Fixed an under-classification leak that returned a bank routing number in
  the clear through `sql_query` / `moneybin sql query` via `INTERSECT`.** The
  set-operation fix in #330 treated `INTERSECT` like `EXCEPT` — values from the
  left branch only, since the right operand filters rather than contributes. That
  holds for `EXCEPT` and not for `INTERSECT`: a row survives an `INTERSECT` only
  when the value is present on both sides, so the value it returns is the right
  operand's as much as the left's. `SELECT '021000021' AS v INTERSECT SELECT
  routing_number FROM core.dim_accounts` classified the column from the left
  branch alone (`TXN_TYPE`, LOW), returned the real `routing_number` unmasked,
  and so confirmed a guessed value. Both operands are now classified, `EXCEPT`
  still takes the left branch alone, and the asymmetry is pinned by one test per
  operator. (#367)
- **Fixed a redaction bypass that returned a bank routing number in the clear
  through `sql_query` / `moneybin sql query` via `PRAGMA storage_info`.**
  DuckDB reports per-segment `stats` for each column, and for a text column
  those statistics are a cleartext prefix of the stored value: the
  `routing_number` segment returned the first eight of its nine digits. An ABA
  routing number's ninth digit is a check digit determined by the first eight,
  so the whole number was recoverable. The statement ran at `sensitivity: "low"`
  with no masking, and it reached tables in `raw`, `prep`, and the SQLMesh
  physical schemas that a `SELECT` refuses outright. `PRAGMA` and `EXPLAIN` are
  no longer accepted by either surface — neither exposes the table it reads as
  a parseable reference, so the schema restriction could not see them, and
  `EXPLAIN ANALYZE` additionally executed the query it was given. Use
  `DESCRIBE <table>` or `SHOW ALL TABLES` to inspect schema; `moneybin db query`
  still runs query plans as raw operator access.
- **Applied the queryable-schema restriction to catalog statements.**
  `DESCRIBE raw.plaid_transactions` returned an internal table's full column
  list while `SELECT ... FROM raw.plaid_transactions` was refused; both are now
  held to `core`, `app`, and `reports`. `SHOW ALL TABLES` still lists the whole
  catalog, including column names and types, because it names no table for the
  restriction to resolve — internal table *shape* remains visible, their row
  values do not.
- **Fixed a redaction bypass that returned bank routing numbers in the clear
  through `sql_query` / `moneybin sql query` when a query carried two
  statements.** Each statement in `SELECT 1; SELECT routing_number FROM
  core.dim_accounts` is individually a legal read, so every existing gate
  passed; the pair parsed to one `Block`, which the classifier did not
  recognize as a data query and the caller therefore treated as metadata —
  executing the string unclassified at LOW. DuckDB returns the last
  statement's rows, so the class map described the first statement while the
  caller received the second. Multi-statement input is now refused, and the
  metadata path routes on a positive allowlist of statement kinds rather than
  on "not a data query", so an unrecognized statement kind fails closed instead
  of executing unmasked — the same default-open fallback that produced the
  `EXCEPT`/`INTERSECT` leak below. (#346)
- **Closed a second route to the same leak, where a `--` comment hid the extra
  statement.** The gates read the query with its whitespace collapsed, which
  erased the newline that ends a `--` comment: in `SELECT 1 AS a; -- note`
  followed by `SELECT routing_number AS a FROM core.dim_accounts`, the
  classifier saw one harmless statement while DuckDB ran both and returned the
  second's rows. Naming both columns `a` matched the classified column name,
  so the fail-closed check that catches a shape mismatch never fired. Queries
  are now parsed exactly as DuckDB receives them, which also stops the same
  collapsing from rewriting spacing inside quoted identifiers and string
  literals — a third way the classified and executed queries could differ.
  Formatted multi-line SQL, trailing `; -- comment`, and `;;` still run. (#346)
- **CVE fixes via dependency bumps:** `mcp` 1.27.1 → 1.28.1, `pillow`
  12.2.0 → 12.3.0, `httplib2` 0.31.2 → 0.32.0, closing 12 advisories. The
  `mcp` ones affect MoneyBin's own MCP server: HTTP transports served
  session requests without verifying the authenticated principal
  (CVE-2026-52869), experimental task handlers let any client read or
  cancel another client's tasks (CVE-2026-52870), and the WebSocket
  transport had no Host/Origin validation (CVE-2026-59950). `pillow`
  (reached through PDF import) covers unvalidated PCF glyph dimensions and
  an `ImageCms` heap-corruption path; `httplib2` (reached through the
  Google Sheets connector) covers unbounded gzip/deflate decompression of
  response bodies. `mcp` and `httplib2` are now declared as direct
  dependencies, since MoneyBin imports both. (#335)
- **Fixed several under-classification leaks that returned CRITICAL-tier
  values (bank routing numbers) in the clear through `sql_query` /
  `moneybin sql query`.** A CTE or derived table named after a real table,
  CTE-nesting depth exhaustion, partial `UNION`-branch resolution,
  `EXCEPT`/`INTERSECT` set operations, and opaque projection forms
  (`COLUMNS(...)`, `PIVOT`, `UNPIVOT`, `SUMMARIZE`, the whole-row
  pseudo-column, `UNNEST` of one) could each cause
  `core.dim_accounts.routing_number` to resolve to `AGGREGATE` (LOW) instead
  of its true `ROUTING_NUMBER` (CRITICAL) class, returning it unmasked. Most
  of these were pre-existing defects in the SQL classifier already on
  `main` — not introduced by this change — surfaced and fixed during an
  audit prompted by the `reports.*` coverage-gap fix below. Any output
  column an undeclared or unresolvable reference reaches now fails closed to
  a new `DataClass.UNRESOLVED` (masked whole) instead of falling back to the
  most-permissive class seen elsewhere in the query. (#330 follow-up)

### Changed
- **Google Sheets MCP connections can no longer set an inferred sign convention
  themselves.** The agent-settable `sign` input was removed; an inferred
  `negative_is_income` convention now requires a human confirmation prompt,
  while the CLI continues to require an explicit `--sign` choice. (#324)

M2 closing out and M3 underway. M2A curator state shipped (transaction notes, tags, splits, manual entry, audit log). M2B architecture reference shipped (`architecture-shared-primitives.md`; writer-coordination contract via short-lived per-call connections). M2C brand surface advancing: `moneybin system doctor` integrity command, `reports.*` recipe library (eight curated views), and the `transform_*` MCP toolset closing the agent ingest loop. M3A Plaid Transactions sync shipped (Phase 1). Doc surface tightened for the personas reachable today; MCP surface hardened with protocol-standard annotations, `accounts_resolve`, list-parameter cap, structured error envelopes, and shell completion. Categorization correctness pass: memo-aware matcher, exemplar accumulation, source-precedence enforcement, auto-fan-out after apply; seed merchant catalogs retired in favor of user-driven and LLM-assist-driven merchant creation.

### Added
- **Bounded MCP standard registry.** The pre-launch MCP cutover now exposes
  one 45-tool standard registry to every generic client; supported hosts may
  defer schemas from that identical registry without reconnect, packs, or
  profiles. Reports extend the `reports` catalog rather than consuming tool
  slots, and future tools require the published admission record. The
  deterministic comparison reduced metadata from 90,734 to 46,454 bytes;
  promotion remains pending observed context-budget and host-deferral evidence.
- **Executable CLI/MCP capability parity.** A checked outcome map now covers all
  45 standard MCP tools and every implemented Typer path by service ownership
  and durable result, replacing the old canonical-name drift test. It includes
  isolated-state parity tests for refresh, reports, annotations, taxonomy,
  consent, import, sync, and SQL. `accounts summary` is now available on the
  CLI, and the formerly-placeholder category and merchant taxonomy commands
  execute through the shared categorization service.
- **Nonblocking MCP sync authentication within the existing four-tool
  surface.** `sync_link(mode="login")` begins device authorization,
  `sync_status(auth_session_id=...)` advances it with idempotent terminal
  replay and local expiry enforcement, and `sync_disconnect(mode="logout")`
  clears credentials plus pending profile-scoped sessions. Secret device codes
  and tokens remain in `SecretStore`; MCP sees only safe user-facing fields.
  Expired flows now shed device codes during any collection update, while the
  newly created or currently addressed flow is preserved within a per-profile
  ceiling of 16 pending and 16 terminal sessions so abandoned or bursty flows
  cannot grow keychain state indefinitely.
  `transactions_categorize_run(operation="improve_ai")` similarly absorbs the
  provider-native AI-upgrade outcome without increasing the 45-tool surface.
- **"What the AI Provider Sees" guide.** A precise, code-verified statement of
  what reaches the model provider when an agent drives MoneyBin — what's masked
  (account/routing numbers, enforced today), what isn't (amounts, descriptions,
  merchants, dates), what the consent ledger does and doesn't gate, what's
  recorded locally, and how to run a fully local model so nothing leaves the
  machine. [`docs/guides/what-the-ai-sees.md`](docs/guides/what-the-ai-sees.md).
- **`moneybin --version`** prints the installed MoneyBin version. (#316)
- **PyPI release pipeline with Trusted Publishing.** A tagged release builds the
  wheel and publishes it to PyPI over OIDC Trusted Publishing (no stored token),
  gated on a clean-install smoke test across macOS and Linux on Python 3.12 and
  3.13 and a post-publish check that installs MoneyBin from the real index. (#316)
- **`moneybin demo` evaluator preset (M3A).** One command sets up an isolated
  `demo` profile, generates synthetic data (`--persona
  basic`/`family`/`freelancer`), runs the full pipeline — match, and categorization
  by the real engine against the merchants the generator invented — to a clean
  `system doctor`, activates the profile, and prints net worth plus next steps: a
  from-install path to a working product with no real financial data. It always
  targets the dedicated `demo` profile (there is no `--profile` target, so it can
  never be pointed at a real one), and re-running rebuilds that profile's database
  from scratch and regenerates (deterministic by default); `--yes` for
  non-interactive use. (#310)
- **Plaid Investments sync (M1G.4).** Securities, investment transactions, and
  dated holdings snapshots (with per-lot tax data) now ride the existing
  `sync pull` job into five new `raw.plaid_*` tables and flow into the
  investment ledger — the shipped cost-basis engine derives lots, realized
  gains, and holdings with no engine changes. Security identity resolves
  through an adopt-or-mint ladder (`SecurityResolver`): adopt an existing
  binding, auto-bind on an unambiguous strong identifier (CUSIP/ISIN/exact
  ticker), or refuse to merge on any ambiguity — a stripped-ticker hit, an
  identifier tie, or a fuzzy name match mints a provisional security and
  files one pending merge decision per candidate for review
  (`investments securities links pending/set/history` on CLI and MCP, also
  surfaced in the `review` sweep and `system_status`). Accepting a merge
  fuses two instruments' tax lots, so it always requires a human confirm —
  over MCP the accept is gated behind an elicitation naming both securities,
  and a client that cannot elicit is directed to the CLI rather than allowed
  to proceed. An opening-lot
  bootstrap seeds pre-window positions from the first holdings snapshot so a
  long-held position doesn't realize a phantom oversold gain on its first
  Plaid-reported sale. `system doctor` gains eight investment reconciliation
  checks: staging rows held for review (splits, underivable transfer
  directions, unmapped subtypes),
  opening-lot-bootstrap gaps, unmodeled short/option legs,
  holdings-vs-ledger divergence, manual-and-Plaid source overlap, unresolved
  securities, and positions the broker or the ledger reports that the other
  side doesn't. A per-pull holdings-snapshot receipt records that an item
  reported even when it returns zero positions, so a fully-liquidated broker
  is visible as liquidated rather than read as still holding its last
  reported positions. Three
  behaviors ship a conservative default pending Plaid Sandbox golden
  validation: reinvest/corporate-action pairing (`event_group_id`) is not yet
  linked, fee inclusion in `amount` is assumed (with a drift guard), and
  every stock split routes to manual review instead of auto-deriving a
  multiplier. (#318)
- **Investment data model & cost-basis engine (M1J.1).** A manually-maintained
  securities catalog (`investments securities add/set/list`) and an
  investment-transaction ledger (`investments add` — buy, sell, reinvest,
  dividend, interest, capital-gain distribution, transfer in/out, deposit,
  withdrawal, split, fee, return of capital) derive tax lots, realized
  gain/loss (short- and long-term, 1099-B-reconciliation-ready), and current
  holdings (`investments holdings` — cost basis only; market value awaits a
  future price-feed pillar). Four cost-basis methods — FIFO, HIFO, specific
  identification, and average cost — apply per-security
  (`investments securities set --method`) or per-account
  (`accounts set --default-cost-basis-method`), falling back to global FIFO;
  `investments lots select` overrides which lots a sale draws from. New
  `investments` / `investments_holdings` / `investments_lots` /
  `investments_gains` / `investments_securities` read and
  `investments_record` / `investments_securities_set` /
  `investments_lots_select` write MCP tools, plus the top-level `investments`
  CLI group (replacing the earlier `accounts investments` placeholder). (#300)
- **Plaid balance snapshots flow into net worth and balance drift.**
  Plaid sync balances now reach `core.fct_balances` → `core.fct_balances_daily`,
  so `reports networth` / `networth-history` and balance-drift detection include
  Plaid-connected accounts (previously only OFX statement balances, tabular
  running balances, and manual assertions contributed). Credit/loan balances are
  recorded as liabilities (negative), and `core.dim_accounts` now sources Plaid
  `official_name`/`account_subtype` under any user override. (#299)
- **Category taxonomy audit — 112-category curated set (M1W).**
  Audited all 108 seed categories against four principles (earn-the-split
  granularity, class-by-accounting-nature, no redundant/orphan categories,
  provider-neutral): retired 5 duplicate/orphan categories (resolving the
  two-mortgage-category ambiguity in favour of `LNP-MTG`) and added 9 — 6 finer
  categories from the 29 unmapped Plaid detailed codes, plus a 3-category
  **Family & Kids** group (`FAM`/`FAM-ACT`/`FAM-SUP`) folded in after a
  coverage audit identified the gap; `class` reconciled end-to-end (no
  reclasses needed).
  Net 108 − 5 + 9 → 112 categories. Seed validation now
  enforces a valid-class invariant, an enumerated coverage report, and an orphan
  allowlist. Purely additive on the M1V bridge — no consumer query changes. (#298)
- **`transactions categorize improve-ai` — upgrade AI-guessed categories to confident Plaid categories (M1U follow-up).**
  New CLI command and matching MCP tool (`transactions_categorize_improve_ai`)
  reverse-look-up every transaction currently `categorized_by='ai'` against the
  `core.bridge_category_source_map` bridge and upgrade it to `provider_native`
  when the match is at MEDIUM confidence or higher. Never touches user, rule,
  or merchant categorizations. (#294)
- **Automatic Plaid category assignment from Personal Finance Category (M1U).**
  Transactions synced from Plaid are now auto-categorized from Plaid's PFC codes
  via the `core.bridge_category_source_map` bridge (source `provider_native`,
  two-tier detailed→primary reverse lookup, confidence-gated at ≥MEDIUM), running
  last after rules and merchants in `categorize_pending` so it clears the long tail
  before the LLM. A rule or merchant you author after the import overrides the Plaid
  category on the next categorize run — the source-precedence ladder holds across
  runs, not just within one write. `transactions categorize stats` gains a
  `plaid_unmapped` count (Plaid transactions whose PFC code has no bridge mapping
  yet). (#292)
- **`core.bridge_category_source_map` — provider-code → canonical-category bridge (M1V).**
  A durable, aggregator-agnostic view resolving any provider's transaction-category
  code to exactly one canonical MoneyBin category, keyed `(source_type,
  source_category_code)`. Two-tier lookup (`code_level`: `detailed` preferred,
  `primary` fallback) so an unmapped detailed code still lands in the right
  top-level category. Backed by `seeds.category_source_map` (91 rows re-derived
  against Plaid's verified Personal-Finance-Category taxonomy) unioned with
  `app.category_source_map` (user overrides always win). Prerequisite for the
  parked Plaid Tier-2b categorizer.
- **Resolve transaction merchants by Plaid `merchant_entity_id` before name matching (M1T).** Two new `app.*` tables (`merchant_links` binding + `merchant_link_decisions` review queue) back an adopt-or-mint ladder that fires at categorization time; a backfill `harvest()` records existing assignments with zero review (conflicts-only). New `merchants links pending / set / history / run` CLI subgroup and `merchants_links_pending / _set / _history / _run` MCP tools surface fuzzy-match proposals; the top-level `review` tool gains a merchant-links queue.
- **Plaid max-data capture.** Plaid sync now captures the institution's original
  (raw) description as a new `original_description` column on
  `core.fct_transactions`, distinct from Plaid's cleaned `description`. The sync
  path also populates currency, authorized date, pending-transaction link, payment
  channel, check number, and merchant location on `core.fct_transactions`
  (previously NULL for Plaid). Merchant entity id and Plaid's detailed
  personal-finance category are captured into `raw.plaid_transactions` for later
  merchant-resolution / categorization work. Run `moneybin sync pull --force` to
  backfill existing transactions. (#283)
- **Import-time account-binding confirmation (M1S.4).** Tabular `import_confirm`
  now surfaces the account resolver's verdict at import time. When an
  interactive human imports a file whose source account resolves to weak merge
  candidate(s) (`institution+last4` / name), the import returns
  `confirmation_required` with `confirmation_payload.{reason="account_confirmation",
  account_proposals[]}` instead of silently minting — the column layout is
  settled, only the account identity needs ratifying. The caller binds each
  proposed account via `account_bindings` (MCP) / `--account-binding
  source_key=ACCOUNT_ID|new` (CLI): adopt an existing account, or `new` to mint
  a distinct one. A `"new"` account can capture `display_name` / `account_subtype`
  / `last_four` / `iso_currency_code` at mint via `account_metadata` (MCP) /
  `--account-meta source_key:field=value` (CLI). Agent / non-interactive imports
  never gate here — they load and leave the proposal in the account-link review
  queue (`accounts_links_pending`). The `moneybin_account_link_review_pending`
  gauge and `moneybin_account_link_confidence` histogram now emit.
- **Account-link review queue (M1S.5).** New `accounts_links_pending` /
  `accounts_links_set` / `accounts_links_history` / `accounts_links_run` MCP
  tools and the `moneybin accounts links` CLI subgroup surface the cross-source
  account-merge proposals the resolver raises (`institution+last4` / name) so a
  weak account match is reviewed, never silently merged. Accepting a proposal
  re-points the provisional account's native references onto the chosen
  canonical account (auto-rejecting siblings); `--standalone` keeps it separate.
  `accounts links run` backfills proposals over existing accounts. Account
  numbers are never surfaced (proposals carry opaque ids + labels only).
- **Smart-import-pdf Phase 2a — deterministic PDF routing to `raw.tabular_transactions`.**
  PDFs that auto-derive (or replay a saved) high-confidence recipe land
  rows in `raw.tabular_transactions` (`source_type='pdf'`) instead of the
  Phase 1 catch-all seed table; everything else (no transaction-shaped
  table, reconciliation failure, missing balance metadata) still falls
  back to `raw.pdf_seeds`. Auto-derived recipes persist to
  `app.pdf_formats` on first contact (keyed by layout fingerprint =
  issuer + sorted dedup headers + page bucket) so a second statement
  with the same layout replays the saved recipe instead of re-deriving.
  Reconciliation gate enforces pre-sign-normalization sum identity with
  the statement's reported balance delta within 1¢. See
  [`docs/specs/smart-import-pdf.md`](docs/specs/smart-import-pdf.md).
- **Smart-import-pdf Phase 2b — bridge round-trip to the driving agent.**
  A native-text PDF the deterministic rung can't crack (low confidence,
  failed reconciliation, missing balances) now hands the document to the AI
  agent already driving MoneyBin instead of silently seeding:
  `import_files`/`import_preview` return a `confirmation_required` envelope
  carrying the document text, a table preview, the layout fingerprint, and a
  plain transparency notice (proceeding surfaces the document to the agent),
  and `import_confirm(bridge_response={recipe, rows})` ratifies. MoneyBin
  re-runs the agent's recipe and reconciles the re-executed rows against the
  statement balances — the authority — before any transactions load, verifies
  the agent's returned rows against the re-execution, and reports any
  row-count divergence. Every hand-off writes a `smart_import_parse` privacy
  audit row and bumps `moneybin_pdf_bridge_egress_total{outcome}`. MCP-only
  for now (gated on `actor_kind="agent"`); a bare CLI keeps the seed fallback.
- **Smart-import-pdf Phase 2b complete — recipe auto-recovery + scanned-PDF
  degradation.** A saved PDF recipe that stops serving its layout (fails
  validation on replay, or stops reconciling) is now re-derived and installed
  as a new audited, undo-reversible version on the next import, instead of
  stranding the broken recipe so every future statement re-escalates. A
  scanned / image-only PDF with no selectable text layer now returns an
  explicit unsupported outcome (a clear "needs a vision-capable backend"
  message, error code `import_pdf_no_text_layer`) rather than a generic
  "no tables extracted" failure. The bridge parser also rejects an agent
  recipe whose amount fields don't match its declared sign convention. See
  [`docs/specs/smart-import-pdf.md`](docs/specs/smart-import-pdf.md).
- **`moneybin import formats list --type {tabular,pdf,all}`** (default
  `all`) filters by format kind and renders tabular + PDF sections in
  text; JSON output is a uniform list with a `type` discriminator per
  row. **`moneybin import formats show <name>`** resolves across both
  namespaces.
- **`import_formats` MCP tool now returns `pdf_formats: list[…]` alongside
  the existing `formats: list[…]`** so agents have parity with the CLI.
  Each PDF row carries `{name, institution_name, document_kind, routing,
  front_end, version, times_used, last_used_at}`.
- **Three new Prometheus metrics under `moneybin_pdf_*`:**
  `extraction_confidence` (Histogram, 0–1), `recipe_hit_total{outcome}`
  (Counter, outcomes: `replay_success`/`replay_failed`), and
  `replay_guard_failure_total` (Counter, no labels — separate raw signal
  for alerting on recipe drift).
- **`import_confirm` MCP tool + `moneybin import confirm` CLI subcommand.**
  Terminal `_confirm` step of the propose→review→confirm flow for smart tabular
  imports. First-encounter imports surface a `confirmation_required` envelope;
  the caller accepts (`accept=True` / `--accept`) or applies a partial-merge
  column-mapping override (`mapping={...}` / `--mapping field=col`). `save_format`
  (default `True`) pins the merged mapping to `app.tabular_formats` for silent reuse.
  Revertible via `import_revert` (data rows) + `system_audit_undo` (format save).
  See [`docs/specs/smart-import-confirmation.md`](docs/specs/smart-import-confirmation.md).
- **Cross-channel confidence contract.** Tabular and gsheet channels share a
  normalized `score` plus derived `tier` (`high`/`medium`/`low`) with configurable
  bands. Defaults: `T_high=0.90`, `T_med=0.70`. Env vars:
  `MONEYBIN_IMPORT___CONFIDENCE__T_HIGH` / `MONEYBIN_IMPORT___CONFIDENCE__T_MED`
  (three underscores between `IMPORT` and `CONFIDENCE` due to Pydantic nested-settings alias).
- **Tiered agent autonomy gate.** `MONEYBIN_IMPORT___SELF_ACCEPT_HIGH` (default
  `False`). When enabled after calibration earns the precision bar, MCP agents may
  self-accept `high`-tier first encounters. The CLI human path always prompts regardless.
- **New `--confirm`/`--mapping` flags on `moneybin import files`.** `--confirm` /
  `--no-confirm` accepts or declines a `confirmation_required` proposal inline;
  `--mapping field=column` (repeatable) is a partial-merge alias of `--override`.
  Non-TTY / `--output json` returns the `confirmation_required` envelope and exits 0.
- **`import_files` MCP envelope now returns `confirmation_required` state** on
  first-encounter unknown layouts, including `proposed_mapping`, `samples`, `flagged`,
  `missing_required`, `unmapped_columns`, and `actions[]` recovery hints pointing at
  `import_confirm`.
- **Six new Prometheus metrics under `moneybin_import_*`:**
  `confirmations_total{channel,tier,outcome}` (outcomes: `accepted|overridden|declined`),
  `detection_score` histogram, `self_accept_total{channel}`, `override_total{channel}`,
  `known_format_reuse_total{channel}`, `revalidation_failure_total{channel}`.
- **`DatabaseLockError` is now emitted consistently on cross-process database
  contention.** A new MoneyBin-owned write critical-section lock coordinates
  before DuckDB's own ATTACH layer, identifying the holder and timing out at
  10 seconds with a `system_status` recovery action. Fixes a regression where
  DuckDB 1.5.3's unified lock-error string (`"Could not set lock on file"`)
  was no longer matched by the classifier, causing raw `duckdb.IOException`
  to leak to MCP, CLI, and Web UI callers. See
  [`docs/specs/database-writer-coordination.md`](docs/specs/database-writer-coordination.md)
  § "PR B hardening pass" and [ADR-010](docs/decisions/010-writer-coordination.md).
- **`Database.checkpoint(reason)` helper** at durable boundaries — wired now
  at post-migration and post-transform-apply; pre-backup / post-compact /
  post-large-import sites land when those features ship. Emits
  `moneybin_db_checkpoint_total{reason=...}`.
- **`system_status` `database_connections` section** identifies the active
  writer (via the lock file) and concurrent readers (via `lsof`). Powers the
  `DatabaseLockError` recovery action.
- **`review` MCP tool and `moneybin review` CLI command** (M1S.5c) — domain-neutral
  orientation sweep that aggregates all three review queues in one call:
  `matches_pending`, `categorize_pending`, and `account_links_pending` (new).
  One "what needs my attention?" call now covers transaction matches, uncategorized
  transactions, and account-link decisions without a separate sweep per domain.

### Deprecated
- **`transactions_review` MCP tool** — use `review` instead. Registered as a
  deprecated alias with description starting with "DEPRECATED: use `review`";
  removed after one minor release.
- **`moneybin transactions review`** — use `moneybin review` instead. Emits a
  deprecation warning to stderr and delegates to the same implementation;
  removed after one minor release.

### Changed
- **`transactions_categorize_assist` renames `description_redacted`/`memo_redacted`
  to `description_scrubbed`/`memo_scrubbed`.** Behavior is unchanged and was
  always correct: merchant text is the categorization signal and is sent to
  the model in full; what is scrubbed is embedded PII such as account numbers
  in the memo. The old field names claimed descriptions were withheld, which
  was never true. The `categorize export` / `commit-from-file` file format
  carries the new field names.
- **Categorization stats split the `rule` bucket into `rule` and
  `merchant_map`.** `transactions_categorize_stats`'s `by_source` breakdown
  previously folded merchant-mapping writes into `by_rule`, so the count
  didn't reconcile with the rules list. The persisted `categorized_by` value
  is unchanged — this is a reporting-only split.
- **Outside a repo checkout, `moneybin mcp install` now writes a config that runs
  the published package, pinned to the installed version.** The generated client
  entry uses `uv tool run --from moneybin==X.Y.Z` instead of pointing at a local
  checkout. The pin is deliberate: MoneyBin runs forward-only schema migrations
  when it opens your database, so an unpinned config would let a newly released
  version install itself on the client's next restart and migrate your encrypted
  ledger with no action from you. Re-run `moneybin mcp install` to move to a newer
  version. (#316)
- **MCP client guide corrected against what the clients actually do.** The Claude
  Desktop section now leads with `.mcpb` desktop extensions as the vendor-blessed
  path (config-file JSON is legacy-but-supported; MoneyBin's own bundle is still
  M3B), and documents two failures that look like bugs but aren't: Cowork's *remote*
  sessions can never see a local MCP server, and managed-org policy flags
  (`isLocalDevMcpEnabled`, `isDesktopExtensionEnabled`) can disable local MCP
  outright. At that point, MoneyBin's **then-105-tool registry exceeded
  Cascade's hard 100-active-tool ceiling** — Windsurf gives no signal when tools are
  dropped, so users had to disable some by hand. The later M3K.2 cut established
  a 47-tool standard registry. The Gemini CLI section explains why
  MoneyBin never sets `trust: true` (it bypasses *all* tool-call confirmations, and
  our surface includes write tools). (#315)
- **Accepting a link merge now requires a human confirm on every surface.** The
  account, merchant, and security link tools (`accounts_links_set`,
  `merchants_links_set`, `investments_securities_links_set`) gate the accept
  branch behind an MCP elicitation naming both entities being fused; a client
  that cannot elicit is directed to the CLI rather than allowed to proceed.
  These proposals are raised precisely *because* identity resolution could not
  bind unambiguously, so accepting one is never a decision an agent should make
  alone. Accept and reject are now explicit rather than inferred from whether a
  target id was supplied. (#318)

- **`core.dim_categories` gains an accounting `class` (M1V).** Every category
  now carries `class` (`income` | `expense` | `transfer` | `debt`), assigned
  at curation time for seed categories and defaulting to `expense` for user
  categories. Unlocks income-statement separation and transfer-exclusion from
  spend reporting.
- **Inbox-sync pending entries now carry their account proposals in the response
  envelope.** Each `account_confirmation` entry returned by `import_inbox_sync` /
  `moneybin import inbox` now includes `account_proposals[]` (source key,
  proposed account, and the candidate pick-list) directly in the response, not
  only in the on-disk `.pending.yml` sidecar. A REST/MCP/CLI-JSON caller can now
  render the pick-list and bind an account without reading the sidecar off disk;
  the CLI human-readable output lists the candidate accounts inline instead of
  pointing at the sidecar.
- **`Database.__init__()` and `get_database()` now require `read_only` as a
  keyword-only argument.** The prior `read_only: bool = False` default is
  removed; every call site declares intent explicitly. This is the physical
  enforcement that complements the SQL allowlists at MCP/CLI boundaries —
  read surfaces open with `ATTACH ... READ_ONLY`, not just by convention.
  Internal API change only; no external callers. See
  [`docs/specs/database-writer-coordination.md`](docs/specs/database-writer-coordination.md)
  and [ADR-010](docs/decisions/010-writer-coordination.md).
- **GSheet alias limit tightened from 63 to 56 chars** (#228) so the
  generated `gsheet_<alias>` view name fits DuckDB's 63-char identifier
  limit. A pre-existing gsheet connection with a 57–63 char alias will
  now raise a clear error on the next `gsheet pull` telling the user to
  reconnect with a shorter alias. Connections with aliases ≤56 chars are
  unaffected.
- **`raw.gsheet_*` and `raw.pdf_*` views: lifecycle columns now `_`-prefixed** (#228).
  System carry columns surface as `_loaded_at`, `_row_number`,
  `_deleted_from_source_at`, and `_page` (instead of the bare names) so
  they can never collide with normalized user headers from the source
  data (e.g. a PDF "Page" column or a Google Sheet "row_number"
  column). Existing `raw.gsheet_<alias>` views regenerate on next
  `gsheet pull`; queries referencing the old names need updating to the
  underscored form. Pre-launch — no migration path.
- **`medium`-confidence tabular imports now gate on confirmation** instead of waving
  through with a sign-convention log warning. Callers receive a `confirmation_required`
  envelope (MCP / `--output json`) or an interactive prompt (TTY CLI). Closes the
  spec-vs-code drift `smart-import-tabular.md` already promised.
- **`gsheet connect --column-mapping` is now partial-merge.** Only the destination
  fields you name are overridden; unspecified fields fall back to the detected mapping.
  Previously the flag replaced the entire mapping — a behavior change to a shipped
  surface. Confidence bands are aligned to `ImportSettings.confidence`.
- **`moneybin import files <single-file>` exits 1 on per-file failure** when no
  per-file knobs are passed. Previously the single-file path used the batch
  soft-fail behavior and exited 0 even when the lone file failed; it now mirrors
  the fail-loud single-file contract so scripts and agents see the failure.
  Pre-launch behavior change — no users affected.
- **Report CLI flags auto-derive from parameter names.** With reports now
  generated from runner signatures, multi-word flags follow the parameter name:
  `moneybin reports cashflow`/`spending` use `--from-month` / `--to-month`
  (replacing the bespoke `--from` / `--to`). Tool/command names are unchanged.
  The `data` payload for the six view-backed reports is now a bare array of
  result rows (the standard envelope shape) instead of the previous typed
  `{rows: [...]}` wrapper — a pre-launch normalization; no other tool exposed
  report rows.
- **Pending-match output now groups copies of the same transaction by component.**
  `transactions_matches_pending` (MCP) and `moneybin transactions matches pending` (CLI)
  enrich each pending dedup row with a `component_key` — the lexicographic MIN packed
  member key of its connected component across all active+pending dedup edges. Edges
  belonging to the same N-way cluster share one `component_key`; the CLI groups them
  into one display block per cluster. Transfer rows are ungrouped (`component_key =
  match_id`). The `actions[]` summary hint reports the edge-to-group ratio.
- **The lock-error string classifier in `_attach_encrypted`** now matches DuckDB
  1.5.3's `"Could not set lock on file"` in addition to the legacy 1.5.2
  `"Conflicting lock"` and `"different configuration"` strings.
- **The default `max_wait` on `get_database()` is now `10.0` seconds** (was 5.0)
  to match the policy ceiling documented in `database-writer-coordination.md`.

### Removed
- **`core.dim_categories.plaid_detailed` (M1V).** The single-aggregator
  category tag is replaced by `core.bridge_category_source_map`, which
  supports multiple providers and guarantees a deterministic one-row-per-code
  reverse lookup.
- **`reports_budget` MCP tool and `reports budget` CLI command.** They
  synthesized from `BudgetService` rather than reading a `reports.*` view,
  violating the `reports_*` = reads-a-view convention; they return through the
  report framework once a `reports.budget` view ships (M3C). `BudgetService`
  and the `budget_*` mutation tools are unaffected.
- **`reports health` CLI stub** — an unimplemented placeholder with no backing
  spec.
- **`sync.enabled` config field.** It was seeded into every profile's
  `config.yaml` and shown by `moneybin profile show` but never read — sync
  gating is server-side. Existing `config.yaml` files keep working (the stale
  key is ignored).

### Fixed
- **An installed MoneyBin could not create a profile or run a transform.** The
  built wheel shipped none of the SQL schema, migrations, SQLMesh models, or
  synthetic demo data it needs at runtime — the `package-data` globs pointed
  outside the package directory, which setuptools silently ignores. The SQLMesh
  project now lives inside the package (`src/moneybin/sqlmesh/`), every runtime
  resource ships in the wheel, and the packaged contents are verified against the
  real built wheel. (#316)
- **PDF statements with no ruled table no longer import zero transactions.**
  Recipe derivation picked its transaction table from `pdfplumber`'s table
  detection, which only fires on *drawn ruling lines* — while the recipe
  executor reads the document's text lines. Real bank statements are typeset
  with whitespace-aligned columns and no rules, so derivation went blind on
  exactly the input the executor consumes: a real Chase statement with a clean
  `ACCOUNT ACTIVITY` section extracted **0 transactions** — its rows either
  landed in an opaque seed table or, for a statement with no ruled content
  anywhere, failed outright with "No tables extracted from PDF". Derivation now
  falls back to reconstructing the table from text lines using the same column
  splitter the recipe executes with. Statements already imported as seeds will
  import correctly on re-import. (#313)
- **Credit-card statements no longer import their charges as income.** The PDF
  importer assumes "negative = expense" for every single-amount-column layout —
  the deposit-account convention — and its only safeguard was "does this
  statement contain a negative amount?" A card statement carries the opposite
  convention (charges positive, payments negative), and almost always has a
  payment or refund row, so it sailed through that check and every charge was
  booked as **income**. Reconciliation could not catch it: it sums the raw signed
  amounts, which tie out to the balance change with the signs exactly backwards.
  The importer now reads the statement's own disclosures (minimum payment, credit
  limit, APR) instead of guessing at its arithmetic, and hands a card statement to
  the AI agent rather than importing it under the wrong convention. Signs cannot
  be inferred from the amounts alone — a checking statement and a card statement
  have identical sign distributions. This also closes the same hole on the
  saved-format replay path, which ran before derivation and skipped the guard
  entirely. (#313)
- **CSV/Excel imports no longer silently drop legitimately identical rows.**
  Transaction ids for sources without a native id are content hashes, so two
  genuinely distinct same-day purchases with the same amount and description
  (two $5.00 coffees at one shop) hashed identically and the staging dedup
  dropped one — real transactions, gone, with no error. The second and later
  rows of identical content now carry an occurrence suffix, matching the scheme
  PDF transaction ids already used. Ids of rows that were never colliding are
  unchanged, so **re-importing an affected file recovers the dropped rows** and
  leaves everything else alone. (#313)
- **PDF statements sharing a filename no longer eat each other's rows.** Seed
  rows were keyed on `(alias, page, row index, content)`, and the alias is just
  the filename stem — so `2024-01/chase.pdf` and `2024-02/chase.pdf` collided,
  and a recurring charge landing at the same row index in both months (an
  identical subscription line) was silently discarded from the second statement.
  The row key now includes the document's content identity. This re-keys existing
  `raw.pdf_seeds` rows: revert an affected PDF import (`moneybin import revert
  <id>`) before re-importing it, or the statement is seeded twice. (#313)
- **A PDF the importer can't parse now reaches the AI agent instead of being
  buried.** Every recipe-derivation failure reported the same reason
  (`no_transaction_table`), which is excluded from agent escalation on the
  grounds that the document isn't a statement at all. So a document that *was* a
  statement and merely defeated the parser was silently filed away as
  unparseable rather than handed to the AI agent that could read it — including
  the single most common bank layout (separate "Withdrawals" and "Deposits"
  columns), which the deterministic parser defers by design. Those now escalate.
  Genuinely non-transactional PDFs (a brokerage positions statement) are routed
  to the seed store as before, and so are statements in a number locale the
  importer cannot replay — escalating those would send your statement to an AI
  provider for a result it provably cannot use. (#313)
- **`mcp install --client chatgpt-desktop` now actually installs.** It printed a
  config snippet and told the user to "choose the local/stdio option" in ChatGPT's
  Connectors UI, calling that "the supported, authenticated path" — but it wrote
  nothing, so following the instructions got you nowhere. The ChatGPT desktop app
  **hosts Codex and shares its MCP configuration** ("The ChatGPT desktop app, Codex
  CLI, and IDE extension support MCP servers and share MCP configuration for the
  same Codex host"), so the command now writes the real `~/.codex/config.toml`
  entry — the same one `--client codex` writes, meaning one install serves the
  ChatGPT desktop app, the Codex CLI, and the IDE extension. It also names the
  restart step (ChatGPT → Settings → MCP servers → Restart) and warns that ChatGPT
  on the **web** cannot see a local server at all: that needs remote MCP (M3D). (#315)
- **MCP install snippets now pin the absolute `uv` path.** macOS clients launched
  from the GUI (Claude Desktop, Cursor) do not inherit the shell's `PATH`, so a bare
  `uv` in the generated config resolved to nothing and the server failed to start —
  surfacing to the user as an opaque client-side error. (#315)
- **Codex installs carry `startup_timeout_sec = 30`.** Codex defaults to 10s, but a
  cold `uv run` (building the environment on first launch) routinely takes 3–15s, so
  the very first connection was the one most likely to time out. (#315)
- **Net worth no longer drops accounts with older statements.**
  `core.fct_balances_daily` built each account's date spine only as far as *that
  account's* last balance observation, so on any later date the account simply
  vanished — and `reports.net_worth` sums the accounts present on a date. An account
  whose statement landed a week before another's therefore contributed nothing to
  the current net worth: a checking account with one January statement was absent
  from a December total. Every account is now carried forward to the newest known
  date, so net worth reflects each account's last known balance. Accounts that are
  genuinely gone are excluded by archiving them (`include_in_net_worth` / `archived`,
  already honored), not by silently ageing out. (#310)
- **First-run guidance points an unset-up profile at `profile create`.** When the
  active profile has never been set up, the "Database not found" message now
  recommends `moneybin profile create <name> --init-inbox` (which scaffolds
  config, database, and inbox) instead of `db init`, which would leave the profile
  unregistered — absent from `moneybin profile list`, with no inbox. A profile that
  *is* registered but has no database still points at `db init`, which is the
  correct verb there. (#310, #315)
- **`moneybin profile create` can now repair a half-made profile.** A profile
  directory with no `config.yaml` — left by a bare `moneybin db init`, a hand
  `mkdir`, or an interrupted delete — was previously a dead end: `profile create`
  refused on the directory's mere existence, `profile list` hid it, and it never
  got an import inbox, with no verb anywhere to finish it. `create` now completes
  such a directory in place (config, inbox, and a database only if one is absent —
  an existing database is never touched or rolled back) and reports that it
  completed rather than created it. `ProfileExistsError` now means "a *registered*
  profile exists", so re-creating a real profile still refuses. (#315)
- **An empty target no longer silently rejects a link-merge proposal forever.**
  On the account, merchant, and security link tools, an empty-string target id
  fell through a truthiness test and was recorded as a permanent REJECT, which
  identity resolution never re-proposes — so a malformed argument could
  permanently suppress a correct merge with no error to the user. Empty targets
  are now an input error. (#318)
- **Undoing the undo of an accepted link merge no longer fails.**
  `MerchantLinksRepo.repoint` and `SecurityLinksRepo.repoint` emitted their two
  audit rows in the reverse of their SQL order, so the undo engine's reverse
  replay re-inserted the new binding before restoring the old one — tripping the
  at-most-one-accepted-binding guard on a state the forward path never produces.
  Every merge redo failed deterministically, with a stack trace rather than a
  message. (#318)
- **Reversing a pending review decision no longer silently discards it.**
  `reverse()` on all four review-queue decision repos (`security_link`,
  `account_link`, `match`, `merchant_link`) checked only `reversed_at IS
  NULL`, so calling it on a still-`pending` row dequeued the item from the
  review queue with no accept or reject ever recorded — defeating the
  human-review guarantee those queues exist to provide. All four now refuse
  to reverse anything but an already-decided (`accepted`/`rejected`) row.
  `SecurityLinksRepo` also gained `repoint()` (replacing an in-place
  `rebind()`), preserving append-only binding history the same way
  `MerchantLinksRepo.repoint` already does. (#318)
- **`moneybin system doctor` now actually runs its SQLMesh invariant
  checks.** Every audit file under `sqlmesh/audits/` was missing `standalone
  TRUE`, so SQLMesh loaded them as generic audits — which only run when a
  model references them in its `audits (...)` property, and none did.
  `system doctor` had therefore been silently reporting zero SQLMesh
  invariants since they shipped, and three audits never executed. All
  audits are now `standalone TRUE` and run on every check; one revived audit
  (`fct_transactions_sign_convention`) was also corrected to stop flagging
  legitimate `$0.00` transactions. (#318)
- **OFX imports no longer silently drop transactions that share a duplicate
  FITID.** Some institutions (observed: Chase) reuse one OFX `FITID` for two
  distinct same-day transactions — a foreign purchase and its
  foreign-transaction fee. Because the raw primary key
  (`(source_transaction_id, account_id, source_file)`) and the OFX dedup window
  (keyed on `(source_transaction_id, account_id)`) both collapse the two rows —
  they always share `source_file` within one import — one of the two was silently
  dropped from the ledger. The extractor now disambiguates colliding FITIDs by
  content so both survive. New imports are correct going forward; to recover data
  **already** affected, revert the affected import (`moneybin import revert <id>`)
  and re-import the file — a plain re-import is not sufficient, because the
  forced-reimport write path upserts by primary key and leaves the stale pre-fix
  row in place. (#304)
- **`moneybin sync pull` no longer stuck-fails on a fully-materialized
  database.** Migration V032 issued `ALTER TABLE seeds.categories`, but on a
  database whose SQLMesh virtual layer is materialized that relation is a view —
  DuckDB rejects the ALTER, leaving the migration stuck and blocking every DB
  open. V032 now only rebuilds `app.user_categories`; the seed table's `class`
  column is owned by SQLMesh and derived by `refresh_views()`, so an upgraded
  database recovers automatically on the next run. (#306)
- **A second migration (V012) no longer stuck-fails on a fully-materialized
  database.** V012 ran `DROP TABLE IF EXISTS` over `seeds.merchants_global/us/ca` —
  former SQLMesh seed models that are views on a materialized database, where
  `DROP TABLE` on a view raises `CatalogException` (the same class as the V032 fix
  above). V012 now drops only the migration-owned `app.merchant_overrides` and
  leaves the seed relations to SQLMesh. A static test (`test_migration_schema_ownership`)
  now scans every migration and fails CI on any migration that writes a
  SQLMesh-owned schema. (#309)
- **`import_preview` surfaces header detection and row-count reconciliation.**
  Silent header-eating (a real data row mistaken for a header) was invisible in
  the preview envelope. The envelope now carries `has_header`, `skip_rows`, and
  `rows_in_file` (the reader's reconciled row accounting: `skip_rows + header +
  rows_read + rows_skipped_trailing`), plus `header_row_looks_like_data` — a
  flag when the row consumed as the header also parses as a transaction (raised
  for an explicit `skip_rows` that eats a data row, and for a headerless Excel
  sheet whose first row is a real transaction). When a red flag is present on an
  auto-detected (unknown) layout, detection `confidence` drops to `low`
  (previously a structurally-suspicious layout could still self-accept at
  `medium`), routing it to the propose→confirm gate instead of an agent
  auto-accepting a wrong mapping.
- **`moneybin system doctor` / `system_doctor` no longer hangs on a populated
  database.** Two integrity checks (the `transaction_categories` foreign-key
  check and the orphan `app.*`-state check) re-ran a correlated subquery
  against `core.fct_transactions` — an expensive merge/dedup/categorization
  view — once per row instead of once overall. Once `app.transaction_categories`
  held enough rows, a full doctor run could take over a minute (past the MCP
  30-second call cap) instead of the roughly 2 seconds it takes now; both
  checks are rewritten as a single anti-join. (#301)
- **Fixed stale command references in CLI hints and docstrings.** `make
  claude-mcp`'s remediation hints pointed at the pre-rename `mcp config
  generate --install` instead of `mcp install`; a synthetic-data reset hint
  pointed at a `moneybin db destroy` command that never existed instead of
  `moneybin profile delete`; and `DoctorSettings` docstrings referenced
  `moneybin doctor` instead of `moneybin system doctor`. (#291)
- **Sync credentials no longer collide across profiles.** Every profile now
  gets its own opaque profile id, and Plaid-broker keychain/token storage is
  scoped to it — previously every profile shared one token slot, so
  authenticating in one profile could affect another. Profiles created before
  this change get an id automatically on their next sync. (#279)
- **`moneybin sync pull` now advances the broker's sync cursor after every
  successful load.** The sync client never acknowledged a completed pull, so
  the broker's per-institution cursor never advanced and every `sync pull`
  re-fetched the same window from Plaid instead of only what's new —
  client-side dedup masked this as wasted work rather than duplicate data. The
  client now acks the broker once the pulled data is durable; a failed ack is
  best-effort and doesn't fail the pull. (#262)
- **A timed-out MCP write call could reset a different, healthy write's
  database connection.** When `tool_timeout_seconds` was configured below the
  database write-lock wait, a call that timed out before acquiring the lock
  could trigger a global connection reset that interrupted an unrelated,
  still-running write instead of only its own. The reset now targets only the
  timed-out call's own connection, and `MCPConfig` rejects a
  `tool_timeout_seconds` below the write-lock wait outright so the unsafe
  configuration can no longer be set. (#244)
- **SQLMesh state migrations now survive a dependency version bump.** After a
  SQLMesh upgrade, the in-process state migration wrote its bookkeeping to a
  throwaway in-memory catalog that vanished at process exit, so every subsequent
  `refresh`/`transform` failed with an opaque "local version ahead of remote"
  error with no CLI way to recover. The migration now targets the persistent
  database and verifies the state actually advanced before recording success;
  `moneybin db migrate status` reports SQLMesh state-vs-package drift, and
  `moneybin db migrate apply` repairs it. (#289)
- **The Plaid `sync link` flow no longer times out mid-approval.** The browser
  link-completion poll now allows 5 minutes (its own `_LINK_POLL_DEADLINE`,
  decoupled from the 120s `/sync/trigger` timeout), so completing a real bank's
  OAuth + MFA no longer aborts the link. (#282)
- **Bare single-account imports now elicit account confirmation instead of
  erroring (M1S.4 extension).** A single-account tabular file (CSV/TSV/Excel)
  imported with no account identifier — no `--account-name`/`--account-id`, no
  `account_bindings`, and no account-name column — previously failed with a
  `ValueError` (inbox: `failed/` with `needs_account_name`). It now returns the
  M1S.4 `confirmation_required` envelope (`reason="account_confirmation"`)
  carrying an account proposal, answered through the existing `import_confirm`
  account-binding channel (`account_bindings={source_key: account_id|"new"}` /
  `--account-binding`) or `--account-name`/`--account-id`. Inbox sync routes the
  file to `pending/` (recoverable) with an account-binding sidecar; the
  `needs_account_name` error code is retired.
- **Bare-import account gate now offers a pick-list instead of a dead end.** When
  a bare single-account file (no account number, no institution match) gates for
  `account_confirmation`, the proposal previously carried `candidates: []` — the
  confirmer was told to pick an account with nothing to pick from. The resolver
  now supplies a **fallback** candidate list (the institution-scoped existing
  accounts when the source's institution is known, otherwise all accounts, capped)
  so the human or agent can adopt an existing account directly. These fallback
  candidates are decision support only — they are never eligible for silent
  auto-adopt, and confirming "new" still mints a standalone account.
- **Confirmed pending files are now archived out of `pending/`.** A successful
  `import confirm` (`import_confirm` / `moneybin import confirm`) that ratifies a
  file sitting in `pending/` now moves the file to `processed/YYYY-MM/` and
  removes its `.pending.yml` sidecar, matching inbox drain semantics. Previously
  a confirmed file lingered in `pending/`, where a later sync could re-surface it.
- **Cross-source account linking now actually fires (M1S.7).** `core.dim_accounts.last_four`
  is now derived from each source's native field (OFX `<ACCTID>` digits, Plaid
  `mask`, tabular account number/label) instead of being NULL for every
  file-imported account. The account matcher's `institution + last4` bridge can
  therefore propose linking a CSV account to its OFX/Plaid twin — previously it
  only worked when forced with an explicit `account_bindings`. Weak matches stay
  review-only: an interactive import surfaces a confirmation, an agent import
  leaves a `pending` proposal in the account-link queue, and two accounts sharing
  a last 4 both surface for review rather than auto-merging. (#257)
- **Account display names now include the last 4 again (M1S.7).** File-imported
  accounts rendered as `Institution Type` with the last-4 fragment dropped
  because `last_four` was NULL; `core.dim_accounts.display_name` now shows the
  derived last 4 (`Institution Type …NNNN`). (#257)
- **Multi-account (Tiller-style) imports record each account's own institution
  (M1S.9).** For a multi-account exporter format with a per-row Institution
  column, every account now gets its own institution (which the cross-source
  bridge can use) instead of a single shared exporter/tool name stamped on all of
  them. (#258)
- **Saved tabular formats no longer store an account label as their institution
  (M1S.8).** An auto-saved format records its resolved (filename/format)
  institution or `unknown`, never the per-account `--account-name` — a format
  describes a column layout, not an account. (#258)
- **`refresh` now rebuilds materialized models after a data-only load.** A
  second import or sync pull (new `raw.*` rows, unchanged model SQL) left
  `core.dim_accounts` — the only `FULL` model — stale and `transforms_pending`
  stuck true, because the refresh drove SQLMesh with `plan` alone (which acts on
  model-definition changes, not data). `refresh`/transform `apply` now also runs
  SQLMesh data processing (`run`) and restates `FULL` models, so newly-pulled
  accounts appear and the pending flag clears.
- **Quieter refresh/import output.** The per-connection `Synced N privacy
  classification comment(s)` line dropped from INFO to DEBUG, and sqlglot's
  `REGEXP_REPLACE with non-literal position` transpile warnings (emitted several
  times per transform) are now suppressed within the SQLMesh boundary — neither
  is actionable signal for users or agents driving the CLI/MCP.

### Security
- **The agent-facing SQL connection can no longer reach remote filesystems.**
  Since DuckDB 1.4.1, the only supported encrypted-write path is the OpenSSL
  crypto inside the `httpfs` extension, which DuckDB silently auto-loads on the
  first encrypted write — leaving a live, unrestricted http/s3 filesystem on
  every MoneyBin connection, including the read-only handle MCP agents run SQL
  against, with nothing disabling it. MoneyBin now loads `httpfs` explicitly only
  where its crypto is needed, disables the HTTP and S3 filesystems on every
  connection, and locks that configuration on read-only connections so agent SQL
  cannot re-enable extension loading to pull in another remote filesystem. (#316)
- **The unauthenticated HTTP MCP transport is now gated behind `--insecure`.**
  `moneybin mcp serve` refuses to start any non-stdio transport (`sse`,
  `streamable-http`) unless `--insecure` is passed, exiting with a usage error
  that names the risk plainly. MoneyBin has no HTTP authentication yet, so a
  network transport would expose all financial data to anyone who can reach the
  port. With `--insecure` the server starts but prints a loud startup warning;
  stdio — the supported install path — is unaffected. Install docs and CLI help
  no longer present the unauthenticated HTTP path as a normal setup route.
  (#287)
- **CVE fixes via dependency bumps.** `cryptography`, `pydantic-settings`, and
  `python-multipart` bumped to clear 5 CVEs; `joserfc` pinned for a transitive
  `authlib`/`fastmcp` CVE. Four starlette CVEs remain suppressed in
  `pip-audit` — the fix requires starlette 1.x, unreachable while
  `sqlmesh[lsp]` pins `fastapi==0.120.1` — and aren't exposed on MoneyBin's
  stdio-only MCP transport. (#280)

### Added
- **PDF import (seed path).** Native-text PDFs import via `moneybin import <file.pdf>` and the inbox; their tables land as a queryable JSON seed (`raw.pdf_seeds`) with an auto-generated typed view (`raw.pdf_<alias>`), reversible like any import. Mapping PDFs to transactions/core is a later phase.
- **Report auto-generation framework — one runner generates every surface.**
  A report is now a single decorated runner (`@report`) that returns a
  parameterized query against its `reports.*` view; the framework introspects
  its signature and docstring to generate the MCP tool, CLI command, and
  `TableRef` wiring, and at call time executes → classifies each output column
  via the report's declared `classes` map (ADR-013) → masks CRITICAL columns →
  builds the envelope. The six view-backed reports (cashflow, spending,
  recurring, merchants, large-transactions, balance-drift) now run through it;
  their query logic and results are unchanged (the `data` envelope shape is
  normalized — see Changed). Packages contribute reports the same way.
- **Audit-log undo consumer.** `system_audit_undo`, `system_audit_history`, and
  `system_audit_get` MCP tools (plus `moneybin system audit undo|history|get`
  CLI parity) make any audited `app.*` mutation reversible as a unit keyed on
  `operation_id`. Each row's inverse is synthesized from its full audit
  before/after image and routed back through the `*Repo` layer; the undo is
  itself audited (`is_undo`/`undoes_operation_id`) and undoable. Block-don't-
  cascade: when a later operation modified the same rows, undo refuses with
  `undo_cascade_blocked` and returns the blocker operations to walk explicitly,
  rather than silently reversing unrelated later work. Notes, tags, and splits
  mutations are now routed through dedicated repos so every annotation is
  undoable. See
  [`docs/specs/data-recovery-contract.md`](docs/specs/data-recovery-contract.md).
- **`sql_query` MCP tool resolves each output column's data class via SQL lineage.**
  sqlglot parses the query, expands `*` against a migration-version-keyed schema
  snapshot, and maps every output column to the `DataClass` it derives from in
  `core.*` / `app.*`. Aggregations follow settled tier rules: `COUNT(*)` /
  `COUNT(DISTINCT col)` → LOW aggregate; `SUM`/`AVG` preserve the source class;
  `MIN`/`MAX` preserve the source class; multi-column expressions take the
  max-tier class; unresolvable projections fall back conservatively to the
  max-tier input class. Data queries reached the `core`/`app` schemas only when
  this shipped; `DESCRIBE`/`SHOW` run as low-sensitivity metadata. Two entries
  in this same release supersede both statements: "Read the ingestion pipeline
  through `sql_query`" for the schemas, and the `PRAGMA storage_info` redaction
  fix for the statements.
- **`moneybin sql query` CLI command — the privacy-safe ad-hoc SQL path.** Full
  CLI↔MCP parity with `sql_query`: both surfaces route through one shared
  `execute_sql_query` primitive (read-only gate, schema restriction,
  sqlglot lineage, CRITICAL masking), so the CLI masks account/routing numbers
  identically and raw SQL is not a privacy bypass on either surface. `--output
  text|json` returns the same envelope shape as MCP. `moneybin db query`/`db
  shell`/`db ui` remain raw, unmasked operator access and point here via their
  banner.
- **N-way dedup collapse.** Three or more copies of the same transaction now
  collapse to a single record even when the duplicates span sources *and*
  overlapping within-source files (e.g. two CSV exports plus one OFX download
  of the same statement). A union-find spanning forest groups every transitively
  linked duplicate into one connected component, so chained matches (A=B, B=C)
  resolve to one gold record instead of leaving a stray copy behind.
- **Agent/CLI-callable `transactions matches pending`.** Lists pending matches
  grouped by component (copies of the same transaction cluster together),
  mirroring the `transactions_matches_pending` MCP tool. Closes the CLI gap where
  `transactions review --type matches --status` only reported counts, never rows.
- **Agent-callable transaction match accept/reject.** `transactions_matches_set` and
  `transactions_matches_pending` MCP tools (plus `transactions_matches_run` /
  `transactions_matches_history`), `moneybin transactions matches set`, and
  non-interactive `transactions review --type matches --confirm/--reject/--confirm-all`.
  Agents and scripts can now accept or reject pending dedup/transfer proposals without
  the interactive review queue; only `pending` decisions are settable, and rejecting an
  already-accepted match surfaces a recovery action pointing at `moneybin transactions
  matches undo`.
- AI consent ledger: `moneybin privacy grant/revoke/revoke-all/status/log` CLI
  commands and `privacy_consent_grant`, `privacy_consent_revoke`,
  `privacy_status`, `privacy_log` MCP tools, backed by the new
  `app.ai_consent_grants` table. Records which AI feature categories you've
  authorized for which backend, with paired audit-log entries. (#210)
- **`moneybin system doctor` app-state integrity checks.** Doctor verifies that every recent mutation of a protected `app.*` table has a paired `app.audit_log` row, plus per-table foreign-key and uniqueness checks; a `--full` flag scans every row instead of the default sampled, recent-only window (`doctor.audit_coverage_lookback_days` / `doctor.audit_coverage_sample_cap` settings). The app-state audit-routing layer routes every protected `app.*` write through a `*Repo` so it pairs with an audit-log row in the same transaction, rolled out per table: category taxonomy and per-transaction categories, merchant mappings, categorization and proposed rules, account settings, balance assertions, and budgets (`accounts set` / `accounts balance assert` / `budget_set` previously bypassed audit), and the "edge" writers outside the service layer — saved tabular-format profiles (`app.tabular_formats`), match decisions (`app.match_decisions`), and import labels (`app.imports`). FK checks resolve `proposed_rules → categorization_rules`, `transaction_categories → core.fct_transactions`, `account_settings`/`balance_assertions` → `core.dim_accounts`, `budgets` → `core.dim_categories`, and `match_decisions` → `core.dim_accounts`. Formally Invariant 10; see [`docs/specs/app-integrity-invariant.md`](docs/specs/app-integrity-invariant.md).
- **Google Sheets as a live tabular source (M3F).** New `moneybin gsheet` CLI subgroup and `gsheet_*` MCP tools support connecting a Google Sheet via direct OAuth (Google "Desktop app" PKCE flow — no shared client secret). Two adapters at connect time: `transactions` (Tiller-style ledger → matching, categorization, and reports pipeline) and `seed` (catch-all for any sheet → JSON storage in `raw.gsheet_seeds` plus an auto-generated typed view queryable via `sql_query` and `moneybin://schema`). Every `refresh_run` re-pulls connected sheets; live mirror with `deleted_from_source_at` soft-delete preserves audit history; per-connection drift detection refuses pulls on structural change until `gsheet reconnect`. New `app.gsheet_connections` + `raw.gsheet_seeds` tables; `deleted_from_source_at` column added to `raw.tabular_transactions` (V019). See [`docs/specs/connect-gsheet.md`](docs/specs/connect-gsheet.md) and the [Google Sheets guide](docs/guides/connect-gsheet.md).
- **`transactions_categorize_run` MCP tool + `moneybin transactions categorize run` CLI command.** Run the categorization engine cascade (rules + merchants) over uncategorized transactions. Fills the gap where adding a merchant mapping previously had no agent-callable path to re-sweep — the only re-trigger path was `transactions_categorize_rules_create(reapply=True)`, which only fires during rule creation. Methods cascade in order; a rule write blocks a merchant write at the same priority. The `"ml"` literal value will be added when ML categorization implementation lands.
- **`moneybin transactions categorize assist` CLI command.** Produces the same redacted records for LLM categorization that the MCP tool returns. Service-layer enforces the redaction contract, so the CLI inherits it — both surfaces are first-class agent paths.
- **`categories_delete` MCP tool + `moneybin categories delete` CLI command.** Hard-delete a user-created category. Refuses by default if the category is referenced by transactions or budgets; `--force` / `force=True` cascades by deleting referencing rows (affected transactions return to uncategorized). Default (seeded) categories cannot be hard-deleted — disable them via `categories_set` instead. Errors map to `CATEGORY_NOT_FOUND`, `CATEGORY_IS_DEFAULT`, and `CATEGORY_HAS_REFERENCES`.
- **`refresh` umbrella across MCP and CLI** — `refresh_run` MCP tool and `moneybin refresh` CLI command are the always-visible entry points for the refresh domain (matching → SQLMesh apply → categorization). Thin wrappers over `RefreshService.refresh()` (introduced in PR #151); both return the same response envelope. `actions[]` hints in `system_status`, `import_*`, and curation tools now point at `refresh_run` instead of the operator-territory `transform_apply`.
- **`moneybin transactions categorize rules create` and `... rules delete` CLI commands.** Closes the CLI-side parity gap for rule lifecycle — MCP counterparts `transactions_categorize_rules_create` and `transactions_categorize_rules_delete` already existed. `create` supports both single-rule (`NAME --pattern X --category Y`) and batch (`--from-file rules.json`) modes; both `create` and `delete` accept `--reapply` to re-evaluate previously-categorized rows. `--output json` returns the same envelope shape as the MCP tools.
- **Agent-experience fixes across the MCP surface.** A new `ValidationErrorMiddleware` converts raw `pydantic_core.ValidationError` on bad kwargs into a standard response envelope with `error.code="invalid_arguments"` and a hint listing accepted parameter names. `reports_networth`, `reports_networth_history`, `reports_spending`, and `reports_cashflow` now populate `actions[]` with concrete next-step suggestions. New `.claude/rules/agent-experience.md` requires an agent-experience report whenever a session touches the MCP server. (The companion `moneybin_discover` no-args enhancement from this batch was superseded by the disclosure-retirement entry below in the same Unreleased cycle.)
- MCP transform tools — `transform_status`, `transform_plan`, `transform_validate`, `transform_audit` — wrap a new `TransformService` and replace the previous CLI-only surface. (`transform_apply` initially shipped here too but has since been folded into `refresh_run(steps=["transform"])` — see Removed.) See [smart-import-transform.md](docs/specs/smart-import-transform.md).
- `system_status` envelope `data.transforms` block (`pending`, `last_apply_at`) plus a `refresh_run` action hint when derived tables are stale.
- Boot-time schema-drift check: when `core.dim_accounts` or `core.fct_balances_daily` is missing expected columns, the MCP server now runs one synchronous `transform apply` self-heal attempt before raising. Closes the chicken-and-egg where the recovery tool lived inside a server that wouldn't start. `system_status` envelope surfaces a `data.schema_drift` block when drift is observed at query time. (PR #146)
- `IMPORT_BATCH_SIZE` Prometheus histogram.
- `--output json` on `moneybin transform {plan,apply,status,validate,audit}` returning the MCP envelope shape.
- **Plaid sync (M3A Phase 1):** new `moneybin sync` CLI subgroup and corresponding MCP tools (`sync_pull`, `sync_status`, `sync_link`, `sync_link_status`, `sync_disconnect`, `sync_review` prompt). Pulls accounts, transactions, and balances from Plaid-connected banks via moneybin-sync, loads into `raw.plaid_*` tables, and flows through SQLMesh staging (with sign-convention flip) into `core.fct_transactions` and `core.dim_accounts`. See [`docs/specs/sync-plaid.md`](docs/specs/sync-plaid.md).
- `ResponseEnvelope`-based responses (all MCP tools and CLI `--output json` commands) now include a top-level `status` field (`"ok"` or `"error"`), giving agents a consistent signal without testing for presence of the `error` key. **Breaking change:** all `--output json` success responses now use `{"status":"ok","data":...}` instead of per-command `{"key":...}` shapes. (PR #128)
- `--json-fields` field-projection added to `moneybin transactions list` as the reference implementation (shared `json_fields_option` + `render_or_json` infrastructure; other read-only commands will adopt progressively). Comma-separated projection: `moneybin transactions list --output json --json-fields transaction_id,date,amount`.
- Shell completion enabled: `moneybin --install-completion` and `moneybin --show-completion` now work.
- Structured JSON error envelopes: when `--output json` is active, runtime errors (DB locked, file not found, etc.) emit a machine-readable error envelope to stdout instead of plain stderr text.
- `moneybin doctor` command — read-only pipeline integrity check that runs SQLMesh named audits (FK integrity, sign convention, transfer balance), dedup reconciliation (verifies raw→core row collapse is fully accounted for by recorded dedup decisions), and categorization coverage. Exits 0 on pass/warn, 1 on fail. Supports `--verbose` for affected IDs and `--output json` for agent consumption. Registered as `system_doctor` MCP tool.
- `transactions_get` MCP tool: primary transaction read with account/date/category/amount/description filters, curation fields (notes, tags, splits), and opaque cursor pagination.
- `moneybin transactions list` CLI command with the same filter surface as `transactions_get`; supports `--output text|json`.
- MCP tool decorator now emits protocol-standard `ToolAnnotations` (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`). Clients can render confirmation UI for destructive operations.
- Decorator-level cap on list-typed tool parameters via `MCPConfig.max_items` (default 500). Exceeding the cap returns `ResponseEnvelope.error` with `code="too_many_items"`.
- `accounts_resolve` MCP tool and `moneybin accounts resolve "<query>"` CLI command — fuzzy-matches free-text references to an `account_id`.
- **`reports.*` SQLMesh views.** Eight curated presentation models — `net_worth`, `cash_flow`, `spending_trend`, `recurring_subscriptions`, `uncategorized_queue`, `merchant_activity`, `large_transactions`, `balance_drift` — back the `moneybin reports *` CLI surface and `reports_*_get` MCP tools. Inaugurates the read-only `reports.*` schema per `architecture-shared-primitives.md`.
- **`moneybin reports recurring`, `merchants`, `uncategorized`, `large-transactions`, `balance-drift`.** New CLI subcommands powered by the recipe library; pair with `--output json` for AI consumers.
- **Transaction curation surface (M2A).** Multi-note threads (`transactions_notes_add/edit/delete/list` MCP tools and `moneybin transactions notes` CLI commands), free-form tags with rename/global rename, split-transaction support (one transaction → many `core.fct_transaction_lines`), manual-entry transactions (`raw.manual_transactions` flowing through staging into `core.fct_transactions`), and a unified `app.audit_log` capturing every curation mutation with row-level + audit-row transactional atomicity. V007 schema migration. (PR #120)
- **LLM-assist categorization workflow.** `transactions_categorize_assist` MCP tool produces a redacted view of uncategorized rows (description normalized, amounts/dates/accounts excluded) for an LLM to propose `(category, subcategory, canonical_merchant_name)`; the LLM persists results via the commit tool. Service-layer enforces the redaction contract so any future surface inherits it. (PR #116)
- **Privacy DataClass registry surfaced in DuckDB column comments.** Every `core.*` and `app.*` column is classified (e.g. `IDENTIFIER`, `AMOUNT`, `DESCRIPTION`, `MERCHANT`), and the classifications sync into DuckDB `COMMENT ON COLUMN` annotations on schema init so SQL clients and MCP `sql_schema` see the classification inline. (PR #169)
- `CHANGELOG.md` (Keep-A-Changelog format) with M0/M1 history backfilled from PR titles.
- `docs/guides/threat-model.md` — one-page user-facing distillation of `privacy-data-protection.md`. What encryption protects against; what it doesn't (forgotten passphrase, malware, AI vendor data flow).
- `docs/architecture.md` (placeholder pointing forward to `architecture-shared-primitives.md` at M2B).
- `docs/audience.md` — who MoneyBin is for, today and at launch.
- `docs/roadmap.md` — milestone status (M0 through M3E + post-launch). Replaces the in-README roadmap matrix.
- `docs/features.md` — capability snapshot with per-feature guide links. Replaces the in-README "What Works Today" table.
- `docs/comparison.md` — wider 8-way competitor comparison and tier framing.
- `docs/licensing.md` — why AGPL, what it does and doesn't mean.
- `pyproject.toml` PyPI-publish-ready metadata (description, classifiers, URLs, keywords). Bumped setuptools floor to ≥77.0 for PEP 639 license metadata.

### Changed
- **Public project documentation and branding refreshed.** The README, roadmap,
  and public technical-reference index now focus on the local CLI, SQL, and MCP
  workflows available today, with clearer navigation and updated project marks.
  (#323)
- **`sql_query` now reports per-query sensitivity instead of a fixed tier.**
  `summary.sensitivity` reflects the highest-tier data class present in the
  actual output columns (e.g. `"low"` for a pure `COUNT(*)` aggregate,
  `"critical"` when an account-identifier column is projected). Previously the
  tool always reported a static `"high"` tier via `unclassified=True`. An agent
  branching on the `sql_schema` unknown-table error code must update: it is now
  `sql_unknown_table` (was the bare `unknown_table`).
- **Refresh now surfaces matcher/categorizer crashes (M2D PR 6).** `refresh_run` and `moneybin refresh` previously swallowed best-effort matching/categorization failures at DEBUG, so a partial pipeline (cross-source dupes accumulating, rows left uncategorized) looked healthy. `RefreshResult` gains `matching_error`, `categorization_error`, and a `self_heal_actions` list; the response envelope now carries structured `recovery_actions` (targeted `refresh_run(steps=[…])` retry plus a `system_doctor` diagnostic) when a step crashes. Real crashes log at ERROR; a first-load missing-view precondition stays a quiet DEBUG so a fresh database's first refresh doesn't report a false failure. Best-effort crashes still don't abort the pipeline or fail the command.
- **Renamed CLI `sync connect` → `sync link` and MCP `sync_connect` → `sync_link`** (with `sync_connect_status` → `sync_link_status`). Establishes the verb-split formalized in `connect-gsheet.md`: `_link` for mediated providers (Plaid-style, server holds tokens), `_connect` for user-controlled storage (direct OAuth). The Plaid sync surface keeps Plaid's "Link" mental model users already recognize. Old names retained as deprecated aliases that warn and forward; will be removed in the next minor release.
- **Error code taxonomy renamed under prefix-grouped namespaces** (M2D PR 2 — data-recovery-contract foundation). Bare-string codes emitted by `classify_user_error` and the `@mcp_tool` decorator now use prefixed forms via the new `moneybin.error_codes` module. Renames an agent might be branching on: `database_not_initialized` → `infra_database_not_initialized`, `database_locked` → `infra_database_locked`, `wrong_key` → `infra_wrong_key`, `schema_drift` → `infra_schema_drift`, `file_not_found` → `infra_file_not_found`, `io_error` → `infra_io_error`, `invalid_input` → `infra_invalid_input` (read-path default; write callers should `raise UserError(code=MUTATION_INVALID_INPUT)` directly per the in-tree migration in PRs 9a–N), `not_found` → `infra_not_found` (read-path; same write-site override applies for `MUTATION_NOT_FOUND`), `too_many_items` → `infra_too_many_items`, `timed_out` → `infra_timed_out`, `sync_error` → `sync_error` (already prefixed). Agents matching code literals against the old strings must update to the new constants. The six recovery-contract prefixes (`import_*`, `mutation_*`, `audit_*`, `refresh_*`, `undo_*`, `recovery_*`) plus `infra_*` and `sync_*` for absorbed legacy codes are documented in `src/moneybin/error_codes.py` and `docs/specs/data-recovery-contract.md` Req 3.
- **AI Code Review now emits tiered findings.** Every inline comment and summary bullet starts with 🔴 **MUST FIX** (correctness / security / breaking / missing tests, gates merge), 🟡 **CONSIDER** (substantive quality: design, refactoring, potential bugs), or 🔵 **NIT** (small consistency issues: docstring formatting, naming drift). Contributors get a scannable severity signal; agent consumers (the `fix-review` skill) can dispatch by tier — fixing all tiers on early review iterations and recording lower-priority work for a later follow-up to avoid endless docstring-rewording cycles. See `CONTRIBUTING.md` § "Reading the AI review".
- **Metrics persistence: 5-minute background flush timer removed.** MCP sessions flush inside `close_db()`; CLI sessions continue to flush via `atexit` (registered conditionally on `stream="cli"` in `setup_observability`). The in-process Prometheus registry and `moneybin stats` CLI are unchanged. Future PRs will wire persistence into write transactions instead of polling.
- **Tabular CSV import: `--format chase_credit`, `--format citi_credit`, and `--format maybe` are no longer accepted** — those built-in format YAMLs were retired in favor of auto-detection, which handles the same shapes. Omit `--format` to let the detector run. As a consequence, `source_origin` for Chase/Citi/"Maybe" imports is now derived from `slugify(account_name)` instead of the format name; to preserve a stable origin across re-imports, pass `--account-name` explicitly (flows that rely only on `--account-id` will record `source_origin="unknown"`). Existing imports keep their historical `source_origin` values. (#181)
- **`transactions_categorize_stats` gains `include_auto: bool = False`.** Pass `include_auto=True` to get auto-rule health metrics (`active_auto_rules`, `pending_proposals`, `transactions_categorized`) alongside the base coverage stats in a single call. The standalone `transactions_categorize_auto_stats` MCP tool is retired; `moneybin transactions categorize auto stats` CLI remains.
- **`transactions_categorize_pending` absorbs `reports_uncategorized`.** New parameters: `sort: Literal["date","impact"] = "date"` (sorts by `ABS(amount) × age_days` when `"impact"`), `min_amount: Decimal = Decimal("0")`, `account: str | None = None` (accepts account ID or display name). Response is now richer — includes `age_days`, `priority_score`, `merchant_id`, `merchant_normalized`, `account_name`, `source_type`, `source_id` from `reports.uncategorized_queue`.
- **`reports_balance_drift` description** now leads with the question it answers: categorical drift-status view, one row per assertion. `accounts_balance_reconcile` description leads with threshold-filtered mismatch-by-day. Mutual disambiguation prose removed.
- **Reports surface: `merchant_id` propagated through `core.fct_transactions` and four `reports.*` views** (`merchant_activity`, `recurring_subscriptions`, `large_transactions`, `uncategorized_queue`). Views project `merchant_id` alongside `merchant_normalized`; aggregations GROUP/PARTITION on the FK. Transactions without a canonical merchant collapse into a single `(uncategorized)` bucket — same shape as the prior `(unknown)` text bucket, but FK-keyed. Closes the identifier-hygiene gap where a merchant rename in `app.user_merchants.canonical_name` silently re-bucketed historical aggregations.
- **`reports_uncategorized` and `reports_balance_drift` accept `display_name` or `account_id` for the `account` filter.** Resolution happens at the service boundary via the new `AccountService.resolve_strict`; ambiguous display-name matches raise `AmbiguousAccountError` (new `account_ambiguous` error code) and unknown references raise `AccountNotFoundError` (new `account_not_found` error code) instead of silently returning doubled or empty results. CLI `--account` help and MCP tool descriptions updated.
- **`app.proposed_rules.rule_id` now links proposal→active-rule** (V016 migration with one-time backfill from `app.categorization_rules` via `merchant_pattern` for approved 1:1 active-rule matches; inactive duplicates from prior override cycles are skipped so the active replacement wins, and genuinely ambiguous matches remain NULL). `approve()` writes the minted rule_id back to its source proposal; `check_overrides()` supersedes via `WHERE rule_id = ?` instead of `LOWER(merchant_pattern)`. Closes a latent bug where two approved proposals sharing a merchant_pattern would both be marked superseded.
- **Renamed MCP tool `transactions_categorize_apply` → `transactions_categorize_commit`** (and matching CLI subcommand `apply` → `commit`, `apply-from-file` → `commit-from-file`). The verb now matches the propose→review→commit workflow vocabulary documented in `transactions_categorize_assist` — the LLM proposes via `_assist`, the user reviews, and the LLM persists via `_commit`. `_apply` was historically overloaded with refresh-domain "apply transforms" (since retired in favor of `refresh_run`); the rename closes that ambiguity. Pre-launch posture: clean rename, no deprecation alias. Prometheus metric names retain the historical `apply` prefix (renaming would break downstream dashboards).
- **MCP read tools dropped the `_list` suffix** to match the noun-only convention for collection / summary / aggregate / time-series reads (shape 5 of `.claude/rules/surface-design.md`). Renames: `categories_list` → `categories`, `merchants_list` → `merchants`, `import_formats_list` → `import_formats`, `import_inbox_list` → `import_inbox_pending` (disambiguated from the CLI bare-callable `moneybin import inbox` drain), `system_audit_list` → `system_audit`, `accounts_list` → `accounts`, `accounts_balance_list` → `accounts_balances` (plural), `accounts_balance_assertions_list` → `accounts_balance_assertions`, `transactions_categorize_rules_list` → `transactions_categorize_rules`, `transactions_categorize_pending_list` → `transactions_categorize_pending`. Hard cut, no deprecation aliases (pre-launch posture per `design-principles.md`). CLI subcommands (`moneybin <group> list`) are unchanged — surface-idiom divergence is intentional. MCP clients with cached tool lists must call the new names.
- **`category_id` FK introduced across seven `app.*` tables** (`transaction_categories`, `budgets`, `user_merchants`, `transaction_splits`, `categorization_rules`, `proposed_rules`, `rule_deactivations`) referencing `core.dim_categories.category_id`. Writers dual-write FK + text; readers (`core.fct_transactions`, `core.fct_transaction_lines`, `core.dim_merchants`) prefer the FK-resolved name and fall back to the text snapshot for orphans. `categories_delete` now cascades across all six writer tables via FK; audit-trail rows in `rule_deactivations` retain unresolvable FKs intentionally. Migrations V014 (backfill all seven tables) and V015 (drop `UNIQUE (category, subcategory)` on `user_categories`). The text-column drop is tracked as Phase 2 follow-up work.
- **Accounts CRUD-to-set collapse.** `accounts_set` (MCP) and `moneybin accounts set` (CLI) now cover every settings field on an account. Three behavioral fields fold in: `display_name` (replaces `accounts_rename`), `include_in_net_worth` (replaces `accounts_include` / `accounts set --include/--exclude`), and `is_archived` (replaces `accounts_archive` and `accounts_unarchive` / `accounts set --archive/--unarchive`). Archiving still cascades `include_in_net_worth=False` atomically; unarchiving does NOT auto-restore include. Service-layer `archive`/`unarchive`/`rename`/`set_include_in_net_worth` survive as thin deprecation delegates for internal callers. Hard cut on the public surfaces — no deprecation aliases (pre-launch posture per `design-principles.md`).
- **MCP tool renamed:** `categories_toggle` → `categories_set`. Matches the `_set` verb established by `budget_set` and `accounts_set` for shape-1b partial-update tools. Same behavior (flip `is_active`); only the verb changes. CLI command renamed in lockstep: `moneybin categories toggle` → `moneybin categories set`. Pre-launch, no deprecation alias.
- **Tool descriptions updated** to document defended exceptions inline: `accounts_balance_assert` (shape-1b upsert despite verb-shaped name), `transactions_tags_rename` (multi-row global mutation despite singular-shaped signature), `transactions_notes_*` (lifecycle-with-id triad), `accounts_balance_reconcile` vs `reports_balance_drift` (per-day numeric threshold filter vs per-assertion-date categorical drift series).
- **MCP money amounts are now JSON numbers, not quoted strings.** `Decimal` fields in the response envelope serialize as JSON numbers (`219584.05`) instead of strings (`"219584.05"`). Internal `Decimal` precision is preserved; the wire format matches what agents and JSON tooling expect by default. `DECIMAL(18,2)` (amounts) and `DECIMAL(18,8)` (prices/quantities/FX) both fit inside float64.
- **`reports.spending_trend.year_month` and `reports.cash_flow.year_month` are now `'YYYY-MM'` strings**, not DATE truncated-to-first-of-month. The output column matches the input parameter format (`from_month`/`to_month`). Existing callers that pass `'YYYY-MM-DD'` still work — the service strips the day before comparison.
- **`reports_spending` and `reports_cashflow` default to the last 12 months** when both `from_month` and `to_month` are omitted, instead of returning every historical month. `actions[]` includes a hint for widening or shifting the window. Agents that need the full history pass an explicit `from_month`.
- **`sql_schema` defaults to a compact catalog** (table names + purposes + column counts) instead of dumping the full ~50KB schema doc. Pass `table='<schema.name>'` for one table's columns and example queries, or `table='*'` for the full document.
- **OFX descriptions are now HTML-entity-decoded at import.** `_decode_text_field` repeatedly applies `html.unescape` to `payee` and `memo` until stable, fixing double-escaped bank output (e.g. Wells Fargo's `AT&amp;amp;T` → `AT&T`). Existing already-imported rows stay as-is until re-import.
- **Refresh is now a top-level domain concept.** Introduced `moneybin.services.refresh.refresh(db) -> RefreshResult` — the post-load pipeline that runs cross-source matching, SQLMesh apply, and deterministic categorization on the current database state. `ImportService.apply_post_import_hooks()`, `_apply_post_import_hooks()`, and the `PostImportHookResult` dataclass are removed; callers (`ImportService.import_files`, `InboxService.sync`, `SyncService.pull`) now invoke `refresh()` directly. Matching and categorization were always source-agnostic; "refresh" names what they do without implying file-import provenance.
- **`moneybin sync pull` auto-runs refresh by default.** After a successful Plaid sync that changes raw state (loads new rows or processes removals), `SyncService.pull()` runs the refresh pipeline once before returning, so `core.dim_accounts` and other derived models reflect the new data immediately. Pass `--no-refresh` (CLI) or `refresh=False` (MCP `sync_pull`) to defer. SQLMesh failures surface as `transforms_applied=false` + `transforms_error` in the result envelope (raw rows stay durable, CLI exits non-zero so agents detect the stale state); matching and categorization are best-effort and log-only on failure. High-frequency callers should defer refresh and schedule it separately — SQLMesh apply dominates pull latency (typically 5–30s).
- **Renamed: `apply_transforms` → `refresh` everywhere.** CLI flags `--apply-transforms/--no-apply-transforms` are now `--refresh/--no-refresh` on `moneybin sync pull` and `moneybin import files`. MCP parameters `apply_transforms` on `sync_pull`, `import_files`, and `import_inbox_sync` are now `refresh`. Service kwargs on `SyncService.pull`, `ImportService.import_file`, `ImportService.import_files`, `InboxService.sync` follow the same rename. Result-envelope fields (`transforms_applied`, `transforms_duration_seconds`, `transforms_error`) keep their names — they describe the SQLMesh-step outcome specifically, which is the only step that surfaces a structured error.
- **Breaking:** MCP `import_file` renamed to `import_files`; accepts `paths: list[str]` and applies transforms once at end of batch. Per-file overrides (`account_name`, `institution`, `format_name`) are no longer exposed on the MCP surface — use the CLI for those.
- **Breaking:** CLI `moneybin import file PATH` renamed to `moneybin import files PATHS...`; the `--skip-transform` flag is replaced by `--apply-transforms / --no-apply-transforms` (default on).
- `moneybin import inbox` and the `import_inbox_sync` MCP tool route through the batch import path; transforms now run once per inbox drain instead of once per file.
- Replace long-lived database singleton with short-lived per-call connections (`get_database(read_only=True/False)`). Write connections retry on lock contention with exponential backoff; read-only connections coexist across processes. New exceptions: `DatabaseLockError`, `DatabaseNotInitializedError`. (#131)
- Renamed `moneybin mcp config generate --install` to `moneybin mcp install`. Default behavior writes the client config; `--print` opts out. Hard cut, no alias. `mcp config path` (lookup-only) is unchanged.
- Tool description audit: every existing `@mcp_tool` description was reviewed against the sign-convention, currency, and mutation-surface invariant rules. Missing invariants were appended; descriptions otherwise unchanged.
- `transactions_categorize_rules_create` (and `CategorizationService.create_rules`) is now idempotent. Each input is deduped against active rules by the matcher+output tuple `(merchant_pattern, match_type, min/max_amount, account_id, category, subcategory)`; `name` and `priority` are metadata and excluded from the key. A retry of the same payload returns the existing `rule_id`s and creates no new rows. The result envelope gains an `existing` counter alongside `created`/`skipped`. Same matcher with a *different* category output still creates a new row — rule-conflict detection is a deferred follow-up.
- Internal rename: `BulkCategorizationResult` → `CategorizationResult`, `bulk_categorize` → `categorize_items`, `validate_bulk_items` → `validate_items`. The "bulk" qualifier is dropped from MoneyBin's surface — list inputs are the default, not the exceptional case.
- Prometheus metric names renamed: `moneybin_categorize_bulk_items_total` → `moneybin_categorize_items_total`, `moneybin_categorize_bulk_duration_seconds` → `moneybin_categorize_duration_seconds`, `moneybin_categorize_bulk_errors_total` → `moneybin_categorize_errors_total`. External dashboards/alerts referencing the old names need updating.
- **Categorization matcher input extended** to memo and structural fields. The deterministic matcher and the LLM-assist redacted view now both consume `match_text = description + memo` plus `transaction_type`, `check_number`, `is_transfer`, `transfer_pair_id`, `payment_channel`, and `amount_sign`. Aggregator transactions (PayPal, Venmo, Zelle, generic ACH) match on the wrapped merchant identity in memo instead of failing on the truncated description. Pattern matching is per-field so user-authored `exact` and anchored-`regex` patterns continue to hit the original field when memo is present. (PR #122)
- **`categorize assist` / `categorize commit` JSON envelope** (then named `categorize apply`; see Changed) carries `transaction_id` as the per-row key (no separate opaque identifier). Export files produced by `categorize assist` flow back into the commit tool unchanged. (PR #122)
- **LLM-assist redaction contract expanded.** The redactor now runs over `memo` in addition to `description`, and structural fields (`transaction_type`, `check_number`, `is_transfer`, `transfer_pair_id`, `payment_channel`, `amount_sign`) are exposed to the LLM as signals. The no-amount / no-date / no-account guarantee is preserved. (PR #122)
- **`transactions_categorize_commit` triggers auto-fan-out** (then named `transactions_categorize_apply`; see Changed). After the batch commits, `categorize_pending()` runs once to apply newly-created merchants and exemplars to remaining uncategorized rows in the same dataset. The "snowball" the cold-start spec promised now works — by the third or fourth import, the LLM is meaningfully less involved. (PR #122)
- **Auto-created merchants accumulate exemplars instead of inventing patterns.** When LLM-assist categorizes a row and proposes a `canonical_merchant_name`, the system appends the exact normalized `match_text` to a `oneOf` exemplar set on the merchant — it no longer creates a `contains` pattern from the full normalized description. Aggregator strings like `PAYPAL INST XFER` no longer over-match across unrelated transactions. (PR #122)
- **Source-precedence enforcement on write.** All categorization writes route through a single guarded path that compares the incoming source's priority against the existing row's. A user manual edit (`'user'`) can never be overwritten by any subsequent rule, merchant, or LLM-assist run. The `categorized_by` column is the lock; no separate lock table. (PR #122)
- **`core.agg_net_worth` retired.** Net worth aggregation now lives at `reports.net_worth` (same SELECT body, new schema) per the `reports.*` convention introduced in `architecture-shared-primitives.md`. Existing `moneybin reports networth` commands and `reports_networth_*` MCP tools transparently repointed.
- **Per-row `updated_at` on `core.*` models.** `updated_at` is now the `MAX` of contributing per-row input timestamps (NULL where all inputs are model-level seeds), instead of `CURRENT_TIMESTAMP` set at SQLMesh refresh time — so `core.dim_accounts.updated_at` / `core.fct_transactions.updated_at` reflect actual row changes instead of looking new after every transform. Model-level freshness is exposed separately via `meta.model_freshness`, which wraps SQLMesh's `_snapshots`. Adds `updated_at` to `app.user_categories`, `app.user_merchants`, and `app.category_overrides`. See [`core-updated-at-convention.md`](docs/specs/core-updated-at-convention.md). (PR #141)
- **`app.categories` and `app.merchants` views retired.** The resolved-dimension views (seeds + user state + overrides) now live as SQLMesh-managed `core.dim_categories` and `core.dim_merchants`. Consumer code already routed through the `TableRef` constants; no API change.
- **Milestone taxonomy re-unified into phase-aligned milestones (2026-05-30).** Replaced the flat M0–M3F grid — where the numbers had stopped tracking the build sequence — with four phase milestones: **M0 Foundation, M1 Ingestion Core, M2 Analysis & Reports, M3 Productization & Distribution**, each with lettered increments (`M1J`) and `.N` work items, and each closed by a test-functionality gate. The phase *is* the gate, so testing batches at four milestones rather than per-increment. `docs/roadmap.md` carries the new scheme and the old→new mapping; dated CHANGELOG history keeps its original labels.
- **Milestone terminology unified.** Retired "Level 0/1" + "Wave 2A/2B/2C/Wave 3" dual systems for one consistent **milestone** convention: M0, M1, M2A, M2B, M2C, M3A, M3B, M3C, M3D, M3E, Post-launch. M3 decomposes into sub-milestones because it has parallel domain (Plaid/investments/multi-currency) and surface (Web UI/hosted) tracks. M3E closing = launch.
- **README significantly tightened** — from ~196 lines to ~115 lines. Storefront pattern: tagline preserved, status callout + Why-bullets + How-It-Works diagram + Quick Start + 5×5 ✓/✗ comparison + Documentation/Community/Contributing/License pointers. In-README roadmap matrix removed (lives in `docs/roadmap.md`); detailed feature inventory removed (lives in `docs/features.md`); 8-column comparison table replaced with tight 5×5 (full version in `docs/comparison.md`); License essay condensed (full rationale in `docs/licensing.md`).
- `.claude/rules/shipping.md` extended with the post-implementation checklist for `CHANGELOG.md`, `docs/roadmap.md`, `docs/features.md`. Documents what does and doesn't earn a CHANGELOG entry.
- `CONTRIBUTING.md` "Where the strategy lives" expanded to include the new docs and a one-line CHANGELOG rule.
- **Spec rename for surface symmetry.** `docs/specs/mcp-tool-surface.md` → `docs/specs/moneybin-mcp.md`; `docs/specs/cli-restructure.md` → `docs/specs/moneybin-cli.md`. Establishes the `moneybin-<surface>.md` naming pattern (extends to a future `moneybin-rest-api.md`). New cross-surface spec [`docs/specs/moneybin-capabilities.md`](docs/specs/moneybin-capabilities.md) maps user-facing capabilities to per-surface registered names; the `.claude/rules/mcp-server.md` "Surface change discipline" rule now requires every tool/command PR to update both the surface-specific spec AND the capabilities map. `git log --follow` works across the rename for history; bookmarks to the old paths should be updated.
- **Breaking — CLI/MCP naming pass (noun-only for query/read surfaces).** Applies the `mcp-server.md` "Tool Taxonomy" convention to ~14 tool/command name pairs that diverged between MCP and CLI. **Reports family (10 names):** MCP `reports_{networth,networth_history,spending,cashflow,recurring,merchants,uncategorized,large_transactions,balance_drift}_get` drop the `_get` suffix; MCP `reports_budget_status` → `reports_budget`. CLI counterparts: `reports networth show` → `reports networth`; `reports networth history` → `reports networth-history`; `reports {cashflow,spending,recurring,merchants,uncategorized,large-transactions,balance-drift} show` → leaf-only equivalents (each sub-app collapses). **Accounts:** CLI `accounts show` → `accounts get` (matches existing MCP `accounts_get`); MCP `accounts_settings_update` → `accounts_set` (matches existing CLI `accounts set`); CLI `accounts balance delete` → `accounts balance assertion-delete` (matches MCP `accounts_balance_assertion_delete`; clarifies scope — deletes the assertion row, not the balance). **Transactions:** MCP `transactions_review_status` → `transactions_review`; MCP `transactions_categorize_rule_delete` → `transactions_categorize_rules_delete` (plural matches sibling `_rules_create`). **Import:** MCP `import_list_formats` → `import_formats_list` (matches existing CLI `import formats list`). **System:** CLI `moneybin doctor` → `moneybin system doctor` (top-level leaf moves under the `system` group, matching MCP `system_doctor`). Shrinks the `tests/integration/test_surface_parity.py` name-drift backlog from 30 MCP-only + 57 CLI-only to 14 + 41 (32 fewer entries). Hard cut, no deprecation aliases (pre-launch posture per `design-principles.md`).
- **`refresh_run` MCP tool gains `steps` parameter; `moneybin refresh` CLI gains `--step` flag.** Optional `list[Literal["match", "transform", "categorize"]]` (MCP) / repeatable `--step` (CLI) scopes which sub-operations execute. Defaults preserved — `refresh_run()` and `moneybin refresh` still run the full cascade. Steps always execute in canonical order (match → transform → categorize) regardless of input order. Symmetric with `transactions_categorize_run(methods=...)`. Unknown step names raise `UserError(code="UNKNOWN_REFRESH_STEP")`.
- **`schema_drift.remediation` and `categories_list` action hints now point at `moneybin refresh`** rather than the operator-territory CLI form `moneybin transform apply`. Agents that hit schema drift or seeded-category gaps get pointed at the umbrella surface that's symmetric with `refresh_run`.
- **Tabular import no longer silently negates inverted-sign amounts.** When the running-balance check detects that amounts appear to be sign-inverted, amounts are imported as-is and a `⚠ Sign convention may be inverted` warning is emitted to stderr. Previously, MoneyBin auto-flipped the signs without notification. Re-run with `--sign` to override explicitly.

### Removed
- **MCP tool `transactions_categorize_auto_stats`** — folded into `transactions_categorize_stats(include_auto=True)`. CLI `moneybin transactions categorize auto stats` is unaffected.
- **MCP tool `reports_uncategorized` and CLI `moneybin reports uncategorized`** — folded into `transactions_categorize_pending` with `sort`, `min_amount`, and `account` parameters. `ReportsService.uncategorized_queue` removed; `CategorizationService.list_uncategorized_transactions` is the canonical path. **Migration note:** the previous tool always sorted by impact (`priority_score DESC`); the replacement defaults to `sort="date"` — pass `sort="impact"` to preserve the prior impact-ranked order.
- **MCP tools `accounts_rename`, `accounts_include`, `accounts_archive`, `accounts_unarchive`** — folded into `accounts_set`.
- **CLI commands `moneybin accounts rename`, `accounts include`, `accounts archive`, `accounts unarchive`** — folded into `moneybin accounts set` with new flags (`--display-name`, `--include/--exclude`, `--archive/--unarchive`, `--clear-display-name`).
- **Client-driven progressive disclosure retired.** Removed the `moneybin_discover` MCP meta-tool, the `MoneyBinSettings.mcp.progressive_disclosure` setting, and the `Visibility(False, tags=...)` server transform. The full registered tool surface is now visible at connect, with orientation delivered through the FastMCP `instructions` field and prefix-grouped tool names. Rationale: `tools/list_changed` client support is too uneven (Claude Desktop unreliable, most generic clients ignore) to design a portable disclosure mechanism around. The `@mcp_tool(domain=...)` decorator argument is preserved as dormant metadata. `moneybin://tools` resource shape simplified from `{core, extended, discover_tool}` to a flat `{namespaces}` list. Server `instructions` text trimmed from ~750 to ~180 tokens by dropping per-tool subsections already covered by tool descriptions. See `docs/specs/mcp-architecture.md` §3 "Tool disclosure: full surface, taxonomy-led".
- **MCP tools `budget_set`, `tax_w2`, `tax_deductions` and the `tax_prep` prompt de-registered** under the new stub-gating rule in `.claude/rules/mcp-server.md`. `budget-tracking.md` is `draft` (the former `budget_set` was only a partial slice of the planned set/status/delete + rollovers feature); there is no backing tax spec at all. The partial budget MCP adapter was removed rather than retained as a decorated or manually registerable dormant callback; `BudgetService` and `moneybin budget set` remain implementation foundations. Re-admit the complete budget lifecycle only when its backing spec reaches `in-progress` or `implemented`. Tracked in `moneybin-mcp.md` §17 "Dependency tracker".
- **W-2 PDF extraction removed entirely.** The `moneybin tax w2` CLI command, `tax_w2` MCP tool, W-2 extractor and loader, `raw.w2_forms` schema table, and `TaxService` are deleted. PDF parsing dependencies (`pdfplumber`, `pytesseract`, `pdf2image`, `pillow`) dropped from the package. The IRS form layout changes annually and LLM-mediated PDF parsing is likely a better primitive than pdfplumber/tesseract for tax data; architecture will be revisited in a future brainstorm. The `docs/specs/archived/w2-extraction.md` spec documents the removed design.
- **MCP tool `transactions_recurring_list`** — duplicate of `reports_recurring` which is strictly richer (confidence scores, cadence, status filter, annualized cost). Consumers using `transactions_recurring_list` should call `reports_recurring` instead. Removed as a duplicate surface.
- `transactions_search` MCP tool (superseded by `transactions_get`, which covers all its filters plus multi-account, multi-category, curation fields, and cursor-based pagination).
- **Seed merchant catalogs retired.** The `seeds.merchants_global/us/ca` SQLMesh seeds, paired `app.merchant_overrides` table, and `'seed'` value in the `categorized_by` precedence enum are removed. `core.dim_merchants` is now a thin view over `app.user_merchants`; all merchants are user-created or system-created on the user's behalf (LLM-assist, auto-rule, Plaid, migration). The original cold-start design layered a curated catalog as priority 7; it shipped as plumbing but the catalog was never populated. Cold-start now relies on Plaid pass-through (when synced) + migration imports + LLM-assist + the auto-rule snowball. V012 migration drops `app.merchant_overrides` on existing databases. Spec amendments in `docs/specs/categorization-cold-start.md` and `categorization-matching-mechanics.md`.
- **`transform_apply` MCP tool.** Folded into `refresh_run(steps=["transform"])`. The granular CLI command `moneybin transform apply` remains as the operator path; only the MCP surface was retired. Pre-launch posture — no deprecation alias. Clients with cached tool lists that call `transform_apply` will receive a tool-not-found error; replace with `refresh_run(steps=["transform"])`.
- **MCP tools `sync_schedule_set`, `sync_schedule_show`, `sync_schedule_remove` removed.** These were stubs returning `not_implemented` — no backing spec and no implementation. The schedule use case is tracked but unbuilt; these tools were surface noise. On `refresh_run` apply failure, the hint now points at `moneybin transform plan` (CLI) rather than the removed MCP tool.
- **MCP tools `transform_status`, `transform_plan`, `transform_validate`, `transform_audit` de-registered from MCP.** These SQLMesh introspection tools are operator territory (category 2, mcp-server.md "When CLI-only is justified") — hands-on developer tooling with no meaningful agent use case absent a code change. CLI commands `moneybin transform status|plan|validate|audit` are unchanged. Tool implementation files remain in place; only the MCP registration is removed.
- **MCP resources `moneybin://status`, `moneybin://accounts`, `moneybin://privacy`, `moneybin://tools`, `accounts://summary`, `moneybin://recent-curation`, `net-worth://summary` removed.** These seven resources duplicated data already reachable via tools and added context-window overhead on every connect. `moneybin://schema` is retained — it has unique composition value for SQL generation that no single tool replicates.

### Security
- **Account/routing-number columns in raw `sql_query` results are now masked,**
  closing the raw-SQL masking bypass. CRITICAL-tier columns
  (`ACCOUNT_IDENTIFIER`, `INSTITUTION_ACCOUNT_NUMBER`, `ROUTING_NUMBER`) are
  masked with the same transforms the typed tools apply (`****<last4>` for
  account numbers, `*****` for routing numbers) — `sql_query` is no longer a
  privileged escape hatch around the privacy middleware.
- **Privacy middleware shipped.** Account numbers, routing numbers, and other CRITICAL-tier fields are now masked by default in every MCP tool response and CLI `--output json` output. Masking is type-driven: tools declare `-> ResponseEnvelope[PayloadType]` whose fields carry `Annotated[..., DataClass.X]` registry markers; the runtime walks the type, derives sensitivity as the max tier across all annotated fields, applies per-class transforms (e.g. account number → `****<last4>`), and writes a structured event to `<profile>/privacy.log.jsonl`. `@mcp_tool` no longer accepts a `sensitivity=` kwarg — sensitivity is derived at registration time and tool registration fails at import if the return type lacks classification. `ResponseEnvelope` is now generic over the payload type. CLI `--output json` runs through the same redactor + log writer; text output bypasses (caller's renderer owns formatting). The `unclassified=True` opt-out on `@mcp_tool` is the documented escape hatch for `sql_query` / `sql_schema`, whose payload shape is decided by the caller's input (PR 4 replaces with sqlglot lineage). See [`docs/specs/privacy-data-classification.md`](docs/specs/privacy-data-classification.md) §"Implemented middleware". (PR #192)
- Profile directories now created with `0o700` permissions (previously `0o755`), matching the `0o600` mode of the privacy event log and the privacy-sensitive nature of per-profile state (encrypted DB, secrets, daily events). (PR #192)

### Fixed
- **Cross-format duplicates no longer double-count.** The same transaction imported from two formats of one account (e.g. Wells Fargo `.qfx` and `.csv`) now collapses into one `core.fct_transactions` row with `source_count=2` instead of two rows. Previously, OFX truncating descriptions differently from CSV pushed cross-format similarity below the auto-merge threshold, so exact duplicates (same account + exact amount + same day) never merged — importing a set of `.csv` twins of already-loaded `.qfx` files produced double the expected rows. Exact-key cross-source pairs now auto-merge regardless of description similarity, with a source-cardinality guard that keeps N genuinely-distinct same-key transactions paired 1:1 rather than over-collapsing. See [`docs/specs/matching-exact-key-dedup.md`](docs/specs/matching-exact-key-dedup.md).
- `moneybin mcp serve` no longer corrupts the MCP JSON-RPC stream when no profile is configured. Previously the first-run wizard wrote a welcome banner to stdout, producing a cascade of "is not valid JSON" parse errors in the host (e.g. Claude Desktop). The server now boots regardless and, on the first tool call, guides setup: elicitation-capable clients are asked for a profile name and the profile is created in place (no restart); tools-only clients receive a single `infra_setup_required` message pointing to `moneybin profile create`. See [`docs/specs/mcp-first-run-setup.md`](docs/specs/mcp-first-run-setup.md).
- Every CLI and MCP entry point crashed at startup on databases created before PR #178 with `BinderException: Table "proposed_rules" does not have a column named "rule_id"`. The schema DDL (which runs before migrations) declared a `CREATE INDEX` on the V016-added `rule_id` column, binding against the pre-V016 table shape before V016 could add the column. The index now lives only in V016, where it belongs; V016 also commits the backfill before creating the index so DuckDB's "Cannot create index with outstanding updates" no longer blocks the upgrade path (same class as V010/V011, see PR #148).
- Migration runner self-heals stuck failure rows when the migration body has changed. Previously, a `success=false` row in `app.schema_migrations` from a prior failure required manual deletion before the next attempt would run. The runner now hashes every migration body, and if a previously-failed migration's body has changed since the failure, the stale row is auto-cleared and the migration retries once. Push the fix, tell users to re-run — no manual cleanup. (PR #156)
- V010 and V011 migrations crashed on existing populated databases with "Cannot create index with outstanding updates" because `ADD COLUMN ... DEFAULT` plus `SET NOT NULL` ran inside the same transaction. The two statements are now split across `COMMIT` / `BEGIN TRANSACTION` so the backfill writes flush before the NOT NULL constraint index builds. Recoverable from a crash via the existing idempotent re-run branch. (PR #148)
- Non-CLI SQLMesh entry points — the SQLMesh VSCode extension, direct `sqlmesh` shell invocations, and the language server — now honor `MONEYBIN_PROFILE`. Previously they loaded `sqlmesh/config.py` without running the MoneyBin CLI callback that registers the profile resolver, raising on `get_settings()`. (PR #160)
- Five categorization correctness bugs surfaced by live OFX checking-account testing: `memo` was dropped from the matcher and LLM input; `_match_description` only operated on `description`; system-generated merchants used over-generalizing `contains` patterns; `categorize_pending` was never called after the categorize-commit tool (then `transactions_categorize_apply`) so the snowball couldn't roll; OFX `<NAME>` truncation hid merchant identity in `<MEMO>` that the matcher never saw. See [`docs/specs/categorization-matching-mechanics.md`](docs/specs/categorization-matching-mechanics.md) for the full diagnosis. (PR #122)

### Security
- CVE fixes via dependency bumps: `urllib3` 2.6.3 → 2.7.0 (PR #127); `pip` and `python-multipart` advisories addressed (PR #124).

---



## [M1] — 2026-05-04 (Data Integrity)

Five M1 deliverables shipped plus companion work. `fct_transactions` is now trustworthy: dedup eliminates double-counting, transfer detection prevents transfer-as-spend distortion, auto-rules categorize new imports, net-worth tracks balances with self-healing reconciliation deltas.

### Added
- **Smart tabular importer** for CSV / TSV / Excel / Parquet / Feather with heuristic column detection, multi-account support, and migration profiles for Tiller, Mint, YNAB, and Maybe. Five-stage pipeline (Format Detection → Reader → Column Mapping → Transform & Validate → Load), three-tier confidence model, `TabularProfile` system with auto-save, `Database.ingest_dataframe()` primitive (#38).
- **OFX/QFX/QBO import parity** through the same `import_log` infrastructure as tabular: re-import detection, `--force` override, institution name auto-resolution from `<FI><ORG>` / FID lookup / filename heuristics, batch revert via `moneybin import revert <id>` (#82, #90).
- **Watched-folder inbox UX** at `~/Documents/MoneyBin/<profile>/inbox/`. `moneybin import inbox` drains successes to `processed/YYYY-MM/` and failures to `failed/YYYY-MM/` with YAML error sidecars. Per-profile lockfile + crash-recovery via staging-rename (#84).
- **Cross-source dedup** with SHA-256 content hashes and golden-record merge. `prep.seed_source_priority` config-driven seed table, `int_transactions__matched` view, `meta.fct_transaction_provenance` (#43, follow-ups #46).
- **Transfer detection** across accounts: shared matching engine Tier 4, `core.bridge_transfers`, always-review v1, four-signal scoring (date distance, keyword, roundness, pair frequency). `is_transfer` and `transfer_pair_id` on `fct_transactions` (#47).
- **Auto-rule learning** from user edits: merchant-first pattern extraction, `app.proposed_rules` review queue with four-state lifecycle, promotion to `app.categorization_rules` at priority 200, correction-handling threshold (#58, follow-ups #60).
- **`moneybin categorize bulk`** CLI with parity for the `categorize_bulk` MCP tool; `BulkRecordingContext` drops per-item DB lookups (#69).
- **Account management namespace.** `accounts list/show/rename/include/archive/unarchive/set` with Plaid-parity metadata (subtype, holder category, currency, credit limit, last four). Reversible account merging via bridge model. `app.account_settings` for display preferences and net-worth inclusion (#107).
- **Net-worth & balance tracking.** `accounts balance show/history/assert/list/delete/reconcile` per-account workflow; `reports networth show/history` cross-account rollup with period-over-period change. Three-model SQLMesh pipeline: `core.fct_balances` (VIEW) → `core.fct_balances_daily` (TABLE, daily carry-forward interpolation) → `core.agg_net_worth` (VIEW). Reconciliation deltas computed and self-healing on reimport (#107).
- **10-scenario test suite** with five-tier assertion taxonomy: structural invariants, semantic correctness (categorization P/R, transfer F1+P+R, negative expectations), pipeline behavior (idempotency, empty/malformed input handling), quality (date continuity, ground-truth coverage), operational. Bug-report recipe documented (#70, PRs #70–#83).
- **Whole-pipeline scenario runner.** Empty encrypted DB → `generate → transform → match → categorize` → assertions/expectations/evaluations against synthetic ground truth and hand-labeled fixtures. `make test-scenarios`. Validation primitives at `src/moneybin/validation/` reusable for live-data `data verify` (#59, #80).
- **Curated `moneybin://schema` MCP resource** + `sql_schema` tool mirror exposing core and select app interface tables with column comments and example queries — eliminates per-session schema reconnaissance (#87, #91).
- **MCP tool wall-clock timeouts** (configurable 30s default) with DuckDB `interrupt()` + connection close on timeout, so a hung tool can't wedge the server's write lock (#97).
- **MCP client install** across nine clients: claude-desktop, claude-code, cursor, windsurf, vscode, gemini-cli, codex (CLI / Desktop / IDE), chatgpt-desktop. Concurrency guide for the single-writer DuckDB lock (#94).
- **v2 MCP/CLI taxonomy.** Path-prefix-verb-suffix naming, entity groups (`accounts`, `transactions`), reference-data groups (`categories`, `merchants`), `reports` for cross-domain rollups, `system` for orientation, `tax` separated, `assets` reserved. ~50-tool rename map applied as a hard cut (#95, #96).
- **YAML golden cases** for `normalize_description()`; parametrized exact-equality tests; contributor-facing surface for adding real-world transaction descriptions (#66).

### Changed
- FastMCP 3.x adoption with per-session visibility (#71, #72).
- `CategorizationService` thin-wrapper consolidation across MCP, CLI, and service callers (#108).
- Simplify passes across `src/moneybin/` subsystems: matching, services, MCP tools, validation (#75, #76, #77, #79, #110).
- pytest-asyncio auto-mode; dropped `asyncio.run` boilerplate (#109).
- Tests run in parallel via pytest-xdist (#67).

### Fixed
- MCP tool names regex compliance for Anthropic/OpenAI clients (#89).
- Schema-mismatch crash on existing DB with stale schema; auto-reopen with migration (#88).
- App-table purpose strings overwritten by stale comments (#92).
- Migration auto-apply gate + inbox error surfacing (#93).
- SQLMesh fork-pool orphan processes causing MCP timeouts (#105).
- CLI `main` shadowing rename (#104).
- MCP schema drift coverage extended to `app.*` interface tables (#106).
- Account matching wired into the tabular import pipeline; `Decimal` end-to-end for monetary values; N+1 merchant batch fix; `ResolvedMapping` refactor (#51–#56).
- N+1 `COUNT(*)` queries in `db info` collapsed into one UNION ALL (#81).

---

## [M0] — 2026-04-30 (Infrastructure)

Foundational systems shipped: encryption-at-rest, schema migrations, observability, profiles, CLI/MCP scaffolding, and the synthetic data generator. Every M1+ feature builds on these.

### Added
- **AES-256-GCM database encryption at rest** via DuckDB's encryption extension. Argon2id KDF for passphrase mode; OS keychain integration for auto-key mode. `Database` connection factory (singleton `get_database()`), `SecretStore` for unified keyring + env-var secret retrieval, `SanitizedLogFormatter` PII safety net on all log handlers. Encryption CLI: `db init/lock/unlock/rotate-key/backup/restore/key show` (#29).
- **Profile system** with `~/.moneybin/profiles/{name}/` isolation. `moneybin profile create/list/switch/delete/show/set` (#30).
- **CLI restructure v1.** Domain command groups, `get_base_dir()` rewrite (defaults to `~/.moneybin/`), `transform` and `categorize` as top-level groups, `db ps`/`db kill`, `mcp list-tools/list-prompts/config generate --install`, `transform status/validate/audit/restate` (thin SQLMesh wrappers), `logs clean/path/tail`. Stubs for future command groups (#30).
- **Dual-path schema migration system.** SQL + Python migrations, auto-upgrade on first invocation, `app.versions` tracking, rebaseline command, SQLMesh version detection. Encrypted-database aware (#31).
- **Observability stack.** Single canonical `LoggingConfig`, `SanitizedLogFormatter` on all handlers, MCP server logging strategy (stderr for hosted, file for local), `prometheus_client` metrics with DuckDB persistence (flush on shutdown + periodic), `@tracked` decorator and `track_duration()` context manager. CLI: `logs clean/path/tail`, `stats` (#32).
- **Persona-based synthetic data generator.** Declarative YAML architecture, three v1 personas (`basic`/alice, `family`/bob, `freelancer`/charlie), ~200 real merchants, deterministic seeding, ground-truth labels in `synthetic.ground_truth` schema. CLI: `moneybin synthetic generate/reset/verify`. Level 2 realism (#37).
- **E2E test infrastructure.** Subprocess-based smoke tests (help, no-DB, DB commands), golden-path workflow tests (synthetic, CSV, OFX, lock/unlock, categorization) (#48).
- **MCP v1 scaffolding.** Response envelope, `@mcp_tool(sensitivity=...)` decorator, namespace registry, privacy middleware stub, prompts/resources (#42).

---

## [Pre-M0] — Pre-April 2026

Initial pipeline implementation that preceded the M0 design overhaul. Specs from this era live in [`docs/specs/archived/`](docs/specs/archived/): OFX import, CSV import (institution profiles), W-2 PDF extraction, rule-based transaction categorization, MCP read tools, MCP write tools.

These features survived the M0/M1 redesign — they're still shipped today, but reimplemented under the new abstractions (`Database` factory, service layer, encrypted-by-default storage, smart tabular importer that supersedes the profile-based CSV system).
