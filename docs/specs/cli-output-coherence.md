# Feature: CLI Output Coherence

## Status
<!-- draft | ready | in-progress | implemented -->
draft

## Milestone

M3K.3 — the third increment under M3K (CLI / MCP UX standards), after M3K.1
([`agent-visualization.md`](agent-visualization.md)) and M3K.2
([`mcp-tool-surface-scaling.md`](mcp-tool-surface-scaling.md)). Those two govern
what the MCP surface returns; this one governs what the CLI's text branch prints.

## Goal

Give the CLI one way to render each kind of result, so every command looks like it
was written by the same hand and every line printed is worth reading. Today three
commands returning rows use three different idioms, four commands print internal
function or MCP tool names at the user, and `reports spending` is illegible at
default terminal width.

## Background

Origin: a 2026-07-25 audit of the CLI's text output, conducted by running every
representative command against a populated profile and reading what came back. It
produced twelve reproducible defects, referenced throughout this spec as F1–F12
and each named at the requirement that closes it. Five of them are worth stating
up front, because they set the scope:

- **F1** — `reports spending` renders eleven columns into an 80-column terminal.
  Every header and nearly every value is elided; not one figure is legible.
- **F2** — `accounts list`, `transactions list`, and `reports networth` are all
  read-projections returning rows, and each renders in a different idiom.
- **F3** — four commands print MCP tool names, Python function names, `repr`
  fragments, or `key=value` debug output at the user.
- **F11** — every invocation prints a profile banner naming two possible sources
  rather than the one that resolved, costing a line per command and telling the
  reader nothing actionable.
- **F12** — `moneybin refresh` narrates one pipeline stage of three, so a run that
  changed nothing and a run that recategorized 400 transactions are nearly
  indistinguishable from its output.

The remaining findings — F4 through F10 — are each named at the requirement that
closes them, in the Requirements section below.

Governing rules, none of which this spec supersedes:

- [`.claude/rules/cli.md`](../../.claude/rules/cli.md) — the `-o/--output`,
  `-q/--quiet`, `--json-fields` contract; exit codes; stdout/stderr split; the
  icon table. This spec adds *how the text branch renders*, which `cli.md` has
  never specified.
- [`.claude/rules/design-principles.md`](../../.claude/rules/design-principles.md)
  → Coherence — "when you add new X, does it look like existing X?" F2 is a
  standing violation of that rule.
- [`.claude/rules/surface-design.md`](../../.claude/rules/surface-design.md) —
  operation shapes. Every command in scope is Shape 5 (read-projection) or Shape 3
  (discrete-verb) and renders accordingly.
- [`.claude/rules/identifiers.md`](../../.claude/rules/identifiers.md) → Guard 1 —
  "the display column is for rendering; the ID is for joining." F7 violates it in
  both directions at once.
- [`design-system/readme.md`](../../design-system/readme.md) → Content
  fundamentals + Visual foundations — the CLI is named there as a surface sharing
  the product's language. Money is always mono with redundant sign; balances
  unsigned; numbers first. Requirement 12 reads "balances unsigned" as its intent
  — no decorative `+` on a positive position — and keeps the `−` on a negative
  one, which that rule was never meant to suppress.
- [`moneybin-cli.md`](moneybin-cli.md) — command taxonomy. **Unchanged by this
  spec.** No command is added, removed, renamed, or re-parented.

The same audit surfaced one defect that is **not** a rendering problem, tracked
separately and referenced here as F0: `reports networth` sums an account once per
balance source, so an account carrying rows from two sources is counted twice.
Liabilities dedup correctly and assets do not, which is the diagnostic lead. It is
named in this spec for one reason — a renderer that silently collapsed the
duplicate rows would make the output *look* correct while the total stayed wrong.
Requirement 35 exists to prevent exactly that.

## Requirements

Numbered, each independently testable.

**Renderers (F2)**

1. A module `moneybin.cli.render` exposes exactly three text renderers —
   `render_rows`, `render_summary`, `render_note` — and no command in
   `src/moneybin/cli/` constructs a `rich.Table` or hand-formats an indented list
   directly.
2. `render_rows` is the only way a command prints a collection of records. It
   emits a Rich table to **stdout**.
3. `render_summary` is the only way a command prints a labelled scalar block
   (`Net worth: …`, `Assets: …`). It emits aligned label/value pairs to
   **stdout**.
4. `render_note` is the only way a command prints an informational status line. It
   emits to **stderr** and is suppressed by `-q/--quiet`. Result framing
   (requirement 10) is not a note and does not travel this path.
5. Result data is never suppressed by `-q` (restates `cli.md`; asserted here
   because the renderers are now the enforcement point). The same guarantee
   covers any statement about what the result omits — see requirement 10.

**Column policy (F1)**

6. Every `reports` command declares its default column set, and `render_rows`
   renders only those. The declaration is **parameter-aware**: a static tuple
   where the projection is fixed, and a callable of the report's own parameters
   where it is not. `cash_flow` is the motivating case —
   `by="account"` selects `account_id, account_name` and no `category`, while
   `by="category"` selects `category` and no account columns
   (`src/moneybin/reports/definitions/cash_flow.py:98-107`), so any single tuple
   would name a field absent in one mode or drop the only grouping dimension in
   the other. A report whose default set does not resolve for a legal parameter
   combination is a spec violation, caught by the contract test in requirement 9.
   **Extension reports** (`register_extension_reports`,
   `src/moneybin/reports/_framework/registry.py:34`) share the same `@report` /
   `ReportSpec` contract and the same `register_report_cli` path, so the field is
   **optional** on `ReportSpec`: an extension that declares nothing keeps working
   unchanged. Its fallback is the **first six columns** of the declared
   projection, with the remainder reachable via `--wide`. Six is a fixed count,
   not a computed fit: `OutputColumn` carries only `name`, `description`, and
   `data_class` (`src/moneybin/reports/_framework/contract.py:62-67`) — no
   display width — so "the columns that fit 80" is not computable without
   measuring runtime values, which would make an extension's column set vary with
   its data. A fixed count is deterministic and needs no new metadata. The
   consequence is stated rather than hidden: **requirement 9's 80-column
   guarantee is contract-tested for in-tree reports and best-effort for
   extensions** until one declares `DEFAULT_COLUMNS`. Requirement 10's framing
   line discloses the omission either way, so a wide extension report is legible
   as truncated rather than silently clipped.
   `docs/specs/extension-contracts.md` gains the optional field and its fallback
   in the same change.
7. Every command with a `DEFAULT_COLUMNS` narrower than its full projection
   accepts `--wide`, which renders all columns.
8. `--output json` is unaffected by `DEFAULT_COLUMNS` and by `--wide`; it returns
   the full projection, filtered only by `--json-fields`.
9. `DEFAULT_COLUMNS` for every report fits in **80 columns** with no elision of any
    header, and no elision of any value that is not free text. 80 is the width F1
    was reproduced at, so a wider bar would let an implementation satisfy this
    spec while the reported defect persists.
10. When columns are omitted, `render_rows` emits one **result-framing** line
    naming the count and the flag: `12 of 23 columns shown — --wide for all`.
    Silent truncation is prohibited. This line is part of the result, not
    chatter: it emits to **stdout** immediately after the table, and `-q` never
    suppresses it. Both properties are load-bearing — routing it to stderr would
    let `moneybin reports spending > report.txt` capture a truncated table with
    no indication it was truncated, and suppressing it under `-q` would do the
    same for `--quiet`. Either alone reintroduces the silent truncation this
    requirement forbids.

**Money and numbers (F9)**

11. A single `format_money` is the only place amounts are stringified for text
    output. Thousands separators always; two decimal places always.
12. Every money column declares a **money kind**, and the renderer never infers
    meaning from the raw number. Four kinds:
    - `flow` — signed under the AGENTS.md accounting convention (negative =
      expense, positive = income). Renders an explicit `+` / `−` (U+2212).
    - `magnitude` — a positive absolute quantity whose polarity is carried by the
      column, not the value (`spending_trend.total_spend` is a positive outflow).
      Renders unsigned; never colored as income.
    - `delta` — a signed *change in a magnitude*, where the sign means direction
      (more / less) rather than income / expense. Declares the polarity of the
      thing it measures, because that determines whether an increase is good or
      bad. `spending_trend.mom_delta` is current-minus-previous spend
      (`src/moneybin/reports/definitions/spending_trend.py:54-58`): a positive
      value means spending *rose*. It renders signed — the direction is the
      column's entire purpose — and colors against the declared polarity, so a
      rise in spending is `--neg-expense`, not `--pos-income`.
    - `balance` — a position, not a movement. Renders **unsigned when
      non-negative, and always retains `−` when negative**. The design system's
      "balances unsigned" rule exists so a checking balance is not decorated with
      a `+`; it does not license dropping a minus. `reports.net_worth` is
      `SUM(d.balance)` over accounts whose liabilities are kept negative
      (`src/moneybin/sqlmesh/models/reports/net_worth.sql:12`), so a net worth of
      −50,000.00 is reachable and would otherwise render identically to
      +50,000.00 — the single worst misread this spec could ship.

    `delta` exists because neither of the other kinds fits a signed delta:
    `flow` would color a spending increase green, and `magnitude` renders
    unsigned, erasing the increase-versus-decrease distinction the column exists
    to convey.

    The kind is declared **per column**, as `money_kind` on `OutputColumn`. It is
    deliberately *not* named `kind`, because `ReportSemantics.kind` already exists
    with the values `position | flow | ratio | count`
    (`src/moneybin/reports/_framework/contract.py:77`). That one is **report**-level
    and cannot serve here: `spending_trend` carries a `magnitude` and a `delta` in
    the same result, so one report-level value cannot describe both columns. The
    two overlap in vocabulary (`flow`, and `position` ≈ `balance`) without being
    the same thing, so the distinct field name is load-bearing — a shared `kind`
    would read as one concept and rot into two.

    For **extension reports**, `money_kind` is optional, defaulting to `flow`.
    Requiring it would break every existing `@report` extension, since
    `OutputColumn` has no such field today. `flow` is the safe default because it
    renders *signed*: an unnecessary `+` on a magnitude column is cosmetic, while
    a dropped `−` is a misread balance. Defaulting to `magnitude` would invert
    that risk. `docs/specs/extension-contracts.md` documents the field and this
    default alongside `DEFAULT_COLUMNS`.
13. Amounts are right-aligned in `render_rows` columns.
14. Color is driven by the money kind plus the value, never by the value alone.
    A `flow` colors `--pos-income` when positive and `--neg-expense` when
    negative; a `magnitude` in a spend-oriented column is never green; a `delta`
    colors against its declared polarity, so a rise in an expense magnitude reads
    `--neg-expense`; a `balance` is uncolored. The sign glyph — where the kind has
    one — is present regardless of color, so the encoding survives a pipe, a
    non-TTY, and `NO_COLOR`. A negative `balance` is the load-bearing case: it
    carries `−` with no color, so the sign is the only channel and must never be
    dropped.
    **Rationale:** `spending_trend.py` declares `total_spend` as a positive
    absolute outflow and `mom_delta` as current-minus-previous spend. Coloring on
    raw sign would render spending green as income and invert the meaning of a
    rising `mom_delta`, contradicting AGENTS.md's sign convention.
15. Color is emitted only when stdout is a TTY and `NO_COLOR` is unset.

**Message hygiene (F3, F11, F12)**

16. No user-facing message *leaks* an internal identifier. The audit matches
    **compound internal identifiers and tool-call syntax**, not bare words:
    - a `snake_case` identifier of two or more segments (`transactions_get`,
      `accounts_links_run`), whether bare or as a `name:` prefix;
    - `key=value` debug fragments outside a recovery action.

    It deliberately does **not** match single-word registered tool names. The
    registry contains tools literally named `accounts` and `reports`
    (`src/moneybin/mcp/surface.py:28-29`), which are also unavoidable product
    vocabulary — a test rejecting every registered name would fail on
    `Accounts: 5` and force unnatural prose for no privacy or clarity gain.

    **Explicit exception — `RecoveryAction` rendering.** `system doctor` and the
    error path render `action.tool(key=value, …)` deliberately
    (`src/moneybin/cli/commands/system/doctor.py:140`), because
    `RecoveryAction.tool` *is* an MCP tool name by contract
    (`src/moneybin/errors.py:59`) and the rendered call is meant to be pasted
    directly by an agent. This is a designed AX affordance, not a leak, and
    requirement 22 preserves it. The audit skips recovery-action lines.
17. No user-facing message names an internal dependency. `SQLMesh` is not a user
    concept; the stage is "transforms".
18. `refresh` emits one `render_note` per pipeline stage naming the stage and its
    observable outcome, including stages whose outcome is zero. A run that changed
    nothing and a run that recategorized 400 transactions are distinguishable from
    stderr alone. This requires a **result-carrier change**, not a renderer-only
    one: `RefreshResult` (`src/moneybin/services/refresh.py:75`) carries
    error-or-`None` per step, and `_run_categorize_step` (line 314) computes its
    counts, logs them, and returns `str | None`, discarding them. The renderer
    cannot recover a count the service already dropped, and must not re-query for
    it. `RefreshResult` therefore gains a per-stage outcome carrying the counts the
    steps already compute. This is the **one** requirement in this spec that
    reaches below the CLI layer, and it is deliberate: the outcome per stage is
    the payload, so no render-layer-only change can satisfy it.
19. The profile banner names the source that actually resolved, or says nothing.
    The ambiguous string `config.yaml or first-run wizard`
    (`src/moneybin/cli/utils.py:254`) does not survive: it lists two candidates
    and confirms neither, costing a line per invocation to say nothing.

**Quiet on success (F6)**

20. `system doctor` prints per-invariant lines only for invariants that fail. On a
    fully-passing run it prints its summary line and nothing else.
21. `--verbose` restores the full per-invariant roll.
22. The failure path is unchanged in content: a failing invariant prints its name
    and its detail.

**Stats (F5)**

23. `stats` renders each metric with its distinguishing dimension, such that no
    two rendered lines share a label.
24. Histogram metrics render an **explicitly declared** unit, carried as a `unit`
    field on the metric declaration in `src/moneybin/metrics/registry.py`. A
    metric that is not a duration does not render `s`.
    **Name-suffix derivation was tried and rejected.** It holds for
    `..._duration_seconds` and `..._rate`, but nine registered metrics end in
    suffixes that name a *dimension* rather than a unit — `..._batch_size`,
    `..._score`, `..._pending`, `..._count`, `..._rows_affected`. The decisive
    case is `moneybin_import_batch_size`, whose observations are **files**: that
    fact lives only in the declaration's description string, which `app.metrics`
    does not persist. Derivation would force `stats` to omit the unit or guess
    it. One declared field applied uniformly is also the coherent choice — a
    derive-here / declare-there split would be two mechanisms for one job.
25. `stats` groups metrics by domain with a header per group, rather than one
    alphabetical list.

**Identity display (F7)**

26. `accounts list` renders the account ID alongside the display name, so rows
    that share a display name are distinguishable. **Balance is deliberately not
    in scope:** `AccountSummary`
    (`src/moneybin/privacy/payloads/accounts.py`) carries no current-balance
    field and `AccountService.list_accounts()` reads only `dim_accounts`, so
    adding one would change the JSON/MCP contract requirement 8 keeps untouched
    — and F0 shows balance data is currently summed once per source, which a new
    surface would reproduce rather than reveal. Tracked as a follow-up.
27. `transactions list` continues to render the account ID. **The display name is
    deliberately not in scope**, for the same reason as requirement 26's balance:
    `TransactionRow` carries only `account_id`
    (`src/moneybin/privacy/payloads/transactions.py:44`) and
    `TransactionGetResult` returns only `transactions` and `next_cursor`
    (`src/moneybin/services/transaction_service.py:136-140`), so a name would
    require a service and payload change — the JSON/MCP contract requirement 8
    keeps untouched. Tracked as a follow-up alongside the balance column.
28. `accounts list` and `transactions list` share the `account_id` column, with
    equal values for the same account, so the two outputs join. **This is what
    closes F7:** the reported defect was that `accounts list` showed names with no
    ID while `transactions list` showed an ID with no name, leaving no shared key.
    Requirement 26 supplies the missing ID, which makes the join possible on the
    column both sides already agree on. Display names on both surfaces are an
    ergonomic improvement on top of that, not the fix.

**Categories (F8)**

29. Category values are resolved to the display taxonomy at one boundary. No
    single rendered column contains both `Food & Drink` and `FOOD_AND_DRINK`.
30. A category with no display mapping renders a single consistent placeholder —
    `Uncategorized` — never the raw provider code. Rendering the raw value would
    itself produce the mixed column requirement 29 prohibits, which is the defect,
    not a disclosure of it. The count of unmapped rows rides the **result framing**
    (requirement 10), not a `render_note`: requirement 4 suppresses notes under
    `--quiet`, and a taxonomy gap the user cannot see is exactly what requirement
    29 exists to prevent. This **widens requirement 10's trigger**: result framing
    is emitted when columns are omitted *or* when any rendered column contains an
    unmapped placeholder. Without that widening the disclosure would vanish in
    exactly the cases it matters — under `--wide`, and for any report whose full
    projection already fits 80 columns, neither of which omits a column. The two
    framing clauses may share one line. The raw provider value remains available
    in `--output json`, which requirement 8 leaves unfiltered.

**Stubs (F4)**

31. Unimplemented commands are hidden from `--help` output at every level
    (`hidden=True`), while remaining invocable so the namespace stays reserved and
    any existing script keeps its exit code.
    **Scope is whole-command stubs only.** A command that reaches
    `_not_implemented` on *some* paths is not a stub and is not hidden — hiding it
    would remove working behavior from `--help`, the opposite of the trust repair
    F4 exists for. The enumeration is the four `sync` stubs (`key rotate`,
    `schedule set|show|remove`), the two `budget` stubs (`set`, `delete`), and the
    three `transactions categorize ml` stubs — **not** a grep for
    `_not_implemented`, which also matches `review`'s `--interactive` branch
    (#358) and any future partial.
32. The not-implemented message names a user-facing next action, not a repo path.
     `docs/specs/*.md` does not appear in any message reachable by an installed
     user.
33. The exit-code policy in `stubs.py` (stubs exit `0`) is unchanged.

**Pagination (F10)**

34. The text branch renders a human paging line stating the number of rows shown,
    whether more exist, and a continuation the reader can actually type —
    `8 shown · more available · raise --limit for more`.
    It must **not** name `--cursor`:
    that option takes a token value
    (`src/moneybin/cli/commands/transactions/list_.py:50-52`), the token is
    deliberately withheld from text output, and
    instructing a reader to pass a flag whose value they were not given produces a
    usage error. Showing the token instead would re-open F10, whose defect was a
    base64 cursor printed at a human. `--cursor` remains the agent's continuation
    over `--output json`, where the token is present.
    **No total is rendered:** `TransactionGetResult` exposes `transactions` and
    `next_cursor` only, computing `total_count` to derive `has_more`
    (`src/moneybin/services/transaction_service.py:793`) and then dropping it
    rather than carrying it onto the result
    (`src/moneybin/services/transaction_service.py:136-140`).
    Surfacing a total would be a service and payload change, which requirement 8
    excludes. `has_more` is sufficient to close F10 — the defect was a base64
    cursor shown to a human, not a missing count.

**Non-interference with data correctness (F0)**

35. No renderer deduplicates, merges, or suppresses rows. `render_rows` emits one
    line per record it is given. A regression test asserts that a payload
    containing two rows identical in every rendered column still renders two
    lines — so a future "tidier output" change cannot mask a data defect behind a
    presentation fix.

## Data Model

No schema changes. No migration. This spec touches presentation only.

One registry change: requirement 24's `unit` field on each metric declaration in
`src/moneybin/metrics/registry.py`. A middle draft of this spec removed it in
favour of deriving the unit from the Prometheus name suffix, on the reasoning
that the convention was already universal. It is not: `..._duration_seconds` and
`..._rate` are self-describing, but `..._batch_size`, `..._score`, `..._pending`,
`..._count`, and `..._rows_affected` name a dimension, not a unit.
`moneybin_import_batch_size` counts **files**, a fact recorded only in the
description string that `app.metrics` does not persist. Declaring the unit is
therefore the smaller of the two changes, not the larger one — derivation buys a
removed field and costs a `stats` surface that cannot label nine of its metrics.

## Implementation Plan

### Files to Create

| Path | Purpose |
|---|---|
| `src/moneybin/cli/render.py` | `render_rows`, `render_summary`, `render_note`, `format_money`, color/TTY gating |
| `tests/moneybin/test_cli/test_render.py` | Renderer unit tests — alignment, sign, separators, TTY gating, quiet |
| `tests/moneybin/test_cli/test_message_hygiene.py` | Requirements 16, 17, 32 — the grep-shaped audits |
| `tests/moneybin/test_cli/test_column_policy.py` | Requirements 6–10 across every `reports` command |

### Files to Modify

| Path | Change |
|---|---|
| `src/moneybin/cli/output.py` | `render_or_json` delegates its text branch to the new renderers; `--wide` joins the shared option set |
| `src/moneybin/reports/_framework/contract.py` (`ReportSpec`, line 88) | Carry the default column set as spec metadata — **this is where the column policy lives**. Parameter-aware per requirement 6 |
| `src/moneybin/reports/_framework/cli_register.py` | `build_cli_command(spec)` (called by `register_report_cli`, line 115) builds each report's Typer signature — this is where the generated `--wide` option and the `DEFAULT_COLUMNS` application land. Note `register_reports_cli`, the plural fan-out, lives in `registry.py:73` and only loops specs; it likely needs no change |
| `src/moneybin/reports/definitions/*.py` | Declare each report's `DEFAULT_COLUMNS`, `spending_trend.py` first (F1) |
| `src/moneybin/cli/commands/reports/networth.py` | The two hand-written NetworthService-backed commands; adopt `render_summary` / `render_rows` |
| `src/moneybin/cli/commands/accounts/__init__.py` | Account ID column (26); adopt `render_rows` |
| `src/moneybin/cli/commands/transactions/list_.py` | Keep rendering the account ID only — requirement 27 excludes the display name (27); drop the `transactions_get …` line (16); human paging (34) |
| `src/moneybin/cli/commands/transactions/categorize/__init__.py` | Uncategorized queue is a Shape-5 read-projection — migrate off `render_rich_table` (1) |
| `src/moneybin/cli/commands/accounts/links.py` | `links pending` (lines 70-89) and `links history` (lines 141-185) hand-format an aligned table via `typer.echo`; there is no `links list` subcommand — requirement 1 applies from day one |
| `src/moneybin/cli/commands/investments/security_links.py` | `links pending` / `links history` hand-format the same padded-column table (lines 78-99, 199-215) |
| `src/moneybin/cli/commands/transactions/notes.py` | `notes list` emits one `typer.echo` per note (line 103) |
| `src/moneybin/cli/commands/transactions/tags.py` | `tags list` emits a tab-separated tag/count list (line 150) |
| `src/moneybin/cli/commands/merchants/links.py` | Same hand-formatted-table pattern as its accounts twin; migrate both together per the coherence rule |
| `src/moneybin/cli/commands/transactions/matches.py` | `matches pending` hand-formats a padded f-string table (lines 61-77) — the third of the three review-queue renderers |
| `src/moneybin/cli/commands/refresh.py` | Per-stage notes (18); drop function-name prefixes and `SQLMesh` (16, 17) |
| `src/moneybin/services/refresh.py` | `RefreshResult` gains per-stage outcomes so the counts `_run_categorize_step` already computes reach the renderer instead of only the log (18) |
| `docs/specs/extension-contracts.md` | Document `DEFAULT_COLUMNS` as an optional `ReportSpec` field and its width-bounded fallback (6) |
| `src/moneybin/cli/commands/system/doctor.py` | Quiet on success (20–22); recovery-action rendering unchanged per req 16's exception |
| `src/moneybin/cli/commands/stats.py` | Dimensions, units, grouping (23–25) |
| `src/moneybin/cli/utils.py` | Profile banner (19); **retire `render_rich_table`** into `render_rows` — it is the shared `rich.Table` builder req 1 supersedes |
| `src/moneybin/cli/commands/stubs.py` | Message copy (32) |
| `src/moneybin/cli/main.py` + group modules | `hidden=True` on stub registrations (31) |
| `src/moneybin/metrics/registry.py` | Add a `unit` field to each histogram declaration (24); add the three counters in Observability below |
| `.claude/rules/cli.md` | Add a "Text rendering" section pointing at this spec — the rule file is where a future contributor looks first |

### Key Decisions

**Three renderers, not a framework.** `render_rows` / `render_summary` /
`render_note` cover every text branch in the CLI today. This is deliberately not a
pluggable formatter registry: per Simplicity First, the abstraction exists to
remove ambiguity about which shape to use, not to add flexibility. If a fourth
shape is genuinely needed later, it is added then.

**Column policy lives with the command, not in the renderer.** `DEFAULT_COLUMNS`
is declared per report because only the report's author knows which columns answer
the question. A width-based auto-selector would be magic — it would silently pick
different columns on different terminals, which
`design-principles.md` → "Magic stays visible" rules out.

**Truncation is always disclosed (10).** Reporting fewer columns than exist is a
bounded view, and per the review's own standard a bounded view that does not say
so reads as complete when it is not.

**Stubs are hidden, not removed (31).** Removing them frees the namespace for a
future collision and breaks any script that tolerates the `0` exit. Hiding them
fixes the trust problem — root `--help` stops promising what it cannot deliver —
at no compatibility cost. This is a pre-launch surface per
`design-principles.md`, so the change is cheap now and expensive later.

**Sign encoding is redundant by construction (14).** Color is an enhancement, never
the encoding. This matches the design system's colorblind rule and survives the
CLI's non-TTY consumers — scripts and agents — which `cli.md` treats as peers.

**The renderers do not dedup (35).** Stated as a requirement, not left to
judgement, because F0 makes it tempting: an account appearing once per balance
source produces visibly repeated rows, and collapsing them in the renderer would
make the net-worth output *look* correct while the total stayed wrong. Rendering
is not the place to repair data — a presentation fix that hides a model defect is
worse than the defect, because it removes the symptom that would have found it.

## CLI Interface

No command is added, removed, or renamed. One new flag:

| Flag | Attaches to | Purpose |
|---|---|---|
| `--wide` | Read-only commands whose `DEFAULT_COLUMNS` is narrower than their projection | Render all columns |

`--wide` is ignored with `--output json` (which is always full), matching how
`--json-fields` is ignored with `--output text`.

Before and after for the worst case (F1). The "before" is real output at 80
columns; the "after" is a **mock** against the `demo` persona, showing the shape
of the change rather than the final column choices:

```
# before — 11 columns at 80 cols, nothing legible
│ 202… │ LOA… │ 117… │ 4    │ 0.00 │ 117… │      │ 0.00  │ 117… │       │ 391… │

# after — DEFAULT_COLUMNS (mock)
  Month     Category            Total      Txns      MoM      YoY
  2026-07   Groceries         −412.55        18    −0.04x   +0.11x
  2026-07   Utilities         −208.00         3    +0.02x   −0.06x

  6 of 11 columns shown — --wide for all
```

## MCP Interface

No change. MCP tools return the envelope; this spec governs the CLI's **text**
branch only. `render_or_json`'s JSON path — redaction, `derive_tier` stamping,
the `privacy.log` audit event — is untouched, and requirement 8 exists to keep it
that way.

## Observability

Per AGENTS.md ("Specs touching app code must include metrics") and
[`observability.md`](observability.md). Three counters, registered in
`src/moneybin/metrics/registry.py` alongside the existing declarations and
following the same Prometheus naming convention the rest of the registry
from:

```python
CLI_WIDE_REQUESTED_TOTAL = Counter(
    "moneybin_cli_wide_requested_total",
    "Times --wide was passed, by command",
    ["command"],
)

CLI_COLUMNS_OMITTED_TOTAL = Counter(
    "moneybin_cli_columns_omitted_total",
    "Times a text render omitted columns from the full projection, by command",
    ["command"],
)

CLI_STUB_INVOKED_TOTAL = Counter(
    "moneybin_cli_stub_invoked_total",
    "Times an unimplemented command was invoked, by command",
    ["command"],
)
```

What each is for — a metric with no question behind it is noise:

- **`wide_requested` vs. `columns_omitted`** flag a report whose `DEFAULT_COLUMNS`
  may be wrong: one whose `--wide` rate approaches its omission rate is hiding
  something readers want. Read as a pointer, not a verdict — see the persistence
  constraint below.
- **`stub_invoked`** answers which hidden stubs users still reach (requirement
  31 hides them from `--help` but keeps them invocable), which is the demand
  signal for implementing one — and detects any surviving path that still
  advertises a stub.

No metric records row contents, amounts, or identifiers — only counts and the
command label, per the PII-in-logs rule.

**These counters persist only on sessions that also write business data.**
`flush_metrics()` returns without flushing when `database_was_written()` is false
(`src/moneybin/observability.py:116`), so a read-only `reports` or stub
invocation discards its observations at exit. That is deliberate and stays:
turning an otherwise read-only command into a write-lock holder is not a trade
this project makes for telemetry, and `--wide` is common enough that the lock
would land on nearly every read. The consequence is stated rather than worked
around: the `--wide`-versus-omission ratio is a **directional** signal drawn from
write-bearing sessions, not a census, and it does not by itself settle whether a
report's `DEFAULT_COLUMNS` is right. Requirement 9's contract test is what
*enforces* the column policy; these counters only suggest where to look next.

## Testing Strategy

Per `.claude/rules/testing.md` and the project's TDD requirement, each numbered
requirement gets a failing test before its implementation.

**Unit** — `format_money` across all four money kinds (`flow` / `magnitude` /
`delta` / `balance`), asserting a spend `magnitude` never renders green and a
positive `delta` on an expense column renders `--neg-expense` (12, 14); TTY and
`NO_COLOR` gating (15); and the stream/suppression matrix — `-q` suppresses
notes, never rows, and never result framing, which lands on stdout (4, 5, 10).
The redirect case is its own assertion: capturing stdout alone must still
contain the omission line.

**Contract, parameterized across every command** — this is where coherence is
actually enforced, and the tests must enumerate commands from the live registry
rather than a hand-maintained list, so a new command cannot silently skip the
contract:

- Every read-only command's text branch renders through one of the three
  renderers (1).
- Every `reports` command's default column set renders within **80** columns with
  no header elided (9) — the same threshold requirement 9 sets, and the width F1
  was reproduced at. Parameter-aware sets are exercised across every legal
  parameter combination, not just the default one (6).
- **Renderer-authored** output — static status copy, labels, headers, notes, and
  result framing — contains no **multi-segment `snake_case` identifier** and no
  `key=value` fragment outside a recovery action (16). The audit is scoped to
  strings the renderer owns and **excludes table-cell payloads**: a transaction
  description reading `order_id=123` or a user-supplied snake_case category label
  is the user's own data, and an assertion over all rendered bytes would demand
  mangling financial data to satisfy a contract about status copy. The test also
  carries requirement 16's exceptions verbatim — single-word registered tool names
  (`accounts`, `reports`) and `RecoveryAction` lines are **not** matched. Stating
  the assertion more broadly than the requirement would fail on legitimate output
  like `Accounts: 5`.
- No reachable message contains `docs/specs/` (32).
- No stub command appears in any `--help` output, **and every
  partially-implemented command still does** (31). Both directions are required:
  the one-sided assertion passes trivially against an implementation that hides
  everything matching `_not_implemented`, which is exactly the over-broad reading
  requirement 31 rules out.

**Regression, F-numbered** — one test per finding, each written to fail against
today's code. Two need specific shapes:

- **F6** — a doctor run with one failing invariant must print that invariant and
  not the 48 passing ones. A fixture where everything passes cannot distinguish
  quiet-on-success from a renderer that prints nothing at all.
- **F8** — a fixture containing both a mapped and an unmapped category, asserting
  the column is homogeneous and the unmapped one is counted in the result framing — not a `render_note`, which requirement 4 suppresses under `-q`. A
  fully-mapped fixture passes trivially.

**Not covered by the default gate:** these are CLI-surface tests in
`tests/moneybin/test_cli/`, so `make check test` is the correct gate. No
scenario-suite run is required — no data shape changes.

## Synthetic Data Requirements

None new. The existing demo persona exercises every renderer. The F8 test needs a
category with no display mapping, which is a fixture concern rather than a
generator one.

## Dependencies

None added. Rich is already a dependency and already renders
`transactions list`.

## Out of Scope

- **F0**, `reports networth` summing an account once per balance source. A
  correctness defect in the model, tracked separately and fixed on its own branch
  with its own test. Requirement 35 keeps this spec from masking it.
- **A TUI or any second front-end.** Considered and declined: a persistent
  full-screen app serves humans only, while `cli.md` treats scripts and agents as
  peer consumers of the same surface. Nothing here forecloses one later.
- **New reports or report capabilities** — charts, sparklines, financial-health
  projections, spending-flexibility tagging, regex search, local backups.
  Considered and declined for this increment; each is additive to a coherent
  render layer rather than blocked by its absence.
- **Command taxonomy.** `moneybin-cli.md` owns it and is unchanged.
- **Implementing any stubbed command.** F4 is about visibility only.
