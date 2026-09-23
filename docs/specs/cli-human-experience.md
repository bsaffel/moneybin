# Architecture: CLI Human Experience

## Status

implemented

## Goal and scope

Make MoneyBin's CLI pleasant to read and operate: a scannable answer, useful
detail to explore, and an unmistakable account of what happened. Each command
returns to the shell. Sustained browsing belongs to the Web UI. A persistent
full-screen terminal application and charts are outside this design's scope.

This design covers human presentation, temporary paging, progress, guided input,
and outcome reporting. It preserves the taxonomy in [MoneyBin CLI](moneybin-cli.md)
and builds on [CLI Output Coherence](cli-output-coherence.md). Existing financial
formatting, redaction, result-framing, and audit requirements remain binding.
This spec records the approved interaction choices and implementation boundary.
The shared presentation boundary and command migration implement this contract.

## Approach and tradeoffs

Extend the existing shared rendering and output boundary. Commands supply domain
results and operation state; shared CLI code owns layout, terminal capabilities,
progress, and prompting. This buys consistent human behavior and one explicit
path for scripts and agents. The cost is migrating legacy output, including
service paths whose structured outcomes do not yet describe partial completion.

Styling commands independently would improve individual screens sooner but retain
inconsistent behavior. A terminal application would enable persistent navigation
but duplicate the Web UI and change the shell workflow. Neither is selected.

Retain Typer/Click and the existing Rich renderers. Before implementation, evaluate
their current paging/progress APIs and maintained prompt libraries against this
contract. Do not build a custom terminal toolkit or add dependencies for decoration.

## Presentation contract

1. Lead with the answer or operation outcome. Add profile, dates, and filters as
   compact context when relevant. Avoid repeated banners.
2. Use whitespace, sentence-case headings, bold key results, and restrained rules
   to separate context, primary result, detail, and next actions. No status icons
   on ordinary rows or individual boxes around every section.
3. Default output includes consequential changes, warnings, and useful next actions.
   Hide zero-change bookkeeping and internal representations.
4. `--wide` reveals additional table columns. `--verbose` reveals diagnostics and
   stage timings. JSON retains its existing full-data/field-projection contract.
5. Quiet suppresses progress and optional chatter, never requested results,
   failures, required recovery actions, truncation, incomplete-data warnings, or
   conversion disclosures.
6. Empty results explain the searched scope and an appropriate next step.
   Distinguish no records, no matches for filters, and unavailable data.
7. Suggestions use executable CLI commands with known arguments and relevant
   explicit profile context. Do not substitute Python calls, MCP method names,
   raw dictionaries, or opaque cursors for human guidance.

## Tables, money, and identity

8. Curate default columns around the question. Use readable entity names with
   enough disambiguation. Keep actionable IDs in the default view when choosing
   a row is the command's purpose; otherwise expose them through wide/detail views.
9. Right-align numeric columns. Reuse canonical money formatting and preserve
   quantity, unit-price, and FX-rate precision contracts. Keep currency explicit:
   an unambiguous shared label for a single-currency block, or per-row currencies.
10. Preserve complete amounts, signs, and currencies. Narrow terminals wrap text,
    omit disclosed optional columns, or use stacked label/value rows when essential
    columns cannot fit. Never clip or split digits into a misleading number.
    Wide output may require horizontal scrolling; disclose that behavior.
11. Flows keep their signs. Positive balances have no decorative plus; negative
    balances retain their minus. Balances are neutral. Color deltas by meaning:
    increased spending is not favorable merely because its sign is positive.
12. Disclose omitted columns, row limits, unknown totals, degraded data, and
    freshness limitations when relevant. Renderers never deduplicate records,
    silently change filters, or recalculate financial truth.

## Styling and terminal capabilities

13. Extend the centralized semantic palette: bold hierarchy, readable secondary
    context, restrained action accents. Respect the user's terminal palette and
    light/dark background. Require no font, fixed background, or glyph package.
14. Use functional symbols sparingly: `✓` success, `!` attention, `×` failure,
    `›` next action/selection. Accompany states with words. Provide ASCII fallbacks;
    avoid pictographic emoji and symbols on routine data rows.
15. Honor `NO_COLOR`. Plain output preserves meaning. Redirected output contains
    no styling/cursor-control sequences. Redirection never changes the selected
    output format automatically.
16. Centralize capability decisions per actual stream. Prompts require usable
    terminal input and visible terminal output. JSON and noninteractive execution
    never prompt or page.

Implementation must reconcile `.claude/rules/cli.md`'s emoji table and document
terminal-specific application of the design system. Fonts remain user-controlled;
the attention symbol is a status marker, not enthusiastic punctuation. CLI
progress extends the motion doctrine's sync-only loop to other running operations.
Idle/decorative motion remains prohibited. Update canonical rules with the
introducing implementation, so contributors do not inherit two conventions.

## Paging

17. Automatically page finite read-only human results exceeding the available
    terminal height, accounting for wrapped lines and navigation hints. Short
    results print directly without a pager flash.
18. Support scrolling, text search, and a visible quit hint. Search covers returned
    results, not the database. Paging never fetches beyond the requested limit.
19. Provide `--no-pager` on affected commands and a persistent MoneyBin setting to
    disable automatic paging; flags override preferences. Never page JSON,
    redirected/noninteractive output, live-follow logs, mutation previews,
    progress, or operation receipts.
20. Keep framing and consequential disclosures inside the paged answer. If a
    suitable pager is unavailable, print normally without losing rows. Pager quit
    is successful reading, not command cancellation or a broken-pipe failure.

## Progress

21. Show one in-place spinner with a meaningful stage label when the total is
    unknown. Use a count/progress bar when the denominator is meaningful.
    Never invent a percentage or time estimate.
22. Replace progress with a concise final receipt. Preserve warnings that arrive
    during progress. Financial amounts never animate. Stage timings belong under
    verbose output and must not masquerade as total operation duration.
23. Disable animation under quiet, JSON, redirection, and a persistent reduced-motion
    preference. Reduced motion uses static stage labels. Noninteractive text may
    emit sparse stages to stderr, never spinner frames. Restore terminal/cursor
    state after success, error, and interruption.

## Guided input and confirmation

24. Ask focused inline questions for missing required choices in interactive
    terminals. Use searchable selectors for collections, with readable names and
    disambiguation. Complete commands avoid unnecessary prompts.
25. Every choice has an explicit argument/flag equivalent. Missing input in JSON
    or noninteractive mode fails promptly with actionable guidance. Never consume
    piped data as prompt input or silently accept a choice.
26. Consequential writes preview the exact scope, count, and effect. Honor existing
    explicit confirmation flags for that scope; supplying all data arguments does
    not itself waive required confirmation. A confirmation flag cannot resolve
    an unspecified or ambiguous target.
27. Bulk confirmation applies to the previewed selection. Revalidate scope before
    mutation; changed scope requires a new decision, never silent widening.
    Escape/decline performs no proposed mutation; disclose any earlier saved work.
28. Prompt answers use the same validation/services as explicit arguments. Do not
    introduce a second business-logic path or print secrets in reconstructed
    commands. Return to the shell after completion or cancellation.

## Outcomes, recovery, and streams

29. Distinguish complete, partial, failed, needs-confirmation, and cancelled in
    words. Partial failure gets no overall success checkmark. Successful work
    producing review candidates can be complete, with a separate attention section.
30. Partial/cancelled receipts say what was saved, what remains, and what is stale.
    Do not claim rollback, zero writes, or safe retry without service evidence.
    If committed state is unknown, say so and offer supported inspection guidance.
31. Ctrl+C stops further work at a safe interruption boundary, cleans up terminal
    presentation, and reports known saved state. Do not promise instantaneous
    interruption. Use the shared error classifier; default errors are concise,
    with sanitized diagnostic detail available under verbose output.
32. Exit status agrees with the outcome: failed/partial/interrupted requested work
    is nonzero. Retain documented command-specific meanings until explicitly
    reconciled. Listing pending review items and quitting a pager are successful
    reads. Do not change exit semantics as an incidental styling change.
33. Results and essential framing remain on stdout; transient progress and optional
    commentary use stderr. Text errors use stderr; JSON keeps its structured error
    channel. Mixed receipts retain their saved/failed scope with the result, so
    stdout redirection cannot make partial work appear complete. Avoid duplicate
    terminal warnings.
34. Recovery actions come from the shared recovery contract. Renderers do not
    invent retry safety, rollback, or automatic recovery policy.

## Discovery

35. Help leads with the command's purpose and a small set of useful examples.
    Group commands by task and options by purpose, preserving the established
    taxonomy. Show relevant paging, detail, and noninteractive controls where
    supported. Completion covers commands/options and safe, inexpensive choices;
    it must not unlock a database, launch a wizard, or contact the network.

## Representative receipts

Fabricated examples illustrate layout, not recorded runs. Styling supplements
the hierarchy shown here.

```text
Transactions · demo · 200 results

Date         Merchant           Account                Amount
────────────────────────────────────────────────────────────
2026-09-16   Example Market     Checking            −84.32 USD
2026-09-16   Example Cafe       Credit card          −6.50 USD
2026-09-15   Example Payroll    Checking         +3,250.00 USD
…

↑↓ scroll · Space next page · / search · q return to shell
```

The ellipsis above abbreviates the specimen only; the pager receives every
returned row. Any database query limit is disclosed separately from the viewport.

```text
✓ Sync complete                                      demo
  58 transactions loaded from 3 institutions

  Institution                             Transactions
  ────────────────────────────────────────────────────
  Example Bank                                      28
  Example Credit                                    30
  Example Investing                                  0

Other changes
  36 transactions categorized using institution data
   2 stale transactions removed

! Needs review
  2 potential transfers
  › moneybin transactions matches pending
```

```text
Transactions · demo · 2026-09-01 through 2026-09-07

No transactions match these filters.
Try a wider date range or remove the account filter.
```

```text
! Sync partially completed
  28 transactions loaded from Example Bank
  Example Investing could not be refreshed: connection timed out

Loaded transactions were saved.
Investment data remains unchanged from the previous sync.
```

```text
Import needs confirmation
  File       example-statement.csv
  Target     Everyday Checking · Example Bank · …1234
  Change     Add 18 transactions

No transactions have been imported.
Import these 18 transactions? [y/N]
```

```text
Sync cancelled
  28 transactions were saved before cancellation.
  Refresh did not finish; reports may still show older data.
```

State claims require service evidence. Disclose staging/migration writes when
applicable; do not generalize "no transactions imported" into "nothing changed."

## Implementation boundary and migration

`cli/render.py` continues to own reusable rendering and semantic styles;
`cli/output.py` owns format selection, envelopes, and output policy. Add narrowly
scoped terminal/prompt helpers as needed, not a pluggable rendering framework.
Keep imports lazy: help and completion must not initialize profiles or databases.

Commands consume structured service outcomes and pass known counts, states, and
recovery actions to presentation. Services must not print human progress directly.
Where needed, expose a small optional progress callback with stage/count information
and no terminal dependency. Sanitized logging remains independent of live progress.

Migrate in bounded slices: foundation and sync receipts; read views and paging;
import/review guidance; remaining commands and help. Mark superseded patterns and
link removal work under the coherence rule. Reconcile the full command inventory
before declaring this design implemented.

Missing currencies, silent truncation, false outcome receipts, and mismatched
preview/write scope must be fixed before affected surfaces satisfy this spec.
Broader category-correction, profile-home, log-filtering, and audit defects found
in the UX review remain separately scoped; presentation must not conceal them.

## Observability and validation

Reuse local registry metrics for operation outcomes/durations, wide requests, and
omitted columns. Add bounded pager-fallback and prompt-outcome counters only where
the new behavior lacks an existing signal. Labels use command paths and fixed
outcome enums, never profile/account names, financial values, input, or search terms.
No telemetry. Rendering/help must not open a database or acquire a write lock just
to persist metrics; retain existing best-effort metric behavior.
Read-only runs therefore need not persist these counters: they are not a census
of pager usage. Tests provide the acceptance evidence for those paths.

Acceptance requires synthetic examples and behavior tests covering:

- Short/long reads, empty/filtered-empty results, mixed currencies, long names,
  large/negative amounts, unknown totals, and truncated/degraded results.
- Normal/narrow terminals, light/dark palettes, no color, ASCII fallback, reduced
  motion, quiet, verbose, wide, and redirected output.
- Pager threshold/wrapping, search, quit, unavailable pager, and no-pager preference.
- Complete/partial/failed/cancelled/confirmation-required operations, terminal
  cleanup, and truthful saved-state reporting, including unknown state.
- Interactive/flag parity, noninteractive refusal, scope revalidation, and exact
  bulk-preview parity.
- Clean JSON without animation/prompts, unchanged redaction/field projection,
  stdout/stderr behavior, and outcome-consistent exit status.
- Help/completion cold-start hygiene, task-oriented help examples, and copyable
  CLI recovery guidance.

Run repository-required checks for each implementation slice and regenerate guide
transcripts from synthetic personas after behavior is verified. Visual polish
alone does not satisfy a result-integrity or interaction test.
