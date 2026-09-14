# Keeping the console readable — which sink a log line belongs in

Cited from [`.claude/rules/cli.md`](../rules/cli.md) §"Exit Codes & stderr" →
"Keeping the console readable".
Read this before demoting a log line, adding a prefix to
`_CONSOLE_SUPPRESSED_PREFIXES`, or deciding where a count should live.

WARNING and above always reach the console. Below that, a record is hidden only
if its logger matches `_CONSOLE_SUPPRESSED_PREFIXES` in `logging/config.py`.
Everything else prints. That list holds two kinds of entry — third-party
libraries that narrate every call (`sqlmesh`, `httpx`, …) and MoneyBin modules
whose INFO is per-run bookkeeping the log file should keep and the terminal
should not. Read the constant for the current membership; each entry carries
the reason it earned a place.

**`logger.debug` is not "hide from console" — it is "drop everywhere."** The
root logger sits at INFO, so a DEBUG record is never emitted and never reaches
the log file either. That distinction decides which of two tools to reach for:

| The line is… | Use | Why |
|---|---|---|
| Already said by a `typer.echo` or another surviving INFO line | `logger.debug` | The file keeps the other copy. `sync pull` reported its categorization total three times from three layers. |
| Detail worth keeping in the file, but in the subsystem's vocabulary rather than the user's | denylist prefix | The file keeps it; the console does not. "Tier 4: 5 potential transfers found" — the user has no tiers; "Loaded 5 Plaid accounts" — their Chase card is not a Plaid account. |

Getting this backwards is easy and quiet: demoting the per-tier match counts to
debug looked like console cleanup, but `MatchResult.summary()` reports only
run-wide totals, so the per-tier split left the log file entirely. Before
demoting, name the other place the information survives.

**`typer.echo` does not reach the log file.** It writes to stderr directly. A
count that exists only in a `typer.echo` is absent from the log, which is why
the Plaid row counts stay at INFO behind a denylist prefix instead.

**A denylist is the deliberate choice here.** An allowlist would be quieter as
new dependencies arrive, but it inverts the default for ~168 `logger.info` sites
and turns every one whose output a user needs into a silent regression — MCP's
host stderr, `log_to_file: false`, schema-migration progress, and "your
`--institution` flag was ignored" each broke that way when it was tried in #356.
What must be hidden is enumerable; what must stay visible is not.

**Adding a prefix hides it from every stream at every level**, including
`--verbose` — but only while a log file exists to hold the copy. Under
`log_to_file: false`, or when the log directory is missing, stderr is the only
sink and the filter stands down entirely, because
`docs/guides/observability.md` and `threat-model.md` both promise stderr is
unaffected by that setting.

Locked by `tests/moneybin/test_logging_config.py::TestConsoleNoiseFilter`, which
checks both directions — denylisted prefixes are hidden, an unnamed logger still
prints — and by `TestMcpStreamKeepsInfoOnStderr` for the host channel.
