# ruff: noqa: S101
"""E2E read-only tests — commands that don't mutate state.

Covers three groups:
- No-DB commands: profile list/show, import preview, logs, mcp config, db ps
- Stub commands: reserved sync/budget commands that print "not implemented"
- DB commands: read-only queries against the shared e2e_profile database
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin import error_codes
from moneybin.cli.main import app
from moneybin.tables import FCT_TRANSACTION_PROVENANCE
from tests.e2e.conftest import FIXTURES_DIR, run_cli

pytestmark = pytest.mark.e2e

# In-process runner for tests that exercise pure CLI argument parsing
# (--help output, stub messages). Subprocess invocation is reserved for
# tests that need real boot/wiring coverage — DB-touching read commands
# and env-dependent NoDB commands.
_runner = CliRunner()


# ---------------------------------------------------------------------------
# No-DB commands — execute real logic without get_database()
# ---------------------------------------------------------------------------


class TestNoDBCommands:
    """Commands that run without an initialized database."""

    def test_profile_list(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("profile", "list", env=e2e_env)
        result.assert_success()

    def test_profile_show(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("profile", "show", env=e2e_env)
        # exit_code may be 0 or 1 depending on whether a profile is set
        assert "Traceback" not in result.stderr

    def test_import_formats_list(self) -> None:
        result = run_cli("import", "formats", "list")
        result.assert_success()

    def test_import_preview(self) -> None:
        fixture = FIXTURES_DIR / "tabular" / "standard.csv"
        result = run_cli("import", "preview", str(fixture))
        result.assert_success()

    def test_logs_path(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("logs", "--print-path", env=e2e_env)
        result.assert_success()

    def test_named_profile_writes_its_log_file(
        self, e2e_env: dict[str, str], e2e_home: Path
    ) -> None:
        """A named profile logs to disk — the path this whole suite runs on.

        ``e2e_env`` sets ``MONEYBIN_PROFILE``, which takes the same branch
        as ``-p``. That branch once skipped profile-aware logging setup
        outright, so every command in this file ran writing no log file at
        all, and SQLMesh's per-statement INFO went to the terminal instead.
        Only a subprocess can catch it: the in-process suite pins an active
        profile, which suppresses the very resolution under test.
        """
        run_cli("logs", "--print-path", env=e2e_env).assert_success()

        logs_dir = e2e_home / "profiles" / "e2e-test" / "logs"
        found = sorted(p.name for p in logs_dir.glob("cli_*.log"))

        assert found, f"no cli_*.log written under {logs_dir}"

    def test_logs_tail(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("logs", "cli", "--lines", "5", env=e2e_env)
        # May exit 0 or 1 if no log files exist yet — no crash is the bar
        assert "Traceback" not in result.stderr

    def test_logs_clean_dry_run(self, e2e_env: dict[str, str]) -> None:
        result = run_cli(
            "logs", "--prune", "--older-than", "30d", "--dry-run", env=e2e_env
        )
        result.assert_success()

    def test_logs_bare_invocation_skips_wizard(self, tmp_path: Path) -> None:
        """Bare ``moneybin logs`` surfaces a usage error, not the wizard.

        Pointed at an empty MONEYBIN_HOME with no MONEYBIN_PROFILE: a normal
        command would invoke ``ensure_default_profile()`` and prompt for a
        profile name on stdin. The leaf's missing-arg check must fire first.
        """
        env = {"MONEYBIN_HOME": str(tmp_path)}
        result = run_cli("logs", env=env)
        assert result.exit_code == 2, result.output
        assert "Missing argument" in result.stderr
        assert "Welcome to MoneyBin" not in result.output

    def test_mcp_list_prompts(self) -> None:
        result = run_cli("mcp", "list-prompts")
        result.assert_success()

    def test_mcp_config_path_resolves_an_ambient_profile(
        self, e2e_env: dict[str, str]
    ) -> None:
        """A globally-named profile satisfies a profile-scoped client path.

        ``mcp config path --client claude-code`` reads the active profile
        through ``get_current_profile(auto_resolve=False)``. That opts out of
        the lazy resolver to avoid the first-run wizard — not out of a profile
        the user named explicitly. ``scripts/claude-mcp.sh`` (``make
        claude-mcp``) shells out to exactly this command with no local
        ``--profile``, so an ambient ``MONEYBIN_PROFILE`` has to satisfy it.

        Only a subprocess catches this: the in-process suite pins an active
        profile, so ``auto_resolve=False`` always finds one there.
        """
        result = run_cli(
            "mcp", "config", "path", "--client", "claude-code", env=e2e_env
        )

        result.assert_success()
        assert "e2e-test" in result.stdout

    def test_mcp_config_show(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("mcp", "config", env=e2e_env)
        result.assert_success()

    def test_mcp_install_print(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("mcp", "install", "--print", env=e2e_env)
        result.assert_success()

    def test_sync_status_unreachable_server_fails_cleanly(
        self, e2e_env: dict[str, str]
    ) -> None:
        """`moneybin sync status` with an unreachable server must exit cleanly.

        Exit non-zero without a Python traceback — handle_cli_errors should
        classify the connection failure into a user-facing message.

        Covers boot/wiring for the live sync commands removed from
        TestStubCommands: catches import-time regressions in sync.py,
        Typer subcommand registration, and SyncClient construction even
        though the actual HTTP request fails.
        """
        env = {
            **e2e_env,
            # 127.0.0.1:1 is reserved and refuses connections fast.
            "MONEYBIN_SYNC__SERVER_URL": "http://127.0.0.1:1",
            # This test asserts direct-dial behavior, so scope the
            # environment to no-proxy regardless of what the parent
            # process inherited (e.g., Claude Code's sandbox SOCKS
            # proxy). Real users don't run with a proxy; the test must
            # exercise the same path they will.
            "HTTPS_PROXY": "",
            "HTTP_PROXY": "",
            "ALL_PROXY": "",
            "NO_PROXY": "*",
            "https_proxy": "",
            "http_proxy": "",
            "all_proxy": "",
            "no_proxy": "*",
        }
        result = run_cli("sync", "status", env=env, timeout=15)
        assert result.exit_code != 0
        assert "Traceback" not in result.stderr

    def test_db_ps(self) -> None:
        result = run_cli("db", "ps")
        result.assert_success()


# ---------------------------------------------------------------------------
# Stub commands — reserved CLI namespace, not yet implemented
# ---------------------------------------------------------------------------


class TestStubCommands:
    """Stubs should print a message and exit 0, not crash.

    Run in-process via CliRunner: stub bodies are pure log + return, with
    no env, DB, or filesystem access — subprocess isolation adds cost
    without coverage. Subprocess-level boot is exercised by the smoke in
    ``test_e2e_help.py`` and the mutating/workflow tiers.
    """

    @pytest.mark.parametrize(
        "cmd",
        [
            ["sync", "key", "rotate"],
            ["sync", "schedule", "set"],
            ["sync", "schedule", "show"],
            ["sync", "schedule", "remove"],
            ["budget", "set", "Food", "500"],
            ["budget", "delete", "Food"],
            # Only the explicit walk is a stub; a bare `review` prints counts.
            ["review", "--interactive"],
        ],
        ids=lambda c: " ".join(c),
    )
    def test_stub_exits_cleanly(self, cmd: list[str]) -> None:
        result = _runner.invoke(app, cmd)
        assert result.exit_code == 0, (
            f"stub {cmd} exited {result.exit_code}\noutput: {result.output}"
        )
        assert "not yet implemented" in result.output.lower()


# ---------------------------------------------------------------------------
# DB commands — read-only queries on the shared e2e_profile database
# ---------------------------------------------------------------------------


class TestDBReadOnlyCommands:
    """Commands that query the database but don't modify it."""

    # ── db ──────────────────────────────────────────────────────────────

    def test_db_info(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "info", env=e2e_profile)
        result.assert_success()

    def test_db_query(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "query", "SELECT 1 AS ok", env=e2e_profile)
        result.assert_success()

    def test_sql_query(self, e2e_profile: dict[str, str]) -> None:
        # Privacy-safe ad-hoc SQL exercises the full data path (read-only gate
        # → lineage → execute → redact). A literal SELECT is data-independent,
        # so it works on a fresh profile where core.* isn't yet materialized.
        result = run_cli("sql", "query", "SELECT 1 AS ok", env=e2e_profile)
        result.assert_success()

    def test_sql_query_rejects_nonallowed_schema(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """The subprocess twin of the CliRunner refusal test, asserted the same way.

        Names a real table (`meta.fct_transaction_provenance`, via the
        `moneybin.tables` constant) and asserts the refusal's error CODE, not
        just a non-zero exit. The previous fixture named a table that exists
        nowhere and asserted only the exit code, so deleting the schema gate
        left it failing on an unknown table and still passing.

        Re-aimed from raw.* when M2O.2 admitted raw/prep through this gate.
        """
        result = run_cli(
            "sql",
            "query",
            f"SELECT transaction_id FROM {FCT_TRANSACTION_PROVENANCE.full_name}",  # noqa: S608  # TableRef constant; the query is the test input, and the gate refuses it
            "--output",
            "json",
            env=e2e_profile,
        )

        assert result.exit_code != 0, result.output
        payload = json.loads(result.stdout)
        assert payload["error"]["code"] == error_codes.SQL_SCHEMA_NOT_ALLOWED

    def test_db_key(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "key", "show", env=e2e_profile)
        result.assert_success()

    def test_db_migrate_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "migrate", "status", env=e2e_profile)
        result.assert_success()

    # ── transform ───────────────────────────────────────────────────────

    def test_transform_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transform", "status", env=e2e_profile)
        result.assert_success()

    def test_transform_validate(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transform", "validate", env=e2e_profile)
        result.assert_success()

    def test_transform_plan(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transform", "plan", env=e2e_profile)
        result.assert_success()

    # ── import ──────────────────────────────────────────────────────────

    def test_import_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("import", "status", env=e2e_profile)
        result.assert_success()

    def test_import_history(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("import", "history", env=e2e_profile)
        result.assert_success()

    def test_import_show_format(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("import", "formats", "show", "tiller", env=e2e_profile)
        result.assert_success()

    # ── transactions categorize ──────────────────────────────────────────

    def test_categorize_assist(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli(
            "transactions", "categorize", "assist", "--output", "json", env=e2e_profile
        )
        result.assert_success()
        import json

        envelope = json.loads(result.stdout)
        assert "data" in envelope
        # CatAssistPayload wraps rows under `transactions`.
        assert isinstance(envelope["data"]["transactions"], list)

    def test_categorize_stats(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transactions", "categorize", "stats", env=e2e_profile)
        result.assert_success()

    def test_categorize_rules_list(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transactions", "categorize", "rules", "list", env=e2e_profile)
        result.assert_success()

    def test_categorize_rules_list_conflicts(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli(
            "transactions", "categorize", "rules", "list-conflicts", env=e2e_profile
        )
        result.assert_success()

    # ── privacy (read paths) ─────────────────────────────────────────────

    def test_privacy_status(self, e2e_profile: dict[str, str]) -> None:
        # status opens the DB read-only (get_database(read_only=True)).
        result = run_cli("privacy", "status", env=e2e_profile)
        result.assert_success()

    def test_privacy_log(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("privacy", "log", env=e2e_profile)
        result.assert_success()

    # ── matches ─────────────────────────────────────────────────────────

    def test_matches_history(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transactions", "matches", "history", env=e2e_profile)
        result.assert_success()

    # ── system ──────────────────────────────────────────────────────────

    def test_system_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("system", "status", env=e2e_profile)
        result.assert_success()

    def test_system_status_json(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("system", "status", "--output", "json", env=e2e_profile)
        result.assert_success()

    # ── mcp ─────────────────────────────────────────────────────────────

    def test_mcp_list_tools(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("mcp", "list-tools", env=e2e_profile)
        result.assert_success()

    # ── accounts / reports help-substring checks (in-process) ──────────
    # Verify each command registers and exposes its documented flags. The
    # shared e2e_profile has no transforms run yet so core.dim_accounts /
    # fct_balances_daily / reports.net_worth do not exist; read commands
    # that require those tables are covered at help-tier only. Write
    # commands (rename, include, archive, set, balance assert/delete/list)
    # are covered in test_e2e_mutating.py which uses isolated envs.
    #
    # Runs via CliRunner because --help is side-effect free per
    # ``.claude/rules/cli.md`` and doesn't need subprocess isolation.

    def test_accounts_list_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "list", "--help"])
        assert result.exit_code == 0, result.output
        assert "--output" in result.output

    def test_accounts_get_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "get", "--help"])
        assert result.exit_code == 0, result.output
        assert "account_id" in result.output.lower() or "ACCOUNT_ID" in result.output

    def test_accounts_resolve_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "resolve", "--help"])
        assert result.exit_code == 0, result.output
        assert "--limit" in result.output
        assert "--output" in result.output

    def test_accounts_balance_show_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "balance", "show", "--help"])
        assert result.exit_code == 0, result.output
        assert "--output" in result.output

    def test_accounts_balance_history_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "balance", "history", "--help"])
        assert result.exit_code == 0, result.output
        assert "--account" in result.output

    def test_accounts_balance_list_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "balance", "list", "--help"])
        assert result.exit_code == 0, result.output
        assert "--output" in result.output

    def test_accounts_balance_assert_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "balance", "assert", "--help"])
        assert result.exit_code == 0, result.output
        assert "--output" not in result.output or "account_id" in result.output.lower()

    def test_accounts_balance_assertion_delete_help(self) -> None:
        result = _runner.invoke(
            app, ["accounts", "balance", "assertion-delete", "--help"]
        )
        assert result.exit_code == 0, result.output
        assert "--yes" in result.output or "assertion_date" in result.output.lower()

    def test_accounts_balance_reconcile_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "balance", "reconcile", "--help"])
        assert result.exit_code == 0, result.output
        assert "--threshold" in result.output

    def test_reports_list_spans_the_registry(self, e2e_profile: dict[str, str]) -> None:
        """The catalog listing reaches all three tiers through one command."""
        result = run_cli("reports", "list", "--output", "json", env=e2e_profile)
        result.assert_success()
        entries = json.loads(result.stdout)["data"]
        assert "core:spending" in {entry["report_id"] for entry in entries}
        assert {entry["tier"] for entry in entries} == {"builtin"}

    def test_reports_networth_help(self) -> None:
        result = _runner.invoke(app, ["reports", "networth", "--help"])
        assert result.exit_code == 0, result.output
        assert "--output" in result.output

    def test_reports_networth_history_help(self) -> None:
        result = _runner.invoke(app, ["reports", "networth-history", "--help"])
        assert result.exit_code == 0, result.output
        assert "--from" in result.output

    # ── review ──────────────────────────────────────────────────────────

    def test_review_status(self, e2e_profile: dict[str, str]) -> None:
        """`moneybin review --status` prints three queue counts and exits 0."""
        result = run_cli("review", "--status", env=e2e_profile)
        result.assert_success()
        out = result.stdout.lower()
        # All three queues appear in text output
        assert "match" in out
        assert "categori" in out
        assert "account" in out or "link" in out

    def test_review_status_json(self, e2e_profile: dict[str, str]) -> None:
        """`moneybin review --status --output json` returns a five-field envelope."""
        import json

        result = run_cli("review", "--status", "--output", "json", env=e2e_profile)
        result.assert_success()
        envelope = json.loads(result.stdout)
        payload = envelope["data"]
        assert "matches_pending" in payload
        assert "categorize_pending" in payload
        assert "account_links_pending" in payload
        assert "merchant_links_pending" in payload
        assert "security_links_pending" in payload
        assert "total" in payload
        assert payload["total"] == (
            payload["matches_pending"]
            + payload["categorize_pending"]
            + payload["account_links_pending"]
            + payload["merchant_links_pending"]
            + payload["security_links_pending"]
        )

    def test_review_type_account_links_status(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """`moneybin review --type account-links --status` exits 0 and shows account-link count."""
        result = run_cli(
            "review", "--type", "account-links", "--status", env=e2e_profile
        )
        result.assert_success()
        out = result.stdout.lower()
        assert "account" in out or "link" in out or "decision" in out

    # ── accounts links (read-only) ───────────────────────────────────────

    def test_accounts_links_pending(self, e2e_profile: dict[str, str]) -> None:
        """`moneybin accounts links pending` exits 0 on an empty queue."""
        result = run_cli("accounts", "links", "pending", env=e2e_profile)
        result.assert_success()

    def test_accounts_links_pending_json(self, e2e_profile: dict[str, str]) -> None:
        """`moneybin accounts links pending --output json` returns an envelope with groups[] and n_pending."""
        import json

        result = run_cli(
            "accounts", "links", "pending", "--output", "json", env=e2e_profile
        )
        result.assert_success()
        envelope = json.loads(result.stdout)
        assert "data" in envelope
        assert "groups" in envelope["data"]
        assert isinstance(envelope["data"]["groups"], list)
        assert "n_pending" in envelope["data"]

    def test_accounts_links_history(self, e2e_profile: dict[str, str]) -> None:
        """`moneybin accounts links history` exits 0 on a fresh profile."""
        result = run_cli("accounts", "links", "history", env=e2e_profile)
        result.assert_success()

    def test_accounts_links_history_json(self, e2e_profile: dict[str, str]) -> None:
        """`moneybin accounts links history --output json` returns an envelope with decisions[]."""
        import json

        result = run_cli(
            "accounts", "links", "history", "--output", "json", env=e2e_profile
        )
        result.assert_success()
        envelope = json.loads(result.stdout)
        assert "data" in envelope
        assert "decisions" in envelope["data"]
        assert isinstance(envelope["data"]["decisions"], list)

    # ── merchants links (read-only) ──────────────────────────────────────

    def test_merchants_links_pending(self, e2e_profile: dict[str, str]) -> None:
        """`moneybin merchants links pending` exits 0 on an empty queue."""
        result = run_cli("merchants", "links", "pending", env=e2e_profile)
        result.assert_success()

    def test_merchants_links_pending_json(self, e2e_profile: dict[str, str]) -> None:
        """`moneybin merchants links pending --output json` returns an envelope with groups[] and n_pending."""
        import json

        result = run_cli(
            "merchants", "links", "pending", "--output", "json", env=e2e_profile
        )
        result.assert_success()
        envelope = json.loads(result.stdout)
        assert "data" in envelope
        assert "groups" in envelope["data"]
        assert isinstance(envelope["data"]["groups"], list)
        assert "n_pending" in envelope["data"]

    def test_merchants_links_history(self, e2e_profile: dict[str, str]) -> None:
        """`moneybin merchants links history` exits 0 on a fresh profile."""
        result = run_cli("merchants", "links", "history", env=e2e_profile)
        result.assert_success()

    def test_merchants_links_history_json(self, e2e_profile: dict[str, str]) -> None:
        """`moneybin merchants links history --output json` returns an envelope with decisions[]."""
        import json

        result = run_cli(
            "merchants", "links", "history", "--output", "json", env=e2e_profile
        )
        result.assert_success()
        envelope = json.loads(result.stdout)
        assert "data" in envelope
        assert "decisions" in envelope["data"]
        assert isinstance(envelope["data"]["decisions"], list)

    # ── investments prices (read-only) ─────────────────────────────────────

    def test_investments_prices_list_unknown_security_exits_nonzero(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """An unresolvable reference is a user error, not an empty result.

        Returning nothing would read as "this security has no prices" when the
        security itself does not exist.
        """
        result = run_cli(
            "investments", "prices", "list", "NOSUCHTICKER", env=e2e_profile
        )
        assert result.exit_code != 0

    def test_investments_prices_list_rejects_a_malformed_since(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """A bad --since is a usage error (exit 2), distinct from a runtime failure."""
        result = run_cli(
            "investments",
            "prices",
            "list",
            "AAPL",
            "--since",
            "last-tuesday",
            env=e2e_profile,
        )
        assert result.exit_code == 2

    # ── fx (read-only) ─────────────────────────────────────────────────────

    def test_fx_list_is_empty_on_a_fresh_profile(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """An uncached pair is an empty series, not a failure.

        `list` reads what is on disk and never fetches, so a fresh profile is
        the ordinary case rather than an error — the command that seeds a rate
        is `fx rate`.
        """
        result = run_cli(
            "fx", "list", "USD", "EUR", "--output", "json", env=e2e_profile
        )
        result.assert_success()
        assert json.loads(result.stdout)["data"]["rows"] == []

    def test_fx_list_rejects_a_malformed_since(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """A bad --since is a usage error (exit 2), distinct from a runtime failure."""
        result = run_cli(
            "fx", "list", "USD", "EUR", "--since", "last-tuesday", env=e2e_profile
        )
        assert result.exit_code == 2

    def test_fx_rate_rejects_a_malformed_date(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """Exit 2 before anything is opened or fetched.

        Read-only tier despite `fx rate` being able to write the rate cache:
        this invocation never reaches the database, which is the behavior under
        test.
        """
        result = run_cli("fx", "rate", "USD", "EUR", "15-03-2026", env=e2e_profile)
        assert result.exit_code == 2
        assert "Traceback (most recent call last)" not in result.stderr

    def test_fx_rate_rejects_a_currency_that_is_not_a_code(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """A malformed code fails loudly instead of matching nothing forever.

        Reported as a runtime error rather than a usage one because the service
        owns currency validation — the CLI does not keep a second copy of the
        ISO-4217 rule to check against first.
        """
        result = run_cli("fx", "rate", "US", "EUR", "2026-03-13", env=e2e_profile)
        assert result.exit_code == 1
        assert "Traceback (most recent call last)" not in result.stderr

    # ── investments securities links (read-only) ───────────────────────────

    def test_investments_securities_links_pending(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """`moneybin investments securities links pending` exits 0 on an empty queue."""
        result = run_cli(
            "investments", "securities", "links", "pending", env=e2e_profile
        )
        result.assert_success()

    def test_investments_securities_links_pending_json(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """`moneybin investments securities links pending --output json` returns an envelope with groups[] and n_pending."""
        import json

        result = run_cli(
            "investments",
            "securities",
            "links",
            "pending",
            "--output",
            "json",
            env=e2e_profile,
        )
        result.assert_success()
        envelope = json.loads(result.stdout)
        assert "data" in envelope
        assert "groups" in envelope["data"]
        assert isinstance(envelope["data"]["groups"], list)
        assert "n_pending" in envelope["data"]

    def test_investments_securities_links_history(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """`moneybin investments securities links history` exits 0 on a fresh profile."""
        result = run_cli(
            "investments", "securities", "links", "history", env=e2e_profile
        )
        result.assert_success()

    def test_investments_securities_links_history_json(
        self, e2e_profile: dict[str, str]
    ) -> None:
        """`moneybin investments securities links history --output json` returns an envelope with decisions[]."""
        import json

        result = run_cli(
            "investments",
            "securities",
            "links",
            "history",
            "--output",
            "json",
            env=e2e_profile,
        )
        result.assert_success()
        envelope = json.loads(result.stdout)
        assert "data" in envelope
        assert "decisions" in envelope["data"]
        assert isinstance(envelope["data"]["decisions"], list)

    # ── stats ───────────────────────────────────────────────────────────

    def test_stats_show(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("stats", env=e2e_profile)
        result.assert_success()
