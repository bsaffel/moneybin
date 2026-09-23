"""CLI tests for `moneybin import inbox` subcommands."""

from __future__ import annotations

import json
import shlex
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import Result
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.services.inbox_service import (
    InboxListResult,
    InboxSyncResult,
)


@contextmanager
def _fake_db_ctx(**kwargs: object) -> Generator[object, None, None]:
    yield object()


@contextmanager
def _fake_get_database(**kwargs: object) -> Generator[object, None, None]:
    yield MagicMock()


@pytest.fixture
def patch_inbox(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> MagicMock:
    """Patch InboxService, get_database, get_settings, handle_cli_errors to skip the real DB."""
    fake = MagicMock()
    fake.root = tmp_path / "inbox-root"

    fake_cls = MagicMock(return_value=fake)
    fake_cls.for_active_profile_no_db = MagicMock(return_value=fake)
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox.InboxService",
        fake_cls,
    )
    monkeypatch.setattr("moneybin.cli.utils.handle_cli_errors", _fake_db_ctx)
    monkeypatch.setattr("moneybin.database.get_database", _fake_get_database)
    monkeypatch.setattr("moneybin.config.get_settings", lambda: MagicMock())
    return fake


def test_inbox_drain_prints_summary(runner: CliRunner, patch_inbox: MagicMock) -> None:
    """Draining the inbox prints 'N imported, M failed' summary."""
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[{"filename": "chase-checking/march.csv", "transactions": 47}],
        failed=[],
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 0, result.stderr
    # The complete operation receipt stays on stdout for redirection.
    assert "chase-checking/march.csv" in result.stdout
    assert "Imported" in result.stdout
    assert "Failed" in result.stdout


def test_inbox_drain_warns_about_a_retired_transfer_in_both_output_modes(
    runner: CliRunner,
    patch_inbox: MagicMock,
) -> None:
    """The drain says the reversal aloud whichever output mode the caller picked.

    `inbox_default` already places this warning ahead of the `quiet` return,
    because the drain is the least supervised surface reaching the
    reconciliation. `--output json` returned before ever reaching it, so the
    guarantee held against `-q` and not against the mode an agent actually
    uses. The count rides in the payload either way; the warning is what names
    `system audit undo`.
    """
    for mode in (
        ["import", "inbox"],
        ["import", "inbox", "--quiet"],
        ["import", "inbox", "--output", "json"],
    ):
        patch_inbox.sync.return_value = InboxSyncResult(
            processed=[{"filename": "chase-checking/march.csv", "transactions": 47}],
            failed=[],
            transfers_retired=2,
        )
        result = runner.invoke(app, mode)
        assert result.exit_code == 0, result.stderr
        assert "Retired 2 previously accepted transfer(s)" in result.stderr, mode
        assert "moneybin system audit undo" in result.stderr, mode


def test_inbox_drain_reports_the_best_effort_steps_its_refresh_ran(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """The drain runs four best-effort steps and reported none of them.

    ``InboxService.sync`` closes with ``run_refresh(steps=None)``, so a watched
    folder quietly reaches the network for exchange rates. Nobody watches a
    watched folder, so this is the surface where a swallowed provider outage
    would sit longest.
    """
    from moneybin.services.refresh_outcome import RefreshStepOutcome, StageOutcome

    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[{"filename": "chase-checking/march.csv", "transactions": 47}],
        failed=[],
        refresh_steps=RefreshStepOutcome(
            stages=(StageOutcome(step="match", ran=True, error="matcher blew up"),),
            rate_pairs_unsupported=("EUR/XTS",),
        ),
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 0, result.stderr
    assert "Matching step failed" in result.stderr
    assert "EUR/XTS" in result.stderr
    assert "moneybin fx set" in result.stderr


def test_inbox_drain_failure_exits_nonzero_but_keeps_failure_facts(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """A failed requested drain is nonzero without discarding its recovery facts."""
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[],
        failed=[
            {
                "filename": "x.csv",
                "error_code": "transform_error",
                "sidecar": "failed/2026-05/x.csv.error.yml",
            }
        ],
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 1
    assert "transform_error" in result.stdout
    assert "Imported" in result.stdout
    assert "Failed" in result.stdout


@pytest.mark.parametrize("transforms_error", ["refresh failed", ""])
def test_inbox_drain_quiet_preserves_its_receipt_and_transform_recovery(
    runner: CliRunner, patch_inbox: MagicMock, transforms_error: str
) -> None:
    """The drain has no routine chatter; quiet preserves results and recovery facts."""
    results: list[Result] = []
    for args in (["import", "inbox"], ["import", "inbox", "--quiet"]):
        patch_inbox.sync.return_value = InboxSyncResult(
            processed=[{"filename": "march.csv", "transactions": 47}],
            failed=[],
            transforms_error=transforms_error,
        )
        results.append(runner.invoke(app, args))

    normal, quiet = results
    assert normal.exit_code == quiet.exit_code == 1
    assert quiet.stdout == normal.stdout
    assert "march.csv" in quiet.stdout
    assert "Derived data may be stale" in quiet.stdout
    assert "moneybin transform plan" in quiet.stdout


def test_inbox_drain_renders_pending_files(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """Pending files (confirmation_required) must appear in text output.

    Before this rendering existed, a confirmation_required outcome on an
    inbox file was silently invisible: the file moved to pending/ and a
    sidecar was written, but the user saw "0 imported, 0 failed" with no
    pointer to the import-confirm command.
    """
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[],
        failed=[],
        pending=[
            {
                "filename": "unknown-statement.csv",
                "channel": "tabular",
                "tier": "medium",
                "score": 0.72,
                "reason": "unknown_layout",
                "moved_to": "pending/2026-05/unknown-statement.csv",
                "sidecar": "pending/2026-05/unknown-statement.csv.pending.yml",
            }
        ],
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 1, result.stderr
    assert "unknown-statement.csv" in result.stdout
    assert "Pending confirmation" in result.stdout
    assert "moneybin import confirm" in result.stdout
    # Non-low tier: --accept ratifies the detected mapping.
    assert "--accept" in result.stdout
    assert "Pending confirmation" in result.stdout


def test_inbox_drain_low_tier_mapping_hint_omits_accept(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """A low-tier mapping confirmation points at --mapping, never --accept.

    resolve_or_confirm re-surfaces low-tier proposals on --accept (it never
    loads them), so an --accept hint would loop the user; only --mapping works.
    """
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[],
        failed=[],
        pending=[
            {
                "filename": "fuzzy.csv",
                "channel": "tabular",
                "tier": "low",
                "score": 0.3,
                "reason": "unknown_layout",
                "moved_to": "pending/2026-05/fuzzy.csv",
                "sidecar": "pending/2026-05/fuzzy.csv.pending.yml",
            }
        ],
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 1, result.stderr
    assert "fuzzy.csv" in result.stdout
    # The suggested command points at --mapping, not --accept. (The only
    # "--accept" in the output is the explanatory "--accept would be rejected".)
    assert "fuzzy.csv --mapping" in result.stdout
    assert "fuzzy.csv --accept" not in result.stdout


def test_inbox_drain_header_position_ambiguous_routes_on_reason_not_tier(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """header_position_ambiguous must recommend --accept despite tier="low".

    ``_gate_header_position_ambiguous`` always packs this reason with
    tier="low" (Confidence(score=0.0, tier="low", ...)), so a generic
    low-tier branch would wrongly claim "--accept would be rejected" —
    exactly backwards, since --accept is the recovery this reason's own
    gate ratifies on, and the one the persisted sidecar
    (inbox_service.py) already recommends. Must also NOT recommend
    `import files ... --confirm`: that command never archives the
    pending file, so the next inbox sync would reprocess it and
    duplicate every transaction just loaded.
    """
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[],
        failed=[],
        pending=[
            {
                "filename": "data_before_header.csv",
                "channel": "tabular",
                "tier": "low",
                "score": 0.0,
                "reason": "header_position_ambiguous",
                "moved_to": "pending/2026-05/data_before_header.csv",
                "sidecar": "pending/2026-05/data_before_header.csv.pending.yml",
                "header_position_ambiguous_rows": [
                    {
                        "transaction_date": "2026-01-01",
                        "amount": "42.50",
                        "description": "Coffee",
                    }
                ],
            }
        ],
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 1, result.stderr
    assert "data_before_header.csv --accept" in result.stdout
    assert "would be rejected" not in result.stdout
    assert "--mapping" not in result.stdout
    assert "import files" not in result.stdout
    # The disputed row is live drain-summary data, not part of the
    # row-free persisted sidecar — the drain must still show it.
    # Already allowlisted (disputed_row_fields) by the time it reaches
    # here, so the rendering is dest=value pairs, not raw positional cells.
    assert "transaction_date" in result.stdout
    assert "Coffee" in result.stdout


def test_inbox_drain_json_output(runner: CliRunner, patch_inbox: MagicMock) -> None:
    """--output json emits a JSON envelope with sync payload."""
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[{"filename": "a.csv", "transactions": 3}],
    )

    result = runner.invoke(app, ["import", "inbox", "--output", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["processed"][0]["filename"] == "a.csv"
    # No pending entries → only paths and counts → low.
    assert payload["summary"]["sensitivity"] == "low"


def test_inbox_drain_busy_is_nonzero_in_json_with_the_service_payload(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """A blocked requested drain keeps its JSON facts but cannot report success."""
    patch_inbox.sync.return_value = InboxSyncResult(
        skipped=[{"filename": "statement.csv", "reason": "inbox_busy"}]
    )

    result = runner.invoke(app, ["import", "inbox", "--output", "json"])

    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout)["data"]["skipped"] == [
        {"filename": "statement.csv", "reason": "inbox_busy"}
    ]


def test_inbox_drain_interrupt_reports_unknown_saved_scope(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """Ctrl-C during a drain cannot claim that earlier file work was rolled back."""
    patch_inbox.sync.side_effect = KeyboardInterrupt

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 130, result.output
    assert "Inbox drain cancelled" in result.stdout
    assert "Saved scope is unknown" in result.stdout


def test_inbox_drain_interrupt_returns_json_failure(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """A script receives a parseable cancellation envelope, not a text receipt."""
    patch_inbox.sync.side_effect = KeyboardInterrupt

    result = runner.invoke(app, ["import", "inbox", "--output", "json"])

    assert result.exit_code == 130, result.output
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "import_interrupted"
    assert payload["error"]["details"] == {
        "outcome": "cancelled",
        "saved_scope": "unknown",
    }


def test_inbox_drain_receipt_never_opens_a_pager(
    runner: CliRunner, patch_inbox: MagicMock, mocker: MockerFixture
) -> None:
    """A long drain receipt stays fully on stdout rather than entering a pager."""
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[
            {"filename": f"file-{index}.csv", "transactions": index}
            for index in range(20)
        ]
    )
    mocker.patch(
        "moneybin.cli.utils.get_terminal_policy",
        return_value=MagicMock(
            output="text", page=True, width=80, height=1, style=False, color=False
        ),
    )
    pager = mocker.patch("moneybin.cli.pager.page_text")

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 0, result.output
    assert "file-19.csv" in result.stdout
    pager.assert_not_called()


def test_inbox_drain_json_pending_is_medium_sensitivity(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """An account_confirmation pending carries an account number — declare critical.

    The CLI has no privacy middleware, so the envelope's declared
    ``summary.sensitivity`` is the only tier signal a JSON consumer sees.

    Reading this row for its display names alone under-declares it by two
    tiers: ``account_proposals[].source_account_key`` is ACCOUNT_IDENTIFIER
    (on OFX, the ``<ACCTID>`` the institution issued), which is CRITICAL, not
    the DESCRIPTION-tier candidate labels beside it. ``_masked_pending`` masks
    the value, but ``dataclasses.asdict`` leaves a bare dict, so
    ``render_or_json`` can derive neither the tier nor ``classes_returned``
    from it — whatever this branch declares is what the JSON summary and the
    privacy-audit row say. MCP's typed ``ImportInboxSyncPayload`` calls the
    same bytes critical, and the two surfaces must not disagree (``cli.md``).
    """
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[],
        pending=[
            {
                "filename": "statement.csv",
                "reason": "account_confirmation",
                "tier": "high",
                "moved_to": "pending/2026-05/statement.csv",
                "sidecar": "pending/2026-05/statement.csv.pending.yml",
                "account_proposals": [
                    {
                        "source_account_key": "csv:abcd",
                        "candidates": [
                            {
                                "account_id": "9f8e7d6c5b4a",
                                "display_name": "Wells Fargo Checking ••3030",
                            }
                        ],
                    }
                ],
            }
        ],
    )

    result = runner.invoke(app, ["import", "inbox", "--output", "json"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["summary"]["sensitivity"] == "critical"


def test_inbox_drain_json_minted_account_is_medium_sensitivity(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """A minted account is medium even when nothing is pending.

    ``accounts_created[].display_name`` is the source's own label for an account
    the drain just minted (USER_NOTE/medium), and a clean drain that mints is
    the common first-import case — no pending entry to raise the tier on its
    behalf. The tier cannot be derived here either: the branch builds its
    payload with ``dataclasses.asdict``, so ``render_or_json`` sees a bare dict
    and leaves the ``low`` fallback standing in the privacy audit record.
    """
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[
            {
                "filename": "march.ofx",
                "transactions": 47,
                "accounts_created": [
                    {"account_id": "9f8e7d6c5b4a", "display_name": "Chase Checking"}
                ],
            }
        ],
        pending=[],
    )

    result = runner.invoke(app, ["import", "inbox", "--output", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["summary"]["sensitivity"] == "medium"


def test_inbox_drain_names_accounts_it_minted(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """A silently minted account must be named on the drain, with its recoveries.

    ``account-identity-resolution.md`` gates the merge, not the mint, and pays
    for that by requiring every surface to name what it created. The drain is
    the most unattended surface in the product — an account minted here is
    exactly the one nobody watched appear — so omitting it is where "magic stays
    visible" fails first.
    """
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[
            {
                "filename": "chase-checking/march.ofx",
                "transactions": 47,
                "accounts_created": [
                    {"account_id": "9f8e7d6c5b4a", "display_name": "Chase Checking"}
                ],
            }
        ],
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 0, result.stderr
    combined = result.stdout + result.stderr
    assert "9f8e7d6c5b4a" in combined, combined
    assert "Chase Checking" in combined, combined
    # Both recoveries the spec names, so a surprise account is correctable
    # without leaving the output that announced it.
    assert "accounts set" in combined, combined
    assert "accounts links run" in combined, combined


def test_inbox_drain_json_masks_the_institutions_account_number(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """The JSON envelope masks the account key the text renderer already masks.

    ``render_or_json`` starts its redaction walk only when
    ``type(envelope.data)`` declares a transform, and this branch hands it
    ``dataclasses.asdict(result)`` — a bare dict that declares nothing — so the
    walk never reaches the nested proposals. On the OFX channel
    ``source_account_key`` is the ``<ACCTID>`` the institution issued, so the
    machine-readable surface was shipping a real account number while the
    terminal beside it printed ``****6789``.
    """
    acctid = "000123456789"
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[],
        failed=[],
        pending=[
            {
                "filename": "statement.ofx",
                "channel": "ofx",
                "tier": "high",
                "score": 1.0,
                "reason": "account_confirmation",
                "moved_to": "pending/2026-05/statement.ofx",
                "sidecar": "pending/2026-05/statement.ofx.pending.yml",
                "account_proposals": [
                    {
                        "source_account_key": acctid,
                        "proposal_ref": "@0",
                        "proposed_account_id": "prov12345678",
                        "candidates": [],
                    }
                ],
            }
        ],
    )

    result = runner.invoke(app, ["import", "inbox", "--output", "json"])

    assert result.exit_code == 1, result.stderr
    assert acctid not in result.stdout, result.stdout
    proposal = json.loads(result.stdout)["data"]["pending"][0]["account_proposals"][0]
    assert proposal["source_account_key"] == "****6789"
    # Ours, not the institution's — RECORD_ID, and the half the caller types.
    assert proposal["proposal_ref"] == "@0"
    assert proposal["proposed_account_id"] == "prov12345678"


@pytest.mark.parametrize(
    "filename",
    ["statement.ofx", "statement with spaces;$(not-run).ofx"],
)
def test_the_drains_copyable_confirm_command_round_trips_ordinary_paths(
    runner: CliRunner, patch_inbox: MagicMock, tmp_path: Path, filename: str
) -> None:
    """The shell's parsed argv preserves ordinary paths and binding arguments."""
    pending_file = tmp_path / filename
    patch_inbox.sync.return_value = InboxSyncResult(
        pending=[
            {
                "filename": "statement.ofx",
                "channel": "ofx",
                "tier": "high",
                "score": 1.0,
                "reason": "account_confirmation",
                "moved_to": str(pending_file),
                "account_proposals": [
                    {
                        "source_account_key": "000123456789",
                        "proposal_ref": "@0",
                        "candidates": [],
                    }
                ],
            }
        ],
    )

    drain = runner.invoke(app, ["import", "inbox"])
    assert drain.exit_code == 1, drain.stderr

    hint = next(
        line
        for line in drain.stdout.splitlines()
        if line.startswith("moneybin import confirm ") and "--account-binding" in line
    )
    command = hint[hint.index("moneybin") :]
    # The two placeholders the caller substitutes: the ref is listed beside each
    # proposal, and the target is theirs to choose.
    argv = shlex.split(command.replace("@N=<account_id|new>", "@0=new"))[1:]

    # This is a shell-argv stub, not a second MoneyBin CLI invocation: it tests
    # the command text the shell receives without creating an import/profile.
    assert argv == [
        "import",
        "confirm",
        str(pending_file),
        "--accept",
        "--account-binding",
        "@0=new",
    ]


@pytest.mark.parametrize(
    "moved_to",
    [
        "pending/statement\nnext.csv",
        "pending/statement\x1b[2J.csv",
        "pending/statement\rnext.csv",
        "pending/statement\x07next.csv",
    ],
)
def test_inbox_drain_control_path_uses_a_safe_recovery_fallback(
    runner: CliRunner, patch_inbox: MagicMock, moved_to: str
) -> None:
    """Terminal-control paths must not become direct shell command output."""
    patch_inbox.sync.return_value = InboxSyncResult(
        pending=[
            {
                "filename": "statement.csv",
                "channel": "ofx",
                "tier": "high",
                "reason": "account_confirmation",
                "moved_to": moved_to,
                "account_proposals": [],
            }
        ]
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 1, result.output
    safe_text = " ".join(result.stdout.split())
    assert "terminal control" in safe_text
    assert "moneybin import inbox --output json" in safe_text
    assert "\x1b" not in result.stdout
    assert "\r" not in result.stdout
    assert "\x07" not in result.stdout
    assert not any(
        line.startswith("moneybin import confirm ")
        for line in result.stdout.splitlines()
    )


def test_inbox_drain_json_keeps_an_exact_control_path(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """The text fallback does not alter the existing machine-readable payload."""
    moved_to = "pending/statement\x1b[2J\nnext.csv"
    patch_inbox.sync.return_value = InboxSyncResult(
        pending=[
            {
                "filename": "statement.csv",
                "channel": "ofx",
                "tier": "high",
                "reason": "account_confirmation",
                "moved_to": moved_to,
                "account_proposals": [],
            }
        ]
    )

    result = runner.invoke(app, ["import", "inbox", "--output", "json"])

    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout)["data"]["pending"][0]["moved_to"] == moved_to
    assert "\x1b" not in result.stdout


def test_inbox_list_prints_would_process(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """`inbox list` shows each file that would be processed."""
    patch_inbox.enumerate.return_value = InboxListResult(
        would_process=[
            {"filename": "chase-checking/march.csv", "account_hint": "chase-checking"}
        ],
    )

    result = runner.invoke(app, ["import", "inbox", "list"])

    assert result.exit_code == 0
    assert "chase-checking/march.csv" in result.stdout


def test_inbox_list_quiet_keeps_scope_and_empty_recovery(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """An empty finite preview remains an actionable answer under --quiet."""
    patch_inbox.enumerate.return_value = InboxListResult()

    result = runner.invoke(app, ["import", "inbox", "list", "--quiet"])

    assert result.exit_code == 0, result.output
    assert "Inbox preview" in result.stdout
    assert "No files" in result.stdout
    assert "moneybin import inbox path" in result.stdout


def test_inbox_list_pages_complete_answer_unless_no_pager_is_requested(
    runner: CliRunner, patch_inbox: MagicMock, mocker: MockerFixture
) -> None:
    """The finite preview uses the pager boundary; its escape prints every file."""
    patch_inbox.enumerate.return_value = InboxListResult(
        would_process=[
            {"filename": f"account/file-{index}.csv", "account_hint": "account"}
            for index in range(20)
        ]
    )
    mocker.patch(
        "moneybin.cli.utils.get_terminal_policy",
        return_value=MagicMock(
            output="text", page=True, width=80, height=1, style=False, color=False
        ),
    )
    paged: list[str] = []

    def capture_page(text: str, *, color: bool, wide: bool) -> bool:
        del color, wide
        paged.append(text)
        return True

    mocker.patch("moneybin.cli.pager.page_text", capture_page)

    normal = runner.invoke(app, ["import", "inbox", "list"])
    bypass = runner.invoke(app, ["import", "inbox", "list", "--no-pager"])

    assert normal.exit_code == 0, normal.output
    assert bypass.exit_code == 0, bypass.output
    assert len(paged) == 1
    assert "file-19.csv" in paged[0]
    assert "file-19.csv" in bypass.stdout


def test_inbox_path_prints_active_profile_root(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """`inbox path` prints the service root directory."""
    result = runner.invoke(app, ["import", "inbox", "path"])

    assert result.exit_code == 0
    assert str(patch_inbox.root) in result.stdout.strip()


def test_inbox_path_quiet_stays_raw_for_command_substitution(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """Quiet cannot erase the requested raw path artifact."""
    result = runner.invoke(app, ["import", "inbox", "path", "--quiet"])

    assert result.exit_code == 0, result.output
    assert result.stdout == f"{patch_inbox.root}\n"


def test_inbox_drain_renders_account_confirmation_pending(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """An account_confirmation pending entry tells the user to bind/name the account.

    Asserts the --account-binding hint appears instead of the generic --mapping text.
    """
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[],
        failed=[],
        pending=[
            {
                "filename": "statement.csv",
                "channel": "tabular",
                "tier": "high",
                "score": 1.0,
                "reason": "account_confirmation",
                "moved_to": "pending/2026-05/statement.csv",
                "sidecar": "pending/2026-05/statement.csv.pending.yml",
            }
        ],
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 1, result.stderr
    assert "statement.csv" in result.stdout
    # --accept (ratifies the settled mapping) is paired with the binding so the
    # copy-pasted command passes the `import confirm` guard; no --mapping override.
    assert "--accept --account-binding" in result.stdout
    assert "Pending confirmation" in result.stdout
    assert "--mapping" not in result.stdout


def test_inbox_drain_names_each_proposal_by_its_ref(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """The drain listing shows the same referent the confirm gate shows.

    This renderer reads proposals out of a persisted sidecar rather than a
    ConfirmationRequired, so it is a second hand-rolled view of one object.
    Two views that describe an account differently is how a user ends up
    binding by a name only one of them accepts.
    """
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[],
        failed=[],
        pending=[
            {
                "filename": "statement.ofx",
                "channel": "ofx",
                "tier": "high",
                "score": 1.0,
                "reason": "account_confirmation",
                "moved_to": "pending/2026-05/statement.ofx",
                "sidecar": "pending/2026-05/statement.ofx.pending.yml",
                "account_proposals": [
                    {
                        "source_account_key": "chase-1234",
                        "proposal_ref": "@0",
                        "candidates": [],
                    }
                ],
            }
        ],
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 1, result.stderr
    assert "@0" in result.stdout, result.stdout
    # Masked, not raw: `source_account_key` is ACCOUNT_IDENTIFIER on every
    # channel, and on OFX — which this fixture is — it carries the <ACCTID> the
    # institution issued. stderr is not exempt from the redaction contract, so
    # the ref above is the half the user types and this is only the
    # disambiguator that tells two proposals apart.
    assert "chase-1234" not in result.stdout, result.stdout
    assert "****1234" in result.stdout, result.stdout


def test_inbox_drain_renders_candidate_ledger_overlap(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """The unattended inbox path shows the evidence needed to choose a candidate."""
    patch_inbox.sync.return_value = InboxSyncResult(
        pending=[
            {
                "filename": "statement.pdf",
                "channel": "pdf",
                "tier": "high",
                "score": 1.0,
                "reason": "account_confirmation",
                "moved_to": "pending/2024-01/statement.pdf",
                "account_proposals": [
                    {
                        "source_account_key": "pdf_doc_1234567890abcdef",
                        "proposal_ref": "@0",
                        "candidates": [
                            {
                                "account_id": "acct_existing01",
                                "display_name": "Checking",
                                "signal": "institution_last4",
                                "overlap_matched": 2,
                                "overlap_comparable": 2,
                                "overlap_window_start": "2024-01-15",
                                "overlap_window_end": "2024-01-20",
                            }
                        ],
                    }
                ],
            }
        ]
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 1, result.stderr
    compact = " ".join(result.stdout.split())
    assert "ledger overlap: 2/2 matched" in compact
    assert "2024-01-15 to 2024-01-20" in compact
