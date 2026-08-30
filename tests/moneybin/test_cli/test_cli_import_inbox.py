"""CLI tests for `moneybin import inbox` subcommands."""

from __future__ import annotations

import json
import shlex
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.services.import_service import ImportResult
from moneybin.services.inbox_service import (
    InboxListResult,
    InboxSyncResult,
)


def _make_import_result(**kwargs: Any) -> ImportResult:
    """Factory for ImportResult with sensible defaults."""
    defaults: dict[str, Any] = {
        "file_path": "statement.ofx",
        "file_type": "ofx",
        "accounts": 1,
        "transactions": 5,
        "import_id": "abc123",
    }
    defaults.update(kwargs)
    return ImportResult(**defaults)


@contextmanager
def _fake_db_ctx(**kwargs: object) -> Generator[object, None, None]:
    yield object()


@contextmanager
def _fake_get_database(**kwargs: object) -> Generator[object, None, None]:
    yield MagicMock()


@pytest.fixture
def runner() -> CliRunner:
    """Return a Typer CliRunner for invoking the root app."""
    return CliRunner()


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
    # Per-file ✓ lines on stdout (data); summary on stderr (status).
    assert "chase-checking/march.csv" in result.stdout
    assert "1 imported" in result.stderr
    assert "0 failed" in result.stderr


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
    for mode in (["import", "inbox"], ["import", "inbox", "--output", "json"]):
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
    from moneybin.services.refresh_outcome import RefreshStepOutcome

    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[{"filename": "chase-checking/march.csv", "transactions": 47}],
        failed=[],
        refresh_steps=RefreshStepOutcome(
            matching_error="matcher blew up",
            rate_pairs_unsupported=("EUR/XTS",),
        ),
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 0, result.stderr
    assert "Matching step failed" in result.stderr
    assert "EUR/XTS" in result.stderr
    assert "moneybin fx set" in result.stderr


def test_inbox_drain_failure_exits_zero_but_warns(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """Failed files exit 0 but display error_code in output."""
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

    assert result.exit_code == 0
    assert "transform_error" in result.stderr
    assert "0 imported" in result.stderr
    assert "1 failed" in result.stderr


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

    assert result.exit_code == 0, result.stderr
    assert "unknown-statement.csv" in result.stderr
    assert "pending confirmation" in result.stderr
    assert "moneybin import confirm" in result.stderr
    # Non-low tier: --accept ratifies the detected mapping.
    assert "--accept" in result.stderr
    assert "1 pending" in result.stderr


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

    assert result.exit_code == 0, result.stderr
    assert "fuzzy.csv" in result.stderr
    # The suggested command points at --mapping, not --accept. (The only
    # "--accept" in the output is the explanatory "--accept would be rejected".)
    assert "fuzzy.csv --mapping" in result.stderr
    assert "fuzzy.csv --accept" not in result.stderr


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

    assert result.exit_code == 0
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

    assert result.exit_code == 0, result.stderr
    assert acctid not in result.stdout, result.stdout
    proposal = json.loads(result.stdout)["data"]["pending"][0]["account_proposals"][0]
    assert proposal["source_account_key"] == "****6789"
    # Ours, not the institution's — RECORD_ID, and the half the caller types.
    assert proposal["proposal_ref"] == "@0"
    assert proposal["proposed_account_id"] == "prov12345678"


def test_the_drains_printed_confirm_command_actually_runs(
    runner: CliRunner, patch_inbox: MagicMock, mocker: MockerFixture, tmp_path: Path
) -> None:
    """The drain's recovery hint must be a command, not a description of one.

    The drain is the one surface whose recovery nobody watches get produced, and
    its hint is assembled by hand from a persisted sidecar rather than from the
    ConfirmationRequired the other surfaces hold — so its command name, flags,
    and referent vocabulary are three hand-copied strings with nothing binding
    them to the CLI they name. Verified against a drifted flag name
    (``--account-bindings``), which this catches and which no assertion on the
    hint text would.

    It does not guard the glued-period paste bug the sign recoveries had: this
    command is quote-delimited, so trailing prose cannot reach the argv.
    """
    pending_file = tmp_path / "statement.ofx"
    pending_file.write_text("OFXHEADER:100\n")
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
    assert drain.exit_code == 0, drain.stderr

    hint = next(
        line for line in drain.stderr.splitlines() if "--account-binding" in line
    )
    command = hint[hint.index("moneybin") : hint.rindex("'")]
    # The two placeholders the caller substitutes: the ref is listed beside each
    # proposal, and the target is theirs to choose.
    argv = shlex.split(command.replace("@N=<account_id|new>", "@0=new"))[1:]

    imported = mocker.patch(
        "moneybin.services.import_service.ImportService.import_file",
        return_value=_make_import_result(),
    )
    mocker.patch(
        "moneybin.services.inbox_service.InboxService.for_active_profile_no_db"
    )

    rerun = runner.invoke(app, argv)

    assert rerun.exit_code == 0, rerun.output
    assert imported.call_args.kwargs["account_bindings"] == {"@0": "new"}


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


def test_inbox_path_prints_active_profile_root(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """`inbox path` prints the service root directory."""
    result = runner.invoke(app, ["import", "inbox", "path"])

    assert result.exit_code == 0
    assert str(patch_inbox.root) in result.stdout.strip()


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

    assert result.exit_code == 0, result.stderr
    assert "statement.csv" in result.stderr
    # --accept (ratifies the settled mapping) is paired with the binding so the
    # copy-pasted command passes the `import confirm` guard; no --mapping override.
    assert "--accept --account-binding" in result.stderr
    assert "1 pending" in result.stderr
    assert "--mapping" not in result.stderr


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

    assert result.exit_code == 0, result.stderr
    assert "@0" in result.stderr, result.stderr
    # Masked, not raw: `source_account_key` is ACCOUNT_IDENTIFIER on every
    # channel, and on OFX — which this fixture is — it carries the <ACCTID> the
    # institution issued. stderr is not exempt from the redaction contract, so
    # the ref above is the half the user types and this is only the
    # disambiguator that tells two proposals apart.
    assert "chase-1234" not in result.stderr, result.stderr
    assert "****1234" in result.stderr, result.stderr


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

    assert result.exit_code == 0, result.stderr
    assert "ledger overlap: 2/2 matched" in result.stderr
    assert "2024-01-15 to 2024-01-20" in result.stderr
