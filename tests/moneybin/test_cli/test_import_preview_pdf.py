"""`moneybin import preview` on a PDF.

Preview routed every file through the *tabular* format detector, so a PDF was
rejected with "Unsupported file type: '.pdf'" even though
``ImportService.pdf_preview`` existed and the MCP ``import_preview`` tool used
it. That left the whole PDF-import debug loop (is this statement deterministic?
how many rows would it yield?) undriveable from the CLI.

CLI wiring only, per `.claude/rules/cli.md` — the service is mocked; routing
behaviour is covered in the extractor and service suites.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app
from moneybin.extractors.confidence import Confidence
from moneybin.services.import_confirmation import (
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    SignConventionProposal,
)
from moneybin.services.import_service import PdfPreviewResult

runner = CliRunner()


@pytest.fixture
def statement(tmp_path: Path) -> Path:
    """A file with a .pdf suffix. Content is irrelevant — the service is mocked."""
    path = tmp_path / "statement.pdf"
    path.write_bytes(b"%PDF-1.4 not a real pdf")
    return path


def _preview_result(path: Path, **overrides: object) -> PdfPreviewResult:
    defaults: dict[str, object] = {
        "file_path": str(path),
        "deterministic": True,
        "decision_reason": "passed",
        "confidence": 0.95,
        "row_count": 24,
    }
    return PdfPreviewResult(**{**defaults, **overrides})  # type: ignore[arg-type]


def _patch_service(mocker: MockerFixture, **kwargs: object) -> MagicMock:
    """Stub ImportService so no database or real PDF parsing is involved."""
    svc = MagicMock()
    mocker.patch("moneybin.services.import_service.ImportService", return_value=svc)
    mocker.patch("moneybin.database.get_database")
    for name, value in kwargs.items():
        setattr(svc, name, value)
    return svc


def test_preview_routes_a_pdf_to_the_pdf_service(
    statement: Path, mocker: MockerFixture
) -> None:
    """A .pdf must reach pdf_preview, not the tabular format detector."""
    svc = _patch_service(
        mocker, pdf_preview=MagicMock(return_value=_preview_result(statement))
    )

    result = runner.invoke(app, ["preview", str(statement)])

    assert result.exit_code == 0, result.output
    assert "Unsupported file type" not in result.output
    svc.pdf_preview.assert_called_once()


def test_preview_reports_the_deterministic_verdict_and_row_count(
    statement: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """The two facts the debug loop exists to answer must both be visible."""
    _patch_service(
        mocker, pdf_preview=MagicMock(return_value=_preview_result(statement))
    )

    with caplog.at_level("INFO"):
        result = runner.invoke(app, ["preview", str(statement)])

    assert result.exit_code == 0, result.output
    assert "24" in result.stdout  # row count
    assert "deterministic" in result.stdout.lower()


def test_preview_reports_a_non_deterministic_pdf_without_failing(
    statement: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """A statement the rung can't crack is a finding to report, not an error.

    Exit stays 0: preview answered the question it was asked. The reason is
    what the user needs, so it has to appear.
    """
    _patch_service(
        mocker,
        pdf_preview=MagicMock(
            return_value=_preview_result(
                statement,
                deterministic=False,
                decision_reason="no_transaction_table",
                confidence=0.0,
                row_count=0,
            )
        ),
    )

    with caplog.at_level("INFO"):
        result = runner.invoke(app, ["preview", str(statement)])

    assert result.exit_code == 0, result.output
    assert "no_transaction_table" in result.stdout


def test_preview_surfaces_a_pending_sign_confirmation(
    statement: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """A card statement's sign proposal must be shown, not raised as a traceback.

    pdf_preview signals this by RAISING, so an unhandled path would crash the
    command on exactly the statements that most need inspecting.

    The proposal goes to stdout and must NOT reach the log: sample rows carry
    merchant descriptions and bare amounts, and SanitizedLogFormatter masks
    neither (`_DOLLAR_PATTERN` requires a literal "$"; no pattern matches
    descriptions). Logging them would persist transaction detail to the session
    log, which `.claude/rules/security.md` forbids.
    """
    outcome = ConfirmationRequired(
        channel="pdf",
        confidence=Confidence(score=1.0, tier="high", flagged=(), missing_required=()),
        proposed=SignConventionProposal(
            sign_convention="negative_is_income",
            evidence=("Minimum Payment Due", "Credit Limit"),
            sample_rows=[
                {
                    "description": "COFFEE SHOP",
                    "as_printed": "12.50",
                    "as_recorded": "-12.50",
                }
            ],
        ),
        reason="sign_convention",
    )
    _patch_service(
        mocker,
        pdf_preview=MagicMock(side_effect=ImportConfirmationRequiredError(outcome)),
    )

    with caplog.at_level("INFO"):
        result = runner.invoke(app, ["preview", str(statement)])

    assert result.exception is None, result.exception
    # Shown to the user...
    assert "COFFEE SHOP" in result.output
    assert "12.50" in result.output
    # ...but never persisted to the log.
    assert "COFFEE SHOP" not in caplog.text
    assert "12.50" not in caplog.text


def test_preview_still_handles_a_csv(tmp_path: Path) -> None:
    """Regression: the tabular path must survive the PDF branch."""
    csv = tmp_path / "txns.csv"
    csv.write_text("Date,Description,Amount\n2024-01-15,Coffee,-4.50\n")

    result = runner.invoke(app, ["preview", str(csv)])

    assert result.exit_code == 0, result.output
    assert "Unsupported file type" not in result.output


def test_preview_routes_a_tcc_denial_through_the_classifier(
    statement: Path,
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreadable file is a classified error message, never a traceback.

    Real trigger: macOS TCC denies reads under ~/Documents, so pdfplumber's
    `open()` raises PermissionError from deep inside the extractor and the CLI
    dumped a full rich traceback at the user. Statements routinely live in
    exactly those protected directories.

    The advice has to come from `permission_advice`, not from this branch: it
    used to hardcode the "Files and Folders" pane for *any* PermissionError,
    which is the wrong pane for a TCC block and fires the macOS remedy on Linux
    and on errnos `chmod` would fix.

    Darwin is pinned rather than read from the host so the conjunction under
    test is the code's, not the runner's — CI is Linux.

    The ❌ message still routes through the logger (assert on caplog); the 💡
    hint goes straight to stderr via ``typer.echo`` and never touches the log
    (`handle_cli_errors`, so it never carries a DuckDB error's caller-authored
    text into a durable file). Since click 8.2, ``result.output`` is its own
    stream mixing stdout and stderr in write order (no ``mix_stderr`` flag
    anymore), so that direct ``typer.echo(err=True)`` lands there too — the
    hint assertions move there.
    """
    monkeypatch.setattr("moneybin.errors.platform.system", lambda: "Darwin")
    # The classifier reads `exc.filename`, so the protected root is expressed
    # there; the fixture file itself only has to exist and end in .pdf.
    blocked = Path.home() / "Documents" / "statement.pdf"
    _patch_service(
        mocker,
        pdf_preview=MagicMock(
            side_effect=PermissionError(1, "Operation not permitted", str(blocked))
        ),
    )

    with caplog.at_level("INFO"):
        result = runner.invoke(app, ["preview", str(statement)])

    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "Traceback" not in result.output
    assert result.exit_code == 1
    # Both halves matter: what failed, and the one-click OS fix for it.
    assert "Operation not permitted" in caplog.text
    assert "Full Disk Access" in result.output
    # The pane the hardcoded advice used to name.
    assert "Files and Folders" not in result.output


def test_preview_does_not_offer_the_tcc_remedy_for_a_mode_denial(
    statement: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """EACCES is a `chmod` problem, and saying "System Settings" would be wrong.

    The isolating half: the old hardcoded hint fired the macOS TCC remedy for
    every PermissionError, so this case got advice that could not fix it. No
    platform pin is needed — the EACCES branch returns before any platform
    test, which is itself the property being checked.

    The ❌ message routes through the logger (caplog); the 💡 hint goes
    straight to stderr via ``typer.echo`` and never touches the log — see
    ``test_preview_routes_a_tcc_denial_through_the_classifier`` above for why
    — so the hint assertions read ``result.output`` instead (since click 8.2
    it's its own stream mixing stdout and stderr in write order, so a direct
    ``typer.echo(err=True)`` lands there too).
    """
    _patch_service(
        mocker,
        pdf_preview=MagicMock(
            side_effect=PermissionError(13, "Permission denied", str(statement))
        ),
    )

    with caplog.at_level("INFO"):
        result = runner.invoke(app, ["preview", str(statement)])

    assert result.exit_code == 1
    assert "Permission denied" in caplog.text
    assert "chmod" in result.output
    assert "Full Disk Access" not in result.output


def _patch_key_failure(mocker: MockerFixture, db_path: Path) -> None:
    """Fail the database open the way a real fresh/locked profile fails.

    `_preview_pdf` opens with `read_only=False`, and `DatabaseNotInitializedError`
    is raised only on the `read_only=True` path (`database.py`). What a real
    caller hits instead is `SecretStore.get_key()` → `SecretNotFoundError` →
    `DatabaseKeyError` — regardless of whether the file exists. Which recovery
    applies is decided from `db_path`, so the test supplies it rather than
    asserting a hardcoded string.
    """
    from moneybin.database import DatabaseKeyError

    mocker.patch(
        "moneybin.database.get_database",
        side_effect=DatabaseKeyError("no encryption key for profile"),
    )
    settings = MagicMock()
    settings.database.path = db_path
    mocker.patch("moneybin.database.get_settings", return_value=settings)


def test_preview_on_a_fresh_install_points_at_db_init(
    statement: Path,
    tmp_path: Path,
    mocker: MockerFixture,
) -> None:
    """No database has ever existed → "db init", never "db unlock".

    The tabular branch degrades to built-in formats when the database is
    missing, so `import preview x.csv` works on a fresh install. A PDF can't
    degrade — the recipe rung reads app.pdf_formats — but it must still say so
    instead of crashing, AND name the recovery that actually works: `db unlock`
    re-derives a key from a stored salt a never-initialized profile doesn't
    have, so pointing there strands the user on the one command that cannot
    succeed.
    """
    _patch_key_failure(mocker, tmp_path / "never-created.duckdb")

    result = runner.invoke(app, ["preview", str(statement)])

    assert "Traceback" not in result.output
    assert result.exit_code == 1
    assert "db init" in result.stderr
    assert "db unlock" not in result.stderr


def test_preview_key_recovery_hint_is_written_to_stderr(
    statement: Path, tmp_path: Path, mocker: MockerFixture
) -> None:
    """The required database recovery remains visible at default log level."""
    _patch_key_failure(mocker, tmp_path / "never-created.duckdb")

    result = runner.invoke(app, ["preview", str(statement)])

    assert result.exit_code == 1
    assert "db init" in result.stderr
    assert "db init" not in result.stdout


def test_preview_on_a_locked_database_points_at_db_unlock(
    statement: Path,
    tmp_path: Path,
    mocker: MockerFixture,
) -> None:
    """The database exists but its key is unavailable → "db unlock".

    The twin of the fresh-install case: same exception, opposite recovery. Both
    directions are asserted because one hardcoded hint satisfies either test
    alone.
    """
    existing = tmp_path / "moneybin.duckdb"
    existing.write_bytes(b"")
    _patch_key_failure(mocker, existing)

    result = runner.invoke(app, ["preview", str(statement)])

    assert result.exit_code == 1
    assert "db unlock" in result.stderr
    assert "db init" not in result.stderr


def test_preview_says_when_tabular_flags_are_ignored_on_a_pdf(
    statement: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """A silently-dropped flag reads as an honoured one.

    The PDF branch returns before any tabular option is consulted, so
    `--format x` produced a clean, confident report that had nothing to do with
    the flag. An agent has no way to tell that apart from success, and will
    carry the same flag into the real import.
    """
    _patch_service(
        mocker, pdf_preview=MagicMock(return_value=_preview_result(statement))
    )

    with caplog.at_level("WARNING"):
        result = runner.invoke(
            app, ["preview", str(statement), "--format", "chase_pdf", "--sheet", "S1"]
        )

    assert result.exit_code == 0, result.output
    assert "--format" in caplog.text
    assert "--sheet" in caplog.text
    # Flags that were NOT passed must not be named.
    assert "--delimiter" not in caplog.text


@pytest.mark.parametrize(
    ("passed", "not_passed"),
    [
        ("--no-row-limit", "--no-size-limit"),
        ("--no-size-limit", "--no-row-limit"),
    ],
)
def test_preview_says_when_limit_overrides_are_ignored_on_a_pdf(
    statement: Path,
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
    passed: str,
    not_passed: str,
) -> None:
    """The limit overrides drop here as completely as the six tabular flags.

    `_preview_pdf` takes the source and nothing else, so neither can reach a
    gate on this branch — and both are documented as carrying over from
    `import files`, which is exactly the inference this warning exists to
    stop. They were accepted in silence when `import preview` learned them.
    """
    _patch_service(
        mocker, pdf_preview=MagicMock(return_value=_preview_result(statement))
    )

    with caplog.at_level("WARNING"):
        result = runner.invoke(app, ["preview", str(statement), passed])

    assert result.exit_code == 0, result.output
    assert "Ignored for a PDF" in caplog.text
    assert passed in caplog.text
    # The flag that was NOT passed must not be named.
    assert not_passed not in caplog.text


def test_preview_stays_quiet_when_no_tabular_flags_are_passed(
    statement: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """The ordinary PDF preview must not carry a spurious warning."""
    _patch_service(
        mocker, pdf_preview=MagicMock(return_value=_preview_result(statement))
    )

    with caplog.at_level("WARNING"):
        result = runner.invoke(app, ["preview", str(statement)])

    assert result.exit_code == 0, result.output
    assert "Ignored for a PDF" not in caplog.text


def test_tcc_denied_preview_is_classified_not_a_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`import preview` on a TCC-blocked file must give the recovery hint.

    Twin of the `import files` preflight: `import_preview` ran its own
    `source.exists()` check before entering `handle_cli_errors()`, and
    `Path.exists()` raises under a TCC denial rather than returning False. That
    made the documented `import preview ~/Documents/...` scenario emit an
    unhandled traceback — the same defect, one command over.
    """
    target = Path.home() / "Documents" / "chase_statement.pdf"
    real_exists = Path.exists

    def _deny(self: Path, **kwargs: object) -> bool:
        if self == target:
            raise PermissionError(1, "Operation not permitted", str(target))
        return real_exists(self, **kwargs)  # pyright: ignore[reportCallIssue,reportArgumentType]

    monkeypatch.setattr(Path, "exists", _deny)

    result = runner.invoke(app, ["preview", str(target)])

    # `preview` is text-only (no --output), so the classified error surfaces
    # through handle_cli_errors' log path rather than a JSON envelope.
    assert result.exit_code == 1, result.output
    assert result.exception is None or isinstance(result.exception, SystemExit), (
        f"unhandled exception leaked: {result.exception!r}"
    )


def test_tcc_denied_confirm_is_classified_not_a_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`import confirm` is the third command with this preflight shape.

    Same defect as `import files` and `import preview`: an `exists()` check
    outside `handle_cli_errors()`. Covered here so the fix isn't re-discovered
    one command per review round.
    """
    target = Path.home() / "Documents" / "statement.pdf"
    real_exists = Path.exists

    def _deny(self: Path, **kwargs: object) -> bool:
        if self == target:
            raise PermissionError(1, "Operation not permitted", str(target))
        return real_exists(self, **kwargs)  # pyright: ignore[reportCallIssue,reportArgumentType]

    monkeypatch.setattr(Path, "exists", _deny)

    result = runner.invoke(app, ["confirm", str(target), "--accept"])

    assert result.exit_code == 1, result.output
    assert result.exception is None or isinstance(result.exception, SystemExit), (
        f"unhandled exception leaked: {result.exception!r}"
    )


# ---------------------------------------------------------------------------
# Consented disclosure (pdf-disclosure-consent.md §3, CLI rows). Real service,
# real encrypted database, synthetic canary statement.
# ---------------------------------------------------------------------------

_REVOKE_HINT = "privacy revoke smart-import-parsing --recipient claude-code"


def _audit_after(action: str) -> list[dict[str, object]]:
    import json

    from moneybin.database import get_database

    with get_database(read_only=True) as db:
        rows = db.execute(
            "SELECT after_value FROM app.audit_log WHERE action = ? "
            "ORDER BY occurred_at",
            [action],
        ).fetchall()
    return [json.loads(r[0]) if r[0] else {} for r in rows]


def _host_grants() -> list[tuple[str, str | None]]:
    from moneybin.database import get_database

    with get_database(read_only=True) as db:
        return db.execute(
            "SELECT backend, approved_via FROM app.ai_consent_grants "
            "WHERE recipient_kind = 'ai_host' AND revoked_at IS NULL"
        ).fetchall()


def _assert_no_canary(*texts: str) -> None:
    from tests.moneybin.pdf_statement_fixtures import CANARIES

    for text in texts:
        for canary in CANARIES:
            assert canary not in text


def _request_id(pdf: Path) -> str:
    import json

    result = runner.invoke(app, ["preview", str(pdf), "-o", "json"])
    assert result.exit_code == 0, result.output
    return json.loads(result.stdout)["data"]["consent_request"]["request_id"]


def _interactive(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate a person at a terminal (stdin, stdout and stderr all TTYs)."""
    from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=False,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=200,
        height=50,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )

    def _policy(**_: object) -> TerminalPolicy:
        return policy

    for module in ("import_cmd", "_import_pdf_disclosure"):
        monkeypatch.setattr(
            f"moneybin.cli.commands.{module}.get_terminal_policy", _policy
        )


def test_preview_non_tty_content_free_text_and_json(
    consent_db: Path,
    canary_pdf: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Without consent: a receipt naming the request id, and no document content."""
    import json

    monkeypatch.delenv("CLAUDECODE", raising=False)

    with caplog.at_level("DEBUG", logger="moneybin"):
        text = runner.invoke(app, ["preview", str(canary_pdf)])
        js = runner.invoke(app, ["preview", str(canary_pdf), "-o", "json"])

    assert text.exit_code == 0, text.output
    assert js.exit_code == 0, js.output
    moneybin_log = "\n".join(
        r.getMessage() for r in caplog.records if r.name.startswith("moneybin")
    )
    _assert_no_canary(text.stdout, text.stderr, js.stdout, js.stderr, moneybin_log)
    wire = json.loads(js.stdout)
    assert wire["status"] == "ok"
    assert wire["data"]["status"] == "ai_assistance_required"
    req = wire["data"]["consent_request"]
    assert "confirmation_token" not in req
    assert req["request_id"] == req["binding_id"]
    assert "remember_available" not in req
    assert req["risk_statement"] in text.stdout
    assert req["request_id"] in text.stdout
    assert f"--allow-ai-disclosure {req['request_id']}" in text.stdout
    assert _audit_after("smart_import_consent") == []


def test_preview_writes_no_parse_audit_without_consent(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLAUDECODE", "1")

    result = runner.invoke(app, ["preview", str(canary_pdf), "-o", "json"])

    assert result.exit_code == 0, result.output
    assert _audit_after("smart_import_parse") == []


def test_flag_with_matching_request_id_releases_redacted_json(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    from tests.moneybin.pdf_statement_fixtures import (
        CANARY_ACCOUNT_MIDDLE,
        CANARY_DESCRIPTION,
    )

    monkeypatch.setenv("CLAUDECODE", "1")
    rid = _request_id(canary_pdf)

    out = runner.invoke(
        app,
        ["preview", str(canary_pdf), "--allow-ai-disclosure", rid, "-o", "json"],
    )

    assert out.exit_code == 0, out.output
    assert CANARY_DESCRIPTION in out.stdout
    assert CANARY_ACCOUNT_MIDDLE not in out.stdout
    data = json.loads(out.stdout)["data"]
    assert data["status"] == "confirmation_required"
    assert "document_text" in data["bridge_payload"]
    # The flag is carried by the agent: it approves this file only.
    assert _host_grants() == []
    [decision] = _audit_after("smart_import_consent")
    assert decision["outcome"] == "approved"
    assert decision["approved_via"] == "cli_flag"
    assert decision["remembered"] is False
    assert decision["grant_id"] is None
    assert len(_audit_after("smart_import_parse")) == 1


@pytest.mark.parametrize("which", ["stale", "other"])
def test_flag_stale_or_other_file_request_id_refused(
    consent_db: Path,
    canary_pdf: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    which: str,
) -> None:
    """An id bound to other bytes releases nothing, even for the same path."""
    import json

    monkeypatch.setenv("CLAUDECODE", "1")
    if which == "stale":
        rid = _request_id(canary_pdf)
        canary_pdf.write_bytes(canary_pdf.read_bytes() + b"\n% edited\n")
    else:
        other = tmp_path / "other" / "statement.pdf"
        other.parent.mkdir()
        other.write_bytes(canary_pdf.read_bytes() + b"\n% another file\n")
        rid = _request_id(other)

    out = runner.invoke(
        app,
        ["preview", str(canary_pdf), "--allow-ai-disclosure", rid, "-o", "json"],
    )

    assert out.exit_code == 1, out.output
    _assert_no_canary(out.stdout, out.stderr)
    assert (
        json.loads(out.stdout)["error"]["code"]
        == "import_pdf_disclosure_request_mismatch"
    )
    assert _audit_after("smart_import_parse") == []
    assert _host_grants() == []


def test_bare_flag_is_usage_error(canary_pdf: Path) -> None:
    result = runner.invoke(app, ["preview", str(canary_pdf), "--allow-ai-disclosure"])

    assert result.exit_code == 2


def test_unidentified_host_flag_releases_once_no_grant(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.moneybin.pdf_statement_fixtures import CANARY_DESCRIPTION

    monkeypatch.delenv("CLAUDECODE", raising=False)
    rid = _request_id(canary_pdf)

    out = runner.invoke(
        app,
        ["preview", str(canary_pdf), "--allow-ai-disclosure", rid, "-o", "json"],
    )
    again = runner.invoke(app, ["preview", str(canary_pdf), "-o", "json"])

    assert out.exit_code == 0, out.output
    assert CANARY_DESCRIPTION in out.stdout
    assert _host_grants() == []
    [decision] = _audit_after("smart_import_consent")
    assert decision["approved_via"] == "cli_flag"
    assert decision["remembered"] is False
    assert again.exit_code == 0, again.output
    _assert_no_canary(again.stdout)


def test_remembered_grant_releases_with_notice(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    from moneybin.database import get_database
    from moneybin.privacy.disclosure import host_from_environment
    from moneybin.services.consent_service import ConsentService
    from tests.moneybin.pdf_statement_fixtures import CANARY_DESCRIPTION

    monkeypatch.setenv("CLAUDECODE", "1")
    with get_database(read_only=False) as db:
        grant, _ = ConsentService(db).grant_host_consent(
            host=host_from_environment(os.environ),
            approved_via="cli_flag",
            actor="cli",
        )

    out = runner.invoke(app, ["preview", str(canary_pdf), "-o", "json"])

    assert out.exit_code == 0, out.output
    assert CANARY_DESCRIPTION in out.stdout
    assert grant.grant_id in out.stderr
    assert _REVOKE_HINT in out.stderr
    assert _audit_after("smart_import_consent")[-1]["approved_via"] == "remembered"


def test_tty_prompt_accept_releases_once_and_names_confirm(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.moneybin.pdf_statement_fixtures import CANARY_DESCRIPTION

    monkeypatch.delenv("CLAUDECODE", raising=False)
    _interactive(monkeypatch)

    out = runner.invoke(app, ["preview", str(canary_pdf)], input="y\n")

    assert out.exit_code == 0, out.output
    assert "Send this PDF's contents to your AI app?" in out.output
    assert "MoneyBin cannot pull it back" in out.output
    assert CANARY_DESCRIPTION in out.stdout
    assert "moneybin import confirm" in out.stdout
    assert "--bridge-response" in out.stdout
    [decision] = _audit_after("smart_import_consent")
    assert decision["approved_via"] == "cli_prompt"
    assert decision["outcome"] == "approved"
    assert _host_grants() == []


def test_tty_prompt_decline_releases_nothing(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CLAUDECODE", raising=False)
    _interactive(monkeypatch)

    out = runner.invoke(app, ["preview", str(canary_pdf)], input="n\n")

    assert out.exit_code == 0, out.output
    _assert_no_canary(out.stdout, out.stderr)
    [decision] = _audit_after("smart_import_consent")
    assert decision["outcome"] == "declined"
    assert decision["approved_via"] == "cli_prompt"
    assert _audit_after("smart_import_parse") == []


def test_agent_shell_with_tty_shows_no_prompt(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R18: an agent with a pty must not answer the consent prompt itself."""
    monkeypatch.setenv("CLAUDECODE", "1")
    _interactive(monkeypatch)

    out = runner.invoke(app, ["preview", str(canary_pdf)], input="y\ny\n")

    assert out.exit_code == 0, out.output
    assert "Send this PDF" not in out.output
    _assert_no_canary(out.stdout, out.stderr)
    assert "--allow-ai-disclosure" in out.stdout
    assert _audit_after("smart_import_consent") == []
    assert _audit_after("smart_import_parse") == []
    assert _host_grants() == []


def test_import_files_yes_never_discloses(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`import files --yes` keeps its meaning and never consents, even remembered."""
    import os

    from moneybin.database import get_database
    from moneybin.privacy.disclosure import host_from_environment
    from moneybin.services.consent_service import ConsentService

    monkeypatch.setenv("CLAUDECODE", "1")
    with get_database(read_only=False) as db:
        ConsentService(db).grant_host_consent(
            host=host_from_environment(os.environ),
            approved_via="cli_flag",
            actor="cli",
        )

    out = runner.invoke(
        app, ["files", str(canary_pdf), "--yes", "--no-refresh", "-o", "json"]
    )

    _assert_no_canary(out.stdout, out.stderr)
    assert _audit_after("smart_import_parse") == []
    assert _audit_after("smart_import_consent") == []


def test_released_bridge_payload_reaches_import_confirm(
    consent_db: Path,
    canary_pdf: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The preview's Next step works: a response authored from it imports."""
    import json

    from tests.moneybin.pdf_statement_fixtures import CANARY_RECIPE, CANARY_ROWS

    monkeypatch.setenv("CLAUDECODE", "1")
    rid = _request_id(canary_pdf)
    released = runner.invoke(
        app,
        ["preview", str(canary_pdf), "--allow-ai-disclosure", rid, "-o", "json"],
    )
    assert released.exit_code == 0, released.output
    response = tmp_path / "response.json"
    response.write_text(json.dumps({"recipe": CANARY_RECIPE, "rows": CANARY_ROWS}))

    confirmed = runner.invoke(
        app,
        [
            "confirm",
            str(canary_pdf),
            "--bridge-response",
            str(response),
            "--confirm",
            "--account-binding",
            "@0=new",
            "-o",
            "json",
        ],
    )

    assert confirmed.exit_code == 0, confirmed.output


# ---------------------------------------------------------------------------
# Fix round 1
# ---------------------------------------------------------------------------


def _wrap_consented_release(monkeypatch: pytest.MonkeyPatch, change: object) -> None:
    """Route the consented ``pdf_preview`` call through ``change(error)``."""
    from moneybin.services.import_confirmation import ImportConfirmationRequiredError
    from moneybin.services.import_service import ImportService

    original = ImportService.pdf_preview

    def _wrapped(self: ImportService, *args: object, **kwargs: object) -> object:
        if kwargs.get("disclosure_consent") is None:
            return original(self, *args, **kwargs)  # type: ignore[arg-type]
        try:
            return original(self, *args, **kwargs)  # type: ignore[arg-type]
        except ImportConfirmationRequiredError as e:
            raise change(e) from None  # type: ignore[operator]

    monkeypatch.setattr(ImportService, "pdf_preview", _wrapped)


def test_verbose_preview_leaks_no_document_text(
    consent_db: Path,
    canary_pdf: Path,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R19: DEBUG logging must not carry PDF text before consent, anywhere."""
    import logging

    monkeypatch.delenv("CLAUDECODE", raising=False)
    with caplog.at_level(logging.DEBUG):
        result = runner.invoke(app, ["preview", str(canary_pdf)])

    assert result.exit_code == 0, result.output
    logged = "\n".join(p.read_text() for p in verbose_file_logging.glob("*.log"))
    _assert_no_canary(result.stdout, result.stderr, caplog.text, logged)


def test_released_json_privacy_log_records_bridge_classes(
    consent_db: Path,
    canary_pdf: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """I1: the CLI egress row names the same classes MCP records for a release."""
    import json

    from moneybin.privacy.classified_envelope import classify
    from moneybin.privacy.payloads.imports import ImportPdfBridgePreviewPayload

    monkeypatch.delenv("CLAUDECODE", raising=False)
    rid = _request_id(canary_pdf)
    out = runner.invoke(
        app,
        ["preview", str(canary_pdf), "--allow-ai-disclosure", rid, "-o", "json"],
    )
    assert out.exit_code == 0, out.output

    log = tmp_path / "privacy_log" / "privacy.log.jsonl"
    event = json.loads(log.read_text().splitlines()[-1])
    expected = classify(ImportPdfBridgePreviewPayload)
    assert event["actor"] == "cli.import_preview"
    assert event["classes_returned"] == expected.classes_returned
    assert event["sensitivity"] == expected.sensitivity
    assert json.loads(out.stdout)["summary"]["sensitivity"] == expected.sensitivity


def test_identified_flag_is_one_time_and_points_at_privacy_grant(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The flag never remembers; the person can pre-grant from their own terminal."""
    import json

    from moneybin.privacy.disclosure import (
        host_for_recipient_name,
        host_from_environment,
    )

    monkeypatch.setenv("CLAUDECODE", "1")
    text = runner.invoke(app, ["preview", str(canary_pdf)])
    js = json.loads(
        runner.invoke(app, ["preview", str(canary_pdf), "-o", "json"]).stdout
    )
    command = "moneybin privacy grant smart-import-parsing --recipient claude-code"
    assert "future PDFs" not in text.stdout
    assert command in text.stdout
    assert any(command in action for action in js["actions"])
    rid = js["data"]["consent_request"]["request_id"]
    out = runner.invoke(app, ["preview", str(canary_pdf), "--allow-ai-disclosure", rid])
    assert out.exit_code == 0, out.output
    assert _host_grants() == []

    # The printed command grants the key this agent shell is checked against.
    granted = host_for_recipient_name(command.rsplit(" ", 1)[-1])
    shell = host_from_environment({"CLAUDECODE": "1"})
    assert granted is not None
    assert granted.recipient_key == shell.recipient_key


@pytest.mark.parametrize("claudecode", [True, False])
def test_cli_disclosure_never_writes_a_grant(
    consent_db: Path,
    canary_pdf: Path,
    monkeypatch: pytest.MonkeyPatch,
    claudecode: bool,
) -> None:
    """Only `privacy grant --recipient` writes an ai_host grant on the CLI."""
    import inspect

    import moneybin.cli.commands._import_pdf_disclosure as cli_disclosure
    from moneybin.services.consent_service import ConsentService
    from tests.moneybin.pdf_statement_fixtures import CANARY_DESCRIPTION

    def _refuse(*_a: object, **_k: object) -> None:
        raise AssertionError("the CLI disclosure path wrote a grant")

    monkeypatch.setattr(ConsentService, "grant_host_consent", _refuse)
    if claudecode:
        monkeypatch.setenv("CLAUDECODE", "1")
    else:
        monkeypatch.delenv("CLAUDECODE", raising=False)
    rid = _request_id(canary_pdf)

    out = runner.invoke(
        app,
        ["preview", str(canary_pdf), "--allow-ai-disclosure", rid, "-o", "json"],
    )

    assert out.exit_code == 0, out.output
    assert CANARY_DESCRIPTION in out.stdout
    assert "grant_host_consent" not in inspect.getsource(cli_disclosure)


def test_unidentified_receipt_does_not_claim_remembering(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CLAUDECODE", raising=False)

    text = runner.invoke(app, ["preview", str(canary_pdf)])

    assert "future PDFs" not in text.stdout
    assert "privacy grant" not in text.stdout


def test_released_text_strips_terminal_escapes(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M6: untrusted PDF text cannot drive the terminal; newlines and tabs survive."""
    import dataclasses

    from moneybin.services.import_confirmation import (
        BridgePayload,
        ImportConfirmationRequiredError,
    )

    def _inject(e: ImportConfirmationRequiredError) -> Exception:
        assert isinstance(e.outcome.proposed, BridgePayload)
        payload = dict(e.outcome.proposed.payload)
        payload["document_text"] = "SAFE\x1b[2JLINE\nCOL1\tCOL2\x1b]0;evil\x07END"
        payload["tables_preview"] = [
            {"page": 1, "header": ["H\x1b[31m"], "rows": [["CELL\x1b]8;;x\x07"]]}
        ]
        return ImportConfirmationRequiredError(
            dataclasses.replace(e.outcome, proposed=BridgePayload(payload=payload))
        )

    monkeypatch.delenv("CLAUDECODE", raising=False)
    _wrap_consented_release(monkeypatch, _inject)
    rid = _request_id(canary_pdf)

    out = runner.invoke(app, ["preview", str(canary_pdf), "--allow-ai-disclosure", rid])

    assert out.exit_code == 0, out.output
    assert "\x1b" not in out.stdout
    assert "\x07" not in out.stdout
    assert "SAFELINE\nCOL1\tCOL2END" in out.stdout
    assert "CELL" in out.stdout


def test_failed_release_records_approval_but_no_grant(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M5/R23: a failed release audits the yes with released=False; no grant."""

    def _boom(_e: object) -> Exception:
        return RuntimeError("release failed")

    monkeypatch.setenv("CLAUDECODE", "1")
    _wrap_consented_release(monkeypatch, _boom)
    rid = _request_id(canary_pdf)

    out = runner.invoke(
        app, ["preview", str(canary_pdf), "--allow-ai-disclosure", rid, "-o", "json"]
    )

    assert out.exit_code != 0
    assert _host_grants() == []
    [row] = _audit_after("smart_import_consent")
    assert row["outcome"] == "approved" and row["approved_via"] == "cli_flag"
    assert row["released"] is False
    assert row["remembered"] is False and row["grant_id"] is None


def test_receipts_label_the_two_reasons_distinctly(
    consent_db: Path, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M8: routing reason before consent, confirmation reason after release."""
    monkeypatch.delenv("CLAUDECODE", raising=False)

    before = runner.invoke(app, ["preview", str(canary_pdf)])
    rid = _request_id(canary_pdf)
    after = runner.invoke(
        app, ["preview", str(canary_pdf), "--allow-ai-disclosure", rid]
    )

    assert "Why AI help is needed: metadata_incomplete" in before.stdout
    assert "Reason:" not in before.stdout
    assert "Reason:" in after.stdout
    assert "Why AI help is needed" not in after.stdout


@pytest.mark.parametrize("interactive", [False, True], ids=["receipt", "prompt"])
def test_risk_statement_file_name_cannot_drive_the_terminal(
    consent_db: Path,
    canary_pdf: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interactive: bool,
) -> None:
    """The risk statement embeds the file name; its control sequences are stripped."""
    import shutil

    evil = tmp_path / "stmt\x1b]0;owned\x07\x1b[2J.pdf"
    shutil.copy(canary_pdf, evil)
    monkeypatch.delenv("CLAUDECODE", raising=False)
    if interactive:
        _interactive(monkeypatch)

    out = runner.invoke(
        app, ["preview", str(evil)], input="n\n" if interactive else None
    )

    assert out.exit_code == 0, out.output
    assert "\x1b" not in out.stdout and "\x07" not in out.stdout
    assert "\x1b" not in out.stderr and "\x07" not in out.stderr
    assert "MoneyBin cannot pull it back" in out.output
