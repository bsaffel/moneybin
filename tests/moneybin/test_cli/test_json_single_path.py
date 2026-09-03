"""`render_or_json` is the only way a command may put JSON on stdout.

`cli.md` promises `--output json` returns the same envelope MCP returns, and
`render_or_json` is the one path that produces it: it derives the tier from the
payload TYPE, applies `redact_typed`, and writes the `privacy.log` audit row.
A hand-rolled `typer.echo(json.dumps(...))` produces bytes that look like output
and carry none of that, so the guard below is a source scan rather than a
behavioural assertion — a bypass leaves no trace at runtime to assert on.

Two sites hold JSON that this scan structurally cannot see, and neither is a
bypass. `commands/mcp.py` renders MCP client config snippets, which are a file
format the user pastes, not a command result. `categorize/export.py` writes an
interchange document that `commit-from-file` reads back, has no
`-o/--output {text,json}` flag at all, and already runs its own `audit_log`
over `RedactedTransaction` rows; both reach stdout through a local variable
rather than a `typer.echo(json.dumps(...))` call, so they cannot appear in the
exemption set below. Each carries a why-comment at its own call site instead.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

import moneybin.cli
import moneybin.cli.utils
from moneybin.cli.main import app
from moneybin.services.categorization.queries import CategorizationStats

CLI_ROOT = Path(moneybin.cli.__file__).parent

# Modules allowed to echo hand-serialized JSON, each for a reason that
# `render_or_json` cannot serve. Asserted by set equality in both directions
# below, so it can only shrink: a module that sheds the pattern must be removed,
# and a module that acquires one fails.
#
# - commands/db.py: `db info` and `db ps` — the operator-territory lifecycle
#   surface (mcp.md, "Hands-on operator territory"), which is CLI-only because
#   the MCP server cannot even start against a locked database. What they emit
#   is the database file's own metadata: its path, its per-table row counts,
#   and the PIDs holding it open. None of it is ledger data, and `db query`
#   beside them is the declared operator bypass that runs with no privacy
#   middleware at all (cli.md, "Operator-bypass banner on direct-DB commands").
# - commands/stats.py: operations metadata, judged by disclosure rather than by
#   column class (security.md). The checkable condition is registry-boundedness:
#   every metric name and label is declared in `metrics/registry.py` and every
#   value is numeric. A metric that ever carries a free-text label voids this
#   exemption and the command must migrate.
# - commands/logs.py: the effective control is write-time — the no-PII log
#   policy plus `SanitizedLogFormatter`. `redact_typed` is type-driven and
#   cannot mask free text after the fact, so migrating would add envelope shape
#   without adding masking. The known gap rides with the exemption: an account
#   label carrying fewer than five digits does not match the masking pattern and
#   reaches the log verbatim (see the refusal path in `import_cmd.py`), and
#   enveloping this command would not change that.
# - commands/migrate.py: operations metadata — schema versions, filenames, and
#   timings describing work the user ran on their own machine.
_HAND_SERIALIZED_JSON_ALLOWED = frozenset({
    "commands/db.py",
    "commands/logs.py",
    "commands/migrate.py",
    "commands/stats.py",
})


def _cli_modules() -> list[Path]:
    """Every module under `moneybin.cli`."""
    return sorted(CLI_ROOT.rglob("*.py"))


# `envelope.to_json()` IS the canonical path — `render_or_json` ends in exactly
# that call — so it is not a serializer this guard hunts for. The other two are:
# `json.dumps` builds a shape by hand, and `model_dump_json` hands a service's
# internal transport model straight to stdout as though it were the contract.
_HAND_SERIALIZERS = frozenset({"dumps", "model_dump_json"})


def _echoed_json(module: Path) -> list[int]:
    """Line numbers where this module echoes a hand-serialized payload directly.

    Only a *direct* argument counts. A `json.dumps` interpolated into an
    f-string is prose for a human reading the text branch — `import formats
    show` prints an extraction recipe that way — and enveloping it would be
    wrong.
    """
    hits: list[int] = []
    for node in ast.walk(ast.parse(module.read_text())):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "echo"):
            continue
        for arg in node.args:
            if (
                isinstance(arg, ast.Call)
                and isinstance(arg.func, ast.Attribute)
                and arg.func.attr in _HAND_SERIALIZERS
            ):
                hits.append(arg.lineno)
    return hits


@pytest.mark.unit
def test_no_command_echoes_hand_serialized_json() -> None:
    """A second JSON path is a second privacy contract."""
    offenders = {
        name: lines
        for module in _cli_modules()
        if (lines := _echoed_json(module))
        and (name := str(module.relative_to(CLI_ROOT)))
        not in _HAND_SERIALIZED_JSON_ALLOWED
    }
    assert offenders == {}, (
        "these commands serialize JSON by hand instead of calling "
        f"render_or_json, skipping redaction and the privacy audit row: "
        f"{offenders}"
    )


@pytest.mark.unit
def test_every_exempt_module_still_has_the_pattern_it_is_excused_for() -> None:
    """Set equality, so a standing excuse cannot outlive its reason.

    A stale entry is the failure mode that matters: a module that has since
    migrated would keep a blanket excuse, and the next hand-serialized payload
    added to it would pass unnoticed.
    """
    live = {
        str(module.relative_to(CLI_ROOT))
        for module in _cli_modules()
        if _echoed_json(module)
    }
    assert live == _HAND_SERIALIZED_JSON_ALLOWED, (
        "the exemption set and the live offender set disagree; unexpected "
        f"offenders: {sorted(live - _HAND_SERIALIZED_JSON_ALLOWED)}; stale "
        f"exemptions: {sorted(_HAND_SERIALIZED_JSON_ALLOWED - live)}"
    )


@pytest.mark.unit
def test_emit_json_is_gone() -> None:
    """`emit_json` printed `{key: payload}` with no envelope, tier, or audit row.

    Named explicitly rather than left to the scan above: the helper hid the
    bypass behind a call that read like a renderer, which is why nine commands
    reached for it.
    """
    assert not hasattr(moneybin.cli.utils, "emit_json")


# --- commands whose JSON path had no home of its own -----------------------


@pytest.fixture
def audit_row(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """Capture the privacy event `render_or_json` writes on the JSON path."""
    captured: dict[str, object] = {}
    monkeypatch.setattr("moneybin.cli.output.write_privacy_event", captured.update)
    return captured


@pytest.mark.unit
def test_categorize_stats_json_is_an_envelope_over_the_typed_coverage_payload(
    monkeypatch: pytest.MonkeyPatch, audit_row: dict[str, object]
) -> None:
    """The flat `by_<source>` keys become the payload's own `by_source` map.

    They were flattened onto the top level only because `emit_json` had nowhere
    else to put them; `CategorizeStatsPayload` is what the MCP tool has always
    returned for the same numbers.
    """
    from moneybin.cli.commands.transactions import categorize as categorize_cli

    service = MagicMock()
    service.stats.return_value = CategorizationStats(
        total=10,
        categorized=7,
        uncategorized=3,
        percent_categorized=70.0,
        by_source={"rule": 5, "user": 2},
        plaid_unmapped=1,
    )
    monkeypatch.setattr(categorize_cli, "get_database", _db_context(MagicMock()))

    def _service(_db: object) -> MagicMock:
        return service

    monkeypatch.setattr(
        "moneybin.services.categorization.CategorizationService", _service
    )

    result = CliRunner().invoke(
        app, ["transactions", "categorize", "stats", "--output", "json"]
    )

    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)
    assert body["data"] == {
        "total_transactions": 10,
        "categorized": 7,
        "uncategorized": 3,
        "percent_categorized": 70.0,
        "by_source": {"rule": 5, "user": 2},
        "plaid_unmapped": 1,
    }
    assert audit_row["classes_returned"] == ["aggregate"]


def _db_context(db: Any) -> Any:
    """A `get_database(...)` stand-in whose context manager yields ``db``."""
    ctx = MagicMock()
    ctx.__enter__.return_value = db
    ctx.__exit__.return_value = False

    def _open(*_args: object, **_kwargs: object) -> Any:
        return ctx

    return _open


@pytest.mark.unit
def test_import_history_json_is_an_envelope_over_the_shared_import_records(
    monkeypatch: pytest.MonkeyPatch, audit_row: dict[str, object]
) -> None:
    """`import history` and the MCP `import_status` tool read one query and now report one shape."""
    records = [
        {
            "import_id": "imp1",
            "source_file": "statements/statement.csv",
            "source_type": "tabular",
            "source_origin": "chase",
            "format_name": "chase_credit",
            "status": "completed",
            "rows_imported": 12,
            "rows_rejected": 0,
            "detection_confidence": 0.9,
            "started_at": "2026-01-02T03:04:05",
            "completed_at": "2026-01-02T03:04:09",
        }
    ]
    monkeypatch.setattr("moneybin.database.get_database", _db_context(MagicMock()))

    def _history(*_args: object, **_kwargs: object) -> list[dict[str, Any]]:
        return records

    monkeypatch.setattr("moneybin.loaders.import_log.get_import_history", _history)

    result = CliRunner().invoke(app, ["import", "history", "--output", "json"])

    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)
    assert body["data"]["records"] == records
    assert body["summary"]["returned_count"] == 1
    assert audit_row["classes_returned"] == ["aggregate"]


@pytest.mark.unit
def test_import_status_json_is_an_envelope_over_the_raw_table_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, audit_row: dict[str, object]
) -> None:
    """`import status` reports per-table row counts, not the import log."""
    from datetime import date

    from moneybin.cli.commands import import_cmd
    from moneybin.services.import_service import RawTableStat

    db_file = tmp_path / "moneybin.duckdb"
    db_file.write_bytes(b"")
    settings = MagicMock()
    settings.database.path = db_file
    monkeypatch.setattr(import_cmd, "get_settings", lambda: settings, raising=False)
    monkeypatch.setattr("moneybin.config.get_settings", lambda: settings)
    monkeypatch.setattr("moneybin.database.get_database", _db_context(MagicMock()))

    def _summary(_self: object) -> list[RawTableStat]:
        return [
            RawTableStat(
                schema="raw",
                table="tabular_transactions",
                rows=12,
                date_min=date(2026, 1, 1),
                date_max=date(2026, 2, 1),
            )
        ]

    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.raw_data_summary", _summary
    )

    result = CliRunner().invoke(app, ["import", "status", "--output", "json"])

    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)
    assert body["data"]["database"] == str(db_file)
    assert body["data"]["exists"] is True
    assert body["data"]["tables"][0]["table"] == "tabular_transactions"
    assert body["data"]["tables"][0]["rows"] == 12
    assert audit_row["classes_returned"]


@pytest.mark.unit
def test_import_status_json_reports_a_missing_database_as_an_error_envelope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The db-missing branch keeps its non-zero exit and gains the error envelope."""
    from moneybin.cli.commands import import_cmd

    settings = MagicMock()
    settings.database.path = tmp_path / "absent.duckdb"
    monkeypatch.setattr("moneybin.config.get_settings", lambda: settings)
    monkeypatch.setattr(import_cmd, "get_settings", lambda: settings, raising=False)

    result = CliRunner().invoke(app, ["import", "status", "--output", "json"])

    assert result.exit_code == 1
    body = json.loads(result.stdout)
    assert body["status"] == "error"
    assert body["error"]["code"]
