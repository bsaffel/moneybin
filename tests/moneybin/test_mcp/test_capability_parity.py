"""Executable capability and durable-outcome parity across CLI and MCP."""

from __future__ import annotations

import importlib
import json
import shutil
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import click
import keyring.errors
import pytest
from pydantic import SecretStr
from typer.main import get_command
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.connectors.sync_auth import SyncAuthService
from moneybin.connectors.sync_models import DeviceAuthorizationChallenge
from moneybin.database import get_database
from moneybin.mcp.surface import STANDARD_TOOL_NAMES
from moneybin.mcp.tools.accounts import accounts_balances_coarse
from moneybin.mcp.tools.import_tools import import_files_coarse, import_revert_coarse
from moneybin.mcp.tools.investments import investments_coarse
from moneybin.mcp.tools.privacy import privacy_consent_set_coarse
from moneybin.mcp.tools.refresh import refresh_run
from moneybin.mcp.tools.reports import reports
from moneybin.mcp.tools.sql import sql_query
from moneybin.mcp.tools.sync import sync_disconnect
from moneybin.mcp.tools.taxonomy import taxonomy_coarse, taxonomy_set_coarse
from moneybin.mcp.tools.transactions import transactions_annotate_coarse
from moneybin.mcp.write_contracts import (
    CategoryStateRequest,
    MerchantStateRequest,
    TagsSet,
)
from tests.moneybin.db_helpers import create_core_dim_stub_views

OUTCOME_MAP_PATH = (
    Path(__file__).parents[2] / "fixtures" / "mcp_capabilities" / "outcome-map.json"
)
UNIMPLEMENTED_CLI_INVOCATIONS = {
    "budget delete": ("Food",),
    "budget set": ("Food", "100"),
    "export run": (),
    "sync key rotate": (),
    "sync schedule remove": (),
    "sync schedule set": (),
    "sync schedule show": (),
    "transactions categorize ml apply": (),
    "transactions categorize ml status": (),
    "transactions categorize ml train": (),
}
UNIMPLEMENTED_CLI_PATHS = set(UNIMPLEMENTED_CLI_INVOCATIONS)
HIDDEN_COMPATIBILITY_ALIASES = {
    "sync connect": "sync link",
    "sync connect-status": "sync link-status",
}


@dataclass(frozen=True, slots=True)
class Exemption:
    """A narrowly reasoned single-surface capability exemption."""

    surface: str
    category: str
    reason: str


@dataclass(frozen=True, slots=True)
class OutcomeMapRow:
    """One stable capability mapped to its executable surface routes."""

    capability_id: str
    mcp_tools: tuple[str, ...]
    cli_commands: tuple[str, ...]
    service_methods: tuple[str, ...]
    observable_outcomes: tuple[str, ...]
    exemption: Exemption | None


def load_outcome_map(path: Path = OUTCOME_MAP_PATH) -> tuple[OutcomeMapRow, ...]:
    """Load the checked machine-readable capability map."""
    payload = cast(list[dict[str, Any]], json.loads(path.read_text()))
    assert isinstance(payload, list)
    rows: list[OutcomeMapRow] = []
    for item in payload:
        assert set(item) == {
            "capability_id",
            "mcp_tools",
            "cli_commands",
            "service_methods",
            "observable_outcomes",
            "exemption",
        }
        raw_exemption = item["exemption"]
        exemption = (
            None
            if raw_exemption is None
            else Exemption(
                surface=raw_exemption["surface"],
                category=raw_exemption["category"],
                reason=raw_exemption["reason"],
            )
        )
        rows.append(
            OutcomeMapRow(
                capability_id=item["capability_id"],
                mcp_tools=tuple(item["mcp_tools"]),
                cli_commands=tuple(item["cli_commands"]),
                service_methods=tuple(item["service_methods"]),
                observable_outcomes=tuple(item["observable_outcomes"]),
                exemption=exemption,
            )
        )
    return tuple(rows)


def registered_cli_commands() -> dict[str, click.Command]:
    """Return every executable path, including hidden compatibility aliases."""
    commands: dict[str, click.Command] = {}

    def walk(command: click.Command, prefix: tuple[str, ...]) -> None:
        if isinstance(command, click.Group):
            if command.invoke_without_command and prefix:
                commands[" ".join(prefix)] = command
            for name, child in command.commands.items():
                walk(child, (*prefix, name))
            return
        commands[" ".join(prefix)] = command

    walk(get_command(app), ())
    return commands


def _resolve_symbol(path: str) -> Any:
    """Resolve a dotted module/class/member path to its live object."""
    parts = path.split(".")
    for split_at in range(len(parts), 0, -1):
        try:
            value: Any = importlib.import_module(".".join(parts[:split_at]))
        except ModuleNotFoundError:
            continue
        for name in parts[split_at:]:
            value = getattr(value, name)
        return value
    raise AssertionError(f"Cannot import service symbol: {path}")


def test_every_non_exempt_capability_has_both_surfaces() -> None:
    rows = load_outcome_map()
    for row in rows:
        if row.exemption is None:
            assert row.mcp_tools, row.capability_id
            assert row.cli_commands, row.capability_id
        else:
            assert row.exemption.surface in {"cli", "mcp"}, row.capability_id
            assert row.exemption.category in {
                "granular-operator-debug",
                "operator-territory",
                "protocol-only",
                "secret-material",
            }, row.capability_id
            assert row.exemption.reason.strip(), row.capability_id
        assert row.service_methods, row.capability_id
        assert row.observable_outcomes, row.capability_id


def test_capability_ids_are_unique() -> None:
    ids = [row.capability_id for row in load_outcome_map()]
    assert len(ids) == len(set(ids))


def test_mapped_mcp_names_are_exactly_the_standard_registry() -> None:
    mapped = {name for row in load_outcome_map() for name in row.mcp_tools}
    assert mapped == STANDARD_TOOL_NAMES


def test_mapped_cli_paths_exist() -> None:
    registered = set(registered_cli_commands())
    mapped = {path for row in load_outcome_map() for path in row.cli_commands}
    assert mapped <= registered


def test_every_implemented_cli_path_is_mapped() -> None:
    commands = registered_cli_commands()
    mapped = {path for row in load_outcome_map() for path in row.cli_commands}
    assert set(commands) - UNIMPLEMENTED_CLI_PATHS == mapped


@pytest.mark.parametrize(
    ("path", "arguments"),
    sorted(UNIMPLEMENTED_CLI_INVOCATIONS.items()),
)
def test_unimplemented_cli_exclusions_execute_as_explicit_stubs(
    path: str,
    arguments: tuple[str, ...],
) -> None:
    result = CliRunner().invoke(app, [*path.split(), *arguments])

    assert result.exit_code == 0, result.output
    assert "This command is not yet implemented." in result.stderr
    assert "docs/specs/" in result.stderr


def test_hidden_cli_paths_are_explicit_mapped_compatibility_aliases() -> None:
    commands = registered_cli_commands()
    hidden = {path for path, command in commands.items() if command.hidden}
    mapped = {path for row in load_outcome_map() for path in row.cli_commands}

    assert hidden == set(HIDDEN_COMPATIBILITY_ALIASES)
    assert hidden <= mapped
    for alias, canonical in HIDDEN_COMPATIBILITY_ALIASES.items():
        assert commands[alias].hidden
        assert canonical in commands


def test_hidden_sync_aliases_execute_the_canonical_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.cli.commands import sync as sync_cli

    link_calls: list[dict[str, object]] = []
    status_calls: list[dict[str, object]] = []

    def record_link(**kwargs: object) -> None:
        link_calls.append(kwargs)

    def record_status(**kwargs: object) -> None:
        status_calls.append(kwargs)

    monkeypatch.setattr(sync_cli, "sync_link", record_link)
    monkeypatch.setattr(
        sync_cli,
        "sync_link_status",
        record_status,
    )

    connect = CliRunner().invoke(
        app,
        ["sync", "connect", "--institution", "Parity Bank", "--no-browser"],
    )
    connect_status = CliRunner().invoke(
        app,
        ["sync", "connect-status", "--session-id", "link_session_1"],
    )

    assert connect.exit_code == connect_status.exit_code == 0
    assert link_calls == [
        {
            "institution": "Parity Bank",
            "no_pull": False,
            "no_browser": True,
            "yes": False,
            "output": "text",
        }
    ]
    assert status_calls == [{"session_id": "link_session_1", "output": "text"}]


def test_mapped_service_symbols_exist_and_are_callable() -> None:
    for row in load_outcome_map():
        for path in row.service_methods:
            assert callable(_resolve_symbol(path)), f"{row.capability_id}: {path}"


def test_flagged_routes_map_their_actual_owners() -> None:
    rows = {row.capability_id: row for row in load_outcome_map()}

    assert (
        "transactions categorize stats" in rows["system.status"].cli_commands
        and "moneybin.services.categorization.CategorizationService.categorization_stats"
        in rows["system.status"].service_methods
    )
    assert (
        "transactions categorize rules apply"
        in rows["transactions.categorize.run"].cli_commands
        and "moneybin.services.categorization.CategorizationService.categorize_run"
        in rows["transactions.categorize.run"].service_methods
    )
    assert (
        "transactions categorize rules list"
        in rows["transactions.categorize.rules.read"].cli_commands
        and "moneybin.services.categorization.CategorizationService.list_rules"
        in rows["transactions.categorize.rules.read"].service_methods
    )
    assert (
        "categories delete" in rows["taxonomy.set"].cli_commands
        and "moneybin.services.categorization.CategorizationService.delete_category"
        in rows["taxonomy.set"].service_methods
    )
    assert {"import formats list", "import formats show"} <= set(
        rows["import.status"].cli_commands
    )
    assert (
        "moneybin.services.import_service.ImportService.list_formats"
        in rows["import.status"].service_methods
    )
    assert (
        "moneybin.services.import_service.ImportService.delete_saved_format"
        in rows["import.revert"].service_methods
    )


def test_flagged_cli_routes_execute_the_mapped_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import MagicMock

    from moneybin.cli.commands import categories as categories_cli
    from moneybin.cli.commands.transactions import categorize as categorize_cli
    from moneybin.cli.commands.transactions.categorize import rules as rules_cli
    from moneybin.extractors.tabular.formats import load_builtin_formats

    db = MagicMock()
    db_context = MagicMock()
    db_context.__enter__.return_value = db
    categorization_service = MagicMock()
    categorization_service.categorization_stats.return_value = {
        "total": 3,
        "categorized": 2,
        "uncategorized": 1,
        "coverage_pct": 66.7,
    }

    def db_factory(**_: Any) -> MagicMock:
        return db_context

    def categorization_factory(_: Any) -> MagicMock:
        return categorization_service

    monkeypatch.setattr(categorize_cli, "get_database", db_factory)
    monkeypatch.setattr(rules_cli, "get_database", db_factory)
    monkeypatch.setattr(
        "moneybin.services.categorization.CategorizationService",
        categorization_factory,
    )

    stats = CliRunner().invoke(
        app,
        ["transactions", "categorize", "stats", "--output", "json"],
    )
    assert stats.exit_code == 0, stats.output
    categorization_service.categorization_stats.assert_called_once_with()
    categorization_service.list_rules.return_value.rules = []
    listed_rules = CliRunner().invoke(
        app,
        ["transactions", "categorize", "rules", "list", "--output", "json"],
    )
    assert listed_rules.exit_code == 0, listed_rules.output
    categorization_service.list_rules.assert_called_once_with()

    delete_service = MagicMock()

    def deletion_factory(_: Any) -> MagicMock:
        return delete_service

    monkeypatch.setattr(categories_cli, "get_database", db_factory)
    monkeypatch.setattr(
        "moneybin.services.categorization.CategorizationService",
        deletion_factory,
    )
    deleted = CliRunner().invoke(
        app,
        ["categories", "delete", "category_parity", "--output", "json"],
    )
    assert deleted.exit_code == 0, deleted.output
    delete_service.delete_category.assert_called_once_with(
        "category_parity",
        force=False,
        actor="cli",
    )

    builtin = load_builtin_formats()
    import_service = MagicMock()
    import_service.list_formats.return_value = (builtin, builtin, [])

    def import_service_factory(_: Any) -> MagicMock:
        return import_service

    monkeypatch.setattr("moneybin.database.get_database", db_factory)
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService",
        import_service_factory,
    )
    listed = CliRunner().invoke(
        app,
        ["import", "formats", "list", "--output", "json"],
    )
    shown = CliRunner().invoke(
        app,
        ["import", "formats", "show", next(iter(builtin)), "--output", "json"],
    )
    assert listed.exit_code == shown.exit_code == 0
    assert import_service.list_formats.call_count == 2
    assert json.loads(listed.stdout)["formats"]
    assert json.loads(shown.stdout)["format"]["name"] == next(iter(builtin))


def _database_pair(seed: Path, tmp_path: Path) -> tuple[Path, Path]:
    """Copy one initialized database into independent CLI and MCP targets."""
    cli_path = tmp_path / "parity-cli.duckdb"
    mcp_path = tmp_path / "parity-mcp.duckdb"
    shutil.copy(seed, cli_path)
    shutil.copy(seed, mcp_path)
    return cli_path, mcp_path


def _select_database(path: Path) -> None:
    """Point the mcp_db fixture's patched settings at one isolated copy."""
    import moneybin.database as database_module

    database_module.get_settings().database.path = path


def _query_rows(path: Path, query: str) -> list[tuple[Any, ...]]:
    _select_database(path)
    with get_database(read_only=True) as db:
        return db.execute(query).fetchall()


def _seed_transaction(path: Path, transaction_id: str) -> None:
    _select_database(path)
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_transactions (
                transaction_id, account_id, transaction_date, amount,
                amount_absolute, transaction_direction, description,
                transaction_type, is_pending, currency_code, source_type,
                source_extracted_at, loaded_at,
                transaction_year, transaction_month, transaction_day,
                transaction_day_of_week, transaction_year_month,
                transaction_year_quarter
            ) VALUES (?, 'ACC001', '2026-04-10', -12.34, 12.34, 'expense',
                      'Parity Coffee', 'DEBIT', false, 'USD', 'manual',
                      '2026-04-10', CURRENT_TIMESTAMP,
                      2026, 4, 10, 3, '2026-04', '2026-Q2')
            """,
            [transaction_id],
        )


def _seed_refresh_sources(path: Path) -> None:
    """Seed source facts that the match and identity stages must turn into reviews."""
    from moneybin.repositories.account_links_repo import AccountLinksRepo

    _select_database(path)
    with get_database(read_only=False) as db:
        db.execute("CREATE SCHEMA IF NOT EXISTS prep")
        db.execute(
            """
            CREATE OR REPLACE VIEW prep.int_transactions__unioned AS
            SELECT *
            FROM (
                VALUES
                    ('parity_csv', 'ACC001', DATE '2026-04-10',
                     DECIMAL '-12.34', 'Parity Coffee', 'csv',
                     'parity_csv', 'parity.csv', 'USD'),
                    ('parity_ofx', 'ACC001', DATE '2026-04-10',
                     DECIMAL '-12.34', 'Parity Coffee', 'ofx',
                     'parity_ofx', 'parity.ofx', 'USD')
            ) AS source(
                source_transaction_id, account_id, transaction_date, amount,
                description, source_type, source_origin, source_file,
                currency_code
            )
            """
        )
        db.execute(
            """
            UPDATE core.dim_accounts
            SET display_name = 'Parity Checking',
                institution_name = 'Parity Bank',
                last_four = '4242'
            WHERE account_id IN ('ACC001', 'ACC002')
            """
        )
        AccountLinksRepo(db).insert(
            link_id="paritylink01",
            account_id="ACC001",
            ref_kind="source_native",
            ref_value="parity-checking",
            source_type="csv",
            source_origin="parity",
            decided_by="auto",
            actor="test",
        )


def _seed_nonzero_networth(path: Path) -> None:
    """Materialize a meaningful report source instead of the empty schema stub."""
    _select_database(path)
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_balances_daily (
                account_id, balance_date, balance, is_observed,
                observation_source, reconciliation_delta
            ) VALUES
                ('ACC001', '2026-07-01', 5000.00, TRUE, 'ofx', NULL),
                ('ACC002', '2026-07-01', 15000.00, TRUE, 'ofx', NULL)
            """
        )
        db.execute(
            """
            CREATE OR REPLACE VIEW reports.net_worth AS
            SELECT
                d.balance_date,
                SUM(d.balance) AS net_worth,
                COUNT(DISTINCT d.account_id) AS account_count,
                SUM(CASE WHEN d.balance > 0 THEN d.balance ELSE 0 END) AS total_assets,
                SUM(CASE WHEN d.balance < 0 THEN d.balance ELSE 0 END) AS total_liabilities
            FROM core.fct_balances_daily AS d
            INNER JOIN core.dim_accounts AS a ON d.account_id = a.account_id
            WHERE a.include_in_net_worth AND NOT a.archived
            GROUP BY d.balance_date
            """
        )


def _seed_balance_parity(path: Path) -> None:
    """Seed as-of and reconciliation rows in one isolated profile database."""
    _select_database(path)
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_balances_daily (
                account_id, balance_date, balance, is_observed,
                observation_source, reconciliation_delta
            ) VALUES
                ('ACC001', '2026-06-29', 4900.00, TRUE, 'ofx', 10.00),
                ('ACC001', '2026-06-30', 5000.00, TRUE, 'ofx', 25.00)
            """
        )


def _seed_lot_parity(path: Path) -> None:
    """Seed open and closed lots in one isolated profile database."""
    _select_database(path)
    with get_database(read_only=False) as db:
        create_core_dim_stub_views(db)
        db.executemany(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 acquisition_type, original_quantity, remaining_quantity,
                 cost_basis_total, cost_basis_remaining, cost_basis_method,
                 currency_code, is_open, basis_incomplete)
            VALUES (?, 'ACC001', 'SEC_PARITY', '2024-01-15', 'buy', 10, ?,
                    1500, ?, 'fifo', 'USD', ?, FALSE)
            """,
            [
                ["lot_open", Decimal("10"), Decimal("1500"), True],
                ["lot_closed", Decimal("0"), Decimal("0"), False],
            ],
        )


@pytest.mark.unit
async def test_refresh_match_identity_has_same_observable_outcome(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
    _seed_refresh_sources(cli_path)
    _seed_refresh_sources(mcp_path)
    match_query = """
        SELECT source_transaction_id_a, source_transaction_id_b, match_status
        FROM app.match_decisions
        WHERE source_transaction_id_a IN ('parity_csv', 'parity_ofx')
           OR source_transaction_id_b IN ('parity_csv', 'parity_ofx')
        ORDER BY source_transaction_id_a, source_transaction_id_b
    """
    identity_query = """
        SELECT provisional_account_id, candidate_account_id, status
        FROM app.account_link_decisions
        WHERE provisional_account_id IN ('ACC001', 'ACC002')
          AND candidate_account_id IN ('ACC001', 'ACC002')
        ORDER BY provisional_account_id, candidate_account_id
    """
    assert _query_rows(cli_path, match_query) == []
    assert _query_rows(mcp_path, match_query) == []
    assert _query_rows(cli_path, identity_query) == []
    assert _query_rows(mcp_path, identity_query) == []

    _select_database(cli_path)
    cli = CliRunner().invoke(
        app,
        [
            "refresh",
            "--step",
            "match",
            "--step",
            "identity",
            "--output",
            "json",
        ],
    )
    assert cli.exit_code == 0, cli.output

    _select_database(mcp_path)
    mcp = (await refresh_run(steps=["match", "identity"])).to_dict()
    cli_data = json.loads(cli.stdout)["data"]
    assert cli_data["matching_error"] == mcp["data"]["matching_error"]
    assert cli_data["identity_errors"] == mcp["data"]["identity_errors"]
    assert cli_data["matching_error"] is None
    assert cli_data["identity_errors"] == []
    cli_matches = _query_rows(cli_path, match_query)
    mcp_matches = _query_rows(mcp_path, match_query)
    cli_identity = _query_rows(cli_path, identity_query)
    mcp_identity = _query_rows(mcp_path, identity_query)
    assert cli_matches == mcp_matches
    assert cli_identity == mcp_identity
    assert len(cli_matches) == 1
    assert set(cli_matches[0][:2]) == {"parity_csv", "parity_ofx"}
    assert len(cli_identity) == 1
    assert set(cli_identity[0][:2]) == {"ACC001", "ACC002"}


@pytest.mark.unit
async def test_report_execution_returns_same_rows(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
    _seed_nonzero_networth(cli_path)
    _seed_nonzero_networth(mcp_path)
    _select_database(cli_path)
    cli = CliRunner().invoke(
        app,
        ["reports", "networth", "--output", "json"],
    )
    assert cli.exit_code == 0, cli.output

    _select_database(mcp_path)
    mcp_envelope = await reports(
        report_id="core:networth",
        parameters={"as_of": None, "account_ids": None},
    )
    mcp = json.loads(mcp_envelope.to_json())
    cli_payload = json.loads(cli.stdout)
    mcp_data = mcp["data"]
    assert cli_payload["data"] == mcp_data["rows"]
    assert mcp_data["rows"]
    assert any(
        float(value) != 0
        for row in mcp_data["rows"]
        for key, value in row.items()
        if key in {"balance", "net_worth"} and value is not None
    )
    assert [column["name"] for column in mcp_data["columns"]] == list(
        mcp_data["rows"][0]
    )
    assert mcp_data["count"] == len(mcp_data["rows"])
    assert mcp_data["truncated"] is False
    assert mcp_data["semantics"]["provenance"]
    assert mcp["summary"]["returned_count"] == mcp_data["count"]
    assert mcp["summary"]["has_more"] is mcp_data["truncated"]


@pytest.mark.unit
async def test_balance_as_of_and_reconciliation_have_same_rows(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
    _seed_balance_parity(cli_path)
    _seed_balance_parity(mcp_path)

    _select_database(cli_path)
    cli_show = CliRunner().invoke(
        app,
        [
            "accounts",
            "balance",
            "show",
            "--account",
            "ACC001",
            "--as-of",
            "2026-06-29",
            "--output",
            "json",
        ],
    )
    cli_reconcile = CliRunner().invoke(
        app,
        [
            "accounts",
            "balance",
            "reconcile",
            "--account",
            "ACC001",
            "--threshold",
            "20",
            "--output",
            "json",
        ],
    )
    assert cli_show.exit_code == cli_reconcile.exit_code == 0

    _select_database(mcp_path)
    mcp_show = await accounts_balances_coarse(
        view="latest",
        reference="ACC001",
        as_of=date(2026, 6, 29),
    )
    mcp_reconcile = await accounts_balances_coarse(
        view="reconcile",
        reference="ACC001",
        threshold=Decimal("20"),
    )

    assert (
        json.loads(cli_show.stdout)["data"]["observations"]
        == json.loads(mcp_show.to_json())["data"]["observations"]
    )
    assert (
        json.loads(cli_reconcile.stdout)["data"]["observations"]
        == (json.loads(mcp_reconcile.to_json())["data"]["observations"])
    )


@pytest.mark.unit
async def test_investment_lot_open_and_all_have_same_rows(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
    _seed_lot_parity(cli_path)
    _seed_lot_parity(mcp_path)

    _select_database(cli_path)
    cli_open = CliRunner().invoke(
        app,
        ["investments", "lots", "list", "--output", "json"],
    )
    cli_all = CliRunner().invoke(
        app,
        ["investments", "lots", "list", "--all", "--output", "json"],
    )
    assert cli_open.exit_code == cli_all.exit_code == 0

    _select_database(mcp_path)
    mcp_open = await investments_coarse(view="lots", open_only=True)
    mcp_all = await investments_coarse(view="lots", open_only=False)

    assert (
        json.loads(cli_open.stdout)["data"]["rows"]
        == json.loads(mcp_open.to_json())["data"]["rows"]
    )
    assert (
        json.loads(cli_all.stdout)["data"]["rows"]
        == json.loads(mcp_all.to_json())["data"]["rows"]
    )


@pytest.mark.unit
async def test_annotation_writes_same_complete_tag_state(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
    _seed_transaction(cli_path, "TXN_PARITY_TAG")
    _seed_transaction(mcp_path, "TXN_PARITY_TAG")

    _select_database(cli_path)
    cli = CliRunner().invoke(
        app,
        [
            "transactions",
            "tags",
            "add",
            "TXN_PARITY_TAG",
            "parity-tag",
            "--output",
            "json",
        ],
    )
    assert cli.exit_code == 0, cli.output

    _select_database(mcp_path)
    await transactions_annotate_coarse(
        requests=[
            TagsSet(
                kind="tags_set",
                transaction_id="TXN_PARITY_TAG",
                tags=["parity-tag"],
            )
        ]
    )
    query = """
        SELECT transaction_id, tag
        FROM app.transaction_tags
        ORDER BY transaction_id, tag
    """
    assert _query_rows(cli_path, query) == _query_rows(mcp_path, query)


@pytest.mark.unit
async def test_taxonomy_writes_same_category_state(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
    _select_database(cli_path)
    cli_create = CliRunner().invoke(app, ["categories", "create", "Parity Travel"])
    assert cli_create.exit_code == 0, cli_create.output
    category_id = cli_create.stdout.strip()
    cli_set = CliRunner().invoke(
        app,
        ["categories", "set", category_id, "--inactive"],
    )
    assert cli_set.exit_code == 0, cli_set.output
    cli_merchant = CliRunner().invoke(
        app,
        ["merchants", "create", "PARITY SHOP", "Parity Shop"],
    )
    assert cli_merchant.exit_code == 0, cli_merchant.output

    _select_database(mcp_path)
    await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Parity Travel",
            ),
            MerchantStateRequest(
                kind="merchant",
                state="present",
                raw_pattern="PARITY SHOP",
                canonical_name="Parity Shop",
                match_type="contains",
            ),
        ]
    )
    mcp_category_id = _query_rows(
        mcp_path,
        """
        SELECT category_id
        FROM app.user_categories
        WHERE category = 'Parity Travel'
        """,
    )[0][0]
    _select_database(mcp_path)
    await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="inactive",
                category_id=mcp_category_id,
            )
        ]
    )
    category_query = """
        SELECT category, subcategory, is_active
        FROM app.user_categories
        WHERE category = 'Parity Travel'
    """
    merchant_query = """
        SELECT raw_pattern, match_type, canonical_name, category, subcategory
        FROM app.user_merchants
        WHERE canonical_name = 'Parity Shop'
    """
    assert _query_rows(cli_path, category_query) == _query_rows(
        mcp_path,
        category_query,
    )
    assert _query_rows(cli_path, merchant_query) == _query_rows(
        mcp_path,
        merchant_query,
    )

    _select_database(cli_path)
    cli_categories = CliRunner().invoke(
        app,
        ["categories", "list", "--include-inactive", "--output", "json"],
    )
    cli_merchants = CliRunner().invoke(
        app,
        ["merchants", "list", "--output", "json"],
    )
    assert cli_categories.exit_code == cli_merchants.exit_code == 0
    _select_database(mcp_path)
    mcp_categories = (
        await taxonomy_coarse(view="categories", include_inactive=True)
    ).to_dict()
    mcp_merchants = (await taxonomy_coarse(view="merchants")).to_dict()
    assert len(json.loads(cli_categories.stdout)["data"]["categories"]) == len(
        mcp_categories["data"]["rows"]
    )
    assert len(json.loads(cli_merchants.stdout)["data"]["merchants"]) == len(
        mcp_merchants["data"]["rows"]
    )


@pytest.mark.unit
async def test_consent_writes_same_effective_grant(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
    _select_database(cli_path)
    cli = CliRunner().invoke(
        app,
        [
            "privacy",
            "grant",
            "mcp-data-sharing",
            "--backend",
            "parity",
            "--yes",
        ],
    )
    assert cli.exit_code == 0, cli.output

    _select_database(mcp_path)
    await privacy_consent_set_coarse(
        categories=["mcp-data-sharing"],
        state="granted",
        backend="parity",
    )
    query = """
        SELECT feature_category, backend, consent_mode, revoked_at IS NULL
        FROM app.ai_consent_grants
        WHERE feature_category = 'mcp-data-sharing'
    """
    assert _query_rows(cli_path, query) == _query_rows(mcp_path, query)


@pytest.mark.unit
async def test_import_writes_same_log_and_raw_row_counts(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
    source = Path(__file__).parents[2] / "fixtures" / "sample_statement.qfx"
    _select_database(cli_path)
    cli = CliRunner().invoke(
        app,
        [
            "import",
            "files",
            str(source),
            "--no-refresh",
            "--output",
            "json",
        ],
    )
    assert cli.exit_code == 0, cli.output

    _select_database(mcp_path)
    mcp = (
        await import_files_coarse(
            paths=[str(source)],
            refresh=False,
        )
    ).to_dict()
    assert "data" in mcp
    log_query = """
        SELECT source_type, status, rows_imported, rows_rejected
        FROM raw.import_log
        ORDER BY started_at
    """
    assert _query_rows(cli_path, log_query) == _query_rows(mcp_path, log_query)
    assert _query_rows(
        cli_path,
        "SELECT COUNT(*) FROM raw.ofx_transactions",
    ) == _query_rows(
        mcp_path,
        "SELECT COUNT(*) FROM raw.ofx_transactions",
    )


@pytest.mark.unit
async def test_saved_format_deletion_has_same_audited_outcome(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    from moneybin.extractors.tabular.formats import TabularFormat, save_format_to_db

    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
    saved = TabularFormat(
        name="parity_saved",
        institution_name="Parity Bank",
        header_signature=["Date", "Amount"],
        field_mapping={"transaction_date": "Date", "amount": "Amount"},
        sign_convention="negative_is_expense",
        date_format="%m/%d/%Y",
    )
    for path in (cli_path, mcp_path):
        _select_database(path)
        with get_database(read_only=False) as db:
            save_format_to_db(db, saved, actor="test")

    _select_database(cli_path)
    cli = CliRunner().invoke(
        app,
        ["import", "formats", "delete", "parity_saved", "--yes"],
    )
    assert cli.exit_code == 0, cli.output

    _select_database(mcp_path)
    required = await import_revert_coarse(
        operation="delete_saved_format",
        format_name="parity_saved",
    )
    assert required.error is not None
    assert required.error.details is not None
    mcp = (
        await import_revert_coarse(
            operation="delete_saved_format",
            format_name="parity_saved",
            confirmation_token=str(required.error.details["confirmation_token"]),
        )
    ).to_dict()
    assert mcp["data"]["status"] == "deleted"
    assert mcp["data"]["operation_id"]

    state_query = """
        SELECT COUNT(*)
        FROM app.tabular_formats
        WHERE name = 'parity_saved'
    """
    audit_query = """
        SELECT action, target_id
        FROM app.audit_log
        WHERE action = 'tabular_format.delete'
        ORDER BY occurred_at
    """
    assert (
        _query_rows(cli_path, state_query)
        == _query_rows(mcp_path, state_query)
        == [(0,)]
    )
    assert _query_rows(cli_path, audit_query) == _query_rows(mcp_path, audit_query)


class _LogoutClient:
    """Sync client fake with real persisted session orchestration around it."""

    def __init__(self) -> None:
        self.logged_out = False

    def begin_login(self) -> DeviceAuthorizationChallenge:
        return DeviceAuthorizationChallenge(
            device_code=SecretStr("secret-device-code"),
            user_code="ABCD-1234",
            verification_uri="https://sync.example/device",
            verification_uri_complete="https://sync.example/device?code=ABCD-1234",
            expires_in=600,
            interval=1,
        )

    def logout(self) -> None:
        self.logged_out = True


@pytest.mark.unit
async def test_sync_logout_clears_same_persisted_session_outcome(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from moneybin.cli.commands import sync as sync_cli
    from moneybin.config import set_current_profile
    from moneybin.mcp.tools import sync as sync_mcp

    keyring_values: dict[tuple[str, str], str] = {}

    def get_password(service: str, name: str) -> str | None:
        return keyring_values.get((service, name))

    def set_password(service: str, name: str, value: str) -> None:
        keyring_values[(service, name)] = value

    def delete_password(service: str, name: str) -> None:
        try:
            del keyring_values[(service, name)]
        except KeyError:
            raise keyring.errors.PasswordDeleteError from None

    monkeypatch.setattr("keyring.get_password", get_password)
    monkeypatch.setattr("keyring.set_password", set_password)
    monkeypatch.setattr("keyring.delete_password", delete_password)
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))

    cli_client = _LogoutClient()
    mcp_client = _LogoutClient()

    set_current_profile("cli-parity")
    SyncAuthService(client=cast(Any, cli_client)).begin()
    monkeypatch.setattr(sync_cli, "_build_sync_client", lambda: cli_client)
    cli = CliRunner().invoke(app, ["sync", "logout"])
    assert cli.exit_code == 0, cli.output

    set_current_profile("mcp-parity")
    SyncAuthService(client=cast(Any, mcp_client)).begin()
    monkeypatch.setattr(
        sync_mcp,
        "_build_sync_auth_service",
        lambda: SyncAuthService(client=cast(Any, mcp_client)),
    )
    mcp = (await sync_disconnect(mode="logout")).to_dict()
    assert mcp["data"]["status"] == "logged_out"
    assert cli_client.logged_out and mcp_client.logged_out
    assert keyring_values == {}


@pytest.mark.unit
async def test_sql_returns_same_rows_and_critical_masking(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
    query = """
        SELECT account_id, routing_number
        FROM core.dim_accounts
        ORDER BY account_id
    """
    _select_database(cli_path)
    cli = CliRunner().invoke(
        app,
        ["sql", "query", query, "--output", "json"],
    )
    assert cli.exit_code == 0, cli.output

    _select_database(mcp_path)
    mcp = (await sql_query(query=query)).to_dict()
    cli_rows = json.loads(cli.stdout)["data"]
    assert cli_rows == mcp["data"]
    assert all(row["routing_number"].startswith("****") for row in cli_rows)
