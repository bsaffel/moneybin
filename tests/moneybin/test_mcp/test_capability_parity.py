"""Executable capability and durable-outcome parity across CLI and MCP."""

from __future__ import annotations

import importlib
import inspect
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import click
import pytest
from pydantic import SecretStr
from typer.main import get_command
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.connectors.sync_auth import SyncAuthService
from moneybin.connectors.sync_models import DeviceAuthorizationChallenge
from moneybin.database import get_database
from moneybin.mcp.surface import STANDARD_TOOL_NAMES
from moneybin.mcp.tools.import_tools import import_files_coarse
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
from moneybin.secrets import SecretNotFoundError, SecretStore

OUTCOME_MAP_PATH = (
    Path(__file__).parents[2] / "fixtures" / "mcp_capabilities" / "outcome-map.json"
)
UNIMPLEMENTED_CLI_PATHS = {
    "budget delete",
    "budget set",
    "export run",
    "sync key rotate",
    "sync schedule remove",
    "sync schedule set",
    "sync schedule show",
    "transactions categorize ml apply",
    "transactions categorize ml status",
    "transactions categorize ml train",
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
    """Return every executable non-hidden path from the live Typer tree."""
    commands: dict[str, click.Command] = {}

    def walk(command: click.Command, prefix: tuple[str, ...]) -> None:
        if isinstance(command, click.Group):
            if command.invoke_without_command and prefix:
                commands[" ".join(prefix)] = command
            for name, child in command.commands.items():
                if not child.hidden:
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


def test_unimplemented_cli_exclusions_remain_explicit_stubs() -> None:
    commands = registered_cli_commands()
    for path in UNIMPLEMENTED_CLI_PATHS:
        callback = commands[path].callback
        assert callback is not None
        assert "_not_implemented" in inspect.getsource(callback), path


def test_mapped_cli_paths_are_not_explicit_stubs() -> None:
    commands = registered_cli_commands()
    mapped = {path for row in load_outcome_map() for path in row.cli_commands}
    for path in mapped:
        callback = commands[path].callback
        assert callback is not None
        assert "_not_implemented" not in inspect.getsource(callback), path


def test_mapped_service_symbols_exist_and_are_callable() -> None:
    for row in load_outcome_map():
        for path in row.service_methods:
            assert callable(_resolve_symbol(path)), f"{row.capability_id}: {path}"


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


@pytest.mark.unit
async def test_refresh_match_identity_has_same_observable_outcome(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
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
    table_queries = {
        "match_decisions": "SELECT COUNT(*) FROM app.match_decisions",
        "account_links": "SELECT COUNT(*) FROM app.account_links",
        "merchant_links": "SELECT COUNT(*) FROM app.merchant_links",
    }
    assert {
        table: _query_rows(cli_path, query)[0][0]
        for table, query in table_queries.items()
    } == {
        table: _query_rows(mcp_path, query)[0][0]
        for table, query in table_queries.items()
    }


@pytest.mark.unit
async def test_report_execution_returns_same_rows(
    mcp_db: Path,
    tmp_path: Path,
) -> None:
    cli_path, mcp_path = _database_pair(mcp_db, tmp_path)
    _select_database(cli_path)
    cli = CliRunner().invoke(
        app,
        ["reports", "networth", "--output", "json"],
    )
    assert cli.exit_code == 0, cli.output

    _select_database(mcp_path)
    mcp = (
        await reports(
            report_id="core:networth",
            parameters={"as_of": None, "account_ids": None},
        )
    ).to_dict()
    assert json.loads(cli.stdout)["data"] == mcp["data"]["rows"]


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


class _MemorySecrets:
    """Minimal real state store for auth-session outcome parity."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get_key(self, name: str) -> str:
        try:
            return self.values[name]
        except KeyError:
            raise SecretNotFoundError(name) from None

    def set_key(self, name: str, value: str) -> None:
        self.values[name] = value

    def delete_key(self, name: str) -> None:
        try:
            del self.values[name]
        except KeyError:
            raise SecretNotFoundError(name) from None


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
) -> None:
    from moneybin.cli.commands import sync as sync_cli
    from moneybin.connectors import sync_auth as sync_auth_module
    from moneybin.mcp.tools import sync as sync_mcp

    cli_store = _MemorySecrets()
    mcp_store = _MemorySecrets()
    cli_client = _LogoutClient()
    mcp_client = _LogoutClient()
    SyncAuthService(
        client=cast(Any, cli_client),
        secrets=cast(SecretStore, cli_store),
    ).begin()
    SyncAuthService(
        client=cast(Any, mcp_client),
        secrets=cast(SecretStore, mcp_store),
    ).begin()

    monkeypatch.setattr(sync_cli, "_build_sync_client", lambda: cli_client)
    monkeypatch.setattr(sync_auth_module, "SecretStore", lambda: cli_store)
    cli = CliRunner().invoke(app, ["sync", "logout"])
    assert cli.exit_code == 0, cli.output

    monkeypatch.setattr(
        sync_mcp,
        "_build_sync_auth_service",
        lambda: SyncAuthService(
            client=cast(Any, mcp_client),
            secrets=cast(SecretStore, mcp_store),
        ),
    )
    mcp = (await sync_disconnect(mode="logout")).to_dict()
    assert mcp["data"]["status"] == "logged_out"
    assert cli_client.logged_out and mcp_client.logged_out
    assert cli_store.values == mcp_store.values == {}


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
