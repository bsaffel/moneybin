"""Doctor separates local coverage, provider observation age and broker health."""

import json
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from moneybin.config import SyncConfig, get_settings
from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_errors import SyncAPIError
from moneybin.database import Database
from moneybin.metrics.registry import NET_WORTH_STALE_BALANCE_ACCOUNTS
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services import doctor_service
from moneybin.services.doctor_service import DoctorService
from moneybin.utils.user_config import get_or_create_profile_id
from tests.moneybin.db_helpers import create_core_tables
from tests.moneybin.test_extractors.test_simplefin_extractor import load, source


@pytest.fixture(autouse=True)
def unconfigured_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep local diagnostics independent of developer broker configuration."""
    settings = get_settings().model_copy(update={"sync": SyncConfig(server_url=None)})
    monkeypatch.setattr(doctor_service, "get_settings", lambda: settings)


def _check(db: Database, monkeypatch: pytest.MonkeyPatch, name: str) -> Any:
    db.execute("SET TimeZone = 'UTC'")
    create_core_tables(db)
    context = MagicMock()
    context.snapshots = {}

    @contextmanager
    def sqlmesh_context(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        yield context

    monkeypatch.setattr("moneybin.audits.runner.sqlmesh_context", sqlmesh_context)
    report = DoctorService(db).run_all()
    result = next((result for result in report.invariants if result.name == name), None)
    assert result is not None, f"Doctor must report {name}"
    return result


def test_unreachable_broker_is_unknown(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = get_settings().model_copy(
        update={"sync": SyncConfig(server_url="https://broker.synthetic.invalid")}
    )
    get_or_create_profile_id(settings.profile_dir)
    monkeypatch.setattr(doctor_service, "get_settings", lambda: settings)

    def unavailable(self: SyncClient) -> Any:
        raise SyncAPIError("private-native https://credential.invalid/token")

    forbidden_login = MagicMock(
        side_effect=AssertionError("Doctor must not initiate authentication")
    )

    monkeypatch.setattr(SyncClient, "list_institutions", unavailable)
    monkeypatch.setattr(SyncClient, "login", forbidden_login)
    monkeypatch.setattr(SyncClient, "begin_login", forbidden_login)
    result = _check(db, monkeypatch, "sync_health")
    forbidden_login.assert_not_called()
    assert result.status == "warn"
    assert "unknown" in result.detail.lower()
    assert result.recovery_actions
    assert "private-native" not in repr(result)
    assert "credential.invalid" not in repr(result)


def test_missing_session_does_not_login_or_make_network_requests(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = get_settings().model_copy(
        update={"sync": SyncConfig(server_url="https://broker.synthetic.invalid")}
    )
    get_or_create_profile_id(settings.profile_dir)
    monkeypatch.setattr(doctor_service, "get_settings", lambda: settings)
    read_token = MagicMock(return_value=None)
    forbidden_login = MagicMock(side_effect=AssertionError("Interactive login"))
    forbidden_network = MagicMock(side_effect=AssertionError("Network request"))
    monkeypatch.setattr(SyncClient, "_read_token", read_token)
    monkeypatch.setattr(SyncClient, "login", forbidden_login)
    monkeypatch.setattr(SyncClient, "begin_login", forbidden_login)
    monkeypatch.setattr(httpx.Client, "request", forbidden_network)

    result = _check(db, monkeypatch, "sync_health")

    read_token.assert_called_once()
    forbidden_login.assert_not_called()
    forbidden_network.assert_not_called()
    assert result.status == "warn"
    assert "unknown" in result.detail.lower()


def test_unconfigured_doctor_never_constructs_client(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = get_settings().model_copy(update={"sync": SyncConfig(server_url=None)})
    monkeypatch.setattr(doctor_service, "get_settings", lambda: settings)

    def forbidden_client(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Unconfigured doctor must remain local")

    monkeypatch.setattr(SyncClient, "__init__", forbidden_client)
    assert _check(db, monkeypatch, "sync_health").status == "skipped"


def test_doctor_keeps_unprojected_source_visible_without_native_id(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    load(db, source(account_changes={"id": "private-native-999999"}))
    result = _check(db, monkeypatch, "sync_completeness")
    assert result.status == "warn"
    assert "unresolved_identity" in result.detail
    assert result.affected_ids == []
    assert "private-native" not in repr(result)


@pytest.mark.parametrize(("age", "status"), [(30, "pass"), (31, "warn")])
def test_stale_provider_balance_despite_fresh_fetch(
    db: Database, monkeypatch: pytest.MonkeyPatch, age: int, status: str
) -> None:
    now = datetime.now(UTC)
    observed = now - timedelta(days=age)
    data = source(account_changes={"balance-date": int(observed.timestamp())})
    data.batches[0].fetched_at = now
    load(db, data)
    create_core_tables(db)
    db.execute("""INSERT INTO core.dim_accounts
        (account_id, account_type, account_subtype, currency_code, source_type)
        VALUES ('canonical', 'depository', 'checking', 'USD', 'simplefin')""")
    origin = db.execute("SELECT source_origin FROM raw.simplefin_accounts").fetchone()
    assert origin is not None
    AccountLinksRepo(db).insert(
        link_id="canonical-observation",
        account_id="canonical",
        ref_kind="source_native",
        ref_value="acct",
        source_type="simplefin",
        source_origin=str(origin[0]),
        decided_by="auto",
        actor="system",
    )
    db.execute(
        """INSERT INTO core.fct_balances_daily
        (account_id, balance_date, balance, is_observed, in_net_worth,
         observation_source, currency_code)
        VALUES ('canonical', ?, 100.25, TRUE, TRUE, 'simplefin', 'USD')""",
        [observed.date()],
    )

    result = _check(db, monkeypatch, "net_worth_stale_balance")

    assert result.status == status
    assert result.affected_ids == []
    if age == 31:
        assert "1" in result.detail
    assert db.execute("SELECT balance FROM core.fct_balances_daily").fetchone() == (
        Decimal("100.25"),
    )


@pytest.mark.parametrize(
    ("include", "archived", "status"), [(False, False, "pass"), (True, True, "warn")]
)
def test_balance_age_uses_historical_contribution(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    include: bool,
    archived: bool,
    status: str,
) -> None:
    create_core_tables(db)
    db.execute(
        """INSERT INTO core.dim_accounts
        (account_id, account_type, currency_code, include_in_net_worth,
         archived, archived_at)
        VALUES ('historical', 'depository', 'USD', ?, ?,
            CASE WHEN ? THEN CURRENT_DATE END)""",
        [include, archived, archived],
    )
    # Archiving today preserves the account's earlier contribution.
    db.execute(
        """INSERT INTO core.fct_balances_daily
        (account_id, balance_date, balance, is_observed, in_net_worth,
         observation_source)
        VALUES ('historical', CURRENT_DATE - INTERVAL 40 DAY, 100, TRUE, ?, 'ofx')""",
        [include],
    )
    result = _check(db, monkeypatch, "net_worth_stale_balance")
    assert result.status == status
    assert result.affected_ids == []
    if archived:
        assert "1 included accounts" in result.detail


@pytest.mark.parametrize("surface", ["invariant", "cli_json", "mcp_payload"])
@pytest.mark.parametrize("accepted_legacy_target", [False, True])
def test_unresolved_stale_source_id_is_never_published_as_canonical(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
    accepted_legacy_target: bool,
) -> None:
    from typer.testing import CliRunner

    from moneybin.cli.main import app
    from moneybin.mcp.tools.system import system_doctor
    from moneybin.services.doctor_service import DoctorReport

    native_id = "999999123456"
    create_core_tables(db)
    db.execute(
        """INSERT INTO core.dim_accounts
        (account_id, display_name, account_type, account_subtype, currency_code,
         source_type, include_in_net_worth, archived)
        VALUES (?, 'Synthetic unresolved', 'depository', 'checking', 'USD',
            'ofx', TRUE, FALSE)""",
        [native_id],
    )
    # Legacy settings and core rows do not prove the source ID was resolved.
    db.execute(
        """INSERT INTO app.account_settings
        (account_id, include_in_net_worth) VALUES (?, TRUE)""",
        [native_id],
    )
    if accepted_legacy_target:
        AccountLinksRepo(db).insert(
            link_id="legacy-pin",
            account_id=native_id,
            ref_kind="source_native",
            ref_value="synthetic-source-key",
            source_type="ofx",
            source_origin="synthetic",
            decided_by="user",
            actor="user",
        )
    db.execute(
        """INSERT INTO core.fct_balances_daily
        (account_id, balance_date, balance, is_observed, in_net_worth,
         observation_source)
        VALUES (?, CURRENT_DATE - INTERVAL 31 DAY, 100.25, TRUE, TRUE, 'ofx')""",
        [native_id],
    )
    result = _check(db, monkeypatch, "net_worth_stale_balance")
    assert result.status == "warn"

    if surface == "invariant":
        assert result.affected_ids == []
        assert native_id not in repr(result)
        assert "1" in result.detail
        return

    report = DoctorReport(invariants=[result], transaction_count=0)

    @contextmanager
    def database_context(*args: Any, **kwargs: Any) -> Generator[Database, None, None]:
        yield db

    def run_all(self: DoctorService, **kwargs: Any) -> DoctorReport:
        return report

    monkeypatch.setattr(DoctorService, "run_all", run_all)
    monkeypatch.setattr("moneybin.database.get_database", database_context)
    monkeypatch.setattr(
        "moneybin.cli.commands.system.doctor.get_database", database_context
    )
    if surface == "cli_json":
        rendered = CliRunner().invoke(app, ["system", "doctor", "--output", "json"])
        assert rendered.exit_code == 0, rendered.output
        payload = json.loads(rendered.output)
    else:
        payload = system_doctor().to_dict()
    invariant = payload["data"]["invariants"][0]
    assert invariant["status"] == "warn"
    assert invariant["affected_ids"] == []
    assert native_id not in json.dumps(payload)


@pytest.mark.parametrize(
    "ref_kind", ["source_native", "persistent_token", "full_number"]
)
def test_stale_diagnostic_withholds_bound_target_ids(
    db: Database, monkeypatch: pytest.MonkeyPatch, ref_kind: str
) -> None:
    create_core_tables(db)
    db.execute("""INSERT INTO core.dim_accounts
        (account_id, account_type, currency_code, source_type)
        VALUES ('canonical', 'depository', 'USD', 'ofx')""")
    db.execute("""INSERT INTO core.fct_balances_daily
        (account_id, balance_date, balance, is_observed, in_net_worth,
         observation_source)
        VALUES ('canonical', CURRENT_DATE - INTERVAL 31 DAY, 100, TRUE, TRUE, 'ofx')""")
    AccountLinksRepo(db).insert(
        link_id="canonical-proof",
        account_id="canonical",
        ref_kind=ref_kind,
        ref_value="synthetic:999999123456",
        source_type="ofx",
        source_origin="synthetic",
        decided_by="auto",
        actor="system",
    )

    result = _check(db, monkeypatch, "net_worth_stale_balance")

    assert result.status == "warn"
    assert result.affected_ids == []
    assert "1" in result.detail
    assert "canonical" not in repr(result)


@pytest.mark.parametrize(("include", "archived"), [(False, False), (True, True)])
def test_stale_gauge_uses_materialized_contribution_until_transform(
    db: Database, monkeypatch: pytest.MonkeyPatch, include: bool, archived: bool
) -> None:
    create_core_tables(db)
    db.execute("""INSERT INTO core.dim_accounts
        (account_id, account_type, currency_code, include_in_net_worth, archived)
        VALUES ('settings-changed', 'depository', 'USD', TRUE, FALSE)""")
    db.execute("""INSERT INTO core.fct_balances_daily
        (account_id, balance_date, balance, is_observed, in_net_worth,
         observation_source)
        VALUES ('settings-changed', CURRENT_DATE - INTERVAL 40 DAY,
            100, TRUE, TRUE, 'ofx')""")

    stale = _check(db, monkeypatch, "net_worth_stale_balance")
    assert stale.status == "warn"
    assert stale.affected_ids == []
    assert NET_WORTH_STALE_BALANCE_ACCOUNTS._value.get() == 1  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
    assert stale.detail is not None
    assert "moneybin accounts balance assert" in stale.detail
    assert stale.recovery_actions is not None
    assert [action.tool for action in stale.recovery_actions] == ["sync_pull"]

    db.execute(
        """INSERT INTO app.account_settings
        (account_id, include_in_net_worth, archived, archived_at)
        VALUES ('settings-changed', ?, ?, CASE WHEN ? THEN CURRENT_DATE END)""",
        [include, archived, archived],
    )

    unchanged = _check(db, monkeypatch, "net_worth_stale_balance")
    assert unchanged.status == "warn"
    assert unchanged.affected_ids == []
    assert NET_WORTH_STALE_BALANCE_ACCOUNTS._value.get() == 1  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals

    # Model the refreshed evidence: exclusion clears history; archive today does not.
    db.execute(
        """UPDATE core.fct_balances_daily SET in_net_worth = ?
        WHERE account_id = 'settings-changed'""",
        [include],
    )
    refreshed = _check(db, monkeypatch, "net_worth_stale_balance")
    assert refreshed.status == ("warn" if include else "pass")
    assert refreshed.affected_ids == []
    assert NET_WORTH_STALE_BALANCE_ACCOUNTS._value.get() == int(include)  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals


def test_unreadable_balance_evidence_warns_without_resetting_gauge(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_core_tables(db)
    db.execute("ALTER TABLE core.fct_balances_daily DROP COLUMN balance_date")
    NET_WORTH_STALE_BALANCE_ACCOUNTS.set(4242)

    result = _check(db, monkeypatch, "net_worth_stale_balance")

    assert result.status == "warn"
    assert result.detail is not None and "unknown" in result.detail
    assert result.affected_ids == []
    assert NET_WORTH_STALE_BALANCE_ACCOUNTS._value.get() == 4242  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
