"""Frozen broker fixtures survive the real local sync and reporting journey."""

from __future__ import annotations

import json
from contextlib import nullcontext
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands import sync as cli_sync
from moneybin.cli.main import app
from moneybin.config import get_settings
from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    LinkInitiateResponse,
    LinkStatusResponse,
    SimpleFINSource,
    SyncDisconnectResponse,
    SyncEnvelopeV2,
)
from moneybin.database import Database
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.reports._framework.catalog import get_report_catalog
from moneybin.services.account_service import AccountService
from moneybin.services.doctor_service import DoctorService
from moneybin.services.sync_completeness import SyncCompletenessService
from moneybin.services.sync_service import SyncService
from moneybin.services.transform_service import TransformService
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env

pytestmark = [pytest.mark.scenarios, pytest.mark.slow]
FIXTURES = Path(__file__).parents[1] / "fixtures" / "sync_v2"


def fixture(name: str) -> SyncEnvelopeV2:
    return SyncEnvelopeV2.model_validate_json((FIXTURES / name).read_text())


def client_for(envelope: SyncEnvelopeV2) -> MagicMock:
    client = MagicMock()
    client.consumer_id = "synthetic-acceptance-profile"
    source = envelope.sources[0]
    now = source.batches[0].fetched_at
    results = [
        item.model_dump(mode="json", exclude={"batches"}) for item in envelope.sources
    ]
    client.trigger_sync.return_value = SimpleNamespace(
        job_id=str(envelope.job_id),
        contract_version=2,
        status=envelope.status,
        transaction_count=0,
    )
    client.get_data.return_value = envelope
    client.get_status.return_value = SimpleNamespace(
        job_id=envelope.job_id,
        contract_version=2,
        status=envelope.status,
        started_at=now,
        completed_at=now,
        acked_at=None,
        results=results,
    )
    client.ack.return_value = SimpleNamespace(
        job_id=envelope.job_id,
        contract_version=2,
        status="acked",
        acked_at=now,
        results=results,
    )
    return client


def balances(db: Database) -> list[tuple[Any, ...]]:
    return db.execute("""SELECT links.ref_value, balances.balance
        FROM core.fct_balances AS balances
        JOIN app.account_links AS links USING (account_id)
        WHERE links.source_type = 'simplefin' ORDER BY links.ref_value""").fetchall()


def raw_payloads(db: Database) -> dict[str, Any]:
    return {
        str(batch_id): json.loads(payload)
        for batch_id, payload in db.execute(
            "SELECT batch_id, original_payload FROM raw.simplefin_responses"
        ).fetchall()
    }


def prohibit_broker_calls(client: MagicMock) -> None:
    client.reset_mock()
    for name in ("trigger_sync", "get_data", "get_status", "ack"):
        getattr(client, name).side_effect = AssertionError(
            "Local recovery/reporting must not contact the broker"
        )


def assert_public_coverage(db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    assert get_settings().sync.server_url is None
    with monkeypatch.context() as guard:
        guard.setattr(
            "moneybin.connectors.sync_client.SyncClient.__init__",
            MagicMock(side_effect=AssertionError("Local reports made a broker client")),
        )
        envelope = (
            get_report_catalog(db)
            .execute(
                db,
                report_id="core:net_worth",
                parameters={},
                limit=100,
            )
            .to_envelope()
        )
        report = envelope.to_dict()
        coverage = report["summary"]["sync_completeness"]
        assert coverage["complete"] is False
        assert "history_unknown" in {reason["kind"] for reason in coverage["reasons"]}
        assert report["summary"]["degraded"] is True
        assert any("doctor" in action.lower() for action in report["actions"])
        doctor = DoctorService(db).run_all()
    checks = {check.name: check for check in doctor.invariants}
    assert checks["sync_completeness"].status == "warn"
    assert "history_unknown" in (checks["sync_completeness"].detail or "")
    assert checks["sync_health"].status == "skipped"
    assert checks["sync_completeness"].affected_ids == []
    for reason in coverage["reasons"]:
        assert reason["affected_account_ids"] == []
        assert reason["affected_account_count"] >= 0
        assert reason["unresolved_scope_count"] >= 0
    disclosed = envelope.to_json() + (checks["sync_completeness"].detail or "")
    assert "acceptance-native" not in disclosed
    assert "synthetic-acceptance-profile" not in disclosed
    assert "Synthetic incomplete account evidence" not in disclosed


def test_shared_link_pull_correction_local_recovery_report_and_disconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = Scenario(
        scenario="simplefin-acceptance",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, _env):
        partial = fixture("acceptance_partial.json")
        source = partial.sources[0]
        assert isinstance(source, SimpleFINSource)
        connection = str(source.connection_id)
        now = source.batches[0].fetched_at
        client = client_for(partial)
        client.initiate_link.return_value = LinkInitiateResponse(
            session_id=str(UUID(int=1200)),
            link_url="https://broker.invalid/claim",
            link_type="widget_flow",
            expiration=now,
        )
        client.poll_link_status.return_value = LinkStatusResponse.model_validate({
            "session_id": str(UUID(int=1200)),
            "status": "linked",
            "provider": "simplefin",
            "connection_id": connection,
            "credential_generation": 1,
            "expiration": now,
        })
        service = SyncService(client=client, db=db, loader=PlaidExtractor(db))
        linked = service.link(provider="simplefin", auto_pull=False)
        assert linked.status == "linked" and linked.connection_id == connection
        client.initiate_link.assert_called_once_with(
            provider="simplefin",
            connection_id=None,
        )
        client.list_institutions.return_value = [
            ConnectedInstitution(
                id=connection,
                provider=source.provider,
                provider_item_id=None,
                status="active",
                created_at=now,
                credential_generation=source.credential_generation,
                protocol_version=source.protocol_version,
            )
        ]
        first = service.pull(connection_id=connection, refresh=False)
        assert first.ingestion_state == "committed"
        assert first.job_status == "partial" and first.ack_state == "confirmed"
        assert TransformService(db).apply().applied
        for native, kind, subtype in [
            ("cash", "depository", "checking"),
            ("credit", "credit", "credit card"),
        ]:
            row = db.execute(
                """SELECT account_id FROM app.account_links
                WHERE source_type='simplefin' AND ref_value=? AND status='accepted'""",
                [native],
            ).fetchone()
            assert row is not None
            AccountService(db).settings_update(
                row[0],
                actor="user",
                account_type=kind,
                account_subtype=subtype,
            )
        prohibit_broker_calls(client)
        assert (
            service.recover(str(partial.job_id), local_only=True).refresh_state
            == "complete"
        )
        assert not client.mock_calls
        assert balances(db) == [("cash", Decimal("100")), ("credit", Decimal("-50"))]
        assert "retrieval_partial" in {
            reason.kind for reason in SyncCompletenessService(db).assess().reasons
        }
        assert_public_coverage(db, monkeypatch)

        corrected = fixture("acceptance_cash_credit.json")
        second_client = client_for(corrected)
        service = SyncService(client=second_client, db=db, loader=PlaidExtractor(db))
        assert service.pull(refresh=False).ingestion_state == "committed"
        assert service.pull(refresh=False).transactions_loaded == 0
        prohibit_broker_calls(second_client)
        factory = MagicMock(return_value=nullcontext(service))
        monkeypatch.setattr(cli_sync, "_build_sync_service", factory)
        repaired = CliRunner().invoke(
            app,
            [
                "sync",
                "pull",
                "--job-id",
                str(corrected.job_id),
                "--local-only",
                "--output",
                "json",
            ],
        )
        # Local work succeeded, but the frozen fixture has unknown history.
        assert repaired.exit_code == 1, repaired.output
        repaired_body = json.loads(repaired.stdout)
        assert repaired_body["data"]["refresh_state"] == "complete"
        assert repaired_body["data"]["complete"] is False
        factory.assert_called_once_with(local_only=True)
        assert not second_client.mock_calls
        assert balances(db) == [("cash", Decimal("105")), ("credit", Decimal("20"))]
        assert db.execute("""SELECT amount FROM prep.int_transactions__unioned
            WHERE source_type='simplefin' ORDER BY amount""").fetchall() == [
            (Decimal("-50"),),
            (Decimal("-15"),),
            (Decimal("70"),),
        ]
        assert db.execute("SELECT count(*) FROM core.fct_transactions").fetchone() == (
            3,
        )
        assert db.execute(
            "SELECT count(*) FROM raw.simplefin_transactions"
        ).fetchone() == (5,)
        originals = raw_payloads(db)
        for envelope in (partial, corrected):
            batch = envelope.sources[0].batches[0]
            assert originals[str(batch.batch_id)] == batch.payload
        assert_public_coverage(db, monkeypatch)
        assert not second_client.mock_calls

        # Credential generations authenticate receipts; native event identity stays stable.
        replacement = corrected.model_copy(deep=True)
        replacement.job_id = UUID(int=1201)
        replacement.sources[0].credential_generation = 2
        replacement.sources[0].batches[0].batch_id = UUID(int=1202)
        next_client = client_for(replacement)
        service = SyncService(client=next_client, db=db, loader=PlaidExtractor(db))
        assert service.pull(refresh=False).ack_state == "confirmed"
        assert db.execute("""SELECT count(DISTINCT transaction_reference)
            FROM raw.simplefin_transactions""").fetchone() == (3,)
        assert (
            db.execute("""SELECT DISTINCT credential_generation
            FROM raw.simplefin_sync_receipts ORDER BY credential_generation""").fetchall()
            == [(1,), (2,)]
        )
        grant = ConnectedInstitution.model_validate({
            "id": connection,
            "provider": "simplefin",
            "provider_item_id": None,
            "institution_name": "Synthetic Acceptance Bank",
            "status": "active",
            "created_at": now,
            "credential_generation": 2,
            "protocol_version": "2.0.0-draft",
            "accounts": [
                {
                    "account_id": native,
                    "name": name,
                    "native_scope": {
                        "kind": "simplefin_v2_connection",
                        "conn_id": "acceptance-native",
                    },
                }
                for native, name in [
                    ("cash", "Synthetic Cash"),
                    ("credit", "Synthetic Credit"),
                ]
            ],
        })
        next_client.list_institutions.return_value = [grant]
        next_client.disconnect.return_value = SyncDisconnectResponse.model_validate({
            "connection_id": connection,
            "credential_generation": 2,
            "accounts": [account.model_dump() for account in grant.accounts],
            "remote_revocation": "user_action_required",
        })
        monkeypatch.setattr(
            cli_sync,
            "_build_sync_service",
            MagicMock(return_value=nullcontext(service)),
        )
        before_disconnect = raw_payloads(db)
        disconnected = CliRunner().invoke(
            app,
            [
                "sync",
                "disconnect",
                "--connection-id",
                connection,
                "--yes",
                "--output",
                "json",
            ],
        )
        assert disconnected.exit_code == 0, disconnected.output
        disconnected_body = json.loads(disconnected.stdout)
        assert disconnected_body["data"]["remote_revocation"] == "user_action_required"
        assert any("Bridge" in action for action in disconnected_body["actions"])
        next_client.disconnect.assert_called_once_with(
            connection,
            credential_generation=2,
            accounts=grant.accounts,
        )
        assert raw_payloads(db) == before_disconnect
        assert all(raw_payloads(db)[key] == value for key, value in originals.items())
        assert "acceptance-native" not in disconnected.stdout
