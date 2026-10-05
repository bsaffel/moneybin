"""Public history keeps coverage and labels without disclosing account targets."""

import json
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from moneybin.adapters.sync_adapters import sync_pull_payload, sync_status_envelope
from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    SimpleFINHistorySnapshot,
    SimpleFINSource,
    SimpleFINV2ConnectionScope,
)
from moneybin.database import Database
from moneybin.extractors.simplefin.identity import source_origin
from tests.moneybin.test_services.test_simplefin_sync_service import (
    _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic fixture
    _envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic fixture
    _service,  # pyright: ignore[reportPrivateUsage]  # shared synthetic fixture
)  # pyright: ignore[reportPrivateUsage]  # shared synthetic fixtures


def _history(*, accounts: bool = True) -> SimpleFINHistorySnapshot:
    return SimpleFINHistorySnapshot.model_validate({
        "observed_at": "2026-10-01T12:00:00Z",
        "replay_required": True,
        "discovery_history_target": "2026-07-01",
        "accounts": [
            {
                "account_id": "acct",
                "native_scope": {
                    "kind": "simplefin_v2_connection",
                    "conn_id": "native-connection",
                },
                "acknowledged_intervals": [],
                "history_target": "2026-07-01",
            }
        ]
        if accounts
        else [],
    })


def test_post_ack_public_history_does_not_rewrite_pre_ack_raw(db: Database) -> None:
    data = _envelope()
    native = data.sources[0]
    assert isinstance(native, SimpleFINSource)
    native.history = _history()
    client = _client(data)
    client.ack.return_value.results = deepcopy(client.ack.return_value.results)
    client.ack.return_value.results[0]["history"]["accounts"][0][
        "acknowledged_intervals"
    ] = [{"start": "2026-09-01T00:00:00Z", "end": "2026-09-20T00:00:00Z"}]
    result = _service(db, client).pull(refresh=False)
    payload = sync_pull_payload(result)
    from dataclasses import asdict

    row = asdict(payload)["sources"][0]
    assert row["history_phase"] == "post_ack"
    assert row["history"]["accounts"][0]["acknowledged_intervals"]
    canonical = db.execute(
        "SELECT account_id FROM app.account_links WHERE source_type='simplefin' AND ref_kind='source_native'"
    ).fetchone()
    assert canonical is not None
    assert result.sources[0].history is not None
    assert result.sources[0].history.accounts[0].account_id == canonical[0]
    assert row["history"]["accounts"][0]["account_id"] is None
    assert row["history"]["accounts"][0]["history_target"] == "2026-07-01"
    assert payload.complete is False
    receipt = db.execute(
        "SELECT source_metadata FROM raw.simplefin_sync_receipts"
    ).fetchone()
    assert receipt is not None
    raw = json.loads(receipt[0])
    assert raw["history"]["accounts"][0]["acknowledged_intervals"] == []


@pytest.mark.parametrize("lost_ack", [False, True])
def test_empty_discovery_target_and_unknown_ack_remain_public(
    db: Database, lost_ack: bool
) -> None:
    data = _envelope(empty=True)
    native = data.sources[0]
    assert isinstance(native, SimpleFINSource)
    native.history = _history(accounts=False)
    client = _client(data)
    if lost_ack:
        client.ack.side_effect = OSError("synthetic lost ack")
    from dataclasses import asdict

    row = asdict(sync_pull_payload(_service(db, client).pull(refresh=False)))[
        "sources"
    ][0]
    assert row["history_phase"] == ("pre_ack" if lost_ack else "post_ack")
    assert row["history"]["discovery_history_target"] == "2026-07-01"
    assert row["history"]["accounts"] == []


def test_unknown_snapshot_is_not_empty_completed_history(db: Database) -> None:
    from dataclasses import asdict

    row = asdict(
        sync_pull_payload(_service(db, _client(_envelope())).pull(refresh=False))
    )["sources"][0]
    assert row["history"] is None
    assert row["history_phase"] == "unknown"


def test_status_scope_mapping_never_leaks_unresolved_native_identity(
    db: Database,
) -> None:
    history = _history()
    other = history.accounts[0].model_copy(deep=True)
    assert isinstance(other.native_scope, SimpleFINV2ConnectionScope)
    other.native_scope.conn_id = (
        "private-native-999999 https://credential.invalid/token"
    )
    history.accounts.append(other)
    connection = ConnectedInstitution(
        id="00000000-0000-0000-0000-000000000064",
        provider="simplefin",
        provider_item_id=None,
        institution_name=None,
        status="active",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        credential_generation=2,
        protocol_version="2.0.0-draft",
        accounts=[],
        history=history,
    )
    from uuid import UUID

    origin = source_origin(
        UUID(connection.id), "2.0.0-draft", {"conn_id": "native-connection"}
    )
    db.execute(
        "INSERT INTO app.account_links (link_id,account_id,ref_kind,ref_value,source_type,source_origin,status,decided_by,decided_at) VALUES ('synthetic-link','canonical-account','source_native','acct','simplefin',?,'accepted','system',CURRENT_TIMESTAMP)",
        [origin],
    )
    client = _client(_envelope())
    client.list_institutions.return_value = [connection]
    connections = _service(db, client).list_connections()
    assert connections[0].safe_history is not None
    assert connections[0].safe_history.accounts[0].account_id == "canonical-account"
    assert connections[0].safe_history.accounts[1].account_id is None
    result = sync_status_envelope(connections, actions=[]).to_dict()
    encoded = json.dumps(result)
    assert "private-native" not in encoded and "credential.invalid" not in encoded
    assert "native-connection" not in encoded
    row = result["data"]["connections"][0]
    assert row["credential_generation"] == 2
    assert row["history_phase"] == "current"
    assert row["provider_balance_at"] is None
    assert row["setup_state"] == "unknown" and row["refresh_state"] == "unknown"
    entries = row["history"]["accounts"]
    assert entries[0]["account_id"] is None
    assert entries[1]["account_id"] is None
    assert entries[1]["display_name"] == "Unresolved account"
    assert entries[1]["history_target"] == "2026-07-01"
