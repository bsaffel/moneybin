"""The version-2 broker contract preserves native provider data."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
from pydantic import ValidationError

import moneybin.connectors.sync_models as sync_models

FIXTURES = Path(__file__).parent / "fixtures" / "sync_v2"
_CREDENTIAL_KEYS = {"access_url", "access_token", "setup_token", "secret"}


def _assert_no_credentials(value: object) -> None:
    if isinstance(value, dict):
        for key, child in cast(dict[str, object], value).items():
            assert key not in _CREDENTIAL_KEYS
            _assert_no_credentials(child)
    elif isinstance(value, list):
        for child in cast(list[object], value):
            _assert_no_credentials(child)
    elif isinstance(value, str):
        assert "synthetic-secret" not in value


def _validate_envelope(payload: dict[str, Any]) -> Any:
    envelope_type = getattr(sync_models, "SyncEnvelopeV2", None)
    assert envelope_type is not None, "client v2 envelope type is missing"
    return envelope_type.model_validate(payload)


def _assert_native_scope_declared() -> None:
    assert "native_scope" in sync_models.ScopeOutcome.model_fields


@pytest.mark.parametrize("name", ["plaid", "simplefin_v1", "simplefin_v2", "partial"])
def test_paired_contract_fixtures(name: str) -> None:
    original = json.loads((FIXTURES / f"{name}.json").read_text())

    envelope = _validate_envelope(original)

    assert envelope.contract_version == 2
    assert (
        json.loads(envelope.model_dump_json(by_alias=True, exclude_unset=True))
        == original
    )
    _assert_no_credentials(json.loads(envelope.model_dump_json()))


def test_v2_rejects_wrong_version() -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    original["contract_version"] = 1

    with pytest.raises(ValidationError):
        _validate_envelope(original)


def test_plaid_payload_preserved() -> None:
    original = json.loads((FIXTURES / "plaid.json").read_text())

    envelope = _validate_envelope(original)

    payload = envelope.sources[0].batches[0].payload
    transactions = cast(list[dict[str, Any]], payload["transactions"])
    assert (
        transactions[0]["amount"]
        == original["sources"][0]["batches"][0]["payload"]["transactions"][0]["amount"]
    )


@pytest.mark.parametrize("missing", ["job_id", "contract_version", "status", "sources"])
def test_v2_rejects_missing_envelope_fields(missing: str) -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    del original[missing]

    with pytest.raises(ValidationError):
        _validate_envelope(original)


@pytest.mark.parametrize("provider", ["unsupported", "simplefin-v2"])
def test_v2_rejects_unsupported_provider(provider: str) -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    original["sources"][0]["provider"] = provider

    with pytest.raises(ValidationError):
        _validate_envelope(original)


def test_v2_rejects_unsupported_simplefin_version() -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    original["sources"][0]["protocol_version"] = "3.0.0"

    with pytest.raises(ValidationError):
        _validate_envelope(original)


@pytest.mark.parametrize(
    "missing",
    [
        "provider",
        "protocol_version",
        "connection_id",
        "credential_generation",
        "batches",
        "outcomes",
    ],
)
def test_v2_rejects_missing_source_fields(missing: str) -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    del original["sources"][0][missing]

    with pytest.raises(ValidationError):
        _validate_envelope(original)


@pytest.mark.parametrize("missing", ["batch_id", "fetched_at", "window", "payload"])
def test_v2_rejects_missing_batch_fields(missing: str) -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    del original["sources"][0]["batches"][0][missing]

    with pytest.raises(ValidationError):
        _validate_envelope(original)


@pytest.mark.parametrize(
    "missing", ["account_id", "native_scope", "window", "status", "error_code"]
)
def test_v2_rejects_missing_outcome_fields(missing: str) -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    del original["sources"][0]["outcomes"][0][missing]

    with pytest.raises(ValidationError):
        _validate_envelope(original)


@pytest.mark.parametrize("status", ["pending", "running", "acked"])
def test_v2_rejects_nonterminal_status(status: str) -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    original["status"] = status

    with pytest.raises(ValidationError):
        _validate_envelope(original)


def test_v2_rejects_nonpositive_credential_generation() -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    original["sources"][0]["credential_generation"] = 0

    with pytest.raises(ValidationError):
        _validate_envelope(original)


def test_v2_rejects_plaid_protocol_version() -> None:
    original = json.loads((FIXTURES / "plaid.json").read_text())
    original["sources"][0]["protocol_version"] = "2.0.0-draft"

    with pytest.raises(ValidationError):
        _validate_envelope(original)


def test_v2_rejects_credential_fields() -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    original["sources"][0]["access_url"] = "synthetic-secret"

    with pytest.raises(ValidationError):
        _validate_envelope(original)


@pytest.mark.parametrize("field", ["fetched_at", "window_start", "retry_at"])
def test_v2_requires_aware_timestamps(field: str) -> None:
    original = json.loads((FIXTURES / "partial.json").read_text())
    if field == "fetched_at":
        original["sources"][0]["batches"][0]["fetched_at"] = "2026-09-30T12:00:00"
    elif field == "window_start":
        original["sources"][0]["batches"][0]["window"]["start"] = "2026-09-01T00:00:00"
    else:
        original["sources"][0]["outcomes"][0]["retry_at"] = "2026-10-02T00:00:00"

    with pytest.raises(ValidationError):
        _validate_envelope(original)


@pytest.mark.parametrize("name", ["plaid", "simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize(
    "fetched_at",
    [
        "2026-10-01T12:05:00.000001Z",
        "2026-10-01T08:05:00.000001-04:00",
        "2099-01-01T00:00:00Z",
    ],
)
def test_v2_rejects_fetch_time_beyond_clock_skew(name: str, fetched_at: str) -> None:
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    original["sources"][0]["batches"][0]["fetched_at"] = fetched_at

    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        with pytest.raises(ValidationError, match="fetch time"):
            _validate_envelope(original)


@pytest.mark.parametrize(
    "fetched_at",
    [
        "2026-10-01T12:05:00Z",
        "2026-10-01T08:05:00-04:00",
        "2001-01-01T00:00:00Z",
    ],
)
def test_v2_preserves_valid_fetch_time_and_historical_replay(fetched_at: str) -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    original["sources"][0]["batches"][0]["fetched_at"] = fetched_at

    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        envelope = _validate_envelope(original)
        restored = sync_models.SyncEnvelopeV2.model_validate_json(
            envelope.model_dump_json(by_alias=True)
        )

    batch = restored.sources[0].batches[0]
    expected = datetime.fromisoformat(fetched_at)
    assert batch.fetched_at == expected
    assert batch.fetched_at.utcoffset() == expected.utcoffset()
    assert batch.payload == original["sources"][0]["batches"][0]["payload"]


@pytest.mark.parametrize("through_envelope", [False, True])
@pytest.mark.parametrize(
    "synced_at",
    [
        "2026-10-01T12:05:00.000001Z",
        "2026-10-01T08:05:00.000001-04:00",
        "2099-01-01T00:00:00Z",
    ],
)
def test_plaid_rejects_nested_sync_time_beyond_clock_skew(
    synced_at: str, through_envelope: bool
) -> None:
    original = json.loads((FIXTURES / "plaid.json").read_text())
    original["sources"][0]["batches"][0]["payload"]["metadata"]["synced_at"] = synced_at

    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        payload = (
            _validate_envelope(original).sources[0].batches[0].payload
            if through_envelope
            else original["sources"][0]["batches"][0]["payload"]
        )
        with pytest.raises(ValidationError, match="sync time"):
            sync_models.PlaidSyncDataResponse.model_validate(payload)


@pytest.mark.parametrize(
    "synced_at",
    [
        "2026-10-01T12:05:00Z",
        "2026-10-01T08:05:00-04:00",
        "2001-01-01T00:00:00Z",
    ],
)
def test_plaid_preserves_valid_nested_sync_time_for_replay(synced_at: str) -> None:
    original = json.loads((FIXTURES / "plaid.json").read_text())
    original["sources"][0]["batches"][0]["payload"]["metadata"]["synced_at"] = synced_at

    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        envelope = _validate_envelope(original)
        payload = envelope.sources[0].batches[0].payload
        parsed = sync_models.PlaidSyncDataResponse.model_validate(payload)
        restored = sync_models.PlaidSyncDataResponse.model_validate_json(
            parsed.model_dump_json()
        )

    assert restored.metadata.synced_at == datetime.fromisoformat(synced_at)
    assert restored.metadata.synced_at.tzinfo == UTC
    assert restored.metadata.synced_at != envelope.sources[0].batches[0].fetched_at
    assert restored.transactions[0].transaction_date.isoformat() == "2026-09-10"
    assert payload == original["sources"][0]["batches"][0]["payload"]
    assert payload["metadata"]["synced_at"] == synced_at


def test_v2_rejects_reversed_window() -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    original["sources"][0]["batches"][0]["window"]["end"] = "2026-08-31T00:00:00Z"

    with pytest.raises(ValidationError):
        _validate_envelope(original)


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
def test_native_scope_distinguishes_same_account_id(name: str) -> None:
    _assert_native_scope_declared()
    original = json.loads((FIXTURES / f"{name}.json").read_text())

    envelope = _validate_envelope(original)

    outcomes = envelope.sources[0].outcomes
    assert outcomes[0].account_id == outcomes[1].account_id
    assert outcomes[0].native_scope != outcomes[1].native_scope
    assert [
        item.model_dump(by_alias=True, exclude_unset=True)["native_scope"]
        for item in outcomes
    ] == [item["native_scope"] for item in original["sources"][0]["outcomes"]]


@pytest.mark.parametrize(
    ("name", "wrong_scope"),
    [
        (
            "plaid",
            {"kind": "simplefin_v2_connection", "conn_id": "native-connection-1"},
        ),
        (
            "simplefin_v1",
            {"kind": "simplefin_v2_connection", "conn_id": "native-connection-1"},
        ),
        (
            "simplefin_v2",
            {
                "kind": "simplefin_v1_org",
                "org": {
                    "name": "Synthetic Bank",
                    "sfin-url": "https://example.invalid/simplefin",
                },
            },
        ),
    ],
)
def test_source_rejects_mismatched_native_scope(
    name: str, wrong_scope: dict[str, object]
) -> None:
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    original["sources"][0]["outcomes"][0]["native_scope"] = wrong_scope

    with pytest.raises(ValidationError):
        _validate_envelope(original)


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
def test_account_specific_simplefin_outcome_requires_native_scope(name: str) -> None:
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    original["sources"][0]["outcomes"][0]["native_scope"] = None

    with pytest.raises(ValidationError):
        _validate_envelope(original)


def test_truly_unscoped_outcome_has_null_scope_and_account() -> None:
    original = json.loads((FIXTURES / "partial.json").read_text())

    envelope = _validate_envelope(original)

    outcome = envelope.sources[0].outcomes[1]
    assert outcome.native_scope is None
    assert outcome.account_id is None


def test_plaid_wire_models_have_explicit_internal_names() -> None:
    for name in (
        "PlaidSyncAccount",
        "PlaidSyncTransaction",
        "PlaidSyncBalance",
        "PlaidSyncDataResponse",
    ):
        assert hasattr(sync_models, name), f"missing {name}"


@pytest.mark.parametrize("collection", ["batches", "outcomes"])
def test_plaid_requires_null_window(collection: str) -> None:
    """Cursor data and snapshots cannot claim one requested date window."""
    original = json.loads((FIXTURES / "plaid.json").read_text())
    original["sources"][0][collection][0]["window"] = {
        "start": "2026-09-01T00:00:00Z",
        "end": "2026-10-01T00:00:00Z",
    }
    with pytest.raises(ValidationError):
        _validate_envelope(original)


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize("collection", ["batches", "outcomes"])
def test_simplefin_requires_requested_window(name: str, collection: str) -> None:
    """Null cannot authorize SimpleFIN history coverage."""
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    original["sources"][0][collection][0]["window"] = None
    with pytest.raises(ValidationError):
        _validate_envelope(original)


def test_plaid_null_window_preserves_investment_metadata() -> None:
    """The investment date range remains native metadata beside cursor cash."""
    original = json.loads((FIXTURES / "plaid.json").read_text())
    payload = original["sources"][0]["batches"][0]["payload"]
    payload["metadata"]["investment_start_date"] = "2026-06-01"
    payload["metadata"]["investment_end_date"] = "2026-09-30"
    payload["investment_transactions"] = [{"amount": "-7.125", "date": "2026-06-02"}]
    envelope = _validate_envelope(original)
    assert envelope.sources[0].batches[0].window is None
    assert envelope.sources[0].batches[0].payload == payload
