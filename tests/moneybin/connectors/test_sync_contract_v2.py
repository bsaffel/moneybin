"""The version-2 broker contract preserves native provider data."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

import moneybin.connectors.sync_models as sync_models
from moneybin.extractors.simplefin.extractor import (
    SimpleFINExtractor,
    SimpleFINJobContext,
)

FIXTURES = Path(__file__).parent / "fixtures" / "sync_v2"
_CREDENTIAL_KEYS = {"access_url", "access_token", "setup_token", "secret"}


def _job_metadata(
    model: str, started: str, completed: str
) -> SimpleFINJobContext | sync_models.SyncStatusV2Response:
    values: dict[str, Any] = {
        "status": "completed",
        "started_at": started,
        "completed_at": completed,
    }
    if model == "context":
        return SimpleFINJobContext.model_validate(values)
    return sync_models.SyncStatusV2Response.model_validate({
        **values,
        "job_id": "11111111-1111-4111-8111-111111111111",
        "contract_version": 2,
        "transaction_count": 0,
        "error": None,
        "acked_at": None,
        "results": [],
    })


@pytest.mark.parametrize("model", ["context", "status"])
@pytest.mark.parametrize("field", ["started_at", "completed_at"])
@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-10-01T12:05:00.000001Z",
        "2026-10-01T08:05:00.000001-04:00",
        "2099-01-01T00:00:00Z",
    ],
)
def test_job_metadata_rejects_time_beyond_clock_skew(
    model: str, field: str, timestamp: str
) -> None:
    started = timestamp if field == "started_at" else "2026-10-01T12:00:00Z"
    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        with pytest.raises(ValidationError, match="clock skew"):
            _job_metadata(model, started, timestamp)


@pytest.mark.parametrize("model", ["context", "status"])
@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-10-01T12:05:00Z",
        "2026-10-01T08:05:00-04:00",
        "2001-01-01T00:00:00Z",
    ],
)
def test_job_metadata_preserves_clock_boundary_offsets_and_history(
    model: str, timestamp: str
) -> None:
    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        metadata = _job_metadata(model, timestamp, timestamp)
        clock.now.return_value = datetime(2027, 1, 1, tzinfo=UTC)
        restored = type(metadata).model_validate_json(metadata.model_dump_json())
    expected = datetime.fromisoformat(timestamp)
    for value in (restored.started_at, restored.completed_at):
        assert value is not None
        assert value == expected
        assert value.utcoffset() == expected.utcoffset()


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


@pytest.mark.parametrize("name", ["plaid", "simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize(
    ("field", "value"),
    [("error_code", "provider_error"), ("retry_at", "2026-10-02T00:00:00Z")],
)
def test_v2_rejects_contradictory_complete_outcome(
    name: str, field: str, value: str
) -> None:
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    original["sources"][0]["outcomes"][0][field] = value

    with pytest.raises(ValidationError, match="Complete outcome"):
        _validate_envelope(original)


@pytest.mark.parametrize("name", ["plaid", "simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize("status", ["partial", "failed", "deferred"])
def test_v2_preserves_incomplete_outcome_error_and_retry(
    name: str, status: str
) -> None:
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    outcome = original["sources"][0]["outcomes"][0]
    outcome.update(
        status=status, error_code="provider_error", retry_at="2026-10-02T00:00:00Z"
    )

    validated = _validate_envelope(original).sources[0].outcomes[0]

    assert validated.status == status
    assert validated.error_code == "provider_error"
    assert validated.retry_at == datetime(2026, 10, 2, tzinfo=UTC)


@pytest.mark.parametrize("name", ["plaid", "simplefin_v1", "simplefin_v2"])
def test_v2_preserves_complete_no_account_outcome(name: str) -> None:
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    outcome = original["sources"][0]["outcomes"][0]
    outcome["account_id"] = None

    validated = _validate_envelope(original).sources[0].outcomes[0]

    assert validated.status == "complete"
    assert validated.account_id is None
    assert validated.error_code is None
    assert validated.retry_at is None
    assert (
        validated.model_dump(mode="json", exclude_unset=True)["native_scope"]
        == outcome["native_scope"]
    )


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize("boundary", ["envelope", "retained"])
def test_simplefin_complete_no_account_requires_native_scope(
    name: str, boundary: str
) -> None:
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    source = original["sources"][0]
    source["batches"] = []
    source["outcomes"][0].update(account_id=None, native_scope=None)

    with pytest.raises(ValidationError, match="Complete SimpleFIN outcome"):
        if boundary == "envelope":
            _validate_envelope(original)
        else:
            sync_models.SimpleFINSourceResult.model_validate({
                key: value for key, value in source.items() if key != "batches"
            })


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize("boundary", ["envelope", "retained"])
@pytest.mark.parametrize("status", ["partial", "failed", "deferred"])
def test_simplefin_unscoped_error_outcome_is_preserved(
    name: str, boundary: str, status: str
) -> None:
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    source = original["sources"][0]
    source["batches"] = []
    source["outcomes"][0].update(
        account_id=None, native_scope=None, status=status, error_code="provider_error"
    )
    validated = (
        _validate_envelope(original).sources[0]
        if boundary == "envelope"
        else sync_models.SimpleFINSourceResult.model_validate({
            key: value for key, value in source.items() if key != "batches"
        })
    )

    outcome = validated.outcomes[0]
    assert outcome.status == status
    assert outcome.account_id is None
    assert outcome.native_scope is None
    assert outcome.error_code == "provider_error"


@pytest.mark.parametrize("boundary", ["envelope", "retained"])
def test_plaid_complete_no_account_requires_item_scope(boundary: str) -> None:
    original = json.loads((FIXTURES / "plaid.json").read_text())
    source = original["sources"][0]
    source["batches"] = []
    source["outcomes"][0].update(account_id=None, native_scope=None)

    with pytest.raises(ValidationError, match="Plaid item scope"):
        if boundary == "envelope":
            _validate_envelope(original)
        else:
            sync_models.PlaidSourceResult.model_validate({
                key: value for key, value in source.items() if key != "batches"
            })


@pytest.mark.parametrize("boundary", ["envelope", "retained"])
@pytest.mark.parametrize("status", ["partial", "failed"])
def test_plaid_unscoped_error_outcome_is_preserved(boundary: str, status: str) -> None:
    original = json.loads((FIXTURES / "plaid.json").read_text())
    source = original["sources"][0]
    source["batches"] = []
    source["outcomes"][0].update(
        account_id=None, native_scope=None, status=status, error_code="provider_error"
    )

    validated = (
        _validate_envelope(original).sources[0]
        if boundary == "envelope"
        else sync_models.PlaidSourceResult.model_validate({
            key: value for key, value in source.items() if key != "batches"
        })
    )

    outcome = validated.outcomes[0]
    assert outcome.status == status
    assert outcome.account_id is None
    assert outcome.native_scope is None
    assert outcome.error_code == "provider_error"


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


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize("boundary", ["batch", "outcome", "retained"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("start", "2026-09-01T12:00:00Z"),
        ("end", "2026-09-30T00:00:00.000001Z"),
        ("end", "2026-10-02T00:00:00Z"),
    ],
)
def test_simplefin_rejects_incomplete_day_window(
    name: str, boundary: str, field: str, value: str
) -> None:
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    source = original["sources"][0]
    collection = "batches" if boundary == "batch" else "outcomes"
    source[collection][0]["window"][field] = value

    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        with pytest.raises(ValidationError, match="SimpleFIN.*window"):
            if boundary == "retained":
                sync_models.SimpleFINSourceResult.model_validate({
                    key: value for key, value in source.items() if key != "batches"
                })
            else:
                _validate_envelope(original)


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize("end", ["2026-10-01T00:00:00Z", "2001-01-01T00:00:00Z"])
def test_simplefin_complete_day_window_preserves_historical_replay(
    name: str, end: str
) -> None:
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    source = original["sources"][0]
    start = datetime.fromisoformat(end) - timedelta(days=30)
    window = {"start": start.isoformat().replace("+00:00", "Z"), "end": end}
    for item in [*source["batches"], *source["outcomes"]]:
        item["window"] = window.copy()
    for batch in source["batches"]:
        batch["fetched_at"] = end

    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        envelope = _validate_envelope(original)
        retained = sync_models.SimpleFINSourceResult.model_validate({
            key: value for key, value in source.items() if key != "batches"
        })
        clock.now.return_value = datetime(2027, 1, 1, tzinfo=UTC)
        restored = sync_models.SyncEnvelopeV2.model_validate_json(
            envelope.model_dump_json(exclude_unset=True)
        )
        restored_retained = sync_models.SimpleFINSourceResult.model_validate_json(
            retained.model_dump_json()
        )

    assert json.loads(restored.model_dump_json(exclude_unset=True)) == original
    assert restored_retained == retained


@pytest.mark.parametrize("collection", ["batches", "outcomes"])
@pytest.mark.parametrize(
    ("field", "value"),
    [("start", "2026-09-01T12:00:00Z"), ("end", "2026-10-02T00:00:00Z")],
)
def test_simplefin_incomplete_day_window_refused_before_ingestion(
    collection: str, field: str, value: str
) -> None:
    original = json.loads((FIXTURES / "simplefin_v2.json").read_text())
    database = MagicMock()
    database.execute.side_effect = AssertionError("Invalid window reached database")

    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        envelope = sync_models.SyncEnvelopeV2.model_validate(original)
        source = cast(sync_models.SimpleFINSource, envelope.sources[0])
        item = source.batches[0] if collection == "batches" else source.outcomes[0]
        assert item.window is not None
        setattr(item.window, field, datetime.fromisoformat(value))
        context = SimpleFINJobContext(
            status="completed",
            started_at=datetime(2026, 10, 1, 11, tzinfo=UTC),
            completed_at=datetime(2026, 10, 1, 12, tzinfo=UTC),
        )

        with pytest.raises(ValidationError, match="SimpleFIN.*window"):
            SimpleFINExtractor(database).load(
                source, job_id=envelope.job_id, job_context=context
            )

    assert database.mock_calls == []


def test_shared_utc_window_preserves_arbitrary_instant_boundaries() -> None:
    window = sync_models.UTCWindow(
        start=datetime(2099, 1, 1, 12, tzinfo=UTC),
        end=datetime(2099, 1, 2, microsecond=1, tzinfo=UTC),
    )

    assert window.start.hour == 12
    assert window.end.microsecond == 1


def _simplefin_envelope_with_history(name: str) -> dict[str, Any]:
    original = json.loads((FIXTURES / f"{name}.json").read_text())
    source = original["sources"][0]
    source["history"] = {
        "observed_at": "2026-10-01T12:00:00Z",
        "replay_required": False,
        "discovery_history_target": None,
        "accounts": [
            {
                "account_id": outcome["account_id"],
                "native_scope": outcome["native_scope"],
                "acknowledged_intervals": [
                    {"start": "2026-09-01T00:00:00Z", "end": "2026-10-01T00:00:00Z"}
                ],
                "history_target": None,
            }
            for outcome in source["outcomes"]
        ],
    }
    return original


@pytest.mark.parametrize(
    ("identity_field", "metadata_field"),
    [
        ("id", "domain"),
        ("id", "name"),
        ("id", "url"),
        ("domain", "name"),
        ("domain", "url"),
        ("name", "url"),
    ],
)
def test_simplefin_history_rejects_duplicate_canonical_identity(
    identity_field: str, metadata_field: str
) -> None:
    original = _simplefin_envelope_with_history("simplefin_v1")
    source = original["sources"][0]
    account = source["history"]["accounts"][0]
    org = account["native_scope"]["org"]
    org.update(domain="synthetic.invalid", url="https://synthetic.invalid")
    if identity_field != "id":
        org["id"] = None
    if identity_field == "name":
        org["domain"] = None
    credited = deepcopy(account)
    credited["native_scope"]["org"][metadata_field] = "changed-metadata"
    account["acknowledged_intervals"] = []
    account["history_target"] = "2026-01-01"
    source["history"]["accounts"] = [account, credited]
    source["outcomes"] = [source["outcomes"][0]]

    with pytest.raises(ValidationError, match="unique scoped accounts"):
        _validate_envelope(original)


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
def test_simplefin_history_rejects_identical_scoped_accounts(name: str) -> None:
    original = _simplefin_envelope_with_history(name)
    accounts = original["sources"][0]["history"]["accounts"]
    accounts.append(deepcopy(accounts[0]))

    with pytest.raises(ValidationError, match="unique scoped accounts"):
        _validate_envelope(original)


@pytest.mark.parametrize(
    ("name", "identity_field"),
    [
        ("simplefin_v1", "account_id"),
        ("simplefin_v1", "sfin-url"),
        ("simplefin_v1", "id"),
        ("simplefin_v1", "domain"),
        ("simplefin_v1", "name"),
        ("simplefin_v2", "account_id"),
        ("simplefin_v2", "conn_id"),
    ],
)
def test_simplefin_history_preserves_distinct_canonical_identities(
    name: str, identity_field: str
) -> None:
    original = _simplefin_envelope_with_history(name)
    source = original["sources"][0]
    account = source["history"]["accounts"][0]
    if name == "simplefin_v1":
        org = account["native_scope"]["org"]
        org["domain"] = "synthetic.invalid"
        if identity_field in {"domain", "name"}:
            org["id"] = None
        if identity_field == "name":
            org["domain"] = None
    distinct = deepcopy(account)
    if identity_field == "account_id":
        distinct["account_id"] = "distinct-account"
    elif name == "simplefin_v1":
        distinct["native_scope"]["org"][identity_field] = "distinct-identity"
    else:
        distinct["native_scope"]["conn_id"] = "distinct-connection"
    source["history"]["accounts"] = [account, distinct]

    envelope = _validate_envelope(original)

    assert (
        envelope.sources[0].history.model_dump(
            mode="json", by_alias=True, exclude_unset=True
        )
        == source["history"]
    )


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize("end", ["2026-10-02T00:00:00Z", "2099-01-01T00:00:00Z"])
def test_simplefin_history_rejects_coverage_after_current_utc_day(
    name: str, end: str
) -> None:
    original = _simplefin_envelope_with_history(name)
    original["sources"][0]["history"]["accounts"][0]["acknowledged_intervals"][0][
        "end"
    ] = end

    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        with pytest.raises(ValidationError, match="Acknowledged intervals"):
            _validate_envelope(original)


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize(
    "observed_at", ["2026-10-01T12:05:00.000001Z", "2099-01-01T00:00:00Z"]
)
def test_simplefin_history_rejects_observation_beyond_clock_skew(
    name: str, observed_at: str
) -> None:
    original = _simplefin_envelope_with_history(name)
    original["sources"][0]["history"]["observed_at"] = observed_at

    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        with pytest.raises(ValidationError, match="History observation time"):
            _validate_envelope(original)


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize(
    "observed_at", ["2026-10-01T12:05:00Z", "2001-01-01T00:00:00Z"]
)
def test_simplefin_history_preserves_valid_boundaries_and_historical_replay(
    name: str, observed_at: str
) -> None:
    original = _simplefin_envelope_with_history(name)
    history = original["sources"][0]["history"]
    history["observed_at"] = observed_at
    if observed_at.startswith("2001"):
        for account in history["accounts"]:
            account["acknowledged_intervals"] = [
                {"start": "2000-12-01T00:00:00Z", "end": "2001-01-01T00:00:00Z"}
            ]

    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
        envelope = _validate_envelope(original)
        clock.now.return_value = datetime(2027, 1, 1, tzinfo=UTC)
        restored = sync_models.SyncEnvelopeV2.model_validate_json(
            envelope.model_dump_json(by_alias=True, exclude_unset=True)
        )

    assert json.loads(restored.model_dump_json(exclude_unset=True)) == original


@pytest.mark.parametrize("name", ["simplefin_v1", "simplefin_v2"])
@pytest.mark.parametrize("field", ["observed_at", "interval_start", "interval_end"])
@pytest.mark.parametrize("offset", ["", "-04:00"])
def test_simplefin_history_requires_explicit_utc(
    name: str, field: str, offset: str
) -> None:
    original = _simplefin_envelope_with_history(name)
    history = original["sources"][0]["history"]
    if field == "observed_at":
        history[field] = "2026-10-01T08:00:00" + offset
    else:
        interval = history["accounts"][0]["acknowledged_intervals"][0]
        boundary = field.removeprefix("interval_")
        interval[boundary] = interval[boundary].removesuffix("Z") + offset

    with patch.object(sync_models, "datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 1, 12, tzinfo=UTC)
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
